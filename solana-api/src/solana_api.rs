#[cfg(test)]
mod mock;

use std::sync::Arc;

use async_trait::async_trait;
use common::solana_sdk::account::Account;
use common::solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use common::solana_sdk::hash::Hash;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::slot_history::Slot;
use common::solana_sdk::transaction::Transaction;
use common::solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
};
use common::solana_transaction_status::{TransactionStatus, UiTransactionEncoding};
use solana_client::client_error::Result as ClientResult;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClientConfig};
use solana_client::rpc_config::{RpcBlockConfig, RpcSendTransactionConfig, RpcTransactionConfig};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_client::rpc_sender::RpcSender;
use solana_rpc_client::http_sender::HttpSender;

use crate::metrics::metrics;

pub const SIGNATURES_LIMIT: usize = 1000;

#[derive(Clone)]
pub struct SolanaApi {
    client: Arc<RpcClient>,
    commitment: CommitmentLevel,
}

impl std::fmt::Debug for SolanaApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SolanaApi")
            .field("client", &"RpcClient")
            .field("commitment", &self.commitment)
            .finish()
    }
}

impl SolanaApi {
    pub fn new(endpoint: impl ToString, finalized: bool) -> Self {
        let commitment = if finalized {
            CommitmentLevel::Finalized
        } else {
            CommitmentLevel::Confirmed
        };
        Self {
            client: Arc::new(RpcClient::new_sender(
                LoggedSender(HttpSender::new(endpoint.to_string())),
                RpcClientConfig::default(),
            )),
            commitment,
        }
    }

    pub async fn get_recent_blockhash(&self) -> ClientResult<Hash> {
        self.client.get_latest_blockhash().await
    }

    pub async fn send_transaction(&self, transaction: &Transaction) -> ClientResult<Signature> {
        self.client
            .send_transaction_with_config(
                transaction,
                RpcSendTransactionConfig {
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    ..Default::default()
                },
            )
            .await
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: &[Signature],
    ) -> ClientResult<Vec<Option<TransactionStatus>>> {
        let res = self.client.get_signature_statuses(signatures).await?.value;
        Ok(res)
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        min: Option<Signature>,
        max: Option<Signature>,
    ) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        metrics().get_signatures_for_address.inc();
        self.client
            .get_signatures_for_address_with_config(
                address,
                GetConfirmedSignaturesForAddress2Config {
                    before: max,
                    until: min,
                    limit: Some(SIGNATURES_LIMIT),
                    commitment: Some(CommitmentConfig {
                        commitment: self.commitment,
                    }),
                },
            )
            .await
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
        metrics().get_transaction.inc();
        self.client
            .get_transaction_with_config(
                signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: Some(CommitmentConfig {
                        commitment: self.commitment,
                    }),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
    }

    pub async fn get_slot_with_commitment(&self) -> ClientResult<Slot> {
        self.client
            .get_slot_with_commitment(CommitmentConfig {
                commitment: self.commitment,
            })
            .await
    }

    pub async fn get_block(&self, slot: Slot, full: bool) -> ClientResult<UiConfirmedBlock> {
        metrics().get_block.inc();
        let details = if full {
            TransactionDetails::Full
        } else {
            TransactionDetails::Signatures
        };
        self.client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(details),
                    rewards: Some(false),
                    commitment: Some(CommitmentConfig {
                        commitment: self.commitment,
                    }),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
    }

    pub async fn get_finalized_slot(&self) -> ClientResult<Slot> {
        self.get_slot(CommitmentLevel::Finalized).await
    }

    pub async fn get_slot(&self, commitment: CommitmentLevel) -> ClientResult<Slot> {
        self.client
            .get_slot_with_commitment(CommitmentConfig { commitment })
            .await
    }

    pub async fn get_finalized_blocks(&self, from: u64) -> ClientResult<Vec<Slot>> {
        metrics().get_blocks.inc();
        self.client
            .get_blocks_with_commitment(from, None, CommitmentConfig::finalized())
            .await
    }

    pub async fn get_account(&self, key: &Pubkey) -> ClientResult<Option<Account>> {
        self.client
            .get_account_with_commitment(
                key,
                CommitmentConfig {
                    commitment: self.commitment,
                },
            )
            .await
            .map(|response| response.value)
    }
}

struct LoggedSender(HttpSender);

#[async_trait]
impl RpcSender for LoggedSender {
    fn get_transport_stats(&self) -> solana_client::rpc_sender::RpcTransportStats {
        self.0.get_transport_stats()
    }

    fn url(&self) -> String {
        self.0.url()
    }

    async fn send(
        &self,
        request: solana_client::rpc_request::RpcRequest,
        params: serde_json::Value,
    ) -> ClientResult<serde_json::Value> {
        tracing::trace!(?request, ?params, "sending request");
        let result = self.0.send(request, params).await;
        tracing::trace!(?result, "request result");
        result
    }
}

#[cfg(test)]
mod test_ext {
    use std::sync::Arc;

    use solana_client::rpc_sender::RpcSender;

    use super::*;

    impl SolanaApi {
        pub fn test() -> (Self, Arc<mock::SharedMock>) {
            let sender = mock::MockSender::new();
            let control = sender.shared.clone();

            (Self::with_sender(sender), control)
        }

        #[cfg(test)]
        pub fn with_sender(sender: impl RpcSender + Send + Sync + 'static) -> Self {
            let client = Arc::new(RpcClient::new_sender(sender, Default::default()));
            Self {
                client,
                commitment: CommitmentLevel::Confirmed,
            }
        }
    }
}
