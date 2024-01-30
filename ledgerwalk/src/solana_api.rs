#[cfg(test)]
mod mock;

use common::solana_sdk::commitment_config::CommitmentLevel;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::transaction::Transaction;
use common::solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use common::solana_transaction_status::{TransactionStatus, UiTransactionEncoding};
use solana_client::client_error::Result as ClientResult;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;

pub const SIGNATURES_LIMIT: usize = 1000;

pub struct SolanaApi {
    client: RpcClient,
}

impl SolanaApi {
    pub fn new(endpoint: impl ToString) -> Self {
        SolanaApi {
            client: RpcClient::new(endpoint.to_string()),
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
        self.client
            .get_signatures_for_address_with_config(
                address,
                GetConfirmedSignaturesForAddress2Config {
                    before: max,
                    until: min,
                    limit: Some(SIGNATURES_LIMIT),
                    commitment: None,
                },
            )
            .await
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
        self.client
            .get_transaction_with_config(
                signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: None,
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
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
            let client = RpcClient::new_sender(sender, Default::default());
            Self { client }
        }
    }
}
