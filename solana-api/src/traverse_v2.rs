use crate::convert::{decode_ui_transaction, TxDecodeError};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::{
    solana_sdk::commitment_config::CommitmentLevel,
    types::{SolanaBlock, SolanaTransaction},
};
use futures_util::Stream;
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError;
use solana_rpc_client_api::custom_error;
use std::ops::{Bound, RangeBounds};
use std::time::Duration;
use thiserror::Error;

use crate::solana_api::SolanaApi;

#[derive(Debug, Error)]
pub enum TraverseError {
    #[error("rpc client error: {0}")]
    RpcError(#[from] ClientError),
    #[error("could not decode transaction: {0}")]
    TxDecodeError(#[from] TxDecodeError),
    #[error("invalid signature: {0}")]
    InvalidSignature(#[from] ParseSignatureError),
}

#[derive(Debug, Clone)]
pub struct TraverseConfig {
    pub endpoint: String,
    /// Request only finalized transactions
    pub finalized: bool,
    pub rps_limit_sleep: Option<Duration>,
    pub status_poll_interval: Duration,
    /// Request only successful transactions
    pub only_success: bool,
    pub target_key: Pubkey,
    pub last_observed: Option<Signature>,
    pub signature_buffer_limit: Option<usize>,
}

impl Default for TraverseConfig {
    fn default() -> Self {
        TraverseConfig {
            endpoint: String::default(),
            finalized: true,
            rps_limit_sleep: None,
            status_poll_interval: Duration::from_secs(1),
            only_success: true,
            target_key: Pubkey::default(),
            last_observed: None,
            signature_buffer_limit: None,
        }
    }
}

#[derive(Debug)]
pub enum LedgerItem {
    Block {
        block: SolanaBlock,
        txs: Vec<SolanaTransaction>,
        commitment: CommitmentLevel,
    },
    MissingBlock {
        slot: u64,
    },
    BlockUpdate {
        slot: u64,
        commitment: CommitmentLevel,
    },
}

pub struct TraverseLedger {
    api: SolanaApi,
    commitment: CommitmentLevel,
}

impl TraverseLedger {
    pub fn new(config: TraverseConfig) -> Self {
        let commitment = if config.finalized {
            CommitmentLevel::Finalized
        } else {
            CommitmentLevel::Confirmed
        };
        TraverseLedger {
            api: SolanaApi::new(config.endpoint, config.finalized),
            commitment,
        }
    }

    pub async fn since_signature(
        &self,
        signature: Signature,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> {
        let tx = self.api.get_transaction(&signature).await.unwrap();
        self.in_range(tx.slot..)
    }

    pub fn in_range<R: RangeBounds<u64> + Iterator<Item = u64> + 'static>(
        &self,
        range: R,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> {
        tracing::info!(start = ?range.start_bound(), end = ?range.end_bound(), "traversing range");
        let api = self.api.clone();
        let commitment = self.commitment;
        let is_finalized = matches!(self.commitment, CommitmentLevel::Finalized);

        stream_generator::generate_try_stream(move |mut stream| async move {
            if matches!(range.start_bound(), Bound::Unbounded) {
                todo!("implement search for start of history");
            }

            for slot in range.step_by(1) {
                tracing::info!(%slot, "getting block");
                let mut current_slot = api.get_slot(commitment).await?;

                while slot > current_slot {
                    tracing::info!(%slot, "waiting for slot");
                    current_slot = api.get_slot(commitment).await?;
                }

                let block = match api.get_block(slot, true).await {
                    Ok(block) => block,
                    Err(err) if matches!(err.kind, ClientErrorKind::RpcError(
                            RpcError::RpcResponseError { code: custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED, .. })
                    ) => {
                        tracing::info!(%slot, "skipped slot");
                        stream.send(Ok(LedgerItem::MissingBlock { slot })).await;
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                let ui_txs = block.transactions;
                let block = SolanaBlock {
                    slot,
                    parent_slot: block.parent_slot,
                    parent_hash: block
                        .previous_blockhash
                        .parse()
                        .map_err(TxDecodeError::from)?,
                    hash: block.blockhash.parse().map_err(TxDecodeError::from)?,
                    time: block.block_time,
                    is_finalized,
                };
                let mut txs = Vec::new();
                for tx in ui_txs.into_iter().flatten() {
                    let tx = decode_ui_transaction(tx, slot)?;
                    txs.push(tx);
                }
                tracing::info!(%slot, count = txs.len(), "fetched transactions");
                stream
                    .send(Ok(LedgerItem::Block {
                        block,
                        txs,
                        commitment,
                    }))
                    .await;
            }
            Ok(())
        })
    }
}
