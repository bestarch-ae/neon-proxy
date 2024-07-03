use std::ops::RangeBounds;
use std::time::Duration;

use crate::convert::{decode_ui_transaction, TxDecodeError};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::{
    solana_sdk::commitment_config::CommitmentLevel,
    types::{SolanaBlock, SolanaTransaction},
};
use either::Either;
use futures_util::{Stream, StreamExt};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError;
use solana_rpc_client_api::custom_error;
use thiserror::Error;
use tokio::time::sleep;

use crate::metrics::metrics;
use crate::solana_api::SolanaApi;

const RECHECK_INTERVAL: Duration = Duration::from_secs(1);

macro_rules! retry {
    ($val:expr, $message:literal) => {
        loop {
            match $val.await {
                Ok(val) => break val,
                Err(err) => {
                    tracing::warn!(%err, retry_in = ?RECHECK_INTERVAL, $message);
                    sleep(RECHECK_INTERVAL).await
                }
            }
        }
    }
}

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
    target_key: Pubkey,
}

impl TraverseLedger {
    pub fn new(config: TraverseConfig) -> Self {
        let commitment = if config.finalized {
            CommitmentLevel::Finalized
        } else {
            CommitmentLevel::Confirmed
        };
        TraverseLedger {
            target_key: config.target_key,
            api: SolanaApi::new(config.endpoint, config.finalized),
            commitment,
        }
    }

    async fn find_beginning_slot(api: SolanaApi, target: Pubkey) -> Result<u64, TraverseError> {
        let mut earliest_slot = u64::MAX;
        let mut earliest_signature: Option<Signature> = None;

        loop {
            tracing::info!(slot = %earliest_slot, "looking for start of history");
            let txs = retry!(
                api.get_signatures_for_address(&target, None, earliest_signature),
                "signature for address"
            );
            if let Some(earliest) = txs.last() {
                earliest_slot = earliest.slot;
                earliest_signature = Some(earliest.signature.parse()?);
                metrics().traverse.earliest_slot.set(earliest_slot as i64);
            } else {
                break;
            }
        }
        Ok(earliest_slot)
    }

    pub fn start_from_beginning(
        &'_ self,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> + '_ {
        let api = self.api.clone();
        let target = self.target_key;

        stream_generator::generate_try_stream(move |mut stream| async move {
            tracing::info!("starting from beginning");
            let start_slot = Self::find_beginning_slot(api.clone(), target).await?;

            let inner = self.in_range(start_slot..);
            tokio::pin!(inner);
            while let Some(item) = inner.next().await {
                stream.send(item).await;
            }
            Ok(())
        })
    }

    pub async fn since_signature(
        &self,
        signature: Signature,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> {
        tracing::info!(%signature, "starting from signature");
        let tx = retry!(self.api.get_transaction(&signature), "getting signature");
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
            for slot in range.step_by(1) {
                tracing::info!(%slot, "getting block");
                let mut current_slot = retry!(api.get_slot(commitment), "getting slot");

                while slot > current_slot {
                    current_slot = retry!(api.get_slot(commitment), "waiting for slot");
                }

                let block = loop {
                    match api.get_block(slot, true).await {
                        Ok(block) => {
                            break Either::Right(block);
                        }
                        Err(err) if matches!(err.kind, ClientErrorKind::RpcError(
                                RpcError::RpcResponseError { code: custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED, .. })
                        ) => {
                            tracing::info!(%slot, "skipped slot");
                            break Either::Left(LedgerItem::MissingBlock { slot });
                        }
                        Err(err) => {
                            tracing::warn!(?err, "error fetching block");
                            sleep(RECHECK_INTERVAL).await;
                            continue;
                        }
                    }
                };
                let block = match block {
                    Either::Left(missing) => {
                        stream.send(Ok(missing)).await;
                        continue;
                    }
                    Either::Right(block) => block,
                };
                metrics().traverse.last_observed_slot.set(slot as i64);
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
