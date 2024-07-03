use std::ops::RangeBounds;
use std::time::Duration;

use crate::convert::{decode_ui_transaction, TxDecodeError};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::solana_transaction_status::UiConfirmedBlock;
use common::{
    solana_sdk::commitment_config::CommitmentLevel,
    types::{SolanaBlock, SolanaTransaction},
};
use futures_util::{Stream, StreamExt};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError;
use solana_rpc_client_api::custom_error;
use thiserror::Error;
use tokio::time::sleep;

use crate::metrics::metrics;
use crate::solana_api::SolanaApi;

use crate::finalization_tracker::{BlockStatus, FinalizationTracker};

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
        commitment: Option<CommitmentLevel>,
    },
}

impl LedgerItem {
    fn slot(&self) -> u64 {
        match self {
            Self::Block { block, .. } => block.slot,
            Self::MissingBlock { slot } => *slot,
            Self::BlockUpdate { slot, .. } => *slot,
        }
    }
}

pub struct TraverseLedger {
    api: SolanaApi,
    commitment: CommitmentLevel,
    config: TraverseConfig,
}

impl TraverseLedger {
    pub fn new(config: TraverseConfig) -> Self {
        let commitment = if config.finalized {
            CommitmentLevel::Finalized
        } else {
            CommitmentLevel::Confirmed
        };
        TraverseLedger {
            api: SolanaApi::new(config.endpoint.clone(), config.finalized),
            config,
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
        let target = self.config.target_key;

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

    async fn get_block(api: &SolanaApi, slot: u64, full: bool) -> Option<UiConfirmedBlock> {
        loop {
            match api.get_block(slot, full).await {
                Ok(block) => {
                    break Some(block);
                }
                Err(err)
                    if matches!(
                        err.kind,
                        ClientErrorKind::RpcError(RpcError::RpcResponseError {
                            code:
                                custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                            ..
                        })
                    ) =>
                {
                    tracing::info!(%slot, "skipped slot");
                    break None;
                }
                Err(err) => {
                    tracing::warn!(?err, "error fetching block");
                    sleep(RECHECK_INTERVAL).await;
                    continue;
                }
            }
        }
    }

    pub fn in_range<R: RangeBounds<u64> + Iterator<Item = u64> + 'static>(
        &self,
        range: R,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> {
        tracing::info!(start = ?range.start_bound(), end = ?range.end_bound(), "traversing range");
        let api = self.api.clone();
        let api2 = self.api.clone();
        let commitment = self.commitment;
        let is_finalized = matches!(self.commitment, CommitmentLevel::Finalized);
        let poll_interval = self.config.status_poll_interval;
        let target = self.config.target_key;
        let only_success = self.config.only_success;

        let inner_stream = stream_generator::generate_try_stream(
            move |mut stream: stream_generator::Yielder<Result<LedgerItem, TraverseError>>| async move {
                for slot in range.step_by(1) {
                    tracing::info!(%slot, "getting block");
                    let mut current_slot = retry!(api.get_slot(commitment), "getting slot");

                    while slot > current_slot {
                        current_slot = retry!(api.get_slot(commitment), "waiting for slot");
                    }

                    let block = Self::get_block(&api, slot, true).await;
                    let block = match block {
                        None => {
                            stream.send(Ok(LedgerItem::MissingBlock { slot })).await;
                            continue;
                        }
                        Some(block) => block,
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
                    let Some(ui_txs) = ui_txs else {
                        return Err(TxDecodeError::MissingTransactions.into());
                    };
                    let mut txs = Vec::new();
                    for tx in ui_txs.into_iter() {
                        if only_success
                            && tx
                                .meta
                                .as_ref()
                                .map(|meta| meta.status.is_err())
                                .unwrap_or(false)
                        {
                            continue;
                        }
                        let tx = decode_ui_transaction(tx, slot)?;
                        if tx.has_key(target) {
                            txs.push(tx);
                        }
                    }
                    metrics()
                        .traverse
                        .transactions_per_block
                        .observe(txs.len() as f64);

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
            },
        );
        stream_generator::generate_try_stream(move |mut stream| async move {
            tokio::pin!(inner_stream);
            let mut tracker = FinalizationTracker::init(api2, poll_interval).await?;

            tokio::select! {
                Ok(block_status) = tracker.next() => {
                    let (slot, is_finalized) = block_status;
                    stream.send(Ok(LedgerItem::BlockUpdate {
                        slot,
                        commitment: is_finalized.then_some(CommitmentLevel::Finalized)
                    })).await;
                }
                Some(item) = inner_stream.next() => {
                    match item {
                        Ok(missing @ LedgerItem::MissingBlock { .. }) => {
                            stream.send(Ok(missing)).await;
                        }
                        Ok(
                            block @ LedgerItem::Block {
                                commitment: CommitmentLevel::Finalized,
                                ..
                            },
                        ) => {
                            stream.send(Ok(block)).await;
                        }
                        Ok(block @ LedgerItem::Block { .. }) => {
                            let slot = block.slot();
                            let status = tracker.check_or_schedule_new_slot(slot);
                            if matches!(status, BlockStatus::Finalized | BlockStatus::Purged) {
                                stream.send(Ok(LedgerItem::BlockUpdate {
                                    slot,
                                    commitment: (status == BlockStatus::Finalized).then_some(CommitmentLevel::Finalized)
                                })).await;
                            }
                            stream.send(Ok(block)).await;
                        }
                        Ok(update) => stream.send(Ok(update)).await,
                        Err(err) => {
                            stream.send(Err(err)).await;
                        }
                    }
                }
            }

            Ok(())
        })
    }
}
