use std::ops::{Bound, RangeBounds};
use std::time::Duration;

use futures_util::{stream, Stream, StreamExt, TryStreamExt};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError;
use solana_rpc_client_api::custom_error;
use thiserror::Error;
use tokio::time::sleep;

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::solana_transaction_status::UiConfirmedBlock;
use common::{
    solana_sdk::commitment_config::CommitmentLevel,
    types::{SolanaBlock, SolanaTransaction},
};

use crate::convert::{decode_ui_transaction, TxDecodeError};
use crate::metrics::metrics;
use crate::solana_api::SolanaApi;

use crate::finalization_tracker::FinalizationTracker;

const ERROR_RECHECK_INTERVAL: Duration = Duration::from_secs(1);
const REGULAR_RECHECK_INTERVAL: Duration = Duration::from_millis(400);
const EMPTY_BLOCK_RETRIES: u32 = 10;

macro_rules! retry {
    ($val:expr, $message:literal) => {
        loop {
            match $val.await {
                Ok(val) => break val,
                Err(err) => {
                    tracing::warn!(%err, retry_in = ?ERROR_RECHECK_INTERVAL, $message);
                    sleep(ERROR_RECHECK_INTERVAL).await
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
    pub max_concurrent_tasks: usize,
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
            max_concurrent_tasks: 10,
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
    pub fn slot(&self) -> u64 {
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
        let mut earliest_slot = api.get_slot(CommitmentLevel::Finalized).await?;
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
        let mut empty_retries = 0;
        loop {
            match api.get_block(slot, full).await {
                Ok(block) => {
                    break Some(block);
                }
                Err(err)
                    if matches!(
                        err.kind,
                        ClientErrorKind::RpcError(RpcError::RpcResponseError {
                            code: custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
                                | custom_error::JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                            ..
                        })
                    ) =>
                {
                    // This is a hack to work around RPC missing blocks.
                    // TODO: Remove it after the problem is fixed
                    if empty_retries < EMPTY_BLOCK_RETRIES {
                        tracing::warn!(%slot, "retrying missing block");
                        empty_retries += 1;
                        sleep(ERROR_RECHECK_INTERVAL).await;
                        continue;
                    }
                    tracing::info!(%slot, "skipped slot");
                    break None;
                }
                Err(err) => {
                    tracing::warn!(%slot, ?err, "error fetching block");
                    sleep(ERROR_RECHECK_INTERVAL).await;
                    continue;
                }
            }
        }
    }

    pub fn in_range<R: RangeBounds<u64> + Iterator<Item = u64> + Send + 'static>(
        &self,
        range: R,
    ) -> impl Stream<Item = Result<LedgerItem, TraverseError>> {
        tracing::info!(start = ?range.start_bound(), end = ?range.end_bound(), "traversing range");
        let api = self.api.clone();
        let commitment = self.commitment;
        let is_finalized = matches!(self.commitment, CommitmentLevel::Finalized);
        let poll_interval = self.config.status_poll_interval;
        let target = self.config.target_key;
        let only_success = self.config.only_success;
        let start_slot = match range.start_bound() {
            Bound::Included(slot) => *slot,
            Bound::Excluded(slot) => slot + 1,
            Bound::Unbounded => 0,
        };
        let end_slot = match range.end_bound() {
            Bound::Included(slot) => *slot,
            Bound::Excluded(slot) => slot - 1,
            Bound::Unbounded => u64::MAX,
        };
        let slots = stream::unfold((0, start_slot), move |(mut current_slot, slot)| {
            let api = api.clone();
            async move {
                if slot < end_slot {
                    while current_slot < slot {
                        tracing::debug!(%slot, %current_slot, "waiting for slot (calling get_slot)");
                        let current_slot_timer =
                            metrics().traverse.get_current_slot_time.start_timer();
                        current_slot = retry!(api.get_slot(commitment), "getting slot");
                        drop(current_slot_timer);
                        sleep(REGULAR_RECHECK_INTERVAL).await;
                    }
                    let task = tokio::spawn(Self::process_slot(
                        slot,
                        api.clone(),
                        commitment,
                        target,
                        only_success,
                        is_finalized,
                    ));
                    Some((task, (current_slot, slot + 1)))
                } else {
                    None
                }
            }
        })
        .buffered(self.config.max_concurrent_tasks).map_err(anyhow::Error::from);

        let (tx, mut rx) = tokio::sync::mpsc::channel(32);

        let api = self.api.clone();
        let tracker_stream = stream_generator::generate_try_stream(move |mut stream| async move {
            use crate::finalization_tracker::BlockStatus;
            let mut tracker = FinalizationTracker::init(api, poll_interval).await?;

            while let Some(slot) = rx.recv().await {
                tracing::debug!(%slot, "checking slot");
                match tracker.check_or_schedule_new_slot(slot) {
                    status @ (BlockStatus::Finalized | BlockStatus::Purged) => {
                        let item = Ok(LedgerItem::BlockUpdate {
                            slot,
                            commitment: (status == BlockStatus::Finalized)
                                .then_some(CommitmentLevel::Finalized),
                        });
                        tracing::debug!(%slot, ?status, "block updated (check)");
                        stream.send(Ok(item)).await;
                    }
                    BlockStatus::Pending => loop {
                        match tracker.try_next().await {
                            Ok(Some((slot, is_finalized))) => {
                                let item = Ok(LedgerItem::BlockUpdate {
                                    slot,
                                    commitment: is_finalized.then_some(CommitmentLevel::Finalized),
                                });
                                tracing::debug!(%slot, %is_finalized, "block updated (next)");
                                stream.send(Ok(item)).await;
                            }
                            Ok(None) => {
                                break;
                            }
                            Err(err) => {
                                tracing::warn!(%err, "tracker error");
                                break;
                            }
                        }
                    },
                }
            }
            Ok(())
        });

        let slots = slots.then(move |block| {
            let tx = tx.clone();
            async move {
                if let Ok(Ok(
                    blk @ LedgerItem::Block {
                        commitment: CommitmentLevel::Processed | CommitmentLevel::Confirmed,
                        ..
                    },
                )) = &block
                {
                    let slot = blk.slot();
                    let _ = tx.send(slot).await;
                }
                block
            }
        });

        let slots = futures_util::stream::select(tracker_stream, slots);
        stream_generator::generate_try_stream(move |mut stream| async move {
            tokio::pin!(slots);

            while let Some(item) = slots.next().await {
                let item = item.expect("task panicked, stream ended");
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
                        stream.send(Ok(block)).await;
                    }
                    Ok(update) => stream.send(Ok(update)).await,
                    Err(err) => {
                        stream.send(Err(err)).await;
                    }
                }
            }
            Ok(())
        })
    }

    async fn process_slot(
        slot: u64,
        api: SolanaApi,
        commitment: CommitmentLevel,
        target: Pubkey,
        only_success: bool,
        is_finalized: bool,
    ) -> Result<LedgerItem, TraverseError> {
        tracing::debug!(%slot, "getting block");
        let get_block_timer = metrics().traverse.get_block_time.start_timer();
        let block = Self::get_block(&api, slot, true).await;
        drop(get_block_timer);

        let block = match block {
            None => {
                return Ok(LedgerItem::MissingBlock { slot });
            }
            Some(block) => block,
        };
        metrics().traverse.last_observed_slot.set(slot as i64);
        let process_transactions_timer = metrics().traverse.process_transactions_time.start_timer();
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
            let decode_transaction_timer =
                metrics().traverse.decode_ui_transaction_time.start_timer();
            let tx = decode_ui_transaction(tx, slot)?;
            drop(decode_transaction_timer);
            if tx.has_key(target) {
                txs.push(tx);
            }
        }
        metrics()
            .traverse
            .transactions_per_block
            .observe(txs.len() as f64);

        tracing::debug!(%slot, count = txs.len(), "fetched transactions");
        drop(process_transactions_timer);
        Ok(LedgerItem::Block {
            block,
            txs,
            commitment,
        })
    }
}
