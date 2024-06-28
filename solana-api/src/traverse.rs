mod finalization_tracker;
#[cfg(test)]
mod tests;

use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Once;
use std::time::Duration;

use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use solana_client::client_error::ClientError;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use thiserror::Error;
use tokio::time::{sleep, sleep_until, Instant};

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::solana_sdk::slot_history::Slot;
use common::solana_transaction_status::UiConfirmedBlock;
use common::types::{SolanaBlock, SolanaTransaction};

use crate::convert::{decode_ui_transaction, TxDecodeError};
use crate::metrics::metrics;
use crate::solana_api::{SolanaApi, SIGNATURES_LIMIT};
use crate::utils::ward;

use finalization_tracker::{BlockStatus, FinalizationTracker};

// const SIGNATURES_LIMIT: usize = 1000;
const RECHECK_INTERVAL: Duration = Duration::from_secs(1);

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
struct Candidate {
    signature: String,
    slot: Slot,
}

#[derive(Debug)]
pub(crate) struct CachedBlock {
    pub slot: Slot,
    pub block: UiConfirmedBlock,
    pub txs: HashSet<String>,
    /// Index of last processed transaction
    pub last_idx: u64,
}

impl CachedBlock {
    fn new(slot: Slot, block: UiConfirmedBlock, txs: HashSet<String>) -> Self {
        Self {
            slot,
            block,
            txs,
            last_idx: 0,
        }
    }

    fn next_transaction(&mut self) -> Option<Result<(u64, String), TxDecodeError>> {
        let Some(signatures) = self.block.signatures.as_ref() else {
            return Some(Err(TxDecodeError::MissingSignatures));
        };
        let iter = signatures.iter().enumerate().skip(self.last_idx as usize);
        for (idx, sign) in iter {
            if self.txs.remove(sign.as_str()) {
                self.last_idx = idx as u64;
                return Some(Ok((idx as u64, sign.clone())));
            }
        }
        None
    }

    fn into_info(self) -> Result<SolanaBlock, TxDecodeError> {
        debug_assert!(self.txs.is_empty());
        if !self.txs.is_empty() {
            tracing::error!(block = ?self, "removing cached block with remaining txs");
        }

        Ok(SolanaBlock {
            slot: self.slot,
            hash: self.block.blockhash.parse()?,
            parent_slot: self.block.parent_slot,
            parent_hash: self.block.previous_blockhash.parse()?,
            time: self.block.block_time,
            is_finalized: false,
        })
    }
}

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

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum LedgerItem {
    Transaction(SolanaTransaction),
    Block(SolanaBlock),
    FinalizedBlock(u64),
    PurgedBlock(u64),
    ReliableLastEmptySlot(u64),
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

impl TraverseConfig {
    const DEFAULT_SIGNATURE_BUFFER_SIZE_CAP_MB: usize = 512;

    pub const fn signature_limit_size_mb_to_len(size: usize) -> usize {
        const MAX_BASE58_SIGNATURE_LEN: usize = 88;
        1024 * 1024 * size / MAX_BASE58_SIGNATURE_LEN
    }
}

impl Default for TraverseConfig {
    fn default() -> Self {
        TraverseConfig {
            endpoint: String::default(),
            finalized: true,
            rps_limit_sleep: None,
            status_poll_interval: finalization_tracker::POLL_INTERVAL,
            only_success: true,
            target_key: Pubkey::default(),
            last_observed: None,
            signature_buffer_limit: Some(Self::signature_limit_size_mb_to_len(
                Self::DEFAULT_SIGNATURE_BUFFER_SIZE_CAP_MB,
            )),
        }
    }
}

pub struct TraverseLedger {
    traverse: BoxStream<'static, Result<InnerLedgerItem, TraverseError>>,
    api: SolanaApi,
    tracker: Option<FinalizationTracker>,
    status_poll_interval: Duration,
    purged_block: Option<u64>,
    is_finalized: bool,
}

impl TraverseLedger {
    pub fn new(config: TraverseConfig) -> Self {
        let mut config = config;
        let api = SolanaApi::new(std::mem::take(&mut config.endpoint), config.finalized);
        Self::new_with_api(api, config)
    }

    pub(crate) fn new_with_api(api: SolanaApi, config: TraverseConfig) -> Self {
        Self {
            is_finalized: config.finalized,
            status_poll_interval: config.status_poll_interval,
            traverse: Box::pin(
                InnerTraverseLedger::new_with_api(api.clone(), config).into_stream(),
            ),
            api,
            tracker: None,
            purged_block: None,
        }
    }

    pub async fn next(&mut self) -> Option<Result<LedgerItem, TraverseError>> {
        if let Some(purged) = self.purged_block.take() {
            return Some(Ok(LedgerItem::PurgedBlock(purged)));
        }

        if self.tracker.is_none() {
            let tracker = match FinalizationTracker::init(
                self.api.clone(),
                self.status_poll_interval,
            )
            .await
            {
                Ok(tracker) => tracker,
                Err(err) => return Some(Err(err.into())),
            };
            self.tracker = Some(tracker);
        }

        let tracker = self.tracker.as_mut().expect("uninit finalization tracker");

        tokio::select! {
            result = tracker.next(), if !self.is_finalized => Some(match result {
                Err(err) => Err(err.into()),
                Ok((slot, true)) => Ok(LedgerItem::FinalizedBlock(slot)),
                Ok((slot, false)) => Ok(LedgerItem::PurgedBlock(slot)),
            }),
            Some(result) = self.traverse.next() => Some(match result {
                res @ Ok(InnerLedgerItem::Transaction(..)) | res @ Err(..) => res.map(Into::into),
                res @ Ok(InnerLedgerItem::Block(..)) if self.is_finalized => res.map(Into::into),
                Ok(InnerLedgerItem::Block(mut block)) => {
                    match tracker.check_or_schedule_new_slot(block.slot) {
                        BlockStatus::Finalized => block.is_finalized = true,
                        BlockStatus::Pending => block.is_finalized = false,
                        BlockStatus::Purged => {
                            block.is_finalized = false;
                            // We need this as long as we stream block AFTER its transactions
                            self.purged_block = Some(block.slot);
                        }
                    }
                    Ok(LedgerItem::Block(block))
                }
                Ok(InnerLedgerItem::ReliableLastEmptySlot(slot)) => {
                    Ok(LedgerItem::ReliableLastEmptySlot(slot))
                }
            })
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum InnerLedgerItem {
    Transaction(SolanaTransaction),
    Block(SolanaBlock),
    ReliableLastEmptySlot(u64),
}

impl From<InnerLedgerItem> for LedgerItem {
    fn from(value: InnerLedgerItem) -> Self {
        match value {
            InnerLedgerItem::Transaction(tx) => Self::Transaction(tx),
            InnerLedgerItem::Block(block) => Self::Block(block),
            InnerLedgerItem::ReliableLastEmptySlot(slot) => Self::ReliableLastEmptySlot(slot),
        }
    }
}

struct InnerTraverseLedger {
    last_observed: Option<Signature>,
    reliable_last_empty_slot: Option<u64>,
    api: SolanaApi,
    // Sigatures buffer. Starts with the oldest/last processed, ends with the most recent.
    // TODO: Limit this
    buffer: VecDeque<Candidate>,
    cached_block: Option<CachedBlock>,
    next_request: Instant,
    config: TraverseConfig,
}

impl InnerTraverseLedger {
    fn new_with_api(api: SolanaApi, config: TraverseConfig) -> Self {
        assert!(config
            .signature_buffer_limit
            .map_or(true, |limit| limit > 1));
        Self {
            api,
            last_observed: config.last_observed,
            reliable_last_empty_slot: None,
            buffer: VecDeque::new(),
            cached_block: None,
            next_request: Instant::now(),
            config,
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<InnerLedgerItem, TraverseError>> {
        stream::unfold(self, |mut this| async move {
            let value = this.try_next().await;
            Some((value, this))
        })
    }

    async fn try_next(&mut self) -> Result<InnerLedgerItem, TraverseError> {
        // we need to check this before we call self.get_block(), because it always returns a block
        // and fills the buffer. we should also make sure that we aren't in the middle of processing
        // a block, meaning last return value was a block, which means that we don't have a cached
        // block anymore.
        if self.buffer.is_empty() && self.cached_block.is_none() {
            if let Some(slot) = self.reliable_last_empty_slot.take() {
                return Ok(InnerLedgerItem::ReliableLastEmptySlot(slot));
            }
        }
        let (idx, signature) = {
            let block = self.get_block().await?;
            if let Some(result) = block.next_transaction() {
                let (idx, sign) = result?;
                let sign: Signature = sign.parse()?;
                (idx, sign)
            } else {
                let block = self
                    .cached_block
                    .take()
                    .expect("`get_block` always returns cached block")
                    .into_info()?;
                return Ok(InnerLedgerItem::Block(block));
            }
        };

        let tx = retry!(
            self.api.get_transaction(&signature),
            "could not request transaction {signature}"
        );

        let mut tx = decode_ui_transaction(tx)
            .inspect_err(|err| tracing::error!(%err, %signature, "could not decode transaction"))?;
        tx.tx_idx = idx;

        Ok(InnerLedgerItem::Transaction(tx))
    }

    async fn get_block(&mut self) -> Result<&mut CachedBlock, ClientError> {
        if self.cached_block.is_none() {
            let mut txs = HashSet::new();
            let fst = self.pop_candidate().await;
            let slot = fst.slot;
            txs.insert(fst.signature);
            while !self.buffer.is_empty() {
                // TODO: NowOrNever so we don't wait for new blocks?
                let candidate = self.pop_candidate().await;
                if candidate.slot != slot {
                    self.buffer.push_front(candidate);
                    metrics().traverse.buffer_len.inc();
                    break;
                } else {
                    txs.insert(candidate.signature);
                }
            }
            let block = retry!(self.api.get_block(slot), "failed requesting block {slot}");
            tracing::debug!(
                slot,
                hash = block.blockhash,
                num_txs = txs.len(),
                "cached new block"
            );
            let num_txs = txs.len();
            self.cached_block = Some(CachedBlock::new(slot, block, txs));
            metrics().traverse.cached_block.set(slot as i64);
            metrics()
                .traverse
                .transactions_per_block
                .observe(num_txs as f64);
        }

        Ok(self.cached_block.as_mut().expect("just replaced the block"))
    }

    // TODO: Uncomment this when 1.18 hits mainnet
    // pub async fn next(&mut self) -> Option<Result<SolanaTransaction, TraverseError>> {
    //     let signature = self.peek_signature().await?;

    //     let tx = match self.api.get_transaction(&signature).await {
    //         Err(err) => {
    //             tracing::error!(%signature, "could not request transaction");
    //             return Some(Err(err.into()));
    //         }
    //         Ok(tx) => tx,
    //     };

    //     let block = match self.get_block(tx.slot).await {
    //         Err(err) => {
    //             tracing::error!(slot = tx.slot, %signature, "could not request block");
    //             return Some(Err(err.into()));
    //         }
    //         Ok(block) => block,
    //     };

    //     match decode_ui_transaction(tx, block) {
    //         Ok(tx) => {
    //             self.buffer.pop_front(); // cleanup
    //             Some(Ok(tx))
    //         }
    //         Err(err) => {
    //             tracing::error!(%signature, ?block, "could not decode transaction");
    //             Some(Err(err.into()))
    //         }
    //     }
    // }
    //
    // async fn get_block(&mut self, slot: Slot) -> Result<&mut CachedBlock, ClientError> {
    //     if self
    //         .cached_block
    //         .as_ref()
    //         .map_or(true, |cached| cached.slot != slot)
    //     {
    //         let block = self.api.get_block(slot).await?;
    //         self.cached_block = Some(CachedBlock::new(block, slot));
    //     }

    //     Ok(self.cached_block.as_mut().expect("just replaced the block"))
    // }

    async fn pop_candidate(&mut self) -> Candidate {
        loop {
            if let Some(candidate) = self.buffer.pop_front() {
                metrics().traverse.buffer_len.dec();
                break candidate;
            }

            sleep_until(self.next_request).await;
            self.recheck_new_signatures().await;
            self.next_request = Instant::now() + RECHECK_INTERVAL;
        }
    }

    /// Populate signature buffer with new signatures.
    async fn recheck_new_signatures(&mut self) {
        if self
            .config
            .signature_buffer_limit
            .map_or(false, |limit| self.buffer.len() >= limit)
        {
            return;
        }
        // getSignaturesForAddress returns `limit` last transaction signatures, where `limit` <= 1000.
        // Response contains a list of signatures sorted from the most recent to the oldest in batch.
        //
        // When this function gets called we need to check every transaction after the last one
        // we've seen, so we use `self.last_observed` as the lower limit (`until` parameter) for
        // requests.
        //
        // Maximum response size is limited to 1000 transaction (SIGNATURES_LIMIT). Since we can't
        // predict the amount of transactions created since the last call, we have to traverse
        // the ledger backwards in batches. We store the earliest transaction signature from
        // the last batch (`earliest`), so we can set the upper limit for the next batch.

        self.reliable_last_empty_slot = None;
        let mut latest_slot = None;
        // Last observed during this invocation.
        let mut last_observed = None;
        // Hash of the earliest transaction in a batch.
        let mut earliest = None;
        metrics().traverse.uncommited_buffer_len.set(0);
        let mut new_signatures = VecDeque::<Candidate>::new();

        let final_sign = self.last_observed.as_ref().map(Signature::to_string);
        let mut prev_len = None;
        let warn_buffer_exceeded = Once::new();
        let mut empty_retries = 0;

        'outer: loop {
            tracing::debug!(
                ?earliest, ?self.last_observed, target = %self.config.target_key,
                "requesting signatures for address"
            );
            // Sorted from the most recent to the oldest
            let txs = loop {
                if earliest.is_none() {
                    match self.api.get_slot_with_commitment().await {
                        Ok(slot) => {
                            latest_slot = Some(slot);
                        }
                        Err(err) => {
                            tracing::error!(%err, "could not get latest slot");
                        }
                    }
                }
                let res = self
                    .api
                    .get_signatures_for_address(
                        &self.config.target_key,
                        self.last_observed,
                        earliest,
                    )
                    .await;

                match (res, self.config.rps_limit_sleep) {
                    (Ok(txs), _) => break txs,
                    (Err(err), duration) => {
                        let duration = duration.unwrap_or(RECHECK_INTERVAL);
                        tracing::error!(%err, "could not request signatures, retry in {duration:?}");
                        sleep(duration).await;
                        continue;
                    }
                }
            };

            if txs.is_empty() {
                if empty_retries < 5 {
                    tracing::debug!("got empty response, try {empty_retries} out of 5");
                    empty_retries += 1;
                    sleep(RECHECK_INTERVAL).await;
                    continue;
                }
                tracing::debug!("stopping traverse, empty response");
                break 'outer;
            }
            empty_retries = 0;

            if let Some(prev) = prev_len.replace(txs.len()) {
                if prev < SIGNATURES_LIMIT {
                    tracing::warn!(
                        previous = prev,
                        current = txs.len(),
                        "previous gSFA response was shorter than 1000, but current is not empty"
                    );
                }
            }

            if last_observed.is_none() {
                let first = txs.first().expect("checked non empty");
                let sign = Signature::from_str(&first.signature);
                // This is the last transaction despite being the first in list.
                let sign = ward!([error] sign, "could not parse last observed signature");
                last_observed.replace(sign);
                metrics().traverse.last_observed_slot.set(first.slot as i64);
            }

            let last = txs.last().expect("checked non empty");
            let sign = Signature::from_str(&last.signature);
            earliest = Some(ward!([error] sign, "could not parse earliest signature in a batch"));
            metrics().traverse.earliest_slot.set(last.slot as i64);

            for item in txs {
                // let sign = ward!(
                //     [error] Signature::from_str(&item.signature),
                //     "could not parse signature"
                // );
                let RpcConfirmedTransactionStatusWithSignature {
                    signature,
                    slot,
                    err,
                    ..
                } = item;

                if final_sign.as_ref().map_or(false, |last| &signature == last) {
                    tracing::debug!("stopping traverse, found final signature");
                    break 'outer;
                }

                if self.config.only_success && err.is_some() {
                    tracing::debug!(?err, %signature, slot, "skipped failed transaction");
                    metrics().traverse.ignored_signatures.inc();
                } else {
                    if self.config.signature_buffer_limit.map_or(false, |limit| {
                        self.buffer.len() + new_signatures.len() + 1 >= limit
                    }) {
                        warn_buffer_exceeded.call_once(|| {
                            tracing::warn!(
                                limit = ?self.config.signature_buffer_limit,
                                "signature buffer was exceeded, newer signatures will be purged"
                            )
                        });
                        new_signatures.pop_back();
                        let sign = &new_signatures.back().expect("must not be empty").signature;
                        let sign =
                            ward!([error] sign.parse(), "could not parse last observed signature");
                        last_observed = Some(sign);
                    } else {
                        metrics().traverse.uncommited_buffer_len.inc();
                    }

                    let sign = Candidate { signature, slot };
                    new_signatures.push_front(sign);
                }
            }
        }

        if last_observed.is_some() {
            self.last_observed = last_observed
        };
        tracing::debug!(
            len = new_signatures.len(),
            first = ?new_signatures.front(), last = ?new_signatures.back(),
            "found new signatures"
        );

        if let Some(latest_slot) = latest_slot {
            if new_signatures
                .back()
                .map_or(true, |first_signature| first_signature.slot < latest_slot)
            {
                self.reliable_last_empty_slot.replace(latest_slot);
            }
        }

        self.buffer.extend(new_signatures);
        metrics().traverse.buffer_len.set(self.buffer.len() as i64);
        metrics().traverse.uncommited_buffer_len.set(0);
    }
}
