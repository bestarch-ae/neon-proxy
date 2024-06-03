mod finalization_tracker;
#[cfg(test)]
mod tests;

use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;

use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use thiserror::Error;
use tokio::time::{sleep, sleep_until, Instant};

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::solana_sdk::slot_history::Slot;
use common::solana_transaction_status::UiConfirmedBlock;
use common::types::{SolanaBlock, SolanaTransaction};

use crate::convert::{decode_ui_transaction, TxDecodeError};
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
}

#[derive(Debug, Clone, Default)]
pub struct TraverseConfig {
    pub endpoint: String,
    /// Request only finalized transactions
    pub finalized: bool,
    pub rps_limit_sleep: Option<Duration>,
    pub status_poll_interval: Option<Duration>,
    /// Request only successful transactions
    pub only_success: bool,
    pub target_key: Pubkey,
    pub last_observed: Option<Signature>,
}

pub struct TraverseLedger {
    traverse: BoxStream<'static, Result<InnerLedgerItem, TraverseError>>,
    api: SolanaApi,
    tracker: Option<FinalizationTracker>,
    status_poll_interval: Option<Duration>,
    purged_block: Option<u64>,
}

impl TraverseLedger {
    pub fn new(config: TraverseConfig) -> Self {
        let mut config = config;
        let api = SolanaApi::new(std::mem::take(&mut config.endpoint), config.finalized);
        Self::new_with_api(api, config)
    }

    pub(crate) fn new_with_api(api: SolanaApi, config: TraverseConfig) -> Self {
        Self {
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
            result = tracker.next() => Some(match result {
                Err(err) => Err(err.into()),
                Ok((slot, true)) => Ok(LedgerItem::FinalizedBlock(slot)),
                Ok((slot, false)) => Ok(LedgerItem::PurgedBlock(slot)),
            }),
            Some(result) = self.traverse.next() => match result {
                res @ Ok(InnerLedgerItem::Transaction(..))
                    | res @ Err(..) => Some(res.map(Into::into)),
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
                    Some(Ok(LedgerItem::Block(block)))
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum InnerLedgerItem {
    Transaction(SolanaTransaction),
    Block(SolanaBlock),
}

impl From<InnerLedgerItem> for LedgerItem {
    fn from(value: InnerLedgerItem) -> Self {
        match value {
            InnerLedgerItem::Transaction(tx) => Self::Transaction(tx),
            InnerLedgerItem::Block(block) => Self::Block(block),
        }
    }
}

struct InnerTraverseLedger {
    last_observed: Option<Signature>,
    api: SolanaApi,
    target_key: Pubkey,
    // Sigatures buffer. Starts with the oldest/last processed, ends with the most recent.
    // TODO: Limit this
    buffer: VecDeque<Candidate>,
    cached_block: Option<CachedBlock>,
    next_request: Instant,
    only_success: bool,
    rps_limit_sleep: Option<Duration>,
}

impl InnerTraverseLedger {
    fn new_with_api(api: SolanaApi, config: TraverseConfig) -> Self {
        Self {
            api,
            target_key: config.target_key,
            last_observed: config.last_observed,
            buffer: VecDeque::new(),
            cached_block: None,
            next_request: Instant::now(),
            only_success: config.only_success,
            rps_limit_sleep: config.rps_limit_sleep,
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<InnerLedgerItem, TraverseError>> {
        stream::unfold(self, |mut this| async move {
            let value = this.try_next().await;
            Some((value, this))
        })
    }

    async fn try_next(&mut self) -> Result<InnerLedgerItem, TraverseError> {
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
            self.cached_block = Some(CachedBlock::new(slot, block, txs));
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
                break candidate;
            }

            sleep_until(self.next_request).await;
            self.recheck_new_signatures().await;
            self.next_request = Instant::now() + RECHECK_INTERVAL;
        }
    }

    /// Populate signature buffer with new signatures.
    async fn recheck_new_signatures(&mut self) {
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

        // Last observed during this invocation.
        let mut last_observed = None;
        // Hash of the earliest transaction in a batch.
        let mut earliest = None;
        let mut new_signatures = VecDeque::new();

        loop {
            tracing::debug!(
                ?earliest, ?self.last_observed, target = %self.target_key,
                "requesting signatures for address"
            );
            let txs = loop {
                let res = self
                    .api
                    .get_signatures_for_address(&self.target_key, self.last_observed, earliest)
                    .await;

                if let Some(sleep_duration) = self.rps_limit_sleep {
                    if matches!(
                        res,
                        Err(ClientError {
                            kind: ClientErrorKind::Reqwest(ref err),
                            ..
                        }) if err.status().map_or(false, |code| code == 429),
                    ) {
                        sleep(sleep_duration).await;
                        continue;
                    }
                }
                break res;
            };
            // Sorted from the most recent to the oldest
            let txs = ward!([error] txs, "could not request signatures");

            if txs.is_empty() {
                break;
            }

            if last_observed.is_none() {
                let sign = Signature::from_str(&txs.first().expect("checked non empty").signature);
                // This is the last transaction despite being the first in list.
                let sign = ward!([error] sign, "could not parse last observed signature");
                last_observed.replace(sign);
            }

            let sign = Signature::from_str(&txs.last().expect("checked non empty").signature);
            earliest = Some(ward!([error] sign, "could not parse earliest signature in a batch"));

            let len = txs.len();

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
                if self.only_success && err.is_some() {
                    tracing::debug!(?err, %signature, slot, "skipped failed transaction");
                } else {
                    let sign = Candidate { signature, slot };
                    new_signatures.push_front(sign);
                }
            }

            // Means we reached last_observed
            if len != SIGNATURES_LIMIT {
                break;
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

        self.buffer.extend(new_signatures);
    }
}
