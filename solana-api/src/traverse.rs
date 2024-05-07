#[cfg(test)]
mod tests;

use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;

use solana_client::client_error::ClientError;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use thiserror::Error;
use tokio::time::{sleep_until, Instant};

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{ParseSignatureError, Signature};
use common::solana_sdk::slot_history::Slot;
use common::solana_transaction_status::UiConfirmedBlock;
use common::types::SolanaTransaction;

use crate::convert::{decode_ui_transaction, TxDecodeError};
use crate::solana_api::{SolanaApi, SIGNATURES_LIMIT};
use crate::utils::ward;

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
    pub block: UiConfirmedBlock,
    pub txs: HashSet<String>,
    /// Index of last processed transaction
    pub last_idx: u64,
}

struct BlockInfo {
    block_time: Option<i64>,
    tx_idx: u64,
    blockhash: String,
    parent_slot: Slot,
}

impl CachedBlock {
    fn new(block: UiConfirmedBlock, txs: HashSet<String>) -> Self {
        Self {
            block,
            txs,
            last_idx: 0,
        }
    }

    fn next_transaction(&mut self) -> Option<Result<String, TxDecodeError>> {
        let Some(signatures) = self.block.signatures.as_ref() else {
            return Some(Err(TxDecodeError::MissingSignatures));
        };
        let iter = signatures.iter().skip(self.last_idx as usize).enumerate();
        for (idx, sign) in iter {
            if self.txs.remove(sign.as_str()) {
                self.last_idx += idx as u64;
                return Some(Ok(sign.clone()));
            }
        }
        None
    }

    fn get_info(&self) -> BlockInfo {
        BlockInfo {
            block_time: self.block.block_time,
            tx_idx: self.last_idx,
            blockhash: self.block.blockhash.clone(),
            parent_slot: self.block.parent_slot,
        }
    }
}

pub struct TraverseLedger {
    last_observed: Option<Signature>,
    api: SolanaApi,
    target_key: Pubkey,
    // Sigatures buffer. Starts with the oldest/last processed, ends with the most recent.
    // TODO: Limit this
    buffer: VecDeque<Candidate>,
    cached_block: Option<CachedBlock>,
    next_request: Instant,
}

impl TraverseLedger {
    pub fn new(api: SolanaApi, target_key: Pubkey, last_observed: Option<Signature>) -> Self {
        Self {
            api,
            target_key,
            last_observed,
            buffer: VecDeque::new(),
            cached_block: None,
            next_request: Instant::now(),
        }
    }

    pub async fn next(&mut self) -> Option<Result<SolanaTransaction, TraverseError>> {
        Some(self.try_next().await)
    }

    async fn try_next(&mut self) -> Result<SolanaTransaction, TraverseError> {
        let (block, signature) = loop {
            let block = self.get_block().await?;
            if let Some(result) = block.next_transaction() {
                let sign: Signature = result?.parse()?;
                break (block.get_info(), sign);
            }
        };

        let tx = self.api.get_transaction(&signature).await.inspect_err(
            |err| tracing::error!(%err, %signature, "could not request transaction"),
        )?;

        let mut tx = decode_ui_transaction(tx)
            .inspect_err(|err| tracing::error!(%err, %signature, "could not decode transaction"))?;

        tx.block_time = block.block_time;
        tx.tx_idx = block.tx_idx;
        tx.blockhash = block.blockhash;
        tx.parent_slot = block.parent_slot;

        Ok(tx)
    }

    async fn get_block(&mut self) -> Result<&mut CachedBlock, ClientError> {
        if self
            .cached_block
            .as_ref()
            .map_or(true, |cached| cached.txs.is_empty())
        {
            let mut txs = HashSet::new();
            let fst = self.pop_candidate().await;
            let slot = fst.slot;
            txs.insert(fst.signature);
            loop {
                // TODO: NowOrNever so we don't wait for new blocks?
                let candidate = self.pop_candidate().await;
                if candidate.slot != slot {
                    self.buffer.push_front(candidate);
                    break;
                } else {
                    txs.insert(candidate.signature);
                }
            }
            let block = self.api.get_block(slot).await?;
            self.cached_block = Some(CachedBlock::new(block, txs));
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
            let txs = self
                .api
                .get_signatures_for_address(&self.target_key, self.last_observed, earliest)
                .await;
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
                    signature, slot, ..
                } = item;
                let sign = Candidate { signature, slot };
                new_signatures.push_front(sign);
            }

            // Means we reached last_observed
            if len != SIGNATURES_LIMIT {
                break;
            }
        }

        if last_observed.is_some() {
            self.last_observed = last_observed
        };

        self.buffer.extend(new_signatures);
    }
}
