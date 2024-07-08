use std::time::Duration;

use anyhow::{Context, Result};
use common::ethnum::U256;
use common::solana_sdk::clock::UnixTimestamp;
use common::solana_sdk::commitment_config::CommitmentLevel;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::types::{HolderOperation, NeonTxInfo, SolanaBlock, SolanaTransaction, TxHash};
use neon_parse::{AccountsDb, Action};
use solana::traverse::v2::LedgerItem;

use crate::accountsdb::DummyAdb;
use crate::metrics::metrics;

const ONE_BLOCK_SEC: f64 = 0.4;

#[derive(Copy, Clone, Debug)]
struct LastBlock {
    slot: u64,
    time: UnixTimestamp,
}

struct NeonBlock {
    block: SolanaBlock,
    txs: Vec<NeonTxInfo>,
    signatures: Vec<(u32, Signature)>,
    canceled: Vec<(TxHash, U256)>,
    holders: Vec<(u32, bool, HolderOperation)>,
}

pub struct Indexer {
    tx_repo: db::TransactionRepo,
    sig_repo: db::SolanaSignaturesRepo,
    holder_repo: db::HolderRepo,
    block_repo: db::BlockRepo,
    tx_log_idx: TxLogIdx,
    neon_tx_idx: u64,
    block_gas_used: U256,
    block_log_idx: u64,
    adb: DummyAdb,
    target: Pubkey,
    last_block: Option<LastBlock>,
}

impl Indexer {
    pub fn new(pool: db::PgPool, target: Pubkey) -> Self {
        let tx_repo = db::TransactionRepo::new(pool.clone());
        let sig_repo = db::SolanaSignaturesRepo::new(pool.clone());
        let holder_repo = db::HolderRepo::new(pool.clone());
        let block_repo = db::BlockRepo::new(pool.clone());
        let adb = DummyAdb::new(target, holder_repo.clone());
        let tx_log_idx = TxLogIdx::new(tx_repo.clone());

        Self {
            tx_repo,
            sig_repo,
            holder_repo,
            block_repo,
            neon_tx_idx: 0,
            tx_log_idx,
            block_gas_used: U256::new(0),
            block_log_idx: 0,
            adb,
            target,
            last_block: None,
        }
    }

    async fn update_last_block(&mut self) {
        if self.last_block.is_some() {
            return;
        }
        if let Some(last_block) = self.block_repo.latest_block_time().await.ok().flatten() {
            self.last_block = Some(LastBlock {
                slot: last_block.0,
                time: last_block.1 as i64,
            });
        }
    }

    pub async fn get_latest_block(&self) -> Result<Option<u64>> {
        Ok(self
            .block_repo
            .latest_block_time()
            .await?
            .map(|(slot, _)| slot))
    }

    async fn save_block(&mut self, block: &NeonBlock) -> anyhow::Result<()> {
        let mut txn = self.tx_repo.begin_transaction().await?;
        let slot = block.block.slot;
        for tx in &block.txs {
            self.tx_repo
                .insert(tx, &mut txn)
                .await
                .context("failed to save neon transaction")?;
            metrics().neon_transactions_saved.inc();
            tracing::info!(
                signature = %tx.neon_signature,
                "saved transaction"
            );
        }
        for (hash, total_gas) in &block.canceled {
            self.tx_repo
                .set_canceled(hash, *total_gas, slot, &mut txn)
                .await
                .context("failed to cancel neon transaction")?;
        }

        let _blk_timer = metrics().block_processing_time.start_timer();
        for (tx_idx, stuck, op) in &block.holders {
            self.holder_repo
                .insert(slot, *tx_idx, *stuck, op, &mut txn)
                .await
                .context("failed to save neon holder")?;
            metrics().holders_saved.inc();
        }

        for (tx_idx, signature) in &block.signatures {
            self.sig_repo
                .insert(slot, *tx_idx, *signature, &mut txn)
                .await
                .context("failed to save solana signature")?;
        }

        self.block_repo
            .insert(&block.block, &mut txn)
            .await
            .context("failed to save solana block")?;
        tracing::info!(slot = slot, "saved solana block");

        txn.commit()
            .await
            .context("failed to commit block transaction")?;

        Ok(())
    }

    async fn process_block(
        &mut self,
        block: SolanaBlock,
        txs: Vec<SolanaTransaction>,
        commitment: CommitmentLevel,
    ) -> anyhow::Result<()> {
        use backoff::{backoff::Backoff, ExponentialBackoff};

        let slot = block.slot;
        let block = self.prepare_block(block, txs, commitment).await?;
        let mut backoff = ExponentialBackoff {
            max_elapsed_time: None,
            ..Default::default()
        };
        while let Some(backoff) = backoff.next_backoff() {
            match self.save_block(&block).await {
                Ok(_) => break,
                Err(err) => {
                    metrics().database_errors.inc();
                    tracing::warn!(slot = %slot, ?err, interval = ?backoff, "failed to save block, retrying");
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        metrics().current_slot.set(slot as i64);

        Ok(())
    }

    async fn prepare_block(
        &mut self,
        block: SolanaBlock,
        txs: Vec<SolanaTransaction>,
        _commitment: CommitmentLevel,
    ) -> anyhow::Result<NeonBlock> {
        /* TODO: remove these entirely */
        self.neon_tx_idx = 0;
        self.block_gas_used = U256::new(0);
        self.block_log_idx = 0;

        let slot = block.slot;

        if let Some(time) = block.time {
            self.last_block = Some(LastBlock { slot, time })
        }

        let mut neon_block = NeonBlock {
            block,
            txs: Vec::new(),
            signatures: Vec::new(),
            canceled: Vec::new(),
            holders: Vec::new(),
        };

        for tx in txs {
            let _tx_timer = metrics().transaction_processing_time.start_timer();
            let signature = tx.tx.signatures[0];
            let tx_idx = tx.tx_idx as u32;
            let slot = tx.slot;

            metrics().transactions_processed.inc();

            let _span = tracing::info_span!("solana transaction", signature = %signature).entered();

            self.adb.set_slot_idx(slot, tx_idx);

            let parse_timer = metrics().neon_parse_time.start_timer();
            let actions = match neon_parse::parse(tx, &mut self.adb, self.target) {
                Ok(actions) => actions,
                Err(err) => {
                    tracing::warn!(?err, "failed to parse solana transaction");
                    metrics().parsing_errors.inc();
                    continue;
                }
            };
            drop(parse_timer);
            tracing::debug!("parsed transactions");
            for action in actions {
                match action {
                    Action::AddTransaction(mut tx) => {
                        tx.tx_idx = self.neon_tx_idx;
                        tx.sol_signature = signature;

                        // only completed transactions increment gas and idx
                        if tx.is_completed {
                            self.neon_tx_idx += 1;
                            self.block_gas_used += tx.gas_used;
                            tx.sum_gas_used = self.block_gas_used;

                            for log in &mut tx.events {
                                if !log.is_hidden {
                                    log.blk_log_idx = self.block_log_idx;
                                    self.block_log_idx += 1;
                                }
                            }
                        }

                        tracing::debug!(?tx, "adding transaction");
                        // all transactions increment tx_log_idx
                        for log in &mut tx.events {
                            if !log.is_hidden {
                                log.tx_log_idx = self.tx_log_idx.next(&tx.neon_signature).await;
                                tracing::debug!(tx = %tx.neon_signature,
                                                    blk_log_idx = %log.blk_log_idx,
                                                    tx_log_idx = %log.tx_log_idx, "log");
                            }
                        }

                        neon_block.txs.push(tx);
                    }
                    Action::CancelTransaction { hash, total_gas } => {
                        neon_block.canceled.push((hash, total_gas));
                    }
                    Action::WriteHolder(op) => {
                        tracing::info!(slot = %slot, pubkey = %op.pubkey(), "saving holder");
                        if let HolderOperation::Delete(pubkey) = &op {
                            self.adb.delete_account(*pubkey);
                        }
                        neon_block.holders.push((tx_idx, false, op));
                    }
                }
            }
        }

        metrics().blocks_processed.inc();
        Ok(neon_block)
    }

    async fn process_block_update(
        &mut self,
        slot: u64,
        commitment: Option<CommitmentLevel>,
    ) -> Result<()> {
        if matches!(commitment, Some(CommitmentLevel::Finalized)) {
            let _blk_timer = metrics().finalized_block_processing_time.start_timer();
            if let Err(err) = self.block_repo.finalize(slot).await {
                tracing::warn!(%err, slot, "failed finalizing block in db");
                metrics().database_errors.inc();
                return Ok(());
            }
            metrics().finalized_blocks_processed.inc();
            tracing::info!(slot, "block was finalized");
        } else {
            let _blk_timer = metrics().purged_block_processing_time.start_timer();
            metrics().purged_blocks_processed.inc();
            if let Err(err) = self.block_repo.purge(slot).await {
                tracing::warn!(%err, slot, "failed purging block in db");
                metrics().database_errors.inc();
                return Ok(());
            }
            tracing::info!(slot, "block was purged");
            self.process_missing_block(slot).await?;
        }
        Ok(())
    }

    fn make_fake_block(&self, slot: u64) -> SolanaBlock {
        let block_time = self.last_block.map(|block| block.time);
        let block_diff = slot - self.last_block.map(|block| block.slot).unwrap_or(slot);
        let additional_time = (block_diff as f64 * ONE_BLOCK_SEC).ceil() as i64;

        SolanaBlock {
            slot,
            hash: fake_hash(slot),
            parent_hash: fake_hash(slot - 1),
            parent_slot: slot - 1,
            time: block_time.map(|time| time + additional_time),
            is_finalized: false,
        }
    }

    async fn process_missing_block(&mut self, slot: u64) -> Result<()> {
        tracing::info!(%slot, "missing/purged block, generating fake");

        self.update_last_block().await;

        let mut txn = self.tx_repo.begin_transaction().await?;

        let block = self.make_fake_block(slot);
        self.block_repo
            .insert(&block, &mut txn)
            .await
            .context("failed to save solana block")?;
        tracing::info!(slot = block.slot, "saved solana block");
        metrics().blocks_processed.inc();
        txn.commit()
            .await
            .context("failed to commit missing block")?;
        Ok(())
    }

    pub async fn process_ledger(&mut self, item: LedgerItem) -> anyhow::Result<()> {
        match item {
            LedgerItem::Block {
                block,
                txs,
                commitment,
            } => self.process_block(block, txs, commitment).await?,
            LedgerItem::BlockUpdate { slot, commitment } => {
                self.process_block_update(slot, commitment).await?
            }
            LedgerItem::MissingBlock { slot } => {
                self.process_missing_block(slot).await?;
            }
        }
        Ok(())
    }
}

struct TxLogIdx {
    repo: db::TransactionRepo,
    cache: lru::LruCache<TxHash, u64>,
}

impl TxLogIdx {
    fn new(repo: db::TransactionRepo) -> Self {
        Self {
            repo,
            cache: lru::LruCache::new(512.try_into().expect("512 > 0")),
        }
    }

    async fn next(&mut self, hash: &TxHash) -> u64 {
        if let Some(idx) = self.cache.get_mut(hash) {
            *idx += 1;
            return *idx;
        }

        loop {
            match self.repo.fetch_last_log_idx(*hash).await {
                Ok(idx) => {
                    let idx = idx.map(|idx| idx + 1).unwrap_or(0) as u32 as u64;
                    self.cache.put(*hash, idx);
                    return idx;
                }
                Err(err) => {
                    tracing::warn!(?err, "failed to fetch log idx");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

fn fake_hash(slot: u64) -> Hash {
    let mut hash = [0xff; 32];
    let bits = slot.ilog2() + 1;
    let bytes = bits / 8 + if bits % 8 > 0 { 1 } else { 0 };
    let bytes = (bytes + 1) as usize;
    hash[32 - bytes..].copy_from_slice(&slot.to_be_bytes()[8 - bytes..]);
    Hash::new_from_array(hash)
}
