use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use common::solana_sdk::commitment_config::CommitmentLevel;
use solana::traverse::v2::{LedgerItem, TraverseConfig, TraverseLedger};
use tokio::sync::mpsc;

use common::ethnum::U256;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::types::{HolderOperation, SolanaBlock, SolanaTransaction, TxHash};
use metrics::metrics;
use neon_parse::Action;
use tokio_stream::{Stream, StreamExt};

mod accountsdb;
mod metrics;

#[derive(Parser)]
struct Args {
    #[arg(value_name = "Pubkey")]
    /// Target pubkey
    target: Pubkey,

    #[arg(
        short,
        long,
        default_value = "https://api.mainnet-beta.solana.com",
        value_name = "URL"
    )]
    /// Solana endpoint
    url: String,

    #[arg(short, long, default_value = None, value_name = "SIGNATURE")]
    /// Transaction to start from
    from: Option<Signature>,

    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    pg_url: String,

    #[arg(long, default_value = None, value_name = "SECS")]
    /// Seconds to sleep between RPS limit rejected gSFA
    rps_limit_sleep: Option<u64>,

    #[arg(long)]
    /// Indexed confirmed blocks and transactions
    confirmed: bool,

    #[arg(long)]
    /// Address for prometheus metrics
    metrics_addr: Option<std::net::SocketAddr>,
}

struct Indexer {
    tx_repo: db::TransactionRepo,
    sig_repo: db::SolanaSignaturesRepo,
    holder_repo: db::HolderRepo,
    block_repo: db::BlockRepo,
    neon_tx_idx: u64,
    block_gas_used: U256,
    block_log_idx: u64,
    tx_log_idx: TxLogIdx,
    adb: accountsdb::DummyAdb,
    target: Pubkey,
}

impl Indexer {
    fn new(pool: db::PgPool, target: Pubkey) -> Self {
        let tx_repo = db::TransactionRepo::new(pool.clone());
        let sig_repo = db::SolanaSignaturesRepo::new(pool.clone());
        let holder_repo = db::HolderRepo::new(pool.clone());
        let block_repo = db::BlockRepo::new(pool.clone());
        let tx_log_idx = TxLogIdx::new(tx_repo.clone());
        let adb = accountsdb::DummyAdb::new(target, holder_repo.clone());

        Self {
            tx_repo,
            sig_repo,
            holder_repo,
            block_repo,
            neon_tx_idx: 0,
            block_gas_used: U256::new(0),
            block_log_idx: 0,
            tx_log_idx,
            adb,
            target,
        }
    }

    async fn process_block(
        &mut self,
        block: SolanaBlock,
        txs: Vec<SolanaTransaction>,
        _commitment: CommitmentLevel,
    ) -> anyhow::Result<()> {
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

                        if let Err(err) = self.tx_repo.insert(&tx).await {
                            tracing::warn!(?err, "failed to save neon transaction");
                            metrics().database_errors.inc();
                        } else {
                            metrics().neon_transactions_saved.inc();
                            tracing::info!(
                                signature = %tx.neon_signature,
                                "saved transaction"
                            );
                        }
                    }
                    Action::CancelTransaction { hash, total_gas } => {
                        if let Err(err) = self.tx_repo.set_canceled(&hash, total_gas, slot).await {
                            tracing::warn!(?err, "failed to cancel neon transaction");
                            metrics().database_errors.inc();
                        }
                    }
                    Action::WriteHolder(op) => {
                        tracing::info!(slot = %slot, pubkey = %op.pubkey(), "saving holder");
                        if let Err(err) =
                            process_holder(&self.holder_repo, slot, tx_idx, &op, &mut self.adb)
                                .await
                        {
                            tracing::warn!(?err, pubkey = %op.pubkey(), "failed to save neon holder");
                            metrics().database_errors.inc();
                        } else {
                            metrics().holders_saved.inc();
                        }
                    }
                }
            }
            if let Err(err) = self.sig_repo.insert(slot, tx_idx, signature).await {
                tracing::warn!(?err, "failed to save solana transaction");
                metrics().database_errors.inc();
            }
        }

        let _blk_timer = metrics().block_processing_time.start_timer();
        self.neon_tx_idx = 0;
        self.block_gas_used = U256::new(0);
        self.block_log_idx = 0;

        if let Err(err) = self.block_repo.insert(&block).await {
            tracing::warn!(?err, slot = block.slot, "failed to save solana block");
            metrics().database_errors.inc();
        } else {
            tracing::info!(slot = block.slot, "saved solana block");
            metrics().blocks_processed.inc();
        }
        Ok(())
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
        }
        Ok(())
    }

    async fn process_missing_block(&mut self, slot: u64) -> Result<()> {
        tracing::info!(%slot, "missing block, generating fake");
        let block = SolanaBlock {
            slot,
            hash: fake_hash(slot),
            parent_hash: fake_hash(slot - 1),
            parent_slot: slot - 1,
            time: None,
            is_finalized: true,
        };
        if let Err(err) = self.block_repo.insert(&block).await {
            tracing::warn!(?err, slot = block.slot, "failed to save solana block");
            metrics().database_errors.inc();
        } else {
            tracing::info!(slot = block.slot, "saved solana block");
            metrics().blocks_processed.inc();
        }
        Ok(())
    }

    async fn process_ledger(&mut self, item: LedgerItem) -> anyhow::Result<()> {
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Args::try_parse()?;

    let pool = db::connect(&opts.pg_url).await?;

    let mut indexer = Indexer::new(pool, opts.target);
    let last_signature = indexer.sig_repo.get_latest().await?;
    let from = opts.from.or(last_signature);

    tracing::info!("starting traversal from {:?}", from);

    let traverse_config = TraverseConfig {
        endpoint: opts.url,
        rps_limit_sleep: opts.rps_limit_sleep.map(Duration::from_secs),
        target_key: opts.target,
        last_observed: from,
        finalized: !opts.confirmed,
        only_success: true,
        ..Default::default()
    };
    let traverse = TraverseLedger::new(traverse_config);

    if let Some(addr) = opts.metrics_addr {
        metrics().expose(addr)?;
    }
    tracing::info!("connected");

    let (tx, mut rx) = mpsc::channel(128);

    let traverse_handle = tokio::spawn(async move {
        let mut traverse = if let Some(signature) = from {
            Box::pin(traverse.since_signature(signature).await)
                as std::pin::Pin<Box<dyn Stream<Item = _> + Send>>
        } else {
            Box::pin(traverse.start_from_beginning())
        };
        while let Some(result) = traverse.next().await {
            if let Err(err) = tx.send(result).await {
                tracing::error!(?err, "failed to send");
            }
            metrics()
                .traverse_channel_capacity
                .set(tx.capacity() as i64);
        }
        tracing::info!("traverse stopped");
    });

    while let Some(result) = rx.recv().await {
        tracing::debug!(?result, "retrieved transaction/block");

        match result {
            Ok(item) => indexer.process_ledger(item).await?,
            Err(err) => {
                tracing::warn!(?err, "failed to retrieve transaction");
                metrics().traverse_errors.inc();
                continue;
            }
        };
    }

    if let Err(err) = traverse_handle.await {
        tracing::error!(?err, "traverse task failed");
    };

    Ok(())
}

fn fake_hash(slot: u64) -> Hash {
    let mut hash = [0xff; 32];
    let bits = slot.ilog2() + 1;
    let bytes = bits / 8 + if bits % 8 > 0 { 1 } else { 0 };
    let bytes = (bytes + 1) as usize;
    hash[32 - bytes..].copy_from_slice(&slot.to_be_bytes()[8 - bytes..]);
    Hash::new_from_array(hash)
}

async fn process_holder(
    repo: &db::HolderRepo,
    slot: u64,
    tx_idx: u32,
    op: &HolderOperation,
    adb: &mut impl neon_parse::AccountsDb,
) -> Result<(), anyhow::Error> {
    if let HolderOperation::Delete(pubkey) = op {
        adb.delete_account(*pubkey);
    }
    repo.insert(slot, tx_idx, false, op).await?;
    Ok(())
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
