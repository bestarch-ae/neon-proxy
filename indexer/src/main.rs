use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use solana::traverse::{LedgerItem, TraverseConfig, TraverseLedger};
use tokio::sync::mpsc;

use common::ethnum::U256;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use metrics::metrics;
use neon_parse::Action;

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Args::try_parse()?;

    let pool = db::connect(&opts.pg_url).await?;
    let tx_repo = db::TransactionRepo::new(pool.clone());
    let sig_repo = db::SolanaSignaturesRepo::new(pool.clone());
    let holder_repo = db::HolderRepo::new(pool.clone());
    let block_repo = db::BlockRepo::new(pool);

    let last_signature = sig_repo.get_latest().await?;
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
    let mut traverse = TraverseLedger::new(traverse_config);
    let mut adb = accountsdb::DummyAdb::new(opts.target, holder_repo.clone());

    if let Some(addr) = opts.metrics_addr {
        metrics().expose(addr)?;
    }
    let mut last_written_slot = None;
    tracing::info!("connected");

    let (tx, mut rx) = mpsc::channel(128);

    let traverse_handle = tokio::spawn(async move {
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

    /* TODO: we should always start from the start of the block otherwise these would be wrong */
    let mut neon_tx_idx = 0;
    let mut block_gas_used = U256::new(0);
    let mut block_log_idx = 0;

    while let Some(result) = rx.recv().await {
        tracing::debug!(?result, "retrieved transaction/block");

        match result {
            Ok(LedgerItem::Transaction(tx)) => {
                let _tx_timer = metrics().transaction_processing_time.start_timer();
                let signature = tx.tx.signatures[0];
                let tx_idx = tx.tx_idx as u32;
                let slot = tx.slot;

                metrics().transactions_processed.inc();
                metrics().current_slot.set(slot as i64);

                let _span =
                    tracing::info_span!("solana transaction", signature = %signature).entered();

                adb.set_slot_idx(slot, tx_idx);

                let parse_timer = metrics().neon_parse_time.start_timer();
                let actions = match neon_parse::parse(tx, &mut adb) {
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
                            tx.tx_idx = neon_tx_idx;

                            // only completed transactions increment gas and idx
                            if tx.is_completed {
                                neon_tx_idx += 1;
                                block_gas_used += tx.gas_used;
                                tx.sum_gas_used = block_gas_used;

                                for log in &mut tx.events {
                                    if !log.is_hidden {
                                        log.log_idx = block_log_idx;
                                        block_log_idx += 1;
                                    }
                                }
                            }

                            if let Err(err) = tx_repo.insert(&tx).await {
                                tracing::warn!(?err, "failed to save neon transaction");
                                metrics().database_errors.inc();
                            } else {
                                metrics().neon_transactions_saved.inc();
                                tracing::info!(
                                    signature = hex::encode(tx.neon_signature),
                                    "saved transaction"
                                );
                            }
                        }
                        Action::CancelTransaction(hash) => {
                            if let Err(err) = tx_repo.set_canceled(&hash, slot).await {
                                tracing::warn!(?err, "failed to cancel neon transaction");
                                metrics().database_errors.inc();
                            }
                        }
                        Action::WriteHolder(op) => {
                            tracing::info!(slot = %slot, pubkey = %op.pubkey(), "saving holder");
                            if let Err(err) =
                                process_holder(&holder_repo, slot, tx_idx, &op, &mut adb).await
                            {
                                tracing::warn!(?err, "failed to save neon holder");
                                metrics().database_errors.inc();
                            } else {
                                metrics().holders_saved.inc();
                            }
                        }
                    }
                }
                if let Err(err) = sig_repo.insert(slot, tx_idx, signature).await {
                    tracing::warn!(?err, "failed to save solana transaction");
                    metrics().database_errors.inc();
                }
            }
            Ok(LedgerItem::Block(block)) => {
                let _blk_timer = metrics().block_processing_time.start_timer();
                neon_tx_idx = 0;
                block_gas_used = U256::new(0);
                block_log_idx = 0;

                if let Err(err) = block_repo.insert(&block).await {
                    tracing::warn!(?err, slot = block.slot, "failed to save solana block");
                    metrics().database_errors.inc();
                } else {
                    tracing::info!(slot = block.slot, "saved solana block");
                    last_written_slot.replace(block.slot);
                    metrics().blocks_processed.inc();
                }
            }
            Ok(LedgerItem::FinalizedBlock(slot)) => {
                let _blk_timer = metrics().finalized_block_processing_time.start_timer();
                if let Err(err) = block_repo.finalize(slot).await {
                    tracing::warn!(%err, slot, "failed finalizing block in db");
                    metrics().database_errors.inc();
                    continue;
                }
                metrics().finalized_blocks_processed.inc();
                tracing::info!(slot, "block was finalized");
            }
            Ok(LedgerItem::PurgedBlock(slot)) => {
                let _blk_timer = metrics().purged_block_processing_time.start_timer();
                metrics().purged_blocks_processed.inc();
                if let Err(err) = block_repo.purge(slot).await {
                    tracing::warn!(%err, slot, "failed purging block in db");
                    metrics().database_errors.inc();
                    continue;
                }
                tracing::info!(slot, "block was purged");
            }
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

async fn process_holder(
    repo: &db::HolderRepo,
    slot: u64,
    tx_idx: u32,
    op: &neon_parse::HolderOperation,
    adb: &mut impl neon_parse::AccountsDb,
) -> Result<(), anyhow::Error> {
    use neon_parse::HolderOperation;
    match op {
        HolderOperation::Create(pubkey) => {
            repo.insert(slot, tx_idx, false, None, pubkey, None, None)
                .await?
        }
        HolderOperation::Write {
            pubkey,
            tx_hash,
            offset,
            data,
        } => {
            repo.insert(
                slot,
                tx_idx,
                false,
                Some(&hex::encode(tx_hash)),
                pubkey,
                Some(*offset as u64),
                Some(data),
            )
            .await?
        }
        HolderOperation::Delete(pubkey) => {
            adb.delete_account(*pubkey);
            repo.insert(slot, tx_idx, false, None, pubkey, None, None)
                .await?
        }
    }
    Ok(())
}
