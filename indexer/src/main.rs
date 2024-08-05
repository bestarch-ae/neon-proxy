use std::io::IsTerminal as _;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use common::solana_sdk::pubkey::Pubkey;
// use futures::stream::StreamExt;

use indexer::Indexer;
use metrics::metrics;
use solana::traverse::v2::{TraverseConfig, TraverseLedger};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

mod accountsdb;
mod indexer;
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

    #[arg(short, long, default_value = None, value_name = "SLOT")]
    /// Slot to start from
    slot: Option<u64>,

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

    #[arg(long, default_value = "16")]
    /// Number of tasks to process slots in parallel
    max_traverse_tasks: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    let opts = Args::try_parse()?;

    let pool = db::connect(&opts.pg_url).await?;

    let mut indexer = Indexer::new(pool, opts.target);
    let last_block = indexer.get_latest_block().await?.map(|x| x + 1);
    let start_slot = last_block.or(opts.slot);

    tracing::info!("starting traversal from slot {:?}", start_slot);

    let traverse_config = TraverseConfig {
        endpoint: opts.url,
        rps_limit_sleep: opts.rps_limit_sleep.map(Duration::from_secs),
        target_key: opts.target,
        last_observed: None,
        finalized: !opts.confirmed,
        only_success: true,
        max_concurrent_tasks: opts.max_traverse_tasks,
        ..Default::default()
    };
    let traverse = TraverseLedger::new(traverse_config);

    if let Some(addr) = opts.metrics_addr {
        metrics().expose(addr)?;
    }
    tracing::info!("connected");

    let (tx, mut rx) = mpsc::channel(128);

    let traverse_handle = tokio::spawn(async move {
        let mut traverse = if let Some(slot) = start_slot {
            Box::pin(traverse.in_range(slot..)) as std::pin::Pin<Box<dyn Stream<Item = _> + Send>>
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
        tracing::debug!(?result, "retrieved new block");

        match result {
            Ok(item) => {
                let slot = item.slot();
                if let Err(err) = indexer.process_ledger(item).await {
                    tracing::error!(error = %err, %slot, "failed processing item");
                }
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
