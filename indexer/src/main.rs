use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use solana::traverse::v2::{TraverseConfig, TraverseLedger};
use tokio::sync::mpsc;

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use indexer::Indexer;
use metrics::metrics;
use tokio_stream::{Stream, StreamExt};

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

    let mut indexer = Indexer::new(pool, opts.target);
    let last_signature = indexer.get_latest_signature().await?;
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
        tracing::debug!(?result, "retrieved new block");

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
