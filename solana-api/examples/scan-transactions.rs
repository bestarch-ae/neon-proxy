use anyhow::Result;
use clap::Parser;

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::types::SolanaBlock;
use solana_api::traverse::{LedgerItem, TraverseConfig, TraverseLedger};
use solana_client::rpc_client::SerializableTransaction;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about)]
/// Request all transactions for a target pubkey in ascending order
struct Cli {
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

    #[arg(short, long)]
    /// Print only slot and signature
    short: bool,

    #[arg(short, long)]
    /// Print transaction logs
    logs: bool,

    #[arg(short, long)]
    /// Print transaction data
    verbose: bool,

    #[arg(long)]
    /// Ignore failed transactions
    no_fail: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let opts = Cli::try_parse()?;

    let traverse_config = TraverseConfig {
        endpoint: opts.url,
        target_key: opts.target,
        last_observed: opts.from,
        finalized: true,
        only_success: opts.no_fail,
        ..Default::default()
    };
    let mut traverse = TraverseLedger::new(traverse_config);

    while let Some(result) = traverse.next().await {
        match result? {
            LedgerItem::Transaction(tx) => {
                if opts.short {
                    println!("{} - {}", tx.slot, tx.tx.get_signature());
                } else {
                    println!("=======");
                    println!("{}", tx.tx.get_signature());
                    println!("-----");
                    println!("Slot: {}, Idx: {}", tx.slot, tx.tx_idx);
                    println!();
                    println!("Version: {:?}", tx.tx.version());
                    println!(
                        "Fee: {}, Compute Units Consumed: {}",
                        tx.fee, tx.compute_units_consumed
                    );
                    println!("Status: {:?}", tx.status);
                    println!("Loaded Addresses: {:?}", tx.loaded_addresses);
                    println!();
                    if opts.logs {
                        println!("Logs:");
                        tx.log_messages.iter().for_each(|log| println!("    {log}"));
                    }
                    if opts.verbose {
                        println!("Message: \n{:#?}", tx.tx);
                    }
                    println!("-----");
                    println!();
                }
            }
            LedgerItem::Block(block) => {
                let SolanaBlock {
                    slot,
                    hash,
                    parent_slot,
                    parent_hash,
                    time,
                } = block;
                println!("===== End of Block =====");
                println!("Slot: {slot}, Hash: {hash}");
                println!("Parent Slot: {parent_slot}, Parent Hash: {parent_hash}");
                println!("Time: {time:?}");
                println!();
            }
        }
    }
    Ok(())
}
