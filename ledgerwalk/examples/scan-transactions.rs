use anyhow::Result;
use clap::Parser;

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use ledgerwalk::solana_api::SolanaApi;
use ledgerwalk::traverse::TraverseLedger;
use solana_client::rpc_client::SerializableTransaction;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Cli::try_parse()?;

    let api = SolanaApi::new(opts.url);
    let mut traverse = TraverseLedger::new(api, opts.target, opts.from);

    while let Some(result) = traverse.next().await {
        let tx = result?;

        if opts.short {
            println!("{} - {}", tx.slot, tx.tx.get_signature());
        } else {
            println!("=======");
            println!("{}", tx.tx.get_signature());
            println!("-----");
            println!("Slot: {}, Idx: {}", tx.slot, tx.tx_idx);
            println!(
                "Blockhash: {}, Block Time {:?}",
                tx.blockhash, tx.block_time
            );
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
    Ok(())
}
