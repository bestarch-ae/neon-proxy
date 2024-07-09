use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{ArgGroup, Parser};

use jsonrpsee::types::ErrorCode;
use rpc_api::{EthApiServer, EthFilterApiServer};
use thiserror::Error;

mod convert;
mod executor;
mod gas_limit_calculator;
mod mempool;
mod neon_api;
mod rpc;

use common::neon_lib::types::{Address, ChDbConfig};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::EncodableKey;
use executor::Executor;
use neon_api::NeonApi;
use rpc::{EthApiImpl, NeonEthApiServer, NeonFilterApiServer};
use solana_api::solana_api::SolanaApi;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    DB(#[from] db::Error),
    #[error("parse error: {0}")]
    Parse(#[from] anyhow::Error),
}

impl From<Error> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: Error) -> Self {
        tracing::error!("error: {}", value);
        match value {
            Error::DB(..) | Error::Parse(..) => ErrorCode::InternalError.into(),
        }
    }
}

#[derive(Parser)]
#[command(group(
    ArgGroup::new("required_group")
    .args(&["symbology_path", "const_gas_price", "pyth_mapping_addr"])
    .required(true)
))]
struct Args {
    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    /// Postgres url
    pg_url: String,

    #[arg(
        short,
        long,
        default_value = "127.0.0.1:8888",
        value_name = "LISTEN_ADDR"
    )]
    listen: String,

    #[arg(
        value_name = "NEON_PUBKEY",
        default_value = "eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU"
    )]
    /// Neon program pubkey
    neon_pubkey: Pubkey,

    #[arg(
        short('c'),
        long,
        value_name = "CONFIG_PUBKEY",
        default_value = "BMp6gEnveANdvSvspESJUrNczuHz1GF5UQKjVLCkAZih"
    )]
    neon_config_pubkey: Pubkey,

    #[arg(
        short('u'),
        long,
        default_value = "https://api.mainnet-beta.solana.com",
        value_name = "URL"
    )]
    /// Solana endpoint
    solana_url: String,

    #[arg(short('w'), long, default_value = "wss://api.mainnet-beta.solana.com")]
    /// Solana websocket endpoint
    solana_ws_url: String,

    #[arg(long)]
    /// Pyth mapping address
    pyth_mapping_addr: Option<Pubkey>,

    #[arg(long, env, value_delimiter = ';')]
    /// Tracer db urls, comma separated
    neon_db_clickhouse_urls: Vec<String>,

    #[arg(long, env)]
    /// Trace db user
    neon_db_clickhouse_user: Option<String>,

    #[arg(long, env)]
    /// Trace db password
    neon_db_clickhouse_password: Option<String>,

    #[arg(long, env, default_value = "245022926")]
    // Neon chain id
    chain_id: u64,

    #[arg(long, requires = "operator_address")]
    /// Path to operator keypair
    operator_keypair: Option<PathBuf>,

    #[arg(long, requires = "operator_keypair")]
    /// Operator ETH address
    operator_address: Option<Address>,

    #[arg(long, env, default_value = "SOL")]
    /// Chain token name
    chain_token_name: String,

    #[arg(long, env, default_value = "NEON")]
    /// Default token name
    default_token_name: String,

    #[arg(long, env, default_value = "1")]
    /// Minimal gas price
    minimal_gas_price: u128,

    #[arg(long, env)]
    /// Constant gas price
    const_gas_price: Option<u128>,

    #[arg(long, env, default_value = "50000")]
    /// Operator fee
    operator_fee: u128,

    #[arg(long, env)]
    symbology_path: Option<PathBuf>,

    #[arg(long, env, default_value = "64")]
    // Max tx account count
    max_tx_account_count: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let _ = tracing_log::LogTracer::init();

    let opts = Args::parse();

    tracing::info!(
        neon_pubkey = %opts.neon_pubkey,
        neon_config = %opts.neon_config_pubkey,
        chain_id = opts.chain_id,
        "starting"
    );

    let pool = db::connect(&opts.pg_url).await.unwrap();
    let tracer_db_config = ChDbConfig {
        clickhouse_url: opts.neon_db_clickhouse_urls,
        clickhouse_user: opts.neon_db_clickhouse_user,
        clickhouse_password: opts.neon_db_clickhouse_password,
    };

    tracing::info!(%opts.neon_pubkey, %opts.neon_config_pubkey, "starting");
    let solana = NeonApi::new(
        opts.solana_url.clone(),
        opts.neon_pubkey,
        opts.neon_config_pubkey,
        tracer_db_config,
        opts.max_tx_account_count,
    );
    let pyth_symbology = if let Some(path) = opts.symbology_path.as_ref() {
        tracing::info!(?path, "loading symbology");
        let raw = std::fs::read_to_string(path).expect("failed to read symbology");
        let symbology_raw: HashMap<String, String> =
            serde_json::from_str(&raw).expect("failed to parse symbology json");
        symbology_raw
            .iter()
            .map(|(k, v)| Pubkey::from_str(v).map(|pubkey| (k.clone(), pubkey)))
            .collect::<Result<HashMap<String, Pubkey>, _>>()
            .expect("failed to parse symbology")
    } else if let Some(mapping_addr) = &opts.pyth_mapping_addr {
        tracing::info!(%mapping_addr, "loading symbology");
        let rpc_client = RpcClient::new(opts.solana_url.clone());
        let symbology = mempool::pyth_collect_symbology(mapping_addr, &rpc_client)
            .await
            .expect("failed to collect pyth symbology");
        drop(rpc_client);
        symbology
    } else {
        HashMap::new()
    };
    let mp_gas_calculator_config = mempool::GasPriceCalculatorConfig {
        operator_fee: opts.operator_fee,
        min_gas_price: opts.minimal_gas_price,
        const_gas_price: opts.const_gas_price,
    };
    let gas_prices_config = mempool::GasPricesConfig {
        ws_url: opts.solana_ws_url.to_owned(),
        base_token: opts.chain_token_name.to_owned(),
        default_token: opts.default_token_name.to_owned(),
    };
    let mp_gas_prices = mempool::GasPrices::try_new(
        gas_prices_config,
        solana.clone(),
        pyth_symbology,
        mp_gas_calculator_config,
    )
    .expect("failed to create gas prices");

    let executor = if let Some((path, address)) = opts.operator_keypair.zip(opts.operator_address) {
        let operator = Keypair::read_from_file(path).expect("cannot read operator keypair");
        Some(
            Executor::initialize(
                solana.clone(),
                SolanaApi::new(opts.solana_url, false),
                opts.neon_pubkey,
                operator,
                address,
            )
            .await
            .expect("could not initialize executor"),
        )
    } else {
        None
    };

    let eth = EthApiImpl::new(pool, solana, opts.chain_id, executor, mp_gas_prices);
    let mut module = jsonrpsee::server::RpcModule::new(());
    module
        .merge(<EthApiImpl as EthApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");

    module.remove_method("eth_getTransactionReceipt");

    module
        .merge(<EthApiImpl as NeonEthApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");

    module
        .merge(<EthApiImpl as EthFilterApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");

    module.remove_method("eth_getLogs");
    module
        .merge(<EthApiImpl as NeonFilterApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");

    let server = jsonrpsee::server::Server::builder()
        .build(&opts.listen)
        .await
        .unwrap();
    tracing::info!("Listening on {}", opts.listen);
    let handle = server.start(module);
    handle.stopped().await;
}
