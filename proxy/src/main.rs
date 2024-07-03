use std::path::PathBuf;

use clap::Parser;
use common::neon_lib::types::{Address, ChDbConfig};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::EncodableKey;
use jsonrpsee::types::ErrorCode;
use rpc_api::{EthApiServer, EthFilterApiServer};
use solana_api::solana_api::SolanaApi;
use thiserror::Error;

mod convert;
mod executor;
mod neon_api;
mod rpc;

use executor::Executor;
use neon_api::NeonApi;
use rpc::{EthApiImpl, NeonEthApiServer, NeonFilterApiServer};

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
    #[arg(long)]
    /// Path to operator keypair
    operator_keypair_path: PathBuf,

    #[arg(long)]
    /// Operator ETH address
    operator_address: Address,
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
    );

    let operator =
        Keypair::read_from_file(opts.operator_keypair_path).expect("cannot read operator keypair");
    let executor = Executor::initialize(
        solana.clone(),
        SolanaApi::new(opts.solana_url, false),
        opts.neon_pubkey,
        operator,
        opts.operator_address,
    )
    .await
    .expect("could not initialize executor");

    let eth = EthApiImpl::new(pool, solana, executor, opts.chain_id);
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
