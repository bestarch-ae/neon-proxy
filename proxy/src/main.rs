use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use clap::{ArgGroup, Parser};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use jsonrpsee::server::Server;
use jsonrpsee::RpcModule;
use operator::Operators;
use rpc_api::{EthApiServer, EthFilterApiServer, NetApiServer, Web3ApiServer};
use tower::Service;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

mod convert;
mod error;
mod rpc;

use common::neon_lib;
use common::neon_lib::types::ChDbConfig;
use common::solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use common::solana_sdk::pubkey::Pubkey;
use executor::Executor;
use mempool::{GasPriceCalculatorConfig, GasPrices, GasPricesConfig, Mempool, MempoolConfig};
use neon_api::NeonApi;
use solana_api::solana_api::SolanaApi;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::rpc::{EthApiImpl, NeonCustomApiServer, NeonEthApiServer, NeonFilterApiServer};

#[derive(Debug, Default, Copy, Clone)]
enum LogFormat {
    Json,
    #[default]
    Plain,
}

impl FromStr for LogFormat {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "json" => Ok(LogFormat::Json),
            "plain" => Ok(LogFormat::Plain),
            any => {
                eprintln!("invalid log format {}, defaulting to plain", any);
                Ok(LogFormat::Plain)
            }
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

    #[arg(long, value_name = "URL")]
    /// Pyth solana endpoint (for fetching symbology)
    /// If not provided, solana_url will be used
    pyth_solana_url: Option<String>,

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

    #[group(flatten)]
    executor: executor::Config,

    #[group(flatten)]
    operator: operator::Config,

    #[group(flatten)]
    gas_prices_calculator_config: GasPriceCalculatorConfig,

    #[arg(long, env, default_value = "SOL")]
    /// Chain token name
    chain_token_name: String,

    #[arg(long, env, default_value = "NEON")]
    /// Default token name
    default_token_name: String,

    #[arg(long, env)]
    symbology_path: Option<PathBuf>,

    #[arg(long, env, default_value = "64")]
    // Max tx account count
    max_tx_account_count: usize,

    #[arg(long, env, default_value = "finalized")]
    simulation_commitment: CommitmentLevel,

    #[arg(long)]
    /// Log format, either json or plain
    log_format: Option<LogFormat>,

    // todo: pick a default value
    #[arg(long, env, default_value = "100")]
    mp_capacity: usize,

    // todo: pick a default value
    #[arg(long, env, default_value = "0.9")]
    mp_capacity_high_watermark: f64,

    // todo: pick a default value
    #[arg(long, env, default_value = "3600")]
    mp_eviction_timeout_sec: u64,
}

fn get_lib_version() -> Option<String> {
    let build_info = serde_json::to_string(&neon_lib::build_info::get_build_info()).ok()?;
    let build_info: serde_json::Value = serde_json::from_str(&build_info).ok()?;
    let version = build_info
        .get("crate_info")
        .and_then(|info| info.get("version"))
        .and_then(|v| v.as_str())?;
    let commit = build_info
        .get("version_control")
        .and_then(|info| info.get("commit_id"))
        .and_then(|i| i.as_str())?;
    Some(format!("{}-{}", version, commit))
}

#[tokio::main]
async fn main() {
    let opts = Args::parse();

    let format = opts.log_format.unwrap_or_default();
    let builder = tracing_subscriber::fmt::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(std::io::stdout().is_terminal());
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Plain => builder.init(),
    }
    let _ = tracing_log::LogTracer::init();

    let neon_lib_version = get_lib_version();

    tracing::info!(
        neon_pubkey = %opts.neon_pubkey,
        neon_config = %opts.neon_config_pubkey,
        default_token = opts.default_token_name,
        simulation_commitment = ?opts.simulation_commitment,
        neon_lib_version = ?neon_lib_version,
        "starting"
    );

    let pool = db::connect(&opts.pg_url)
        .await
        .context("connecting to db")
        .unwrap();
    let tracer_db_config = ChDbConfig {
        clickhouse_url: opts.neon_db_clickhouse_urls,
        clickhouse_user: opts.neon_db_clickhouse_user,
        clickhouse_password: opts.neon_db_clickhouse_password,
    };

    let operator = Operators::from_config(opts.operator).unwrap();

    let neon_api = NeonApi::new(
        opts.solana_url.clone(),
        opts.neon_pubkey,
        opts.neon_config_pubkey,
        tracer_db_config,
        opts.max_tx_account_count,
        Some(CommitmentConfig {
            commitment: opts.simulation_commitment,
        }),
    );

    let config = neon_api
        .get_config()
        .await
        .expect("failed to get EVM config");
    tracing::info!(?config, "evm config");
    let default_token_name = opts.default_token_name.to_lowercase();
    let default_chain_id = config
        .chains
        .iter()
        .find(|c| c.name == default_token_name)
        .expect("default chain not found")
        .id;
    tracing::info!(%default_chain_id, %default_token_name, "default chain");
    let additional_chains = config
        .chains
        .iter()
        .filter(|&c| (c.id != default_chain_id))
        .map(|c| (c.name.to_lowercase(), c.id))
        .collect::<HashMap<_, _>>();
    tracing::info!(?additional_chains, "additional chains");

    let rpc_client = RpcClient::new(
        opts.pyth_solana_url
            .unwrap_or(opts.solana_url.clone())
            .clone(),
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
        mempool::pyth_collect_symbology(mapping_addr, &rpc_client)
            .await
            .expect("failed to collect pyth symbology")
    } else {
        HashMap::new()
    };
    let gas_prices_config = GasPricesConfig {
        ws_url: opts.solana_ws_url.to_owned(),
        base_token: opts.chain_token_name.to_owned(),
        default_token: opts.default_token_name.to_owned(),
        default_chain_id,
    };
    let chain_token_map = config
        .chains
        .iter()
        .map(|c| (c.id, c.name.to_uppercase()))
        .collect::<HashMap<_, _>>();
    let mp_gas_prices = mempool::GasPrices::try_new(
        gas_prices_config,
        neon_api.clone(),
        rpc_client,
        pyth_symbology,
        opts.gas_prices_calculator_config,
        chain_token_map,
    )
    .expect("failed to create gas prices");

    let executor = if opts.executor.operator_keypair.is_some() {
        let (executor, executor_task) = Executor::initialize_and_start(
            neon_api.clone(),
            SolanaApi::new(opts.solana_url, false),
            opts.neon_pubkey,
            opts.executor,
            Some(pool.clone()),
        )
        .await
        .expect("could not initialize executor");
        tokio::spawn(executor_task);
        Some(executor)
    } else {
        None
    };

    let mempool = if let Some(executor) = &executor {
        // todo: we should have a single executor; and we shouldn't allow direct access to it
        //  bypassing mempool
        let executor = executor.clone();
        let mp_config = MempoolConfig {
            capacity: opts.mp_capacity,
            capacity_high_watermark: opts.mp_capacity_high_watermark,
            eviction_timeout_sec: opts.mp_eviction_timeout_sec,
        };
        let mut mempool = Mempool::<Executor, GasPrices>::new(
            mp_config,
            mp_gas_prices.clone(),
            executor,
            neon_api.clone(),
        );
        mempool.start().await.expect("failed to start mempool");
        Some(Arc::new(mempool))
    } else {
        None
    };

    let eth = EthApiImpl::new(
        pool.clone(),
        neon_api.clone(),
        default_chain_id,
        mempool,
        mp_gas_prices.clone(),
        operator,
        neon_lib_version.unwrap_or("UNKNOWN".to_string()),
    );
    let mut other_tokens = HashSet::new();
    let mut tokens_methods = HashMap::new();
    let default_methods = build_module(eth.clone());
    for (token_name, &chain_id) in additional_chains.iter() {
        tracing::info!(%token_name, %chain_id, "adding chain");
        let new_eth = eth.clone().with_chain_id(chain_id);
        let new_methods = build_module(new_eth);
        tokens_methods.insert(token_name.clone(), new_methods);
        other_tokens.insert(token_name.clone());
    }

    let addr: SocketAddr = opts.listen.parse().expect("invalid listen address");
    let (stop_handle, server_handle) = jsonrpsee::server::stop_channel();
    let svc_builder = Server::builder().to_service_builder();
    let stop_handle2 = stop_handle.clone();

    let make_service = make_service_fn(move |_conn: &AddrStream| {
        let stop_handle = stop_handle2.clone();
        let svc_builder = svc_builder.clone();
        let tokens_methods = tokens_methods.clone();
        let default_methods = default_methods.clone();

        async move {
            Ok::<_, Box<dyn StdError + Send + Sync>>(service_fn(move |req| {
                let stop_handle = stop_handle.clone();
                let svc_builder = svc_builder.clone();
                let token_name = req.uri().path().trim_matches('/').to_lowercase();
                tracing::info!(?token_name, "request");
                let methods = tokens_methods
                    .get(&token_name)
                    .unwrap_or_else(|| {
                        tracing::info!(?token_name, "token not found, using default methods");
                        &default_methods
                    })
                    .clone();
                let mut svc = svc_builder.build(methods, stop_handle);
                svc.call(req)
            }))
        }
    });

    let server = hyper::Server::bind(&addr).serve(make_service);

    tokio::spawn(async move {
        let graceful = server.with_graceful_shutdown(async move { stop_handle.shutdown().await });
        graceful.await.expect("server error");
    });

    tracing::info!("Listening on {}", opts.listen);
    server_handle.stopped().await;
}

fn build_module(eth: EthApiImpl) -> RpcModule<()> {
    let mut module = RpcModule::new(());

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

    module
        .merge(<EthApiImpl as NeonCustomApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");
    module
        .merge(<EthApiImpl as NetApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");
    module
        .merge(<EthApiImpl as Web3ApiServer>::into_rpc(eth.clone()))
        .expect("no conflicts");

    module
}
