use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use clap::{ArgGroup, Parser};
use futures_util::TryFutureExt;
use hyper::body::HttpBody;
use hyper::{Body, Request, Response};
use jsonrpsee::server::Server;
use jsonrpsee::types::ErrorCode;
use jsonrpsee::RpcModule;
use rpc_api::{EthApiServer, EthFilterApiServer};
use serde_json::Value;
use thiserror::Error;
use tower::{Layer, Service};

mod convert;
mod executor;
mod mempool;
mod neon_api;
mod rpc;

use common::neon_lib::types::ChDbConfig;
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

    #[group(flatten)]
    executor: executor::Config,

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
        default_token = opts.default_token_name,
        "starting"
    );

    let pool = db::connect(&opts.pg_url).await.unwrap();
    let tracer_db_config = ChDbConfig {
        clickhouse_url: opts.neon_db_clickhouse_urls,
        clickhouse_user: opts.neon_db_clickhouse_user,
        clickhouse_password: opts.neon_db_clickhouse_password,
    };

    tracing::info!(%opts.neon_pubkey, %opts.neon_config_pubkey, "starting");
    let neon_api = NeonApi::new(
        opts.solana_url.clone(),
        opts.neon_pubkey,
        opts.neon_config_pubkey,
        tracer_db_config,
        opts.max_tx_account_count,
    );

    let config = neon_api
        .get_config()
        .await
        .expect("failed to get EVM config");
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
        neon_api.clone(),
        pyth_symbology,
        mp_gas_calculator_config,
    )
    .expect("failed to create gas prices");

    let executor = if let Some(path) = &opts.executor.operator_keypair {
        let operator = Keypair::read_from_file(path).expect("cannot read operator keypair");
        let (executor, executor_task) = Executor::initialize_and_start(
            neon_api.clone(),
            SolanaApi::new(opts.solana_url, false),
            opts.neon_pubkey,
            operator,
            opts.executor.operator_address,
            opts.executor.init_operator_balance,
        )
        .await
        .expect("could not initialize executor");
        tokio::spawn(executor_task);
        Some(executor)
    } else {
        None
    };
    let eth = EthApiImpl::new(
        pool.clone(),
        neon_api.clone(),
        default_chain_id,
        executor.clone(),
        mp_gas_prices.clone(),
    );
    let mut other_tokens = HashSet::new();
    let mut module = build_module(eth.clone(), None);
    for (token_name, &chain_id) in additional_chains.iter() {
        tracing::info!(%token_name, %chain_id, "adding chain");
        let new_eth = eth.clone().with_chain_id(chain_id);
        let new_module = build_module(new_eth, Some(token_name));
        module.merge(new_module).expect("no conflicts");
        other_tokens.insert(token_name.clone());
    }

    let service_builder =
        tower::ServiceBuilder::new().layer(ProxyTokenRequestLayer::new(other_tokens));

    let server = Server::builder()
        .set_http_middleware(service_builder)
        .build(&opts.listen)
        .await
        .unwrap();

    tracing::info!("Listening on {}", opts.listen);
    let handle = server.start(module);
    handle.stopped().await;
}

fn build_module(eth: EthApiImpl, prefix: Option<&str>) -> RpcModule<()> {
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

    if let Some(prefix) = prefix {
        rename_methods(&mut module, prefix);
    }

    module
}

fn rename_methods<T: Send + Sync + 'static>(module: &mut RpcModule<T>, prefix: &str) {
    let method_names = module.method_names().collect::<Vec<_>>();
    for name in method_names {
        let new_name: &'static str = build_method_name_with_prefix(prefix, name);
        module
            .register_alias(new_name, name)
            .expect("failed to register alias");
        module.remove_method(name);
    }
}

fn build_method_name_with_prefix(prefix: &str, method_name: &str) -> &'static str {
    let new_name = format!("{prefix}_{method_name}");
    Box::leak(new_name.into_boxed_str())
}

#[derive(Debug, Clone)]
pub struct ProxyTokenRequest<S> {
    inner: S,
    token_names: Arc<HashSet<String>>,
}

impl<S> ProxyTokenRequest<S> {
    pub fn new(inner: S, token_names: Arc<HashSet<String>>) -> Self {
        Self { inner, token_names }
    }
}

#[derive(Debug, Clone)]
pub struct ProxyTokenRequestLayer {
    token_names: Arc<HashSet<String>>,
}

impl ProxyTokenRequestLayer {
    pub fn new(token_names: HashSet<String>) -> Self {
        Self {
            token_names: Arc::new(token_names),
        }
    }
}

impl<S> Layer<S> for ProxyTokenRequestLayer {
    type Service = ProxyTokenRequest<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProxyTokenRequest::new(inner, self.token_names.clone())
    }
}

impl<S> Service<Request<Body>> for ProxyTokenRequest<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Response: 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>> + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn StdError + Send + Sync + 'static>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().trim_matches('/').to_lowercase();
        if self.token_names.contains(&path) {
            tracing::info!(token_name=?path, "token request");
            let mut inner = self.inner.clone();
            let fut = async move {
                let body_bytes = match body.collect().await {
                    Ok(body) => body.to_bytes(),
                    Err(err) => {
                        tracing::warn!(?err, "Failed to read request body");
                        let response = Response::builder()
                            .status(400)
                            .body(Body::from("Failed to read request body"))
                            .unwrap();
                        return Ok(response);
                    }
                };
                let mut request_json: Value = match serde_json::from_slice(&body_bytes) {
                    Ok(json) => json,
                    Err(_) => {
                        let response = Response::builder()
                            .status(400)
                            .body(Body::from("Invalid JSON in request body"))
                            .unwrap();
                        return Ok(response);
                    }
                };

                if let Some(method) = request_json["method"].as_str() {
                    let new_method = format!("{path}_{method}");
                    request_json["method"] = Value::String(new_method);
                }

                let new_body = Body::from(request_json.to_string());
                let new_req = Request::from_parts(parts, new_body);

                inner.call(new_req).await.map_err(Into::into)
            };

            Box::pin(fut)
        } else {
            tracing::info!("default token request");
            Box::pin(
                self.inner
                    .call(Request::from_parts(parts, body))
                    .map_err(Into::into),
            )
        }
    }
}
