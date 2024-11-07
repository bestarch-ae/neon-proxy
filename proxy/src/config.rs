use std::path::PathBuf;
use std::str::FromStr;

use clap::{ArgGroup, Args, Parser};

use common::neon_lib::commands::get_config::GetConfigResponse;
use common::neon_lib::types::ChDbConfig;
use common::solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use common::solana_sdk::pubkey::Pubkey;
pub use mempool::GasPriceCalculatorConfig;

#[derive(Debug, Default, Copy, Clone)]
pub enum LogFormat {
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

#[derive(Debug, Parser)]
#[command(group(
    ArgGroup::new("required_group")
    .args(&["symbology_path", "const_gas_price", "pyth_mapping_addr"])
    .required(true)
))]
pub struct Cli {
    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    /// Postgres url
    pub pg_url: String,

    #[arg(
        short,
        long,
        default_value = "127.0.0.1:8888",
        value_name = "LISTEN_ADDR"
    )]
    pub listen: String,

    #[arg(
        value_name = "NEON_PUBKEY",
        default_value = "eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU"
    )]
    /// Neon program pubkey
    pub neon_pubkey: Pubkey,

    #[arg(
        short('u'),
        long,
        default_value = "https://api.mainnet-beta.solana.com",
        value_name = "URL"
    )]
    /// Solana endpoint
    pub solana_url: String,

    #[arg(long, env, default_value = "245022926")]
    // Neon chain id
    pub chain_id: u64,

    #[arg(long)]
    /// Log format, either json or plain
    pub log_format: Option<LogFormat>,

    #[group(flatten)]
    pub neon_api: NeonApi,

    #[group(flatten)]
    pub tracer_db: TracerDb,

    #[group(flatten)]
    pub operator: operator_pool::Config,

    #[group(flatten)]
    pub mempool: Mempool,

    #[group(flatten)]
    pub gas_price: GasPrice,

    #[group(flatten)]
    pub gas_prices_calculator_config: GasPriceCalculatorConfig,
}

#[derive(Debug, Args)]
#[group(id = "NeonApiConfig")]
pub struct NeonApi {
    #[arg(long, env, default_value = "64")]
    // Max tx account count
    pub max_tx_account_count: usize,

    #[arg(long, env, default_value = "finalized")]
    pub simulation_commitment: CommitmentLevel,

    #[arg(
        short('c'),
        long,
        value_name = "CONFIG_PUBKEY",
        default_value = "BMp6gEnveANdvSvspESJUrNczuHz1GF5UQKjVLCkAZih"
    )]
    pub neon_config_pubkey: Pubkey,
}

#[derive(Debug, Args)]
#[group(id = "MempoolConfig")]
pub struct Mempool {
    // pub todo: pick a default value
    #[arg(long, env, default_value = "100")]
    pub mp_capacity: usize,

    // pub todo: pick a default value
    #[arg(long, env, default_value = "0.9")]
    pub mp_capacity_high_watermark: f64,

    // pub todo: pick a default value
    #[arg(long, env, default_value = "3600")]
    pub mp_eviction_timeout_sec: u64,

    /// Evicted sender transaction count cache size. Disabled by default.
    #[arg(long, env, default_value = "0")]
    pub mp_tx_count_cache_size: usize,

    /// Executed transaction cache size. Disabled by default.
    #[arg(long, env, default_value = "0")]
    pub mp_tx_cache_size: usize,
}

#[derive(Debug, Args)]
#[group(id = "TracerDbConfig")]
pub struct TracerDb {
    #[arg(long, env, value_delimiter = ';')]
    /// Tracer db urls, comma separated
    pub neon_db_clickhouse_urls: Vec<String>,

    #[arg(long, env)]
    /// Trace db user
    pub neon_db_clickhouse_user: Option<String>,

    #[arg(long, env)]
    /// Trace db password
    pub neon_db_clickhouse_password: Option<String>,
}

#[derive(Debug, Args)]
#[group(id = "GasPriceConfig")]
pub struct GasPrice {
    #[arg(long, value_name = "URL")]
    /// Pyth solana endpoint (for fetching symbology)
    /// If not provided, solana_url will be used
    pub pyth_solana_url: Option<String>,

    #[arg(short('w'), long, default_value = "wss://api.mainnet-beta.solana.com")]
    /// Solana websocket endpoint
    pub solana_ws_url: String,

    #[arg(long)]
    /// Pyth mapping address
    pub pyth_mapping_addr: Option<Pubkey>,

    #[arg(long, env)]
    pub symbology_path: Option<PathBuf>,

    #[arg(long, env, default_value = "SOL")]
    /// Chain token name
    pub chain_token_name: String,

    #[arg(long, env, default_value = "NEON")]
    /// Default token name
    pub default_token_name: String,
}

impl Cli {
    pub fn neon_api(&self) -> neon_api::NeonApi {
        let tracer_db_config = ChDbConfig {
            clickhouse_url: self.tracer_db.neon_db_clickhouse_urls.clone(),
            clickhouse_user: self.tracer_db.neon_db_clickhouse_user.clone(),
            clickhouse_password: self.tracer_db.neon_db_clickhouse_password.clone(),
        };

        neon_api::NeonApi::new(
            self.solana_url.clone(),
            self.neon_pubkey,
            self.neon_api.neon_config_pubkey,
            tracer_db_config,
            self.neon_api.max_tx_account_count,
            Some(CommitmentConfig {
                commitment: self.neon_api.simulation_commitment,
            }),
        )
    }
}

pub trait NeonConfigExt {
    // fn chain_name_for_id(&self, chain_id: u64) -> Option<&str>;
    fn chain_id_for_name(&self, name: &str) -> Option<u64>;
}

impl NeonConfigExt for GetConfigResponse {
    // fn chain_name_for_id(&self, chain_id: u64) -> Option<&str> {
    //     self.chains
    //         .iter()
    //         .find(|c| c.id == chain_id)
    //         .map(|chain| chain.name.as_ref())
    // }

    fn chain_id_for_name(&self, name: &str) -> Option<u64> {
        self.chains
            .iter()
            .find(|c| c.name == name)
            .map(|chain| chain.id)
    }
}
