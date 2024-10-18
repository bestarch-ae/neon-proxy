use std::path::PathBuf;
use std::str::FromStr;

use clap::{ArgGroup, Parser};

use common::solana_sdk::commitment_config::CommitmentLevel;
use common::solana_sdk::pubkey::Pubkey;
use mempool::GasPriceCalculatorConfig;

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

#[derive(Parser)]
#[command(group(
    ArgGroup::new("required_group")
    .args(&["symbology_path", "const_gas_price", "pyth_mapping_addr"])
    .required(true)
))]
pub struct Args {
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
        short('c'),
        long,
        value_name = "CONFIG_PUBKEY",
        default_value = "BMp6gEnveANdvSvspESJUrNczuHz1GF5UQKjVLCkAZih"
    )]
    pub neon_config_pubkey: Pubkey,

    #[arg(
        short('u'),
        long,
        default_value = "https://api.mainnet-beta.solana.com",
        value_name = "URL"
    )]
    /// Solana endpoint
    pub solana_url: String,

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

    #[arg(long, env, value_delimiter = ';')]
    /// Tracer db urls, comma separated
    pub neon_db_clickhouse_urls: Vec<String>,

    #[arg(long, env)]
    /// Trace db user
    pub neon_db_clickhouse_user: Option<String>,

    #[arg(long, env)]
    /// Trace db password
    pub neon_db_clickhouse_password: Option<String>,

    #[arg(long, env, default_value = "245022926")]
    // Neon chain id
    pub chain_id: u64,

    #[group(flatten)]
    pub operator: operator_pool::Config,

    #[group(flatten)]
    pub gas_prices_calculator_config: GasPriceCalculatorConfig,

    #[arg(long, env, default_value = "SOL")]
    /// Chain token name
    pub chain_token_name: String,

    #[arg(long, env, default_value = "NEON")]
    /// Default token name
    pub default_token_name: String,

    #[arg(long, env)]
    pub symbology_path: Option<PathBuf>,

    #[arg(long, env, default_value = "64")]
    // Max tx account count
    pub max_tx_account_count: usize,

    #[arg(long, env, default_value = "finalized")]
    pub simulation_commitment: CommitmentLevel,

    #[arg(long)]
    /// Log format, either json or plain
    pub log_format: Option<LogFormat>,

    // pub todo: pick a default value
    #[arg(long, env, default_value = "100")]
    pub mp_capacity: usize,

    // pub todo: pick a default value
    #[arg(long, env, default_value = "0.9")]
    pub mp_capacity_high_watermark: f64,

    // pub todo: pick a default value
    #[arg(long, env, default_value = "3600")]
    pub mp_eviction_timeout_sec: u64,
}
