mod gas_price_calculator;
mod gas_prices;
mod pyth_price_collector;

use pyth_sdk_solana::PythError;
use thiserror::Error;

use solana_api::solana_client::pubsub_client::PubsubClientError;
use solana_api::solana_rpc_client_api::client_error::Error as SolanaClientError;

pub use gas_price_calculator::GasPriceCalculatorConfig;
pub use gas_prices::GasPrices;
pub use pyth_price_collector::pyth_collect_symbology;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("solana rpc client error: {0}")]
    SolanaRpcClientError(#[from] SolanaClientError),
    #[error("pyth error: {0}")]
    PythError(#[from] PythError),
    #[error("pubsub client error: {0}")]
    SolanaPubsubClientError(#[from] PubsubClientError),
    #[error("tokio send error: {0}")]
    TokioSendError(String),
    #[error("system time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("base token not found in pyth symbology: {0}")]
    BaseTokenNotFound(String),
    #[error("default token not found in pyth symbology: {0}")]
    DefaultTokenNotFound(String),
}
