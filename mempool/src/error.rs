use pyth_sdk_solana::PythError;
use reth_primitives::alloy_primitives::SignatureError;
use reth_primitives::Address;
use thiserror::Error;

use neon_api::NeonApiError;
use solana_api::solana_client::pubsub_client::PubsubClientError;
use solana_api::solana_rpc_client_api::client_error::Error as SolanaClientError;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("solana rpc client error: {0}")]
    SolanaRpcClientError(#[from] SolanaClientError),
    #[error("pyth error: {0}")]
    PythError(#[from] PythError),
    #[error("pubsub client error: {0}")]
    SolanaPubsubClientError(#[from] PubsubClientError),
    #[error("failed to send subscribe fn")]
    FailedToSendUnsubscribe,
    #[error("system time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("base token not found in pyth symbology: {0}")]
    BaseTokenNotFound(String),
    #[error("default token not found in pyth symbology: {0}")]
    DefaultTokenNotFound(String),
    #[error("token not found in pyth symbology: {0}")]
    TokenNotFound(String),
    #[error("already known")]
    AlreadyKnown,
    #[error("wrong chain id")]
    UnknownChainID,
    #[error("replacement transaction underpriced")]
    Underprice,
    #[error("cannot recover signer from tx: {0}")]
    SignaturesError(#[from] SignatureError),
    #[error("nonce too low")]
    NonceTooLow,
    #[error("nonce too high")]
    NonceTooHigh,
    #[error("unsupported tx type")]
    UnsupportedTxType,
    #[error("unknown sender: {0}")]
    UnknownSender(Address),
    #[error("neon api error: {0}")]
    NeonApiError(#[from] NeonApiError),
}

impl MempoolError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Underprice => -32000,
            Self::NonceTooLow => -32002,
            _ => jsonrpsee::types::ErrorCode::InternalError.code(),
        }
    }
}

impl From<MempoolError> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: MempoolError) -> Self {
        let error_code = value.error_code();
        Self::owned(error_code, value.to_string(), None::<String>)
    }
}
