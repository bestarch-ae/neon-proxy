use jsonrpsee::core::RpcResult;
use jsonrpsee::types::error::{CALL_EXECUTION_FAILED_CODE, INVALID_PARAMS_CODE};
use jsonrpsee::types::{ErrorCode, ErrorObjectOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    DB(#[from] db::Error),
    #[error("parse error: {0}")]
    Parse(#[from] anyhow::Error),
    #[error("neon api error: {0}")]
    NeonApi(#[from] neon_api::NeonApiError),
    #[error("mempool error: {0}")]
    Mempool(#[from] mempool::MempoolError),
    #[error("mempool error: {0}")]
    Preflight(#[from] mempool::PreFlightError),
    #[error("method not implemented")]
    Unimplemented,
    #[error("other error: {0}")]
    Other(anyhow::Error),
}

impl From<Error> for jsonrpsee::types::ErrorObjectOwned {
    fn from(error: Error) -> Self {
        tracing::error!(?error, "error encountered");
        match error {
            Error::DB(..) | Error::Parse(..) | Error::Other(..) => ErrorCode::InternalError.into(),
            Error::NeonApi(error) => error.into(),
            Error::Mempool(error) => error.into(),
            Error::Preflight(error) => error.into(),
            Error::Unimplemented => unimplemented::<()>().unwrap_err(),
        }
    }
}

pub fn unimplemented<T>() -> RpcResult<T> {
    Err(ErrorObjectOwned::borrowed(
        ErrorCode::MethodNotFound.code(),
        "method not implemented",
        None,
    ))
}

pub fn invalid_params(msg: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned::<()>(INVALID_PARAMS_CODE, msg, None)
}

pub fn call_execution_failed(msg: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned::<()>(CALL_EXECUTION_FAILED_CODE, msg, None)
}
