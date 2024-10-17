use jsonrpsee::core::RpcResult;
use jsonrpsee::types::error::CALL_EXECUTION_FAILED_CODE;
use jsonrpsee::types::error::{INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG};
use jsonrpsee::types::error::{INVALID_PARAMS_CODE, INVALID_PARAMS_MSG};
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
    #[error("operator error: {0}")]
    Operator(#[from] operator::Error),
    #[error("operator error: {0}")]
    OperatorPool(#[from] operator_pool::Error),
    #[error("method not implemented")]
    Unimplemented,
    #[error("other error: {0}")]
    Other(anyhow::Error),
}

impl From<Error> for ErrorObjectOwned {
    fn from(error: Error) -> Self {
        tracing::error!(?error, "error encountered");
        match error {
            Error::DB(..) | Error::Parse(..) | Error::Other(..) => ErrorCode::InternalError.into(),
            Error::NeonApi(error) => error.into(),
            Error::Mempool(error) => error.into(),
            Error::Preflight(error) => error.into(),
            Error::Operator(error) => operator_into_jsonrpsee(error),
            Error::OperatorPool(error) => operator_pool_into_jsonrpsee(error),
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
    jsonrpsee_error(INVALID_PARAMS_CODE, INVALID_PARAMS_MSG, msg)
}

pub fn call_execution_failed(msg: impl Into<String>) -> ErrorObjectOwned {
    jsonrpsee_error(CALL_EXECUTION_FAILED_CODE, "Call execution failed", msg)
}

pub fn internal_error(msg: impl Into<String>) -> ErrorObjectOwned {
    jsonrpsee_error(INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, msg)
}

fn jsonrpsee_error(
    code: i32,
    default_msg: &'static str,
    msg: impl Into<String>,
) -> ErrorObjectOwned {
    let msg = msg.into();
    let msg = if msg.is_empty() {
        default_msg.into()
    } else {
        msg
    };
    ErrorObjectOwned::owned::<()>(code, msg, None)
}

fn operator_into_jsonrpsee(error: operator::Error) -> ErrorObjectOwned {
    match error {
        operator::Error::Signature(_) => call_execution_failed("Error signing message"),
        operator::Error::Load(_) => internal_error(""),
    }
}

fn operator_pool_into_jsonrpsee(error: operator_pool::Error) -> ErrorObjectOwned {
    use operator_pool::Error;

    match error {
        Error::UnknownOperator(address) => {
            call_execution_failed(format!("Unknown sender {address}"))
        }
        Error::Load(_) | Error::Executor(_) => internal_error(""),
        Error::Operator(error) => operator_into_jsonrpsee(error),
    }
}
