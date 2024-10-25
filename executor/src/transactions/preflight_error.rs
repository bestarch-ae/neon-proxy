use anyhow::{bail, Context, Result};

use solana_api::solana_api::{ClientError, ClientErrorKind};
use solana_api::solana_rpc_client_api::{request, response};

use super::ongoing::{OngoingTransaction, TxStage};
use super::{TransactionBuilder, MAX_COMPUTE_UNITS, MAX_HEAP_SIZE};

/// Solana preflight error kind.
#[derive(Debug)]
pub enum TxErrorKind {
    CuMeterExceeded,
    TxSizeExceeded,
    AltFail,
    BadExternalCall,
}

impl TxErrorKind {
    pub fn from_error(err: &ClientError, tx: &OngoingTransaction) -> Option<Self> {
        match err.kind {
            ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
                code: -32602,
                ref message,
                ..
            }) if message.contains("Transaction too large:") => {
                return Some(TxErrorKind::TxSizeExceeded)
            }
            _ => (),
        };

        if tx.is_alt() {
            return Some(Self::AltFail);
        }

        if extract_logs(&err.kind)
            .iter()
            .rfind(|log| log.contains("exceeded CUs meter at BPF instruction"))
            .is_some()
        {
            return Some(Self::CuMeterExceeded);
        }

        if tx.has_external_call_fail() {
            return Some(Self::BadExternalCall);
        }

        None
    }
}

fn extract_logs(err: &ClientErrorKind) -> &'_ [String] {
    match err {
        ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
            data:
                request::RpcResponseErrorData::SendTransactionPreflightFailure(
                    response::RpcSimulateTransactionResult {
                        logs: Some(ref logs),
                        ..
                    },
                ),
            ..
        }) => logs.as_slice(),
        _ => &[],
    }
}

