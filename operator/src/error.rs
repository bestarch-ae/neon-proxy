use reth_primitives::Address;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown operator")]
    UnknownOperator(Address),
    #[error("signature error: {0}")]
    Signature(#[from] alloy_signer::Error),
    #[error("load error: {0}")]
    Load(anyhow::Error),
}
