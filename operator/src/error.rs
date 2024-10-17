use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("signature error: {0}")]
    Signature(#[from] alloy_signer::Error),
    #[error("load error: {0}")]
    Load(anyhow::Error),
}
