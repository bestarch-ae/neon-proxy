use common::evm_loader::error::Error as NeonError;
use common::evm_loader::types::Transaction;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("neon error")]
    Neon(#[from] NeonError),
    #[error("invalid instruction")]
    InvalidInstruction,
    #[error("not implemented")]
    NotImplemented,
}

pub fn parse(bytes: &[u8]) -> Result<Option<Transaction>, Error> {
    let tag = bytes.first().ok_or_else(|| Error::InvalidInstruction)?;
    match tag {
        0x31 => {
            tracing::info!("found deposit instruction");
        }
        0x32 => {
            tracing::info!("found execute from instruction");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            return Ok(Some(tx));
        }
        _ => {
            tracing::warn!("not implemented tag: {}", tag);
        }
    }
    Ok(None) // TODO
}

fn decode_execute_from_ix(bytes: &[u8]) -> Result<Transaction, Error> {
    let _treasure_index = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let tx = Transaction::from_rlp(&bytes[4..])?;
    Ok(tx)
}
