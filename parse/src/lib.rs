use thiserror::Error;

use common::solana_sdk::signature::Signature;
use common::types::{NeonTxInfo, SolanaTransaction};

mod log;
mod transaction;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to decode solana")]
    Solana,
    #[error("Failed to decode logs")]
    Log(#[from] log::Error),
    #[error("Failed to decode transaction")]
    Transaction(#[from] transaction::Error),
}

#[allow(dead_code)] // TODO
#[derive(Debug)]
struct SolTxSigSlotInfo {
    pub signature: Signature,
    pub block_slot: u64,
}

#[allow(dead_code)] // TODO
#[derive(Debug)]
struct SolTxMetaInfo {
    pub ident: SolTxSigSlotInfo,
}

pub fn parse(transaction: SolanaTransaction) -> Result<Vec<NeonTxInfo>, Error> {
    let SolanaTransaction { slot, tx, .. } = transaction;
    let sig_slot_info = SolTxSigSlotInfo {
        signature: tx.signatures[0],
        block_slot: slot,
    };
    let _meta_info = SolTxMetaInfo {
        ident: sig_slot_info,
    };
    for ix in tx.message.instructions() {
        let neon_tx = transaction::parse(&ix.data)?;
        tracing::info!("neon tx {:?}", neon_tx);
    }
    let log_info = match log::parse(transaction.log_messages) {
        Ok(log) => log,
        Err(err) => panic!("log parsing error {:?}", err),
    };
    tracing::info!("log info {:?}", log_info);
    Ok(Vec::new())
}
