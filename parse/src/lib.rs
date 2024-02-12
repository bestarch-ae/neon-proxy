use thiserror::Error;

use common::evm_loader::types::Address;
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
    let mut txs = Vec::new();
    for (idx, ix) in tx.message.instructions().iter().enumerate() {
        let neon_tx = transaction::parse(&ix.data)?;
        tracing::info!("neon tx {:?}", neon_tx);
        if let Some(neon_tx) = neon_tx {
            txs.push((idx, neon_tx));
        }
    }
    let log_info = match log::parse(transaction.log_messages) {
        Ok(log) => log,
        Err(err) => panic!("log parsing error {:?}", err),
    };
    tracing::info!("log info {:?}", log_info);
    let mut tx_infos = Vec::new();
    for (idx, tx) in txs {
        let tx_info = NeonTxInfo {
            tx_type: 0,                                                        // TODO
            neon_signature: log_info.sig.map(hex::encode).unwrap_or_default(), // TODO
            from: Address::default(),                                          // TODO
            contract: None,                                                    // TODO
            transaction: tx,
            events: log_info.event_list.clone(), // TODO
            gas_used: log_info
                .ret
                .as_ref()
                .map(|r| r.gas_used)
                .unwrap_or_default(), // TODO
            sum_gas_used: log_info
                .ix
                .as_ref()
                .map(|i| i.total_gas_used)
                .unwrap_or_default(), // TODO:
            // unclear what this is
            sol_signature: String::default(), // TODO: should be in input?
            sol_slot: transaction.slot,
            sol_tx_idx: transaction.tx_idx,
            sol_ix_idx: idx as u64,
            sol_ix_inner_idx: 0, // TODO: what is this?
            status: log_info.ret.as_ref().map(|r| r.status).unwrap_or_default(), // TODO
            is_cancelled: log_info
                .ret
                .as_ref()
                .map(|r| r.is_canceled)
                .unwrap_or_default(), // TODO
            is_completed: false, // TODO: ???
        };
        tx_infos.push(tx_info);
    }
    Ok(tx_infos)
}
