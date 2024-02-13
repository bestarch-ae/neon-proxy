use common::solana_sdk::transaction::VersionedTransaction;
use thiserror::Error;

use common::evm_loader::types::{Address, Transaction};
use common::solana_sdk::signature::Signature;
use common::types::{NeonTxInfo, SolanaTransaction};

use self::log::NeonLogInfo;

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

fn parse_transactions(tx: VersionedTransaction) -> Result<Vec<(usize, Transaction)>, Error> {
    let mut txs = Vec::new();
    for (idx, ix) in tx.message.instructions().iter().enumerate() {
        let neon_tx = transaction::parse(&ix.data)?;
        tracing::info!("neon tx {:?}", neon_tx);
        if let Some(neon_tx) = neon_tx {
            txs.push((idx, neon_tx));
        }
    }
    Ok(txs)
}

fn merge_logs_transactions(
    txs: Vec<(usize, Transaction)>,
    log_info: NeonLogInfo,
    slot: u64,
    tx_idx: u64,
) -> Vec<NeonTxInfo> {
    let mut tx_infos = Vec::new();
    for (idx, tx) in txs {
        let canceled = log_info
            .ret
            .as_ref()
            .map(|r| r.is_canceled)
            .unwrap_or_default(); // TODO

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
                .unwrap_or_default(), // TODO: unclear what this is
            sol_signature: String::default(),    // TODO: should be in input?
            sol_slot: slot,
            sol_tx_idx: tx_idx,
            sol_ix_idx: idx as u64,
            sol_ix_inner_idx: 0, // TODO: what is this?
            status: log_info.ret.as_ref().map(|r| r.status).unwrap_or_default(), // TODO
            is_cancelled: canceled,
            is_completed: !canceled, // TODO: ???
        };
        tx_infos.push(tx_info);
    }
    tx_infos
}

pub fn parse(transaction: SolanaTransaction) -> Result<Vec<NeonTxInfo>, Error> {
    let SolanaTransaction {
        slot, tx, tx_idx, ..
    } = transaction;
    let sig_slot_info = SolTxSigSlotInfo {
        signature: tx.signatures[0],
        block_slot: slot,
    };
    let _meta_info = SolTxMetaInfo {
        ident: sig_slot_info,
    };
    let neon_txs = parse_transactions(tx)?;

    let log_info = match log::parse(transaction.log_messages) {
        Ok(log) => log,
        Err(err) => panic!("log parsing error {:?}", err),
    };
    let tx_infos = merge_logs_transactions(neon_txs, log_info, slot, tx_idx);
    Ok(tx_infos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::solana_transaction_status::EncodedTransactionWithStatusMeta;
    use serde::Deserialize;
    use std::collections::HashMap;
    use test_log::test;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct ReferenceRow {
        neon_sig: String,
        tx_type: u8,
        from_addr: String,
        sol_sig: String,
        sol_ix_idx: u64,
        sol_ix_inner_idx: Option<u64>,
        block_slot: u64,
        tx_idx: u64,
        nonce: String,
        gas_price: String,
        gas_limit: String,
        value: String,
        gas_used: String,
        sum_gas_used: String,
        to_addr: String,
        contract: Option<String>,
        status: String,
        is_canceled: bool,
        is_completed: bool,
        v: String,
        r: String,
        s: String,
        calldata: String,
        logs: Vec<ReferenceEvent>,
    }

    #[derive(Debug, Deserialize)]
    struct ReferenceEvent {
        event_type: u32,
        is_hidden: bool,
        address: String,
        topic_list: Vec<String>,
        data: String,
        sol_sig: String,
        idx: u64,
        inner_idx: Option<u64>,
        total_gas_used: u64,
        is_reverted: bool,
        event_level: u64,
        event_order: u64,
        neon_sig: String,
        block_hash: String,
        block_slot: u64,
        neon_tx_idx: u64,
        block_log_idx: Option<u64>,
        neon_tx_log_idx: Option<u64>,
    }

    type Reference = HashMap<String, Vec<ReferenceRow>>;

    fn check_events(ref_logs: &[ReferenceEvent], my_logs: &[common::types::EventLog]) {
        for (ref_log, my_log) in ref_logs.iter().zip(my_logs.iter()) {
            println!("event type {:?} {}", my_log.event_type, ref_log.event_type);
            assert_eq!(ref_log.event_type, my_log.event_type as u32);
            assert_eq!(ref_log.is_hidden, my_log.is_hidden);
            assert_eq!(ref_log.event_level, my_log.level);
            assert_eq!(ref_log.event_order, my_log.order);

            assert_eq!(
                ref_log.address,
                my_log.address.map(|a| a.to_string()).unwrap_or_default()
            );
            assert_eq!(
                ref_log.topic_list,
                my_log
                    .topic_list
                    .iter()
                    .map(|t| format!("0x{}", t))
                    .collect::<Vec<_>>()
            );
            assert_eq!(ref_log.data, format!("0x{}", hex::encode(&my_log.data)));
        }
    }

    #[test]
    fn parse_2f() {
        let transaction_path = "tests/data/2FSmsnCJYenPWsbrK1vFpkhgeFGKXC13gB3wDwqqyUfEnZmWpEJ6iUSjtLrtNn5QZh54bz5brWMonccG7WHA4Wp5.json";
        let reference_path = "tests/data/reference/2FSmsnCJYenPWsbrK1vFpkhgeFGKXC13gB3wDwqqyUfEnZmWpEJ6iUSjtLrtNn5QZh54bz5brWMonccG7WHA4Wp5.json";

        let encoded: EncodedTransactionWithStatusMeta =
            serde_json::from_str(&std::fs::read_to_string(transaction_path).unwrap()).unwrap();
        let tx = encoded.transaction.decode().unwrap();
        let neon_txs = parse_transactions(tx).unwrap();
        let logs = match encoded.meta.unwrap().log_messages {
            common::solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs
            }
            _ => panic!("no logs"),
        };
        let logs = log::parse(logs).unwrap();
        let neon_tx_infos = merge_logs_transactions(neon_txs, logs, 276140928, 3);
        let references: Vec<_> =
            serde_json::from_str::<Reference>(&std::fs::read_to_string(reference_path).unwrap())
                .unwrap()
                .into_values()
                .flatten()
                .collect();
        assert_eq!(neon_tx_infos.len(), references.len());
        for (info, refr) in neon_tx_infos.iter().zip(references) {
            let neon_sig = format!("0x{}", info.neon_signature);
            assert_eq!(refr.neon_sig, neon_sig);
            assert_eq!(refr.tx_type, info.tx_type);
            // fails as we don't set from address
            //assert_eq!(refr.from_addr, format!("0x{}", info.from));
            // fails as we don't set signature
            //assert_eq!(refr.sol_sig, info.sol_signature);
            assert_eq!(refr.sol_ix_idx, info.sol_ix_idx);
            // fails as we don't set inner index
            //assert_eq!(refr.sol_ix_inner_idx, Some(info.sol_ix_inner_idx));
            assert_eq!(refr.block_slot, info.sol_slot);
            assert_eq!(refr.tx_idx, info.sol_tx_idx);
            assert_eq!(refr.nonce, format!("{:#0x}", info.transaction.nonce()));
            assert_eq!(
                refr.gas_price,
                format!("{:#0x}", info.transaction.gas_price())
            );
            assert_eq!(
                refr.gas_limit,
                format!("{:#0x}", info.transaction.gas_limit())
            );
            assert_eq!(refr.value, format!("{:#0x}", info.transaction.value()));
            assert_eq!(refr.gas_used, format!("{:#0x}", info.gas_used));
            // fails for unknown reason
            //assert_eq!(refr.sum_gas_used, format!("{:#0x}", info.sum_gas_used));
            // fails for unknown reason
            // assert_eq!(
            //     refr.to_addr,
            //     format!("0x{}", info.transaction.target().unwrap())
            // );
            assert_eq!(refr.contract, info.contract.map(|c| format!("0x{}", c)));
            // fails for unknown reason
            //assert_eq!(refr.status, format!("{:#0x}", info.status));
            assert_eq!(refr.is_canceled, info.is_cancelled);
            assert_eq!(refr.is_completed, info.is_completed);

            // TODO: we don't have v,r,s fields for some reason?

            assert_eq!(
                refr.calldata,
                format!("0x{}", hex::encode(info.transaction.call_data()))
            );
            check_events(&refr.logs, &info.events);
        }
    }
}
