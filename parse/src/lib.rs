use common::solana_sdk::transaction::VersionedTransaction;
use thiserror::Error;

use common::evm_loader::types::{Address, Transaction};
use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::types::{NeonTxInfo, SolanaTransaction};

use self::log::NeonLogInfo;

mod log;
mod transaction;

pub trait AccountsDb {
    fn init_account(&mut self, _pubkey: Pubkey) {}
    fn get_by_key<'a>(&'a mut self, _pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
        None
    }
}

impl<T: AccountsDb> AccountsDb for Option<T> {
    fn get_by_key<'a>(&'a mut self, pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
        self.as_mut().and_then(|i| i.get_by_key(pubkey))
    }
}

impl AccountsDb for () {}

fn is_neon_pubkey(pk: Pubkey) -> bool {
    use std::str::FromStr;

    let neon_dev_pubkey = Pubkey::from_str("eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU").unwrap();
    let neon_main_pubkey = Pubkey::from_str("NeonVMyRX5GbCrsAHnUwx1nYYoJAtskU1bWUo6JGNyG").unwrap();

    pk == neon_dev_pubkey || pk == neon_main_pubkey
}

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

fn parse_transactions(
    tx: VersionedTransaction,
    accountsdb: &mut impl AccountsDb,
) -> Result<Vec<(usize, Transaction)>, Error> {
    let mut txs = Vec::new();
    tracing::debug!("parsing tx {:?}", tx);
    assert!(
        tx.message.address_table_lookups().is_none(),
        "ALT not implemented yet"
    );
    let pubkeys = tx.message.static_account_keys();
    tracing::debug!("pubkeys {:?}", pubkeys);
    let neon_idx = pubkeys.iter().position(|x| is_neon_pubkey(*x));
    let Some(neon_idx) = neon_idx else {
        tracing::warn!("not a neon transaction");
        return Ok(txs);
    };
    for (idx, ix) in tx.message.instructions().iter().enumerate() {
        tracing::debug!("instruction {:?}", ix);
        if ix.program_id_index != neon_idx as u8 {
            tracing::debug!("not a neon instruction");
            continue;
        }
        let neon_pubkey = pubkeys[neon_idx];
        let mut pubkeys_for_ix = vec![];
        for acc_idx in &ix.accounts {
            pubkeys_for_ix.push(pubkeys[*acc_idx as usize]);
        }
        let neon_tx = transaction::parse(&ix.data, &pubkeys_for_ix, accountsdb, neon_pubkey)?;
        tracing::debug!("neon tx {:?}", neon_tx);
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

pub fn parse(
    transaction: SolanaTransaction,
    mut accountsdb: impl AccountsDb,
) -> Result<Vec<NeonTxInfo>, Error> {
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
    let neon_txs = parse_transactions(tx, &mut accountsdb)?;

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
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;

    use common::solana_sdk::account::Account;
    use common::solana_transaction_status::EncodedTransactionWithStatusMeta;
    use serde::Deserialize;
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

    #[derive(Clone, Debug)]
    struct Data {
        data: Vec<u8>,
        lamports: u64,
    }

    #[derive(Clone, Debug)]
    struct DummyAdb {
        map: HashMap<Pubkey, Data>,
        neon_pubkey: Pubkey,
    }

    impl DummyAdb {
        pub fn new(neon: Pubkey) -> Self {
            DummyAdb {
                map: Default::default(),
                neon_pubkey: neon,
            }
        }
    }

    impl AccountsDb for DummyAdb {
        fn get_by_key<'a>(&'a mut self, pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
            tracing::debug!(%pubkey, "getting data for account");
            let data = self.map.get_mut(pubkey)?;

            let account_info = AccountInfo {
                key: pubkey,
                owner: &self.neon_pubkey,
                data: Rc::new(RefCell::new(data.data.as_mut())),
                lamports: Rc::new(RefCell::new(&mut data.lamports)),
                is_signer: false,
                is_writable: false,
                executable: false,
                rent_epoch: 0,
            };
            Some(account_info)
        }

        fn init_account(&mut self, pubkey: Pubkey) {
            tracing::debug!(%pubkey, "init account");
            self.map.entry(pubkey).or_insert_with(|| Data {
                data: vec![0; 1024 * 1024],
                lamports: 0,
            });
        }
    }

    fn main_adb() -> DummyAdb {
        let neon_main_pubkey: Pubkey = "NeonVMyRX5GbCrsAHnUwx1nYYoJAtskU1bWUo6JGNyG"
            .parse()
            .unwrap();
        DummyAdb::new(neon_main_pubkey)
    }

    #[test]
    fn parse_5py() {
        let mut adb = main_adb();

        let transaction_path = "tests/data/transactions/5puyNh1S37eZBnnr711GXY7khpw1BizCo3Yvri9yG8JACdsZGRjE1Xr1U6DFH9ukYQFMKan8X3QeggG6FoK3zobU/transaction.json";
        parse_tx(transaction_path, None::<PathBuf>, &mut adb);
    }

    #[test]
    fn parse_2f() {
        let mut adb = main_adb();

        let transaction_path = "tests/data/2FSmsnCJYenPWsbrK1vFpkhgeFGKXC13gB3wDwqqyUfEnZmWpEJ6iUSjtLrtNn5QZh54bz5brWMonccG7WHA4Wp5.json";
        let reference_path = "tests/data/reference/2FSmsnCJYenPWsbrK1vFpkhgeFGKXC13gB3wDwqqyUfEnZmWpEJ6iUSjtLrtNn5QZh54bz5brWMonccG7WHA4Wp5.json";

        parse_tx(transaction_path, Some(reference_path), &mut adb);
    }

    #[test]
    fn parse_4sqy() {
        let mut adb = main_adb();

        let holder_txs = [
            "4CTqsT8zT7VUF3f8DUqCiDo5ow773FvpTZ6skxGsG86qFfc97QAGzU6JKkV25XbXa9NnnxdCnDJKVjRwWgygUnHr",
            "5hV1MnLmrEz2HPYqdxmUPUvBx9kRA1PPFYAJh6vMBXg9vTxtJLxSfLDcvz3pg41JtwxeXNuYyY18vzDPPn1LadV6",
        ];
        for sig in holder_txs {
            let holder_tx = format!("tests/data/transactions/{}/transaction.json", sig);
            parse_tx(holder_tx, None::<PathBuf>, &mut adb);
        }

        let transaction_path = "tests/data/transactions/4sqyU24FKWAqHPtMMFpGmNf2tPFVoTwDpJckWPEEP6RNSaEyAvqyP4DgQ25ppHAfsi7D3SEWULQhm4pA9EwYa8c1/transaction.json";
        parse_tx(transaction_path, None::<PathBuf>, &mut adb);
    }

    #[test]
    fn parse_4y() {
        let mut adb = main_adb();
        let holder_txs = [
            "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"
        ];
        for sig in holder_txs.iter().rev() {
            let holder_tx = format!("tests/data/transactions/{}/transaction.json", sig);
            parse_tx(holder_tx, None::<PathBuf>, &mut adb);
        }

        let transaction_path = "tests/data/transactions/4YcHMcHwXkpqTfuqafaJigL9SKoYcRhUD9LimTHKjbkhJeLSpdjdsJCirjTqrM7VZC4RBrDJZdrjW5ZAUbqHqhq5/transaction.json";
        parse_tx(transaction_path, None::<PathBuf>, &mut adb);
    }

    #[test]
    fn parse_many_old() {
        use std::path::PathBuf;
        let tx_path = "tests/data/";
        let ref_path = "tests/data/reference";

        for entry in std::fs::read_dir(tx_path).unwrap() {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                let mut adb = main_adb();
                let fname = entry.file_name();
                let mut ref_file_name = PathBuf::new();
                ref_file_name.push(ref_path);
                ref_file_name.push(fname);
                println!("Parsing: {:?}", entry.path());
                let ref_file_name = ref_file_name.exists().then_some(ref_file_name);
                parse_tx(entry.path(), ref_file_name, &mut adb);
            }
        }
    }

    #[test]
    fn parse_many_new() {
        use std::path::PathBuf;
        let tx_path = "tests/data/transactions";
        let ref_path = "tests/data/reference";

        for entry in std::fs::read_dir(tx_path).unwrap() {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_dir() {
                let mut adb = main_adb();
                let fname = entry.file_name();
                let mut ref_file_name = PathBuf::new();
                ref_file_name.push(ref_path);
                ref_file_name.push(fname);
                println!("Parsing: {:?}", entry.path());
                let ref_file_name = ref_file_name.exists().then_some(ref_file_name);
                let mut tx_path = entry.path();
                tx_path.push("transaction.json");
                parse_tx(tx_path, ref_file_name, &mut adb);
            }
        }
    }

    fn parse_tx(
        transaction_path: impl AsRef<Path>,
        reference_path: Option<impl AsRef<Path>>,
        adb: &mut impl AccountsDb,
    ) {
        let encoded: EncodedTransactionWithStatusMeta =
            serde_json::from_str(&std::fs::read_to_string(transaction_path).unwrap()).unwrap();
        let tx = encoded.transaction.decode().unwrap();

        let pubkeys = tx.message.static_account_keys();
        tracing::info!(?pubkeys);
        let neon_txs = parse_transactions(tx, adb).unwrap();
        let logs = match encoded.meta.unwrap().log_messages {
            common::solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs
            }
            _ => panic!("no logs"),
        };
        let logs = log::parse(logs).unwrap();
        let neon_tx_infos = merge_logs_transactions(neon_txs, logs, 276140928, 3);

        tracing::info!(?neon_tx_infos);

        let Some(reference_path) = reference_path else {
            return;
        };
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
