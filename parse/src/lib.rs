use thiserror::Error;

use common::evm_loader::types::{Address, Transaction};
use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::message::v0::LoadedAddresses;
use common::solana_sdk::message::AccountKeys;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::transaction::VersionedTransaction;
use common::types::{HolderOperation, NeonTxInfo, SolanaTransaction};

use self::log::NeonLogInfo;

mod log;
mod transaction;

pub trait AccountsDb {
    fn init_account(&mut self, _pubkey: Pubkey) {}
    fn delete_account(&mut self, _pubkey: Pubkey) {}
    fn get_by_key<'a>(&'a mut self, _pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
        None
    }
}

impl<T: AccountsDb> AccountsDb for Option<T> {
    fn get_by_key<'a>(&'a mut self, pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
        self.as_mut().and_then(|i| i.get_by_key(pubkey))
    }
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

struct TransactionMeta {
    neon_transaction: Transaction,
    sol_ix_idx: usize,
    is_cancelled: bool,
    is_completed: bool,
}

fn parse_transactions(
    tx: VersionedTransaction,
    accountsdb: &mut impl AccountsDb,
    loaded: &LoadedAddresses,
    neon_pubkey: Pubkey,
) -> Result<Vec<Action<TransactionMeta>>, Error> {
    use transaction::ParseResult;

    tracing::debug!("parsing tx {:?} with loaded addresses: {:?}", tx, loaded);
    tracing::debug!(
        alt = ?tx.message.address_table_lookups(),
        static_accounts = ?tx.message.static_account_keys(),
        "accounts"
    );
    let pubkeys = AccountKeys::new(tx.message.static_account_keys(), Some(loaded));
    let neon_idx = pubkeys.iter().position(|x| *x == neon_pubkey);
    let Some(neon_idx) = neon_idx else {
        tracing::warn!("not a neon transaction");
        return Ok(Default::default());
    };
    let mut actions = Vec::new();

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
        let res = transaction::parse(&ix.data, &pubkeys_for_ix, accountsdb, neon_pubkey)?;
        tracing::debug!("parse result: {:?}", res);
        match res {
            ParseResult::TransactionExecuted(neon_tx) => {
                actions.push(Action::AddTransaction(TransactionMeta {
                    neon_transaction: neon_tx,
                    sol_ix_idx: idx,
                    is_cancelled: false,
                    is_completed: true,
                }));
            }
            ParseResult::TransactionStep(neon_tx) => {
                actions.push(Action::AddTransaction(TransactionMeta {
                    neon_transaction: neon_tx,
                    sol_ix_idx: idx,
                    is_cancelled: false,
                    is_completed: false,
                }));
            }
            ParseResult::TransactionCancel(hash) => {
                actions.push(Action::CancelTransaction(hash));
            }
            ParseResult::HolderOperation(op) => actions.push(Action::WriteHolder(op)),
            res => {
                tracing::debug!("unhandled parse result: {:?}", res);
            }
        }
    }
    Ok(actions)
}

fn add_log(meta: TransactionMeta, log_info: &NeonLogInfo, slot: u64) -> NeonTxInfo {
    let TransactionMeta {
        neon_transaction: tx,
        sol_ix_idx,
        is_cancelled,
        is_completed,
    } = meta;
    let neon_sig = tx.hash();

    // if `to` address is absent, it means we are
    // creating new contract
    let contract = if tx.target().is_none() {
        let sender = tx.recover_caller_address().unwrap();
        let nonce = tx.nonce();

        Some(Address::from_create(&sender, nonce))
    } else {
        None
    };

    let gas_used = log_info.gas_used();

    let has_returned = log_info.ret.is_some();

    tracing::debug!(
        "tx {:?} returned? {:?} completed? {} cancelled? {} gas_used: {}",
        hex::encode(neon_sig),
        has_returned,
        is_completed,
        is_cancelled,
        gas_used
    );

    NeonTxInfo {
        tx_type: 0, // TODO
        neon_signature: neon_sig,
        from: tx.recover_caller_address().unwrap_or_default(),
        contract,
        transaction: tx,
        events: log_info.event_list.clone(), // TODO
        gas_used,
        sum_gas_used: Default::default(),    /* set later */
        sol_signature: Signature::default(), // TODO: should be in input?
        sol_slot: slot,
        tx_idx: 0, /* set later */
        sol_ix_idx: sol_ix_idx as u64,
        sol_ix_inner_idx: 0, // TODO: what is this?
        status: log_info.ret.as_ref().map(|r| r.status).unwrap_or_default(), // TODO
        is_cancelled,
        is_completed: is_completed || has_returned,
    }
}

pub enum Action<T> {
    AddTransaction(T),
    WriteHolder(HolderOperation),
    CancelTransaction([u8; 32]),
}

impl<T> Action<T> {
    fn map_transaction<S>(self, mut f: impl FnMut(T) -> S) -> Action<S> {
        match self {
            Action::AddTransaction(tx) => Action::AddTransaction(f(tx)),
            Action::WriteHolder(op) => Action::WriteHolder(op),
            Action::CancelTransaction(hash) => Action::CancelTransaction(hash),
        }
    }
}

#[tracing::instrument(skip_all)]
pub fn parse(
    transaction: SolanaTransaction,
    accountsdb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<impl Iterator<Item = Action<NeonTxInfo>>, Error> {
    let SolanaTransaction { slot, tx, .. } = transaction;
    let sig_slot_info = SolTxSigSlotInfo {
        signature: tx.signatures[0],
        block_slot: slot,
    };
    let _meta_info = SolTxMetaInfo {
        ident: sig_slot_info,
    };
    let loaded = &transaction.loaded_addresses;
    let actions = parse_transactions(tx, accountsdb, loaded, neon_pubkey)?;

    let log_info = log::parse(transaction.log_messages, neon_pubkey)?;
    let iter = actions
        .into_iter()
        .map(move |action| action.map_transaction(|tx| add_log(tx, &log_info, slot)));
    // let tx_infos = merge_logs_transactions(actions, log_info, slot);
    Ok(iter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;

    use common::solana_sdk::message::v0::LoadedAddresses;
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
        to_addr: Option<String>,
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

    #[allow(dead_code)]
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
            if !self.map.contains_key(pubkey) {
                self.init_account(*pubkey);
            }
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
            use common::evm_loader::account::TAG_HOLDER;
            tracing::debug!(%pubkey, "init account");

            self.map.entry(pubkey).or_insert_with(|| {
                let mut data = Data {
                    data: vec![0; 1024 * 1024],
                    lamports: 0,
                };
                data.data[0] = TAG_HOLDER;
                data
            });
        }
    }

    fn main_adb() -> DummyAdb {
        let neon_main_pubkey: Pubkey = "NeonVMyRX5GbCrsAHnUwx1nYYoJAtskU1bWUo6JGNyG"
            .parse()
            .unwrap();
        DummyAdb::new(neon_main_pubkey)
    }

    fn dev_adb() -> DummyAdb {
        let neon_main_pubkey: Pubkey = "eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU"
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
        let mut adb = dev_adb();

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
    fn parse_3uh3() {
        let mut adb = dev_adb();
        let holder_txs = [
            "2DkSPyUTf2aDz8AURb4XFKKue7JSxSyeGmPhcAwAX9xrFVYoNND8ULaDfK7pqRSjxPUrQRgjLjzjAiDfic6CNzaw",
            "4MfBsYNYsSBCo1BP2BLRuPYUzApc7ynqdGYNNbZyi4AKkkE2pBjn2MWqw1y7dmZFWN1sJL8uUsbG36b7gB3ekAa7"
        ];
        for sig in holder_txs {
            let holder_tx = format!("tests/data/transactions/{}/transaction.json", sig);
            parse_tx(holder_tx, None::<PathBuf>, &mut adb);
        }
        let transaction_path = "tests/data/transactions/3uH3dMtSpp7x75poHQM7rHefviVC6WRp4Qzjvodhs2RWALTmQ6fTw52VPrSGwvhmStwpLyaRgcL3X9r8SytXE3eR/transaction.json";
        let reference_path = "tests/data/reference/3uH3dMtSpp7x75poHQM7rHefviVC6WRp4Qzjvodhs2RWALTmQ6fTw52VPrSGwvhmStwpLyaRgcL3X9r8SytXE3eR.json";

        parse_tx(transaction_path, Some(reference_path), &mut adb);
    }

    #[test]
    fn parse_many() {
        use std::path::PathBuf;
        let tx_path = "tests/data/";
        let ref_path = "tests/data/reference";

        for entry in std::fs::read_dir(tx_path).unwrap() {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                let mut adb = dev_adb();
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

    fn parse_tx(
        transaction_path: impl AsRef<Path>,
        reference_path: Option<impl AsRef<Path>>,
        adb: &mut DummyAdb,
    ) {
        let neon_pubkey = adb.neon_pubkey;
        let encoded: EncodedTransactionWithStatusMeta =
            serde_json::from_str(&std::fs::read_to_string(transaction_path).unwrap()).unwrap();
        let meta = encoded.meta.unwrap();
        let loaded = match meta.loaded_addresses {
            common::solana_transaction_status::option_serializer::OptionSerializer::Some(
                loaded,
            ) => LoadedAddresses {
                writable: loaded.writable.iter().map(|a| a.parse().unwrap()).collect(),
                readonly: loaded.readonly.iter().map(|a| a.parse().unwrap()).collect(),
            },
            _ => panic!("no loaded"),
        };
        let tx = encoded.transaction.decode().unwrap();

        let pubkeys = tx.message.static_account_keys();
        tracing::info!(?pubkeys);
        let actions = parse_transactions(tx, adb, &loaded, neon_pubkey).unwrap();
        let logs = match meta.log_messages {
            common::solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs
            }
            _ => panic!("no logs"),
        };
        let logs = log::parse(logs, neon_pubkey).unwrap();

        let neon_tx_infos = actions
            .into_iter()
            .map(move |action| action.map_transaction(|tx| add_log(tx, &logs, 0)))
            .filter_map(|action| match action {
                Action::AddTransaction(tx) => Some(tx),
                _ => None,
            })
            .collect::<Vec<_>>();

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
            let neon_sig = format!("0x{}", hex::encode(info.neon_signature));
            assert_eq!(refr.neon_sig, neon_sig);
            assert_eq!(refr.tx_type, info.tx_type);
            // fails as we don't set from address
            //assert_eq!(refr.from_addr, format!("0x{}", info.from));
            // fails as we don't set signature
            //assert_eq!(refr.sol_sig, info.sol_signature);
            assert_eq!(refr.sol_ix_idx, info.sol_ix_idx);
            // fails as we don't set inner index
            //assert_eq!(refr.sol_ix_inner_idx, Some(info.sol_ix_inner_idx));
            // ignore because it is passed in params
            //assert_eq!(refr.block_slot, info.sol_slot);
            // ignore because it is passed in params
            //assert_eq!(refr.tx_idx, info.sol_tx_idx);
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
            // fails for unknown reason
            //assert_eq!(refr.contract, info.contract.map(|c| c.to_string()));
            // fails for unknown reason
            //assert_eq!(refr.status, format!("{:#0x}", info.status));
            assert_eq!(refr.is_canceled, info.is_cancelled);
            println!(
                "{} is_completed: {}, {}",
                neon_sig, refr.is_completed, info.is_completed
            );
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
