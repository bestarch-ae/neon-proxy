use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::rc::Rc;

use anyhow::{Context, Error};
use clap::Parser;
use common::solana_sdk::message::v0::LoadedAddresses;
use common::types::{EventLog, NeonTxInfo, SolanaTransaction};

use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransactionWithStatusMeta,
};

use neon_parse::parse;
use neon_parse::AccountsDb;
use serde::Serialize;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    transaction_path: String,
    previous_transactions: Vec<String>,
    #[arg(
        short,
        long,
        default_value = "NeonVMyRX5GbCrsAHnUwx1nYYoJAtskU1bWUo6JGNyG"
    )]
    neon_pubkey: Pubkey,
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

#[derive(Debug, Serialize)]
struct Event {
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

impl From<EventLog> for Event {
    fn from(val: EventLog) -> Self {
        Event {
            event_type: val.event_type as u32,
            is_hidden: val.is_hidden,
            address: val.address.map(|a| a.to_string()).unwrap_or_default(),
            topic_list: val
                .topic_list
                .into_iter()
                .map(|x| format!("0x{}", x))
                .collect(),
            data: format!("0x{}", hex::encode(val.data)),
            // TODO: everything below
            sol_sig: String::new(),
            idx: 0,
            inner_idx: None,
            total_gas_used: 0,
            is_reverted: false,
            event_level: val.level,
            event_order: val.order,
            neon_sig: format!("0x{}", 0),
            block_hash: format!("0x{}", 0),
            block_slot: 0,
            neon_tx_idx: 0,
            block_log_idx: None,
            neon_tx_log_idx: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct DbRow {
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
    logs: Vec<Event>,
}

impl From<NeonTxInfo> for DbRow {
    fn from(val: NeonTxInfo) -> Self {
        DbRow {
            neon_sig: format!("0x{}", val.neon_signature),
            tx_type: val.tx_type,
            from_addr: val.from.to_string(),
            sol_sig: String::new(),
            sol_ix_idx: val.sol_ix_idx,
            sol_ix_inner_idx: Some(val.sol_ix_inner_idx),
            block_slot: val.sol_slot,
            tx_idx: val.sol_tx_idx,
            nonce: format!("{:#0x}", val.transaction.nonce()),
            gas_price: format!("{:#0x}", val.transaction.gas_price()),
            gas_limit: format!("{:#0x}", val.transaction.gas_limit()),
            value: format!("{:#0x}", val.transaction.value()),
            gas_used: format!("{:#0x}", val.gas_used),
            sum_gas_used: format!("{:#0x}", val.sum_gas_used),
            to_addr: val
                .transaction
                .target()
                .map(|x| x.to_string())
                .unwrap_or_default(),
            contract: val.contract.map(|c| format!("0x{}", c)),
            status: format!("{:#0x}", val.status),
            is_canceled: val.is_cancelled,
            is_completed: val.is_completed,
            v: format!("{:#0x}", 0), // TODO: ???
            r: format!("{:#0x}", val.transaction.r()),
            s: format!("{:#0x}", val.transaction.s()),
            calldata: format!("0x{}", hex::encode(val.transaction.call_data())),
            logs: val.events.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[repr(transparent)]
struct Wrapped<T>(T);

impl From<EncodedTransactionWithStatusMeta> for Wrapped<SolanaTransaction> {
    fn from(value: EncodedTransactionWithStatusMeta) -> Self {
        let meta = value.meta.unwrap();
        let OptionSerializer::Some(log_messages) = meta.log_messages else {
            panic!("no logs");
        };
        let OptionSerializer::Some(loaded_addresses) = meta.loaded_addresses else {
            panic!("no loaded addresses")
        };
        let loaded_addresses = LoadedAddresses {
            writable: loaded_addresses
                .writable
                .into_iter()
                .map(|x| x.parse().unwrap())
                .collect(),
            readonly: loaded_addresses
                .readonly
                .into_iter()
                .map(|x| x.parse().unwrap())
                .collect(),
        };
        Wrapped(SolanaTransaction {
            slot: 0,
            tx_idx: 0,
            tx: value.transaction.decode().unwrap(),
            loaded_addresses,
            status: meta.status,
            log_messages,
            inner_instructions: Vec::new(),
            compute_units_consumed: 0,
            fee: 0,
        })
    }
}

fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let mut accounts_db = DummyAdb::new(args.neon_pubkey);

    let mut txs = args.previous_transactions.clone();
    txs.push(args.transaction_path);

    for tx in txs {
        let encoded: EncodedTransactionWithStatusMeta = serde_json::from_str(
            &read_to_string(&tx).with_context(|| format!("reading file: {}", &tx))?,
        )
        .with_context(|| format!("deserializing json: {}", tx))?;
        let transaction: Wrapped<SolanaTransaction> = encoded.into();
        let transaction = transaction.0;
        let signature = transaction.tx.signatures[0];
        let tx_infos = parse(transaction, &mut accounts_db)
            .with_context(|| format!("parsing transaction {}", signature))?;

        for tx_info in tx_infos {
            let tx_info: DbRow = tx_info.into();
            println!("{}", serde_json::to_string_pretty(&tx_info).unwrap());
        }
    }

    Ok(())
}
