use common::evm_loader::account::Holder;

use common::evm_loader::error::Error as NeonError;
use common::evm_loader::types::Transaction;
use common::solana_sdk::pubkey::Pubkey;

use super::AccountsDb;
use arrayref::array_ref;
use thiserror::Error;

pub type TxHash = [u8; 32];

pub mod tag {
    pub const DEPOSIT: u8 = 0x31;
    pub const HOLDER_CREATE: u8 = 0x24;
    pub const HOLDER_DELETE: u8 = 0x25;
    pub const HOLDER_WRITE: u8 = 0x26;

    pub const TX_EXEC_FROM_DATA: u8 = 0x32;
    pub const TX_EXEC_FROM_ACCOUNT: u8 = 0x33;
    pub const TX_STEP_FROM_DATA: u8 = 0x34;
    pub const TX_STEP_FROM_ACCOUNT: u8 = 0x35;
    pub const TX_STEP_FROM_ACCOUNT_NO_CHAINID: u8 = 0x36;
    pub const CANCEL: u8 = 0x37;

    pub const DEPOSIT_DEPRECATED: u8 = 0x27;
    pub const TX_EXEC_FROM_DATA_DEPRECATED: u8 = 0x1f;
    pub const TX_EXEC_FROM_ACCOUNT_DEPRECATED: u8 = 0x2a;
    pub const TX_STEP_FROM_DATA_DEPRECATED: u8 = 0x20;
    pub const TX_STEP_FROM_ACCOUNT_DEPRECATED: u8 = 0x21;
    pub const TX_STEP_FROM_ACCOUNT_NO_CHAINID_DEPRECATED: u8 = 0x22;
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("neon error")]
    Neon(#[from] NeonError),
    #[error("invalid instruction")]
    InvalidInstruction,
    #[error("not implemented")]
    NotImplemented,
    #[error("account not found: {0}")]
    AccountNotFound(Pubkey),
    #[error("unknown instruction tag: 0x{:x}", .0)]
    UnknownTag(u8),
}

#[derive(Debug)]
pub enum HolderOperation {
    Create(Pubkey),
    Delete(Pubkey),
    Write {
        pubkey: Pubkey,
        tx_hash: [u8; 32],
        offset: usize,
        data: Vec<u8>,
    },
}

impl HolderOperation {
    pub fn pubkey(&self) -> Pubkey {
        match self {
            Self::Create(pubkey) => *pubkey,
            Self::Delete(pubkey) => *pubkey,
            Self::Write { pubkey, .. } => *pubkey,
        }
    }
}

#[derive(Debug)]
pub enum ParseResult {
    TransactionExecuted(Transaction),
    TransactionStep(Transaction),
    TransactionCancel(TxHash),
    HolderOperation(HolderOperation),
    Deposit,
}

pub fn parse(
    bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<ParseResult, Error> {
    let tag = bytes.first().ok_or_else(|| Error::InvalidInstruction)?;
    let res = match *tag {
        tag::DEPOSIT => {
            tracing::info!("found deposit instruction");
            ParseResult::Deposit
        }
        tag::TX_EXEC_FROM_DATA => {
            tracing::info!("found deprecated tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_DATA => {
            tracing::info!("found tx step from data");
            let tx = decode_step_from_ix(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT => {
            tracing::info!("found tx step from account");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT => {
            tracing::info!("found tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID => {
            tracing::info!("found tx step from account no chain_id");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::HOLDER_CREATE => {
            tracing::info!("found holder create instruction");
            let op = decode_holder_create(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::HolderOperation(op)
        }
        tag::HOLDER_DELETE => {
            tracing::info!("found holder delete instruction");
            let op = decode_holder_delete(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::HolderOperation(op)
        }
        tag::HOLDER_WRITE => {
            tracing::info!("found holder write instruction");
            let op = decode_holder_write(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::HolderOperation(op)
        }
        /* old: currently uses the same code, but potentially may change */
        tag::DEPOSIT_DEPRECATED => {
            tracing::info!("found deprecated deposit instruction");
            ParseResult::Deposit
        }
        tag::TX_EXEC_FROM_DATA_DEPRECATED => {
            tracing::info!("found deprecated tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_DATA_DEPRECATED => {
            tracing::info!("found deprecated tx step from data");
            let tx = decode_step_from_ix(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT_DEPRECATED => {
            tracing::info!("found deprecated tx step from account");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT_DEPRECATED => {
            tracing::info!("found deprecated tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID_DEPRECATED => {
            tracing::info!("found deprecated tx step from account no chain_id");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResult::TransactionStep(tx)
        }
        tag::CANCEL => {
            tracing::info!("found cancel instruction");
            let hash = decode_cancel(&bytes[1..], accounts, adb)?;
            ParseResult::TransactionCancel(hash)
        }
        _ => {
            tracing::warn!("not implemented tag: 0x{:x}", tag);
            return Err(Error::UnknownTag(*tag));
        }
    };
    Ok(res)
}

fn decode_holder_create(
    _instruction: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<HolderOperation, Error> {
    use common::evm_loader::account::TAG_HOLDER;

    let holder_pubkey = accounts[0];

    adb.init_account(holder_pubkey);
    let account = adb.get_by_key(&holder_pubkey).unwrap();
    account.data.borrow_mut()[0] = TAG_HOLDER;
    let mut holder = Holder::from_account(&neon_pubkey, account).unwrap();
    holder.clear();

    Ok(HolderOperation::Create(holder_pubkey))
}

fn decode_holder_delete(
    _instruction: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    _neon_pubkey: Pubkey,
) -> Result<HolderOperation, Error> {
    let holder_pubkey = accounts[0];

    if let Some(holder) = adb.get_by_key(&holder_pubkey) {
        let mut data = holder.data.borrow_mut();
        data.fill(0);
    }
    Ok(HolderOperation::Delete(holder_pubkey))
}

fn decode_holder_write(
    instruction: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<HolderOperation, Error> {
    use common::evm_loader::account::TAG_HOLDER;
    let holder_pubkey = accounts[0];
    let transaction_hash = *array_ref![instruction, 0, 32];
    let offset = usize::from_le_bytes(*array_ref![instruction, 32, 8]);
    let data = &instruction[32 + 8..];

    tracing::info!(pubkey = %holder_pubkey, ?transaction_hash, ?offset, ?data, "holder write");

    let mut holder = match adb.get_by_key(&holder_pubkey) {
        Some(account) => {
            let holder = Holder::from_account(&neon_pubkey, account).unwrap();
            holder
        }
        None => {
            adb.init_account(accounts[0]);
            let account = adb.get_by_key(&holder_pubkey).unwrap();
            account.data.borrow_mut()[0] = TAG_HOLDER;
            let mut holder = Holder::from_account(&neon_pubkey, account).unwrap();
            holder.clear();
            holder
        }
    };
    holder.update_transaction_hash(transaction_hash);
    holder.write(offset, data)?;

    Ok(HolderOperation::Write {
        pubkey: holder_pubkey,
        tx_hash: transaction_hash,
        offset,
        data: data.to_vec(),
    })
}

fn decode_exec_from_account(
    _bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    pubkey: Pubkey,
) -> Result<Transaction, Error> {
    let holder_key = accounts[0];
    let account = adb
        .get_by_key(&holder_key)
        .ok_or(Error::AccountNotFound(holder_key))?;
    let holder = Holder::from_account(&pubkey, account).unwrap();
    let message = holder.transaction();
    let trx = Transaction::from_rlp(&message)?;

    Ok(trx)
}

fn decode_cancel(
    instruction: &[u8],
    _accounts: &[Pubkey],
    _adb: &mut impl AccountsDb,
) -> Result<TxHash, Error> {
    let transaction_hash = array_ref!(instruction, 0, 32);
    Ok(*transaction_hash)
}

fn decode_step_from_account(
    _bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    pubkey: Pubkey,
) -> Result<Transaction, Error> {
    use common::evm_loader::account::legacy::{
        TAG_HOLDER_DEPRECATED, TAG_STATE_FINALIZED_DEPRECATED,
    };
    use common::evm_loader::account::{TAG_HOLDER, TAG_STATE, TAG_STATE_FINALIZED};

    let holder_key = accounts[0];
    let holder_or_storage = adb
        .get_by_key(&holder_key)
        .ok_or(Error::AccountNotFound(holder_key))?;
    let tag = common::evm_loader::account::tag(&pubkey, &holder_or_storage).unwrap();
    tracing::debug!(tag, "holder account tag");
    match tag {
        TAG_HOLDER | TAG_HOLDER_DEPRECATED => {
            let holder = Holder::from_account(&pubkey, holder_or_storage.clone()).unwrap();
            let message = holder.transaction();
            let trx = Transaction::from_rlp(&message)?;

            return Ok(trx);
        }
        TAG_STATE => {
            tracing::warn!("TAG_STATE not implemented");
        }
        TAG_STATE_FINALIZED | TAG_STATE_FINALIZED_DEPRECATED => {
            tracing::warn!("TAG_STATE_FINALIZED not implemented");
        }
        _ => todo!("unknown tag {} (not implemented)", tag),
    }
    todo!()
}

fn decode_step_from_ix(
    bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    pubkey: Pubkey,
) -> Result<Transaction, Error> {
    use common::evm_loader::account::{TAG_HOLDER, TAG_STATE, TAG_STATE_FINALIZED};

    let _treasury_index = u32::from_le_bytes(*array_ref![bytes, 0, 4]);
    let _step_count = u64::from(u32::from_le_bytes(*array_ref![bytes, 4, 4]));
    // skip 4 bytes
    let message = &bytes[4 + 4 + 4..];
    let storage_key = accounts[0];
    let storage = adb
        .get_by_key(&storage_key)
        .ok_or(Error::AccountNotFound(storage_key))?;

    let tag = common::evm_loader::account::tag(&pubkey, &storage).unwrap();
    tracing::debug!(tag, "storage account tag");

    match tag {
        TAG_HOLDER | TAG_STATE_FINALIZED => {
            let trx = Transaction::from_rlp(message)?;
            Ok(trx)
        }
        TAG_STATE => {
            todo!("TAG_STATE not implemented")
        }
        _ => todo!("unknown tag {} (not implemented)", tag),
    }
}

fn decode_execute_from_ix(bytes: &[u8]) -> Result<Transaction, Error> {
    let _treasury_index = u32::from_le_bytes(*array_ref![bytes, 0, 4]);
    let tx = Transaction::from_rlp(&bytes[4..])?;
    Ok(tx)
}
