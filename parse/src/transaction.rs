use common::evm_loader::account::Holder;

use common::evm_loader::error::Error as NeonError;
use common::evm_loader::types::Transaction;
use common::solana_sdk::pubkey::Pubkey;
use common::types::HolderOperation;

use super::AccountsDb;
use arrayref::array_ref;
use thiserror::Error;

pub type TxHash = [u8; 32];

pub mod tag {
    pub const COLLECT_TREASURE: u8 = 0x1e;

    pub const HOLDER_CREATE: u8 = 0x24;
    pub const HOLDER_DELETE: u8 = 0x25;
    pub const HOLDER_WRITE: u8 = 0x26;
    pub const CREATE_MAIN_TREASURY: u8 = 0x29;

    pub const ACCOUNT_CREATE_BALANCE: u8 = 0x30;
    pub const DEPOSIT: u8 = 0x31;

    pub const TX_EXEC_FROM_DATA: u8 = 0x32;
    pub const TX_EXEC_FROM_ACCOUNT: u8 = 0x33;
    pub const TX_STEP_FROM_DATA: u8 = 0x34;
    pub const TX_STEP_FROM_ACCOUNT: u8 = 0x35;
    pub const TX_STEP_FROM_ACCOUNT_NO_CHAINID: u8 = 0x36;
    pub const CANCEL: u8 = 0x37;
    pub const TX_EXEC_FROM_DATA_SOLANA_CALL: u8 = 0x38;
    pub const TX_EXEC_FROM_ACCOUNT_SOLANA_CALL: u8 = 0x39;

    pub const OPERATOR_BALANCE_CREATE: u8 = 0x3a;
    pub const OPERATOR_BALANCE_DELETE: u8 = 0x3b;
    pub const OPERATOR_BALANCE_WITHDRAW: u8 = 0x3c;

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
pub enum BalanceOperation {
    Create,
    Delete,
    Withdraw,
}

#[derive(Debug)]
pub enum ParseResult {
    TransactionExecuted(Transaction),
    TransactionStep(Transaction),
    TransactionCancel(TxHash),
    HolderOperation(HolderOperation),
    Deposit,
    AccountCreateBalance,
    CollectTreasure,
    CreateMainTreasury,
    #[allow(dead_code)]
    OperatorOperation(BalanceOperation),
}

pub fn parse(
    bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<ParseResult, Error> {
    let tag = bytes.first().ok_or_else(|| Error::InvalidInstruction)?;
    let res = match *tag {
        tag::COLLECT_TREASURE => {
            tracing::info!("found collect treasure instruction");
            ParseResult::CollectTreasure
        }
        tag::CREATE_MAIN_TREASURY => {
            tracing::info!("found create main treasury instruction");
            ParseResult::CreateMainTreasury
        }
        tag::ACCOUNT_CREATE_BALANCE => {
            tracing::info!("found account create balance instruction");
            ParseResult::AccountCreateBalance
        }
        tag::DEPOSIT => {
            tracing::info!("found deposit instruction");
            ParseResult::Deposit
        }
        tag::OPERATOR_BALANCE_CREATE => {
            tracing::info!("found operator balance create instruction");
            ParseResult::OperatorOperation(BalanceOperation::Create)
        }
        tag::OPERATOR_BALANCE_DELETE => {
            tracing::info!("found operator balance delete instruction");
            ParseResult::OperatorOperation(BalanceOperation::Delete)
        }
        tag::OPERATOR_BALANCE_WITHDRAW => {
            tracing::info!("found operator balance withdraw instruction");
            ParseResult::OperatorOperation(BalanceOperation::Withdraw)
        }
        tag::TX_EXEC_FROM_DATA => {
            tracing::debug!("found tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from instruction data"
            );
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_EXEC_FROM_DATA_SOLANA_CALL => {
            tracing::debug!("found tx exec from data with solana call");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from instruction data (solana call)"
            );
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_DATA => {
            tracing::debug!("found tx step from data");
            let tx = decode_step_from_ix(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "iterative execution from instruction data"
            );
            ParseResult::TransactionStep(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT => {
            tracing::debug!("found tx step from account");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "iterative execution from holder account"
            );
            ParseResult::TransactionStep(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT => {
            tracing::debug!("found tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from holder account"
            );
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT_SOLANA_CALL => {
            tracing::debug!("found tx exec from account with solana call");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from holder account (solana call)"
            );
            ParseResult::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID => {
            tracing::debug!("found tx step from account no chain_id");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "iterative execution from holder account (before EIP155)"
            );
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
    let account = adb.get_by_key(&holder_pubkey).unwrap(); // we just did init
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

    tracing::debug!(pubkey = %holder_pubkey, "holder delete");
    if let Some(account) = adb.get_by_key(&holder_pubkey) {
        let mut data = account.data.borrow_mut();
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

    tracing::debug!(pubkey = %holder_pubkey, ?transaction_hash, ?offset, ?data, "holder write");

    let (mut holder, acc) = match adb.get_by_key(&holder_pubkey) {
        Some(account) => {
            let acc = account.clone();
            let holder = Holder::from_account(&neon_pubkey, account).unwrap();

            (holder, acc)
        }
        None => {
            tracing::warn!(pubkey = %holder_pubkey, "creating holder account implicitly on HOLDER_WRITE");
            /* we haven't seen this account yet, but let's create it and hope for the best */
            adb.init_account(holder_pubkey);
            let account = adb.get_by_key(&holder_pubkey).unwrap(); // we just did init
            let acc = account.clone();
            account.data.borrow_mut()[0] = TAG_HOLDER;
            let mut holder = Holder::from_account(&neon_pubkey, account).unwrap();
            holder.clear();
            (holder, acc)
        }
    };
    holder.update_transaction_hash(transaction_hash);
    holder.write(offset, data)?;

    tracing::info!(pubkey = %holder_pubkey, data = %common::solana_sdk::hash::hash(&acc.data.borrow()), "holder account after write");

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
