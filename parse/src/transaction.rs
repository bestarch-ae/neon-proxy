use common::evm_loader::account::Holder;

use common::evm_loader::error::Error as NeonError;
use common::evm_loader::types::Transaction;
use common::solana_sdk::pubkey::Pubkey;
use common::types::{HolderOperation, TxHash};

use super::AccountsDb;
use arrayref::array_ref;
use thiserror::Error;

pub use common::neon_instruction::tag;

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
pub struct ParseResult {
    pub tag: u8,
    pub kind: ParseResultKind,
}

#[derive(Debug)]
pub enum ParseResultKind {
    TransactionExecuted(Transaction),
    TransactionStep(Option<Transaction>),
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
    let kind = match *tag {
        tag::COLLECT_TREASURE => {
            tracing::info!("found collect treasure instruction");
            ParseResultKind::CollectTreasure
        }
        tag::CREATE_MAIN_TREASURY => {
            tracing::info!("found create main treasury instruction");
            ParseResultKind::CreateMainTreasury
        }
        tag::ACCOUNT_CREATE_BALANCE => {
            tracing::info!("found account create balance instruction");
            ParseResultKind::AccountCreateBalance
        }
        tag::DEPOSIT => {
            tracing::info!("found deposit instruction");
            ParseResultKind::Deposit
        }
        tag::OPERATOR_BALANCE_CREATE => {
            tracing::info!("found operator balance create instruction");
            ParseResultKind::OperatorOperation(BalanceOperation::Create)
        }
        tag::OPERATOR_BALANCE_DELETE => {
            tracing::info!("found operator balance delete instruction");
            ParseResultKind::OperatorOperation(BalanceOperation::Delete)
        }
        tag::OPERATOR_BALANCE_WITHDRAW => {
            tracing::info!("found operator balance withdraw instruction");
            ParseResultKind::OperatorOperation(BalanceOperation::Withdraw)
        }
        tag::TX_EXEC_FROM_DATA | tag::TX_EXEC_FROM_DATA_DEPRECATED_V13 => {
            tracing::debug!(%tag, "found tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from instruction data"
            );
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::TX_EXEC_FROM_DATA_SOLANA_CALL | tag::TX_EXEC_FROM_DATA_SOLANA_CALL_V13 => {
            tracing::debug!(%tag, "found tx exec from data with solana call");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from instruction data (solana call)"
            );
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_DATA => {
            tracing::debug!("found tx step from data");
            let tx = decode_step_from_ix(&bytes[1..])?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "iterative execution from instruction data"
            );
            ParseResultKind::TransactionStep(Some(tx))
        }
        tag::TX_STEP_FROM_ACCOUNT
        | tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID
        | tag::TX_STEP_FROM_ACCOUNT_DEPRECATED
        | tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID_DEPRECATED => {
            match *tag {
                tag::TX_STEP_FROM_ACCOUNT => {
                    tracing::debug!("found tx step from account");
                }
                tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID => {
                    tracing::debug!("found tx step from account no chain_id");
                }
                tag::TX_STEP_FROM_ACCOUNT_DEPRECATED => {
                    tracing::info!("found deprecated tx step from account");
                }
                tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID_DEPRECATED => {
                    tracing::info!("found deprecated tx step from account no chain_id");
                }
                _ => unreachable!("all cases covered above"),
            }
            let tx = match decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey) {
                Ok(tx) => Some(tx),
                Err(Error::AccountNotFound(_pubkey)) => None,
                Err(err) => return Err(err),
            };
            tracing::info!(
                tx = tx.as_ref().map(|tx| hex::encode(tx.hash)),
                "iterative execution from holder account"
            );
            ParseResultKind::TransactionStep(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT => {
            tracing::debug!("found tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from holder account"
            );
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT_DEPRECATED => {
            tracing::info!("found deprecated tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::TX_EXEC_FROM_ACCOUNT_SOLANA_CALL => {
            tracing::debug!("found tx exec from account with solana call");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            tracing::info!(
                tx = hex::encode(tx.hash),
                "non-iterative execution from holder account (solana call)"
            );
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::HOLDER_CREATE => {
            tracing::info!("found holder create instruction");
            let op = decode_holder_create(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResultKind::HolderOperation(op)
        }
        tag::HOLDER_DELETE => {
            tracing::info!("found holder delete instruction");
            let op = decode_holder_delete(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResultKind::HolderOperation(op)
        }
        tag::HOLDER_WRITE => {
            tracing::info!("found holder write instruction");
            let op = decode_holder_write(&bytes[1..], accounts, adb, neon_pubkey)?;
            ParseResultKind::HolderOperation(op)
        }
        /* old: currently uses the same code, but potentially may change */
        tag::DEPOSIT_DEPRECATED => {
            tracing::info!("found deprecated deposit instruction");
            ParseResultKind::Deposit
        }
        tag::TX_EXEC_FROM_DATA_DEPRECATED => {
            tracing::info!("found deprecated tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            ParseResultKind::TransactionExecuted(tx)
        }
        tag::TX_STEP_FROM_DATA_DEPRECATED => {
            tracing::info!("found deprecated tx step from data");
            let tx = decode_step_from_ix(&bytes[1..])?;
            ParseResultKind::TransactionStep(Some(tx))
        }

        tag::CANCEL => {
            tracing::info!("found cancel instruction");
            let hash = decode_cancel(&bytes[1..], accounts, adb)?;
            ParseResultKind::TransactionCancel(hash)
        }
        _ => {
            tracing::warn!("not implemented tag: 0x{:x}", tag);
            return Err(Error::UnknownTag(*tag));
        }
    };
    Ok(ParseResult { tag: *tag, kind })
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
    let transaction_hash = *array_ref!(instruction, 0, 32);
    Ok(transaction_hash.into())
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
    tracing::debug!(%holder_key, tag, "holder account tag");
    match tag {
        TAG_HOLDER | TAG_HOLDER_DEPRECATED => {
            let holder = Holder::from_account(&pubkey, holder_or_storage.clone()).unwrap();
            let message = holder.transaction();
            if message.is_empty() {
                tracing::error!(account = %holder_key, storage = ?holder_or_storage,
                            "empty transaction in holder account, should not happen");
                return Err(Error::NotImplemented);
            }
            let trx = Transaction::from_rlp(&message)?;

            Ok(trx)
        }
        TAG_STATE => {
            tracing::warn!("TAG_STATE not implemented");
            Err(Error::NotImplemented)
        }
        TAG_STATE_FINALIZED | TAG_STATE_FINALIZED_DEPRECATED => {
            tracing::warn!("TAG_STATE_FINALIZED not implemented");
            Err(Error::NotImplemented)
        }
        _ => {
            tracing::error!("unknown tag {} (not implemented)", tag);
            Err(Error::NotImplemented)
        }
    }
}

fn decode_step_from_ix(bytes: &[u8]) -> Result<Transaction, Error> {
    let _treasury_index = u32::from_le_bytes(*array_ref![bytes, 0, 4]);
    let _step_count = u64::from(u32::from_le_bytes(*array_ref![bytes, 4, 4]));
    // skip 4 bytes
    let message = &bytes[4 + 4 + 4..];
    let trx = Transaction::from_rlp(message)?;
    Ok(trx)
}

fn decode_execute_from_ix(bytes: &[u8]) -> Result<Transaction, Error> {
    let _treasury_index = u32::from_le_bytes(*array_ref![bytes, 0, 4]);
    let tx = Transaction::from_rlp(&bytes[4..])?;
    Ok(tx)
}
