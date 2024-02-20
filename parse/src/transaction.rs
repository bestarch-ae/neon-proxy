use common::evm_loader::account::Holder;

use common::evm_loader::error::Error as NeonError;
use common::evm_loader::types::Transaction;
use common::solana_sdk::pubkey::Pubkey;

use super::AccountsDb;
use arrayref::array_ref;
use thiserror::Error;

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
}

pub fn parse(
    bytes: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<Option<Transaction>, Error> {
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
        /* old */
        0x1f => {
            tracing::info!("found deprecated tx exec from data");
            let tx = decode_execute_from_ix(&bytes[1..])?;
            return Ok(Some(tx));
        }
        0x21 => {
            tracing::info!("found deprecated tx step from account");
            let tx = decode_step_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            return Ok(Some(tx));
        }
        0x26 => {
            tracing::info!("found holder write instruction");
            decode_holder_write(&bytes[1..], accounts, adb, neon_pubkey)?;
        }
        0x2a => {
            tracing::info!("found deprecated tx exec from account");
            let tx = decode_exec_from_account(&bytes[1..], accounts, adb, neon_pubkey)?;
            return Ok(Some(tx));
        }
        _ => {
            tracing::warn!("not implemented tag: 0x{:x}", tag);
            panic!("not implemented tag");
        }
    }
    Ok(None) // TODO
}

fn decode_holder_write(
    instruction: &[u8],
    accounts: &[Pubkey],
    adb: &mut impl AccountsDb,
    neon_pubkey: Pubkey,
) -> Result<(), Error> {
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

    Ok(())
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

fn decode_execute_from_ix(bytes: &[u8]) -> Result<Transaction, Error> {
    let _treasure_index = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let tx = Transaction::from_rlp(&bytes[4..])?;
    Ok(tx)
}
