use thiserror::Error;

use common::solana_sdk::hash::ParseHashError;
use common::solana_sdk::message::v0::LoadedAddresses;
use common::solana_sdk::pubkey::{ParsePubkeyError, Pubkey};
use common::solana_transaction_status::option_serializer::OptionSerializer;
use common::solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use common::solana_transaction_status::UiLoadedAddresses;
use common::solana_transaction_status::{EncodedTransaction, EncodedTransactionWithStatusMeta};
use common::types::SolanaTransaction;

#[derive(Debug, Error)]
pub enum TxDecodeError {
    #[error("absent transaction meta")]
    MissingMeta,
    #[error("absent transaction logs")]
    MissingLogs,
    #[error("absent transaction consumed compute units")]
    MissingCUnits,
    #[error("absent transaction loaded addresses")]
    MissingLoadedAddr,
    #[error("absent signatures in block")]
    MissingSignatures,
    #[error("absent transactions in block")]
    MissingTransactions,
    #[error("transaction cannot be found in block")]
    MissingTxInBlock,
    #[error("invalide loaded addresses: {0}")]
    InvalidLoadedAddr(ParsePubkeyError),
    #[error("invalid transaction encoding ({0:?})")]
    InvalidEncoding(EncodedTransaction),
    #[error("invalid block hash: {0}")]
    InvalidHash(#[from] ParseHashError),
}

trait OptionSerializerExt {
    type Output;
    fn opt(self) -> Option<Self::Output>;
}

// I hate this type
impl<T> OptionSerializerExt for OptionSerializer<T> {
    type Output = T;
    fn opt(self) -> Option<Self::Output> {
        self.into()
    }
}

pub fn decode_ui_transaction(
    tx: EncodedTransactionWithStatusMeta,
    slot: u64,
) -> Result<SolanaTransaction, TxDecodeError> {
    let EncodedTransactionWithStatusMeta {
        transaction,
        meta,
        version: _,
    } = tx;

    let meta = meta.ok_or(TxDecodeError::MissingMeta)?;
    let Some(tx) = transaction.decode() else {
        return Err(TxDecodeError::InvalidEncoding(transaction));
    };

    let pre_balance = meta.pre_balances.first().copied().unwrap_or(0);
    let post_balance = meta.post_balances.first().copied().unwrap_or(0);

    let result = SolanaTransaction {
        slot,

        tx_idx: 0,
        tx,
        loaded_addresses: meta
            .loaded_addresses
            .opt()
            .map(decode_loaded_addresses)
            .transpose()
            .map_err(TxDecodeError::InvalidLoadedAddr)?
            .unwrap_or_default(),
        status: meta.err.map_or(Ok(()), Err),
        log_messages: meta.log_messages.opt().ok_or(TxDecodeError::MissingLogs)?,
        compute_units_consumed: meta.compute_units_consumed.opt().unwrap_or(0),
        fee: meta.fee,
        sol_expense: pre_balance as i64 - post_balance as i64,
    };

    Ok(result)
}

pub fn decode_confirmed_ui_transaction(
    tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<SolanaTransaction, TxDecodeError> {
    let EncodedConfirmedTransactionWithStatusMeta {
        slot, transaction, ..
    } = tx;
    decode_ui_transaction(transaction, slot)
}

fn decode_loaded_addresses(
    addresses: UiLoadedAddresses,
) -> Result<LoadedAddresses, ParsePubkeyError> {
    let UiLoadedAddresses { writable, readonly } = addresses;
    Ok(LoadedAddresses {
        writable: writable
            .into_iter()
            .map(|s| s.parse::<Pubkey>())
            .collect::<Result<Vec<Pubkey>, _>>()?,
        readonly: readonly
            .into_iter()
            .map(|s| s.parse::<Pubkey>())
            .collect::<Result<Vec<Pubkey>, _>>()?,
    })
}
