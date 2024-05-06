use common::solana_sdk::message::v0::LoadedAddresses;
use common::solana_sdk::pubkey::{ParsePubkeyError, Pubkey};
use common::solana_transaction_status::option_serializer::OptionSerializer;
use common::solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use common::solana_transaction_status::UiLoadedAddresses;
use common::solana_transaction_status::{EncodedTransaction, EncodedTransactionWithStatusMeta};
use common::types::SolanaTransaction;
use thiserror::Error;

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
    #[error("invalide loaded addresses: {0}")]
    InvalidLoadedAddr(ParsePubkeyError),
    #[error("invalid transaction encoding ({0:?})")]
    InvalidEncoding(EncodedTransaction),
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
    tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<SolanaTransaction, TxDecodeError> {
    let EncodedConfirmedTransactionWithStatusMeta {
        slot,
        transaction,
        block_time,
    } = tx;
    let EncodedTransactionWithStatusMeta {
        transaction,
        meta,
        version: _,
    } = transaction;

    let meta = meta.ok_or(TxDecodeError::MissingMeta)?;
    let Some(tx) = transaction.decode() else {
        return Err(TxDecodeError::InvalidEncoding(transaction));
    };

    let result = SolanaTransaction {
        slot,
        parent_slot: 0,                // TODO: needs block
        blockhash: Default::default(), // TODO: needs block
        block_time,

        tx_idx: 0, // TODO: needs block
        tx,
        loaded_addresses: meta
            .loaded_addresses
            .opt()
            .map(decode_loaded_addresses)
            .ok_or(TxDecodeError::MissingLoadedAddr)?
            .map_err(TxDecodeError::InvalidLoadedAddr)?,
        status: meta.err.map_or(Ok(()), Err),
        log_messages: meta.log_messages.opt().ok_or(TxDecodeError::MissingLogs)?,
        inner_instructions: Vec::new(),
        compute_units_consumed: meta
            .compute_units_consumed
            .opt()
            .ok_or(TxDecodeError::MissingCUnits)?,
        fee: meta.fee,
    };
    Ok(result)
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

// TODO
// fn decode_inner_instructions(inner_ixs: UiInnerInstructions) -> InnerInstructions {
//     let UiInnerInstructions {
//         index,
//         instructions,
//     } = inner_ixs;

//     InnerInstructions {
//         index,
//         instructions: instructions
//             .into_iter()
//             .map(|ix| InnerInstruction {
//                 instruction: CompiledInstruction {
//                     program_id_index: ix.program_id_index,
//                     accounts: ix.accounts,
//                     data: ix.data.into_bytes(), // TODO: b64?
//                 },
//             })
//             .collect(),
//     }
// }
