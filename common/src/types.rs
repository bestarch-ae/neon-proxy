use std::fmt::{Display, Formatter};

use alloy_consensus::{Transaction as TransactionTrait, TxEnvelope};
use anyhow::bail;
use ethnum::U256;
use evm_loader::types::{Address, Transaction};
use reth_primitives::alloy_primitives;
use solana_sdk::clock::UnixTimestamp;
use solana_sdk::hash::Hash;
use solana_sdk::message::{v0::LoadedAddresses, AccountKeys};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::{Result as TransactionResult, VersionedTransaction};
use solana_transaction_status::InnerInstructions;

pub trait TxEnvelopeExt {
    fn chain_id(&self) -> Result<Option<u64>, anyhow::Error>;
    fn input_len(&self) -> Result<usize, anyhow::Error>;
    fn gas_limit(&self) -> Result<u128, anyhow::Error>;
    fn gas_price(&self) -> Result<Option<u128>, anyhow::Error>;
    fn value(&self) -> Result<alloy_primitives::U256, anyhow::Error>;
}

impl TxEnvelopeExt for TxEnvelope {
    fn chain_id(&self) -> Result<Option<u64>, anyhow::Error> {
        match self {
            TxEnvelope::Legacy(signed) => Ok(signed.tx().chain_id),
            TxEnvelope::Eip1559(signed) => Ok(signed.tx().chain_id()),
            TxEnvelope::Eip2930(signed) => Ok(signed.tx().chain_id()),
            TxEnvelope::Eip4844(signed) => Ok(signed.tx().chain_id()),
            _ => bail!("unsupported tx type"),
        }
    }

    fn input_len(&self) -> Result<usize, anyhow::Error> {
        match self {
            TxEnvelope::Legacy(signed) => Ok(signed.tx().input.len()),
            TxEnvelope::Eip1559(signed) => Ok(signed.tx().input.len()),
            TxEnvelope::Eip2930(signed) => Ok(signed.tx().input.len()),
            TxEnvelope::Eip4844(signed) => Ok(signed.tx().tx().input.len()),
            _ => bail!("unsupported tx type"),
        }
    }

    fn gas_limit(&self) -> Result<u128, anyhow::Error> {
        match self {
            TxEnvelope::Legacy(signed) => Ok(signed.tx().gas_limit),
            TxEnvelope::Eip1559(signed) => Ok(signed.tx().gas_limit()),
            TxEnvelope::Eip2930(signed) => Ok(signed.tx().gas_limit()),
            TxEnvelope::Eip4844(signed) => Ok(signed.tx().gas_limit()),
            _ => bail!("unsupported tx type"),
        }
    }

    fn gas_price(&self) -> Result<Option<u128>, anyhow::Error> {
        match self {
            TxEnvelope::Legacy(signed) => Ok(signed.tx().gas_price()),
            TxEnvelope::Eip1559(signed) => Ok(signed.tx().gas_price()),
            TxEnvelope::Eip2930(signed) => Ok(signed.tx().gas_price()),
            TxEnvelope::Eip4844(signed) => Ok(signed.tx().gas_price()),
            _ => bail!("unsupported tx type"),
        }
    }

    fn value(&self) -> Result<alloy_primitives::U256, anyhow::Error> {
        match self {
            TxEnvelope::Legacy(signed) => Ok(signed.tx().value),
            TxEnvelope::Eip1559(signed) => Ok(signed.tx().value),
            TxEnvelope::Eip2930(signed) => Ok(signed.tx().value),
            TxEnvelope::Eip4844(signed) => Ok(signed.tx().tx().value),
            _ => bail!("unsupported tx type"),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TxHash([u8; 32]);

impl TxHash {
    pub fn as_array(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 32]> for TxHash {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl TryFrom<Vec<u8>> for TxHash {
    type Error = Vec<u8>;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        <[u8; 32]>::try_from(value).map(Self)
    }
}

impl Display for TxHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for TxHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

/// Solana block info.
#[derive(Debug, Clone)]
pub struct SolanaBlock {
    pub slot: Slot,
    pub hash: Hash,
    pub parent_slot: Slot,
    pub parent_hash: Hash,
    pub time: Option<UnixTimestamp>,
    pub is_finalized: bool,
}

/// Solana transaction.
#[derive(Debug, Clone)]
pub struct SolanaTransaction {
    pub slot: Slot,
    pub tx_idx: u64,
    pub tx: VersionedTransaction,
    pub loaded_addresses: LoadedAddresses,
    pub status: TransactionResult<()>,
    pub log_messages: Vec<String>,
    pub inner_instructions: Vec<InnerInstructions>, // Do we really need this
    pub compute_units_consumed: u64,
    pub fee: u64,
}

impl SolanaTransaction {
    pub fn has_key(&self, pubkey: Pubkey) -> bool {
        let pubkeys = AccountKeys::new(
            self.tx.message.static_account_keys(),
            Some(&self.loaded_addresses),
        );
        pubkeys.iter().any(|pk| *pk == pubkey)
    }
}

/// Represents instruction with NEON invocation inside. Inserted into `solana_neon_transations`.
#[derive(Debug, Clone)]
pub struct NeonIxReceipt {
    pub sol_signature: String,
    pub slot: u64,
    pub tx_idx: u64,
    pub ix_idx: u64,
    pub inner_idx: u64,

    pub neon_ix_code: u8,    // TODO: Check,
    pub is_successful: bool, // TODO: Result?

    pub max_heap_size: u64,
    pub used_heap_size: u64,

    pub max_bpf_cycle_cnt: u64,
    pub used_bpf_cycle_cnt: u64,
}

/// Neon Transaction. Not yet sure if represents a single contract invocation or a completed transation.
/// Inserted into `neon_transactions` table. Lacks `Clone` due to `evm-loader` implementation.
#[derive(Debug)]
pub struct NeonTxInfo {
    // TODO: Remove this line https://github.com/neonlabsorg/neon-evm/blob/4236c916031f9e6385b831769a8539d552299df0/evm_loader/program/src/lib.rs#L30
    pub tx_type: u8,
    pub neon_signature: TxHash,
    pub from: Address,
    // TODO: This migth be a method
    pub contract: Option<Address>,
    pub transaction: Transaction, // TODO: Clone
    pub events: Vec<EventLog>,

    pub gas_used: U256,
    pub sum_gas_used: U256, // TODO: What is this?
    pub neon_steps: u64,

    // Solana index
    // TODO: Should probably be Arc-ed or Bytes
    // TODO 2: Should probably be replaced by a ref to corresponding `NeonIxReceeipt`
    pub sol_signature: Signature,
    pub sol_slot: u64,
    pub tx_idx: u64,
    pub sol_ix_idx: u64,
    pub sol_ix_inner_idx: u64,

    pub status: u8,
    pub is_completed: bool,
    pub is_cancelled: bool,
}

#[derive(Debug)]
pub struct CanceledNeonTxInfo {
    pub neon_signature: TxHash,
    pub sol_slot: u64,
    pub tx_idx: u64,
    pub gas_used: U256,
    pub sum_gas_used: U256,
}

/// Event kinds can be logged to solana transaction logs.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u32)]
pub enum EventKind {
    Log = 1,

    StepReset = 50,
    InvalidRevision = 51,

    EnterCall = 101,
    EnterCallCode = 102,
    EnterStaticCall = 103,
    EnterDelegateCall = 104,
    // Unsure if we can distinguish these two since both logged as create.
    // https://github.com/neonlabsorg/neon-evm/blob/63226c399763af49eb27828b7d17f51c16d432e1/evm_loader/program/src/evm/opcode.rs#L1072
    EnterCreate = 105,
    EnterCreate2 = 106,

    // Remaining types do not log an address.
    ExitStop = 201,
    ExitReturn = 202,
    ExitSelfDestruct = 203,
    ExitRevert = 204,
    ExitSendAll = 205,

    Return = 300,
    // Could not find this one
    Cancel = 301,
}

impl EventKind {
    pub fn is_reset(&self) -> bool {
        matches!(self, EventKind::StepReset)
    }

    pub fn is_exit(&self) -> bool {
        matches!(
            self,
            EventKind::ExitStop
                | EventKind::ExitReturn
                | EventKind::ExitSelfDestruct
                | EventKind::ExitRevert
                | EventKind::ExitSendAll
        )
    }

    pub fn is_start(&self) -> bool {
        matches!(
            self,
            EventKind::EnterCall
                | EventKind::EnterCallCode
                | EventKind::EnterStaticCall
                | EventKind::EnterDelegateCall
                | EventKind::EnterCreate
                | EventKind::EnterCreate2
        )
    }
}

// ===== Alternative Event =====
// #[derive(Debug, Clone)]
// #[repr(u8)]
// /// Event kinds can be logged to solana transaction logs.
// pub enum EventKind2 {
//     Log {
//         address: Address,
//         // TODO: capped at 4, maybe use smallvec or array
//         topic_list: Vec<U256>,
//         data: Vec<u8>,
//     } = 2,

//     EnterCall {
//         address: Address,
//     } = 101,
//     EnterCallCode {
//         address: Address,
//     } = 102,
//     EnterStaticCall {
//         address: Address,
//     } = 103,
//     EnterDelegateCall {
//         address: Address,
//     } = 104,
//     // Unsure if we can distinguish these two since both logged as create.
//     // https://github.com/neonlabsorg/neon-evm/blob/63226c399763af49eb27828b7d17f51c16d432e1/evm_loader/program/src/evm/opcode.rs#L1072
//     EnterCreate {
//         address: Address,
//     } = 105,
//     EnterCreate2 {
//         address: Address,
//     } = 106,

//     // Remaining types do not log an address.
//     ExitStop {
//         data: Vec<u8>,
//     } = 201,
//     ExitReturn {
//         data: Vec<u8>,
//     } = 202,
//     ExitSelfDestruct {
//         data: Vec<u8>,
//     } = 203,
//     ExitRevert {
//         data: Vec<u8>,
//     } = 204,

//     Return {
//         status: u8,
//     } = 300,
//     // Could not find this one
//     Cancel = 301,
// }

// TODO: Consider using sum type event instead of flat struct
/// Represents single Neon event logged to solana transaction logs.
/// Inserted into `neon_transaction_logs` (probably).
#[derive(Debug, Clone)]
pub struct EventLog {
    pub event_type: EventKind,
    pub is_hidden: bool,
    pub is_reverted: bool,

    pub address: Option<Address>,
    // TODO: capped at 4, maybe use smallvec or array
    pub topic_list: Vec<U256>,
    pub data: Vec<u8>, // TODO: HexBytes?

    pub tx_log_idx: u64,  /* transaction log idx */
    pub blk_log_idx: u64, /* block log idx */
    pub level: u64,
    pub order: u64,
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

pub mod utils {
    use evm_loader::types::Transaction;
    use evm_loader::types::{AccessListTx, LegacyTx, TransactionPayload};

    pub fn clone_evm_transaction(tx: &Transaction) -> Transaction {
        let payload = match &tx.transaction {
            TransactionPayload::Legacy(legacy) => TransactionPayload::Legacy(LegacyTx {
                nonce: legacy.nonce,
                gas_price: legacy.gas_price,
                gas_limit: legacy.gas_limit,
                target: legacy.target,
                value: legacy.value,
                call_data: legacy.call_data.clone(),
                v: legacy.v,
                r: legacy.r,
                s: legacy.s,
                chain_id: legacy.chain_id,
                recovery_id: legacy.recovery_id,
            }),
            TransactionPayload::AccessList(acctx) => TransactionPayload::AccessList(AccessListTx {
                nonce: acctx.nonce,
                gas_price: acctx.gas_price,
                gas_limit: acctx.gas_limit,
                target: acctx.target,
                value: acctx.value,
                call_data: acctx.call_data.clone(),
                r: acctx.r,
                s: acctx.s,
                chain_id: acctx.chain_id,
                recovery_id: acctx.recovery_id,
                access_list: acctx.access_list.clone(),
            }),
        };
        Transaction {
            transaction: payload,
            byte_len: tx.byte_len,
            hash: tx.hash,
            signed_hash: tx.signed_hash,
        }
    }
}
