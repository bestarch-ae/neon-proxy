use ethnum::U256;
use evm_loader::types::{Address, Transaction};

use solana_sdk::clock::UnixTimestamp;
use solana_sdk::message::v0::LoadedAddresses;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::{Result as TransactionResult, VersionedTransaction};
use solana_transaction_status::InnerInstruction;

/// Solana transaction with block info.
#[derive(Debug, Clone)]
pub struct SolanaTransaction {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub blockhash: String,
    pub block_time: Option<UnixTimestamp>,

    /// Position of this transaction inside the block
    pub tx_idx: u64,
    pub tx: VersionedTransaction,
    pub loaded_addresses: LoadedAddresses,
    pub status: TransactionResult<()>,
    pub log_messages: Vec<String>,
    pub inner_instructions: Vec<InnerInstruction>, // Do we really need this
    pub compute_units_consumed: u64,
    pub fee: u64,
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
    // TODO: Should probably be Arc-ed or Bytes
    pub neon_signature: String,
    pub from: Address,
    // TODO: This migth be a method
    pub contract: Option<Address>,
    pub transaction: Transaction, // TODO: Clone
    pub events: Vec<EventLog>,

    pub gas_used: U256,
    pub sum_gas_used: U256, // TODO: What is this?

    // Solana index
    // TODO: Should probably be Arc-ed or Bytes
    // TODO 2: Should probably be replaced by a ref to corresponding `NeonIxReceeipt`
    pub sol_signature: String,
    pub sol_slot: u64,
    pub sol_tx_idx: u64,
    pub sol_ix_idx: u64,
    pub sol_ix_inner_idx: u64,

    pub status: u8, // TODO: Why TEXT in DB?
    pub is_completed: bool,
    pub is_cancelled: bool,
}

/// Event kinds can be logged to solana transaction logs.
#[derive(Debug, Clone, Copy)]
pub enum EventKind {
    Log = 1,

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

    Return = 300,
    // Could not find this one
    Cancel = 301,
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
    pub is_hidden: bool, // TODO: WTF? Do we store hidden events?

    pub address: Option<Address>,
    // TODO: capped at 4, maybe use smallvec or array
    pub topic_list: Vec<U256>,
    pub data: Vec<u8>, // TODO: HexBytes?

    pub log_idx: u64,
    pub level: u64,
    pub order: u64,
}
