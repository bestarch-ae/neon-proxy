mod chain_pool;
mod sender_pool;

use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, ChainId};

use crate::mempool::{EthTxHash, GasPrice};

pub use chain_pool::{ChainPool, Config as ChainPoolConfig, NeonApiGetTxCount};
pub use sender_pool::{SenderPool, SenderPoolState};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionResult {
    pub tx_hash: EthTxHash,
    pub chain_id: ChainId,
    pub success: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QueueRecord {
    pub sender: Address,
    pub tx_hash: EthTxHash,
    pub nonce: TxNonce,
    pub sorting_gas_price: GasPrice,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum QueueUpdateAdd {
    Pending(QueueRecord),
    Gapped(QueueRecord),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum QueueUpdateMove {
    GappedToPending(Vec<QueueRecord>),
    PendingToGapped(Vec<QueueRecord>),
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[must_use = "Updates must be applied"]
pub struct QueuesUpdate {
    pub add_update: Option<QueueUpdateAdd>,
    pub move_update: Option<QueueUpdateMove>,
    pub remove_nonce_too_small: Vec<QueueRecord>,
    pub remove_queued_nonce_too_small: Option<QueueRecord>,
}
