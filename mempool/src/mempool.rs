use std::cmp::{max, Reverse};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use alloy_consensus::{Transaction, TxEnvelope};
use dashmap::mapref::one::RefMut;
use dashmap::{DashMap, DashSet};
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, ChainId, B256};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, RwLock};

use common::neon_lib::types::BalanceAddress;
use common::solana_sdk::pubkey::Pubkey;
use executor::{ExecuteRequest, ExecutorTrait};
use neon_api::NeonApi;

use crate::error::MempoolError;
use crate::GasPricesTrait;

const ONE_BLOCK_MS: u64 = 400;
const CMD_CHANNEL_SIZE: usize = 1024;
const EXEC_RESULT_CHANNEL_SIZE: usize = 1024;
const EXEC_INTERVAL_MS: u64 = ONE_BLOCK_MS;

pub type EthTxHash = B256;
pub type GasPrice = u128;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ExecutionResult {
    tx_hash: EthTxHash,
    chain_id: ChainId,
    success: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct QueueRecord {
    pub sender: Address,
    pub tx_hash: EthTxHash,
    pub nonce: TxNonce,
    pub sorting_gas_price: GasPrice,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub capacity: usize,
    pub capacity_high_watermark: f64,
    pub eviction_timeout_sec: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TxRecord {
    /// Tx execution request
    pub tx_request: ExecuteRequest,
    /// Tx chain id (as it's in the transaction).
    pub tx_chain_id: Option<ChainId>,
    /// The sender (signer) of the transaction.
    pub sender: Address,
    /// The nonce of the transaction.
    pub nonce: TxNonce,
    /// The gas price of the transaction.
    pub gas_price: Option<GasPrice>,
    /// Gas price used for sorting transactions in the pool, if tx has no gas price it's initially
    /// set to 0 and later updated to suggested gas price * 2 (same applies to txs without chain_id).
    pub sorting_gas_price: GasPrice,
}

impl TxRecord {
    pub fn should_set_gas_price(&self) -> bool {
        self.gas_price.is_none() || self.tx_chain_id.is_none()
    }

    pub fn chain_id(&self) -> ChainId {
        self.tx_chain_id
            .unwrap_or(self.tx_request.fallback_chain_id())
    }
}

impl Deref for TxRecord {
    type Target = ExecuteRequest;

    fn deref(&self) -> &Self::Target {
        &self.tx_request
    }
}

impl TryFrom<ExecuteRequest> for TxRecord {
    type Error = MempoolError;

    fn try_from(tx_request: ExecuteRequest) -> Result<Self, Self::Error> {
        let sender = tx_request.recover_signer()?;
        let nonce = tx_nonce(tx_request.tx())?;
        let gas_price = tx_gas_price(tx_request.tx())?;
        let sorting_gas_price = gas_price.unwrap_or(0);
        let chain_id = tx_chain_id(tx_request.tx())?;
        Ok(Self {
            tx_request,
            sender,
            nonce,
            gas_price,
            sorting_gas_price,
            tx_chain_id: chain_id,
        })
    }
}

#[inline]
fn tx_nonce(tx: &TxEnvelope) -> Result<TxNonce, MempoolError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().nonce()),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().nonce()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().nonce()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().nonce()),
        _ => Err(MempoolError::UnsupportedTxType),
    }
}

#[inline]
fn tx_gas_price(tx: &TxEnvelope) -> Result<Option<u128>, MempoolError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().gas_price()),
        _ => Err(MempoolError::UnsupportedTxType),
    }
}

#[inline]
fn tx_chain_id(tx: &TxEnvelope) -> Result<Option<ChainId>, MempoolError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().chain_id),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().chain_id()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().chain_id()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().chain_id()),
        _ => Err(MempoolError::UnsupportedTxType),
    }
}

#[derive(Debug)]
enum Command {
    ScheduleTx(TxRecord, oneshot::Sender<Result<(), MempoolError>>),
    Shutdown,
    ExecuteTx,
}

pub struct Mempool<E: ExecutorTrait, G: GasPricesTrait> {
    config: Config,
    channel: Sender<Command>,
    executor: Arc<E>,
    txs: Arc<DashMap<EthTxHash, TxRecord>>,
    chains: Vec<ChainId>,
    chain_pools: Arc<DashMap<ChainId, ChainPool>>,
    gas_prices: G,
    token_map: HashMap<ChainId, Pubkey>,
    neon_api: NeonApi,
}

impl<E: ExecutorTrait, G: GasPricesTrait> Mempool<E, G> {
    pub fn new(
        config: Config,
        chains: Vec<ChainId>,
        gas_prices: G,
        executor: Arc<E>,
        neon_api: NeonApi,
    ) -> Self {
        let capacity_high_watermark =
            (config.capacity as f64 * config.capacity_high_watermark) as usize;
        let chain_pools = Arc::new(
            chains
                .iter()
                .map(|&chain_id| {
                    (
                        chain_id,
                        ChainPool::new(chain_id, config.capacity, capacity_high_watermark),
                    )
                })
                .collect(),
        );
        let (channel, rx) = mpsc::channel::<Command>(CMD_CHANNEL_SIZE);

        let this = Self {
            config,
            channel,
            executor,
            txs: Arc::new(DashMap::new()),
            chain_pools,
            chains,
            gas_prices,
            // todo: populate from evm config
            token_map: HashMap::new(),
            neon_api,
        };
        this.start(rx);
        this
    }

    pub fn get_pending_tx_cnt(&self, sender: &Address, chain_id: ChainId) -> Option<u64> {
        if let Some(chain_pool) = self.chain_pools.get(&chain_id) {
            if let Some(sender_pool) = chain_pool.sender_pools.get(sender) {
                return match sender_pool.state {
                    PoolState::Suspended | PoolState::Unresolved => None,
                    PoolState::Queued(_) | PoolState::Processing(_) | PoolState::Idle => {
                        let queue_noonce = sender_pool
                            .pending_nonce_queue
                            .peek_max()
                            .map(|x| *x.1)
                            .unwrap_or(0);
                        let chain_tx_count = sender_pool.chain_tx_count;
                        Some(max(queue_noonce, chain_tx_count))
                    }
                };
            }
        }
        None
    }

    /// Gets a transaction by hash.
    pub fn get_tx_by_hash(&self, tx_hash: EthTxHash) -> Option<TxEnvelope> {
        self.txs.get(&tx_hash).map(|x| x.value().tx().clone())
    }

    /// Gets a transaction by sender and nonce.
    pub fn get_tx_by_sender_nonce(
        &self,
        sender: &Address,
        nonce: TxNonce,
        chain_id: ChainId,
    ) -> Option<TxEnvelope> {
        if let Some(chain_pool) = self.chain_pools.get(&chain_id) {
            if let Some(sender_pool) = chain_pool.sender_pools.get(sender) {
                if let Some(record) = sender_pool.nonce_map.get(&nonce) {
                    return self
                        .txs
                        .get(&record.tx_hash)
                        .map(|x| x.value().tx().clone());
                }
            }
        }
        None
    }

    /// Schedules a new transaction for execution.
    pub async fn schedule_tx(&self, tx_request: ExecuteRequest) -> Result<(), MempoolError> {
        tracing::debug!(?tx_request, "scheduling tx");
        let tx_record = TxRecord::try_from(tx_request)?;

        if self.txs.contains_key(tx_record.tx_hash()) {
            return Err(MempoolError::AlreadyKnown);
        }

        let chain_id = tx_record.chain_id();
        if !self.chains.contains(&chain_id) {
            // todo: request evm config and check maybe there's a new chain id;
            // it should be requested elsewhere periodically
            return Err(MempoolError::UnknownChainID(chain_id));
        }

        if !self.chain_pools.contains_key(&chain_id) {
            return Err(MempoolError::UnknownChainID(chain_id));
        };

        let (cmd_tx, cmd_rx) = oneshot::channel();
        self.channel
            .send(Command::ScheduleTx(tx_record, cmd_tx))
            .await
            .unwrap();
        cmd_rx.await.unwrap()
    }

    pub async fn shutdown(&self) {
        self.channel.send(Command::Shutdown).await.unwrap();
    }

    // todo: Arc<Self>?
    fn start(&self, cmd_rx: mpsc::Receiver<Command>) {
        // todo: do we want to update chain ids list periodically? but don't remove any chain ids;
        // could be something like arc swap probably
        let chain_ids = self.chains.clone();
        let token_map = self.token_map.clone();
        let gas_prices = self.gas_prices.clone();
        let executor = Arc::clone(&self.executor);
        let eviction_timeout_sec = self.config.eviction_timeout_sec;

        // start update tx count loop for each chain
        for &chain_id in &chain_ids {
            // todo: unwrap
            let chain_pool = self.chain_pools.get(&chain_id).unwrap();
            let suspended_senders = Arc::clone(&chain_pool.suspended_senders);
            let sender_pools = Arc::clone(&chain_pool.sender_pools);
            let tx_price_queue = Arc::clone(&chain_pool.tx_price_queue);
            let pending_price_reversed_queue = Arc::clone(&chain_pool.pending_price_reversed_queue);
            let gapped_price_reversed_queue = Arc::clone(&chain_pool.gapped_price_reversed_queue);
            let txs_copy = Arc::clone(&self.txs);
            let neon_api = self.neon_api.clone();
            tokio::spawn(async move {
                run_update_tx_count_loop(
                    chain_id,
                    neon_api,
                    suspended_senders,
                    pending_price_reversed_queue,
                    gapped_price_reversed_queue,
                    sender_pools,
                    tx_price_queue,
                    txs_copy,
                )
                .await
            });

            let chain_pools = Arc::downgrade(&self.chain_pools);
            let txs = Arc::downgrade(&self.txs);
            tokio::spawn(async move {
                run_sender_heartbeat_loop(eviction_timeout_sec, chain_id, chain_pools, txs).await
            });
        }

        // start sender pool resolver loop
        let (resolver_tx, resolver_rx) = mpsc::channel::<(Address, ChainId)>(CMD_CHANNEL_SIZE);
        let chain_pools = Arc::downgrade(&self.chain_pools);
        let neon_api = self.neon_api.clone();
        let txs = Arc::downgrade(&self.txs);
        tokio::spawn(async move {
            run_sender_pool_resolver_loop(resolver_rx, txs, chain_pools, neon_api).await
        });

        // start main loop
        let txs = Arc::clone(&self.txs);
        let chain_pools = Arc::clone(&self.chain_pools);
        let cmd_tx = self.channel.clone();
        tokio::spawn(async move {
            let mut cmd_rx = cmd_rx;
            let (exec_result_tx, mut exec_result_rx) =
                mpsc::channel::<ExecutionResult>(EXEC_RESULT_CHANNEL_SIZE);
            let mut exec_interval =
                tokio::time::interval(tokio::time::Duration::from_millis(EXEC_INTERVAL_MS));
            let mut chain_idx = 0;
            loop {
                tokio::select! {
                    _ = exec_interval.tick() => {
                        execute_tx(&chain_ids, &mut chain_idx, &chain_pools, &txs, &exec_result_tx, Arc::clone(&executor)).await;
                        exec_interval.reset();
                    }
                    Some(execution_result) = exec_result_rx.recv() => {
                        process_execution_result(execution_result, &chain_pools, &txs, &resolver_tx).await;
                        cmd_tx.send(Command::ExecuteTx).await.unwrap();
                    }
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Command::Shutdown => {
                                tracing::info!("shutting down mempool");
                                return;
                            }
                            Command::ScheduleTx(tx, tx_result) => {
                                let result = schedule_tx(tx, &chain_pools, &txs, &gas_prices, &token_map, &resolver_tx).await;
                                tx_result.send(result).unwrap();
                            }
                            Command::ExecuteTx => {
                                execute_tx::<E>(&chain_ids, &mut chain_idx, &chain_pools, &txs, &exec_result_tx, Arc::clone(&executor)).await;
                                exec_interval.reset();
                            }
                        }
                    }
                }
            }
        });
    }
}

pub(crate) struct ChainPool {
    pub capacity: usize,
    pub capacity_high_watermark: usize,
    pub chain_id: ChainId,
    pub sender_pools: Arc<DashMap<Address, SenderPool>>,
    /// Priority queue of sender pools sorted by the last update time. The sender pool with the
    /// oldest update time is the first in the queue. This queue is used for cleaning up the mempool
    /// from sender pools that are not used for a long time.
    /// Sender pools are added to this queue when they are created and updated when a new
    /// transaction is added to the pool.
    pub sender_heartbeat_queue: Arc<RwLock<PriorityQueue<Address, Reverse<SystemTime>>>>,
    /// Priority queue for transactions sorted by gas price and ready for execution. Each sender
    /// can have only one transaction in this queue at a time. This way we can ensure that we don't
    /// execute transactions from one sender in parallel.
    pub tx_price_queue: Arc<RwLock<PriorityQueue<QueueRecord, GasPrice>>>,
    /// Priority queue of all pending transactions from all sender pools sorted by gas price in
    /// reverse order. This queue is used for cleaning up the mempool when it reaches its capacity.
    pub pending_price_reversed_queue: Arc<RwLock<PriorityQueue<QueueRecord, Reverse<GasPrice>>>>,
    /// Priority queue if all gapped transactions from all sender pools sorted by gas price in
    /// reverse order. This queue is used for cleaning up the mempool when it reaches its capacity.
    pub gapped_price_reversed_queue: Arc<RwLock<PriorityQueue<QueueRecord, Reverse<GasPrice>>>>,
    /// Hash set of suspended senders. When a sender pool is suspended, it's moved to this set.
    /// Sender gets suspended when its lower nonce is higher than the chain tx count. In such case
    /// we have a gap and should wait for the missing transactions to be executed.
    pub suspended_senders: Arc<DashSet<Address>>,
}

impl ChainPool {
    pub fn new(chain_id: ChainId, capacity: usize, capacity_high_watermark: usize) -> Self {
        Self {
            capacity,
            capacity_high_watermark,
            chain_id,
            sender_pools: Arc::new(DashMap::new()),
            sender_heartbeat_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            tx_price_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            pending_price_reversed_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            gapped_price_reversed_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            suspended_senders: Arc::new(DashSet::new()),
        }
    }

    /// Gets the next transaction to be executed from the chain pool.
    pub async fn get_for_execution(&mut self) -> Option<EthTxHash> {
        let (tx_record, _) = self.tx_price_queue.write().await.pop()?;
        self.pending_price_reversed_queue
            .write()
            .await
            .remove(&tx_record);
        if let Some(mut sender_pool) = self.sender_pools.get_mut(&tx_record.sender) {
            sender_pool.set_processing(tx_record.nonce);
        }
        Some(tx_record.tx_hash)
    }

    pub async fn queue_new_tx(&self, sender_pool: &mut SenderPool) {
        let sender = sender_pool.sender;
        tracing::debug!(%sender, "queueing new tx");

        if sender_pool.state != PoolState::Idle {
            tracing::warn!(%sender, chain_id = %self.chain_id, state = ?sender_pool.state, "queue_new_tx: sender pool not idle");
            return;
        }

        if let Some((top, nonce)) = sender_pool.pending_nonce_queue.pop_min() {
            let sorting_gas_price = top.sorting_gas_price;
            if nonce == sender_pool.chain_tx_count {
                self.pending_price_reversed_queue.write().await.remove(&top);
                self.tx_price_queue
                    .write()
                    .await
                    .push(top, sorting_gas_price);
                sender_pool.state = PoolState::Queued(nonce);
                tracing::debug!(%sender, %nonce, "tx queued");
            } else {
                sender_pool.gapped_nonce_queue.push(top.clone(), nonce);
                self.pending_price_reversed_queue.write().await.remove(&top);
                self.gapped_price_reversed_queue
                    .write()
                    .await
                    .push(top, Reverse(sorting_gas_price));
                self.suspended_senders.insert(sender);
                tracing::debug!(%sender, "failed to queue a tx, sender pool suspended");
                sender_pool.state = PoolState::Suspended;
            }
        } else if sender_pool.gapped_nonce_queue.is_empty() {
            tracing::debug!(%sender, "nothing to queue, sender pool is idle");
            sender_pool.state = PoolState::Idle;
        } else {
            tracing::debug!(%sender, "nothing to queue, attempting to move txs from gapped to pending");
            let mut pending = self.pending_price_reversed_queue.write().await;
            let mut gapped = self.gapped_price_reversed_queue.write().await;
            move_from_gapped_to_pending(sender_pool, &mut pending, &mut gapped);
            self.attempt_to_queue_or_update_state(sender_pool).await;
        }
        tracing::debug!(%sender, state = ?sender_pool.state, "done queueing new tx");
    }

    pub async fn add(&mut self, record: QueueRecord) {
        let Some(mut sender_pool) = self.sender_pools.get_mut(&record.sender) else {
            return;
        };

        tracing::debug!(tx_hash = %record.tx_hash, nonce = %record.nonce, sender_state = ?sender_pool.state, "adding tx to pool");

        self.sender_heartbeat_queue
            .write()
            .await
            .change_priority(&record.sender, Reverse(SystemTime::now()));

        let nonce = record.nonce;
        let sorting_gas_price = record.sorting_gas_price;
        sender_pool.nonce_map.insert(nonce, record.clone());

        match sender_pool.state {
            PoolState::Idle => {
                let sender = record.sender;
                sender_pool.pending_nonce_queue.push(record.clone(), nonce);
                self.pending_price_reversed_queue
                    .write()
                    .await
                    .push(record.clone(), Reverse(sorting_gas_price));
                let can_queue =
                    if let Some((_, &top_nonce)) = sender_pool.pending_nonce_queue.peek_min() {
                        top_nonce == sender_pool.chain_tx_count
                    } else {
                        false
                    };
                if can_queue {
                    // todo: unwrap
                    let (queue_record, queued_nonce) =
                        sender_pool.pending_nonce_queue.pop_min().unwrap();
                    self.tx_price_queue
                        .write()
                        .await
                        .push(queue_record.clone(), sorting_gas_price);
                    sender_pool.state = PoolState::Queued(queued_nonce)
                } else {
                    let mut pending = self.pending_price_reversed_queue.write().await;
                    let mut gapped = self.gapped_price_reversed_queue.write().await;
                    move_from_pending_to_gapped(&mut sender_pool, &mut pending, &mut gapped);
                    self.suspended_senders.insert(sender);
                    sender_pool.state = PoolState::Suspended;
                }
            }
            PoolState::Unresolved => {
                tracing::debug!(tx_hash = %record.tx_hash, nonce = %record.nonce, "tx added to unresolved pool");
                sender_pool.gapped_nonce_queue.push(record.clone(), nonce);
                self.gapped_price_reversed_queue
                    .write()
                    .await
                    .push(record, Reverse(sorting_gas_price));
            }
            PoolState::Suspended => {
                if nonce == sender_pool.chain_tx_count {
                    self.suspended_senders.remove(&record.sender);
                    sender_pool.state = PoolState::Queued(nonce);
                    self.tx_price_queue
                        .write()
                        .await
                        .push(record.clone(), sorting_gas_price);
                    self.pending_price_reversed_queue
                        .write()
                        .await
                        .push(record, Reverse(sorting_gas_price));
                } else {
                    sender_pool.gapped_nonce_queue.push(record.clone(), nonce);
                    self.gapped_price_reversed_queue
                        .write()
                        .await
                        .push(record, Reverse(sorting_gas_price));
                }
            }
            PoolState::Queued(_) | PoolState::Processing(_) => {
                sender_pool.pending_nonce_queue.push(record.clone(), nonce);
                self.pending_price_reversed_queue
                    .write()
                    .await
                    .push(record, Reverse(sorting_gas_price));
            }
        }
    }

    pub async fn purge_over_capacity_txs(&mut self) -> Vec<QueueRecord> {
        let mut to_remove = Vec::new();
        let len = self.len().await;
        if len <= self.capacity {
            tracing::debug!(
                len,
                capacity = self.capacity,
                "purge_over_capacity_txs: nothing to remove"
            );
            return to_remove;
        }
        let to_remove_cnt = self.len().await - self.capacity;
        tracing::debug!(
            len,
            capacity = self.capacity,
            to_remove_cnt,
            "purge_over_capacity_txs: removing txs"
        );
        let mut removed = 0;
        if to_remove_cnt > 0 {
            tracing::debug!(tx_to_remove = %to_remove_cnt, chain_id = %self.chain_id, "clearing gapped txs from mempool");
            let mut glbq = self.gapped_price_reversed_queue.write().await;
            for _ in 0..to_remove_cnt {
                if let Some((record, _)) = glbq.pop() {
                    to_remove.push(record);
                    removed += 1;
                } else {
                    break;
                }
            }
        }
        let to_remove_cnt = to_remove_cnt - removed;
        if to_remove_cnt > 0 {
            tracing::debug!(tx_to_remove = %to_remove_cnt, chain_id = %self.chain_id, "clearing pending txs from mempool");
            let mut glbq = self.pending_price_reversed_queue.write().await;
            for _ in 0..to_remove_cnt {
                if let Some((record, _)) = glbq.pop() {
                    to_remove.push(record);
                    removed += 1;
                } else {
                    break;
                }
            }
        }

        for record in &to_remove {
            self.remove(record).await;
        }
        to_remove
    }

    pub async fn remove_by(&mut self, sender: &Address, nonce: TxNonce) {
        if let Some(mut sender_pool) = self.sender_pools.get_mut(sender) {
            let Some(existing) = sender_pool.remove_by_nonce(nonce) else {
                return;
            };
            self.tx_price_queue.write().await.remove(&existing);
            self.pending_price_reversed_queue
                .write()
                .await
                .remove(&existing);
            self.gapped_price_reversed_queue
                .write()
                .await
                .remove(&existing);
        }
    }

    pub async fn remove(&mut self, record: &QueueRecord) {
        self.tx_price_queue.write().await.remove(record);
        if let Some(mut sender_pool) = self.sender_pools.get_mut(&record.sender) {
            sender_pool.remove(record);
        }
    }

    pub async fn remove_sender_pool(&self, sender_pool: &mut SenderPool) -> Vec<EthTxHash> {
        let sender = sender_pool.sender;
        tracing::debug!(%sender, "removing sender pool");
        let mut removed = Vec::new();

        if matches!(sender_pool.state, PoolState::Processing(_)) {
            return removed;
        }

        if sender_pool.is_empty() {
            return removed;
        }

        let mut pending = self.pending_price_reversed_queue.write().await;
        if let PoolState::Queued(queued_nonce) = sender_pool.state {
            if let Some(record) = sender_pool.get_by_nonce(queued_nonce) {
                self.tx_price_queue.write().await.remove(record);
                pending.remove(record);
            }
        }

        for (record, _) in sender_pool.pending_nonce_queue.drain() {
            pending.remove(&record);
            removed.push(record.tx_hash);
        }
        drop(pending);

        let mut gapped = self.gapped_price_reversed_queue.write().await;
        for (record, _) in sender_pool.gapped_nonce_queue.drain() {
            gapped.remove(&record);
            removed.push(record.tx_hash);
        }
        tracing::debug!(%sender, removed_txs = %removed.len(), "sender pool removed");

        removed
    }

    #[inline]
    pub async fn len(&self) -> usize {
        self.pending_price_reversed_queue.read().await.len()
            + self.gapped_price_reversed_queue.read().await.len()
    }

    /// Gets or creates a sender pool for the given sender.
    pub async fn get_or_create_sender_pool(
        &mut self,
        sender: &Address,
    ) -> (RefMut<'_, Address, SenderPool>, bool) {
        let exists = self.sender_pools.contains_key(sender);

        if exists {
            self.sender_heartbeat_queue
                .write()
                .await
                .push(*sender, Reverse(SystemTime::now()));
        }

        let pool_ref = self
            .sender_pools
            .entry(*sender)
            .or_insert_with(|| SenderPool::new(self.chain_id, *sender));

        (pool_ref, !exists)
    }

    async fn attempt_to_queue_or_update_state(&self, sender_pool: &mut SenderPool) {
        let mut tx_queue = self.tx_price_queue.write().await;
        let suspended_senders = Arc::clone(&self.suspended_senders);
        attempt_to_queue_or_update_state(sender_pool, &mut tx_queue, &suspended_senders);
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum PoolState {
    /// We do not know current chain_tx_count. All transactions are added to the gapped queue.
    /// It's not possible to schedule transactions from this pool to the chain pool.
    Unresolved,
    /// The sender pools status should be determined by the chain tx count and updated accordingly.
    Idle,
    /// The sender pool is suspended, meaning it's pending nonce queue is empty and the pool has
    /// a gap between the chain tx count and the lowest nonce in the pool. The pool should be
    /// suspended until the gap is filled.
    Suspended,
    /// Transaction from the sender pool are scheduled to the chain pool.
    Queued(TxNonce),
    /// The sender pool has a transaction being executed by the executor.
    Processing(TxNonce),
}

#[derive(Debug)]
pub(crate) struct SenderPool {
    pub chain_id: ChainId,
    pub sender: Address,
    pub state: PoolState,
    /// Priority queue of nonces. Contains only nonces/records that are not yet being scheduled in
    /// the chain pool tx_price_queue for execution and can be scheduled for execution.
    pub pending_nonce_queue: DoublePriorityQueue<QueueRecord, TxNonce>,
    /// Priority queue of nonces. Contains only gapped records.
    pub gapped_nonce_queue: DoublePriorityQueue<QueueRecord, TxNonce>,
    /// Map of nonces to queue records. Contains only nonces that are not yet being executed, i.e.
    /// nonces that are in the nonce queue or in the chain pool tx_price_queue. Processing
    /// transactions are not included in this map.
    pub nonce_map: HashMap<TxNonce, QueueRecord>,
    pub chain_tx_count: u64,
}

impl SenderPool {
    pub fn new(chain_id: ChainId, sender: Address) -> Self {
        Self {
            chain_id,
            sender,
            state: PoolState::Unresolved,
            pending_nonce_queue: DoublePriorityQueue::new(),
            gapped_nonce_queue: DoublePriorityQueue::new(),
            nonce_map: HashMap::new(),
            chain_tx_count: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self.state, PoolState::Idle) && self.nonce_map.is_empty()
    }

    #[inline]
    pub fn is_unresolved_empty(&self) -> bool {
        matches!(self.state, PoolState::Unresolved) && self.nonce_map.is_empty()
    }

    #[inline]
    pub fn get_by_nonce(&self, nonce: TxNonce) -> Option<&QueueRecord> {
        self.nonce_map.get(&nonce)
    }

    #[inline]
    pub fn remove_by_nonce(&mut self, nonce: TxNonce) -> Option<QueueRecord> {
        if let Some(record) = self.nonce_map.remove(&nonce) {
            self.pending_nonce_queue.remove(&record);
            self.gapped_nonce_queue.remove(&record);

            if self.state == PoolState::Queued(nonce) {
                self.state = PoolState::Idle;
            }

            return Some(record);
        }
        None
    }

    #[inline]
    pub fn remove(&mut self, record: &QueueRecord) {
        if self.state == PoolState::Queued(record.nonce) {
            self.state = PoolState::Idle;
        }
        self.pending_nonce_queue.remove(record);
        self.gapped_nonce_queue.remove(record);
        self.nonce_map.remove(&record.nonce);
    }

    #[inline]
    pub fn set_processing(&mut self, nonce: TxNonce) {
        self.state = PoolState::Processing(nonce);
        self.nonce_map.remove(&nonce);
        if self.chain_tx_count == nonce {
            self.chain_tx_count += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_update_tx_count_loop(
    chain_id: ChainId,
    neon_api: NeonApi,
    suspended_senders: Arc<DashSet<Address>>,
    pending_price_reversed_queue: Arc<RwLock<PriorityQueue<QueueRecord, Reverse<GasPrice>>>>,
    gapped_price_reversed_queue: Arc<RwLock<PriorityQueue<QueueRecord, Reverse<GasPrice>>>>,
    sender_pools: Arc<DashMap<Address, SenderPool>>,
    tx_price_queue: Arc<RwLock<PriorityQueue<QueueRecord, GasPrice>>>,
    txs_global: Arc<DashMap<EthTxHash, TxRecord>>,
) {
    use common::evm_loader::types::Address;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(3 * ONE_BLOCK_MS));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut senders_to_remove = Vec::new();
                tracing::debug!(chain_id = chain_id, "updating tx count");
                for sender_addr in suspended_senders.iter() {
                    tracing::debug!(sender_addr=%*sender_addr, "updating tx count");
                    let Some(mut sender_pool) = sender_pools.get_mut(&*sender_addr) else {
                        tracing::debug!(sender_addr=%*sender_addr, "sender pool not found, removing from suspended senders");
                        senders_to_remove.push(*sender_addr);
                        continue;
                    };

                    if sender_pool.state != PoolState::Suspended {
                        tracing::debug!(sender_addr=%*sender_addr, state = ?sender_pool.state, "sender pool not suspended, removing from suspended senders");
                        senders_to_remove.push(*sender_addr);
                        continue;
                    }

                    let balance_addr = BalanceAddress {
                        chain_id,
                        address: Address::from(<[u8; 20]>::from(sender_addr.0)),
                    };
                    let tx_count = match neon_api.get_transaction_count(balance_addr, None).await {
                        Ok(tx_count) => tx_count,
                        Err(err) => {
                            tracing::error!(?err, "failed to get tx count");
                            continue;
                        }
                    };

                    if tx_count == sender_pool.chain_tx_count {
                        tracing::debug!(sender_addr=%*sender_addr, %tx_count, "tx count not changed");
                        continue;
                    }
                    sender_pool.chain_tx_count = tx_count;
                    tracing::debug!(sender_addr=%*sender_addr, %tx_count, "tx count updated");


                    let mut pending = pending_price_reversed_queue.write().await;
                    let mut gapped = gapped_price_reversed_queue.write().await;
                    remove_records_with_nonce_le(
                        &mut sender_pool,
                        tx_count,
                        Arc::clone(&txs_global),
                        &mut pending,
                        &mut gapped,
                    );
                    rebalance_pending_gapped_queues(&mut sender_pool, &mut pending, &mut gapped);
                    let mut tx_price_queue = tx_price_queue.write().await;
                    if !attempt_to_queue(&mut sender_pool, &mut tx_price_queue) && sender_pool.is_empty() {
                        sender_pool.state = PoolState::Idle;
                        tracing::debug!(%sender_pool.sender, "nothing to queue, sender pool is idle");
                    }
                    if sender_pool.state != PoolState::Suspended {
                        tracing::debug!(sender_addr=%*sender_addr, state = ?sender_pool.state, "sender pool not suspended, removing from suspended senders");
                        senders_to_remove.push(*sender_addr);
                    }
                }
                for sender_addr in senders_to_remove {
                    suspended_senders.remove(&sender_addr);
                    tracing::debug!(%sender_addr, "removed from suspended senders");
                }

                tracing::debug!(chain_id = chain_id, "tx count updated");
            }
        }
    }
}

async fn run_sender_pool_resolver_loop(
    mut rx: mpsc::Receiver<(Address, ChainId)>,
    txs: Weak<DashMap<EthTxHash, TxRecord>>,
    chain_pools: Weak<DashMap<ChainId, ChainPool>>,
    neon_api: NeonApi,
) {
    use common::evm_loader::types::Address;

    while let Some((sender, chain_id)) = rx.recv().await {
        tracing::debug!(%sender, %chain_id, "resolving sender pool");
        let Some(chain_pools_arc) = chain_pools.upgrade() else {
            tracing::info!("run_sender_pool_resolver_loop stopped");
            break;
        };
        let Some(chain_pool) = chain_pools_arc.get(&chain_id) else {
            continue;
        };
        let Some(mut sender_pool) = chain_pool.sender_pools.get_mut(&sender) else {
            continue;
        };
        if sender_pool.state == PoolState::Unresolved {
            let balance_addr = BalanceAddress {
                chain_id,
                address: Address::from(<[u8; 20]>::from(sender.0)),
            };
            // todo: unwrap a result
            let chain_tx_count = neon_api
                .get_transaction_count(balance_addr, None)
                .await
                .unwrap();

            tracing::debug!(%sender, %chain_id, %chain_tx_count, "chain tx count resolved");

            sender_pool.chain_tx_count = chain_tx_count;

            let Some(txs_arc) = txs.upgrade() else {
                tracing::info!("run_sender_pool_resolver_loop stopped");
                break;
            };
            let mut pending = chain_pool.pending_price_reversed_queue.write().await;
            let mut gapped = chain_pool.gapped_price_reversed_queue.write().await;
            remove_records_with_nonce_le(
                &mut sender_pool,
                chain_tx_count,
                txs_arc,
                &mut pending,
                &mut gapped,
            );
            tracing::debug!(
                pending_count = pending.len(),
                gapped_count = gapped.len(),
                "before rebalancing queues"
            );
            rebalance_pending_gapped_queues(&mut sender_pool, &mut pending, &mut gapped);
            let mut tx_price_queue = chain_pool.tx_price_queue.write().await;
            tracing::debug!(
                pending_count = pending.len(),
                gapped_count = gapped.len(),
                "after rebalancing queues"
            );
            attempt_to_queue_or_update_state(
                &mut sender_pool,
                &mut tx_price_queue,
                &chain_pool.suspended_senders,
            );
            tracing::debug!(%sender, %chain_id, state = ?sender_pool.state, "sender pool resolved");
            if sender_pool.is_empty() {
                chain_pool.remove_sender_pool(&mut sender_pool).await;
            }
        }
    }
}

async fn run_sender_heartbeat_loop(
    eviction_timeout_sec: u64,
    chain_id: ChainId,
    chain_pools: Weak<DashMap<ChainId, ChainPool>>,
    txs: Weak<DashMap<EthTxHash, TxRecord>>,
) {
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(eviction_timeout_sec / 10));
    let eviction_timeout_sec = std::time::Duration::from_secs(eviction_timeout_sec);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tracing::debug!(%chain_id, "sender heartbeat");
                let Some(chain_pools) = chain_pools.upgrade() else {
                    tracing::info!("run_sender_heartbeat_loop stopped");
                    break;
                };

                let Some(chain_pool) = chain_pools.get(&chain_id) else {
                    tracing::error!("chain pool not found");
                    continue;
                };

                let now = SystemTime::now();
                let threshold = now - eviction_timeout_sec;
                let mut heartbeat_queue = chain_pool.sender_heartbeat_queue.write().await;
                while let Some((sender, Reverse(updated_at))) = heartbeat_queue.pop() {
                    // todo: unwrap
                    let mut sender_pool = chain_pool.sender_pools.get_mut(&sender).unwrap();
                    if matches!(sender_pool.state, PoolState::Processing(_)) && updated_at >= threshold {
                        heartbeat_queue.push(sender, Reverse(updated_at));
                        break;
                    }
                    tracing::debug!("dropping sender pool");
                    let removed_hashes = chain_pool.remove_sender_pool(&mut sender_pool).await;
                    let Some(txs_arc) = txs.upgrade() else {
                        tracing::info!("run_sender_heartbeat_loop stopped");
                        break;
                    };
                    for removed_hash in removed_hashes {
                        txs_arc.remove(&removed_hash);
                    }
                }
            }
        }
    }
}

pub(crate) fn move_from_gapped_to_pending(
    sender_pool: &mut SenderPool,
    pending_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    gapped_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
) {
    let mut expected_nonce = sender_pool.chain_tx_count;
    tracing::debug!(%expected_nonce, "checking nonces");
    tracing::debug!(
        gapped_len = sender_pool.gapped_nonce_queue.len(),
        "gapped queue len"
    );
    while let Some((_, &tx_nonce)) = sender_pool.gapped_nonce_queue.peek_min() {
        tracing::debug!(%tx_nonce, %expected_nonce, "checking nonce");
        if tx_nonce == expected_nonce {
            // Expect should be safe here since peek_min already checked the condition.
            let (moved_record, moved_nonce) = sender_pool
                .gapped_nonce_queue
                .pop_min()
                .expect("checked above");

            sender_pool
                .pending_nonce_queue
                .push(moved_record.clone(), moved_nonce);

            let sorting_gas_price = moved_record.sorting_gas_price;
            gapped_price_reversed_queue.remove(&moved_record);
            pending_price_reversed_queue.push(moved_record, Reverse(sorting_gas_price));

            expected_nonce += 1;
        } else {
            break;
        }
    }
}

pub(crate) fn move_from_pending_to_gapped(
    sender_pool: &mut SenderPool,
    pending_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    gapped_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
) {
    let expected_nonce = sender_pool.chain_tx_count;
    while let Some((_, &tx_nonce)) = sender_pool.pending_nonce_queue.peek_min() {
        if tx_nonce > expected_nonce {
            let (moved_record, moved_nonce) = sender_pool
                .pending_nonce_queue
                .pop_min()
                .expect("checked above");

            sender_pool
                .gapped_nonce_queue
                .push(moved_record.clone(), moved_nonce);

            let sorting_gas_price = moved_record.sorting_gas_price;
            pending_price_reversed_queue.remove(&moved_record);
            gapped_price_reversed_queue.push(moved_record, Reverse(sorting_gas_price));
        } else {
            break;
        }
    }
}

pub(crate) fn rebalance_pending_gapped_queues(
    sender_pool: &mut SenderPool,
    pending_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    gapped_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
) {
    move_from_gapped_to_pending(
        sender_pool,
        pending_price_reversed_queue,
        gapped_price_reversed_queue,
    );
    move_from_pending_to_gapped(
        sender_pool,
        pending_price_reversed_queue,
        gapped_price_reversed_queue,
    );
}

#[inline]
pub(crate) fn attempt_to_queue_or_update_state(
    sender_pool: &mut SenderPool,
    tx_price_queue: &mut PriorityQueue<QueueRecord, GasPrice>,
    suspended_senders: &DashSet<Address>,
) {
    if !attempt_to_queue(sender_pool, tx_price_queue) {
        if sender_pool.is_empty() {
            sender_pool.state = PoolState::Idle;
            tracing::debug!(%sender_pool.sender, "nothing to queue, sender pool is idle");
        } else if sender_pool.is_unresolved_empty() {
            tracing::debug!(%sender_pool.sender, "nothing to queue, sender pool is unresolved");
        } else {
            sender_pool.state = PoolState::Suspended;
            suspended_senders.insert(sender_pool.sender);
            tracing::debug!(%sender_pool.sender, "failed to queue a tx, sender pool suspended");
        }
    }
}

#[inline]
pub(crate) fn attempt_to_queue(
    sender_pool: &mut SenderPool,
    tx_price_queue: &mut PriorityQueue<QueueRecord, GasPrice>,
) -> bool {
    if let Some((top, nonce)) = sender_pool.pending_nonce_queue.pop_min() {
        // todo: do we want to check that nonce == sender_pool.chain_tx_count?
        let sorting_gas_price = top.sorting_gas_price;
        tx_price_queue.push(top, sorting_gas_price);
        sender_pool.state = PoolState::Queued(nonce);
        tracing::debug!(%sender_pool.chain_id, %nonce, "tx queued");
        return true;
    }
    false
}

/// Removes all records with nonce less to the given nonce.
/// Returns the list of removed tx hashes.
pub(crate) fn remove_records_with_nonce_le(
    sender_pool: &mut SenderPool,
    nonce: TxNonce,
    txs: Arc<DashMap<EthTxHash, TxRecord>>,
    pending_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    gapped_price_reversed_queue: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
) {
    let mut to_remove = Vec::new();
    while let Some((_, current_nonce)) = sender_pool.pending_nonce_queue.peek_min() {
        if *current_nonce < nonce {
            if let Some((record, removed_nonce)) = sender_pool.pending_nonce_queue.pop_min() {
                to_remove.push(removed_nonce);
                pending_price_reversed_queue.remove(&record);
                txs.remove(&record.tx_hash);
            }
        } else {
            break;
        }
    }
    while let Some((_, current_nonce)) = sender_pool.gapped_nonce_queue.peek_min() {
        if *current_nonce < nonce {
            if let Some((record, removed_nonce)) = sender_pool.gapped_nonce_queue.pop_min() {
                to_remove.push(removed_nonce);
                gapped_price_reversed_queue.remove(&record);
                txs.remove(&record.tx_hash);
            }
        } else {
            break;
        }
    }
    for nonce in to_remove {
        sender_pool.nonce_map.remove(&nonce);
    }
}

async fn execute_tx<E: ExecutorTrait>(
    chain_ids: &[ChainId],
    chain_idx: &mut usize,
    chain_pools: &DashMap<ChainId, ChainPool>,
    txs: &DashMap<EthTxHash, TxRecord>,
    exec_result_tx: &Sender<ExecutionResult>,
    executor: Arc<E>,
) {
    for _ in 0..chain_ids.len() {
        if *chain_idx >= chain_ids.len() {
            *chain_idx = 0;
        }
        let chain_id = chain_ids[*chain_idx];
        *chain_idx += 1;
        if let Some(mut chain_pool) = chain_pools.get_mut(&chain_id) {
            if let Some(tx_hash) = chain_pool.get_for_execution().await {
                if let Some(tx) = txs.get(&tx_hash) {
                    let tx_executor = Arc::clone(&executor);
                    let tx_request = tx.tx_request.clone();
                    let tx_eth_hash = *tx.tx_hash();
                    let exec_result_tx = exec_result_tx.clone();
                    tracing::debug!(%tx_eth_hash, %chain_id, "scheduling tx for execution");
                    tokio::spawn(async move {
                        tracing::debug!(%tx_eth_hash, %chain_id, "executing tx");
                        let (result_tx, result_rx) = oneshot::channel();
                        if let Err(err) = tx_executor
                            .handle_transaction(tx_request, Some(result_tx))
                            .await
                        {
                            tracing::debug!(?err, "failed to execute tx");
                            exec_result_tx
                                .send(ExecutionResult {
                                    tx_hash: tx_eth_hash,
                                    chain_id,
                                    success: false,
                                })
                                .await
                                .unwrap();
                        };
                        tracing::error!(%tx_eth_hash, %chain_id, "successfully executed tx");
                        let result = result_rx.await.unwrap();
                        exec_result_tx
                            .send(ExecutionResult {
                                tx_hash: tx_eth_hash,
                                chain_id,
                                success: result.is_success(),
                            })
                            .await
                            .unwrap();
                    });
                }
            }
        };
    }
}

pub(crate) async fn process_execution_result(
    execution_result: ExecutionResult,
    chain_pools: &DashMap<ChainId, ChainPool>,
    txs: &DashMap<EthTxHash, TxRecord>,
    resolver_tx: &Sender<(Address, ChainId)>,
) {
    let Some((_tx_hash, record)) = txs.remove(&execution_result.tx_hash) else {
        tracing::error!(chain_id = %execution_result.chain_id, "chain pool not found");
        return;
    };

    let Some(chain_pool) = chain_pools.get_mut(&execution_result.chain_id) else {
        tracing::error!(chain_id = %execution_result.chain_id, "chain pool not found");
        return;
    };

    let Some(mut sender_pool) = chain_pool.sender_pools.get_mut(&record.sender) else {
        tracing::error!(chain_id = %execution_result.chain_id, sebser = ?record.sender, "sender pool not found");
        return;
    };

    if execution_result.success {
        sender_pool.state = PoolState::Idle;
        chain_pool.queue_new_tx(&mut sender_pool).await;
        if sender_pool.is_empty() {
            chain_pool.remove_sender_pool(&mut sender_pool).await;
        }
    } else {
        sender_pool.state = PoolState::Unresolved;
        resolver_tx
            .send((record.sender, execution_result.chain_id))
            .await
            .unwrap()
    }
}

pub(crate) async fn schedule_tx<G: GasPricesTrait>(
    mut tx: TxRecord,
    chain_pools: &DashMap<ChainId, ChainPool>,
    txs: &DashMap<EthTxHash, TxRecord>,
    gas_prices: &G,
    token_map: &HashMap<ChainId, Pubkey>,
    resolver_tx: &Sender<(Address, ChainId)>,
) -> Result<(), MempoolError> {
    tracing::debug!(tx_hash = %tx.tx_hash(), "schedule tx command");
    let chain_id = tx.chain_id();
    let Some(mut chain_pool) = chain_pools.get_mut(&chain_id) else {
        return Err(MempoolError::UnknownChainID(chain_id));
    };

    let (sender_pool, created) = chain_pool.get_or_create_sender_pool(&tx.sender).await;

    if let PoolState::Processing(nonce) = sender_pool.state {
        if nonce == tx.nonce {
            return Err(MempoolError::NonceTooLow(nonce, nonce + 1));
        }
    }

    if sender_pool.chain_tx_count > tx.nonce {
        return Err(MempoolError::NonceTooLow(
            sender_pool.chain_tx_count,
            tx.nonce,
        ));
    }

    if tx.should_set_gas_price() {
        let Some(gas_price) = gas_prices.get_gas_price(Some(chain_id)) else {
            return Err(MempoolError::UnknownChainID(chain_id));
        };
        tx.sorting_gas_price = gas_price * 2;
    }

    let tx_hash = *tx.tx_hash();

    let drop_existing = if let Some(existing_record) = sender_pool.get_by_nonce(tx.nonce) {
        if existing_record.tx_hash == tx_hash {
            return Err(MempoolError::AlreadyKnown);
        }
        let existing_record = txs.get(&existing_record.tx_hash).unwrap();
        if existing_record.value().sorting_gas_price >= tx.sorting_gas_price {
            return Err(MempoolError::Underprice);
        }
        true
    } else {
        false
    };

    let sender_chain_tx_count = sender_pool.chain_tx_count;
    drop(sender_pool);
    let chain_pool_len = chain_pool.len().await;
    if chain_pool_len > chain_pool.capacity_high_watermark {
        let gapped_queue = chain_pool.gapped_price_reversed_queue.read().await;
        let gapped_tx = gapped_queue.peek();
        if tx.nonce > sender_chain_tx_count {
            if let Some((gapped_tx, _)) = gapped_tx {
                if tx.sorting_gas_price < gapped_tx.sorting_gas_price {
                    return Err(MempoolError::Underprice);
                }
            } else {
                return Err(MempoolError::NonceTooHigh(sender_chain_tx_count, tx.nonce));
            }
        } else if chain_pool_len >= chain_pool.capacity && gapped_tx.is_none() {
            let pending_queue = chain_pool.pending_price_reversed_queue.read().await;
            let pending_tx = pending_queue.peek();
            if let Some((pending_tx, _)) = pending_tx {
                if tx.sorting_gas_price < pending_tx.sorting_gas_price {
                    return Err(MempoolError::Underprice);
                }
            }
        }
    }

    if drop_existing {
        chain_pool.remove_by(&tx.sender, tx.nonce).await;
    }

    let to_remove = chain_pool.purge_over_capacity_txs().await;
    for record in to_remove {
        txs.remove(&record.tx_hash);
    }

    tracing::debug!(%tx_hash, "adding tx to pool");
    let record = QueueRecord {
        sender: tx.sender,
        tx_hash,
        nonce: tx.nonce,
        sorting_gas_price: tx.sorting_gas_price,
    };
    let sender = tx.sender;
    txs.insert(tx_hash, tx);
    chain_pool.add(record).await;
    tracing::debug!(%tx_hash, "tx added to pool");
    if created {
        resolver_tx.send((sender, chain_id)).await.unwrap();
    }
    Ok(())
}
