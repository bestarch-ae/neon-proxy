use std::cmp::Reverse;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use priority_queue::PriorityQueue;
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, BlockNumberOrTag, ChainId};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};

use common::neon_lib::types::BalanceAddress;
use common::solana_sdk::pubkey::Pubkey;
use executor::ExecutorTrait;
use neon_api::{NeonApi, NeonApiError};

use crate::mempool::{Command, EthTxHash, GasPrice, TxRecord};
use crate::pools::{
    ExecutionResult, QueueRecord, QueueUpdateAdd, QueueUpdateMove, QueuesUpdate, SenderPool,
    SenderPoolState, SenderResolverRecord, SendersResolver, SendersResolverCommand, StateUpdate,
};
use crate::{GasPricesTrait, MempoolError};

const RESOLVER_CHANNEL_SIZE: usize = 1024;
const EXEC_RESULT_CHANNEL_SIZE: usize = 1024;
const ONE_BLOCK_MS: u64 = 400;
const EXEC_INTERVAL_MS: u64 = ONE_BLOCK_MS;

pub trait GetTxCountTrait: Clone + Send + Sync + 'static {
    fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> impl Future<Output = Result<u64, NeonApiError>> + Send;
}

#[derive(Clone)]
pub struct NeonApiGetTxCount(pub NeonApi);

impl GetTxCountTrait for NeonApiGetTxCount {
    async fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<u64, NeonApiError> {
        self.0.get_transaction_count(addr, tag).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub chain_id: ChainId,
    pub capacity: usize,
    pub capacity_high_watermark: f64,
    pub token_pkey: Pubkey,
    pub eviction_timeout_sec: u64,
}

pub struct ChainPool<E: ExecutorTrait, G: GasPricesTrait, C: GetTxCountTrait> {
    capacity: usize,
    capacity_high_watermark: usize,
    chain_id: ChainId,
    sender_pools: HashMap<Address, SenderPool>,
    /// Priority queue of sender pools sorted by the last update time. The sender pool with the
    /// oldest update time is the first in the queue. This queue is used for cleaning up the mempool
    /// from sender pools that are not used for a long time.
    /// Sender pools are added to this queue when they are created and updated when a new
    /// transaction is added to the pool.
    sender_heartbeat_queue: PriorityQueue<Address, Reverse<SystemTime>>,
    /// Priority queue for transactions sorted by gas price and ready for execution. Each sender
    /// can have only one transaction in this queue at a time. This way we can ensure that we don't
    /// execute transactions from one sender in parallel.
    tx_price_queue: PriorityQueue<QueueRecord, GasPrice>,
    /// Priority queue of all pending transactions from all sender pools sorted by gas price in
    /// reverse order. This queue is used for cleaning up the mempool when it reaches its capacity.
    pending_price_reversed_queue: PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    /// Priority queue if all gapped transactions from all sender pools sorted by gas price in
    /// reverse order. This queue is used for cleaning up the mempool when it reaches its capacity.
    gapped_price_reversed_queue: PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    /// Chain token pubkey
    token_pkey: Pubkey,
    gas_prices: G,
    tx_count_api: C,
    /// Transaction executor
    executor: Arc<E>,
    /// Transaction records repository
    txs: Arc<DashMap<EthTxHash, TxRecord>>,
    /// Timeout for evicting sender pools from the mempool
    eviction_timeout_sec: u64,
    resolver_tx: Sender<SendersResolverCommand>,
}

impl<E: ExecutorTrait, G: GasPricesTrait, C: GetTxCountTrait> ChainPool<E, G, C> {
    pub fn new(
        config: Config,
        gas_prices: G,
        get_tx_api: C,
        executor: Arc<E>,
        txs: Arc<DashMap<EthTxHash, TxRecord>>,
        resolver_tx: Sender<SendersResolverCommand>,
    ) -> Self {
        let capacity_high_watermark =
            (config.capacity as f64 * config.capacity_high_watermark) as usize;
        Self {
            capacity: config.capacity,
            capacity_high_watermark,
            chain_id: config.chain_id,
            sender_pools: HashMap::new(),
            sender_heartbeat_queue: PriorityQueue::new(),
            tx_price_queue: PriorityQueue::new(),
            pending_price_reversed_queue: PriorityQueue::new(),
            gapped_price_reversed_queue: PriorityQueue::new(),
            token_pkey: config.token_pkey,
            gas_prices,
            tx_count_api: get_tx_api,
            executor,
            txs,
            eviction_timeout_sec: config.eviction_timeout_sec,
            resolver_tx,
        }
    }

    pub fn create_and_start(
        config: Config,
        gas_prices: G,
        get_tx_api: C,
        executor: Arc<E>,
        txs: Arc<DashMap<EthTxHash, TxRecord>>,
        cmd_tx: Sender<Command>,
        cmd_rx: Receiver<Command>,
    ) {
        let (resolver_tx, resolver_rx) =
            mpsc::channel::<SendersResolverCommand>(RESOLVER_CHANNEL_SIZE);
        let this = Self::new(config, gas_prices, get_tx_api, executor, txs, resolver_tx);
        tokio::spawn(this.start(cmd_tx, cmd_rx, resolver_rx));
    }

    pub async fn start(
        mut self,
        cmd_tx: Sender<Command>,
        cmd_rx: Receiver<Command>,
        resolver_rx: Receiver<SendersResolverCommand>,
    ) {
        let sender_resolver = SendersResolver::new(self.tx_count_api.clone());
        tokio::spawn(sender_resolver.start(resolver_rx, cmd_tx.clone()));

        let mut cmd_rx = cmd_rx;
        let (exec_result_tx, mut exec_result_rx) =
            mpsc::channel::<ExecutionResult>(EXEC_RESULT_CHANNEL_SIZE);
        let mut exec_interval =
            tokio::time::interval(tokio::time::Duration::from_millis(EXEC_INTERVAL_MS));
        let mut heartbeat_interval = tokio::time::interval(tokio::time::Duration::from_secs(
            self.eviction_timeout_sec / 10,
        ));
        let eviction_timeout_sec = std::time::Duration::from_secs(self.eviction_timeout_sec);
        loop {
            tokio::select! {
                 _ = exec_interval.tick() => {
                    self.execute_tx(exec_result_tx.clone());
                    exec_interval.reset();
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        Command::Shutdown => {
                            self.resolver_tx
                                .send(SendersResolverCommand::Shutdown)
                                .await
                                .unwrap();
                            tracing::info!("shutting down mempool");
                            return;
                        }
                        Command::ScheduleTx(tx, tx_result) => {
                            let result = self.add_tx(tx).await;
                            tx_result.send(result).unwrap();
                        }
                        Command::ExecuteTx => {
                            self.execute_tx(exec_result_tx.clone());
                            exec_interval.reset();
                        }
                        Command::SetTxCount(addr, tx_count) => {
                            if let Some(sender_pool) = self.sender_pools.get_mut(&addr) {
                                let queues_update = sender_pool.set_tx_count(tx_count);
                                self.apply_queues_update(queues_update).await;
                            }
                        }
                        Command::GetPendingTxCount(addr, result_tx) => {
                            if let Some(sender_pool) = self.sender_pools.get(&addr) {
                                result_tx.send(sender_pool.get_pending_tx_count()).unwrap();
                            } else {
                                result_tx.send(None).unwrap();
                            }
                        }
                        Command::GetTxHash(addr, nonce, result_tx) => {
                            if let Some(sender_pool) = self.sender_pools.get(&addr) {
                                if let Some(record) = sender_pool.get_by_nonce(nonce) {
                                    result_tx.send(Some(record.tx_hash)).unwrap();
                                } else {
                                    result_tx.send(None).unwrap();
                                }
                            } else {
                                result_tx.send(None).unwrap();
                            }
                        }
                    }
                }
                Some(execution_result) = exec_result_rx.recv() => {
                    if let Err(err) = self.process_execution_result(execution_result).await {
                        tracing::error!(?err, "failed to process execution result");
                    }
                    cmd_tx.send(Command::ExecuteTx).await.unwrap();
                }
                _ = heartbeat_interval.tick() => {
                    tracing::debug!(chain_id = %self.chain_id, "sender heartbeat");
                    let now = SystemTime::now();
                    let threshold = now - eviction_timeout_sec;
                    while let Some((sender, Reverse(updated_at))) = self.sender_heartbeat_queue.pop() {
                        // todo: unwrap
                        let sender_pool = self.sender_pools.get_mut(&sender).unwrap();
                        if matches!(sender_pool.state, SenderPoolState::Processing(_)) && updated_at >= threshold {
                            self.sender_heartbeat_queue.push(sender, Reverse(updated_at));
                            break;
                        }
                        tracing::debug!("dropping sender pool");
                        self.remove_sender_pool(&sender).await;
                    }
                }
            }
        }
    }

    async fn add_tx(&mut self, mut tx: TxRecord) -> Result<(), MempoolError> {
        tracing::debug!(tx_hash = %tx.tx_hash(), "schedule tx command");
        let chain_id = tx.chain_id();
        let sender = tx.sender;

        let (sender_pool, created) = self.get_or_create_sender_pool(&sender).await;
        // Clone necessary data from sender_pool to avoid mutable borrow later
        let sender_pool_state = sender_pool.state;
        let tx_count = sender_pool.tx_count;
        let existing_record_tx_hash = sender_pool
            .get_by_nonce(tx.nonce)
            .map(|record| record.tx_hash);

        if let SenderPoolState::Processing(nonce) = sender_pool_state {
            if nonce == tx.nonce {
                return Err(MempoolError::NonceTooLow(nonce, nonce + 1));
            }
        }

        if tx_count > tx.nonce {
            return Err(MempoolError::NonceTooLow(tx.nonce, tx_count));
        }

        if tx.should_set_gas_price() {
            let Some(gas_price) = self.gas_prices.get_gas_for_token_pkey(&self.token_pkey) else {
                return Err(MempoolError::UnknownChainID(chain_id));
            };
            tx.sorting_gas_price = gas_price * 2;
        }

        let tx_hash = *tx.tx_hash();

        let drop_existing = if let Some(existing_record_tx_hash) = existing_record_tx_hash {
            if existing_record_tx_hash == tx_hash {
                return Err(MempoolError::AlreadyKnown);
            }
            let existing_record = self.txs.get(&existing_record_tx_hash).unwrap();
            if existing_record.value().sorting_gas_price >= tx.sorting_gas_price {
                return Err(MempoolError::Underprice);
            }
            true
        } else {
            false
        };

        let sender_chain_tx_count = tx_count;
        let chain_pool_len = self.len();
        if chain_pool_len > self.capacity_high_watermark {
            let gapped_tx = self.gapped_price_reversed_queue.peek();
            if tx.nonce > sender_chain_tx_count {
                if let Some((gapped_tx, _)) = gapped_tx {
                    if tx.sorting_gas_price < gapped_tx.sorting_gas_price {
                        return Err(MempoolError::Underprice);
                    }
                } else {
                    return Err(MempoolError::NonceTooHigh(tx.nonce, sender_chain_tx_count));
                }
            } else if chain_pool_len >= self.capacity && gapped_tx.is_none() {
                let pending_tx = self.pending_price_reversed_queue.peek();
                if let Some((pending_tx, _)) = pending_tx {
                    if tx.sorting_gas_price < pending_tx.sorting_gas_price {
                        return Err(MempoolError::Underprice);
                    }
                }
            }
        }

        if drop_existing {
            self.remove_by(&sender, tx.nonce);
        }

        let to_remove = self.purge_over_capacity_txs();
        for record in to_remove {
            self.txs.remove(&record.tx_hash);
        }

        tracing::debug!(%tx_hash, "adding tx to pool");
        let record = QueueRecord {
            sender,
            tx_hash,
            nonce: tx.nonce,
            sorting_gas_price: tx.sorting_gas_price,
        };
        self.txs.insert(tx_hash, tx);
        self.add_record(record).await;
        self.queue_new_tx(&sender, !created).await?;
        tracing::debug!(%tx_hash, "tx added to pool");
        Ok(())
    }

    /// Gets the next transaction to be executed from the chain pool.
    fn get_for_execution(&mut self) -> Option<EthTxHash> {
        let (tx_record, _) = self.tx_price_queue.pop()?;
        self.pending_price_reversed_queue.remove(&tx_record);
        if let Some(sender_pool) = self.sender_pools.get_mut(&tx_record.sender) {
            sender_pool.set_processing(tx_record.nonce);
        }
        Some(tx_record.tx_hash)
    }

    async fn queue_new_tx(
        &mut self,
        sender: &Address,
        update_tx_count: bool,
    ) -> Result<(), MempoolError> {
        tracing::debug!(%sender, "queueing new tx");
        let Some(sender_pool) = self.sender_pools.get_mut(sender) else {
            return Err(MempoolError::UnknownSender(*sender));
        };

        if matches!(
            sender_pool.state,
            SenderPoolState::Processing(_) | SenderPoolState::Queued(_)
        ) {
            return Ok(());
        }

        if update_tx_count {
            let queues_update = sender_pool.update_tx_count(&self.tx_count_api).await?;
            self.apply_queues_update(queues_update).await;
        }

        // reborrow sender pool to make borrow checker happy
        let Some(sender_pool) = self.sender_pools.get_mut(sender) else {
            return Err(MempoolError::UnknownSender(*sender));
        };

        if let Some(tx) = sender_pool.get_for_queueing() {
            let gas_price = tx.sorting_gas_price;
            self.tx_price_queue.push(tx, gas_price);
            tracing::debug!(%sender, tx_count = %sender_pool.tx_count, "tx queued");
        }

        Ok(())
    }

    async fn apply_queues_update(&mut self, update: QueuesUpdate) {
        use common::evm_loader::types::Address;

        if let Some(record) = update.remove_queued_nonce_too_small {
            self.tx_price_queue.remove(&record);
            self.txs.remove(&record.tx_hash);
        }
        match update.add_update {
            Some(QueueUpdateAdd::Pending(record)) => {
                let price = record.sorting_gas_price;
                self.pending_price_reversed_queue
                    .push(record, Reverse(price));
            }
            Some(QueueUpdateAdd::Gapped(record)) => {
                let price = record.sorting_gas_price;
                self.gapped_price_reversed_queue
                    .push(record, Reverse(price));
            }
            None => {}
        }
        match update.move_update {
            Some(QueueUpdateMove::GappedToPending(records)) => {
                for record in records {
                    self.gapped_price_reversed_queue.remove(&record);
                    let price = record.sorting_gas_price;
                    self.pending_price_reversed_queue
                        .push(record, Reverse(price));
                }
            }
            Some(QueueUpdateMove::PendingToGapped(records)) => {
                for record in records {
                    self.pending_price_reversed_queue.remove(&record);
                    let price = record.sorting_gas_price;
                    self.gapped_price_reversed_queue
                        .push(record, Reverse(price));
                }
            }
            None => {}
        }
        for record in update.remove_nonce_too_small {
            self.gapped_price_reversed_queue.remove(&record);
            self.pending_price_reversed_queue.remove(&record);
            self.txs.remove(&record.tx_hash);
        }
        let _ = match update.state_update {
            Some(StateUpdate::Suspended(addr)) => {
                self.resolver_tx
                    .send(SendersResolverCommand::Add(SenderResolverRecord {
                        sender: BalanceAddress {
                            chain_id: self.chain_id,
                            address: Address::from(<[u8; 20]>::from(addr.0)),
                        },
                        nonce: 0,
                    }))
                    .await
            }
            Some(StateUpdate::Unsuspended(addr)) => {
                self.resolver_tx
                    .send(SendersResolverCommand::Remove(addr))
                    .await
            }
            None => Ok(()),
        }
        .inspect_err(|err| tracing::error!(?err, "failed to send sender resolver command"));
    }

    async fn add_record(&mut self, record: QueueRecord) {
        let Some(sender_pool) = self.sender_pools.get_mut(&record.sender) else {
            return;
        };

        tracing::debug!(tx_hash = %record.tx_hash, nonce = %record.nonce, sender_state = ?sender_pool.state, "adding tx to pool");

        self.sender_heartbeat_queue
            .change_priority(&record.sender, Reverse(SystemTime::now()));

        let queues_update = sender_pool.add(record.clone());
        self.apply_queues_update(queues_update).await;
    }

    fn execute_tx(&mut self, exec_result_tx: Sender<ExecutionResult>) {
        let Some(tx_hash) = self.get_for_execution() else {
            return;
        };

        let Some(tx) = self.txs.get(&tx_hash) else {
            return;
        };

        let tx = tx.clone();
        let tx_executor = Arc::clone(&self.executor);
        let tx_request = tx.tx_request.clone();
        let tx_eth_hash = *tx.tx_hash();
        let chain_id = self.chain_id;

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

    async fn process_execution_result(
        &mut self,
        execution_result: ExecutionResult,
    ) -> Result<(), MempoolError> {
        let Some((_tx_hash, record)) = self.txs.remove(&execution_result.tx_hash) else {
            tracing::error!(chain_id = %execution_result.chain_id, "tx not found in the registry");
            return Ok(());
        };

        let Some(sender_pool) = self.sender_pools.get_mut(&record.sender) else {
            tracing::error!(chain_id = %execution_result.chain_id, sebser = ?record.sender, "sender pool not found");
            return Ok(());
        };

        sender_pool.set_idle();

        if sender_pool.is_empty() {
            let sender = sender_pool.sender;
            self.remove_sender_pool(&sender).await;
        } else {
            self.queue_new_tx(&record.sender, true).await?;
        }

        Ok(())
    }

    fn purge_over_capacity_txs(&mut self) -> Vec<QueueRecord> {
        let mut to_remove = Vec::new();
        let len = self.len();
        if len <= self.capacity {
            tracing::debug!(
                len,
                capacity = self.capacity,
                "purge_over_capacity_txs: nothing to remove"
            );
            return to_remove;
        }
        let to_remove_cnt = len - self.capacity;
        tracing::debug!(
            len,
            capacity = self.capacity,
            to_remove_cnt,
            "purge_over_capacity_txs: removing txs"
        );
        let mut removed = 0;
        if to_remove_cnt > 0 {
            tracing::debug!(tx_to_remove = %to_remove_cnt, chain_id = %self.chain_id, "clearing gapped txs from mempool");
            for _ in 0..to_remove_cnt {
                if let Some((record, _)) = self.gapped_price_reversed_queue.pop() {
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
            for _ in 0..to_remove_cnt {
                if let Some((record, _)) = self.pending_price_reversed_queue.pop() {
                    to_remove.push(record);
                    removed += 1;
                } else {
                    break;
                }
            }
        }

        for record in &to_remove {
            self.tx_price_queue.remove(record);
            self.txs.remove(&record.tx_hash);
            if let Some(sender_pool) = self.sender_pools.get_mut(&record.sender) {
                sender_pool.remove(record)
            }
        }
        to_remove
    }

    fn remove_by(&mut self, sender: &Address, nonce: TxNonce) {
        if let Some(sender_pool) = self.sender_pools.get_mut(sender) {
            let Some(existing) = sender_pool.remove_by_nonce(nonce) else {
                return;
            };
            self.tx_price_queue.remove(&existing);
            self.pending_price_reversed_queue.remove(&existing);
            self.gapped_price_reversed_queue.remove(&existing);
            self.txs.remove(&existing.tx_hash);
        }
    }

    async fn remove_sender_pool(&mut self, sender: &Address) {
        tracing::debug!(%sender, "removing sender pool");
        let Some(mut sender_pool) = self.sender_pools.remove(sender) else {
            tracing::error!(%sender, "sender pool not found");
            return;
        };

        if matches!(sender_pool.state, SenderPoolState::Processing(_)) {
            self.sender_pools.insert(*sender, sender_pool);
            return;
        }

        if sender_pool.is_empty() {
            return;
        }

        if let SenderPoolState::Queued(queued_nonce) = sender_pool.state {
            if let Some(record) = sender_pool.get_by_nonce(queued_nonce) {
                self.pending_price_reversed_queue.remove(record);
                self.tx_price_queue.remove(record);
            }
        }

        if sender_pool.state == SenderPoolState::Suspended {
            self.resolver_tx
                .send(SendersResolverCommand::Remove(*sender))
                .await
                .unwrap();
        }

        for record in sender_pool.drain() {
            self.pending_price_reversed_queue.remove(&record);
            self.gapped_price_reversed_queue.remove(&record);
            self.txs.remove(&record.tx_hash);
        }
        tracing::debug!(%sender, "sender pool removed");
    }

    fn len(&self) -> usize {
        self.pending_price_reversed_queue.len() + self.gapped_price_reversed_queue.len()
    }

    /// Gets or creates a sender pool for the given sender.
    async fn get_or_create_sender_pool(&mut self, sender: &Address) -> (&SenderPool, bool) {
        let mut created = false;
        if !self.sender_pools.contains_key(sender) {
            self.sender_heartbeat_queue
                .push(*sender, Reverse(SystemTime::now()));
            let mut sender_pool = SenderPool::new(self.chain_id, *sender);
            // it's an empty sender pool, we don't care about queues update
            if let Err(err) = sender_pool.update_tx_count(&self.tx_count_api).await {
                tracing::error!(?err, "failed to update tx count");
            }
            self.sender_pools.insert(*sender, sender_pool);
            created = true;
        }

        (self.sender_pools.get(sender).unwrap(), created)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_network::TxSignerSync;
    use alloy_signer_wallet::LocalWallet;
    use reth_primitives::{TxKind, U256};

    use common::solana_sdk::signature::Keypair;
    use common::solana_sdk::signature::Signature;
    use executor::{ExecuteRequest, ExecuteResult};

    struct MockExecutor;

    impl ExecutorTrait for MockExecutor {
        async fn handle_transaction(
            &self,
            tx_request: ExecuteRequest,
            result_sender: Option<oneshot::Sender<ExecuteResult>>,
        ) -> anyhow::Result<Signature> {
            tracing::info!(?tx_request, "mock executor: handling tx");
            if let Some(sender) = result_sender {
                let _ = sender.send(ExecuteResult::Success);
            }
            Ok(Signature::new_unique())
        }
    }

    #[derive(Clone)]
    struct MockGasPrices;

    impl GasPricesTrait for MockGasPrices {
        fn get_gas_price(&self, _chain_id: Option<u64>) -> u128 {
            1
        }

        fn get_gas_for_token_pkey(&self, _token_pkey: &Pubkey) -> Option<u128> {
            Some(1)
        }
    }

    #[derive(Clone)]
    struct MockGetTxCount;

    impl GetTxCountTrait for MockGetTxCount {
        async fn get_transaction_count(
            &self,
            _addr: BalanceAddress,
            _tag: Option<BlockNumberOrTag>,
        ) -> Result<u64, NeonApiError> {
            Ok(0)
        }
    }

    fn create_chain_pool() -> ChainPool<MockExecutor, MockGasPrices, MockGetTxCount> {
        let config = Config {
            chain_id: 1,
            capacity: 10,
            capacity_high_watermark: 0.8,
            token_pkey: Pubkey::new_unique(),
            eviction_timeout_sec: 60,
        };
        ChainPool::new(
            config,
            MockGasPrices,
            MockGetTxCount,
            Arc::new(MockExecutor),
            Arc::new(DashMap::new()),
            mpsc::channel(1).0,
        )
    }

    fn create_exec_req(nonce: TxNonce) -> ExecuteRequest {
        let keypair = Keypair::new();
        let eth = LocalWallet::from_slice(keypair.secret().as_ref()).unwrap();
        let mut tx = TxLegacy {
            nonce,
            gas_price: 2,
            gas_limit: 2_000_000,
            to: TxKind::Create,
            value: U256::ZERO,
            input: Default::default(),
            chain_id: Some(1),
        };
        let signature = eth.sign_transaction_sync(&mut tx).unwrap();
        let tx = tx.into_signed(signature);
        ExecuteRequest::new(tx.into(), 1)
    }

    fn create_tx_record(
        gas_price: Option<GasPrice>,
        sorting_gas_price: GasPrice,
        nonce: TxNonce,
        sender: Address,
    ) -> TxRecord {
        TxRecord {
            tx_request: create_exec_req(0),
            tx_chain_id: Some(1),
            sender,
            nonce,
            gas_price,
            sorting_gas_price,
        }
    }

    #[tokio::test]
    async fn test_get_or_create_sender_pool() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::default();
        let (sender_pool, is_created) = chain_pool.get_or_create_sender_pool(&sender).await;
        assert!(is_created);
        assert_eq!(sender_pool.sender, sender);
        assert_eq!(sender_pool.state, SenderPoolState::Idle);

        let (sender_pool, is_created) = chain_pool.get_or_create_sender_pool(&sender).await;
        assert!(!is_created);
        assert_eq!(sender_pool.sender, sender);
        assert_eq!(sender_pool.state, SenderPoolState::Idle);
    }

    #[tokio::test]
    async fn test_add_tx_already_known() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::default();
        let tx0 = create_tx_record(None, 1, 0, sender);
        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Ok(())));
        let result = chain_pool.add_tx(tx0).await;
        assert!(matches!(result, Err(MempoolError::AlreadyKnown)));
    }

    #[tokio::test]
    async fn test_add_tx_nonce_too_low() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::default();
        let tx0 = create_tx_record(Some(2), 2, 0, sender);
        let (_sender_pool, _is_created) = chain_pool.get_or_create_sender_pool(&sender).await;
        let sender_pool = chain_pool.sender_pools.get_mut(&sender).unwrap();
        sender_pool.tx_count = 1;

        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Err(MempoolError::NonceTooLow(0, 1))));
    }

    #[tokio::test]
    async fn test_add_tx_nonce_too_low_processing() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::default();
        let tx0 = create_tx_record(Some(2), 2, 0, sender);
        let tx1 = create_tx_record(Some(2), 2, 1, sender);
        let (_sender_pool, _is_created) = chain_pool.get_or_create_sender_pool(&sender).await;
        let sender_pool = chain_pool.sender_pools.get_mut(&sender).unwrap();
        sender_pool.tx_count = 1;
        sender_pool.set_processing(1);

        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Err(MempoolError::NonceTooLow(0, 2))));

        let result = chain_pool.add_tx(tx1.clone()).await;
        assert!(matches!(result, Err(MempoolError::NonceTooLow(1, 2))));
    }

    #[tokio::test]
    async fn test_add_tx_underpriced_existing() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::default();
        let tx0 = create_tx_record(Some(2), 2, 0, sender);
        let tx1 = create_tx_record(Some(1), 1, 0, sender);
        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Ok(())));

        let result = chain_pool.add_tx(tx1.clone()).await;
        assert!(matches!(result, Err(MempoolError::Underprice)));
    }

    #[tokio::test]
    async fn test_add_tx_underpriced_watermark() {
        let mut chain_pool = create_chain_pool();
        chain_pool.capacity_high_watermark = 0;
        chain_pool.capacity = 3;
        let sender = Address::default();
        let other_sender = Address::random();
        let tx1 = create_tx_record(Some(2), 2, 1, other_sender);
        let tx0 = create_tx_record(Some(1), 1, 1, sender);
        let result = chain_pool.add_tx(tx1.clone()).await;
        assert!(matches!(result, Ok(())));

        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Err(MempoolError::Underprice)));
    }

    // todo; test underprice capacity

    #[tokio::test]
    async fn test_add_tx_nonce_too_high() {
        let mut chain_pool = create_chain_pool();
        chain_pool.capacity_high_watermark = 0;
        chain_pool.capacity = 3;
        let sender = Address::default();
        let tx0 = create_tx_record(Some(2), 2, 0, sender);
        let tx1 = create_tx_record(Some(2), 2, 1, sender);
        let result = chain_pool.add_tx(tx0.clone()).await;
        assert!(matches!(result, Ok(())));

        let result = chain_pool.add_tx(tx1.clone()).await;
        assert!(matches!(result, Err(MempoolError::NonceTooHigh(1, 0))));
    }
}
