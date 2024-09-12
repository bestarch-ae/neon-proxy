use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_signer_wallet::LocalWallet;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use dashmap::{DashMap, DashSet};
use executor::{ExecuteRequest, ExecuteResult};
use priority_queue::PriorityQueue;
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, TxKind, U256};
use tokio::sync::oneshot;

use crate::mempool::{
    attempt_to_queue, attempt_to_queue_or_update_state, move_from_gapped_to_pending,
    move_from_pending_to_gapped, rebalance_pending_gapped_queues, remove_records_with_nonce_le,
    ChainPool, EthTxHash, GasPrice, PoolState, QueueRecord, SenderPool, TxRecord,
};
use crate::{GasPricesTrait, MempoolError};

#[allow(dead_code)]
struct MockExecutor;

#[allow(dead_code)]
impl MockExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn handle_transaction(
        &self,
        tx_request: ExecuteRequest,
        result_sender: Option<oneshot::Sender<ExecuteResult>>,
    ) -> Result<(), MempoolError> {
        tracing::info!(?tx_request, "mock executor: handling tx");
        if let Some(sender) = result_sender {
            let _ = sender.send(ExecuteResult::Success);
        }
        Ok(())
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

fn req(nonce: TxNonce) -> ExecuteRequest {
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

fn create_queue_record(
    sender: Address,
    tx_hash: EthTxHash,
    nonce: TxNonce,
    gas_price: GasPrice,
) -> QueueRecord {
    QueueRecord {
        sender,
        tx_hash,
        nonce,
        sorting_gas_price: gas_price,
    }
}

fn create_tx_record(
    gas_price: Option<GasPrice>,
    sorting_gas_price: GasPrice,
    nonce: TxNonce,
    sender: Address,
) -> TxRecord {
    TxRecord {
        tx_request: req(0),
        tx_chain_id: Some(1),
        sender,
        nonce,
        gas_price,
        sorting_gas_price,
    }
}

fn queue_record_from_tx_record(tx_record: &TxRecord) -> QueueRecord {
    QueueRecord {
        sender: tx_record.sender,
        tx_hash: *tx_record.tx_hash(),
        nonce: tx_record.nonce,
        sorting_gas_price: tx_record.sorting_gas_price,
    }
}

fn create_sender_pool() -> SenderPool {
    SenderPool::new(1, Address::random())
}

fn create_chain_pool() -> ChainPool {
    ChainPool::new(1, 100, 90)
}

fn fill_pending(
    sender_pool: &mut SenderPool,
    pending: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    records: Vec<QueueRecord>,
) {
    for record in records {
        sender_pool
            .pending_nonce_queue
            .push(record.clone(), record.nonce);
        pending.push(record.clone(), Reverse(record.sorting_gas_price));
    }
}

fn fill_gapped(
    sender_pool: &mut SenderPool,
    gapped: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    records: Vec<QueueRecord>,
) {
    for record in records {
        sender_pool
            .gapped_nonce_queue
            .push(record.clone(), record.nonce);
        gapped.push(record.clone(), Reverse(record.sorting_gas_price));
    }
}

fn assert_pending_and_clean(
    sender_pool: &mut SenderPool,
    pending: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    records: Vec<&QueueRecord>,
) {
    assert_eq!(sender_pool.pending_nonce_queue.len(), records.len());
    assert_eq!(pending.len(), records.len());

    for record in records {
        assert!(sender_pool.pending_nonce_queue.remove(record).is_some());
        assert!(pending.remove(record).is_some());
    }
}

fn assert_gapped_and_clean(
    sender_pool: &mut SenderPool,
    gapped: &mut PriorityQueue<QueueRecord, Reverse<GasPrice>>,
    records: Vec<&QueueRecord>,
) {
    assert_eq!(sender_pool.gapped_nonce_queue.len(), records.len());
    assert_eq!(gapped.len(), records.len());

    for record in records {
        assert!(sender_pool.gapped_nonce_queue.remove(record).is_some());
        assert!(gapped.remove(record).is_some());
    }
}

#[test]
fn test_move_from_gapped_to_pending() {
    let mut sender_pool = create_sender_pool();
    let mut pending_price_reversed_queue = PriorityQueue::new();
    let mut gapped_price_reversed_queue = PriorityQueue::new();

    let record0 = create_queue_record(Address::random(), EthTxHash::random(), 0, 100);
    let record1 = create_queue_record(Address::random(), EthTxHash::random(), 1, 200);
    let record2 = create_queue_record(Address::random(), EthTxHash::random(), 2, 300);
    let record3 = create_queue_record(Address::random(), EthTxHash::random(), 3, 400);

    // pending empty; gapped: 0, 1; result: pending 0, 1; gapped: empty
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record0.clone(), record1.clone()],
    );
    move_from_gapped_to_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1],
    );
    assert_gapped_and_clean(&mut sender_pool, &mut gapped_price_reversed_queue, vec![]);

    // pending: 1; gapped: 0, 2; result: pending 0, 1; gapped: 3
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record1.clone()],
    );
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record0.clone(), record2.clone()],
    );
    move_from_gapped_to_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1],
    );
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record2],
    );

    // pending: 2; gapped: 0, 1, 3; result: pending 0, 1, 2; gapped: 3
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record2.clone()],
    );
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record0.clone(), record1.clone(), record3.clone()],
    );
    move_from_gapped_to_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1, &record2],
    );
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record3],
    );

    // pending: 0, 1, gapped: 2; result: pending 0, 1; gapped: 2
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record0.clone(), record1.clone()],
    );
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record2.clone()],
    );
    move_from_gapped_to_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1],
    );
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record2],
    );

    // pending: empty; gapped: 1; result: pending empty; gapped: 1
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record1.clone()],
    );
    move_from_gapped_to_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(&mut sender_pool, &mut pending_price_reversed_queue, vec![]);
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record1],
    );
}

#[test]
fn test_move_from_pending_to_gapped() {
    let mut sender_pool = create_sender_pool();
    let mut pending_price_reversed_queue = PriorityQueue::new();
    let mut gapped_price_reversed_queue = PriorityQueue::new();

    sender_pool.chain_tx_count = 1;

    let record0 = create_queue_record(Address::random(), EthTxHash::random(), 0, 100);
    let record1 = create_queue_record(Address::random(), EthTxHash::random(), 1, 200);
    let record2 = create_queue_record(Address::random(), EthTxHash::random(), 2, 300);
    let record3 = create_queue_record(Address::random(), EthTxHash::random(), 3, 400);

    // pending: 0, 1, 2; result: pending: 0, 1, 2; gapped: empty
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record0.clone(), record1.clone(), record2.clone()],
    );
    move_from_pending_to_gapped(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1, &record2],
    );
    assert_gapped_and_clean(&mut sender_pool, &mut gapped_price_reversed_queue, vec![]);

    // pending: 2, 3; result: pending: empty; gapped: 2, 3
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record2.clone(), record3.clone()],
    );
    move_from_pending_to_gapped(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(&mut sender_pool, &mut pending_price_reversed_queue, vec![]);
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record2, &record3],
    );

    // pending: 0, 1, 3; result: pending: 0, 1, 3; gapped: empty
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record0.clone(), record1.clone(), record3.clone()],
    );
    move_from_pending_to_gapped(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record0, &record1, &record3],
    );
    assert_gapped_and_clean(&mut sender_pool, &mut gapped_price_reversed_queue, vec![]);

    // pending: 1, 3; result: pending: 1, 3; gapped: empty
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record1.clone(), record3.clone()],
    );
    move_from_pending_to_gapped(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record1, &record3],
    );
    assert_gapped_and_clean(&mut sender_pool, &mut gapped_price_reversed_queue, vec![]);
}

#[test]
fn test_rebalance_pending_gapped_queues() {
    let mut sender_pool = create_sender_pool();
    let mut pending_price_reversed_queue = PriorityQueue::new();
    let mut gapped_price_reversed_queue = PriorityQueue::new();

    sender_pool.chain_tx_count = 1;

    let record1 = create_queue_record(Address::random(), EthTxHash::random(), 1, 200);
    let record2 = create_queue_record(Address::random(), EthTxHash::random(), 2, 300);
    let record3 = create_queue_record(Address::random(), EthTxHash::random(), 3, 400);
    let record4 = create_queue_record(Address::random(), EthTxHash::random(), 4, 500);

    // pending: empty; gapped: 2; result: pending: empty; gapped: 2
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record2.clone()],
    );
    rebalance_pending_gapped_queues(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(&mut sender_pool, &mut pending_price_reversed_queue, vec![]);
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record2],
    );

    // pending: 2, 3 ; gapped: 1, 4; result: pending: 1, 2, 3, 4; gapped: empty
    fill_pending(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![record2.clone(), record3.clone()],
    );
    fill_gapped(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![record1.clone(), record4.clone()],
    );
    rebalance_pending_gapped_queues(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );
    assert_pending_and_clean(
        &mut sender_pool,
        &mut pending_price_reversed_queue,
        vec![&record1, &record2, &record3],
    );
    assert_gapped_and_clean(
        &mut sender_pool,
        &mut gapped_price_reversed_queue,
        vec![&record4],
    );
}

#[test]
fn test_remove_records_with_nonce_le() {
    let mut sender_pool = create_sender_pool();
    let mut pending_price_reversed_queue = PriorityQueue::new();
    let mut gapped_price_reversed_queue = PriorityQueue::new();
    let txs = Arc::new(DashMap::new());
    let sender = sender_pool.sender;
    let tx1 = create_tx_record(Some(100), 100, 1, sender);
    let tx2 = create_tx_record(Some(200), 200, 2, sender);
    let record1 = queue_record_from_tx_record(&tx1);
    let record2 = queue_record_from_tx_record(&tx2);
    sender_pool.pending_nonce_queue.push(record1.clone(), 1);
    sender_pool.pending_nonce_queue.push(record2.clone(), 2);
    pending_price_reversed_queue.push(record1.clone(), Reverse(record1.sorting_gas_price));
    pending_price_reversed_queue.push(record2.clone(), Reverse(record2.sorting_gas_price));
    sender_pool.nonce_map.insert(1, record1.clone());
    sender_pool.nonce_map.insert(2, record2.clone());
    txs.insert(record1.tx_hash, tx1);
    txs.insert(record2.tx_hash, tx2);
    let tx3 = create_tx_record(Some(300), 300, 3, sender);
    let record3 = queue_record_from_tx_record(&tx3);
    sender_pool.gapped_nonce_queue.push(record3.clone(), 3);
    sender_pool.nonce_map.insert(3, record3.clone());
    gapped_price_reversed_queue.push(record3.clone(), Reverse(record3.sorting_gas_price));
    txs.insert(record3.tx_hash, tx3);
    let tx4 = create_tx_record(Some(400), 400, 4, sender);
    let record4 = queue_record_from_tx_record(&tx4);
    sender_pool.gapped_nonce_queue.push(record4.clone(), 4);
    gapped_price_reversed_queue.push(record4.clone(), Reverse(record4.sorting_gas_price));
    sender_pool.nonce_map.insert(4, record4.clone());
    txs.insert(record4.tx_hash, tx4);

    remove_records_with_nonce_le(
        &mut sender_pool,
        4,
        txs.clone(),
        &mut pending_price_reversed_queue,
        &mut gapped_price_reversed_queue,
    );

    assert!(pending_price_reversed_queue.get(&record1).is_none());
    assert!(pending_price_reversed_queue.get(&record2).is_none());
    assert!(gapped_price_reversed_queue.get(&record3).is_none());
    assert!(gapped_price_reversed_queue.get(&record4).is_some());

    assert!(sender_pool.pending_nonce_queue.get(&record1).is_none());
    assert!(sender_pool.pending_nonce_queue.get(&record2).is_none());
    assert!(sender_pool.gapped_nonce_queue.get(&record3).is_none());
    assert!(sender_pool.gapped_nonce_queue.get(&record4).is_some());

    assert!(!sender_pool.nonce_map.contains_key(&record1.nonce));
    assert!(!sender_pool.nonce_map.contains_key(&record2.nonce));
    assert!(!sender_pool.nonce_map.contains_key(&record3.nonce));
    assert!(sender_pool.nonce_map.contains_key(&record4.nonce));

    assert!(txs.get(&record1.tx_hash).is_none());
    assert!(txs.get(&record2.tx_hash).is_none());
    assert!(txs.get(&record3.tx_hash).is_none());
    assert!(txs.get(&record4.tx_hash).is_some());
}

#[test]
fn test_attempt_to_queue_success() {
    let mut sender_pool = create_sender_pool();
    let mut tx_price_queue = PriorityQueue::new();
    let record = create_queue_record(Address::random(), EthTxHash::random(), 0, 100);
    sender_pool.pending_nonce_queue.push(record.clone(), 0);
    let result = attempt_to_queue(&mut sender_pool, &mut tx_price_queue);
    assert!(result);
    assert!(tx_price_queue.get(&record).is_some());
    assert_eq!(sender_pool.state, PoolState::Queued(0));
}

#[test]
fn test_attempt_to_queue_failure() {
    let mut sender_pool = create_sender_pool();
    let mut tx_price_queue = PriorityQueue::new();
    let result = attempt_to_queue(&mut sender_pool, &mut tx_price_queue);
    assert!(!result);
    assert!(tx_price_queue.is_empty());
    assert_eq!(sender_pool.state, PoolState::Unresolved);
}

#[test]
fn test_attempt_to_queue_or_update_state_success() {
    let mut sender_pool = create_sender_pool();
    let mut tx_price_queue = PriorityQueue::new();
    let suspended_senders = DashSet::new();
    let record = create_queue_record(Address::random(), EthTxHash::random(), 0, 100);
    sender_pool.pending_nonce_queue.push(record.clone(), 0);
    attempt_to_queue_or_update_state(&mut sender_pool, &mut tx_price_queue, &suspended_senders);
    assert!(tx_price_queue.get(&record).is_some());
    assert_eq!(sender_pool.state, PoolState::Queued(0));
    assert!(suspended_senders.is_empty());
}

#[test]
fn test_attempt_to_queue_or_update_state_empty_pool() {
    let mut sender_pool = create_sender_pool();
    let mut tx_price_queue = PriorityQueue::new();
    let suspended_senders = DashSet::new();
    attempt_to_queue_or_update_state(&mut sender_pool, &mut tx_price_queue, &suspended_senders);
    assert_eq!(sender_pool.state, PoolState::Unresolved);
    assert!(tx_price_queue.is_empty());
    assert!(suspended_senders.is_empty());

    let mut sender_pool = create_sender_pool();
    sender_pool.state = PoolState::Idle;
    let mut tx_price_queue = PriorityQueue::new();
    let suspended_senders = DashSet::new();
    attempt_to_queue_or_update_state(&mut sender_pool, &mut tx_price_queue, &suspended_senders);
    assert_eq!(sender_pool.state, PoolState::Idle);
    assert!(tx_price_queue.is_empty());
    assert!(suspended_senders.is_empty());
}

#[test]
fn test_attempt_to_queue_or_update_state_suspend_pool() {
    let mut sender_pool = create_sender_pool();
    let mut tx_price_queue = PriorityQueue::new();
    let suspended_senders = DashSet::new();
    let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
    sender_pool.nonce_map.insert(1, record);
    attempt_to_queue_or_update_state(&mut sender_pool, &mut tx_price_queue, &suspended_senders);
    assert_eq!(sender_pool.state, PoolState::Suspended);
    assert!(tx_price_queue.is_empty());
    assert!(suspended_senders.contains(&sender_pool.sender));
}

mod sender_pool {
    use super::*;

    #[test]
    fn test_set_processing() {
        let mut sender_pool = create_sender_pool();
        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 0, 100);
        sender_pool.nonce_map.insert(0, record.clone());
        sender_pool.pending_nonce_queue.push(record, 0);
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        sender_pool.set_processing(0);
        assert_eq!(sender_pool.state, PoolState::Processing(0));
        assert!(sender_pool.nonce_map.is_empty());
        assert_eq!(sender_pool.chain_tx_count, 1);
    }

    #[test]
    fn test_remove_by_nonce() {
        let mut sender_pool = create_sender_pool();
        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.pending_nonce_queue.push(record.clone(), 1);
        let removed_record = sender_pool.remove_by_nonce(1);
        assert!(removed_record.is_some());
        assert_eq!(removed_record.unwrap().nonce, 1);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        assert!(sender_pool.nonce_map.is_empty());

        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.gapped_nonce_queue.push(record.clone(), 1);
        let removed_record = sender_pool.remove_by_nonce(1);
        assert!(removed_record.is_some());
        assert_eq!(removed_record.unwrap().nonce, 1);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        assert!(sender_pool.nonce_map.is_empty());

        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.state = PoolState::Queued(1);
        let removed_record = sender_pool.remove_by_nonce(1);
        assert!(removed_record.is_some());
        assert_eq!(removed_record.unwrap().nonce, 1);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Idle);
        assert!(sender_pool.nonce_map.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut sender_pool = create_sender_pool();
        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.pending_nonce_queue.push(record.clone(), 1);
        sender_pool.remove(&record);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        assert!(sender_pool.nonce_map.is_empty());

        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.gapped_nonce_queue.push(record.clone(), 1);
        sender_pool.remove(&record);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        assert!(sender_pool.nonce_map.is_empty());

        let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 100);
        sender_pool.nonce_map.insert(1, record.clone());
        sender_pool.state = PoolState::Queued(1);
        sender_pool.remove(&record);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.state, PoolState::Idle);
        assert!(sender_pool.nonce_map.is_empty());
    }
}

mod chain_pool {
    use super::*;

    #[tokio::test]
    async fn test_get_for_execution() {
        let mut chain_pool = create_chain_pool();
        let sender = Address::random();
        let record0 = create_queue_record(sender, EthTxHash::random(), 1, 100);
        let record1 = create_queue_record(sender, EthTxHash::random(), 2, 150);
        let mut queue = chain_pool.tx_price_queue.write().await;
        queue.push(record0.clone(), record0.sorting_gas_price);
        queue.push(record1.clone(), record1.sorting_gas_price);
        drop(queue);
        let next_tx = chain_pool.get_for_execution().await;
        assert_eq!(next_tx, Some(record1.tx_hash));
        let next_tx = chain_pool.get_for_execution().await;
        assert_eq!(next_tx, Some(record0.tx_hash));
        let next_tx = chain_pool.get_for_execution().await;
        assert!(next_tx.is_none());
    }

    #[tokio::test]
    async fn test_add_to_unresolved() {
        let mut chain_pool = create_chain_pool();
        let sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        chain_pool.sender_pools.insert(sender, sender_pool);
        let record = create_queue_record(sender, EthTxHash::random(), 1, 100);
        chain_pool.add(record.clone()).await;
        assert!(chain_pool
            .pending_price_reversed_queue
            .write()
            .await
            .is_empty());
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Unresolved);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert_eq!(sender_pool.gapped_nonce_queue.len(), 1);
        assert!(sender_pool.gapped_nonce_queue.get(&record).is_some());
        assert!(sender_pool.nonce_map.contains_key(&1));
    }

    #[tokio::test]
    async fn test_add_to_idle() {
        let mut chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        sender_pool.state = PoolState::Idle;
        chain_pool.sender_pools.insert(sender, sender_pool);
        let record = create_queue_record(sender, EthTxHash::random(), 1, 100);
        chain_pool.add(record.clone()).await;
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .is_empty());
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        assert_eq!(chain_pool.suspended_senders.len(), 1);
        assert!(chain_pool.suspended_senders.get(&sender).is_some());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Suspended);
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert_eq!(sender_pool.gapped_nonce_queue.len(), 1);
        assert!(sender_pool.gapped_nonce_queue.get(&record).is_some());
        assert!(sender_pool.nonce_map.contains_key(&1));
        drop(sender_pool);

        let mut chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        sender_pool.state = PoolState::Idle;
        chain_pool.sender_pools.insert(sender, sender_pool);
        let record = create_queue_record(sender, EthTxHash::random(), 0, 100);
        chain_pool.add(record.clone()).await;
        assert_eq!(
            chain_pool.pending_price_reversed_queue.write().await.len(),
            1
        );
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .is_empty());
        assert_eq!(chain_pool.tx_price_queue.read().await.len(), 1);
        assert!(chain_pool
            .tx_price_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool.suspended_senders.is_empty());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Queued(0));
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert!(sender_pool.nonce_map.contains_key(&0));
    }

    #[tokio::test]
    async fn test_add_to_suspended() {
        let mut chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        let record0 = create_queue_record(sender, EthTxHash::random(), 0, 100);
        let record1 = create_queue_record(sender, EthTxHash::random(), 1, 100);
        sender_pool.state = PoolState::Suspended;
        sender_pool.gapped_nonce_queue.push(record1.clone(), 1);
        sender_pool.nonce_map.insert(1, record1.clone());
        chain_pool.sender_pools.insert(sender, sender_pool);
        chain_pool.suspended_senders.insert(sender);
        chain_pool
            .gapped_price_reversed_queue
            .write()
            .await
            .push(record1.clone(), Reverse(record1.sorting_gas_price));
        chain_pool.add(record0.clone()).await;
        assert_eq!(
            chain_pool.pending_price_reversed_queue.write().await.len(),
            1
        );
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .get(&record0)
            .is_some());
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .get(&record1)
            .is_some());
        assert_eq!(chain_pool.tx_price_queue.read().await.len(), 1);
        assert!(chain_pool
            .tx_price_queue
            .read()
            .await
            .get(&record0)
            .is_some());
        assert!(chain_pool.suspended_senders.is_empty());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Queued(0));
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert_eq!(sender_pool.gapped_nonce_queue.len(), 1);
        assert!(sender_pool.gapped_nonce_queue.get(&record1).is_some());
        assert!(sender_pool.nonce_map.contains_key(&0));
        assert!(sender_pool.nonce_map.contains_key(&1));
    }

    #[tokio::test]
    async fn test_add_to_queued() {
        let mut chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        sender_pool.state = PoolState::Queued(0);
        chain_pool.sender_pools.insert(sender, sender_pool);

        let record = create_queue_record(sender, EthTxHash::random(), 1, 100);
        chain_pool.add(record.clone()).await;
        assert_eq!(
            chain_pool.pending_price_reversed_queue.read().await.len(),
            1
        );
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .is_empty());

        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Queued(0));
        assert_eq!(sender_pool.pending_nonce_queue.len(), 1);
        assert!(sender_pool.pending_nonce_queue.get(&record).is_some());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert!(sender_pool.nonce_map.contains_key(&1));
    }

    #[tokio::test]
    async fn test_add_to_processed() {
        let mut chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        sender_pool.state = PoolState::Processing(0);
        sender_pool.chain_tx_count = 1;
        chain_pool.sender_pools.insert(sender, sender_pool);

        let record = create_queue_record(sender, EthTxHash::random(), 1, 100);
        chain_pool.add(record.clone()).await;
        assert_eq!(
            chain_pool.pending_price_reversed_queue.read().await.len(),
            1
        );
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .get(&record)
            .is_some());
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .is_empty());

        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.state, PoolState::Processing(0));
        assert_eq!(sender_pool.pending_nonce_queue.len(), 1);
        assert!(sender_pool.pending_nonce_queue.get(&record).is_some());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert!(sender_pool.nonce_map.contains_key(&1));
    }

    #[tokio::test]
    async fn test_queue_new_tx() {
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.state = PoolState::Idle;
        let record0 = create_queue_record(sender_pool.sender, EthTxHash::random(), 0, 100);
        let record1 = create_queue_record(sender_pool.sender, EthTxHash::random(), 1, 150);
        // Queue empty pool remains Idle
        chain_pool.queue_new_tx(&mut sender_pool).await;
        assert_eq!(sender_pool.state, PoolState::Idle);
        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        assert!(chain_pool.suspended_senders.is_empty());

        // Pending 0, 1; queues 0; 1 remains in pending
        sender_pool
            .pending_nonce_queue
            .push(record0.clone(), record0.nonce);
        sender_pool.nonce_map.insert(record0.nonce, record0.clone());
        chain_pool
            .pending_price_reversed_queue
            .write()
            .await
            .push(record0.clone(), Reverse(record0.sorting_gas_price));
        sender_pool
            .pending_nonce_queue
            .push(record1.clone(), record1.nonce);
        sender_pool.nonce_map.insert(record1.nonce, record1.clone());
        chain_pool
            .pending_price_reversed_queue
            .write()
            .await
            .push(record0.clone(), Reverse(record0.sorting_gas_price));
        chain_pool.queue_new_tx(&mut sender_pool).await;
        assert_eq!(sender_pool.state, PoolState::Queued(0));
        assert_eq!(chain_pool.tx_price_queue.read().await.len(), 1);
        assert!(chain_pool
            .tx_price_queue
            .read()
            .await
            .get(&record0)
            .is_some());
        assert_eq!(sender_pool.pending_nonce_queue.len(), 1);
        assert!(sender_pool.pending_nonce_queue.get(&record1).is_some());
        assert_eq!(sender_pool.nonce_map.len(), 2);

        // Pending 1, chain_tx_count: 0; and gapped is empty; suspends sender
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.state = PoolState::Idle;
        sender_pool
            .pending_nonce_queue
            .push(record1.clone(), record1.nonce);
        sender_pool.nonce_map.insert(record1.nonce, record1.clone());
        chain_pool
            .pending_price_reversed_queue
            .write()
            .await
            .push(record1.clone(), Reverse(record1.sorting_gas_price));
        chain_pool.queue_new_tx(&mut sender_pool).await;
        assert_eq!(sender_pool.state, PoolState::Suspended);
        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        assert_eq!(chain_pool.suspended_senders.len(), 1);
        assert!(chain_pool
            .suspended_senders
            .get(&sender_pool.sender)
            .is_some());
        assert!(chain_pool
            .pending_price_reversed_queue
            .read()
            .await
            .is_empty());
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .get(&record1)
            .is_some());
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert_eq!(sender_pool.gapped_nonce_queue.len(), 1);
        assert!(sender_pool.gapped_nonce_queue.get(&record1).is_some());
        assert_eq!(sender_pool.nonce_map.len(), 1);

        // Pending empty, gapped: 1, chain_tx_count: 1; queue(1)
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.state = PoolState::Idle;
        sender_pool.chain_tx_count = 1;
        sender_pool
            .gapped_nonce_queue
            .push(record1.clone(), record1.nonce);
        sender_pool.nonce_map.insert(record1.nonce, record1.clone());
        chain_pool
            .gapped_price_reversed_queue
            .write()
            .await
            .push(record1.clone(), Reverse(record1.sorting_gas_price));
        chain_pool.queue_new_tx(&mut sender_pool).await;
        assert_eq!(sender_pool.state, PoolState::Queued(1));
        assert_eq!(chain_pool.tx_price_queue.read().await.len(), 1);
        assert!(chain_pool
            .tx_price_queue
            .read()
            .await
            .get(&record1)
            .is_some());
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .is_empty());
        assert!(sender_pool.gapped_nonce_queue.is_empty());
        assert_eq!(sender_pool.nonce_map.len(), 1);
        assert!(sender_pool.nonce_map.contains_key(&1));

        // Pending empty, gapped: 1, chain_tx_count: 0; suspends sender
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.state = PoolState::Idle;
        sender_pool
            .gapped_nonce_queue
            .push(record1.clone(), record1.nonce);
        sender_pool.nonce_map.insert(record1.nonce, record1.clone());
        chain_pool
            .gapped_price_reversed_queue
            .write()
            .await
            .push(record1.clone(), Reverse(record1.sorting_gas_price));
        chain_pool.queue_new_tx(&mut sender_pool).await;
        assert_eq!(sender_pool.state, PoolState::Suspended);
        assert!(chain_pool.tx_price_queue.read().await.is_empty());
        assert_eq!(chain_pool.suspended_senders.len(), 1);
        assert!(chain_pool
            .suspended_senders
            .get(&sender_pool.sender)
            .is_some());
        assert!(sender_pool.pending_nonce_queue.is_empty());
        assert_eq!(sender_pool.gapped_nonce_queue.len(), 1);
        assert!(sender_pool.gapped_nonce_queue.get(&record1).is_some());
        assert_eq!(sender_pool.nonce_map.len(), 1);
    }

    #[tokio::test]
    async fn test_queue_new_tx_not_idle() {
        for state in [
            PoolState::Processing(0),
            PoolState::Queued(0),
            PoolState::Suspended,
        ] {
            let chain_pool = create_chain_pool();
            let mut sender_pool = create_sender_pool();
            let record = create_queue_record(sender_pool.sender, EthTxHash::random(), 0, 100);
            sender_pool.state = state;
            sender_pool
                .pending_nonce_queue
                .push(record.clone(), record.nonce);
            sender_pool.nonce_map.insert(record.nonce, record.clone());
            chain_pool
                .pending_price_reversed_queue
                .write()
                .await
                .push(record.clone(), Reverse(record.sorting_gas_price));
            chain_pool.queue_new_tx(&mut sender_pool).await;
            assert_eq!(sender_pool.state, state);
        }
    }

    #[tokio::test]
    async fn test_purge_over_capacity_txs_no_removal_when_under_capacity() {
        let mut chain_pool = create_chain_pool();
        let sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        chain_pool.sender_pools.insert(sender, sender_pool);
        let record = create_queue_record(sender, EthTxHash::random(), 1, 100);
        chain_pool.add(record.clone()).await;
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        let removed_records = chain_pool.purge_over_capacity_txs().await;
        assert!(removed_records.is_empty());
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.nonce_map.len(), 1);
    }

    #[tokio::test]
    async fn test_purge_over_capacity_txs_gapped_removal() {
        let mut chain_pool = create_chain_pool();
        let sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        chain_pool.sender_pools.insert(sender, sender_pool);
        chain_pool.capacity = 1;
        let record0 = create_queue_record(sender, EthTxHash::random(), 1, 100);
        let record1 = create_queue_record(sender, EthTxHash::random(), 2, 150);
        chain_pool.add(record0.clone()).await;
        chain_pool.add(record1.clone()).await;
        let removed_records = chain_pool.purge_over_capacity_txs().await;

        assert_eq!(removed_records.len(), 1);
        assert_eq!(removed_records[0], record0); // lower gas price is removed first
        assert_eq!(chain_pool.gapped_price_reversed_queue.read().await.len(), 1);
        assert!(chain_pool
            .gapped_price_reversed_queue
            .read()
            .await
            .get(&record1)
            .is_some());
        let sender_pool = chain_pool.sender_pools.get(&sender).unwrap();
        assert_eq!(sender_pool.nonce_map.len(), 1);
        assert!(sender_pool.nonce_map.contains_key(&2));
    }
}

mod schedule_tx {
    use super::*;

    use reth_primitives::ChainId;
    use tokio::sync::mpsc;

    use crate::mempool::schedule_tx;

    #[tokio::test]
    async fn test_unknown_chain_id() {
        let tx = create_tx_record(Some(100), 100, 0, Address::random());
        let chain_pools = DashMap::new();
        let txs = DashMap::new();
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);

        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::UnknownChainID(_))));
    }

    #[tokio::test]
    async fn test_nonce_too_low_processing_state() {
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.state = PoolState::Processing(1);
        let sender = sender_pool.sender;
        let tx = create_tx_record(Some(100), 100, 1, sender);
        chain_pool.sender_pools.insert(sender, sender_pool);
        let chain_pools = DashMap::new();
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);

        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::NonceTooLow(_, _))));
    }

    #[tokio::test]
    async fn test_nonce_too_low_chain_tx_count() {
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        sender_pool.chain_tx_count = 3;
        let sender = sender_pool.sender;
        let tx = create_tx_record(Some(100), 100, 1, sender);
        chain_pool.sender_pools.insert(sender, sender_pool);
        let chain_pools = DashMap::new();
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);

        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::NonceTooLow(_, _))));
    }

    #[tokio::test]
    async fn test_already_known() {
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        let tx = create_tx_record(Some(100), 100, 0, sender);
        let existing_q_record = queue_record_from_tx_record(&tx);
        sender_pool.nonce_map.insert(0, existing_q_record.clone());
        let chain_pools = DashMap::new();
        chain_pool.sender_pools.insert(sender, sender_pool);
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        txs.insert(existing_q_record.tx_hash, tx.clone());
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);

        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::AlreadyKnown)));
    }

    #[tokio::test]
    async fn test_underpriced_transaction() {
        let chain_pool = create_chain_pool();
        let mut sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        let tx = create_tx_record(Some(100), 100, 0, sender);
        let existing_tx = create_tx_record(Some(150), 150, 0, sender);
        let existing_q_record = queue_record_from_tx_record(&existing_tx);
        sender_pool.nonce_map.insert(0, existing_q_record);
        chain_pool.sender_pools.insert(sender, sender_pool);
        let chain_pools = DashMap::new();
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        txs.insert(*existing_tx.tx_hash(), existing_tx);
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);

        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::Underprice)));
    }

    #[tokio::test]
    async fn test_unknown_token_key() {
        let tx = create_tx_record(None, 100, 0, Address::random());
        let chain_pools = DashMap::new();
        let chain_pool = create_chain_pool();
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        let gas_prices = MockGasPrices;
        let token_map = HashMap::new();
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);
        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(matches!(result, Err(MempoolError::UnknownChainID(_))));
    }

    #[tokio::test]
    async fn test_schedule_tx_success_simple() {
        let chain_pool = create_chain_pool();
        let sender_pool = create_sender_pool();
        let sender = sender_pool.sender;
        let tx = create_tx_record(Some(100), 100, 0, sender);
        let tx_hash = *tx.tx_hash();
        let chain_id = tx.chain_id();
        chain_pool.sender_pools.insert(tx.sender, sender_pool);
        let chain_pools = DashMap::new();
        chain_pools.insert(tx.chain_id(), chain_pool);
        let txs = DashMap::new();
        let gas_prices = MockGasPrices;
        let mut token_map = HashMap::new();
        token_map.insert(chain_id, Pubkey::new_unique());
        let (resolver_tx, _) = mpsc::channel::<(Address, ChainId)>(2);
        let result = schedule_tx(
            tx,
            &chain_pools,
            &txs,
            &gas_prices,
            &token_map,
            &resolver_tx,
        )
        .await;
        assert!(result.is_ok());
        assert!(txs.contains_key(&tx_hash));
    }
}
