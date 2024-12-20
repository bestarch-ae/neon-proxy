use std::cmp::max;
use std::collections::HashMap;

use priority_queue::DoublePriorityQueue;
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, BlockNumberOrTag, ChainId};

use common::neon_lib::types::BalanceAddress;

use crate::pools::chain_pool::GetTxCountTrait;
use crate::pools::{QueueRecord, QueueUpdateAdd, QueueUpdateMove, QueuesUpdate};
use crate::MempoolError;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SenderPoolState {
    /// The sender pools status should be determined by the chain tx count and updated accordingly.
    Idle,
    /// Transaction from the sender pool are scheduled to the chain pool.
    Queued(TxNonce),
    /// The sender pool has a transaction being executed by the executor.
    Processing(TxNonce),
}

impl SenderPoolState {
    fn nonce(&self) -> Option<TxNonce> {
        match self {
            Self::Idle => None,
            Self::Queued(nonce) | Self::Processing(nonce) => Some(*nonce),
        }
    }
}

#[derive(Debug)]
pub struct SenderPool {
    /// Chain id of the sender pool. (useful for debug/logging)
    #[allow(dead_code)]
    pub chain_id: ChainId,
    pub sender: Address,
    pub state: SenderPoolState,
    /// Priority queue of nonces. Contains only nonces/records that are not yet being scheduled in
    /// the chain pool tx_price_queue for execution and can be scheduled for execution.
    pub pending_nonce_queue: DoublePriorityQueue<QueueRecord, TxNonce>,
    /// Priority queue of nonces. Contains only gapped records.
    pub gapped_nonce_queue: DoublePriorityQueue<QueueRecord, TxNonce>,
    /// Map of nonces to queue records. Contains only nonces that are not yet being executed, i.e.
    /// nonces that are in the nonce queue or in the chain pool tx_price_queue. Processing
    /// transactions are not included in this map.
    pub nonce_map: HashMap<TxNonce, QueueRecord>,
    pub tx_count: u64,
}

impl SenderPool {
    pub fn new(chain_id: ChainId, sender: Address) -> Self {
        Self {
            chain_id,
            sender,
            state: SenderPoolState::Idle,
            pending_nonce_queue: DoublePriorityQueue::new(),
            gapped_nonce_queue: DoublePriorityQueue::new(),
            nonce_map: HashMap::new(),
            tx_count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.state, SenderPoolState::Idle) && self.nonce_map.is_empty()
    }

    pub fn is_suspended(&self) -> bool {
        self.state == SenderPoolState::Idle
            && self.pending_nonce_queue.is_empty()
            && !self.gapped_nonce_queue.is_empty()
    }

    pub fn get_pending_tx_count(&self) -> Option<u64> {
        if self.is_suspended() {
            return None;
        }

        let queue_nonce = self
            .pending_nonce_queue
            .peek_max()
            .map(|x| *x.1)
            .unwrap_or(0);
        let state_nonce = self.state.nonce().unwrap_or(0);
        Some(max(queue_nonce, self.tx_count).max(state_nonce))
    }

    pub fn get_by_nonce(&self, nonce: TxNonce) -> Option<&QueueRecord> {
        self.nonce_map.get(&nonce)
    }

    pub fn remove_by_nonce(&mut self, nonce: TxNonce) -> Option<QueueRecord> {
        if let Some(record) = self.nonce_map.remove(&nonce) {
            self.pending_nonce_queue.remove(&record);
            self.gapped_nonce_queue.remove(&record);

            if self.state == SenderPoolState::Queued(nonce) {
                self.state = SenderPoolState::Idle;
            }

            return Some(record);
        }
        None
    }

    // here we assume that we're adding records with nonce >= tx_count;
    pub fn add(&mut self, record: QueueRecord) -> QueuesUpdate {
        let nonce = record.nonce;
        self.nonce_map.insert(nonce, record.clone());

        let QueueMetrics { pending, .. } = self.get_metrics();
        let fits_by_tx_count = self.queued_tx_count() == nonce;
        let fits_by_range = pending.map_or(false, |(min, max)| nonce > min && nonce <= max + 1);
        let add_update = if fits_by_range || fits_by_tx_count {
            self.pending_nonce_queue.push(record.clone(), nonce);
            QueueUpdateAdd::Pending(record)
        } else {
            self.gapped_nonce_queue.push(record.clone(), nonce);
            QueueUpdateAdd::Gapped(record)
        };

        let mut result = self.rebalance();
        result.add_update = Some(add_update);
        result
    }

    pub fn set_tx_count(&mut self, tx_count: u64) -> QueuesUpdate {
        self.tx_count = tx_count;
        self.rebalance()
    }

    pub fn rebalance(&mut self) -> QueuesUpdate {
        let mut result = QueuesUpdate::default();

        // drop transactions with nonce < tx_count
        while let Some((_, &queued_nonce)) = self.pending_nonce_queue.peek_min() {
            if queued_nonce < self.tx_count {
                if let Some((record, _)) = self.pending_nonce_queue.pop_min() {
                    result.remove_nonce_too_small.push(record);
                }
            } else {
                break;
            }
        }
        while let Some((_, &queued_nonce)) = self.gapped_nonce_queue.peek_min() {
            if queued_nonce < self.tx_count {
                if let Some((record, _)) = self.gapped_nonce_queue.pop_min() {
                    result.remove_nonce_too_small.push(record);
                }
            } else {
                break;
            }
        }
        for record in &result.remove_nonce_too_small {
            self.nonce_map.remove(&record.nonce);
        }

        let mut move_to_gapped = Vec::new();
        // check if we need to dequeue a queued tx because it's nonce diverged from the tx count
        if let SenderPoolState::Queued(queued_nonce) = self.state {
            match queued_nonce.cmp(&self.tx_count) {
                std::cmp::Ordering::Less => {
                    self.state = SenderPoolState::Idle;
                    result.remove_queued_nonce_too_small = self.nonce_map.remove(&queued_nonce);
                }
                std::cmp::Ordering::Greater => {
                    result.remove_queued_nonce_too_small = self.nonce_map.remove(&queued_nonce);
                    if let Some(nonce_record) = self.nonce_map.get(&queued_nonce) {
                        move_to_gapped.push(nonce_record.clone());
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
        }

        // move pending txs to gapped if the first pending tx has a nonce > tx_count
        let min_pending = self.pending_nonce_queue.peek_min();
        if min_pending.map_or(false, |(_, &min)| min > self.queued_tx_count()) {
            while let Some((record, nonce)) = self.pending_nonce_queue.pop_min() {
                self.gapped_nonce_queue.push(record.clone(), nonce);
                move_to_gapped.push(record);
            }
            if !move_to_gapped.is_empty() {
                result.move_update = Some(QueueUpdateMove::PendingToGapped(move_to_gapped));
            }
            return result;
        }

        // if pending queue is empty there's a chance we can move gapped txs to pending
        let max_pending = self.pending_nonce_queue.peek_max().map(|(_, &n)| n + 1);
        let min_gapped = self.gapped_nonce_queue.peek_min().map(|(_, &n)| n);
        if let Some(mut last_nonce) =
            min_gapped.filter(|min_gapped| max_pending.unwrap_or(self.tx_count) == *min_gapped)
        {
            let mut move_to_pending = Vec::new();
            while self
                .gapped_nonce_queue
                .peek_min()
                .map_or(false, |(_, &nonce)| nonce == last_nonce)
            {
                let (record, nonce) = self
                    .gapped_nonce_queue
                    .pop_min()
                    .expect("checked existence");
                move_to_pending.push(record.clone());
                self.pending_nonce_queue.push(record, nonce);
                last_nonce += 1;
            }
            if !move_to_pending.is_empty() {
                result.move_update = Some(QueueUpdateMove::GappedToPending(move_to_pending));
            }
        }

        result
    }

    pub fn get_for_queueing(&mut self) -> Option<QueueRecord> {
        if let Some((_, &nonce)) = self.pending_nonce_queue.peek_min() {
            if nonce == self.tx_count {
                self.state = SenderPoolState::Queued(nonce);
                return self.pending_nonce_queue.pop_min().map(|x| x.0);
            }
        }
        None
    }

    pub fn remove(&mut self, record: &QueueRecord) {
        if self.state == SenderPoolState::Queued(record.nonce) {
            self.state = SenderPoolState::Idle;
        }
        self.pending_nonce_queue.remove(record);
        self.gapped_nonce_queue.remove(record);
        self.nonce_map.remove(&record.nonce);
    }

    pub fn set_processing(&mut self, nonce: TxNonce) {
        self.state = SenderPoolState::Processing(nonce);
        self.nonce_map.remove(&nonce);
        if self.tx_count == nonce {
            self.tx_count += 1;
        }
    }

    pub fn set_idle(&mut self) {
        self.state = SenderPoolState::Idle;
    }

    pub fn drain(&mut self) -> impl Iterator<Item = QueueRecord> + '_ {
        self.state = SenderPoolState::Idle;
        self.pending_nonce_queue.clear();
        self.gapped_nonce_queue.clear();
        self.nonce_map.drain().map(|(_, value)| value)
    }

    pub async fn update_tx_count<C: GetTxCountTrait>(
        &mut self,
        tx_count_api: &C,
    ) -> Result<QueuesUpdate, MempoolError> {
        use common::evm_loader::types::Address;

        let balance_addr = BalanceAddress {
            chain_id: self.chain_id,
            address: Address::from(<[u8; 20]>::from(self.sender.0)),
        };
        let tx_count = tx_count_api
            .get_transaction_count(balance_addr, Some(BlockNumberOrTag::Safe))
            .await?;
        Ok(self.set_tx_count(tx_count))
    }

    pub fn log_self(&self, caller: &'static str) {
        let Self {
            chain_id,
            sender,
            state,
            tx_count,
            nonce_map,
            ..
        } = self;
        let QueueMetrics { pending, gapped } = self.get_metrics();
        let (min_pending, max_pending) = pending.unzip();
        let (min_gapped, max_gapped) = gapped.unzip();
        tracing::debug!(
            chain_id, %sender, ?state, tx_count, nonce_map_len = nonce_map.len(),
            min_pending, max_pending, min_gapped, max_gapped,
            caller, "sender pool state"
        )
    }

    /// Tx count if queued transaction is considered processed
    fn queued_tx_count(&self) -> u64 {
        if let SenderPoolState::Queued(count) = self.state {
            count + 1
        } else {
            self.tx_count
        }
    }

    fn get_metrics(&self) -> QueueMetrics {
        let p_min = |queue: &DoublePriorityQueue<_, _>| queue.peek_min().map(|(_, &p)| p);
        let p_max = |queue: &DoublePriorityQueue<_, _>| queue.peek_max().map(|(_, &p)| p);
        let metrics = |queue: &DoublePriorityQueue<_, _>| p_min(queue).zip(p_max(queue));
        QueueMetrics {
            pending: metrics(&self.pending_nonce_queue),
            gapped: metrics(&self.gapped_nonce_queue),
        }
    }
}

struct QueueMetrics {
    pending: Option<(u64, u64)>,
    gapped: Option<(u64, u64)>,
}

#[cfg(test)]
#[allow(unused)] // TODO
mod tests {
    use std::collections::VecDeque;

    use proptest::prelude::*;

    use super::*;
    use crate::mempool::EthTxHash;

    const CHAIN_ID: ChainId = 1;

    fn create_record(nonce: TxNonce) -> QueueRecord {
        QueueRecord {
            sender: Address::random(),
            tx_hash: EthTxHash::random(),
            nonce,
            sorting_gas_price: 100,
        }
    }

    fn create_sender_pool() -> SenderPool {
        SenderPool::new(CHAIN_ID, Address::random())
    }

    #[test]
    fn test_is_empty() {
        let pool = create_sender_pool();
        assert!(pool.is_empty());

        let mut pool_with_record = create_sender_pool();
        let record = create_record(0);
        pool_with_record.nonce_map.insert(0, record);
        assert!(!pool_with_record.is_empty());
    }

    #[test]
    fn test_get_pending_tx_count() {
        let mut pool = create_sender_pool();
        pool.tx_count = 1;
        assert_eq!(pool.get_pending_tx_count(), Some(1));

        pool.pending_nonce_queue.push(create_record(1), 1);
        pool.pending_nonce_queue.push(create_record(2), 2);
        assert_eq!(pool.get_pending_tx_count(), Some(2));

        pool.gapped_nonce_queue.push(create_record(3), 3);
        assert_eq!(pool.get_pending_tx_count(), Some(2));
        pool.pending_nonce_queue.clear();
        assert_eq!(pool.get_pending_tx_count(), None);
    }

    #[test]
    fn test_add_to_pending_queue() {
        let mut pool = create_sender_pool();

        let record0 = create_record(0);
        let result = pool.add(record0.clone());
        let expected_result = QueuesUpdate {
            add_update: Some(QueueUpdateAdd::Pending(record0.clone())),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.pending_nonce_queue.len(), 1);
        assert_eq!(pool.gapped_nonce_queue.len(), 0);
        assert!(pool.nonce_map.contains_key(&record0.nonce));

        let record1 = create_record(1);
        let result = pool.add(record1.clone());
        let expected_result = QueuesUpdate {
            add_update: Some(QueueUpdateAdd::Pending(record1.clone())),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.pending_nonce_queue.len(), 2);
        assert_eq!(pool.gapped_nonce_queue.len(), 0);
        assert!(pool.nonce_map.contains_key(&record1.nonce));

        let mut pool = create_sender_pool();
        pool.state = SenderPoolState::Queued(0);
        let result = pool.add(record1.clone());
        let expected_result = QueuesUpdate {
            add_update: Some(QueueUpdateAdd::Pending(record1.clone())),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.pending_nonce_queue.len(), 1);
        assert_eq!(pool.gapped_nonce_queue.len(), 0);
        assert_eq!(pool.state, SenderPoolState::Queued(0));
    }

    #[test]
    fn test_add_to_gapped_queue() {
        let mut pool = create_sender_pool();
        let record2 = create_record(2);
        let result = pool.add(record2.clone());
        let expected_result = QueuesUpdate {
            add_update: Some(QueueUpdateAdd::Gapped(record2.clone())),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.gapped_nonce_queue.len(), 1);
        assert!(pool.nonce_map.contains_key(&record2.nonce));

        let mut pool = create_sender_pool();
        pool.add(create_record(0));
        pool.add(create_record(1));
        let record3 = create_record(3);
        let result = pool.add(record3.clone());
        let expected_result = QueuesUpdate {
            add_update: Some(QueueUpdateAdd::Gapped(record3.clone())),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.gapped_nonce_queue.len(), 1);
        assert!(pool.nonce_map.contains_key(&record3.nonce));
        assert_eq!(pool.state, SenderPoolState::Idle);
    }

    #[test]
    fn test_remove_by_nonce() {
        let mut pool = create_sender_pool();

        let record = create_record(0);
        pool.add(record.clone());
        let removed = pool.remove_by_nonce(0);
        assert_eq!(removed, Some(record.clone()));
        assert!(!pool.nonce_map.contains_key(&0));
        assert_eq!(pool.pending_nonce_queue.len(), 0);

        let record = create_record(1);
        pool.add(record.clone());
        let removed = pool.remove_by_nonce(1);
        assert_eq!(removed, Some(record.clone()));
        assert!(!pool.nonce_map.contains_key(&1));
        assert_eq!(pool.gapped_nonce_queue.len(), 0);
    }

    #[test]
    fn test_drain_pool() {
        let mut pool = create_sender_pool();

        let record1 = create_record(0);
        let record2 = create_record(1);
        let record3 = create_record(3);

        pool.add(record1.clone());
        pool.add(record2.clone());
        pool.add(record3.clone());

        let mut drained = pool.drain().collect::<Vec<_>>();
        drained.sort_by_key(|x| x.nonce);
        assert_eq!(drained, vec![record1, record2, record3]);
        assert!(pool.nonce_map.is_empty());
        assert!(pool.pending_nonce_queue.is_empty());
        assert!(pool.gapped_nonce_queue.is_empty());
    }

    #[test]
    fn test_set_processing() {
        let mut pool = create_sender_pool();
        pool.add(create_record(0));

        pool.set_processing(0);
        assert_eq!(pool.state, SenderPoolState::Processing(0));
        assert!(!pool.nonce_map.contains_key(&0));
        assert_eq!(pool.tx_count, 1);
    }

    #[test]
    fn test_set_idle() {
        let mut pool = create_sender_pool();

        pool.state = SenderPoolState::Processing(1);
        pool.set_idle();

        assert_eq!(pool.state, SenderPoolState::Idle);
    }

    #[test]
    fn test_set_tx_count_no_change() {
        let mut pool = create_sender_pool();
        pool.tx_count = 5;

        let result = pool.set_tx_count(5);

        let expected_result = QueuesUpdate::default();
        assert_eq!(result, expected_result);
        assert_eq!(pool.tx_count, 5);
    }

    #[test]
    fn test_set_tx_count_not_suspended() {
        let mut pool = create_sender_pool();

        let record0 = create_record(0);
        let record1 = create_record(1);
        let record2 = create_record(2);
        let record4 = create_record(4);
        let record5 = create_record(5);
        let record6 = create_record(6);
        let record8 = create_record(8);

        // pending
        pool.add(record0.clone());
        pool.add(record1.clone());
        pool.add(record2.clone());
        // gapped
        pool.add(record4.clone());
        pool.add(record5.clone());
        pool.add(record6.clone());
        pool.add(record8.clone());

        let result = pool.set_tx_count(2);
        let expected_result = QueuesUpdate {
            remove_nonce_too_small: vec![record0.clone(), record1.clone()],
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.tx_count, 2);
        assert_eq!(pool.state, SenderPoolState::Idle);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record2.clone(), 2)])
        );
        assert_eq!(
            pool.gapped_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![
                (record4.clone(), 4),
                (record5.clone(), 5),
                (record6.clone(), 6),
                (record8.clone(), 8)
            ])
        );
        assert!(!pool.nonce_map.contains_key(&0));
        assert!(!pool.nonce_map.contains_key(&1));

        let result = pool.set_tx_count(5);
        let expected_result = QueuesUpdate {
            remove_nonce_too_small: vec![record2.clone(), record4.clone()],
            move_update: Some(QueueUpdateMove::GappedToPending(vec![
                record5.clone(),
                record6.clone(),
            ])),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.tx_count, 5);
        assert_eq!(pool.state, SenderPoolState::Idle);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![
                (record5.clone(), 5),
                (record6.clone(), 6),
            ])
        );
        assert_eq!(
            pool.gapped_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record8.clone(), 8)])
        );

        let mut pool = create_sender_pool();
        pool.tx_count = 2;
        let record2 = create_record(2);
        let record3 = create_record(3);
        pool.add(record2.clone());
        pool.add(record3.clone());

        let result = pool.set_tx_count(1);
        let expected_result = QueuesUpdate {
            move_update: Some(QueueUpdateMove::PendingToGapped(vec![
                record2.clone(),
                record3.clone(),
            ])),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![])
        );
        assert_eq!(
            pool.gapped_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![
                (record2.clone(), 2),
                (record3.clone(), 3)
            ])
        );
    }

    #[test]
    fn test_set_tx_count_suspended() {
        let mut pool = create_sender_pool();
        pool.tx_count = 1;

        let record2 = create_record(2);
        let record3 = create_record(3);
        let record5 = create_record(5);

        pool.add(record2.clone());
        pool.add(record3.clone());
        pool.add(record5.clone());

        let result = pool.set_tx_count(2);
        let expected_result = QueuesUpdate {
            move_update: Some(QueueUpdateMove::GappedToPending(vec![
                record2.clone(),
                record3.clone(),
            ])),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.state, SenderPoolState::Idle);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![
                (record2.clone(), 2),
                (record3.clone(), 3),
            ])
        );
        assert_eq!(
            pool.gapped_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record5.clone(), 5)])
        );

        let mut pool = create_sender_pool();

        pool.add(record2.clone());

        let result = pool.set_tx_count(1);
        let expected_result = QueuesUpdate::default();
        assert_eq!(result, expected_result);
        assert_eq!(pool.pending_nonce_queue.len(), 0);
        assert_eq!(pool.gapped_nonce_queue.len(), 1);
    }

    #[test]
    fn test_set_tx_count_deque() {
        let record0 = create_record(0);
        let record1 = create_record(1);
        let record2 = create_record(2);
        let record3 = create_record(3);
        let record5 = create_record(5);

        let mut pool = create_sender_pool();
        pool.nonce_map.insert(0, record0.clone());
        pool.nonce_map.insert(0, record0.clone());
        pool.state = SenderPoolState::Queued(0);
        pool.add(record1.clone());
        let result = pool.set_tx_count(1);
        let expected_result = QueuesUpdate {
            remove_queued_nonce_too_small: Some(record0.clone()),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.state, SenderPoolState::Idle);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record1, 1)])
        );
        assert!(pool.gapped_nonce_queue.is_empty());

        let mut pool = create_sender_pool();
        pool.state = SenderPoolState::Queued(0);
        pool.nonce_map.insert(0, record0.clone());
        pool.add(record2.clone());
        let result = pool.set_tx_count(1);
        let expected_result = QueuesUpdate {
            remove_queued_nonce_too_small: Some(record0.clone()),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        // assert_eq!(pool.state, SenderPoolState::Suspended);
        assert!(pool.pending_nonce_queue.is_empty());
        assert_eq!(
            pool.gapped_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record2.clone(), 2)])
        );

        let mut pool = create_sender_pool();
        pool.state = SenderPoolState::Queued(0);
        pool.nonce_map.insert(0, record0.clone());
        pool.add(record2.clone());
        pool.add(record3.clone());
        pool.add(record5.clone());
        let result = pool.set_tx_count(2);
        let expected_result = QueuesUpdate {
            remove_queued_nonce_too_small: Some(record0.clone()),
            move_update: Some(QueueUpdateMove::GappedToPending(vec![
                record2.clone(),
                record3.clone(),
            ])),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.state, SenderPoolState::Idle);
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![
                (record2.clone(), 2),
                (record3.clone(), 3)
            ])
        );
    }

    #[test]
    fn test_set_tx_count_processing() {
        let mut pool = create_sender_pool();
        pool.state = SenderPoolState::Processing(1);
        pool.tx_count = 2;
        let record3 = create_record(3);
        pool.add(record3.clone());
        let result = pool.set_tx_count(3);
        let expected_result = QueuesUpdate {
            move_update: Some(QueueUpdateMove::GappedToPending(vec![record3.clone()])),
            ..Default::default()
        };
        assert_eq!(result, expected_result);
        assert_eq!(pool.state, SenderPoolState::Processing(1));
        assert_eq!(
            pool.pending_nonce_queue,
            DoublePriorityQueue::<QueueRecord, TxNonce>::from(vec![(record3, 3)])
        );
    }

    proptest! {
        /// Tests that all transactions from an arbitrary shuffled continuous valid sequence
        /// get eventually queued.
        #[test]
        fn test_continuous_sequence(
            nonces in (1..1000u64)                            // Sequence of length from 1 to 1000
                .prop_map(|len| (0..len).collect::<Vec<_>>()) // of all numbers from 0 to len
                .prop_shuffle()                               // arbitrarily shuffled
        ) {
            let mut pool = create_sender_pool();
            for nonce in &nonces {
                let rec = create_record(*nonce);
                pool.add(rec);
            }
            for nonce in 0..nonces.len() {
                let rec = pool.get_for_queueing().unwrap();
                assert_eq!(rec.nonce, nonce as u64);
                pool.set_processing(rec.nonce);
                pool.set_idle();
            }
            assert!(pool.is_empty())
        }
    }
}
