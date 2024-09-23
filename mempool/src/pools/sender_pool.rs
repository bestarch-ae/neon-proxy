use std::cmp::max;
use std::collections::HashMap;

use priority_queue::DoublePriorityQueue;
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, ChainId};

use crate::pools::{QueueRecord, QueuesUpdate, StateUpdate};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SenderPoolState {
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

    pub fn get_pending_tx_count(&self) -> Option<u64> {
        match self.state {
            SenderPoolState::Suspended => None,
            SenderPoolState::Queued(_) | SenderPoolState::Processing(_) | SenderPoolState::Idle => {
                let queue_nonce = self
                    .pending_nonce_queue
                    .peek_max()
                    .map(|x| *x.1)
                    .unwrap_or(0);
                Some(max(queue_nonce, self.tx_count))
            }
        }
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

    pub fn add(&mut self, record: QueueRecord) -> QueuesUpdate {
        let mut result = QueuesUpdate::default();
        let nonce = record.nonce;
        self.nonce_map.insert(nonce, record.clone());
        if let Some((_, &max_nonce)) = self.pending_nonce_queue.peek_max() {
            if max_nonce + 1 == nonce {
                self.pending_nonce_queue.push(record.clone(), nonce);
                result.add_to_pending.push(record.clone());
                return result;
            }
        } else if nonce == self.tx_count {
            self.pending_nonce_queue.push(record.clone(), nonce);
            result.add_to_pending.push(record.clone());
            let mut last_nonce = nonce + 1;
            while let Some((_, &nonce)) = self.gapped_nonce_queue.peek_min() {
                if nonce == last_nonce {
                    if let Some((record, _)) = self.gapped_nonce_queue.pop_min() {
                        result.move_to_pending.push(record.clone());
                        self.pending_nonce_queue.push(record, nonce);
                        last_nonce += 1;
                    }
                } else {
                    break;
                }
            }
            if self.state == SenderPoolState::Suspended {
                self.state = SenderPoolState::Idle;
                result.state_update = StateUpdate::Unsuspended(self.sender);
            }
            return result;
        }

        result.add_to_gapped.push(record.clone());
        self.gapped_nonce_queue.push(record, nonce);
        result
    }

    pub fn set_tx_count(&mut self, tx_count: u64) -> QueuesUpdate {
        let mut result = QueuesUpdate::default();
        if self.tx_count == tx_count {
            return result;
        }
        self.tx_count = tx_count;

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
        if let SenderPoolState::Queued(queued_nonce) = self.state {
            match queued_nonce.cmp(&self.tx_count) {
                std::cmp::Ordering::Less => {
                    self.state = SenderPoolState::Idle;
                    result.remove_queued_nonce_too_small = self.nonce_map.remove(&queued_nonce);
                }
                std::cmp::Ordering::Greater => {
                    self.state = SenderPoolState::Suspended;
                    result.state_update = StateUpdate::Suspended(self.sender);
                    if let Some(nonce_record) = self.nonce_map.get(&queued_nonce) {
                        result.move_to_gapped.push(nonce_record.clone());
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
        }

        if self.tx_count
            < self
                .pending_nonce_queue
                .peek_min()
                .map(|(_, &n)| n)
                .unwrap_or(u64::MIN)
        {
            while let Some((record, nonce)) = self.pending_nonce_queue.pop_min() {
                self.gapped_nonce_queue.push(record.clone(), nonce);
                result.move_to_gapped.push(record);
            }
            if !matches!(
                self.state,
                SenderPoolState::Suspended
                    | SenderPoolState::Processing(_)
                    | SenderPoolState::Queued(_)
            ) {
                self.state = SenderPoolState::Suspended;
                result.state_update = StateUpdate::Suspended(self.sender);
            }
            return result;
        }

        if self.pending_nonce_queue.is_empty() {
            let mut last_nonce = self.tx_count;
            while let Some((_, &nonce)) = self.gapped_nonce_queue.peek_min() {
                if nonce == last_nonce {
                    if let Some((record, _)) = self.gapped_nonce_queue.pop_min() {
                        result.move_to_pending.push(record.clone());
                        self.pending_nonce_queue.push(record, nonce);
                        last_nonce += 1;
                    }
                } else {
                    break;
                }
            }
        }

        if !matches!(
            self.state,
            SenderPoolState::Processing(_) | SenderPoolState::Queued(_)
        ) {
            if let Some(min_nonce) = self.pending_nonce_queue.peek_min().map(|(_, &n)| n) {
                if min_nonce == self.tx_count {
                    if self.state == SenderPoolState::Suspended {
                        result.state_update = StateUpdate::Unsuspended(self.sender);
                        self.state = SenderPoolState::Idle;
                    }
                } else if self.state != SenderPoolState::Suspended {
                    self.state = SenderPoolState::Suspended;
                    result.state_update = StateUpdate::Suspended(self.sender);
                }
            } else if self.state == SenderPoolState::Suspended && self.gapped_nonce_queue.is_empty()
            {
                result.state_update = StateUpdate::Unsuspended(self.sender);
                self.state = SenderPoolState::Idle;
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

    pub fn drain(&mut self) -> impl Iterator<Item = QueueRecord> {
        self.state = SenderPoolState::Idle;
        let transactions: Vec<QueueRecord> = self.nonce_map.values().cloned().collect();
        self.nonce_map.clear();
        self.pending_nonce_queue.clear();
        self.gapped_nonce_queue.clear();
        transactions.into_iter()
    }
}
