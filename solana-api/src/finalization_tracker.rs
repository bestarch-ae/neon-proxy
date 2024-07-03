use std::collections::VecDeque;
use std::future;
use std::time::Duration;

use solana_client::client_error::ClientError;
use tokio::time::{sleep_until, Instant};

use crate::solana_api::SolanaApi;

pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum BlockStatus {
    Pending,
    Purged,
    Finalized,
}

#[derive(Debug)]
pub(super) struct FinalizationTracker {
    init_slot: u64,
    finalized_slots: VecDeque<u64>,
    pending_slots: VecDeque<u64>,
    api: SolanaApi,
    last_checked: Instant,
    poll_interval: Duration,
}

impl FinalizationTracker {
    pub async fn init(api: SolanaApi, poll_interval: Duration) -> Result<Self, ClientError> {
        let init_slot = api.get_finalized_slot().await?;
        Ok(Self {
            init_slot,
            finalized_slots: VecDeque::new(),
            pending_slots: VecDeque::new(),
            api,
            last_checked: Instant::now(),
            poll_interval,
        })
    }

    /// Checks if slot was finalized or purged. If status is not available yet,
    /// schedules slot to be checked later.
    /// Panics if called with a slot that is lower than slots from previous calls.
    pub fn check_or_schedule_new_slot(&mut self, slot: u64) -> BlockStatus {
        assert!(self.pending_slots.back().map_or(true, |&back| slot > back));
        assert!(self
            .pending_slots
            .front()
            .map_or(true, |&front| slot > front),);

        // Required to keep correct order in the `next` method
        if !self.pending_slots.is_empty() {
            self.pending_slots.push_back(slot);
            return BlockStatus::Pending;
        }

        if slot <= self.init_slot {
            return BlockStatus::Finalized;
        }

        if let Some(is_finalized) = self.make_progress_for_slot(slot) {
            return if is_finalized {
                BlockStatus::Finalized
            } else {
                BlockStatus::Purged
            };
        }

        self.pending_slots.push_back(slot);
        BlockStatus::Pending
    }

    /// Returns next pending slot and a boolean if the slot was finalized or purged.
    /// Is cancel safe
    pub async fn next(&mut self) -> Result<(u64, bool), ClientError> {
        loop {
            if let Some(pending_slot) = self.pending_slots.front().copied() {
                if let Some(is_finalized) = self.make_progress_for_slot(pending_slot) {
                    self.pending_slots.pop_front();
                    return Ok((pending_slot, is_finalized));
                }

                // At this point `self.finalized_slots` must be empty`
                self.update_finalized_slots().await?;
                continue;
            }

            // At this point there's no point in looping since we have no slots to check
            future::pending().await
        }
    }

    fn make_progress_for_slot(&mut self, slot: u64) -> Option<bool> {
        while self
            .finalized_slots
            .front()
            .map_or(false, |&front| front <= slot)
        {
            let front = self.finalized_slots.pop_front().expect("checked exists");
            if front == slot {
                return Some(true);
            }
        }

        if self
            .finalized_slots
            .front()
            .map_or(false, |&front| front > slot)
        {
            return Some(false);
        }

        None
    }

    async fn update_finalized_slots(&mut self) -> Result<(), ClientError> {
        if let Some(from) = self.pending_slots.front() {
            if self.last_checked.elapsed() < self.poll_interval {
                sleep_until(self.last_checked + self.poll_interval).await;
            }
            let new_blocks = self.api.get_finalized_blocks(*from).await?;
            self.last_checked = Instant::now();
            self.finalized_slots.extend(new_blocks);
        }

        Ok(())
    }
}
