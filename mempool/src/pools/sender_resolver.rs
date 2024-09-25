use std::sync::{Arc, Weak};

use crossbeam_queue::SegQueue;
use dashmap::DashSet;
use reth_primitives::alloy_primitives::{FixedBytes, TxNonce};
use reth_primitives::Address;
use tokio::sync::mpsc::{Receiver, Sender};

use common::neon_lib::types::BalanceAddress;

use crate::mempool::Command;
use crate::pools::chain_pool::GetTxCountTrait;

// todo: duplicate
const ONE_BLOCK_MS: u64 = 400;

#[derive(Debug)]
pub enum SendersResolverCommand {
    Add(SenderResolverRecord),
    Remove(Address),
    Shutdown,
}

#[derive(Debug)]
pub struct SenderResolverRecord {
    pub nonce: TxNonce,
    pub sender: BalanceAddress,
}

#[derive(Debug)]
pub struct SendersResolver<C: GetTxCountTrait> {
    tx_count_api: C,
    senders: Arc<SegQueue<SenderResolverRecord>>,
    senders_to_remove: Arc<DashSet<Address>>,
}

impl<C: GetTxCountTrait> SendersResolver<C> {
    pub fn new(tx_count_api: C) -> Self {
        Self {
            tx_count_api,
            senders: Arc::new(SegQueue::new()),
            senders_to_remove: Arc::new(DashSet::new()),
        }
    }

    pub async fn start(
        self,
        cmd_rx: Receiver<SendersResolverCommand>,
        chain_pool_cmd_tx: Sender<Command>,
    ) {
        let mut cmd_rx = cmd_rx;

        tokio::spawn(resolver_loop(
            self.tx_count_api.clone(),
            Arc::downgrade(&self.senders),
            Arc::downgrade(&self.senders_to_remove),
            chain_pool_cmd_tx,
        ));

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SendersResolverCommand::Add(record) => {
                    self.senders.push(record);
                }
                SendersResolverCommand::Remove(sender) => {
                    self.senders_to_remove.insert(sender);
                }
                SendersResolverCommand::Shutdown => {
                    return;
                }
            }
        }
    }
}

async fn resolver_loop<C: GetTxCountTrait>(
    tx_count_api: C,
    queue: Weak<SegQueue<SenderResolverRecord>>,
    to_remove: Weak<DashSet<Address>>,
    chain_pool_cmd_tx: Sender<Command>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(3 * ONE_BLOCK_MS));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                loop {
                    let Some(q) = queue.upgrade() else {
                        return;
                    };
                    let Some(ignore) = to_remove.upgrade() else {
                        return;
                    };
                    let Some(record) = q.pop() else {
                        break;
                    };
                    let sender_address = Address(FixedBytes(record.sender.address.0));
                    if ignore.contains(&sender_address) {
                        continue;
                    }
                    let tx_count = match tx_count_api.get_transaction_count(record.sender, None).await {
                        Ok(tx_count) => tx_count,
                        Err(err) => {
                            tracing::error!(?err, "failed to get tx count");
                            continue;
                        }
                    };

                    if tx_count != record.nonce {
                        chain_pool_cmd_tx.send(Command::SetTxCount(sender_address, tx_count)).await.unwrap();
                    } else {
                        q.push(record);
                    }
                }
            }
        }
    }
}
