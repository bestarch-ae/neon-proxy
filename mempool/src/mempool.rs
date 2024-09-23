use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use alloy_consensus::{Transaction, TxEnvelope};
use dashmap::DashMap;
use reth_primitives::alloy_primitives::TxNonce;
use reth_primitives::{Address, ChainId, B256};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use executor::{ExecuteRequest, ExecutorTrait};
use neon_api::NeonApi;

use crate::error::MempoolError;
use crate::pools::{ChainPool, ChainPoolConfig};
use crate::GasPricesTrait;

pub type EthTxHash = B256;
pub type GasPrice = u128;

const CMD_CHANNEL_SIZE: usize = 1024;

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
pub enum Command {
    ScheduleTx(TxRecord, oneshot::Sender<Result<(), MempoolError>>),
    Shutdown,
    ExecuteTx,
    SetTxCount(Address, TxNonce),
    GetPendingTxCount(Address, oneshot::Sender<Option<u64>>),
    GetTxHash(Address, TxNonce, oneshot::Sender<Option<EthTxHash>>),
}

pub struct Mempool<E: ExecutorTrait, G: GasPricesTrait> {
    config: Config,
    executor: Arc<E>,
    txs: Arc<DashMap<EthTxHash, TxRecord>>,
    chain_pool_txs: HashMap<ChainId, Sender<Command>>,
    neon_api: NeonApi,
    gas_prices: G,
}

impl<E: ExecutorTrait, G: GasPricesTrait> Mempool<E, G> {
    pub fn new(config: Config, gas_prices: G, executor: Arc<E>, neon_api: NeonApi) -> Self {
        Self {
            config,
            executor,
            txs: Arc::new(DashMap::new()),
            chain_pool_txs: HashMap::new(),
            neon_api,
            gas_prices,
        }
    }

    /// Gets the number of transactions for a given sender with pending commitment.
    pub async fn get_pending_tx_cnt(&self, sender: &Address, chain_id: ChainId) -> Option<u64> {
        if let Some(tx) = self.chain_pool_txs.get(&chain_id) {
            let (cmd_tx, cmd_rx) = oneshot::channel();
            tx.send(Command::GetPendingTxCount(*sender, cmd_tx))
                .await
                .unwrap();
            return cmd_rx.await.unwrap();
        }
        None
    }

    /// Gets a transaction by hash.
    pub fn get_tx_by_hash(&self, tx_hash: EthTxHash) -> Option<TxEnvelope> {
        self.txs.get(&tx_hash).map(|x| x.value().tx().clone())
    }

    /// Gets a transaction by sender and nonce.
    pub async fn get_tx_by_sender_nonce(
        &self,
        sender: &Address,
        nonce: TxNonce,
        chain_id: ChainId,
    ) -> Option<TxEnvelope> {
        if let Some(tx) = self.chain_pool_txs.get(&chain_id) {
            let (cmd_tx, cmd_rx) = oneshot::channel();
            tx.send(Command::GetTxHash(*sender, nonce, cmd_tx))
                .await
                .unwrap();
            return cmd_rx
                .await
                .unwrap()
                .map(|x| self.get_tx_by_hash(x).unwrap());
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

        if let Some(tx) = self.chain_pool_txs.get(&chain_id) {
            let (cmd_tx, cmd_rx) = oneshot::channel();
            tx.send(Command::ScheduleTx(tx_record, cmd_tx))
                .await
                .unwrap();
            cmd_rx.await.unwrap()
        } else {
            Err(MempoolError::UnknownChainID(chain_id))
        }
    }

    pub async fn shutdown(&self) {
        for tx in self.chain_pool_txs.values() {
            tx.send(Command::Shutdown).await.unwrap();
        }
    }

    pub async fn start(&mut self) -> Result<(), MempoolError> {
        let config = self.neon_api.get_config().await?;

        for chain_info in config.chains {
            let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(CMD_CHANNEL_SIZE);
            let chain_id = chain_info.id;
            let token_pkey = chain_info.token;

            self.chain_pool_txs.insert(chain_id, cmd_tx.clone());

            let config = ChainPoolConfig {
                chain_id,
                capacity: self.config.capacity,
                capacity_high_watermark: self.config.capacity_high_watermark,
                token_pkey,
                eviction_timeout_sec: self.config.eviction_timeout_sec,
            };
            ChainPool::create_and_start(
                config,
                self.gas_prices.clone(),
                self.neon_api.clone(),
                Arc::clone(&self.executor),
                Arc::clone(&self.txs),
                cmd_tx,
                cmd_rx,
            );
        }
        Ok(())
    }
}
