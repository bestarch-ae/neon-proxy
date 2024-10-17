use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxEnvelope, TypedTransaction};
use anyhow::anyhow;
use futures_util::{StreamExt, TryStreamExt};
use operator::Operator;
use reth_primitives::Address;
use reth_primitives::{BlockId, BlockNumberOrTag, B256};
use rpc_api_types::RichBlock;
use rpc_api_types::{Filter, FilterBlockOption};

use sqlx::PgPool;

use common::types::NeonTxInfo;
use db::WithBlockhash;
use executor::{ExecuteRequest, Executor};
use mempool::{GasPrices, Mempool, PreFlightValidator};
use neon_api::NeonApi;
use operator::Operators;

use crate::convert::build_block;
use crate::convert::{convert_rich_log, LogFilters};
use crate::error::Error;
pub use crate::rpc::eth::{NeonEthApiServer, NeonFilterApiServer};
pub use crate::rpc::neon::{NeonCustomApiServer, NeonLog};

mod eth;
mod neon;
mod net;
mod web3;

#[derive(Clone)]
pub struct EthApiImpl {
    transactions: ::db::TransactionRepo,
    blocks: ::db::BlockRepo,
    neon_api: NeonApi,
    chain_id: u64,
    mempool: Option<Arc<Mempool<Executor, GasPrices>>>,
    mp_gas_prices: GasPrices,
    operators: Operators,
    lib_version: String,
}

impl EthApiImpl {
    pub fn new(
        pool: PgPool,
        neon_api: NeonApi,
        chain_id: u64,
        mempool: Option<Arc<Mempool<Executor, GasPrices>>>,
        mp_gas_prices: GasPrices,
        operators: Operators,
        lib_version: String,
    ) -> Self {
        let transactions = ::db::TransactionRepo::new(pool.clone());
        let blocks = ::db::BlockRepo::new(pool.clone());

        Self {
            transactions,
            blocks,
            neon_api,
            mempool,
            chain_id,
            mp_gas_prices,
            operators,
            lib_version,
        }
    }

    pub fn with_chain_id(self, chain_id: u64) -> Self {
        Self { chain_id, ..self }
    }

    async fn find_slot(&self, tag: BlockNumberOrTag) -> Result<u64, Error> {
        let is_finalized = match tag {
            BlockNumberOrTag::Number(slot) => return Ok(slot),
            BlockNumberOrTag::Earliest => return Ok(self.blocks.earliest_slot().await?),
            BlockNumberOrTag::Pending => false,
            // Confirmed
            BlockNumberOrTag::Latest => false,
            // Finalized
            BlockNumberOrTag::Finalized | BlockNumberOrTag::Safe => true,
        };

        Ok(self.blocks.latest_number(is_finalized).await?)
    }

    async fn normalize_filter(&self, filter: Filter) -> Result<Filter, Error> {
        let mut filter = filter;
        if let FilterBlockOption::Range {
            ref mut from_block,
            ref mut to_block,
        } = filter.block_option
        {
            for tag_ref in [from_block, to_block] {
                if let Some(tag) = tag_ref {
                    let tag = *tag;
                    tag_ref.replace(BlockNumberOrTag::Number(self.find_slot(tag).await?));
                }
            }
        }
        Ok(filter)
    }

    async fn get_block(
        &self,
        by: db::BlockBy,
        full: bool,
        is_pending: bool,
    ) -> Result<Option<RichBlock>, Error> {
        let Some(mut block) = self.blocks.fetch_by(by).await? else {
            return Ok(None);
        };
        let slot = block.slot;
        let txs = if is_pending {
            block.slot += 1;
            Vec::new()
        } else {
            self.transactions
                .fetch(::db::TransactionBy::Slot(slot))
                .await?
                .into_iter()
                .map(|tx| tx.inner)
                .collect()
        };
        Ok(Some(build_block(block, txs, full)?.into()))
    }

    async fn get_tag_by_block_id(&self, block_id: BlockId) -> Result<BlockNumberOrTag, Error> {
        match block_id {
            BlockId::Hash(hash) => {
                use common::solana_sdk::hash::Hash;
                let hash = Hash::new_from_array(hash.block_hash.0);
                let block = self
                    .get_block(db::BlockBy::Hash(hash), false, false)
                    .await?;
                Ok(block
                    .and_then(|block| block.header.number.map(BlockNumberOrTag::Number))
                    .ok_or_else(|| Error::Other(anyhow!("could not find block: {hash}")))?)
            }
            BlockId::Number(BlockNumberOrTag::Pending) => Ok(BlockNumberOrTag::Pending),
            BlockId::Number(BlockNumberOrTag::Latest) => Ok(BlockNumberOrTag::Latest),
            BlockId::Number(tag) => Ok(BlockNumberOrTag::Number(self.find_slot(tag).await?)),
        }
    }

    async fn get_transaction(
        &self,
        by: db::TransactionBy,
    ) -> Result<Option<WithBlockhash<NeonTxInfo>>, Error> {
        let tx = self.transactions.fetch(by).await?.into_iter().next();
        Ok(tx)
    }

    async fn get_logs(&self, filters: LogFilters) -> Result<Vec<NeonLog>, Error> {
        self.transactions
            .fetch_rich_logs(
                filters.block,
                &filters.address,
                filters.topics.each_ref().map(Vec::as_slice),
            )
            .map_ok(convert_rich_log)
            .map(|item| Ok(item??))
            .try_collect::<Vec<_>>()
            .await
    }

    async fn neon_evm_version(&self) -> Result<String, Error> {
        let config = self.neon_api.get_config().await?;
        let version = format!("Neon-EVM/v{}-{}", config.version, config.revision);
        Ok(version)
    }

    async fn send_transaction(&self, request: ExecuteRequest) -> Result<B256, Error> {
        if let Some(mempool) = self.mempool.as_ref() {
            let hash = *request.tx_hash();
            tracing::debug!(tx_hash = %hash, ?request, "sending transaction");
            let price_model = self.mp_gas_prices.get_gas_price_model(Some(self.chain_id));
            PreFlightValidator::validate(&request, &self.neon_api, &self.transactions, price_model)
                .await?;
            mempool.schedule_tx(request).await.inspect_err(
                |error| tracing::warn!(%hash, %error, "could not schedule transaction"),
            )?;

            tracing::info!(tx_hash = %hash, "sendRawTransaction done");
            Ok(hash)
        } else {
            tracing::debug!(?request, "skip sending transaction, mempool disabled");
            Err(Error::Unimplemented)
        }
    }

    fn get_operator(&self, address: &Address) -> Result<Arc<Operator>, Error> {
        self.operators.try_get(address).map_err(Into::into)
    }

    fn sign_transaction(&self, addr: &Address, tx: TypedTransaction) -> Result<TxEnvelope, Error> {
        let operator = self.get_operator(addr)?;
        let mut tx = tx;
        // TODO: Remove this nonsense when reth gets updated
        let signable: &mut dyn SignableTransaction<_> = match tx {
            TypedTransaction::Legacy(ref mut tx) => tx,
            TypedTransaction::Eip2930(ref mut tx) => tx,
            TypedTransaction::Eip1559(ref mut tx) => tx,
            TypedTransaction::Eip4844(ref mut tx) => tx,
        };
        let signature = operator.sign_eth_transaction(signable)?;
        // TODO: Remove this nonsense when reth gets updated
        let envelope: TxEnvelope = match tx {
            TypedTransaction::Legacy(tx) => tx.into_signed(signature).into(),
            TypedTransaction::Eip2930(tx) => tx.into_signed(signature).into(),
            TypedTransaction::Eip1559(tx) => tx.into_signed(signature).into(),
            TypedTransaction::Eip4844(tx) => tx.into_signed(signature).into(),
        };
        Ok(envelope)
    }
}
