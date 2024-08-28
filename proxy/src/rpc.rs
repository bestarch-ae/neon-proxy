use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use jsonrpsee::core::RpcResult;
use jsonrpsee::types::{ErrorCode, ErrorObjectOwned};
use reth_primitives::{BlockId, BlockNumberOrTag};
use rpc_api_types::Log;
use rpc_api_types::RichBlock;
use rpc_api_types::{Filter, FilterBlockOption};

use sqlx::PgPool;

use common::types::NeonTxInfo;
use db::WithBlockhash;
use executor::Executor;
use neon_api::NeonApi;

use crate::convert::build_block;
use crate::convert::{convert_rich_log, LogFilters};
pub use crate::rpc::eth::{NeonEthApiServer, NeonFilterApiServer};
pub use crate::rpc::neon::NeonCustomApiServer;
use crate::Error;

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
    executor: Option<Arc<Executor>>,
    mp_gas_prices: mempool::GasPrices,
    lib_version: String,
}

impl EthApiImpl {
    pub fn new(
        pool: PgPool,
        neon_api: NeonApi,
        chain_id: u64,
        executor: Option<Arc<Executor>>,
        mp_gas_prices: mempool::GasPrices,
        lib_version: String,
    ) -> Self {
        let transactions = ::db::TransactionRepo::new(pool.clone());
        let blocks = ::db::BlockRepo::new(pool.clone());

        Self {
            transactions,
            blocks,
            neon_api,
            executor,
            chain_id,
            mp_gas_prices,
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

    async fn get_tag_by_block_id(&self, block_id: BlockId) -> RpcResult<BlockNumberOrTag> {
        match block_id {
            BlockId::Hash(hash) => {
                use common::solana_sdk::hash::Hash;
                let hash = Hash::new_from_array(hash.block_hash.0);
                let block = self
                    .get_block(db::BlockBy::Hash(hash), false, false)
                    .await?;
                Ok(block
                    .and_then(|block| block.header.number.map(BlockNumberOrTag::Number))
                    .ok_or::<ErrorObjectOwned>(ErrorCode::InternalError.into())?)
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

    async fn get_logs(&self, filters: LogFilters) -> Result<Vec<Log>, Error> {
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

    async fn neon_evm_version(&self) -> RpcResult<String> {
        let config = self.neon_api.get_config().await?;
        let version = format!("Neon-EVM/v{}-{}", config.version, config.revision);
        Ok(version)
    }
}

pub fn unimplemented<T>() -> RpcResult<T> {
    Err(ErrorObjectOwned::borrowed(
        ErrorCode::MethodNotFound.code(),
        "method not implemented",
        None,
    ))
}
