use alloy_consensus::TxEnvelope;
use alloy_rlp::Decodable;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::ErrorCode;
use jsonrpsee::types::ErrorObjectOwned;
use reth_primitives::TxKind;
use reth_primitives::{Address, BlockId, BlockNumberOrTag, Bytes, B256, B64, U256, U64};
use rpc_api::servers::EthApiServer;
use rpc_api::EthFilterApiServer;
use rpc_api_types::serde_helpers::JsonStorageKey;
use rpc_api_types::Filter;
use rpc_api_types::FilterBlockOption;
use rpc_api_types::FilterChanges;
use rpc_api_types::FilterId;
use rpc_api_types::Log;
use rpc_api_types::PendingTransactionFilterKind;
use rpc_api_types::{
    state::StateOverride, AccessListWithGasUsed, AnyTransactionReceipt, BlockOverrides, Bundle,
    EIP1186AccountProofResponse, EthCallResponse, FeeHistory, Header, Index, RichBlock,
    StateContext, SyncStatus, Transaction, TransactionRequest, Work,
};
use sqlx::PgPool;

use common::neon_lib::types::{BalanceAddress, TxParams};
use common::types::NeonTxInfo;
use db::WithBlockhash;

use crate::convert::convert_filters;
use crate::convert::convert_rich_log;
use crate::convert::LogFilters;
use crate::convert::{
    build_block, neon_to_eth, neon_to_eth_receipt, NeonLog, NeonTransactionReceipt,
};
use crate::executor::Executor;
use crate::mempool;
use crate::neon_api::NeonApi;
use crate::Error;

#[derive(Clone)]
pub struct EthApiImpl {
    transactions: ::db::TransactionRepo,
    blocks: ::db::BlockRepo,
    neon_api: NeonApi,
    chain_id: u64,
    executor: Option<Executor>,
    mp_gas_prices: mempool::GasPrices,
}

impl EthApiImpl {
    pub fn new(
        pool: PgPool,
        neon_api: NeonApi,
        chain_id: u64,
        executor: Option<Executor>,
        mp_gas_prices: mempool::GasPrices,
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
        }
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

    /// Returns none for latest, and pending
    async fn get_slot_by_block_id(&self, block_id: BlockId) -> RpcResult<Option<u64>> {
        match block_id {
            BlockId::Hash(hash) => {
                use common::solana_sdk::hash::Hash;
                let hash = Hash::new_from_array(hash.block_hash.0);
                let block = self
                    .get_block(db::BlockBy::Hash(hash), false, false)
                    .await?;
                Ok(block.and_then(|block| block.header.number))
            }
            BlockId::Number(BlockNumberOrTag::Pending | BlockNumberOrTag::Latest) => Ok(None),
            BlockId::Number(tag) => Ok(Some(self.find_slot(tag).await?)),
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
}

fn unimplemented<T>() -> RpcResult<T> {
    Err(ErrorObjectOwned::borrowed(
        ErrorCode::MethodNotFound.code(),
        "method not implemented",
        None,
    ))
}

#[rpc(server, namespace = "eth")]
trait NeonEthApi: EthApiServer {
    #[method(name = "getTransactionReceipt")]
    async fn neon_transaction_receipt(
        &self,
        hash: B256,
    ) -> RpcResult<Option<NeonTransactionReceipt>>;
}

#[async_trait]
impl NeonEthApiServer for EthApiImpl {
    async fn neon_transaction_receipt(
        &self,
        hash: B256,
    ) -> RpcResult<Option<NeonTransactionReceipt>> {
        let receipt = <Self as EthApiServer>::transaction_receipt(self, hash).await?;
        let receipt = receipt.map(crate::convert::to_neon_receipt);
        Ok(receipt)
    }
}

#[async_trait]
impl EthApiServer for EthApiImpl {
    /// Returns the protocol version encoded as a string.
    async fn protocol_version(&self) -> RpcResult<U64> {
        unimplemented()
    }

    /// Returns an object with data about the sync status or false.
    fn syncing(&self) -> RpcResult<SyncStatus> {
        unimplemented()
    }

    /// Returns the client coinbase address.
    async fn author(&self) -> RpcResult<Address> {
        unimplemented()
    }

    /// Returns a list of addresses owned by client.
    fn accounts(&self) -> RpcResult<Vec<Address>> {
        unimplemented()
    }

    /// Returns the number of most recent block.
    /// TODO: why is this not async?
    fn block_number(&self) -> RpcResult<U256> {
        let blocks = self.blocks.clone();
        let slot_time = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { blocks.latest_block_time().await })
        })
        .map_err(Error::from)?;
        let slot = slot_time.map(|(slot, _)| slot).unwrap_or(0);
        Ok(U256::from(slot))
    }

    /// Returns the chain ID of the current network.
    async fn chain_id(&self) -> RpcResult<Option<U64>> {
        Ok(Some(U64::from(self.chain_id)))
    }

    /// Returns information about a block by hash.
    async fn block_by_hash(&self, hash: B256, full: bool) -> RpcResult<Option<RichBlock>> {
        use common::solana_sdk::hash::Hash;

        let hash = Hash::new_from_array(hash.0);
        self.get_block(db::BlockBy::Hash(hash), full, false)
            .await
            .map_err(Into::into)
    }

    /// Returns information about a block by number.
    async fn block_by_number(
        &self,
        tag: BlockNumberOrTag,
        full: bool,
    ) -> RpcResult<Option<RichBlock>> {
        let slot = self.find_slot(tag).await?;
        self.get_block(db::BlockBy::Slot(slot), full, tag.is_pending())
            .await
            .map_err(Into::into)
    }

    /// Returns the number of transactions in a block from a block matching the given block hash.
    async fn block_transaction_count_by_hash(&self, _hash: B256) -> RpcResult<Option<U256>> {
        unimplemented()
    }

    /// Returns the number of transactions in a block matching the given block number.
    async fn block_transaction_count_by_number(
        &self,
        _number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        unimplemented()
    }

    /// Returns the number of uncles in a block from a block matching the given block hash.
    async fn block_uncles_count_by_hash(&self, _hash: B256) -> RpcResult<Option<U256>> {
        unimplemented()
    }

    /// Returns the number of uncles in a block with given block number.
    async fn block_uncles_count_by_number(
        &self,
        _number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        unimplemented()
    }

    /// Returns all transaction receipts for a given block.
    async fn block_receipts(
        &self,
        _block_id: BlockId,
    ) -> RpcResult<Option<Vec<AnyTransactionReceipt>>> {
        unimplemented()
    }

    /// Returns an uncle block of the given block and index.
    async fn uncle_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<RichBlock>> {
        unimplemented()
    }

    /// Returns an uncle block of the given block and index.
    async fn uncle_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<RichBlock>> {
        unimplemented()
    }

    /// Returns the EIP-2718 encoded transaction if it exists.
    ///
    /// If this is a EIP-4844 transaction that is in the pool it will include the sidecar.
    async fn raw_transaction_by_hash(&self, _hash: B256) -> RpcResult<Option<Bytes>> {
        unimplemented()
    }

    /// Returns the information about a transaction requested by transaction hash.
    async fn transaction_by_hash(&self, hash: B256) -> RpcResult<Option<Transaction>> {
        let tx = self
            .get_transaction(db::TransactionBy::Hash(hash.0.into()))
            .await?
            .map(|tx| neon_to_eth(tx.inner, tx.blockhash).map_err(Error::from))
            .transpose()?;
        Ok(tx)
    }

    /// Returns information about a raw transaction by block hash and transaction index position.
    async fn raw_transaction_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<Bytes>> {
        unimplemented()
    }

    /// Returns information about a transaction by block hash and transaction index position.
    async fn transaction_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<Transaction>> {
        unimplemented()
    }

    /// Returns information about a raw transaction by block number and transaction index
    /// position.
    async fn raw_transaction_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<Bytes>> {
        unimplemented()
    }

    /// Returns information about a transaction by block number and transaction index position.
    async fn transaction_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<Transaction>> {
        unimplemented()
    }

    /// Returns the receipt of a transaction by transaction hash.
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<AnyTransactionReceipt>> {
        let receipt = self
            .get_transaction(db::TransactionBy::Hash(hash.0.into()))
            .await?
            .map(|tx| neon_to_eth_receipt(tx.inner, tx.blockhash).map_err(Error::from))
            .transpose()?;
        Ok(receipt)
    }

    /// Returns the balance of the account of given address.
    async fn balance(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        use crate::convert::ToReth;
        use common::evm_loader::types::Address;

        let slot = if let Some(block_number) = block_number {
            self.get_slot_by_block_id(block_number).await?
        } else {
            None
        };

        let balance_address = BalanceAddress {
            address: Address::from(<[u8; 20]>::from(address.0)),
            chain_id: self.chain_id,
        };
        let balance = self.neon_api.get_balance(balance_address, slot).await?;

        Ok(balance.to_reth())
    }

    /// Returns the value from a storage position at a given address
    async fn storage_at(
        &self,
        _address: Address,
        _index: JsonStorageKey,
        _block_number: Option<BlockId>,
    ) -> RpcResult<B256> {
        unimplemented()
    }

    /// Returns the number of transactions sent from an address at given block number.
    async fn transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<U256> {
        use common::evm_loader::types::Address;

        let slot = if let Some(block_number) = block_number {
            self.get_slot_by_block_id(block_number).await?
        } else {
            None
        };

        let balance_address = BalanceAddress {
            address: Address::from(<[u8; 20]>::from(address.0)),
            chain_id: self.chain_id,
        };
        let balance = self
            .neon_api
            .get_transaction_count(balance_address, slot)
            .await?;

        Ok(U256::from(balance))
    }

    /// Returns code at a given address at given block number.
    async fn get_code(
        &self,
        _address: Address,
        _block_number: Option<BlockId>,
    ) -> RpcResult<Bytes> {
        unimplemented()
    }

    /// Returns the block's header at given number.
    async fn header_by_number(&self, _hash: BlockNumberOrTag) -> RpcResult<Option<Header>> {
        unimplemented()
    }

    /// Returns the block's header at given hash.
    async fn header_by_hash(&self, _hash: B256) -> RpcResult<Option<Header>> {
        unimplemented()
    }

    /// Executes a new message call immediately without creating a transaction on the block chain.
    async fn call(
        &self,
        request: TransactionRequest,
        _block_number: Option<BlockId>,
        _state_overrides: Option<StateOverride>,
        _block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<Bytes> {
        use crate::convert::ToNeon;
        tracing::info!("call {:?}", request);

        let tx = TxParams {
            nonce: request.nonce,
            from: request.from.map(ToNeon::to_neon).unwrap_or_default(),
            to: match request.to {
                Some(TxKind::Call(addr)) => Some(ToNeon::to_neon(addr)),
                Some(TxKind::Create) => None,
                None => None,
            },
            data: request.input.data.map(|data| data.to_vec()),
            value: request.value.map(ToNeon::to_neon),
            gas_limit: request.gas.map(U256::from).map(ToNeon::to_neon),
            gas_price: request.gas_price.map(U256::from).map(ToNeon::to_neon),
            access_list: request
                .access_list
                .map(|list| list.0.into_iter().map(ToNeon::to_neon).collect()),
            actual_gas_used: None,
            chain_id: Some(self.chain_id),
        };
        let data = self.neon_api.call(tx).await?;
        Ok(Bytes::from(data))
    }

    /// Simulate arbitrary number of transactions at an arbitrary blockchain index, with the
    /// optionality of state overrides
    async fn call_many(
        &self,
        _bundle: Bundle,
        _state_context: Option<StateContext>,
        _state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<EthCallResponse>> {
        unimplemented()
    }

    /// Generates an access list for a transaction.
    ///
    /// This method creates an [EIP2930](https://eips.ethereum.org/EIPS/eip-2930) type accessList based on a given Transaction.
    ///
    /// An access list contains all storage slots and addresses touched by the transaction, except
    /// for the sender account and the chain's precompiles.
    ///
    /// It returns list of addresses and storage keys used by the transaction, plus the gas
    /// consumed when the access list is added. That is, it gives you the list of addresses and
    /// storage keys that will be used by that transaction, plus the gas consumed if the access
    /// list is included. Like eth_estimateGas, this is an estimation the list could change
    /// when the transaction is actually mined. Adding an accessList to your transaction does
    /// not necessary result in lower gas usage compared to a transaction without an access
    /// list.
    async fn create_access_list(
        &self,
        _request: TransactionRequest,
        _block_number: Option<BlockId>,
    ) -> RpcResult<AccessListWithGasUsed> {
        unimplemented()
    }

    /// Generates and returns an estimate of how much gas is necessary to allow the transaction to
    /// complete.
    async fn estimate_gas(
        &self,
        request: TransactionRequest,
        _block_number: Option<BlockId>,
        _state_override: Option<StateOverride>,
    ) -> RpcResult<U256> {
        use crate::convert::{ToNeon, ToReth};
        tracing::info!("estimate_gas {request:?}");
        let tx = TxParams {
            nonce: request.nonce,
            from: request.from.map(ToNeon::to_neon).unwrap_or_default(),
            to: match request.to {
                Some(TxKind::Call(addr)) => Some(ToNeon::to_neon(addr)),
                Some(TxKind::Create) => None,
                None => None,
            },
            data: request.input.data.map(|data| data.to_vec()),
            value: request.value.map(ToNeon::to_neon),
            gas_limit: request.gas.map(U256::from).map(ToNeon::to_neon),
            gas_price: request.gas_price.map(U256::from).map(ToNeon::to_neon),
            access_list: request
                .access_list
                .map(|list| list.0.into_iter().map(ToNeon::to_neon).collect()),
            actual_gas_used: None,
            chain_id: Some(self.chain_id),
        };
        let gas = self.neon_api.estimate_gas(tx).await.map(ToReth::to_reth)?;
        Ok(gas)
    }

    /// Returns the current price per gas in wei.
    async fn gas_price(&self) -> RpcResult<U256> {
        let price = self.mp_gas_prices.get_gas_price();
        Ok(U256::from(price))
    }

    /// Introduced in EIP-1559, returns suggestion for the priority for dynamic fee transactions.
    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256> {
        unimplemented()
    }

    /// Introduced in EIP-4844, returns the current blob base fee in wei.
    async fn blob_base_fee(&self) -> RpcResult<U256> {
        unimplemented()
    }

    /// Returns the Transaction fee history
    ///
    /// Introduced in EIP-1559 for getting information on the appropriate priority fee to use.
    ///
    /// Returns transaction base fee per gas and effective priority fee per gas for the
    /// requested/supported block range. The returned Fee history for the returned block range
    /// can be a subsection of the requested range if not all blocks are available.
    async fn fee_history(
        &self,
        _block_count: u64,
        _newest_block: BlockNumberOrTag,
        _reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory> {
        unimplemented()
    }

    /// Returns whether the client is actively mining new blocks.
    async fn is_mining(&self) -> RpcResult<bool> {
        unimplemented()
    }

    /// Returns the number of hashes per second that the node is mining with.
    async fn hashrate(&self) -> RpcResult<U256> {
        unimplemented()
    }

    /// Returns the hash of the current block, the seedHash, and the boundary condition to be met
    /// (“target”)
    async fn get_work(&self) -> RpcResult<Work> {
        unimplemented()
    }

    /// Used for submitting mining hashrate.
    ///
    /// Can be used for remote miners to submit their hash rate.
    /// It accepts the miner hash rate and an identifier which must be unique between nodes.
    /// Returns `true` if the block was successfully submitted, `false` otherwise.
    async fn submit_hashrate(&self, _hashrate: U256, _id: B256) -> RpcResult<bool> {
        unimplemented()
    }

    /// Used for submitting a proof-of-work solution.
    async fn submit_work(
        &self,
        _nonce: B64,
        _pow_hash: B256,
        _mix_digest: B256,
    ) -> RpcResult<bool> {
        unimplemented()
    }

    /// Sends transaction will block waiting for signer to return the
    /// transaction hash.
    async fn send_transaction(&self, _request: TransactionRequest) -> RpcResult<B256> {
        unimplemented()
    }

    /// Sends signed transaction, returning its hash.
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        if let Some(executor) = self.executor.as_ref() {
            let bytes: &mut &[u8] = &mut bytes.as_ref();
            let envelope = TxEnvelope::decode(bytes)
                .map_err(|_| ErrorObjectOwned::from(ErrorCode::InvalidParams))?;
            let hash = *envelope.tx_hash();
            executor
                .handle_transaction(envelope)
                .await
                .map_err(|_| ErrorObjectOwned::from(ErrorCode::InternalError))?; // TODO

            Ok(hash)
        } else {
            unimplemented()
        }
    }

    /// Returns an Ethereum specific signature with: sign(keccak256("\x19Ethereum Signed Message:\n"
    /// + len(message) + message))).
    async fn sign(&self, _address: Address, _message: Bytes) -> RpcResult<Bytes> {
        unimplemented()
    }

    /// Signs a transaction that can be submitted to the network at a later time using with
    /// `sendRawTransaction.`
    async fn sign_transaction(&self, _transaction: TransactionRequest) -> RpcResult<Bytes> {
        unimplemented()
    }

    /// Signs data via [EIP-712](https://github.com/ethereum/EIPs/blob/master/EIPS/eip-712.md).
    async fn sign_typed_data(
        &self,
        _address: Address,
        _data: serde_json::Value,
    ) -> RpcResult<Bytes> {
        unimplemented()
    }

    /// Returns the account and storage values of the specified account including the Merkle-proof.
    /// This call can be used to verify that the data you are pulling from is not tampered with.
    async fn get_proof(
        &self,
        _address: Address,
        _keys: Vec<JsonStorageKey>,
        _block_number: Option<BlockId>,
    ) -> RpcResult<EIP1186AccountProofResponse> {
        unimplemented()
    }
}

#[rpc(server, namespace = "eth")]
trait NeonFilterApi: EthFilterApiServer {
    #[method(name = "getLogs")]
    async fn neon_logs(&self, filter: Filter) -> RpcResult<Vec<NeonLog>>;
}

#[async_trait]
impl NeonFilterApiServer for EthApiImpl {
    async fn neon_logs(&self, filter: Filter) -> RpcResult<Vec<NeonLog>> {
        let logs = <Self as EthFilterApiServer>::logs(self, filter).await?;
        let logs = logs.into_iter().map(crate::convert::to_neon_log).collect();
        Ok(logs)
    }
}

#[async_trait]
impl EthFilterApiServer for EthApiImpl {
    /// Returns logs matching given filter object.
    async fn logs(&self, filter: Filter) -> RpcResult<Vec<Log>> {
        let filter = self.normalize_filter(filter).await?;
        let filters = convert_filters(filter).map_err(Error::from)?;
        Ok(self.get_logs(filters).await?)
    }

    /// Creates anew filter and returns its id.
    async fn new_filter(&self, _filter: Filter) -> RpcResult<FilterId> {
        unimplemented()
    }

    /// Creates a new block filter and returns its id.
    async fn new_block_filter(&self) -> RpcResult<FilterId> {
        unimplemented()
    }

    /// Creates a pending transaction filter and returns its id.
    async fn new_pending_transaction_filter(
        &self,
        _kind: Option<PendingTransactionFilterKind>,
    ) -> RpcResult<FilterId> {
        unimplemented()
    }

    /// Returns all filter changes since last poll.
    async fn filter_changes(&self, _id: FilterId) -> RpcResult<FilterChanges> {
        unimplemented()
    }

    /// Returns all logs matching given filter (in a range 'from' - 'to').
    async fn filter_logs(&self, _id: FilterId) -> RpcResult<Vec<Log>> {
        unimplemented()
    }

    /// Uninstalls filter.
    async fn uninstall_filter(&self, _id: FilterId) -> RpcResult<bool> {
        unimplemented()
    }
}
