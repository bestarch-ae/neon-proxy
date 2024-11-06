use alloy_rlp::Encodable;
use anyhow::Context;
use jsonrpsee::core::{async_trait, RpcResult};
use jsonrpsee::proc_macros::rpc;
use reth_primitives::{Address, BlockId, BlockNumberOrTag, Bytes, TxKind, B256, B64, U256, U64};
use rpc_api::servers::EthApiServer;
use rpc_api::EthFilterApiServer;
use rpc_api_types::serde_helpers::JsonStorageKey;
use rpc_api_types::state::StateOverride;
use rpc_api_types::{AccessListWithGasUsed, EIP1186AccountProofResponse, EthCallResponse};
use rpc_api_types::{AnyTransactionReceipt, Transaction, TransactionRequest};
use rpc_api_types::{BlockOverrides, Header, RichBlock};
use rpc_api_types::{Bundle, FeeHistory, Index, StateContext, SyncStatus, Work};
use rpc_api_types::{Filter, FilterChanges, FilterId};
use rpc_api_types::{Log, PendingTransactionFilterKind, SyncInfo};

use common::convert::{ToNeon, ToReth};
use common::neon_lib::types::{BalanceAddress, TxParams};
use executor::ExecuteRequest;
use mempool::GasPricesTrait;

use crate::convert::{convert_filters, EthNeonLog, NeonTransactionReceipt};
use crate::convert::{neon_to_eth, neon_to_eth_receipt};
use crate::error::{call_execution_failed, invalid_params, unimplemented, Error};
use crate::rpc::EthApiImpl;

#[async_trait]
impl EthApiServer for EthApiImpl {
    /// Returns the protocol version encoded as a string.
    async fn protocol_version(&self) -> RpcResult<U64> {
        unimplemented()
    }

    /// Returns an object with data about the sync status or false.
    fn syncing(&self) -> RpcResult<SyncStatus> {
        let neon_api = self.neon_api.clone();
        let status: Result<_, Error> = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let earliest_slot = self.blocks.earliest_slot().await.context("earliest")?;
                let latest_slot = self.blocks.latest_number(false).await.context("latest")?;
                neon_api
                    .get_health()
                    .await
                    .with_context(|| "failed to get health")?;
                Ok(SyncStatus::Info(SyncInfo {
                    starting_block: U256::from(earliest_slot),
                    current_block: U256::from(latest_slot),
                    highest_block: U256::from(latest_slot),
                    warp_chunks_amount: None,
                    warp_chunks_processed: None,
                }))
            })
        });
        match status {
            Ok(healthy) => Ok(healthy),
            Err(_err) => Ok(SyncStatus::None),
        }
    }

    /// Returns the client coinbase address.
    async fn author(&self) -> RpcResult<Address> {
        unimplemented()
    }

    /// Returns a list of addresses owned by client.
    fn accounts(&self) -> RpcResult<Vec<Address>> {
        Ok(self.operators.addresses().copied().collect())
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
    async fn block_transaction_count_by_hash(&self, hash: B256) -> RpcResult<Option<U256>> {
        use common::solana_sdk::hash::Hash;

        let hash = Hash::new_from_array(hash.0);
        Ok(Some(
            self.get_block(db::BlockBy::Hash(hash), false, false)
                .await
                .map(|block| block.map(|block| U256::from(block.transactions.len())))
                .map_err(Error::from)?
                .unwrap_or(U256::ZERO),
        ))
    }

    /// Returns the number of transactions in a block matching the given block number.
    async fn block_transaction_count_by_number(
        &self,
        number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        let slot = self.find_slot(number).await?;
        Ok(Some(
            self.get_block(db::BlockBy::Slot(slot), false, number.is_pending())
                .await
                .map(|block| {
                    Some(
                        block
                            .map(|block| U256::from(block.transactions.len()))
                            .unwrap_or(U256::ZERO),
                    )
                })
                .map_err(Error::from)?
                .unwrap_or(U256::ZERO),
        ))
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
        hash: B256,
        index: Index,
    ) -> RpcResult<Option<Transaction>> {
        let tx = self
            .get_transaction(db::TransactionBy::BlockHashAndIndex(
                hash.0.into(),
                Some(usize::from(index) as u64),
            ))
            .await?
            .map(|tx| neon_to_eth(tx.inner, tx.blockhash).map_err(Error::from))
            .transpose()?;
        Ok(tx)
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
        number: BlockNumberOrTag,
        index: Index,
    ) -> RpcResult<Option<Transaction>> {
        let number = self.find_slot(number).await?;
        let tx = self
            .get_transaction(db::TransactionBy::BlockNumberAndIndex(
                number,
                usize::from(index) as u64,
            ))
            .await?
            .map(|tx| neon_to_eth(tx.inner, tx.blockhash).map_err(Error::from))
            .transpose()?;
        Ok(tx)
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
        use common::evm_loader::types::Address;

        let Some(block_number) = block_number else {
            return Err(invalid_params("Missing block number"));
        };
        let tag = Some(self.get_tag_by_block_id(block_number).await?);

        let balance_address = BalanceAddress {
            address: Address::from(<[u8; 20]>::from(address.0)),
            chain_id: self.chain_id,
        };
        let balance = self.neon_api.get_balance(balance_address, tag).await?;

        tracing::info!(%address, ?block_number, %balance, ?tag, "get balance returns");

        Ok(balance.to_reth())
    }

    /// Returns the value from a storage position at a given address
    async fn storage_at(
        &self,
        address: Address,
        index: JsonStorageKey,
        block_number: Option<BlockId>,
    ) -> RpcResult<B256> {
        let slot = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };

        let data = self
            .neon_api
            .get_storage_at(address.to_neon(), index.0.to_neon(), slot)
            .await?;
        Ok(B256::from(data))
    }

    /// Returns the number of transactions sent from an address at given block number.
    async fn transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<U256> {
        use common::evm_loader::types::Address;

        let slot = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };

        let mut mpl_tx_cnt = 0;
        if let Some(mempool) = &self.mempool {
            if matches!(slot, Some(BlockNumberOrTag::Pending)) {
                mpl_tx_cnt = mempool
                    .get_pending_tx_cnt(&address, self.chain_id)
                    .await
                    .unwrap_or(0)
            }
        }

        let balance_address = BalanceAddress {
            address: Address::from(<[u8; 20]>::from(address.0)),
            chain_id: self.chain_id,
        };
        let chain_tx_cnt = self
            .neon_api
            .get_transaction_count(balance_address, slot)
            .await?;

        let cnt = std::cmp::max(mpl_tx_cnt, chain_tx_cnt);

        Ok(U256::from(cnt))
    }

    /// Returns code at a given address at given block number.
    async fn get_code(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<Bytes> {
        let slot = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };

        let bytes = self.neon_api.get_code(address.to_neon(), slot).await?;
        Ok(bytes.unwrap_or_default())
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
        block_number: Option<BlockId>,
        _state_overrides: Option<StateOverride>,
        _block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<Bytes> {
        tracing::info!("call {:?}", request);

        let tag = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };

        let gas = U256::from(request.gas.unwrap_or(1 << 64)).to_neon();
        let gas_price = U256::from(request.gas.unwrap_or(1 << 64)).to_neon();

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
            gas_limit: Some(gas),
            gas_price: Some(gas_price),
            access_list: request
                .access_list
                .map(|list| list.0.into_iter().map(ToNeon::to_neon).collect()),
            actual_gas_used: None,
            chain_id: Some(self.chain_id),
        };
        let response = self.neon_api.call(tx, tag).await?;
        Ok(Bytes::from(response.result))
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
        block_number: Option<BlockId>,
        _state_override: Option<StateOverride>,
    ) -> RpcResult<U256> {
        tracing::info!(?block_number, "estimate_gas {request:?}");
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

        let tag = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };

        let gas = self
            .neon_api
            .estimate_gas(tx, tag)
            .await
            .map(ToReth::to_reth)?;
        Ok(gas)
    }

    /// Returns the current price per gas in wei.
    async fn gas_price(&self) -> RpcResult<U256> {
        let price = self.mp_gas_prices.get_gas_price(Some(self.chain_id));
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
        Ok(false)
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

    /// Signs a transaction that can be submitted to the network at a later time using with
    /// `sendRawTransaction.`
    async fn sign_transaction(&self, request: TransactionRequest) -> RpcResult<Bytes> {
        tracing::info!(?request, "sign transaction");
        let address = request
            .from
            .ok_or_else(|| invalid_params("Missing sender"))?;
        let transaction = request.build_typed_tx().map_err(|request| {
            tracing::debug!(?request, "cannot build transaction from request");
            invalid_params("Invalid transaction request")
        })?;
        let envelope = self.sign_transaction(&address, transaction)?;
        let mut bytes = Vec::new();
        envelope.encode(&mut bytes);
        Ok(bytes.into())
    }

    /// Sends transaction will block waiting for signer to return the
    /// transaction hash.
    async fn send_transaction(&self, request: TransactionRequest) -> RpcResult<B256> {
        use common::evm_loader::types::Address;

        let mut request = request;
        tracing::info!(?request, "send transaction");
        let address = request
            .from
            .ok_or_else(|| invalid_params("Missing sender"))?;

        if request.to.is_none() {
            tracing::debug!("setting to to Create");
            request.to = Some(TxKind::Create);
        }

        if request.nonce.is_none() {
            let balance_address = BalanceAddress {
                address: Address::from(<[u8; 20]>::from(address.0)),
                chain_id: self.chain_id,
            };
            let nonce = self
                .neon_api
                .get_transaction_count(balance_address, Some(BlockNumberOrTag::Pending))
                .await?;
            tracing::debug!(?nonce, "setting nonce");
            request = request.nonce(nonce);
        }

        let transaction = request.build_typed_tx().map_err(|request| {
            tracing::debug!(?request, "cannot build transaction from request");
            invalid_params("Invalid transaction request")
        })?;
        let envelope = self.sign_transaction(&address, transaction)?;
        self.send_transaction(ExecuteRequest::new(envelope, self.chain_id))
            .await
            .map_err(Into::into)
    }

    /// Sends signed transaction, returning its hash.
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        tracing::info!(%bytes, "sendRawTransaction");
        let execute_request = ExecuteRequest::from_bytes(&bytes, self.chain_id)
            .inspect_err(|error| tracing::warn!(%bytes, %error, "could not decode transaction"))
            .map_err(|_| invalid_params(""))?;
        self.send_transaction(execute_request)
            .await
            .map_err(Into::into)
    }

    /// Returns an Ethereum specific signature with: sign(keccak256("\x19Ethereum Signed Message:\n"
    /// + len(message) + message))).
    async fn sign(&self, address: Address, message: Bytes) -> RpcResult<Bytes> {
        tracing::info!(%address, ?message, "sign");
        let operator = self.get_operator(&address)?;

        match operator.sign_message(&message) {
            Ok(signature) => Ok(signature.as_bytes().into()),
            Err(error) => {
                tracing::warn!(%address, ?message, ?error, "failed signing message");
                // Error format taken from neon-proxy.py
                Err(call_execution_failed("Error signing message"))
            }
        }
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

#[rpc(server, namespace = "eth")]
trait NeonFilterApi: EthFilterApiServer {
    #[method(name = "getLogs")]
    async fn neon_logs(&self, filter: Filter) -> RpcResult<Vec<EthNeonLog>>;
}

#[async_trait]
impl NeonFilterApiServer for EthApiImpl {
    async fn neon_logs(&self, filter: Filter) -> RpcResult<Vec<EthNeonLog>> {
        let logs = <Self as EthFilterApiServer>::logs(self, filter).await?;
        let logs = logs
            .into_iter()
            .map(crate::convert::to_eth_neon_log)
            .collect();
        Ok(logs)
    }
}

#[async_trait]
impl EthFilterApiServer for EthApiImpl {
    /// Returns logs matching given filter object.
    async fn logs(&self, filter: Filter) -> RpcResult<Vec<Log>> {
        let filter = self.normalize_filter(filter).await?;
        let filters = convert_filters(filter).map_err(Error::from)?;
        Ok(self
            .get_logs(filters)
            .await?
            .into_iter()
            .map(|l| l.log)
            .collect())
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
