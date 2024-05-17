use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use reth_primitives::{Address, BlockId, BlockNumberOrTag, Bytes, B256, B64, U256, U64};
use rpc_api::servers::EthApiServer;
use rpc_api_types::serde_helpers::JsonStorageKey;
use rpc_api_types::{
    state::StateOverride, AccessListWithGasUsed, AnyTransactionReceipt, BlockOverrides, Bundle,
    EIP1186AccountProofResponse, EthCallResponse, FeeHistory, Header, Index, RichBlock,
    StateContext, SyncStatus, Transaction, TransactionRequest, Work,
};
use sqlx::PgPool;

use crate::convert::build_block;
use crate::db;

#[derive(Clone)]
pub struct EthApiImpl {
    transactions: db::TransactionRepo,
    blocks: ::db::BlockRepo,
}

impl EthApiImpl {
    pub fn new(pool: PgPool) -> Self {
        let transactions = db::TransactionRepo::new(pool.clone());
        let blocks = ::db::BlockRepo::new(pool);
        Self {
            transactions,
            blocks,
        }
    }
}

#[async_trait]
impl EthApiServer for EthApiImpl {
    /// Returns the protocol version encoded as a string.
    async fn protocol_version(&self) -> RpcResult<U64> {
        todo!()
    }

    /// Returns an object with data about the sync status or false.
    fn syncing(&self) -> RpcResult<SyncStatus> {
        todo!()
    }

    /// Returns the client coinbase address.
    async fn author(&self) -> RpcResult<Address> {
        todo!()
    }

    /// Returns a list of addresses owned by client.
    fn accounts(&self) -> RpcResult<Vec<Address>> {
        todo!()
    }

    /// Returns the number of most recent block.
    fn block_number(&self) -> RpcResult<U256> {
        todo!()
    }

    /// Returns the chain ID of the current network.
    async fn chain_id(&self) -> RpcResult<Option<U64>> {
        todo!()
    }

    /// Returns information about a block by hash.
    async fn block_by_hash(&self, _hash: B256, _full: bool) -> RpcResult<Option<RichBlock>> {
        todo!()
    }

    /// Returns information about a block by number.
    async fn block_by_number(
        &self,
        number: BlockNumberOrTag,
        full: bool,
    ) -> RpcResult<Option<RichBlock>> {
        let BlockNumberOrTag::Number(slot) = number else {
            todo!()
        };
        let Some(block) = self.blocks.fetch_by_slot(slot).await.unwrap() else {
            return Ok(None);
        };
        let (txs, receipts) = self
            .transactions
            .fetch_transactions_with_receipts_for_block(slot)
            .await
            .unwrap()
            .into_iter()
            .unzip();
        Ok(Some(build_block(block, receipts, txs, full).into()))
    }

    /// Returns the number of transactions in a block from a block matching the given block hash.
    async fn block_transaction_count_by_hash(&self, _hash: B256) -> RpcResult<Option<U256>> {
        todo!()
    }

    /// Returns the number of transactions in a block matching the given block number.
    async fn block_transaction_count_by_number(
        &self,
        _number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        todo!()
    }

    /// Returns the number of uncles in a block from a block matching the given block hash.
    async fn block_uncles_count_by_hash(&self, _hash: B256) -> RpcResult<Option<U256>> {
        todo!()
    }

    /// Returns the number of uncles in a block with given block number.
    async fn block_uncles_count_by_number(
        &self,
        _number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        todo!()
    }

    /// Returns all transaction receipts for a given block.
    async fn block_receipts(
        &self,
        _block_id: BlockId,
    ) -> RpcResult<Option<Vec<AnyTransactionReceipt>>> {
        todo!()
    }

    /// Returns an uncle block of the given block and index.
    async fn uncle_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<RichBlock>> {
        todo!()
    }

    /// Returns an uncle block of the given block and index.
    async fn uncle_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<RichBlock>> {
        todo!()
    }

    /// Returns the EIP-2718 encoded transaction if it exists.
    ///
    /// If this is a EIP-4844 transaction that is in the pool it will include the sidecar.
    async fn raw_transaction_by_hash(&self, _hash: B256) -> RpcResult<Option<Bytes>> {
        todo!()
    }

    /// Returns the information about a transaction requested by transaction hash.
    async fn transaction_by_hash(&self, hash: B256) -> RpcResult<Option<Transaction>> {
        let tx = self.transactions.get_by_hash(hash).await.unwrap();
        Ok(tx)
    }

    /// Returns information about a raw transaction by block hash and transaction index position.
    async fn raw_transaction_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<Bytes>> {
        todo!()
    }

    /// Returns information about a transaction by block hash and transaction index position.
    async fn transaction_by_block_hash_and_index(
        &self,
        _hash: B256,
        _index: Index,
    ) -> RpcResult<Option<Transaction>> {
        todo!()
    }

    /// Returns information about a raw transaction by block number and transaction index
    /// position.
    async fn raw_transaction_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<Bytes>> {
        todo!()
    }

    /// Returns information about a transaction by block number and transaction index position.
    async fn transaction_by_block_number_and_index(
        &self,
        _number: BlockNumberOrTag,
        _index: Index,
    ) -> RpcResult<Option<Transaction>> {
        todo!()
    }

    /// Returns the receipt of a transaction by transaction hash.
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<AnyTransactionReceipt>> {
        let receipt = self.transactions.receipt_by_hash(hash).await.unwrap();
        Ok(receipt)
    }

    /// Returns the balance of the account of given address.
    async fn balance(&self, _address: Address, _block_number: Option<BlockId>) -> RpcResult<U256> {
        todo!()
    }

    /// Returns the value from a storage position at a given address
    async fn storage_at(
        &self,
        _address: Address,
        _index: JsonStorageKey,
        _block_number: Option<BlockId>,
    ) -> RpcResult<B256> {
        todo!()
    }

    /// Returns the number of transactions sent from an address at given block number.
    async fn transaction_count(
        &self,
        _address: Address,
        _block_number: Option<BlockId>,
    ) -> RpcResult<U256> {
        todo!()
    }

    /// Returns code at a given address at given block number.
    async fn get_code(
        &self,
        _address: Address,
        _block_number: Option<BlockId>,
    ) -> RpcResult<Bytes> {
        todo!()
    }

    /// Returns the block's header at given number.
    async fn header_by_number(&self, _hash: BlockNumberOrTag) -> RpcResult<Option<Header>> {
        todo!()
    }

    /// Returns the block's header at given hash.
    async fn header_by_hash(&self, _hash: B256) -> RpcResult<Option<Header>> {
        todo!()
    }

    /// Executes a new message call immediately without creating a transaction on the block chain.
    async fn call(
        &self,
        _request: TransactionRequest,
        _block_number: Option<BlockId>,
        _state_overrides: Option<StateOverride>,
        _block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<Bytes> {
        todo!()
    }

    /// Simulate arbitrary number of transactions at an arbitrary blockchain index, with the
    /// optionality of state overrides
    async fn call_many(
        &self,
        _bundle: Bundle,
        _state_context: Option<StateContext>,
        _state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<EthCallResponse>> {
        todo!()
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
    /// list is included. Like eth_estimateGas, this is an estimation{todo!()} the list could change
    /// when the transaction is actually mined. Adding an accessList to your transaction does
    /// not necessary result in lower gas usage compared to a transaction without an access
    /// list.
    async fn create_access_list(
        &self,
        _request: TransactionRequest,
        _block_number: Option<BlockId>,
    ) -> RpcResult<AccessListWithGasUsed> {
        todo!()
    }

    /// Generates and returns an estimate of how much gas is necessary to allow the transaction to
    /// complete.
    async fn estimate_gas(
        &self,
        _request: TransactionRequest,
        _block_number: Option<BlockId>,
        _state_override: Option<StateOverride>,
    ) -> RpcResult<U256> {
        todo!()
    }

    /// Returns the current price per gas in wei.
    async fn gas_price(&self) -> RpcResult<U256> {
        todo!()
    }

    /// Introduced in EIP-1559, returns suggestion for the priority for dynamic fee transactions.
    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256> {
        todo!()
    }

    /// Introduced in EIP-4844, returns the current blob base fee in wei.
    async fn blob_base_fee(&self) -> RpcResult<U256> {
        todo!()
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
        todo!()
    }

    /// Returns whether the client is actively mining new blocks.
    async fn is_mining(&self) -> RpcResult<bool> {
        todo!()
    }

    /// Returns the number of hashes per second that the node is mining with.
    async fn hashrate(&self) -> RpcResult<U256> {
        todo!()
    }

    /// Returns the hash of the current block, the seedHash, and the boundary condition to be met
    /// (“target”)
    async fn get_work(&self) -> RpcResult<Work> {
        todo!()
    }

    /// Used for submitting mining hashrate.
    ///
    /// Can be used for remote miners to submit their hash rate.
    /// It accepts the miner hash rate and an identifier which must be unique between nodes.
    /// Returns `true` if the block was successfully submitted, `false` otherwise.
    async fn submit_hashrate(&self, _hashrate: U256, _id: B256) -> RpcResult<bool> {
        todo!()
    }

    /// Used for submitting a proof-of-work solution.
    async fn submit_work(
        &self,
        _nonce: B64,
        _pow_hash: B256,
        _mix_digest: B256,
    ) -> RpcResult<bool> {
        todo!()
    }

    /// Sends transaction{todo!()} will block waiting for signer to return the
    /// transaction hash.
    async fn send_transaction(&self, _request: TransactionRequest) -> RpcResult<B256> {
        todo!()
    }

    /// Sends signed transaction, returning its hash.
    async fn send_raw_transaction(&self, _bytes: Bytes) -> RpcResult<B256> {
        todo!()
    }

    /// Returns an Ethereum specific signature with: sign(keccak256("\x19Ethereum Signed Message:\n"
    /// + len(message) + message))).
    async fn sign(&self, _address: Address, _message: Bytes) -> RpcResult<Bytes> {
        todo!()
    }

    /// Signs a transaction that can be submitted to the network at a later time using with
    /// `sendRawTransaction.`
    async fn sign_transaction(&self, _transaction: TransactionRequest) -> RpcResult<Bytes> {
        todo!()
    }

    /// Signs data via [EIP-712](https://github.com/ethereum/EIPs/blob/master/EIPS/eip-712.md).
    async fn sign_typed_data(
        &self,
        _address: Address,
        _data: serde_json::Value,
    ) -> RpcResult<Bytes> {
        todo!()
    }

    /// Returns the account and storage values of the specified account including the Merkle-proof.
    /// This call can be used to verify that the data you are pulling from is not tampered with.
    async fn get_proof(
        &self,
        _address: Address,
        _keys: Vec<JsonStorageKey>,
        _block_number: Option<BlockId>,
    ) -> RpcResult<EIP1186AccountProofResponse> {
        todo!()
    }
}
