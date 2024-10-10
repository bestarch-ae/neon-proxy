use jsonrpsee::core::{async_trait, RpcResult};
use reth_primitives::{Bytes, B256};
use rpc_api::Web3ApiServer;

use crate::rpc::EthApiImpl;

#[async_trait]
impl Web3ApiServer for EthApiImpl {
    async fn client_version(&self) -> RpcResult<String> {
        self.neon_evm_version().await.map_err(Into::into)
    }

    fn sha3(&self, data: Bytes) -> RpcResult<B256> {
        use common::solana_sdk::keccak::{hash, Hash};
        let Hash(hash) = hash(&data);
        Ok(B256::from(hash))
    }
}
