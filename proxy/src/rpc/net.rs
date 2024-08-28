use jsonrpsee::core::{async_trait, RpcResult};
use reth_primitives::U64;
use rpc_api::NetApiServer;
use rpc_api_types::PeerCount;

use crate::rpc::EthApiImpl;

#[async_trait]
impl NetApiServer for EthApiImpl {
    fn version(&self) -> RpcResult<String> {
        Ok(self.chain_id.to_string())
    }

    fn peer_count(&self) -> RpcResult<PeerCount> {
        let neon_api = self.neon_api.clone();
        let peer_count = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { neon_api.get_cluster_size().await })
        })? as u64;

        Ok(PeerCount::Hex(U64::from(peer_count)))
    }

    fn is_listening(&self) -> RpcResult<bool> {
        Ok(false)
    }
}
