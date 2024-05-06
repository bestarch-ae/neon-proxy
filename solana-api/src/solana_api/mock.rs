use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use solana_client::client_error::ClientError;
use solana_client::rpc_request::RpcRequest;
use solana_client::rpc_sender::{RpcSender, RpcTransportStats};

type RequestResult = Result<Value, ClientError>;

#[derive(Default)]
pub struct SharedMock {
    pub once: DashMap<(RpcRequest, String), RequestResult>,
    // ClientError does not implement Clone, so we use this map only for positive requests.
    pub always: DashMap<(RpcRequest, String), Value>,
}

pub struct MockSender {
    pub(super) shared: Arc<SharedMock>,
}

impl MockSender {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let this = Self {
            shared: Arc::default(),
        };

        let version: Value =
            serde_json::from_str(r#"{"feature-set":4192065167,"solana-core":"1.10.31"}"#).unwrap();
        this.shared
            .always
            .insert((RpcRequest::GetVersion, "null".to_string()), version);
        this
    }
}

#[async_trait]
impl RpcSender for MockSender {
    async fn send(&self, request: RpcRequest, params: Value) -> RequestResult {
        tracing::info!(?request, ?params, "outgoing request");

        let params_str = serde_json::to_string(&params).expect("could not serialize params");

        let key = &(request, params_str);
        let response = match (
            self.shared.once.contains_key(key),
            self.shared.always.contains_key(key),
        ) {
            (true, true) => panic!("request in both maps: {request} {params}"),
            (false, false) => panic!("no request: {request} {params}"),
            (true, false) => self.shared.once.remove(key).unwrap().1,
            (false, true) => Ok(self.shared.always.get(key).unwrap().clone()),
        };

        response
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }

    fn url(&self) -> String {
        String::new()
    }
}
