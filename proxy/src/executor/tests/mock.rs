use base64::prelude::{Engine, BASE64_STANDARD as BASE64};
use jsonrpsee::core::async_trait;
use serde_json::Value;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::client_error::Result as ClientResult;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig,
};
use solana_client::rpc_request::RpcRequest;
use solana_client::rpc_response::{Response, RpcResponseContext, RpcSimulateTransactionResult};
use solana_client::rpc_sender::{RpcSender, RpcTransportStats};
use solana_program_test::BanksClient;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::transaction::VersionedTransaction;
use tarpc::context;

use common::neon_lib::rpc::Rpc;
use common::solana_sdk::account::Account;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_transaction_status::{
    TransactionConfirmationStatus, TransactionStatus, UiTransactionEncoding,
};

pub struct BanksRpcMock(pub BanksClient);

impl BanksRpcMock {
    async fn with_context<T>(&self, commitment: CommitmentLevel, value: T) -> Response<T> {
        let mut rpc = self.0.clone();
        let slot = rpc
            .get_slot_with_context(context::current(), commitment)
            .await
            .unwrap();
        Response {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value,
        }
    }
}

#[async_trait(?Send)]
impl Rpc for BanksRpcMock {
    async fn get_account(&self, key: &Pubkey) -> ClientResult<Option<Account>> {
        let mut client = self.0.clone();
        Ok(client.get_account(*key).await.unwrap())
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> ClientResult<Vec<Option<Account>>> {
        let mut res = Vec::new();
        let mut client = self.0.clone();
        for key in pubkeys {
            res.push(client.get_account(*key).await.unwrap())
        }
        Ok(res)
    }

    async fn get_block_time(&self, _: u64) -> ClientResult<i64> {
        Ok(0)
    }

    async fn get_slot(&self) -> ClientResult<u64> {
        let mut client = self.0.clone();
        Ok(client
            .get_slot_with_context(context::current(), Default::default())
            .await
            .unwrap())
    }

    async fn get_deactivated_solana_features(&self) -> ClientResult<Vec<Pubkey>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl RpcSender for BanksRpcMock {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        tracing::info!(?request, ?params, "outgoing request");
        // eprintln!("outgoing request: {request:?} {params:?}");

        let mut rpc = self.0.clone();
        let deserialize_transaction = |tx, encoding| {
            let serialized = match encoding {
                Some(UiTransactionEncoding::Base64) | None => BASE64.decode(tx).unwrap(),
                Some(UiTransactionEncoding::Base58) => bs58::decode(tx).into_vec().unwrap(),
                encoding => panic!("unsupported tx encoding: {encoding:?}"),
            };
            let transaction: VersionedTransaction = bincode::deserialize(&serialized).unwrap();
            transaction
        };
        let get_account = |mut rpc: BanksClient, key: &str, config: &RpcAccountInfoConfig| {
            let key: Pubkey = key.parse().unwrap();
            let commitment = config.commitment.unwrap_or_default().commitment;
            let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);

            async move {
                let account = rpc
                    .get_account_with_commitment(key, commitment)
                    .await
                    .unwrap();
                account.map(|acc| UiAccount::encode(&key, &acc, encoding, None, None))
            }
        };
        let get_commitment = |params: &Value| {
            params
                .get("commitment")
                .cloned()
                .map_or(Ok(CommitmentLevel::default()), serde_json::from_value)
        };

        let response: Value = match request {
            RpcRequest::GetVersion => {
                serde_json::from_str(r#"{"feature-set":4192065167,"solana-core":"1.10.31"}"#)
                    .unwrap()
            }
            RpcRequest::GetSlot => {
                let commitment = get_commitment(&params)?;
                let slot = rpc
                    .get_slot_with_context(context::current(), commitment)
                    .await
                    .unwrap();
                serde_json::to_value(slot)?
            }
            RpcRequest::GetBlockTime => serde_json::to_value(0)?,
            RpcRequest::GetAccountInfo => {
                let (key, config): (String, RpcAccountInfoConfig) = serde_json::from_value(params)?;
                let commitment = config.commitment.unwrap_or_default().commitment;
                let ui_account = get_account(rpc, &key, &config).await;
                serde_json::to_value(self.with_context(commitment, ui_account).await)?
            }
            RpcRequest::GetMultipleAccounts => {
                let (keys, config): (Vec<String>, RpcAccountInfoConfig) =
                    serde_json::from_value(params)?;
                let commitment = config.commitment.unwrap_or_default().commitment;
                let mut accounts = Vec::new();
                for key in keys {
                    let ui_account = get_account(rpc.clone(), &key, &config).await;
                    accounts.push(ui_account);
                }
                serde_json::to_value(self.with_context(commitment, accounts).await)?
            }
            RpcRequest::GetLatestBlockhash => {
                let commitment = get_commitment(&params)?;
                let (hash, block) = rpc
                    .get_latest_blockhash_with_commitment(commitment)
                    .await
                    .unwrap()
                    .unwrap();
                let hash = hash.to_string();
                serde_json::to_value(
                    self.with_context(
                        commitment,
                        serde_json::json!({
                            "blockhash": hash,
                            "lastValidBlockHeight": block,
                        }),
                    )
                    .await,
                )?
            }
            RpcRequest::IsBlockhashValid => {
                let commitment = get_commitment(&params)?;
                serde_json::to_value(self.with_context(commitment, true).await)?
            }
            RpcRequest::GetSignatureStatuses => {
                // let (signatures, _params): (Vec<String>, Option<Value>) =
                //     serde_json::from_value(params)?;
                let [signatures]: [Vec<String>; 1] = serde_json::from_value(params)?;
                let res = rpc
                    .get_transaction_statuses(
                        signatures.into_iter().map(|s| s.parse().unwrap()).collect(),
                    )
                    .await
                    .unwrap();
                let result: Vec<_> = res
                    .into_iter()
                    .map(|status| {
                        status.map(|status| TransactionStatus {
                            slot: status.slot,
                            confirmations: status.confirmations,
                            status: if let Some(err) = status.err.clone() {
                                Err(err)
                            } else {
                                Ok(())
                            },
                            err: status.err,
                            confirmation_status: status.confirmation_status.map(|comm| {
                                match format!("{comm:?}").as_str() {
                                    "Processed" => TransactionConfirmationStatus::Processed,
                                    "Confirmed" => TransactionConfirmationStatus::Confirmed,
                                    "Finalized" => TransactionConfirmationStatus::Finalized,
                                    other => unreachable!("{other}"),
                                }
                            }),
                        })
                    })
                    .collect();
                let result = self.with_context(Default::default(), result).await;
                serde_json::to_value(result)?
            }
            RpcRequest::SendTransaction => {
                let (serialized, config): (String, RpcSendTransactionConfig) =
                    serde_json::from_value(params)?;
                let transaction = deserialize_transaction(serialized, config.encoding);
                let sign = *transaction.signatures.first().expect("missing signature");
                rpc.send_transaction(transaction).await.unwrap();

                serde_json::to_value(sign.to_string())?
            }
            RpcRequest::SimulateTransaction => {
                let (transaction, config): (String, RpcSimulateTransactionConfig) =
                    serde_json::from_value(params)?;
                let commitment = config.commitment.unwrap_or_default().commitment;

                let mut tx = deserialize_transaction(transaction, config.encoding);
                let hash = rpc.get_latest_blockhash().await.unwrap();
                tx.message.set_recent_blockhash(hash);

                let res = rpc
                    .simulate_transaction_with_commitment(tx, commitment)
                    .await
                    .unwrap();
                let (logs, other) = res
                    .simulation_details
                    .map(|details| (details.logs, (details.units_consumed, details.return_data)))
                    .unzip();
                let (units_consumed, return_data) = other.unzip();

                let result = RpcSimulateTransactionResult {
                    err: res.result.and_then(Result::err),
                    logs,
                    accounts: None,
                    units_consumed,
                    return_data: return_data.flatten().map(Into::into),
                };
                serde_json::to_value(self.with_context(commitment, result).await)?
            }
            request => panic!("unsupported request: {request}"),
        };

        Ok(response)
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }

    fn url(&self) -> String {
        String::new()
    }
}
