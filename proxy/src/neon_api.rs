use std::sync::Arc;

use common::ethnum::U256;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::rpc::{CallDbClient, CloneRpcClient, RpcEnum};
use common::neon_lib::tracing::tracers::TracerTypeEnum;
use common::neon_lib::types::{BalanceAddress, ChDbConfig, EmulateRequest, TracerDb, TxParams};
use common::neon_lib::{commands, NeonError};
use common::solana_sdk::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tracing::{error, warn};

#[derive(Debug)]
struct Task {
    slot: Option<u64>,
    tx_index_in_block: Option<u64>,
    cmd: TaskCommand,
}

impl Task {
    pub fn new(cmd: TaskCommand, slot: Option<u64>, tx_index_in_block: Option<u64>) -> Self {
        Self {
            slot,
            tx_index_in_block,
            cmd,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum TaskCommand {
    GetBalance {
        addr: BalanceAddress,
        response: oneshot::Sender<Result<U256, NeonError>>,
    },
    GetTransactionCount {
        addr: BalanceAddress,
        response: oneshot::Sender<Result<u64, NeonError>>,
    },
    EmulateCall {
        tx: TxParams,
        response: oneshot::Sender<Result<Vec<u8>, NeonError>>,
    },
    EstimateGas {
        tx: TxParams,
        response: oneshot::Sender<Result<EmulateResponse, NeonError>>,
    },
}

struct Context {
    rpc: RpcEnum,
    neon_pubkey: Pubkey,
}

async fn build_rpc(
    rpc_client: &CloneRpcClient,
    tracer_db: Option<&TracerDb>,
    slot: Option<u64>,
    tx_index_in_block: Option<u64>,
) -> Result<RpcEnum, NeonError> {
    if let Some(slot) = slot {
        if let Some(tracer_db) = tracer_db {
            Ok(RpcEnum::CallDbClient(
                CallDbClient::new(tracer_db.clone(), slot, tx_index_in_block).await?,
            ))
        } else {
            Err(NeonError::InvalidChDbConfig)
        }
    } else {
        Ok(RpcEnum::CloneRpcClient(rpc_client.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct NeonApi {
    channel: Sender<Task>,
}

impl NeonApi {
    pub fn new(
        url: String,
        neon_pubkey: Pubkey,
        config_key: Pubkey,
        tracer_db_config: ChDbConfig,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Task>(128);

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            let local = LocalSet::new();

            local.spawn_local(async move {
                let client = RpcClient::new(url);
                let client = CloneRpcClient {
                    rpc: Arc::new(client),
                    max_retries: 10,
                    key_for_config: config_key,
                };
                let tracer_db = if tracer_db_config.clickhouse_url.is_empty() {
                    warn!("Clickhouse url is empty, tracer db will not be used");
                    None
                } else {
                    Some(TracerDb::new(&tracer_db_config))
                };
                while let Some(task) = rx.recv().await {
                    let rpc = match build_rpc(
                        &client,
                        tracer_db.as_ref(),
                        task.slot,
                        task.tx_index_in_block,
                    )
                    .await
                    {
                        Ok(rpc) => rpc,
                        Err(err) => {
                            error!("failed to build rpc: {err:?}");
                            continue;
                        }
                    };
                    let ctx = Context { rpc, neon_pubkey };
                    tokio::task::spawn_local(Self::execute(task.cmd, ctx));
                }
            });

            rt.block_on(local);
        });

        Self { channel: tx }
    }

    pub async fn get_balance(
        &self,
        addr: BalanceAddress,
        slot: Option<u64>,
    ) -> Result<U256, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetBalance { addr, response: tx },
                slot,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        slot: Option<u64>,
    ) -> Result<u64, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetTransactionCount { addr, response: tx },
                slot,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn call(&self, params: TxParams) -> Result<Vec<u8>, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::EmulateCall {
                    tx: params,
                    response: tx,
                },
                None,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn estimate_gas(&self, params: TxParams) -> Result<U256, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::EstimateGas {
                    tx: params,
                    response: tx,
                },
                None,
                None,
            ))
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        // TODO: do actual calculations
        Ok(U256::from(resp?.used_gas))
    }

    async fn execute(cmd: TaskCommand, ctx: Context) {
        match cmd {
            TaskCommand::EstimateGas { tx, response } => {
                let config = commands::get_config::execute(&ctx.rpc, ctx.neon_pubkey)
                    .await
                    .expect("config didnt fail"); // TODO

                let req = EmulateRequest {
                    step_limit: None,
                    chains: Some(config.chains),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                let resp = commands::emulate::execute(
                    &ctx.rpc,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                let resp = match resp {
                    Ok((resp, _something)) => Ok(resp),
                    Err(err) => Err(err),
                };
                let _ = response.send(resp);
            }

            TaskCommand::EmulateCall { tx, response } => {
                tracing::info!(?tx, "emulate_call");
                let config = commands::get_config::execute(&ctx.rpc, ctx.neon_pubkey)
                    .await
                    .expect("config didnt fail"); // TODO

                let req = EmulateRequest {
                    step_limit: None,
                    chains: Some(config.chains),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                let resp = commands::emulate::execute(
                    &ctx.rpc,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                tracing::info!(?resp, "emulate_call");
                let resp = match resp {
                    Ok((resp, _something)) => Ok(resp.result),
                    Err(err) => Err(err),
                };
                let _ = response.send(resp);
            }

            TaskCommand::GetBalance { addr, response } => {
                let resp =
                    commands::get_balance::execute(&ctx.rpc, &ctx.neon_pubkey, &[addr]).await;
                tracing::info!(?resp, "get_balance");
                let resp = match resp {
                    Ok(resp) => {
                        let mut balance = common::ethnum::U256::default();
                        for resp in resp {
                            balance = resp.balance;
                        }
                        Ok(balance)
                    }
                    Err(err) => Err(err),
                };
                let _ = response.send(resp);
            }

            TaskCommand::GetTransactionCount { addr, response } => {
                let resp =
                    commands::get_balance::execute(&ctx.rpc, &ctx.neon_pubkey, &[addr]).await;
                tracing::info!(?resp, "get_transaction_count");
                let resp = match resp {
                    Ok(resp) => {
                        let mut count = 0;
                        for resp in resp {
                            count = resp.trx_count;
                        }
                        Ok(count)
                    }
                    Err(err) => Err(err),
                };
                let _ = response.send(resp);
            }
        }
    }
}
