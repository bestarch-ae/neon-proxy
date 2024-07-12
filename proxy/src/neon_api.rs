mod gas_limit_calculator;

use std::sync::Arc;

use jsonrpsee::types::ErrorCode;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tracing::{error, warn};

use common::ethnum::U256;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::commands::get_config::GetConfigResponse;
use common::neon_lib::rpc::{CallDbClient, CloneRpcClient, RpcEnum};
use common::neon_lib::tracing::tracers::TracerTypeEnum;
use common::neon_lib::types::{BalanceAddress, ChDbConfig, EmulateRequest, TracerDb, TxParams};
use common::neon_lib::{commands, NeonError};
use common::solana_sdk::pubkey::Pubkey;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;

use self::gas_limit_calculator::{GasLimitCalculator, GasLimitError};

#[derive(Debug, Error)]
pub enum NeonApiError {
    #[error("neon error: {0}")]
    NeonError(#[from] NeonError),
    #[error("gas limit error: {0}")]
    GasLimitError(#[from] GasLimitError),
}

impl NeonApiError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::NeonError(err) => err.error_code() as i32,
            Self::GasLimitError(_) => 3,
        }
    }
}

impl From<NeonApiError> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: NeonApiError) -> Self {
        let error_code = value.error_code();
        match value {
            NeonApiError::NeonError(err) => Self::owned(
                ErrorCode::InternalError.code(),
                err.to_string(),
                None::<String>,
            ),
            NeonApiError::GasLimitError(err) => {
                Self::owned(error_code, err.to_string(), None::<String>)
            }
        }
    }
}

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
        response: oneshot::Sender<(GetConfigResponse, Result<EmulateResponse, NeonError>)>,
    },
    Emulate {
        tx: TxParams,
        response: oneshot::Sender<Result<EmulateResponse, NeonError>>,
    },
    GetConfig(oneshot::Sender<Result<GetConfigResponse, NeonError>>),
}

struct Context {
    rpc_client: CloneRpcClient,
    default_rpc: RpcEnum,
    neon_pubkey: Pubkey,
}

impl Context {
    pub async fn new(
        rpc_client: CloneRpcClient,
        tracer_db: Option<&TracerDb>,
        slot: Option<u64>,
        tx_index_in_block: Option<u64>,
        neon_pubkey: Pubkey,
    ) -> Result<Self, NeonError> {
        let default_rpc = if let Some(slot) = slot {
            if let Some(tracer_db) = tracer_db {
                RpcEnum::CallDbClient(
                    CallDbClient::new(tracer_db.clone(), slot, tx_index_in_block).await?,
                )
            } else {
                warn!("tracer_db is not configured, falling back to RpcClient");
                RpcEnum::CloneRpcClient(rpc_client.clone())
            }
        } else {
            RpcEnum::CloneRpcClient(rpc_client.clone())
        };

        Ok(Self {
            rpc_client,
            default_rpc,
            neon_pubkey,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NeonApi {
    channel: Sender<Task>,
    gas_limit_calculator: GasLimitCalculator,
}

impl NeonApi {
    pub fn new(
        url: String,
        neon_pubkey: Pubkey,
        config_key: Pubkey,
        tracer_db_config: ChDbConfig,
        max_tx_account_cnt: usize,
    ) -> Self {
        let client = RpcClient::new(url);
        Self::new_with_custom_rpc_client(
            client,
            neon_pubkey,
            config_key,
            tracer_db_config,
            max_tx_account_cnt,
        )
    }

    pub fn new_with_custom_rpc_client(
        client: RpcClient,
        neon_pubkey: Pubkey,
        config_key: Pubkey,
        tracer_db_config: ChDbConfig,
        max_tx_account_cnt: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Task>(128);

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            let local = LocalSet::new();

            local.spawn_local(async move {
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
                    let ctx = Context::new(
                        client.clone(),
                        tracer_db.as_ref(),
                        task.slot,
                        task.tx_index_in_block,
                        neon_pubkey,
                    )
                    .await
                    .expect("context creation failed");
                    tokio::task::spawn_local(Self::execute(task.cmd, ctx));
                }
            });

            rt.block_on(local);
        });

        Self {
            channel: tx,
            gas_limit_calculator: GasLimitCalculator::new(max_tx_account_cnt),
        }
    }

    pub async fn get_balance(
        &self,
        addr: BalanceAddress,
        slot: Option<u64>,
    ) -> Result<U256, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetBalance { addr, response: tx },
                slot,
                None,
            ))
            .await
            .unwrap();
        Ok(rx.await.unwrap()?)
    }

    pub async fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        slot: Option<u64>,
    ) -> Result<u64, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetTransactionCount { addr, response: tx },
                slot,
                None,
            ))
            .await
            .unwrap();
        Ok(rx.await.unwrap()?)
    }

    pub async fn call(&self, params: TxParams) -> Result<Vec<u8>, NeonApiError> {
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
        Ok(rx.await.unwrap()?)
    }

    pub async fn estimate_gas(
        &self,
        params: TxParams,
        slot: Option<u64>,
    ) -> Result<U256, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::EstimateGas {
                    tx: params.clone(),
                    response: tx,
                },
                slot,
                None,
            ))
            .await
            .unwrap();
        let (evm_config, resp) = rx.await.unwrap();
        let holder_msg_size = evm_config
            .config
            .get("NEON_HOLDER_MSG_SIZE")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let emul_resp = resp?;
        let total_gas = self
            .gas_limit_calculator
            .estimate(&params, &emul_resp, holder_msg_size)?;
        Ok(U256::from(total_gas))
    }

    pub async fn emulate(&self, params: TxParams) -> Result<EmulateResponse, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::Emulate {
                    tx: params,
                    response: tx,
                },
                None,
                None,
            ))
            .await
            .unwrap();
        Ok(rx.await.unwrap()?)
    }

    pub async fn get_config(&self) -> Result<GetConfigResponse, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(TaskCommand::GetConfig(tx), None, None))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    async fn execute(task: TaskCommand, ctx: Context) {
        match task {
            TaskCommand::EstimateGas { tx, response } => {
                let config = commands::get_config::execute(&ctx.rpc_client, ctx.neon_pubkey)
                    .await
                    .expect("config didnt fail"); // TODO

                let req = EmulateRequest {
                    step_limit: None,
                    chains: Some(config.chains.clone()),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                let resp = commands::emulate::execute(
                    &ctx.default_rpc,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                let resp = match resp {
                    Ok((resp, _something)) => Ok(resp),
                    Err(err) => Err(err),
                };
                let _ = response.send((config, resp));
            }

            TaskCommand::Emulate { tx, response } => {
                let config = commands::get_config::execute(&ctx.rpc_client, ctx.neon_pubkey)
                    .await
                    .expect("config didnt fail"); // TODO

                let req = EmulateRequest {
                    step_limit: None,
                    chains: Some(config.chains.clone()),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                let resp = commands::emulate::execute(
                    &ctx.default_rpc,
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
                let config = commands::get_config::execute(&ctx.rpc_client, ctx.neon_pubkey)
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
                    &ctx.default_rpc,
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
                    commands::get_balance::execute(&ctx.default_rpc, &ctx.neon_pubkey, &[addr])
                        .await;
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
                    commands::get_balance::execute(&ctx.default_rpc, &ctx.neon_pubkey, &[addr])
                        .await;
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

            TaskCommand::GetConfig(response) => {
                let resp = commands::get_config::execute(&ctx.rpc_client, ctx.neon_pubkey).await;
                let _ = response.send(resp);
            }
        }
    }
}
