mod gas_limit_calculator;

use std::borrow::Borrow;
use std::str::FromStr;
use std::sync::Arc;

use jsonrpsee::types::ErrorCode;
use reth_primitives::{BlockNumberOrTag, Bytes};
use serde::Deserialize;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tracing::{error, warn};

use common::ethnum::U256;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::commands::get_config::GetConfigResponse;
use common::neon_lib::commands::simulate_solana::{
    SimulateSolanaResponse, SimulateSolanaTransactionResult,
};
use common::neon_lib::rpc::{CallDbClient, CloneRpcClient, RpcEnum};
use common::neon_lib::tracing::tracers::TracerTypeEnum;
use common::neon_lib::types::{
    BalanceAddress, ChDbConfig, EmulateRequest, SimulateSolanaRequest, TracerDb, TxParams,
};
use common::neon_lib::{commands, NeonError};
use common::solana_sdk::commitment_config::CommitmentConfig;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::transaction::Transaction;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;

use self::gas_limit_calculator::{GasLimitCalculator, GasLimitError};

#[derive(Copy, Clone, Debug)]
enum ExitStatus {
    Success,
    Revert,
    StepLimitExceeded,
}

impl FromStr for ExitStatus {
    type Err = NeonApiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "succeed" => Ok(Self::Success),
            "revert" => Ok(Self::Revert),
            "step limit exceeded" => Ok(Self::StepLimitExceeded),
            _ => Err(NeonApiError::InvalidExitStatus(s.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum NeonApiError {
    #[error("neon error: {0}")]
    Neon(#[from] NeonError),
    #[error("gas limit error: {0}")]
    GasLimit(#[from] GasLimitError),
    #[error("transaction serialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("invalid exit status: {0}")]
    InvalidExitStatus(String),
    #[error("emulation failed: {0}")]
    EmulationFailed(String, Option<Bytes>),
}

impl NeonApiError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Neon(err) => err.error_code() as i32,
            Self::GasLimit(_) | Self::EmulationFailed(_, _) => 3,
            _ => ErrorCode::InternalError.code(),
        }
    }
}

impl From<NeonApiError> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: NeonApiError) -> Self {
        let error_code = value.error_code();
        match value {
            NeonApiError::Neon(err) => Self::owned(
                ErrorCode::InternalError.code(),
                err.to_string(),
                None::<String>,
            ),
            NeonApiError::GasLimit(err) => Self::owned(error_code, err.to_string(), None::<String>),
            NeonApiError::EmulationFailed(msg, data) => Self::owned(error_code, msg, data),
            _ => ErrorCode::InternalError.into(),
        }
    }
}

#[derive(Debug)]
struct Task {
    tag: Option<BlockNumberOrTag>,
    tx_index_in_block: Option<u64>,
    cmd: TaskCommand,
}

impl Task {
    pub fn new(
        cmd: TaskCommand,
        tag: Option<BlockNumberOrTag>,
        tx_index_in_block: Option<u64>,
    ) -> Self {
        Self {
            tag,
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
        response: oneshot::Sender<Result<EmulateResponse, NeonApiError>>,
    },
    EstimateGas {
        tx: TxParams,
        response: oneshot::Sender<(GetConfigResponse, Result<EmulateResponse, NeonApiError>)>,
    },
    Emulate {
        tx: TxParams,
        response: oneshot::Sender<Result<EmulateResponse, NeonError>>,
    },
    GetConfig(oneshot::Sender<Result<GetConfigResponse, NeonError>>),
    Simulate {
        request: SimulateSolanaRequest,
        response: oneshot::Sender<Result<SimulateSolanaResponse, NeonError>>,
    },
}

struct Context {
    rpc_client_finalized: CloneRpcClient,
    rpc_client_simulation: CloneRpcClient,
    default_rpc: RpcEnum,
    neon_pubkey: Pubkey,
}

async fn build_rpc(
    mut client_builder: impl FnMut(CommitmentConfig) -> RpcClient,
    tracer_db: Option<&TracerDb>,
    tag: Option<BlockNumberOrTag>,
    tx_index_in_block: Option<u64>,
    config_key: Pubkey,
) -> Result<RpcEnum, NeonError> {
    match tag {
        Some(BlockNumberOrTag::Number(slot)) => {
            if let Some(tracer_db) = tracer_db {
                Ok(RpcEnum::CallDbClient(
                    CallDbClient::new(tracer_db.clone(), slot, tx_index_in_block).await?,
                ))
            } else {
                warn!("tracer_db is not configured, falling back to RpcClient");
                Ok(RpcEnum::CloneRpcClient(CloneRpcClient {
                    rpc: Arc::new(client_builder(CommitmentConfig::finalized())),
                    max_retries: 10,
                    key_for_config: config_key,
                }))
            }
        }
        Some(BlockNumberOrTag::Pending | BlockNumberOrTag::Latest) => {
            Ok(RpcEnum::CloneRpcClient(CloneRpcClient {
                rpc: Arc::new(client_builder(CommitmentConfig::processed())),
                max_retries: 10,
                key_for_config: config_key,
            }))
        }
        Some(BlockNumberOrTag::Safe) => Ok(RpcEnum::CloneRpcClient(CloneRpcClient {
            rpc: Arc::new(client_builder(CommitmentConfig::confirmed())),
            max_retries: 10,
            key_for_config: config_key,
        })),
        // Earliest should resolved into a slot number using db by that moment,
        // if it wasn't done, just ignoring it
        None | Some(BlockNumberOrTag::Finalized | BlockNumberOrTag::Earliest) => {
            Ok(RpcEnum::CloneRpcClient(CloneRpcClient {
                rpc: Arc::new(client_builder(CommitmentConfig::finalized())),
                max_retries: 10,
                key_for_config: config_key,
            }))
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SimulateConfig {
    pub compute_units: Option<u64>,
    pub heap_size: Option<u32>,
    pub account_limit: Option<usize>,
    pub verify: bool,
    pub blockhash: Hash,
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
        simulation_commitment: Option<CommitmentConfig>,
    ) -> Self {
        let url_cloned = url.clone();
        Self::new_with_custom_rpc_clients(
            move |commitment| RpcClient::new_with_commitment(url_cloned.clone(), commitment),
            neon_pubkey,
            config_key,
            tracer_db_config,
            max_tx_account_cnt,
            simulation_commitment,
        )
    }

    pub fn new_with_custom_rpc_clients(
        mut client_builder: impl FnMut(CommitmentConfig) -> RpcClient + Send + 'static,
        neon_pubkey: Pubkey,
        config_key: Pubkey,
        tracer_db_config: ChDbConfig,
        max_tx_account_cnt: usize,
        simulation_commitment: Option<CommitmentConfig>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Task>(128);

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            let local = LocalSet::new();

            local.spawn_local(async move {
                let tracer_db = if tracer_db_config.clickhouse_url.is_empty() {
                    warn!("Clickhouse url is empty, tracer db will not be used");
                    None
                } else {
                    Some(TracerDb::new(&tracer_db_config))
                };

                let simulation_commitment =
                    simulation_commitment.unwrap_or(CommitmentConfig::finalized());

                let finalized_client = CloneRpcClient {
                    rpc: Arc::new(client_builder(CommitmentConfig::finalized())),
                    max_retries: 10,
                    key_for_config: config_key,
                };
                let simulation_client = CloneRpcClient {
                    rpc: Arc::new(client_builder(simulation_commitment)),
                    max_retries: 10,
                    key_for_config: config_key,
                };
                while let Some(task) = rx.recv().await {
                    let rpc = match build_rpc(
                        &mut client_builder,
                        tracer_db.as_ref(),
                        task.tag,
                        task.tx_index_in_block,
                        config_key,
                    )
                    .await
                    {
                        Ok(rpc) => rpc,
                        Err(err) => {
                            error!("failed to build rpc: {err:?}");
                            continue;
                        }
                    };
                    let ctx = Context {
                        rpc_client_finalized: finalized_client.clone(),
                        rpc_client_simulation: simulation_client.clone(),
                        default_rpc: rpc,
                        neon_pubkey,
                    };
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
        tag: Option<BlockNumberOrTag>,
    ) -> Result<U256, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetBalance { addr, response: tx },
                tag,
                None,
            ))
            .await
            .unwrap();
        Ok(rx.await.unwrap()?)
    }

    pub async fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<u64, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetTransactionCount { addr, response: tx },
                tag,
                None,
            ))
            .await
            .unwrap();
        Ok(rx.await.unwrap()?)
    }

    pub async fn call(
        &self,
        params: TxParams,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<EmulateResponse, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::EmulateCall {
                    tx: params,
                    response: tx,
                },
                tag,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn estimate_gas(
        &self,
        params: TxParams,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<U256, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::EstimateGas {
                    tx: params.clone(),
                    response: tx,
                },
                tag,
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
        if let Err(err) = &resp {
            error!(?err, "estimate gas emulation failed");
        }
        let resp = resp?;
        let total_gas = self
            .gas_limit_calculator
            .estimate(&params, &resp, holder_msg_size)?;
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

    pub async fn simulate(
        &self,
        config: SimulateConfig,
        txs: &[impl Borrow<Transaction>],
    ) -> Result<Vec<SimulateSolanaTransactionResult>, NeonApiError> {
        let transactions = txs
            .iter()
            .map(|tx| bincode::serialize(tx.borrow()))
            .collect::<Result<_, _>>()?;
        let (tx, rx) = oneshot::channel();
        let request = SimulateSolanaRequest {
            compute_units: config.compute_units,
            heap_size: config.heap_size,
            account_limit: config.account_limit,
            verify: Some(config.verify),
            blockhash: config.blockhash.to_bytes(),
            transactions,
        };
        self.channel
            .send(Task::new(
                TaskCommand::Simulate {
                    request,
                    response: tx,
                },
                None,
                None,
            ))
            .await
            .unwrap();
        let result = rx.await.unwrap()?;

        // HACK: `transactions` is private in neonlib
        #[derive(Deserialize, Debug, Default)]
        struct SimulateSolanaResponseLocal {
            transactions: Vec<SimulateSolanaTransactionResult>,
        }

        let value = serde_json::to_value(result).map_err(NeonError::from)?;
        let result: SimulateSolanaResponseLocal =
            serde_json::from_value(value).map_err(NeonError::from)?;

        Ok(result.transactions)
    }

    async fn execute(task: TaskCommand, ctx: Context) {
        match task {
            TaskCommand::EstimateGas { tx, response } => {
                let config =
                    commands::get_config::execute(&ctx.rpc_client_finalized, ctx.neon_pubkey)
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
                if let RpcEnum::CloneRpcClient(rpc) = &ctx.default_rpc {
                    tracing::debug!(commitment = ?rpc.commitment(), "estimate gas task command, emulate commitment");
                }
                let resp = commands::emulate::execute(
                    &ctx.default_rpc,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                let resp = match resp {
                    Ok((resp, _something)) => decode_neon_response(resp),
                    Err(err) => Err(err.into()),
                };
                let _ = response.send((config, resp));
            }

            TaskCommand::Emulate { tx, response } => {
                let config =
                    commands::get_config::execute(&ctx.rpc_client_finalized, ctx.neon_pubkey)
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
                let config =
                    commands::get_config::execute(&ctx.rpc_client_finalized, ctx.neon_pubkey)
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
                tracing::info!(?req, "emulate_call");
                let resp = commands::emulate::execute(
                    &ctx.default_rpc,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                tracing::info!(?resp, "emulate_call");
                let resp = match resp {
                    Ok((resp, _something)) => decode_neon_response(resp),
                    Err(err) => Err(err.into()),
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
                let resp =
                    commands::get_config::execute(&ctx.rpc_client_finalized, ctx.neon_pubkey).await;
                let _ = response.send(resp);
            }

            TaskCommand::Simulate { request, response } => {
                let resp =
                    commands::simulate_solana::execute(&ctx.rpc_client_simulation, request).await;
                let _ = response.send(resp);
            }
        }
    }
}

fn decode_neon_response(response: EmulateResponse) -> Result<EmulateResponse, NeonApiError> {
    use common::evm_loader::error;
    let status = ExitStatus::from_str(&response.exit_status)?;

    match status {
        ExitStatus::Success => Ok(response),
        ExitStatus::StepLimitExceeded => Err(NeonApiError::EmulationFailed(
            "step limit exceeded".to_owned(),
            None,
        )),
        ExitStatus::Revert => {
            let msg = if let Some(error) = error::format_revert_error(&response.result) {
                format!("execution reverted: {}", error)
            } else {
                "execution reverted".to_string()
            };
            Err(NeonApiError::EmulationFailed(
                msg,
                Some(Bytes::from(response.result)),
            ))
        }
    }
}
