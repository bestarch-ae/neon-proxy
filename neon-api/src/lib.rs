mod gas_limit_calculator;

use std::borrow::Borrow;
use std::str::FromStr;
use std::sync::Arc;

use common::neon_lib::commands::get_balance::GetBalanceResponse;
use reth_primitives::{BlockNumberOrTag, Bytes};
use serde::Deserialize;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tracing::{error, warn};

use common::ethnum::U256;
use common::evm_loader::types::Address;
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
use solana_api::solana_rpc_client_api::response::RpcVersionInfo;

use self::gas_limit_calculator::{GasLimitCalculator, GasLimitError};

const DEFAULT_MAX_EMULATE_EVM_STEP_COUNT: u64 = 500_000;

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

#[cfg(feature = "jsonrpsee")]
impl NeonApiError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Neon(err) => err.error_code() as i32,
            Self::GasLimit(_) | Self::EmulationFailed(_, _) => 3,
            _ => jsonrpsee::types::ErrorCode::InternalError.code(),
        }
    }
}

#[cfg(feature = "jsonrpsee")]
impl From<NeonApiError> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: NeonApiError) -> Self {
        let error_code = value.error_code();
        match value {
            NeonApiError::Neon(err) => Self::owned(
                jsonrpsee::types::ErrorCode::InternalError.code(),
                err.to_string(),
                None::<String>,
            ),
            NeonApiError::GasLimit(err) => Self::owned(error_code, err.to_string(), None::<String>),
            NeonApiError::EmulationFailed(msg, data) => Self::owned(error_code, msg, data),
            _ => jsonrpsee::types::ErrorCode::InternalError.into(),
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
        response: oneshot::Sender<Result<EmulateResponse, NeonApiError>>,
    },
    GetConfig(oneshot::Sender<Result<GetConfigResponse, NeonError>>),
    Simulate {
        request: SimulateSolanaRequest,
        response: oneshot::Sender<Result<SimulateSolanaResponse, NeonError>>,
    },
    GetSolanaVersion(oneshot::Sender<Result<RpcVersionInfo, NeonApiError>>),
    GetClusterSize(oneshot::Sender<Result<usize, NeonApiError>>),
    GetHealth(oneshot::Sender<Result<(), NeonApiError>>),
    GetStorageAt {
        response: oneshot::Sender<Result<[u8; 32], NeonApiError>>,
        address: Address,
        index: U256,
    },
    GetCode {
        response: oneshot::Sender<Result<Option<Bytes>, NeonApiError>>,
        address: Address,
    },
    GetNeonAccount {
        response: oneshot::Sender<Result<Vec<GetBalanceResponse>, NeonApiError>>,
        address: BalanceAddress,
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
    let mut with_commitment = |commitment| {
        tracing::debug!(?commitment, "creating client with commitment");
        Ok(RpcEnum::CloneRpcClient(CloneRpcClient {
            rpc: Arc::new(client_builder(commitment)),
            max_retries: 10,
            key_for_config: config_key,
        }))
    };
    match tag {
        Some(BlockNumberOrTag::Number(slot)) => {
            if let Some(tracer_db) = tracer_db {
                Ok(RpcEnum::CallDbClient(
                    CallDbClient::new(tracer_db.clone(), slot, tx_index_in_block).await?,
                ))
            } else {
                warn!("tracer_db is not configured, falling back to RpcClient");
                with_commitment(CommitmentConfig::finalized())
            }
        }
        Some(BlockNumberOrTag::Pending | BlockNumberOrTag::Latest) => {
            with_commitment(CommitmentConfig::processed())
        }
        Some(BlockNumberOrTag::Safe) => with_commitment(CommitmentConfig::confirmed()),
        // Earliest should resolved into a slot number using db by that moment,
        // if it wasn't done, just ignoring it
        None | Some(BlockNumberOrTag::Finalized | BlockNumberOrTag::Earliest) => {
            with_commitment(CommitmentConfig::finalized())
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
    pubkey: Pubkey,
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
            pubkey: neon_pubkey,
            channel: tx,
            gas_limit_calculator: GasLimitCalculator::new(max_tx_account_cnt),
        }
    }

    pub fn pubkey(&self) -> Pubkey {
        self.pubkey
    }

    pub async fn get_solana_version(&self) -> Result<RpcVersionInfo, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(TaskCommand::GetSolanaVersion(tx), None, None))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_storage_at(
        &self,
        addr: Address,
        index: U256,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<[u8; 32], NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetStorageAt {
                    response: tx,
                    address: addr,
                    index,
                },
                tag,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_code(
        &self,
        addr: Address,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<Option<Bytes>, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetCode {
                    response: tx,
                    address: addr,
                },
                tag,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_balance(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<U256, NeonApiError> {
        let account = self.get_neon_account(addr, tag).await?;
        let mut balance = common::ethnum::U256::default();
        for resp in account {
            balance = resp.balance;
        }
        Ok(balance)
    }

    pub async fn get_transaction_count(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<u64, NeonApiError> {
        let account = self.get_neon_account(addr, tag).await?;
        let mut count = 0;
        for resp in account {
            count = resp.trx_count;
        }
        Ok(count)
    }

    pub async fn get_neon_account(
        &self,
        addr: BalanceAddress,
        tag: Option<BlockNumberOrTag>,
    ) -> Result<Vec<GetBalanceResponse>, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(
                TaskCommand::GetNeonAccount {
                    response: tx,
                    address: addr,
                },
                tag,
                None,
            ))
            .await
            .unwrap();
        rx.await.unwrap()
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
        let resp = self.emulate_raw(params).await?;
        decode_neon_response(resp)
    }

    // Returns raw response without checking for errors inside
    pub async fn emulate_raw(&self, params: TxParams) -> Result<EmulateResponse, NeonApiError> {
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
        rx.await.unwrap()
    }

    pub async fn get_config(&self) -> Result<GetConfigResponse, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(TaskCommand::GetConfig(tx), None, None))
            .await
            .unwrap();
        rx.await.unwrap().map_err(Into::into)
    }

    pub async fn get_cluster_size(&self) -> Result<usize, NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(TaskCommand::GetClusterSize(tx), None, None))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_health(&self) -> Result<(), NeonApiError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::new(TaskCommand::GetHealth(tx), None, None))
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
            id: None,
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
                    step_limit: Some(DEFAULT_MAX_EMULATE_EVM_STEP_COUNT),
                    chains: Some(config.chains.clone()),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                if let RpcEnum::CloneRpcClient(rpc) = &ctx.default_rpc {
                    tracing::debug!(commitment = ?rpc.commitment(), "estimate gas task command, emulate commitment");
                }
                // HACK: We're using the simulation RPC client here because a few tests fail
                // when using the finalized RPC client due to insufficient balance.
                // TODO: This should be fixed in the future by properly determining
                //  which commitment level to use for emulation.
                let resp = commands::emulate::execute(
                    &ctx.rpc_client_simulation,
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
                    step_limit: Some(DEFAULT_MAX_EMULATE_EVM_STEP_COUNT),
                    chains: Some(config.chains.clone()),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                let resp = commands::emulate::execute(
                    &ctx.rpc_client_simulation,
                    ctx.neon_pubkey,
                    req,
                    None::<TracerTypeEnum>,
                )
                .await;
                let resp = match resp {
                    Ok((resp, _something)) => Ok(resp),
                    Err(err) => Err(err.into()),
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
                    step_limit: Some(DEFAULT_MAX_EMULATE_EVM_STEP_COUNT),
                    chains: Some(config.chains),
                    trace_config: None,
                    accounts: Vec::new(),
                    tx,
                    solana_overrides: None,
                };
                tracing::info!(?req, "emulate_call");
                let resp = commands::emulate::execute(
                    &ctx.rpc_client_simulation,
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

            TaskCommand::GetSolanaVersion(response) => {
                let resp = ctx.rpc_client_finalized.get_version().await;
                let _ = response.send(resp.map_err(NeonError::from).map_err(Into::into));
            }

            TaskCommand::GetClusterSize(response) => {
                let resp = ctx.rpc_client_finalized.get_cluster_nodes().await;
                let resp = resp
                    .map(|list| list.len())
                    .map_err(NeonError::from)
                    .map_err(Into::into);
                let _ = response.send(resp);
            }

            TaskCommand::GetHealth(response) => {
                let resp = ctx.rpc_client_finalized.get_health().await;
                let _ = response.send(resp.map_err(NeonError::from).map_err(Into::into));
            }

            TaskCommand::GetStorageAt {
                response,
                address,
                index,
            } => {
                let resp = commands::get_storage_at::execute(
                    &ctx.rpc_client_simulation,
                    &ctx.neon_pubkey,
                    address,
                    index,
                )
                .await;
                let _ = response.send(
                    resp.map(|r| r.0)
                        .map_err(NeonError::from)
                        .map_err(Into::into),
                );
            }

            TaskCommand::GetCode { response, address } => {
                let resp = commands::get_contract::execute(
                    &ctx.rpc_client_simulation,
                    &ctx.neon_pubkey,
                    &[address],
                )
                .await;
                let resp = resp.map(|r| r.into_iter().next().map(|r| r.code));
                let _ = response.send(
                    resp.map(|code| code.map(Bytes::from))
                        .map_err(NeonError::from)
                        .map_err(Into::into),
                );
            }

            TaskCommand::GetNeonAccount { response, address } => {
                let resp =
                    commands::get_balance::execute(&ctx.default_rpc, &ctx.neon_pubkey, &[address])
                        .await;
                let _ = response.send(resp.map_err(NeonError::from).map_err(Into::into));
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
