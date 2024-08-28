use std::collections::HashMap;

use jsonrpsee::core::{async_trait, RpcResult};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::{ErrorCode, ErrorObjectOwned};
use reth_primitives::{Address, BlockId, BlockNumberOrTag, Bytes, U256, U64};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use common::convert::{ToNeon, ToReth};
use common::neon_lib::commands::emulate::{EmulateResponse, SolanaAccount};
use common::neon_lib::commands::get_balance::BalanceStatus;
use common::neon_lib::types::{BalanceAddress, SerializedAccount, TxParams};
use common::solana_sdk::pubkey::Pubkey;

use crate::rpc::EthApiImpl;

#[serde_as]
#[allow(unused)]
#[derive(Deserialize, Debug)]
pub struct NeonCall {
    #[serde_as(as = "Option<HashMap<DisplayFromStr,_>>")]
    #[serde(alias = "solana_overrides")]
    sol_account_dict: Option<HashMap<Pubkey, Option<SerializedAccount>>>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NeonEmulateResponse {
    exit_code: String,
    external_solana_call: bool,
    revert_before_solana_call: bool,
    revert_after_solana_call: bool,
    result: Bytes,
    num_evm_steps: u64,
    gas_used: u64,
    num_iterations: u64,
    solana_accounts: Vec<SolanaAccount>,
}

impl From<EmulateResponse> for NeonEmulateResponse {
    fn from(value: EmulateResponse) -> Self {
        NeonEmulateResponse {
            exit_code: value.exit_status,
            external_solana_call: value.external_solana_call,
            revert_before_solana_call: value.reverts_before_solana_calls,
            revert_after_solana_call: value.reverts_after_solana_calls,
            result: Bytes::from(value.result),
            num_evm_steps: value.steps_executed,
            gas_used: value.used_gas,
            num_iterations: value.iterations,
            solana_accounts: value.solana_accounts,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct NeonAccountResponse {
    status: BalanceStatus,
    address: Address,
    transaction_count: U256,
    balance: U256,
    chain_d: U64,
    solana_address: Pubkey,
    contract_solana_address: Pubkey,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EvmParams {
    neon_account_seed_version: u64,
    neon_max_evm_steps_in_last_iteration: u32,
    neon_min_evm_steps_in_iteration: u64,
    neon_gas_limit_multiplier_without_chain_id: u32,
    neon_holder_message_size: usize,
    neon_payment_to_treasury: i32,
    neon_storage_entries_in_contract_account: u32,
    neon_treasury_pool_count: u64,
    neon_treasury_pool_seed: String,
    neon_evm_program_id: String,
}

#[rpc(server, namespace = "neon")]
trait NeonCustomApi {
    #[method(name = "proxyVersion", aliases = ["neon_proxy_version"])]
    fn proxy_version(&self) -> RpcResult<String>;

    #[method(name = "solanaVersion")]
    async fn solana_version(&self) -> RpcResult<String>;

    #[method(name = "cliVersion", aliases = ["neon_cli_version"])]
    fn cli_version(&self) -> RpcResult<String>;

    #[method(name = "evmVersion")]
    async fn evm_version(&self) -> RpcResult<String>;

    #[method(name = "emulate")]
    async fn emulate(
        &self,
        tx: Bytes,
        neon_call: Option<NeonCall>,
        tag: Option<BlockNumberOrTag>,
    ) -> RpcResult<NeonEmulateResponse>;

    #[method(name = "getAccount")]
    async fn get_account(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<NeonAccountResponse>;

    #[method(name = "earliestBlockNumber")]
    async fn earliest_block_number(&self) -> RpcResult<U64>;

    #[method(name = "finalizedBlockNumber")]
    async fn finalized_block_number(&self) -> RpcResult<U64>;

    #[method(name = "getEvmParams")]
    async fn evm_params(&self) -> RpcResult<EvmParams>;
}

#[async_trait]
impl NeonCustomApiServer for EthApiImpl {
    fn proxy_version(&self) -> RpcResult<String> {
        let version = format!("Neon-proxy/v{}", env!("CARGO_PKG_VERSION"));
        Ok(version)
    }

    async fn solana_version(&self) -> RpcResult<String> {
        let version = self.neon_api.get_solana_version().await?;
        let version = format!("Solana/v{}", version.solana_core);
        Ok(version)
    }

    async fn evm_version(&self) -> RpcResult<String> {
        self.neon_evm_version().await
    }

    fn cli_version(&self) -> RpcResult<String> {
        let version = format!("Neon-cli/v{}", self.lib_version);
        Ok(version)
    }

    async fn emulate(
        &self,
        tx: Bytes,
        neon_call: Option<NeonCall>,
        tag: Option<BlockNumberOrTag>,
    ) -> RpcResult<NeonEmulateResponse> {
        use common::evm_loader::types::Transaction;
        tracing::info!(?tx, ?neon_call, ?tag, "neon_emulate");

        let tx = Transaction::from_rlp(&tx)
            .inspect_err(|error| tracing::warn!(%tx, %error, "could not decode transaction"))
            .map_err(|_| ErrorObjectOwned::from(ErrorCode::InvalidParams))?;
        let params = TxParams::from_transaction(Address::default().to_neon(), &tx);

        let resp = self.neon_api.emulate_raw(params).await?;
        Ok(resp.into())
    }

    async fn get_account(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<NeonAccountResponse> {
        let slot = if let Some(block_number) = block_number {
            Some(self.get_tag_by_block_id(block_number).await?)
        } else {
            None
        };
        let balance_address = BalanceAddress {
            address: address.to_neon(),
            chain_id: self.chain_id,
        };

        let account = self
            .neon_api
            .get_neon_account(balance_address, slot)
            .await?;
        let account = account
            .first()
            .ok_or::<ErrorObjectOwned>(ErrorCode::InternalError.into())?;
        let resp = NeonAccountResponse {
            status: account.status,
            address,
            transaction_count: U256::from(account.trx_count),
            balance: account.balance.to_reth(),
            chain_d: U64::from(self.chain_id),
            solana_address: account.solana_address,
            contract_solana_address: account.contract_solana_address,
        };
        Ok(resp)
    }

    async fn earliest_block_number(&self) -> RpcResult<U64> {
        self.blocks
            .earliest_slot()
            .await
            .map(U64::from)
            .map_err(|_| ErrorObjectOwned::from(ErrorCode::InternalError))
    }

    async fn finalized_block_number(&self) -> RpcResult<U64> {
        self.blocks
            .latest_number(true)
            .await
            .map(U64::from)
            .map_err(|_| ErrorObjectOwned::from(ErrorCode::InternalError))
    }

    async fn evm_params(&self) -> RpcResult<EvmParams> {
        use std::collections::BTreeMap;
        let config = self.neon_api.get_config().await?.config;
        tracing::info!(?config, "EVM CONFIG");
        fn get_int_param<T: std::str::FromStr>(
            map: &BTreeMap<String, String>,
            name: &str,
        ) -> Option<T> {
            map.get(name).and_then(|v| v.parse().ok())
        }
        let params = EvmParams {
            neon_account_seed_version: get_int_param(&config, "NEON_ACCOUNT_SEED_VERSION")
                .unwrap_or_default(),
            neon_max_evm_steps_in_last_iteration: get_int_param(
                &config,
                "NEON_EVM_STEPS_LAST_ITERATION_MAX",
            )
            .unwrap_or_default(),
            neon_min_evm_steps_in_iteration: get_int_param(&config, "NEON_EVM_STEPS_MIN")
                .unwrap_or_default(),
            neon_gas_limit_multiplier_without_chain_id: get_int_param(
                &config,
                "NEON_GAS_LIMIT_MULTIPLIER_NO_CHAINID",
            )
            .unwrap_or_default(),
            neon_holder_message_size: get_int_param(&config, "NEON_HOLDER_MSG_SIZE")
                .unwrap_or_default(),
            neon_payment_to_treasury: get_int_param(&config, "NEON_PAYMENT_TO_TREASURE")
                .unwrap_or_default(),
            neon_storage_entries_in_contract_account: get_int_param(
                &config,
                "NEON_STORAGE_ENTRIES_IN_CONTRACT_ACCOUNT",
            )
            .unwrap_or_default(),
            neon_treasury_pool_count: get_int_param(&config, "NEON_TREASURY_POOL_COUNT")
                .unwrap_or_default(),
            neon_treasury_pool_seed: config
                .get("NEON_TREASURY_POOL_SEED")
                .cloned()
                .unwrap_or_default(),
            neon_evm_program_id: self.neon_api.pubkey().to_string(),
        };
        Ok(params)
    }
}
