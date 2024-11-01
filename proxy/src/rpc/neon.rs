use std::collections::HashMap;

use futures_util::stream::TryStreamExt;
use futures_util::StreamExt;
use jsonrpsee::core::{async_trait, RpcResult};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::{ErrorCode, ErrorObjectOwned};
use reth_primitives::{Address, BlockId, BlockNumberOrTag, Bytes, B256, U256, U64};
use rpc_api_types::other::OtherFields;
use rpc_api_types::{Filter, Log, Transaction, WithOtherFields};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
// use evm_loader::types::{Address as NeonAddress};

use common::convert::{ToNeon, ToReth};
use common::neon_instruction::tag::tag_to_str;
use common::neon_lib::commands::emulate::{EmulateResponse, SolanaAccount};
use common::neon_lib::commands::get_balance::BalanceStatus;
use common::neon_lib::types::{BalanceAddress, SerializedAccount, TxParams};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use common::types::EventKind;
use mempool::GasPriceModel;

use crate::convert::{neon_to_eth, neon_to_eth_receipt, to_neon_receipt, NeonTransactionReceipt};
use crate::error::{internal_error, Error};
use crate::rpc::EthApiImpl;

#[serde_as]
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    token_name: String,
    #[serde_as(as = "DisplayFromStr")]
    token_mint: Pubkey,
    token_chain_id: U64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NeonReceipt {
    receipt: NeonTransactionReceipt,
    solana_block_hash: Option<B256>,
    solana_complete_transaction_signature: Signature,
    solana_complete_instruction_index: u64,
    solana_complete_inner_instruction_index: Option<u64>,
    neon_raw_transaction: Bytes,
    neon_is_completed: bool,
    neon_is_canceled: bool,
    solana_transactions: Vec<SolanaTransaction>,
    neon_costs: Vec<NeonCost>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NeonCost {
    neon_operator_address: Address,
    solana_lamport_expense: i64,
    neon_alan_income: u64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaTransaction {
    #[serde(skip)]
    sol_sig: Vec<u8>,
    solana_transaction_is_success: bool,
    solana_block_slot: u64,
    solana_lamport_expense: u64,
    neon_operator_address: Address,
    solana_instructions: Vec<SolanaInstruction>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaInstruction {
    solana_program: String,
    solana_instruction_index: u64,
    solana_inner_instruction_index: Option<u64>,
    svm_heap_size_limit: u64,
    svm_cycles_limit: u64,
    svm_cycles_used: u64,
    neon_instruction_code: u64,
    neon_instruction_name: String,
    neon_evm_steps: u64,
    neon_total_evm_steps: u64,
    neon_gas_used: u64,
    neon_total_gas_used: u64,
    neon_transaction_fee: u64,
    neon_miner: Option<Address>,
    neon_logs: Vec<NeonLog>,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReceiptDetail {
    Ethereum,
    Neon,
    SolanaTransactionList,
}

#[serde_as]
#[derive(Serialize, Debug, Clone)]
#[serde(transparent)]
pub struct SignatureList {
    #[serde_as(as = "Vec<DisplayFromStr>")]
    inner: Vec<Signature>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum SolanaByNeonResponse {
    Signatures(SignatureList),
}

impl From<Vec<Signature>> for SolanaByNeonResponse {
    fn from(value: Vec<Signature>) -> Self {
        SolanaByNeonResponse::Signatures(SignatureList { inner: value })
    }
}

#[serde_as]
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NeonLog {
    #[serde(flatten)]
    pub log: Log,
    pub removed: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub solana_transaction_signature: Signature,
    pub solana_instruction_index: u64,
    pub solana_inner_instruction_index: Option<u64>,
    pub solana_address: Option<Address>,
    pub neon_event_type: EventKind,
    pub neon_event_level: u64,
    pub neon_event_order: u64,
    pub neon_is_hidden: bool,
    pub neon_is_reverted: bool,
}

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

#[derive(Serialize, Debug, Clone)]
pub struct NeonVersions {
    proxy: String,
    evm: String,
    core: String,
    cli: String,
    solana: String,
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

    #[method(name = "versions")]
    async fn versions(&self) -> RpcResult<NeonVersions>;

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

    #[method(name = "getTransactionBySenderNonce")]
    async fn transaction_by_sender_nonce(
        &self,
        sender: Address,
        nonce: U256,
    ) -> RpcResult<Option<Transaction>>;

    #[method(name = "getLogs")]
    async fn logs(&self, request: Filter) -> RpcResult<Vec<WithOtherFields<NeonLog>>>;

    #[method(name = "getSolanaTransactionByNeonTransaction")]
    async fn solana_by_neon(
        &self,
        hash: B256,
        full: Option<bool>,
    ) -> RpcResult<SolanaByNeonResponse>;

    #[method(name = "getTransactionReceipt")]
    async fn transaction_receipt(
        &self,
        hash: B256,
        detail: ReceiptDetail,
    ) -> RpcResult<Option<NeonReceipt>>;

    #[method(name = "getNativeTokenList")]
    async fn native_token_list(&self) -> RpcResult<Vec<Token>>;

    #[method(name = "gasPrice")]
    async fn gas_price(&self) -> RpcResult<GasPriceModel>;
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
        self.neon_evm_version().await.map_err(Into::into)
    }

    fn cli_version(&self) -> RpcResult<String> {
        let version = format!("Neon-cli/v{}", self.lib_version);
        Ok(version)
    }

    async fn versions(&self) -> RpcResult<NeonVersions> {
        let versions = NeonVersions {
            proxy: self.proxy_version()?,
            evm: self.evm_version().await?,
            core: format!("Neon-Core-API/v{}", self.lib_version),
            cli: self.cli_version()?,
            solana: self.solana_version().await?,
        };
        Ok(versions)
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

    async fn transaction_by_sender_nonce(
        &self,
        sender: Address,
        nonce: U256,
    ) -> RpcResult<Option<Transaction>> {
        tracing::info!(%sender, %nonce, "by sender nonce");
        let tx = self
            .get_transaction(db::TransactionBy::SenderNonce {
                address: sender.to_neon(),
                nonce: nonce.to_neon().as_u64(),
                chain_id: self.chain_id,
            })
            .await?
            .map(|tx| neon_to_eth(tx.inner, tx.blockhash).map_err(Error::from))
            .transpose()?;
        tracing::info!("tx {:?}", tx);
        Ok(tx)
    }

    async fn logs(&self, filter: Filter) -> RpcResult<Vec<WithOtherFields<NeonLog>>> {
        use crate::convert::convert_filters;

        let filter = self.normalize_filter(filter).await?;
        let filters = convert_filters(filter).map_err(Error::from)?;
        let logs = self
            .get_logs(filters)
            .await?
            .into_iter()
            .map(|log| {
                let mut other = OtherFields::default();
                other.insert(
                    "address".to_string(),
                    serde_json::to_value(log.log.inner.address.to_string()).unwrap(),
                );
                WithOtherFields { inner: log, other }
            })
            .collect();
        Ok(logs)
    }

    async fn solana_by_neon(
        &self,
        hash: B256,
        _full: Option<bool>,
    ) -> RpcResult<SolanaByNeonResponse> {
        let stream = self.transactions.fetch_solana_signatures(hash.0);
        let signatures: Vec<_> = stream.try_collect().await.map_err(|err| {
            ErrorObjectOwned::owned(ErrorCode::InternalError.code(), err.to_string(), None::<()>)
        })?;
        Ok(SolanaByNeonResponse::from(signatures))
    }

    async fn transaction_receipt(
        &self,
        hash: B256,
        detail: ReceiptDetail,
    ) -> RpcResult<Option<NeonReceipt>> {
        use crate::convert::convert_filters;
        use common::evm_loader::types::Address as EvmAddress;

        let Some(tx_info) = self
            .get_transaction(db::TransactionBy::Hash(hash.0.into()))
            .await?
        else {
            return Ok(None);
        };

        let log_filter = Filter::new().select(hash);
        let log_filters = convert_filters(log_filter).map_err(Error::from)?;
        let logs = self.get_logs(log_filters).await?;

        let mut neon_costs_draft = HashMap::new();
        let mut sol_txs: Vec<SolanaTransaction> = vec![];
        if detail == ReceiptDetail::SolanaTransactionList {
            let mut sol_tx_ixs = self
                .sol_neon_transactions
                .fetch_with_costs(hash.0.into())
                .await;

            while let Some(result) = sol_tx_ixs.next().await {
                let sol_tx_ix = result.map_err(Error::from)?;
                let sol_sig = &sol_tx_ix.transaction.sol_sig;
                let neon_operator_address =
                    Address::new(EvmAddress::from(sol_tx_ix.cost.operator).0);

                if sol_txs.last().map_or(true, |last| last.sol_sig != *sol_sig) {
                    sol_txs.push(SolanaTransaction {
                        sol_sig: sol_sig.clone(),
                        solana_transaction_is_success: sol_tx_ix.transaction.is_success,
                        solana_block_slot: sol_tx_ix.transaction.block_slot as u64,
                        solana_lamport_expense: sol_tx_ix.cost.sol_spent as u64,
                        neon_operator_address,
                        solana_instructions: vec![],
                    });
                }

                let neon_transaction_fee = sol_tx_ix.transaction.neon_gas_used.as_u64()
                    * tx_info.inner.transaction.gas_price().as_u64();
                let cost = neon_costs_draft
                    .entry(neon_operator_address)
                    .or_insert(NeonCost {
                        neon_operator_address,
                        solana_lamport_expense: 0,
                        neon_alan_income: 0,
                    });
                cost.solana_lamport_expense += sol_tx_ix.cost.sol_spent;
                cost.neon_alan_income += neon_transaction_fee;

                let sol_tx = sol_txs.last_mut().unwrap();

                let sol_tx_ix_sig = common::solana_sdk::signature::Signature::from(
                    <[u8; 64]>::try_from(sol_tx_ix.transaction.sol_sig.clone())
                        .map_err(|_| internal_error("failed to parse tx signature"))?,
                );

                let sol_ix_logs = logs
                    .iter()
                    .filter(|log| {
                        log.solana_transaction_signature == sol_tx_ix_sig
                            && log.solana_instruction_index == sol_tx_ix.transaction.idx as u64
                            && log.solana_inner_instruction_index
                                == sol_tx_ix.transaction.inner_idx.map(|v| v as u64)
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                let sol_ix = SolanaInstruction {
                    solana_program: "NeonEVM".to_owned(),
                    solana_instruction_index: sol_tx_ix.transaction.idx as u64,
                    solana_inner_instruction_index: sol_tx_ix
                        .transaction
                        .inner_idx
                        .map(|v| v as u64),
                    svm_heap_size_limit: sol_tx_ix.transaction.used_heap_size as u64,
                    svm_cycles_limit: sol_tx_ix.transaction.max_bpf_cycle_cnt as u64,
                    svm_cycles_used: sol_tx_ix.transaction.used_bpf_cycle_cnt as u64,
                    neon_instruction_code: sol_tx_ix.transaction.ix_code as u64,
                    neon_instruction_name: tag_to_str(sol_tx_ix.transaction.ix_code as u8)
                        .to_owned(),
                    neon_total_evm_steps: sol_tx_ix.transaction.neon_step_cnt as u64,
                    neon_evm_steps: sol_tx_ix.transaction.neon_step_cnt as u64,
                    neon_gas_used: sol_tx_ix.transaction.neon_gas_used.as_u64(),
                    neon_total_gas_used: sol_tx_ix.transaction.neon_total_gas_used.as_u64(),
                    neon_transaction_fee,
                    neon_miner: None, // todo:
                    neon_logs: sol_ix_logs,
                };
                sol_tx.solana_instructions.push(sol_ix);
            }
        }

        let solana_complete_transaction_signature = tx_info.inner.sol_signature;
        let solana_complete_instruction_index = tx_info.inner.sol_ix_idx;
        let solana_complete_inner_instruction_index = tx_info.inner.sol_ix_inner_idx;
        let neon_is_completed = tx_info.inner.is_completed;
        let eth_receipt =
            neon_to_eth_receipt(tx_info.inner, tx_info.blockhash).map_err(Error::from)?;
        let neon_receipt = to_neon_receipt(eth_receipt);
        let solana_block_hash = neon_receipt.block_hash;

        let receipt = NeonReceipt {
            receipt: neon_receipt,
            solana_block_hash,
            solana_complete_transaction_signature,
            solana_complete_instruction_index,
            solana_complete_inner_instruction_index,
            neon_raw_transaction: Bytes::default(), // TODO: ???
            neon_is_completed,
            neon_is_canceled: false, // TODO: ???
            solana_transactions: sol_txs,
            neon_costs: neon_costs_draft.values().cloned().collect(),
        };

        Ok(Some(receipt))
    }

    async fn native_token_list(&self) -> RpcResult<Vec<Token>> {
        let config = self.neon_api.get_config().await?;
        Ok(config
            .chains
            .iter()
            .map(|chain| Token {
                token_name: chain.name.to_uppercase(),
                token_mint: chain.token,
                token_chain_id: U64::from(chain.id),
            })
            .collect())
    }

    async fn gas_price(&self) -> RpcResult<GasPriceModel> {
        self.mp_gas_prices
            .get_gas_price_model(Some(self.chain_id))
            .ok_or(ErrorObjectOwned::owned(
                ErrorCode::InternalError.code(),
                "Gas price model not found".to_string(),
                None::<()>,
            ))
    }
}
