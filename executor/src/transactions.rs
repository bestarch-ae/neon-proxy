mod alt;
mod emulator;
mod holder;
mod ongoing;
/// Solana preflight error handling.
mod preflight_error;

use std::mem;
use std::sync::Arc;

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use anyhow::{bail, Context, Result};
use arc_swap::access::Access;
use arc_swap::ArcSwap;
use ethnum::U256;
use evm_loader::config::ACCOUNT_SEED_VERSION;
use neon_lib::commands::emulate::SolanaAccount as NeonSolanaAccount;
use neon_lib::commands::get_config::ChainInfo;
use neon_lib::types::Address;
use reth_primitives::B256;
use semver::Version;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::HASH_BYTES;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::MESSAGE_HEADER_LENGTH;
use solana_sdk::packet::PACKET_DATA_SIZE;
use solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};
use solana_sdk::signature::SIGNATURE_BYTES;
use solana_sdk::system_program;
use solana_sdk::transaction::Transaction;
use typed_builder::TypedBuilder;

use common::neon_instruction::tag;
use common::EmulateResponseExt;
use neon_api::NeonApi;
use operator::Operator;
use solana_api::solana_api::SolanaApi;

use self::alt::{AltInfo, AltManager, AltUpdateInfo};
use self::emulator::{get_chain_id, Emulator, IterInfo};
use self::holder::{AcquireHolder, HolderInfo, HolderManager, RecoverableHolderState};
use self::ongoing::{TxData, TxStage};
use crate::ExecuteRequest;

pub use self::ongoing::OngoingTransaction;
pub use self::preflight_error::TxErrorKind;

const CU_IX_SIZE: usize = compiled_ix_size(0, 5 /* serialized data length */);
// Taken from neon-proxy.py
const MAX_HEAP_SIZE: u32 = 256 * 1024;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

#[derive(Debug, TypedBuilder)]
pub struct Config {
    pub program_id: Pubkey,
    pub operator: Arc<Operator>,
    pub max_holders: u8,
    #[builder(default)]
    pub pg_pool: Option<db::PgPool>,
}

#[derive(Debug, Clone)]
pub struct EvmConfig {
    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,

    chains: Vec<ChainInfo>,
    version: Version,
}

impl EvmConfig {
    const DEFAULT_NEON_EVM_VERSION: Version = Version::new(1, 14, 0);

    pub fn empty() -> Self {
        Self {
            treasury_pool_count: 0,
            treasury_pool_seed: Vec::new(),
            chains: Vec::new(),
            version: Self::DEFAULT_NEON_EVM_VERSION,
        }
    }
}

#[derive(Debug)]
pub struct TransactionBuilder {
    program_id: Pubkey,
    neon_api: NeonApi,

    operator: Arc<Operator>,
    emulator: Emulator,
    holder_mgr: HolderManager,
    alt_mgr: AltManager,

    evm_config: ArcSwap<EvmConfig>,
}

/// ## Utility methods.
impl TransactionBuilder {
    pub async fn new(
        solana_api: SolanaApi,
        neon_api: NeonApi,
        config: Config,
    ) -> anyhow::Result<Self> {
        let Config {
            program_id,
            operator,
            max_holders,
            pg_pool,
        } = config;
        let emulator = Emulator::new(neon_api.clone(), 0, operator.pubkey());
        let holder_mgr = HolderManager::new(
            operator.pubkey(),
            program_id,
            solana_api.clone(),
            max_holders,
        );
        let alt_mgr = AltManager::new(operator.pubkey(), solana_api.clone(), pg_pool).await;
        let this = Self {
            program_id,
            neon_api,
            operator,
            emulator,
            holder_mgr,
            alt_mgr,
            evm_config: ArcSwap::from_pointee(EvmConfig::empty()),
        };
        this.reload_config().await?;

        Ok(this)
    }

    pub async fn reload_config(&self) -> anyhow::Result<()> {
        let config = self.neon_api.get_config().await?;

        let treasury_pool_count: u32 = config
            .config
            .get("NEON_TREASURY_POOL_COUNT")
            .context("missing NEON_TREASURY_POOL_COUNT in config")?
            .parse()?;
        let treasury_pool_seed = config
            .config
            .get("NEON_TREASURY_POOL_SEED")
            .context("missing NEON_TREASURY_POOL_SEED in config")?
            .as_bytes()
            .to_vec();
        let evm_steps_min: u64 = config
            .config
            .get("NEON_EVM_STEPS_MIN")
            .context("missing NEON_EVM_STEPS_MIN in config")?
            .parse()?;

        let chains = config.chains;
        let version: Version = config.version.parse().unwrap_or_else(|error| {
            tracing::error!(
                ?error, version = config.version, default = %EvmConfig::DEFAULT_NEON_EVM_VERSION,
                "error parsing version in config, fallback to default"
            );
            EvmConfig::DEFAULT_NEON_EVM_VERSION
        });

        let config = EvmConfig {
            treasury_pool_count,
            treasury_pool_seed,
            version,
            chains,
        };

        self.evm_config.store(config.into());
        self.emulator.set_evm_steps_min(evm_steps_min);

        Ok(())
    }

    pub async fn recover(&mut self, init_holders: bool) -> anyhow::Result<Vec<OngoingTransaction>> {
        let mut output = Vec::new();
        let holders = self.holder_mgr.recover().await;

        for holder in holders {
            let stage = match holder.state {
                RecoverableHolderState::Pending(tx) => {
                    let Some(chain_id) = get_chain_id(&tx) else {
                        tracing::warn!(
                            operator = %self.operator.pubkey(),
                            pubkey = %holder.info.pubkey(),
                            tx_hash = %tx.tx_hash(),
                            "cannot determine chain id for recovered holder"
                        );
                        continue;
                    };
                    let request = ExecuteRequest::new(tx, chain_id);
                    TxStage::holder_fill(holder.info, request)
                }
                RecoverableHolderState::State {
                    tx_hash,
                    chain_id: Some(chain_id),
                    accounts,
                } => {
                    let iter_info = IterInfo::max(self.emulator.evm_steps_min() as u32); // FIXME
                    let accounts = accounts
                        .into_iter()
                        .map(|pubkey| NeonSolanaAccount {
                            pubkey,
                            is_writable: true,
                            is_legacy: true,
                        })
                        .collect();
                    TxStage::recovered_holder(tx_hash, chain_id, holder.info, iter_info, accounts)
                }
                RecoverableHolderState::State { chain_id: None, .. } => {
                    tracing::warn!(
                        operator = %self.operator.pubkey(),
                        pubkey = %holder.info.pubkey(),
                        tx_hash = %holder.state.tx_hash(),
                        "cannot determine chain id for recovered holder"
                    );
                    // TODO: Cancel
                    continue;
                }
            };
            output.push(
                self.next_step_inner(stage)
                    .await?
                    .expect("filled holder proceeds to execution"),
            );
        }

        if init_holders {
            for holder in self.holder_mgr.create_all_remaining().await? {
                let ixs = holder.create_ixs.expect("must exist for create");
                let tx = TxStage::Final {
                    tx_data: None,
                    holder: Some(holder.info),
                }
                .ongoing(&ixs, &self.operator.pubkey());
                output.push(tx);
            }
        }
        Ok(output)
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn pubkey(&self) -> Pubkey {
        self.operator.pubkey()
    }

    pub fn chains(&self) -> impl Access<Vec<ChainInfo>> + '_ {
        self.evm_config.map(|config: &EvmConfig| &config.chains)
    }

    pub fn operator_balance(&self, chain_id: u64) -> Pubkey {
        let chain_id = U256::from(chain_id);
        let opkey = self.operator.pubkey();

        let seeds: &[&[u8]] = &[
            &[ACCOUNT_SEED_VERSION],
            opkey.as_ref(),
            &self.operator.address().0 .0,
            &chain_id.to_be_bytes(),
        ];
        Pubkey::find_program_address(seeds, &self.program_id).0
    }

    pub fn treasury_pool(&self, hash: &B256) -> anyhow::Result<(u32, Pubkey)> {
        let base_idx = u32::from_le_bytes(*hash.0.first_chunk().expect("B256 is longer than 4"));
        let evm_config = self.evm_config.load();
        let treasury_pool_idx = base_idx % evm_config.treasury_pool_count;
        let (treasury_pool_address, _seed) = Pubkey::try_find_program_address(
            &[
                &evm_config.treasury_pool_seed,
                &treasury_pool_idx.to_le_bytes(),
            ],
            &self.program_id,
        )
        .context("cannot find program address")?;
        Ok((treasury_pool_idx, treasury_pool_address))
    }

    pub fn init_operator_balance(&self, chain_id: u64) -> OngoingTransaction {
        let addr = self.operator_balance(chain_id);

        const TAG_IDX: usize = 0;
        const ADDR_IDX: usize = TAG_IDX + 1;
        const CHAIN_ID_IDX: usize = ADDR_IDX + mem::size_of::<Address>();
        const DATA_LEN: usize = CHAIN_ID_IDX + mem::size_of::<u64>();

        let mut data = vec![0; DATA_LEN];
        data[TAG_IDX] = tag::OPERATOR_BALANCE_CREATE;
        data[ADDR_IDX..CHAIN_ID_IDX].copy_from_slice(&self.operator.address().0 .0);
        data[CHAIN_ID_IDX..].copy_from_slice(&chain_id.to_le_bytes());

        let accounts = vec![
            AccountMeta::new(self.operator.pubkey(), true), // TODO: maybe readonly?
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new(addr, false),
        ];

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        TxStage::operational().ongoing(&[ix], &self.operator.pubkey())
    }
}

/// ## Transaction flow.
impl TransactionBuilder {
    pub async fn start_execution(&self, tx: ExecuteRequest) -> anyhow::Result<OngoingTransaction> {
        let chain_id = get_chain_id(&tx);
        let fits_in_solana_tx = Self::from_data_tx_len(tx.length(), 0) < PACKET_DATA_SIZE;

        tracing::debug!(
            ?tx,
            tx_hash = %tx.tx_hash(),
            encoded_length = tx.length(),
            fits_in_solana_tx,
            "start execution"
        );
        if let Some(chain_id) = fits_in_solana_tx.then_some(chain_id).flatten() {
            self.start_data_execution(tx, chain_id).await
        } else {
            self.start_holder_execution(tx).await
        }
    }

    pub async fn next_step(
        &self,
        tx: OngoingTransaction,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        self.next_step_inner(tx.disassemble()).await
    }

    async fn next_step_inner(&self, stage: TxStage) -> anyhow::Result<Option<OngoingTransaction>> {
        match stage {
            TxStage::HolderFill { info, tx: envelope } if info.is_empty() => {
                self.execute_from_holder(info, envelope).await.map(Some)
            }
            TxStage::HolderFill { info, tx: envelope } => {
                Ok(Some(self.fill_holder(info, envelope)))
            }
            TxStage::AltFill {
                info,
                tx_data,
                holder,
            } => self.fill_alt(info, tx_data, holder).await.map(Some),
            TxStage::IterativeExecution {
                tx_data,
                holder,
                iter_info,
                alt,
                from_data,
            } => self.step(iter_info, tx_data, holder, from_data, alt).await,
            TxStage::DataExecution {
                tx_data,
                chain_id,
                holder,
                alt,
            } => self
                .execute_from_data(tx_data, holder, chain_id, alt)
                .await
                .map(Some),
            TxStage::RecoveredHolder {
                tx_hash,
                chain_id,
                holder,
                iter_info,
                accounts,
            } => {
                self.recovered_step(tx_hash, chain_id, holder, iter_info, accounts)
                    .await
            }
            TxStage::Final { .. } | TxStage::Cancel { .. } => Ok(None),
        }
    }

    pub async fn retry(
        &self,
        tx: OngoingTransaction,
        error: TxErrorKind,
    ) -> Result<OngoingTransaction> {
        let tx_hash = tx.tx_hash();
        tracing::debug!(?tx_hash, ?error, "retry tx");
        match error {
            TxErrorKind::CuMeterExceeded => self.handle_cu_meter(tx),
            TxErrorKind::TxSizeExceeded => self.handle_tx_size(tx).await,
            TxErrorKind::AltFail => self.handle_alt(tx).await,
            TxErrorKind::BadExternalCall | TxErrorKind::Other => {
                self.handle_preflight_error(tx).await
            }
            TxErrorKind::AlreadyProcessed => bail!("must be handled before"),
        }
    }
}

/// ## ALT helpers
impl TransactionBuilder {
    async fn start_from_alt(
        &self,
        tx_data: TxData,
        holder: Option<HolderInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let acquire = self
            .alt_mgr
            .acquire(tx_data.emulate.solana_accounts.iter().map(|acc| acc.pubkey))
            .await?;
        match acquire {
            alt::UpdateProgress::WriteChunk { ix, info } => {
                tracing::debug!(
                    tx_hash = %tx_data.envelope.tx_hash(), ?info, ?holder,
                    "extending ALT"
                );
                Ok(TxStage::alt_fill(info, tx_data, holder).ongoing(&[ix], &self.pubkey()))
            }
            alt::UpdateProgress::Ready(alt) => self.proceed_with_alt(tx_data, holder, alt).await,
        }
    }

    async fn fill_alt(
        &self,
        info: AltUpdateInfo,
        tx_data: TxData,
        holder: Option<HolderInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        match self.alt_mgr.update(info) {
            alt::UpdateProgress::WriteChunk { ix, info } => {
                tracing::debug!(
                    tx_hash = %tx_data.envelope.tx_hash(), ?info, ?holder,
                    "write next ALT chunk"
                );
                Ok(TxStage::alt_fill(info, tx_data, holder).ongoing(&[ix], &self.pubkey()))
            }
            alt::UpdateProgress::Ready(info) => self.proceed_with_alt(tx_data, holder, info).await,
        }
    }

    async fn proceed_with_alt(
        &self,
        tx_data: TxData,
        holder: Option<HolderInfo>,
        alt: AltInfo,
    ) -> anyhow::Result<OngoingTransaction> {
        match holder {
            Some(holder) => {
                self.execute_from_holder_emulated(holder, tx_data, Some(alt))
                    .await
            }
            None => {
                let chain_id = get_chain_id(&tx_data.envelope)
                    .context("empty chain id in emulated from_data alt transaction")?;
                self.dispatch_data_execution_by_version(tx_data, chain_id, Some(alt))
                    .await
            }
        }
    }
}

/// ## Execution from data.
impl TransactionBuilder {
    /// Execution (both iterative and non-iterative) from instruction data entrypoint.
    ///
    /// Returns new [`OngoingTransaction`] that contains:
    /// - a `TransactionExecuteFromInstruction` in case input can be executed in one Solana tx and
    ///   has no further steps,
    /// - or a `HolderCreate` that will resolve into `TransactionStepFromInstruction` on the next step.
    async fn start_data_execution(
        &self,
        tx: ExecuteRequest,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);
        let length_estimate = Self::from_data_tx_len(
            tx_data.envelope.length(),
            tx_data.emulate.solana_accounts.len(),
        );
        let alt_length_estimate = Self::from_data_alt_tx_len(
            tx_data.envelope.length(),
            tx_data.emulate.solana_accounts.len(),
        );
        tracing::debug!(
            tx_hash = %tx_data.envelope.tx_hash(),
            emulate = ?tx_data.emulate,
            length_estimate,
            alt_length_estimate,
            "start data execution"
        );
        if alt_length_estimate >= PACKET_DATA_SIZE {
            tracing::warn!(
            tx_hash = %tx_data.envelope.tx_hash(),
                "transaction estimate is too big, fallback to holder execution"
            );
            self.start_holder_execution(tx_data.envelope).await
        } else if length_estimate > PACKET_DATA_SIZE {
            self.start_from_alt(tx_data, None).await
        } else {
            self.dispatch_data_execution_by_version(tx_data, chain_id, None)
                .await
        }
    }

    async fn dispatch_data_execution_by_version(
        &self,
        tx_data: TxData,
        chain_id: u64,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        // NOTE: Use the last version with the old behaviour to support pre release version.
        // (e.g. 1.0.0-dev < 1.0.0)
        const BREAKPOINT: Version = Version::new(1, 14, u64::MAX);

        if self.evm_config.load().version > BREAKPOINT {
            self.empty_holder_for_data(tx_data, chain_id, alt, false)
                .await
        } else {
            #[allow(deprecated)]
            self.execute_from_data_deprecated(tx_data, chain_id, alt)
                .await
        }
    }

    async fn execute_from_data(
        &self,
        tx_data: TxData,
        holder: HolderInfo,
        chain_id: u64,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let holder_key = *holder.pubkey();
        let fallback_iterative = |tx_data, alt| async move {
            self.step(None, tx_data, holder, true, alt)
                .await
                .transpose()
                .expect("must be some")
        };
        if self.emulator.needs_iterative_execution(&tx_data.emulate) {
            tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, resize iter count");
            return fallback_iterative(tx_data, alt).await;
        }
        let tag = if tx_data.emulate.has_external_solana_call() {
            tag::TX_EXEC_FROM_DATA_SOLANA_CALL
        } else {
            tag::TX_EXEC_FROM_DATA
        };

        let (accounts, mut data) = self.execute_base(
            tag,
            tx_data.envelope.tx_hash(),
            chain_id,
            &tx_data.emulate.solana_accounts,
            Some(holder_key),
            None,
        )?;

        data.reserve(tx_data.envelope.encode_2718_len());
        tx_data.envelope.encode_2718(&mut &mut data);

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        let ixs = with_budget(ix, MAX_COMPUTE_UNITS);
        if !self
            .emulator
            .check_single_execution(tx_data.envelope.tx_hash(), &ixs)
            .await?
        {
            tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, failed single execution simulation");
            return fallback_iterative(tx_data, alt).await;
        }

        self.build_ongoing(TxStage::final_data(tx_data), &ixs, alt)
    }

    #[deprecated = "pre 1.15 behaviour"]
    async fn execute_from_data_deprecated(
        &self,
        tx_data: TxData,
        chain_id: u64,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let fallback_iterative = |tx_data, alt| async move {
            self.empty_holder_for_data(tx_data, chain_id, alt, true)
                .await
        };
        if self.emulator.needs_iterative_execution(&tx_data.emulate) {
            tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, resize iter count");
            return fallback_iterative(tx_data, alt).await;
        }
        let tag = if tx_data.emulate.has_external_solana_call() {
            tag::TX_EXEC_FROM_DATA_SOLANA_CALL_V13
        } else {
            tag::TX_EXEC_FROM_DATA_DEPRECATED_V13
        };

        let (accounts, mut data) = self.execute_base(
            tag,
            tx_data.envelope.tx_hash(),
            chain_id,
            &tx_data.emulate.solana_accounts,
            None,
            None,
        )?;

        data.reserve(tx_data.envelope.encode_2718_len());
        tx_data.envelope.encode_2718(&mut &mut data);

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        let ixs = with_budget(ix, MAX_COMPUTE_UNITS);
        if !self
            .emulator
            .check_single_execution(tx_data.envelope.tx_hash(), &ixs)
            .await?
        {
            tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, failed single execution simulation");
            return fallback_iterative(tx_data, alt).await;
        }

        self.build_ongoing(TxStage::final_data(tx_data), &ixs, alt)
    }

    /// Creates empty holder account to be used during execution from data.
    /// Only used in [`Self::start_data_execution`] until proper holder managing implemented.
    async fn empty_holder_for_data(
        &self,
        tx_data: TxData,
        chain_id: u64,
        alt: Option<AltInfo>,
        force_iterative: bool, // Used for deprecated behaviour
    ) -> anyhow::Result<OngoingTransaction> {
        let AcquireHolder {
            info: holder,
            create_ixs,
        } = self.holder_mgr.acquire_holder(None).await?;
        tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), ?holder, "creating new holder");
        match (force_iterative, create_ixs) {
            (true, Some(ixs)) => {
                Ok(TxStage::step_data(holder, tx_data, None, alt).ongoing(&ixs, &self.pubkey()))
            }
            (true, None) => self
                .step(None, tx_data, holder, true, alt)
                .await
                .transpose()
                .expect("first step always some"),
            (false, Some(ixs)) => {
                Ok(TxStage::data(tx_data, chain_id, holder, alt).ongoing(&ixs, &self.pubkey()))
            }
            (false, None) => self.execute_from_data(tx_data, holder, chain_id, alt).await,
        }
    }

    fn from_data_tx_len(tx_len: usize, add_accounts_len: usize) -> usize {
        Self::tx_len_estimate(tx_len, add_accounts_len)
    }

    fn from_data_alt_tx_len(tx_len: usize, add_accounts_len: usize) -> usize {
        Self::alt_tx_len_estimate(tx_len, add_accounts_len)
    }
}

/// ## Execution from holder.
impl TransactionBuilder {
    /// Execution (both iterative and non-iterative) from holder account entrypoint.
    ///
    /// Returns new [`OngoingTransaction`] that creates holder account, fills it with input
    /// transaction data on subsequent steps and eventually resolving into [`Self::execute_from_holder`].
    async fn start_holder_execution(
        &self,
        tx: ExecuteRequest,
    ) -> anyhow::Result<OngoingTransaction> {
        let AcquireHolder {
            info: holder,
            create_ixs,
        } = self.holder_mgr.acquire_holder(Some(&tx)).await?;
        tracing::debug!(tx_hash = %tx.tx_hash(), ?holder, "start holder execution");

        if let Some(ixs) = create_ixs {
            Ok(TxStage::holder_fill(holder, tx).ongoing(&ixs, &self.pubkey()))
        } else {
            Ok(self.fill_holder(holder, tx))
        }
    }

    /// Write next data chunk into holder account.
    fn fill_holder(&self, info: HolderInfo, tx: ExecuteRequest) -> OngoingTransaction {
        let offset_before = info.offset();
        let mut info = info;
        let ix = self.holder_mgr.write_next_holder_chunk(&mut info);
        tracing::debug!(tx_hash = %tx.tx_hash(), holder = ?info, offset_before, "next holder chunk");
        TxStage::holder_fill(info, tx).ongoing(&[ix], &self.pubkey())
    }

    /// Executes transaction from provided holder account.
    ///
    /// Returns new [`OngoingTransaction`] that contains:
    /// - a `TransactionExecuteFromAccount` in case input can be executed in one Solana tx and
    ///   has no further steps,
    /// - or a `TransactionStepFromAccount`.
    async fn execute_from_holder(
        &self,
        holder: HolderInfo,
        tx: ExecuteRequest,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);
        let length_estimate = Self::from_holder_tx_len(tx_data.emulate.solana_accounts.len());
        tracing::debug!(
            tx_hash = %tx_data.envelope.tx_hash(),
            emulate = ?tx_data.emulate,
            length_estimate,
            "execute from written holder"
        );
        if length_estimate > PACKET_DATA_SIZE {
            self.start_from_alt(tx_data, Some(holder)).await
        } else {
            self.execute_from_holder_emulated(holder, tx_data, None)
                .await
        }
    }

    async fn execute_from_holder_emulated(
        &self,
        holder: HolderInfo,
        tx_data: TxData,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let chain_id = get_chain_id(&tx_data.envelope.tx);

        let fallback_to_iterative = |tx_data, holder, alt| async move {
            self.step(None, tx_data, holder, false, alt)
                .await
                .transpose()
                .expect("must be some")
        };
        let needs_iterative = self.emulator.needs_iterative_execution(&tx_data.emulate);
        let chain_id = match (chain_id, needs_iterative) {
            (None, _) => {
                tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, no chain id");
                return fallback_to_iterative(tx_data, holder, alt).await;
            }
            (_, true) => {
                tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, resize iter count");
                return fallback_to_iterative(tx_data, holder, alt).await;
            }
            (Some(chain_id), false) => chain_id,
        };
        let tag = if tx_data.emulate.has_external_solana_call() {
            tag::TX_EXEC_FROM_ACCOUNT_SOLANA_CALL
        } else {
            tag::TX_EXEC_FROM_ACCOUNT
        };

        let (accounts, data) = self.execute_base(
            tag,
            holder.hash(),
            chain_id,
            &tx_data.emulate.solana_accounts,
            Some(*holder.pubkey()),
            None,
        )?;

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        let ixs = with_budget(ix, MAX_COMPUTE_UNITS);
        if !self
            .emulator
            .check_single_execution(tx_data.envelope.tx_hash(), &ixs)
            .await?
        {
            tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), "fallback to iterative, failed single execution simulation");
            return fallback_to_iterative(tx_data, holder, alt).await;
        }

        let stage = TxStage::final_holder(holder, tx_data);
        self.build_ongoing(stage, &ixs, alt)
    }

    fn from_holder_tx_len(add_accounts_len: usize) -> usize {
        Self::tx_len_estimate(0, add_accounts_len)
    }
}

/// ## Iterative execution.
impl TransactionBuilder {
    /// Make iterative execution progress.
    ///
    /// Calculates number of optimal iterations if `iter_info` is absent, otherwise returns next
    /// next step if needed. Must return `Some` if `iter_info` is `None`.
    async fn step(
        &self,
        iter_info: Option<IterInfo>,
        tx_data: TxData,
        holder: HolderInfo,
        from_data: bool,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        let chain_id = get_chain_id(&tx_data.envelope.tx);
        let tx_hash = tx_data.envelope.tx_hash();
        let mut iter_info = match iter_info {
            Some(iter_info) if iter_info.is_finished() => {
                tracing::debug!(%tx_hash, "iterations finished");
                return Ok(None);
            }
            Some(info) => info,
            None => {
                let build_tx = |iter_info: &mut IterInfo| {
                    let mut txs = Vec::new();
                    while !iter_info.is_finished() {
                        let ix = self.build_step(BuildStep::collect(
                            from_data,
                            iter_info,
                            &tx_data,
                            *holder.pubkey(),
                        ))?;
                        txs.push(Transaction::new_with_payer(
                            &with_budget(ix, MAX_COMPUTE_UNITS),
                            Some(&self.pubkey()),
                        ));
                    }
                    Ok(txs)
                };

                self.emulator
                    .calculate_iterations(tx_hash, &tx_data.emulate, build_tx)
                    .await?
            }
        };

        tracing::debug!(%tx_hash, ?iter_info, "new iteration");
        let ix = self.build_step(BuildStep::collect(
            from_data,
            &mut iter_info,
            &tx_data,
            *holder.pubkey(),
        ))?;
        let cu_limit = iter_info.cu_limit();
        let stage = match (from_data, chain_id) {
            (true, Some(_)) => TxStage::step_data(holder, tx_data, Some(iter_info), alt.clone()),
            (false, Some(_)) | (false, None) => {
                TxStage::step_holder(holder, tx_data, iter_info, alt.clone())
            }
            (true, None) => unreachable!("would have failed earlier"),
        };

        self.build_ongoing(stage, &with_budget(ix, cu_limit), alt.clone())
            .map(Some)
    }

    async fn recovered_step(
        &self,
        tx_hash: B256,
        chain_id: u64,
        holder: HolderInfo,
        iter_info: IterInfo,
        accounts: Vec<NeonSolanaAccount>,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        if self.holder_mgr.is_holder_finalized(holder.pubkey()).await? {
            return Ok(None);
        }

        let mut iter_info = iter_info;
        let ix = self.build_step(BuildStep {
            iter_info: &mut iter_info,
            tx_hash: &tx_hash,
            tx: None,
            solana_accounts: &accounts,
            holder: *holder.pubkey(),
            chain_id: Some(chain_id),
            fallback_chain_id: chain_id,
        })?;

        self.build_ongoing(
            TxStage::recovered_holder(tx_hash, chain_id, holder, iter_info, accounts),
            &[ix],
            None, // TODO
        )
        .map(Some)
    }

    fn cancel(
        &self,
        tx_data: TxData,
        holder: HolderInfo,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let chain_id =
            get_chain_id(&tx_data.envelope).unwrap_or(tx_data.envelope.fallback_chain_id);
        let operator_balance = self.operator_balance(chain_id);
        let tx_hash = tx_data.envelope.tx_hash();

        tracing::debug!(%tx_hash, chain_id, %operator_balance, ?holder, "cancel transaction");

        let mut accounts = vec![
            AccountMeta::new(*holder.pubkey(), false),
            AccountMeta::new(self.operator.pubkey(), true),
            AccountMeta::new(operator_balance, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];
        accounts.extend(
            tx_data
                .emulate
                .solana_accounts
                .iter()
                .map(|acc| AccountMeta {
                    pubkey: acc.pubkey,
                    is_writable: acc.is_writable,
                    is_signer: false,
                }),
        );

        const TX_HASH_IDX: usize = TransactionBuilder::TAG_IDX + mem::size_of::<u8>();
        const DATA_END: usize = TX_HASH_IDX + 32;
        let mut data = vec![0; DATA_END];

        data[Self::TAG_IDX] = tag::CANCEL;
        data[TX_HASH_IDX..DATA_END].copy_from_slice(tx_hash.as_ref());

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        self.build_ongoing(
            TxStage::cancel(*tx_data.envelope.tx_hash(), holder),
            &[ix],
            alt,
        )
    }
}

struct BuildStep<'a> {
    iter_info: &'a mut IterInfo,
    tx_hash: &'a B256,
    tx: Option<&'a TxEnvelope>,
    solana_accounts: &'a [NeonSolanaAccount],
    holder: Pubkey,
    chain_id: Option<u64>,
    fallback_chain_id: u64,
}

impl<'a> BuildStep<'a> {
    fn collect(
        from_data: bool,
        iter_info: &'a mut IterInfo,
        tx_data: &'a TxData,
        holder: Pubkey,
    ) -> Self {
        Self {
            iter_info,
            tx_hash: tx_data.envelope.tx_hash(),
            tx: from_data.then_some(&tx_data.envelope),
            solana_accounts: &tx_data.emulate.solana_accounts,
            holder,
            chain_id: get_chain_id(&tx_data.envelope),
            fallback_chain_id: tx_data.envelope.fallback_chain_id,
        }
    }
}

impl TransactionBuilder {
    fn build_step(&self, params: BuildStep<'_>) -> anyhow::Result<Instruction> {
        let BuildStep {
            mut iter_info,
            tx_hash,
            tx,
            solana_accounts,
            holder,
            chain_id,
            fallback_chain_id,
        } = params;
        let (tag, chain_id) = match (tx.is_some(), chain_id) {
            (true, Some(chain_id)) => (tag::TX_STEP_FROM_DATA, chain_id),
            (false, Some(chain_id)) => (tag::TX_STEP_FROM_ACCOUNT, chain_id),
            (false, None) => (tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID, fallback_chain_id),
            (true, None) => bail!("missing chain_id in step from data: {tx:?}"),
        };

        let (accounts, mut data) = self.execute_base(
            tag,
            tx_hash,
            chain_id,
            solana_accounts,
            Some(holder),
            Some(&mut iter_info),
        )?;

        if let Some(tx) = tx {
            data.reserve(tx.encode_2718_len());
            tx.encode_2718(&mut &mut data);
        }

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        Ok(ix)
    }
}

/// ## Common Logic.
impl TransactionBuilder {
    // Common data
    const TAG_IDX: usize = 0;
    const TREASURY_IDX_IDX: usize = Self::TAG_IDX + mem::size_of::<u8>();
    // Step data
    const STEP_COUNT_IDX: usize = Self::TREASURY_IDX_IDX + mem::size_of::<u32>();
    const UNIQ_IDX_IDX: usize = Self::STEP_COUNT_IDX + mem::size_of::<u32>();
    const STEP_END_IDX: usize = Self::UNIQ_IDX_IDX + mem::size_of::<u32>();
    // No Step
    const NO_STEP_END_IDX: usize = Self::STEP_COUNT_IDX;

    /// Prepare common instruction data and account list for a `TransactionStep`/`TransactionExecute`
    /// NEON EVM instructions.
    fn execute_base(
        &self,
        tag: u8,
        hash: &B256,
        chain_id: u64,
        collected_accounts: &[NeonSolanaAccount],
        holder: Option<Pubkey>,
        iter_info: Option<&mut IterInfo>,
    ) -> anyhow::Result<(Vec<AccountMeta>, Vec<u8>)> {
        let (treasury_pool_idx, treasury_pool_address) = self.treasury_pool(hash)?;
        let operator_balance = self.operator_balance(chain_id);

        tracing::debug!(
            tag, tx_hash = %hash, chain_id, %operator_balance,
            treasury_pool_idx, %treasury_pool_address,
            ?holder, ?iter_info,
            "build execution transaction"
        );

        let mut accounts: Vec<_> = holder
            .into_iter()
            .map(|key| AccountMeta::new(key, false))
            .collect();
        accounts.extend([
            AccountMeta::new(self.operator.pubkey(), true),
            AccountMeta::new(treasury_pool_address, false),
            AccountMeta::new(operator_balance, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]);
        accounts.extend(collected_accounts.iter().map(|acc| AccountMeta {
            pubkey: acc.pubkey,
            is_writable: acc.is_writable,
            is_signer: false,
        }));

        let data_len = iter_info
            .as_ref()
            .map_or(Self::NO_STEP_END_IDX, |_| Self::STEP_END_IDX);
        let mut data = vec![0; data_len];

        data[Self::TAG_IDX] = tag;
        data[Self::TREASURY_IDX_IDX..Self::STEP_COUNT_IDX]
            .copy_from_slice(&treasury_pool_idx.to_le_bytes());
        if let Some(iter_info) = iter_info {
            data[Self::STEP_COUNT_IDX..Self::UNIQ_IDX_IDX]
                .copy_from_slice(&iter_info.step_count().to_le_bytes());
            data[Self::UNIQ_IDX_IDX..Self::STEP_END_IDX]
                .copy_from_slice(&iter_info.next_idx().to_le_bytes());
        }

        Ok((accounts, data))
    }

    // Holder
    // Operator
    // Treasury Pool
    // Operator Balance
    // System Program
    // NEON EVM Program
    // Compute Budget Program
    const ESTIMATE_BASE_ACCOUNTS: usize = 7;
    // Use STEP_END for a more conservative estimate that remains valid
    // for both single and iterative execution models
    const ESTIMATE_BASE_DATA: usize = TransactionBuilder::STEP_END_IDX;

    fn tx_len_estimate(tx_len: usize, add_accounts_len: usize) -> usize {
        serialized_tx_length(
            [
                CU_IX_SIZE,
                CU_IX_SIZE,
                compiled_ix_size(
                    Self::ESTIMATE_BASE_ACCOUNTS,
                    Self::ESTIMATE_BASE_DATA + tx_len,
                ),
            ],
            Self::ESTIMATE_BASE_ACCOUNTS + add_accounts_len,
        )
    }

    fn alt_tx_len_estimate(tx_len: usize, add_accounts_len: usize) -> usize {
        serialized_alt_tx_length(
            [
                CU_IX_SIZE,
                CU_IX_SIZE,
                compiled_ix_size(
                    Self::ESTIMATE_BASE_ACCOUNTS,
                    Self::ESTIMATE_BASE_DATA + tx_len,
                ),
            ],
            Self::ESTIMATE_BASE_ACCOUNTS,
            add_accounts_len,
        )
    }

    fn build_ongoing(
        &self,
        stage: TxStage,
        ixs: &[Instruction],
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        Ok(match alt {
            Some(alt) => stage.ongoing_alt(ixs, &self.pubkey(), alt)?,
            None => stage.ongoing(ixs, &self.pubkey()),
        })
    }
}

fn with_budget(ix: Instruction, cu_limit: u32) -> [Instruction; 3] {
    let cu = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
    let heap = ComputeBudgetInstruction::request_heap_frame(MAX_HEAP_SIZE);

    [cu, heap, ix]
}

/// Serialized compact u16 length in bytes
const fn compact_u16_len(value: u16) -> usize {
    match value {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000.. => 3, // Extremely unlikely
    }
}

const fn serialized_tx_length<const N: usize>(ixs_lens: [usize; N], accounts_len: usize) -> usize {
    serialized_alt_tx_length(ixs_lens, accounts_len, 0)
}

const fn serialized_alt_tx_length<const N: usize>(
    ixs_lens: [usize; N],
    accounts_len: usize,
    num_accounts_in_alt: usize,
) -> usize {
    let mut ix_sum = 0;
    let mut idx = 0;
    while idx < N {
        ix_sum += ixs_lens[idx];
        idx += 1;
    }

    // We currently use only 1 ALT per transaction
    let (alt_array_len, alt_payload, message_tag_len) = if num_accounts_in_alt == 0 {
        (0, 0, 0)
    } else {
        (
            compact_u16_len(1),
            alt_descriptor_size(num_accounts_in_alt),
            1,
        )
    };

    // v0 transaction layout is actually identical to legacy transaction layout except for:
    //   1. having a tag in the beginning of the payload,
    //   2. having a ALT array in the end.
    // See [`solana_sdk::message::VersionedMessage`].
    1 // Sig array size is always `1` that is serialized as single byte (See compact U16) 
        + SIGNATURE_BYTES                            // We always have only one solana signature
        + message_tag_len                            // Message Tag: empty if legacy, 1 if v0
        + MESSAGE_HEADER_LENGTH                      // Message header
        + short_vec_size(accounts_len, PUBKEY_BYTES) // Accounts array
        + HASH_BYTES                                 // Recent Blockhash
        + compact_u16_len(N as u16)                  // Ixs array length
        + ix_sum                                     // Sum instruction data size
        + alt_array_len
        + alt_payload
}

const fn short_vec_size(len: usize, type_size: usize) -> usize {
    compact_u16_len(len as u16) + type_size * len
}

const fn compiled_ix_size(accounts_len: usize, data_len: usize) -> usize {
    1 // Program Id index
        // + compact_u16_len(accounts_len as u16) // Account Info array len
        // + accounts_len * 1                     // Account Info indice
        + short_vec_size(accounts_len, mem::size_of::<u8>()) // Account Info indice array
        // + compact_u16_len(data_len as u16)     // Data array len
        // + data_len                             // Data 
        + short_vec_size(data_len, mem::size_of::<u8>()) // Data array
}

/// See [`solana_sdk::message::v0::MessageAddressTableLookup`]
const fn alt_descriptor_size(len: usize) -> usize {
    PUBKEY_BYTES // Pubkey
        + short_vec_size(len, mem::size_of::<u8>()) // Writable
        + short_vec_size(0, mem::size_of::<u8>()) // Readable
}
