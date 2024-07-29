mod alt;
mod emulator;
mod holder;
mod ongoing;

use std::mem;
use std::sync::atomic::AtomicU8;

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use anyhow::{bail, Context};
use reth_primitives::B256;

use common::ethnum::U256;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::SolanaAccount as NeonSolanaAccount;
use common::neon_lib::types::Address;
use common::solana_sdk::compute_budget::ComputeBudgetInstruction;
use common::solana_sdk::hash::HASH_BYTES;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::message::MESSAGE_HEADER_LENGTH;
use common::solana_sdk::packet::PACKET_DATA_SIZE;
use common::solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};
use common::solana_sdk::signature::{Keypair, SIGNATURE_BYTES};
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;
use solana_api::solana_api::SolanaApi;

use crate::executor::transactions::emulator::get_neon_emulate_request;
use crate::neon_api::NeonApi;

use self::alt::AltInfo;
use self::emulator::{get_chain_id, Emulator, IterInfo};
use self::holder::HolderInfo;
use self::ongoing::{TxData, TxStage};

pub use self::ongoing::OngoingTransaction;

const CU_IX_SIZE: usize = compiled_ix_size(0, 5 /* serialized data length */);
// Taken from neon-proxy.py
const MAX_HEAP_SIZE: u32 = 256 * 1024;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

pub struct TransactionBuilder {
    program_id: Pubkey,

    solana_api: SolanaApi,

    operator: Keypair,
    operator_address: Address,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,

    emulator: Emulator,
    holder_counter: AtomicU8,

    default_chain_id: u64,
}

/// ## Utility methods.
impl TransactionBuilder {
    pub async fn new(
        program_id: Pubkey,
        solana_api: SolanaApi,
        neon_api: NeonApi,
        operator: Keypair,
        address: Address,
        default_chain_id: u64,
    ) -> anyhow::Result<Self> {
        let config = neon_api.get_config().await?;

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

        let emulator = Emulator::new(neon_api, evm_steps_min, operator.pubkey());

        Ok(Self {
            program_id,
            solana_api,
            operator,
            operator_address: address,
            treasury_pool_count,
            treasury_pool_seed,
            emulator,
            holder_counter: AtomicU8::new(0),
            default_chain_id,
        })
    }

    pub fn keypair(&self) -> &Keypair {
        &self.operator
    }

    pub fn pubkey(&self) -> Pubkey {
        self.operator.pubkey()
    }

    pub fn default_chain_id(&self) -> u64 {
        self.default_chain_id
    }

    pub fn operator_balance(&self, chain_id: u64) -> Pubkey {
        let chain_id = U256::from(chain_id);
        let opkey = self.operator.pubkey();

        let seeds: &[&[u8]] = &[
            &[ACCOUNT_SEED_VERSION],
            opkey.as_ref(),
            &self.operator_address.0,
            &chain_id.to_be_bytes(),
        ];
        Pubkey::find_program_address(seeds, &self.program_id).0
    }

    pub fn treasury_pool(&self, hash: &B256) -> anyhow::Result<(u32, Pubkey)> {
        let base_idx = u32::from_le_bytes(*hash.0.first_chunk().expect("B256 is longer than 4"));
        let treasury_pool_idx = base_idx % self.treasury_pool_count;
        let (treasury_pool_address, _seed) = Pubkey::try_find_program_address(
            &[&self.treasury_pool_seed, &treasury_pool_idx.to_le_bytes()],
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
        data[ADDR_IDX..CHAIN_ID_IDX].copy_from_slice(&self.operator_address.0);
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
    pub async fn start_execution(&self, tx: TxEnvelope) -> anyhow::Result<OngoingTransaction> {
        // 1. holder
        // 2. payer
        // 3. treasury-pool-address,
        // 4. payer-token-address
        // 5. SolSysProg.ID
        // +6: NeonProg.ID
        // +7: CbProg.ID
        const BASE_ACCOUNT_COUNT: usize = 7;

        let request = get_neon_emulate_request(&tx)?;
        let chain_id = request.chain_id; // FIXME
        let fits_in_solana_tx =
            tx.length() < PACKET_DATA_SIZE - BASE_ACCOUNT_COUNT * mem::size_of::<Pubkey>();

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
        let stage = tx.disassemble();
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
                holder: Some(holder),
            } if info.is_empty() => self
                .execute_from_holder_emulated(holder, tx_data, Some(info))
                .await
                .map(Some),
            TxStage::AltFill {
                info,
                tx_data,
                holder: None,
            } if info.is_empty() => {
                let chain_id = get_chain_id(&tx_data.envelope)
                    .context("empty chain id in emulated from_data alt transaction")?;
                self.execute_from_data_emulated(tx_data, chain_id, Some(info))
                    .await
                    .map(Some)
            }
            TxStage::AltFill {
                info,
                tx_data,
                holder,
            } => self.fill_alt(info, tx_data, holder).map(Some),
            TxStage::IterativeExecution {
                tx_data,
                holder,
                iter_info,
                alt,
                from_data,
            } => self.step(iter_info, tx_data, holder, from_data, alt).await,
            // Single iteration stuff
            TxStage::SingleExecution { .. } | TxStage::Operational => Ok(None),
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
        tx: TxEnvelope,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);
        if Self::from_data_tx_len(
            tx_data.envelope.length(),
            tx_data.emulate.solana_accounts.len(),
        ) > PACKET_DATA_SIZE
        {
            self.start_from_alt(tx_data, None).await
        } else {
            self.execute_from_data_emulated(tx_data, chain_id, None)
                .await
        }
    }

    async fn execute_from_data_emulated(
        &self,
        tx_data: TxData,
        chain_id: u64,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let fallback_iterative =
            |tx_data, alt| async move { self.empty_holder_for_iterative_data(tx_data, alt).await };
        if self.emulator.needs_iterative_execution(&tx_data.emulate) {
            return fallback_iterative(tx_data, alt).await;
        }

        let (accounts, mut data) = self.execute_base(
            tag::TX_EXEC_FROM_DATA,
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
            return fallback_iterative(tx_data, alt).await;
        }

        self.build_ongoing(TxStage::execute_data(tx_data), &ixs, alt)
    }

    /// Creates empty holder account to be used during iterative execution.
    /// Only used in [`Self::start_data_execution`] until proper holder managing implemented.
    async fn empty_holder_for_iterative_data(
        &self,
        tx_data: TxData,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let holder = self.new_holder_info(None)?;
        let ixs = self.create_holder(&holder).await?;
        let tx =
            TxStage::step_data(*holder.pubkey(), tx_data, None, alt).ongoing(&ixs, &self.pubkey());
        Ok(tx)
    }

    fn from_data_tx_len(tx_len: usize, add_accounts_len: usize) -> usize {
        Self::tx_len_estimate(tx_len, add_accounts_len)
    }
}

/// ## Execution from holder.
impl TransactionBuilder {
    /// Execution (both iterative and non-iterative) from holder account entrypoint.
    ///
    /// Returns new [`OngoingTransaction`] that creates holder account, fills it with input
    /// transaction data on subsequent steps and eventually resolving into [`Self::execute_from_holder`].
    async fn start_holder_execution(&self, tx: TxEnvelope) -> anyhow::Result<OngoingTransaction> {
        let holder = self.new_holder_info(Some(&tx))?;
        let ixs = self.create_holder(&holder).await?;

        Ok(TxStage::holder_fill(holder, tx).ongoing(&ixs, &self.pubkey()))
    }

    /// Write next data chunk into holder account.
    fn fill_holder(&self, info: HolderInfo, tx: TxEnvelope) -> OngoingTransaction {
        let mut info = info;
        let ix = self.write_next_holder_chunk(&mut info);
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
        tx: TxEnvelope,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);
        if Self::from_holder_tx_len(tx_data.emulate.solana_accounts.len()) > PACKET_DATA_SIZE {
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
        let chain_id = get_chain_id(&tx_data.envelope);

        let fallback_to_iterative = |tx_data, holder, alt| async move {
            self.step(None, tx_data, holder, false, alt)
                .await
                .transpose()
                .expect("must be some")
        };
        let needs_iterative = self.emulator.needs_iterative_execution(&tx_data.emulate);
        let chain_id = match (chain_id, needs_iterative) {
            (None, _) | (_, true) => {
                return fallback_to_iterative(tx_data, *holder.pubkey(), alt).await
            }
            (Some(chain_id), false) => chain_id,
        };

        let (accounts, data) = self.execute_base(
            tag::TX_EXEC_FROM_ACCOUNT,
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
            return fallback_to_iterative(tx_data, *holder.pubkey(), alt).await;
        }

        let stage = TxStage::execute_holder(*holder.pubkey(), tx_data);
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
        holder: Pubkey,
        from_data: bool,
        alt: Option<AltInfo>,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        let chain_id = get_chain_id(&tx_data.envelope);
        let mut iter_info = match iter_info {
            Some(iter_info) if iter_info.is_finished() => return Ok(None),
            Some(info) => info,
            None => {
                let build_tx = |iter_info: &mut IterInfo| {
                    let mut txs = Vec::new();
                    while !iter_info.is_finished() {
                        let ix =
                            self.build_step(iter_info, &tx_data, holder, from_data, chain_id)?;
                        txs.push(Transaction::new_with_payer(
                            &with_budget(ix, MAX_COMPUTE_UNITS),
                            Some(&self.pubkey()),
                        ));
                    }
                    Ok(txs)
                };

                self.emulator
                    .calculate_iterations(&tx_data.emulate, build_tx)
                    .await?
            }
        };

        let ix = self.build_step(&mut iter_info, &tx_data, holder, from_data, chain_id)?;
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

    fn build_step(
        &self,
        iter_info: &mut IterInfo,
        tx_data: &TxData,
        holder: Pubkey,
        from_data: bool,
        chain_id: Option<u64>,
    ) -> anyhow::Result<Instruction> {
        let mut iter_info = iter_info;
        let (tag, chain_id) = match (from_data, chain_id) {
            (true, Some(chain_id)) => (tag::TX_STEP_FROM_DATA, chain_id),
            (false, Some(chain_id)) => (tag::TX_STEP_FROM_ACCOUNT, chain_id),
            (false, None) => (tag::TX_STEP_FROM_ACCOUNT_NO_CHAINID, self.default_chain_id),
            (true, None) => bail!("missing chain_id in step from data: {tx_data:?}"),
        };

        let (accounts, mut data) = self.execute_base(
            tag,
            tx_data.envelope.tx_hash(),
            chain_id,
            &tx_data.emulate.solana_accounts,
            Some(holder),
            Some(&mut iter_info),
        )?;

        if from_data {
            data.reserve(tx_data.envelope.encode_2718_len());
            tx_data.envelope.encode_2718(&mut &mut data);
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

    fn tx_len_estimate(tx_len: usize, add_accounts_len: usize) -> usize {
        // Holder
        // Operator
        // Treasury Pool
        // Operator Balance
        // System Program
        // NEON EVM Program
        // Compute Budget Program
        const BASE_ACCOUNTS: usize = 7;
        // Use STEP_END for a more conservative estimate that remains valid
        // for both single and iterative execution models
        const BASE_DATA: usize = TransactionBuilder::STEP_END_IDX;

        serialized_tx_length(
            [
                CU_IX_SIZE,
                CU_IX_SIZE,
                compiled_ix_size(BASE_ACCOUNTS, BASE_DATA + tx_len),
            ],
            BASE_ACCOUNTS + add_accounts_len,
        )
    }

    fn build_ongoing(
        &self,
        stage: TxStage,
        ixs: &[Instruction],
        alt: Option<AltInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        Ok(match alt {
            Some(alt) => stage.ongoing_alt(ixs, &self.pubkey(), alt.into_account())?,
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
    let mut ix_sum = 0;
    let mut idx = 0;
    while idx < N {
        ix_sum += ixs_lens[idx];
        idx += 1;
    }

    1 // Sig array size is always `1` that is serialized as single byte (See compact U16) 
        + SIGNATURE_BYTES                      // We always have only one solana signature
        + MESSAGE_HEADER_LENGTH                // Message header
        + compact_u16_len(accounts_len as u16) // Account array length (See compact U16)
        + accounts_len * PUBKEY_BYTES          // Account keys
        + HASH_BYTES                           // Recent Blockhash
        + compact_u16_len(N as u16)            // Ixs array length
        + ix_sum // Sum instruction data size
}

#[allow(clippy::identity_op)]
const fn compiled_ix_size(accounts_len: usize, data_len: usize) -> usize {
    1 // Program Id index
    + compact_u16_len(accounts_len as u16) // Account Info array len
    + accounts_len * 1                     // Account Info indice
    + compact_u16_len(data_len as u16)     // Data array len
    + data_len // Data
}
