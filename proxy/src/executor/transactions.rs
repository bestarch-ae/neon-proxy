mod emulator;
mod holder;
mod ongoing;

use std::mem;
use std::sync::atomic::AtomicU8;

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use anyhow::Context;
use reth_primitives::B256;

use common::ethnum::U256;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::SolanaAccount as NeonSolanaAccount;
use common::neon_lib::types::Address;
use common::solana_sdk::compute_budget::ComputeBudgetInstruction;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::packet::PACKET_DATA_SIZE;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;
use solana_api::solana_api::SolanaApi;

use crate::executor::transactions::emulator::get_neon_emulate_request;
use crate::neon_api::NeonApi;

use self::holder::HolderInfo;
pub use self::ongoing::OngoingTransaction;
use self::ongoing::{TxData, TxStage};
use emulator::{Emulator, IterInfo};

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
}

impl TransactionBuilder {
    pub async fn new(
        program_id: Pubkey,
        solana_api: SolanaApi,
        neon_api: NeonApi,
        operator: Keypair,
        address: Address,
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
        })
    }

    pub fn keypair(&self) -> &Keypair {
        &self.operator
    }

    pub fn pubkey(&self) -> Pubkey {
        self.operator.pubkey()
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
        let chain_id = request.chain_id.context("unknown chain id")?; // FIXME

        if tx.length() < PACKET_DATA_SIZE - BASE_ACCOUNT_COUNT * mem::size_of::<Pubkey>() {
            self.start_data_execution(tx, chain_id).await
        } else {
            self.start_holder_execution(tx, chain_id).await
        }
    }

    pub async fn next_step(
        &self,
        tx: OngoingTransaction,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        let (stage, chain_id) = tx.disassemble();
        match stage {
            TxStage::HolderFill { info, tx: envelope } if info.is_empty() => self
                .execute_from_holder(info, envelope, chain_id)
                .await
                .map(Some),
            TxStage::HolderFill { info, tx: envelope } => {
                Ok(Some(self.fill_holder(info, envelope, chain_id)))
            }
            TxStage::IterativeExecution {
                tx_data,
                holder,
                iter_info,
                from_data,
            } => {
                self.step(iter_info, tx_data, holder, from_data, chain_id)
                    .await
            }
            // Single iteration stuff
            TxStage::SingleExecution { .. } | TxStage::Operational => Ok(None),
        }
    }

    async fn start_data_execution(
        &self,
        tx: TxEnvelope,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);
        let fallback_iterative = |tx_data| async move {
            self.empty_holder_for_iterative_data(tx_data, chain_id)
                .await
        };
        if self.emulator.needs_iterative_execution(&tx_data.emulate) {
            return fallback_iterative(tx_data).await;
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
            return fallback_iterative(tx_data).await;
        }

        Ok(TxStage::execute_data(tx_data).ongoing(&ixs, &self.pubkey(), chain_id))
    }

    async fn empty_holder_for_iterative_data(
        &self,
        tx_data: TxData,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let holder = self.new_holder_info(None)?;
        let ixs = self.create_holder(&holder).await?;
        let tx = TxStage::step_data(*holder.pubkey(), tx_data, None).ongoing(
            &ixs,
            &self.pubkey(),
            chain_id,
        );
        Ok(tx)
    }

    async fn start_holder_execution(
        &self,
        tx: TxEnvelope,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let holder = self.new_holder_info(Some(&tx))?;
        let ixs = self.create_holder(&holder).await?;

        Ok(TxStage::holder_fill(holder, tx).ongoing(&ixs, &self.pubkey(), chain_id))
    }

    fn fill_holder(&self, info: HolderInfo, tx: TxEnvelope, chain_id: u64) -> OngoingTransaction {
        let mut info = info;
        let ix = self.write_next_holder_chunk(&mut info);
        TxStage::holder_fill(info, tx).ongoing(&[ix], &self.pubkey(), chain_id)
    }

    async fn execute_from_holder(
        &self,
        holder: HolderInfo,
        tx: TxEnvelope,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let emulate = self.emulator.emulate(&tx).await?;
        let tx_data = TxData::new(tx, emulate);

        let fallback_to_iterative = |tx_data, holder| async move {
            self.step(None, tx_data, holder, false, chain_id)
                .await
                .transpose()
                .expect("must be some")
        };
        if self.emulator.needs_iterative_execution(&tx_data.emulate) {
            return fallback_to_iterative(tx_data, *holder.pubkey()).await;
        }

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
            return fallback_to_iterative(tx_data, *holder.pubkey()).await;
        }

        let stage = TxStage::execute_holder(*holder.pubkey(), tx_data);
        Ok(stage.ongoing(&ixs, &self.pubkey(), chain_id))
    }

    async fn step(
        &self,
        iter_info: Option<IterInfo>,
        tx_data: TxData,
        holder: Pubkey,
        from_data: bool,
        chain_id: u64,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
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
        let stage = match from_data {
            true => TxStage::step_data(holder, tx_data, Some(iter_info)),
            false => TxStage::step_holder(holder, tx_data, iter_info),
        };

        Ok(Some(stage.ongoing(
            &with_budget(ix, cu_limit),
            &self.pubkey(),
            chain_id,
        )))
    }

    fn build_step(
        &self,
        iter_info: &mut IterInfo,
        tx_data: &TxData,
        holder: Pubkey,
        from_data: bool,
        chain_id: u64,
    ) -> anyhow::Result<Instruction> {
        let mut iter_info = iter_info;
        let tag = if from_data {
            tag::TX_STEP_FROM_DATA
        } else {
            tag::TX_STEP_FROM_ACCOUNT
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

        const TAG_IDX: usize = 0;
        const TREASURY_IDX_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const STEP_COUNT_IDX: usize = TREASURY_IDX_IDX + mem::size_of::<u32>();
        const UNIQ_IDX_IDX: usize = STEP_COUNT_IDX + mem::size_of::<u32>();
        const END_IDX: usize = UNIQ_IDX_IDX + mem::size_of::<u32>();
        let mut data = vec![0; iter_info.as_ref().map_or(STEP_COUNT_IDX, |_| END_IDX)];

        data[TAG_IDX] = tag;
        data[TREASURY_IDX_IDX..STEP_COUNT_IDX].copy_from_slice(&treasury_pool_idx.to_le_bytes());
        if let Some(iter_info) = iter_info {
            data[STEP_COUNT_IDX..UNIQ_IDX_IDX]
                .copy_from_slice(&iter_info.step_count().to_le_bytes());
            data[UNIQ_IDX_IDX..END_IDX].copy_from_slice(&iter_info.next_idx().to_le_bytes());
        }

        Ok((accounts, data))
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

        TxStage::operational().ongoing(&[ix], &self.operator.pubkey(), chain_id)
    }
}

fn with_budget(ix: Instruction, cu_limit: u32) -> [Instruction; 3] {
    let cu = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
    let heap = ComputeBudgetInstruction::request_heap_frame(MAX_HEAP_SIZE);

    [cu, heap, ix]
}
