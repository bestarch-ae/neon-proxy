use std::mem;
use std::sync::atomic::{AtomicU8, Ordering};

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use anyhow::Context;
use reth_primitives::B256;

use common::ethnum::U256;
use common::evm_loader::account;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::{EmulateResponse, SolanaAccount as NeonSolanaAccount};
use common::neon_lib::types::{Address, TxParams};
use common::solana_sdk::compute_budget::ComputeBudgetInstruction;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::packet::PACKET_DATA_SIZE;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_instruction;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;
use solana_api::solana_api::SolanaApi;

use crate::convert::ToNeon;
use crate::neon_api::NeonApi;

#[derive(Debug)]
enum TxStage {
    Operational,
    HolderFill {
        info: HolderInfo,
        tx: TxEnvelope,
    },
    IterativeExecution {
        tx_data: TxData,
        holder: Pubkey,
        iter_info: IterativeInfo,
        from_data: bool,
    },
    SingleExecution {
        tx_data: TxData,
        holder: Option<Pubkey>,
    },
}

impl TxStage {
    fn ongoing(self, ixs: &[Instruction], payer: &Pubkey, chain_id: u64) -> OngoingTransaction {
        OngoingTransaction {
            stage: self,
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            chain_id,
        }
    }

    fn holder_fill(info: HolderInfo, tx: TxEnvelope) -> Self {
        Self::HolderFill { info, tx }
    }

    fn execute_data(tx_data: TxData) -> Self {
        Self::SingleExecution {
            tx_data,
            holder: None,
        }
    }

    fn execute_holder(holder: Pubkey, tx_data: TxData) -> Self {
        Self::SingleExecution {
            tx_data,
            holder: Some(holder),
        }
    }

    fn step_data(holder: Pubkey, tx_data: TxData, iter_info: IterativeInfo) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info,
            from_data: true,
        }
    }

    fn step_holder(holder: Pubkey, tx_data: TxData, iter_info: IterativeInfo) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info,
            from_data: false,
        }
    }
}

#[derive(Debug)]
pub struct OngoingTransaction {
    stage: TxStage,
    tx: Transaction,
    chain_id: u64,
}

#[derive(Debug)]
struct TxData {
    envelope: TxEnvelope,
    emulate: EmulateResponse,
}

impl TxData {
    fn new(envelope: TxEnvelope, emulate: EmulateResponse) -> Self {
        Self { envelope, emulate }
    }
}

impl OngoingTransaction {
    fn new_operational(ixs: &[Instruction], payer: &Pubkey, chain_id: u64) -> Self {
        TxStage::Operational.ongoing(ixs, payer, chain_id)
    }

    pub fn eth_tx(&self) -> Option<&TxEnvelope> {
        match &self.stage {
            TxStage::HolderFill { tx: envelope, .. }
            | TxStage::IterativeExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::SingleExecution {
                tx_data: TxData { envelope, .. },
                ..
            } => Some(envelope),
            TxStage::Operational => None,
        }
    }

    pub fn blockhash(&self) -> &Hash {
        &self.tx.message.recent_blockhash // TODO: None if default?
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn sign(&mut self, signers: &[&Keypair], blockhash: Hash) -> anyhow::Result<&Transaction> {
        self.tx
            .try_sign(signers, blockhash)
            .context("could not sign transactions")?;
        Ok(&self.tx)
    }
}

// TODO: move to submodule to hide fields`
#[derive(Clone, Debug)]
struct IterativeInfo {
    step_count: u32,
    #[allow(dead_code)]
    iterations: u32,
    unique_idx: u32,
}

impl IterativeInfo {
    fn new(step_count: u32, iterations: u32) -> Self {
        Self {
            step_count,
            iterations,
            unique_idx: 0,
        }
    }

    fn next_idx(&mut self) -> u32 {
        let out = self.unique_idx;
        self.unique_idx += 1;
        out
    }

    fn is_finished(&self) -> bool {
        self.unique_idx >= self.iterations
    }
}

#[derive(Debug)]
struct HolderInfo {
    seed: String,
    pubkey: Pubkey,
    data: Vec<u8>,
    current_offset: usize,
    hash: B256,
}

impl HolderInfo {
    fn is_empty(&self) -> bool {
        self.current_offset >= self.data.len()
    }
}

pub struct TransactionBuilder {
    program_id: Pubkey,

    solana_api: SolanaApi,
    neon_api: NeonApi,

    operator: Keypair,
    operator_address: Address,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,
    evm_steps_min: u64,

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

        Ok(Self {
            program_id,
            solana_api,
            neon_api,
            operator,
            operator_address: address,
            treasury_pool_count,
            treasury_pool_seed,
            evm_steps_min,
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

    fn calculate_resize_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        self.calculate_wrap_iter_cnt(emulate).saturating_sub(2)
    }

    fn calculate_exec_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        (emulate.steps_executed + self.evm_steps_min).saturating_sub(1) / self.evm_steps_min
    }

    fn calculate_wrap_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        emulate.iterations - self.calculate_exec_iter_cnt(emulate)
    }

    fn needs_iterative_execution(&self, emulate: &EmulateResponse) -> bool {
        self.calculate_resize_iter_cnt(emulate) > 0 || emulate.steps_executed >= 40
    }

    fn calculate_iterations(&self, emulate: &EmulateResponse) -> IterativeInfo {
        // TODO: non default
        let steps_per_iteration = self.evm_steps_min;
        let iterations =
            self.calculate_exec_iter_cnt(emulate) + self.calculate_wrap_iter_cnt(emulate);
        IterativeInfo::new(steps_per_iteration as u32, iterations as u32)
    }

    fn maybe_iter_info(&self, emulate: &EmulateResponse) -> Option<IterativeInfo> {
        self.needs_iterative_execution(emulate)
            .then(|| self.calculate_iterations(emulate))
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
        let emulate = self.neon_api.emulate(request).await?;

        println!("emulate: {emulate:#?}");

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
        match tx.stage {
            TxStage::HolderFill { info, tx: envelope } if info.is_empty() => self
                .execute_from_holder(info, envelope, tx.chain_id)
                .await
                .map(Some),
            TxStage::HolderFill { info, tx: envelope } => {
                Ok(Some(self.fill_holder(info, envelope, tx.chain_id)))
            }
            TxStage::IterativeExecution {
                tx_data,
                holder,
                iter_info,
                from_data,
            } => self.step(iter_info, tx_data, holder, from_data, tx.chain_id),
            // Single iteration stuff
            TxStage::SingleExecution { .. } | TxStage::Operational => Ok(None),
        }
    }

    async fn start_data_execution(
        &self,
        tx: TxEnvelope,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let request = get_neon_emulate_request(&tx)?;
        let emulate = self.neon_api.emulate(request).await?;
        let tx_data = TxData::new(tx, emulate);
        if let Some(iter_info) = self.maybe_iter_info(&tx_data.emulate) {
            let holder = self.new_holder_info(None)?;
            let ixs = self.create_holder(&holder).await?;
            let tx = TxStage::step_data(holder.pubkey, tx_data, iter_info).ongoing(
                &ixs,
                &self.pubkey(),
                chain_id,
            );
            return Ok(tx);
        }

        let (accounts, mut data) = self.execute_base(
            tag::TX_EXEC_FROM_DATA,
            tx_data.envelope.tx_hash(),
            chain_id,
            &tx_data.emulate.solana_accounts, // TODO: Do we really need them later?
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

        Ok(TxStage::execute_data(tx_data).ongoing(&with_budget(ix), &self.pubkey(), chain_id))
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
        let request = get_neon_emulate_request(&tx)?;
        let emulate = self.neon_api.emulate(request).await?;
        let mut iter_info = self.maybe_iter_info(&emulate);
        let tag = if iter_info.is_some() {
            tag::TX_STEP_FROM_ACCOUNT
        } else {
            tag::TX_EXEC_FROM_ACCOUNT
        };

        let (accounts, data) = self.execute_base(
            tag,
            &holder.hash,
            chain_id,
            &emulate.solana_accounts,
            Some(holder.pubkey),
            iter_info.as_mut(),
        )?;

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        let tx_data = TxData::new(tx, emulate);
        let stage = match iter_info {
            Some(iter_info) => TxStage::step_holder(holder.pubkey, tx_data, iter_info),
            None => TxStage::execute_holder(holder.pubkey, tx_data),
        };
        Ok(stage.ongoing(&with_budget(ix), &self.pubkey(), chain_id))
    }

    fn step(
        &self,
        iter_info: IterativeInfo,
        tx_data: TxData,
        holder: Pubkey,
        from_data: bool,
        chain_id: u64,
    ) -> anyhow::Result<Option<OngoingTransaction>> {
        if iter_info.is_finished() {
            return Ok(None);
        }
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

        let stage = match from_data {
            true => TxStage::step_data(holder, tx_data, iter_info),
            false => TxStage::step_holder(holder, tx_data, iter_info),
        };
        Ok(Some(stage.ongoing(
            &with_budget(ix),
            &self.pubkey(),
            chain_id,
        )))
    }

    fn execute_base(
        &self,
        tag: u8,
        hash: &B256,
        chain_id: u64,
        collected_accounts: &[NeonSolanaAccount],
        holder: Option<Pubkey>,
        iter_info: Option<&mut IterativeInfo>,
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
            data[STEP_COUNT_IDX..UNIQ_IDX_IDX].copy_from_slice(&iter_info.step_count.to_le_bytes());
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

        OngoingTransaction::new_operational(&[ix], &self.operator.pubkey(), chain_id)
    }

    fn new_holder_info(&self, tx: Option<&TxEnvelope>) -> anyhow::Result<HolderInfo> {
        let idx = self.holder_counter.fetch_add(1, Ordering::Relaxed);
        let seed = format!("holder{idx}");
        let pubkey = Pubkey::create_with_seed(&self.pubkey(), &seed, &self.program_id)
            .context("cannot create holder address")?;
        let (hash, data) = if let Some(tx) = tx {
            let mut data = Vec::with_capacity(tx.encode_2718_len());
            tx.encode_2718(&mut &mut data);
            (*tx.tx_hash(), data)
        } else {
            (Default::default(), Vec::new())
        };

        Ok(HolderInfo {
            seed,
            pubkey,
            data,
            current_offset: 0,
            hash,
        })
    }

    async fn create_holder(&self, holder: &HolderInfo) -> anyhow::Result<[Instruction; 2]> {
        self.create_holder_inner(&holder.seed, holder.pubkey).await
    }

    async fn create_holder_inner(
        &self,
        seed: &str,
        key: Pubkey,
    ) -> anyhow::Result<[Instruction; 2]> {
        const HOLDER_DATA_LEN: usize = 256 * 1024; // neon_proxy.py default
        const HOLDER_META_LEN: usize =
            account::ACCOUNT_PREFIX_LEN + mem::size_of::<account::HolderHeader>();
        const HOLDER_SIZE: usize = HOLDER_META_LEN + HOLDER_DATA_LEN;

        let sp_ix = system_instruction::create_account_with_seed(
            &self.pubkey(),
            &key,
            &self.pubkey(),
            seed,
            self.solana_api
                .minimum_rent_for_exemption(HOLDER_SIZE)
                .await?,
            HOLDER_SIZE as u64,
            &self.program_id,
        );

        const TAG_IDX: usize = 0;
        const SEED_LEN_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const SEED_IDX: usize = SEED_LEN_IDX + mem::size_of::<u64>();
        let seed_len = seed.as_bytes().len();

        let mut data = vec![0; SEED_IDX + seed_len];
        data[TAG_IDX] = tag::HOLDER_CREATE;
        data[SEED_LEN_IDX..SEED_IDX].copy_from_slice(&(seed_len as u64).to_le_bytes());
        data[SEED_IDX..].copy_from_slice(seed.as_bytes());

        let accounts = vec![
            AccountMeta::new(key, false),
            AccountMeta::new_readonly(self.pubkey(), true),
        ];

        let neon_ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        Ok([sp_ix, neon_ix])
    }

    fn write_next_holder_chunk(&self, holder: &mut HolderInfo) -> Instruction {
        const CHUNK_LEN: usize = 930;

        if holder.is_empty() {
            panic!("attempt to write empty holder");
        }

        const TAG_IDX: usize = 0;
        const HASH_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const OFFSET_IDX: usize = HASH_IDX + mem::size_of::<B256>();
        const DATA_IDX: usize = OFFSET_IDX + mem::size_of::<u64>();

        let chunk_end = holder.data.len().min(holder.current_offset + CHUNK_LEN);
        let chunk_start = holder.current_offset;
        let chunk = &holder.data[chunk_start..chunk_end];
        holder.current_offset = chunk_end;

        let mut data = vec![0; DATA_IDX + chunk.len()];
        data[TAG_IDX] = tag::HOLDER_WRITE;
        data[HASH_IDX..OFFSET_IDX].copy_from_slice(holder.hash.as_slice());
        data[OFFSET_IDX..DATA_IDX].copy_from_slice(&chunk_start.to_le_bytes());
        data[DATA_IDX..].copy_from_slice(chunk);

        let accounts = vec![
            AccountMeta::new(holder.pubkey, false),
            AccountMeta::new(self.pubkey(), true),
        ];

        Instruction {
            program_id: self.program_id,
            accounts,
            data,
        }
    }
}

fn with_budget(ix: Instruction) -> [Instruction; 3] {
    // Taken from neon-proxy.py
    const MAX_HEAP_SIZE: u32 = 256 * 1024;
    const MAX_COMPUTE_UNITS: u32 = 1_400_000;

    let cu = ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS);
    let heap = ComputeBudgetInstruction::request_heap_frame(MAX_HEAP_SIZE);

    [cu, heap, ix]
}

fn get_neon_emulate_request(tx: &TxEnvelope) -> anyhow::Result<TxParams> {
    let from = tx
        .recover_signer()
        .context("could not recover signer")?
        .to_neon();
    let request = match &tx {
        TxEnvelope::Legacy(tx) => TxParams {
            nonce: Some(tx.tx().nonce),
            from,
            to: tx.tx().to.to().copied().map(ToNeon::to_neon),
            data: Some(tx.tx().input.0.to_vec()),
            value: Some(tx.tx().value.to_neon()),
            gas_limit: Some(tx.tx().gas_limit.into()),
            actual_gas_used: None,
            gas_price: Some(tx.tx().gas_price.into()),
            access_list: None,
            chain_id: tx.tx().chain_id,
        },
        TxEnvelope::Eip2930(tx) => TxParams {
            nonce: Some(tx.tx().nonce),
            from,
            to: tx.tx().to.to().copied().map(ToNeon::to_neon),
            data: Some(tx.tx().input.0.to_vec()),
            value: Some(tx.tx().value.to_neon()),
            gas_limit: Some(tx.tx().gas_limit.into()),
            actual_gas_used: None,
            gas_price: Some(tx.tx().gas_price.into()),
            access_list: Some(
                tx.tx()
                    .access_list
                    .iter()
                    .cloned()
                    .map(ToNeon::to_neon)
                    .collect(),
            ),
            chain_id: Some(tx.tx().chain_id),
        },
        tx => anyhow::bail!("unsupported transaction: {:?}", tx.tx_type()),
    };

    Ok(request)
}
