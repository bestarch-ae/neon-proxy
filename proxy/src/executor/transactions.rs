use std::mem;
use std::sync::atomic::{AtomicU8, Ordering};

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use anyhow::{bail, Context};
use reth_primitives::B256;

use common::ethnum::U256;
use common::evm_loader::account;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::{EmulateResponse, SolanaAccount as NeonSolanaAccount};
use common::neon_lib::types::Address;
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

#[derive(Debug)]
pub struct OngoingTransaction {
    holder: Option<HolderInfo>,
    eth_tx: Option<TxData>,
    tx: Transaction,
    blockhash: Hash,
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
    fn new_eth(eth_tx: TxData, ixs: &[Instruction], payer: &Pubkey, chain_id: u64) -> Self {
        Self {
            holder: None,
            eth_tx: Some(eth_tx),
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            blockhash: Hash::default(),
            chain_id,
        }
    }

    fn new_operational(ixs: &[Instruction], payer: &Pubkey, chain_id: u64) -> Self {
        Self {
            holder: None,
            eth_tx: None,
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            blockhash: Hash::default(),
            chain_id,
        }
    }

    pub fn eth_tx(&self) -> Option<&TxEnvelope> {
        self.eth_tx.as_ref().map(|data| &data.envelope)
    }

    pub fn blockhash(&self) -> &Hash {
        &self.blockhash // TODO: None if default?
    }

    pub fn sign(&mut self, signers: &[&Keypair], blockhash: Hash) -> anyhow::Result<&Transaction> {
        self.tx
            .try_sign(signers, blockhash)
            .context("could not sign transactions")?;
        Ok(&self.tx)
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

    operator: Keypair,
    operator_address: Address,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,

    holder_counter: AtomicU8,
}

impl TransactionBuilder {
    pub fn new(
        program_id: Pubkey,
        solana_api: SolanaApi,
        operator: Keypair,
        address: Address,
        treasury_pool_count: u32,
        treasury_pool_seed: Vec<u8>,
    ) -> Self {
        Self {
            program_id,
            solana_api,
            operator,
            operator_address: address,
            treasury_pool_count,
            treasury_pool_seed,
            holder_counter: AtomicU8::new(0),
        }
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

    pub async fn start_execution(
        &self,
        tx: TxEnvelope,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        // 1. holder
        // 2. payer
        // 3. treasury-pool-address,
        // 4. payer-token-address
        // 5. SolSysProg.ID
        // +6: NeonProg.ID
        // +7: CbProg.ID
        const BASE_ACCOUNT_COUNT: usize = 7;

        if tx.length() < PACKET_DATA_SIZE - BASE_ACCOUNT_COUNT * mem::size_of::<Pubkey>() {
            self.build_simple(tx, emulate, chain_id)
        } else {
            self.start_holder_execution(tx, emulate, chain_id).await
        }
    }

    pub fn next_step(&self, tx: OngoingTransaction) -> anyhow::Result<Option<OngoingTransaction>> {
        match (tx.holder, tx.eth_tx) {
            (Some(holder), Some(data)) if holder.is_empty() => self
                .execute_from_holder(holder, data.emulate, tx.chain_id)
                .map(Some),
            (Some(mut holder), eth_tx @ Some(_)) => {
                let ix = self.write_next_holder_chunk(&mut holder);
                let tx = OngoingTransaction {
                    holder: Some(holder),
                    eth_tx,
                    tx: Transaction::new_with_payer(&[ix], Some(&self.pubkey())),
                    blockhash: Hash::default(),
                    chain_id: tx.chain_id,
                };
                Ok(Some(tx))
            }
            (Some(holder), None) if holder.is_empty() => Ok(None), // Holder execution finished
            (Some(holder), None) => {
                bail!("invalid tx state, holder is not empty, but tx data missing: {holder:?}")
            }
            // Currently we support only holder related multi-tx stuff
            (None, _) => Ok(None),
        }
    }

    fn build_simple(
        &self,
        tx: TxEnvelope,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let (accounts, mut data) = self.execute_simple_base(
            tag::TX_EXEC_FROM_DATA,
            tx.tx_hash(),
            chain_id,
            emulate.solana_accounts.clone(), // TODO: Do we really need them later?
            None,
        )?;

        data.reserve(tx.encode_2718_len());
        tx.encode_2718(&mut &mut data);

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        Ok(OngoingTransaction::new_eth(
            TxData::new(tx, emulate),
            &with_budget(ix),
            &self.operator.pubkey(),
            chain_id,
        ))
    }

    fn execute_from_holder(
        &self,
        holder: HolderInfo,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let (accounts, data) = self.execute_simple_base(
            tag::TX_EXEC_FROM_ACCOUNT,
            &holder.hash,
            chain_id,
            emulate.solana_accounts,
            Some(holder.pubkey),
        )?;

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        Ok(OngoingTransaction {
            holder: Some(holder),
            eth_tx: None,
            tx: Transaction::new_with_payer(&with_budget(ix), Some(&self.pubkey())),
            blockhash: Hash::default(),
            chain_id,
        })
    }

    fn execute_simple_base(
        &self,
        tag: u8,
        hash: &B256,
        chain_id: u64,
        collected_accounts: Vec<NeonSolanaAccount>,
        holder: Option<Pubkey>,
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
        accounts.extend(collected_accounts.into_iter().map(|acc| AccountMeta {
            pubkey: acc.pubkey,
            is_writable: acc.is_writable,
            is_signer: false,
        }));

        const TAG_IDX: usize = 0;
        const TREASURY_IDX_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const DATA_IDX: usize = TREASURY_IDX_IDX + mem::size_of::<u32>();
        let mut data = vec![0; DATA_IDX];

        data[TAG_IDX] = tag;
        data[TREASURY_IDX_IDX..DATA_IDX].copy_from_slice(&treasury_pool_idx.to_le_bytes());

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

    async fn start_holder_execution(
        &self,
        tx: TxEnvelope,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let holder =
            self.new_holder_info(self.holder_counter.fetch_add(1, Ordering::Relaxed), &tx)?;
        let ixs = self.create_holder(&holder).await?;

        Ok(OngoingTransaction {
            holder: Some(holder),
            eth_tx: Some(TxData::new(tx, emulate)),
            tx: Transaction::new_with_payer(&ixs, Some(&self.pubkey())),
            blockhash: Hash::default(),
            chain_id,
        })
    }

    fn new_holder_info(&self, idx: u8, tx: &TxEnvelope) -> anyhow::Result<HolderInfo> {
        let seed = format!("holder{idx}");
        let pubkey = Pubkey::create_with_seed(&self.pubkey(), &seed, &self.program_id)
            .context("cannot create holder address")?;
        let hash = *tx.tx_hash();
        let mut data = Vec::with_capacity(tx.encode_2718_len());
        tx.encode_2718(&mut &mut data);

        Ok(HolderInfo {
            seed,
            pubkey,
            data,
            current_offset: 0,
            hash,
        })
    }

    async fn create_holder(&self, holder: &HolderInfo) -> anyhow::Result<[Instruction; 2]> {
        const HOLDER_META_LEN: usize =
            account::ACCOUNT_PREFIX_LEN + mem::size_of::<account::HolderHeader>();
        let holder_size = HOLDER_META_LEN + holder.data.len();
        let sp_ix = system_instruction::create_account_with_seed(
            &self.pubkey(),
            &holder.pubkey,
            &self.pubkey(),
            &holder.seed,
            self.solana_api
                .minimum_rent_for_exemption(holder_size)
                .await?,
            holder_size as u64,
            &self.program_id,
        );

        const TAG_IDX: usize = 0;
        const SEED_LEN_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const SEED_IDX: usize = SEED_LEN_IDX + mem::size_of::<u64>();
        let seed_len = holder.seed.as_bytes().len();

        let mut data = vec![0; SEED_IDX + seed_len];
        data[TAG_IDX] = tag::HOLDER_CREATE;
        data[SEED_LEN_IDX..SEED_IDX].copy_from_slice(&(seed_len as u64).to_le_bytes());
        data[SEED_IDX..].copy_from_slice(holder.seed.as_bytes());

        let accounts = vec![
            AccountMeta::new(holder.pubkey, false),
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
