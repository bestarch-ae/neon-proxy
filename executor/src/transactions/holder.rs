use std::fmt;
use std::mem;
use std::sync::atomic::Ordering;

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Encodable2718;
use anyhow::Context;
use reth_primitives::B256;

use common::evm_loader::account;
use common::neon_instruction::tag;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::system_instruction;

use super::TransactionBuilder;

const HOLDER_DATA_LEN: usize = 256 * 1024; // neon_proxy.py default
const HOLDER_META_LEN: usize =
    account::ACCOUNT_PREFIX_LEN + mem::size_of::<account::HolderHeader>();
const HOLDER_SIZE: usize = HOLDER_META_LEN + HOLDER_DATA_LEN;

pub(super) struct HolderInfo {
    seed: String,
    pubkey: Pubkey,
    data: Vec<u8>,
    current_offset: usize,
    hash: B256,
}

impl fmt::Debug for HolderInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HolderInfo")
            .field("seed", &self.seed)
            .field("pubkey", &self.pubkey)
            .field("data", &hex::encode(&self.data))
            .field("current_offset", &self.current_offset)
            .field("hash", &self.hash)
            .finish()
    }
}

impl HolderInfo {
    pub fn is_empty(&self) -> bool {
        self.current_offset >= self.data.len()
    }

    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }

    pub fn offset(&self) -> usize {
        self.current_offset
    }

    pub fn hash(&self) -> &B256 {
        &self.hash
    }
}

impl TransactionBuilder {
    pub(super) fn new_holder_info(&self, tx: Option<&TxEnvelope>) -> anyhow::Result<HolderInfo> {
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

    pub(super) async fn create_holder(
        &self,
        holder: &HolderInfo,
    ) -> anyhow::Result<[Instruction; 2]> {
        let sp_ix = system_instruction::create_account_with_seed(
            &self.pubkey(),
            &holder.pubkey,
            &self.pubkey(),
            &holder.seed,
            self.solana_api
                .minimum_rent_for_exemption(HOLDER_SIZE)
                .await?,
            HOLDER_SIZE as u64,
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

    pub(super) fn write_next_holder_chunk(&self, holder: &mut HolderInfo) -> Instruction {
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
