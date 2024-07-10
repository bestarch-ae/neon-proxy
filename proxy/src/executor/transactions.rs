use std::mem;

use alloy_consensus::TxEnvelope;
use alloy_rlp::Encodable;
use anyhow::Context;

use common::ethnum::U256;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::types::Address;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;

#[derive(Debug, Clone)]
pub struct OngoingTransaction {
    // holder_idx: Option<usize>,
    eth_tx: Option<TxEnvelope>,
    tx: Transaction,
    blockhash: Hash,
}

impl OngoingTransaction {
    fn new_eth(eth_tx: TxEnvelope, ixs: &[Instruction], payer: &Pubkey) -> Self {
        Self {
            eth_tx: Some(eth_tx),
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            blockhash: Hash::default(),
        }
    }

    fn new_operational(ixs: &[Instruction], payer: &Pubkey) -> Self {
        Self {
            eth_tx: None,
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            blockhash: Hash::default(),
        }
    }

    pub fn eth_tx(&self) -> Option<&TxEnvelope> {
        self.eth_tx.as_ref()
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

pub struct TransactionBuilder {
    program_id: Pubkey,

    operator: Keypair,
    operator_address: Address,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,
}

impl TransactionBuilder {
    pub fn new(
        program_id: Pubkey,
        operator: Keypair,
        address: Address,
        treasury_pool_count: u32,
        treasury_pool_seed: Vec<u8>,
    ) -> Self {
        Self {
            program_id,
            operator,
            operator_address: address,
            treasury_pool_count,
            treasury_pool_seed,
        }
    }

    pub fn keypair(&self) -> &Keypair {
        &self.operator
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

    pub fn build_simple(
        &self,
        tx: TxEnvelope,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<OngoingTransaction> {
        let base_idx =
            u32::from_le_bytes(*tx.tx_hash().0.first_chunk().expect("B256 is longer than 4"));
        let treasury_pool_idx = base_idx % self.treasury_pool_count;
        let (treasury_pool_address, _seed) = Pubkey::try_find_program_address(
            &[&self.treasury_pool_seed, &treasury_pool_idx.to_le_bytes()],
            &self.program_id,
        )
        .context("cannot find program address")?;

        let operator_balance = self.operator_balance(chain_id);

        let mut accounts = vec![
            AccountMeta::new(self.operator.pubkey(), true),
            AccountMeta::new(treasury_pool_address, false),
            AccountMeta::new(operator_balance, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];
        accounts.extend(emulate.solana_accounts.into_iter().map(|acc| AccountMeta {
            pubkey: acc.pubkey,
            is_writable: acc.is_writable,
            is_signer: false,
        }));

        let data_len = mem::size_of::<u8>() // Tag
            + mem::size_of::<u32>()
            + tx.length();
        let mut data = vec![0; data_len];

        data[0] = tag::TX_EXEC_FROM_DATA;
        data[1..1 + mem::size_of_val(&treasury_pool_idx)]
            .copy_from_slice(&treasury_pool_idx.to_le_bytes());
        tx.encode(&mut &mut data[(1 + mem::size_of::<u32>())..]);

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        Ok(OngoingTransaction::new_eth(
            tx,
            &[ix],
            &self.operator.pubkey(),
        ))
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

        OngoingTransaction::new_operational(&[ix], &self.operator.pubkey())
    }
}
