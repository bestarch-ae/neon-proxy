use std::mem;
use std::sync::Arc;

use alloy_consensus::TxEnvelope;
use alloy_rlp::Encodable;
use anyhow::Context;

use common::ethnum::U256;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::types::{Address, TxParams};
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;
use solana_api::solana_api::SolanaApi;

use crate::convert::ToNeon;
use crate::neon_api::NeonApi;

#[derive(Clone)]
pub struct Executor {
    program_id: Pubkey,

    neon_api: NeonApi,
    solana_api: SolanaApi,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,

    operator: Arc<Keypair>,
    operator_addr: Address,
}

impl Executor {
    pub async fn initialize(
        neon_api: NeonApi,
        solana_api: SolanaApi,
        neon_pubkey: Pubkey,
        operator: Keypair,
        operator_addr: Address,
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

        Ok(Self {
            program_id: neon_pubkey,
            neon_api,
            solana_api,
            treasury_pool_count,
            treasury_pool_seed,
            operator: Arc::new(operator),
            operator_addr,
        })
    }

    pub async fn handle_transaction(&self, tx: TxEnvelope) -> anyhow::Result<()> {
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
        let chain_id = request.chain_id.context("unknown chain id")?; // FIXME
        self.init_operator_balance(chain_id)
            .await
            .context("cannot init operator balance")?;

        let emulate_result = self
            .neon_api
            .emulate(request)
            .await
            .context("could not emulate transaction")?;

        let ix = self.build_simple(tx, emulate_result, chain_id)?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.operator.pubkey()));
        let blockhash = self
            .solana_api
            .get_recent_blockhash()
            .await
            .context("could not request blockhash")?; // TODO: force confirmed

        tx.try_sign(&[&self.operator], blockhash)
            .context("could not sign request")?;
        self.solana_api
            .send_transaction(&tx)
            .await
            .context("could not send transaction")?;

        Ok(())
    }

    fn build_simple(
        &self,
        tx: TxEnvelope,
        emulate: EmulateResponse,
        chain_id: u64,
    ) -> anyhow::Result<Instruction> {
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
        let mut data = Vec::with_capacity(data_len);

        data[0] = tag::TX_EXEC_FROM_DATA;
        data[1..1 + mem::size_of_val(&treasury_pool_idx)]
            .copy_from_slice(&treasury_pool_idx.to_le_bytes());
        tx.encode(&mut &mut data[(1 + mem::size_of::<u32>())..]);

        let ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };
        Ok(ix)
    }

    async fn init_operator_balance(&self, chain_id: u64) -> anyhow::Result<()> {
        let addr = self.operator_balance(chain_id);
        if let Some(acc) = self
            .solana_api
            .get_account(&addr)
            .await
            .context("cannot request balance acc")?
        {
            if acc.owner != self.program_id {
                anyhow::bail!("operator balance account ({addr}) exists, but hash invalid owner");
            }
            return Ok(());
        }

        const TAG_IDX: usize = 0;
        const ADDR_IDX: usize = TAG_IDX;
        const CHAIN_ID_IDX: usize = ADDR_IDX + mem::size_of::<Address>();
        const DATA_LEN: usize = CHAIN_ID_IDX + mem::size_of::<u64>();

        let mut data = Vec::with_capacity(DATA_LEN);
        data[TAG_IDX] = tag::OPERATOR_BALANCE_CREATE;
        data[ADDR_IDX..CHAIN_ID_IDX].copy_from_slice(&self.operator_addr.0);
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
        let blockhash = self.solana_api.get_recent_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.operator.pubkey()));
        tx.try_sign(&[&self.operator], blockhash)
            .context("failed signing create balance transaction")?;

        self.solana_api
            .send_transaction(&tx)
            .await
            .context("failed create balance transaction send")?;

        Ok(())
    }

    fn operator_balance(&self, chain_id: u64) -> Pubkey {
        let chain_id = U256::from(chain_id);
        let opkey = self.operator.pubkey();

        let seeds: &[&[u8]] = &[
            &[ACCOUNT_SEED_VERSION],
            opkey.as_ref(),
            &self.operator_addr.0,
            &chain_id.to_be_bytes(),
        ];
        Pubkey::find_program_address(seeds, &self.program_id).0
    }
}
