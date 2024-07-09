#[cfg(test)]
mod tests;

use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_rlp::Encodable;
use anyhow::Context;
use dashmap::DashMap;
use solana_api::solana_api::SolanaApi;
use tokio::sync::Notify;
use tokio::time::sleep;

use common::ethnum::U256;
use common::evm_loader::config::ACCOUNT_SEED_VERSION;
use common::neon_instruction::tag;
use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::types::{Address, TxParams};
use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::system_program;
use common::solana_sdk::transaction::Transaction;
use common::solana_sdk::transaction::TransactionError;
use common::solana_transaction_status::TransactionStatus;

use crate::convert::ToNeon;
use crate::neon_api::NeonApi;

pub struct Executor {
    program_id: Pubkey,

    neon_api: NeonApi,
    solana_api: SolanaApi,

    treasury_pool_count: u32,
    treasury_pool_seed: Vec<u8>,

    operator: Keypair,
    operator_addr: Address,

    pending_transactions: DashMap<Signature, OngoingTransaction>,
    notify: Notify,
}

impl Executor {
    pub async fn initialize_and_start(
        neon_api: NeonApi,
        solana_api: SolanaApi,
        neon_pubkey: Pubkey,
        operator: Keypair,
        operator_addr: Address, // TODO: derive from keypair
    ) -> anyhow::Result<(Arc<Self>, impl Future<Output = anyhow::Result<()>>)> {
        let this =
            Self::initialize(neon_api, solana_api, neon_pubkey, operator, operator_addr).await?;
        let this = Arc::new(this);
        let this_to_run = this.clone();
        Ok((this, this_to_run.run()))
    }

    async fn initialize(
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
        let notify = Notify::new();

        Ok(Self {
            program_id: neon_pubkey,
            neon_api,
            solana_api,
            treasury_pool_count,
            treasury_pool_seed,
            operator,
            operator_addr,
            pending_transactions: DashMap::new(),
            notify,
        })
    }

    pub async fn handle_transaction(&self, tx: TxEnvelope) -> anyhow::Result<Signature> {
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

        // TODO: store in ongoing transaction
        let emulate_result = self
            .neon_api
            .emulate(request)
            .await
            .context("could not emulate transaction")?;

        let ix = self.build_simple(&tx, emulate_result, chain_id)?;
        let tx = OngoingTransaction::new_eth(tx, &[ix], &self.operator.pubkey());
        let signature = self.sign_and_send_transaction(tx).await?;

        Ok(signature)
    }

    /// Sign, send and register transaction to be confirmed.
    /// The only method in this module that can call `send_transaction`
    /// or insert into `pending_transactions` map.
    async fn sign_and_send_transaction(&self, tx: OngoingTransaction) -> anyhow::Result<Signature> {
        let mut tx = tx;
        let blockhash = self
            .solana_api
            .get_recent_blockhash()
            .await
            .context("could not request blockhash")?; // TODO: force confirmed

        // This will replace bh and clear signatures in case it's a retry
        tx.tx
            .try_sign(&[&self.operator], blockhash)
            .context("could not sign request")?;

        let signature = self
            .solana_api
            .send_transaction(&tx.tx)
            .await
            .context("could not send transaction")?;

        tracing::info!(%signature, tx = ?tx.eth_tx, "sent new transaction");
        let do_notify = self.pending_transactions.is_empty();
        self.pending_transactions.insert(signature, tx); // TODO: check none?
        if do_notify {
            self.notify.notify_waiters();
        }

        Ok(signature)
    }

    async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let mut signatures = Vec::new();
        loop {
            if self.pending_transactions.is_empty() {
                self.notify.notified().await;
            } else {
                sleep(POLL_INTERVAL).await;
            }

            signatures.clear();
            let current_len = self.pending_transactions.len();
            if current_len > signatures.capacity() {
                signatures.reserve(current_len - signatures.capacity());
            }
            for tx in self.pending_transactions.iter() {
                signatures.push(*tx.key());
            }

            // TODO: request finalized
            let result = match self.solana_api.get_signature_statuses(&signatures).await {
                Err(err) => {
                    tracing::warn!(%err, "could not request signature statuses");
                    continue;
                }
                Ok(res) => res,
            };

            for (signature, status) in signatures.drain(..).zip(result) {
                self.handle_signature_status(signature, status).await;
            }
        }
    }

    async fn handle_signature_status(
        &self,
        signature: Signature,
        status: Option<TransactionStatus>,
    ) {
        macro_rules! bail_if_absent {
            ($kv:expr) => {
                if let Some(value) = $kv {
                    value
                } else {
                    tracing::warn!(%signature, "missing pending transaction data");
                    return;
                }
            }
        }

        let Some(status) = status else {
            let hash = bail_if_absent!(self.pending_transactions.get(&signature)).blockhash;
            let is_valid = self
                .solana_api
                .is_blockhash_valid(&hash)
                .await
                .inspect_err(|err| tracing::warn!(%err, "could not check blockhash validity"))
                .unwrap_or(true);
            if !is_valid {
                let (_, tx) = bail_if_absent!(self.pending_transactions.remove(&signature));
                self.handle_expired_transaction(signature, tx).await;
            }
            return;
        };

        let (_, tx) = bail_if_absent!(self.pending_transactions.remove(&signature));
        let slot = status.slot;
        if let Some(err) = status.err {
            self.handle_error(signature, tx, slot, err).await;
        } else {
            self.handle_success(signature, tx, slot).await;
        }
    }

    async fn handle_expired_transaction(&self, signature: Signature, tx: OngoingTransaction) {
        // TODO: retry counter
        tracing::warn!(%signature, "transaction blockhash expired, retrying");
        if let Err(error) = self.sign_and_send_transaction(tx).await {
            tracing::error!(%signature, %error, "failed retrying transaction");
        }
    }

    async fn handle_success(&self, signature: Signature, _tx: OngoingTransaction, slot: u64) {
        // TODO: maybe add Instant to ongoing transaction.
        tracing::info!(%signature, slot, "transaction was confirmed");

        // TODO: follow up transactions
    }

    async fn handle_error(
        &self,
        signature: Signature,
        _tx: OngoingTransaction,
        slot: u64,
        err: TransactionError,
    ) {
        tracing::warn!(%signature, slot, %err, "transaction was confirmed, but failed");

        // TODO: do we retry?
        // TODO: do we request logs?
    }

    fn build_simple(
        &self,
        tx: &TxEnvelope,
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
        Ok(ix)
    }

    async fn init_operator_balance(&self, chain_id: u64) -> anyhow::Result<Option<Signature>> {
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
            return Ok(None);
        }

        const TAG_IDX: usize = 0;
        const ADDR_IDX: usize = TAG_IDX + 1;
        const CHAIN_ID_IDX: usize = ADDR_IDX + mem::size_of::<Address>();
        const DATA_LEN: usize = CHAIN_ID_IDX + mem::size_of::<u64>();

        let mut data = vec![0; DATA_LEN];
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
        let tx = OngoingTransaction::new_operational(&[ix], &self.operator.pubkey());
        let signature = self.sign_and_send_transaction(tx).await?;

        Ok(Some(signature))
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

#[derive(Debug, Clone)]
struct OngoingTransaction {
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
}
