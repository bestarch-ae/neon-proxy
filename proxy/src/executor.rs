#[cfg(test)]
mod tests;
mod transactions;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use anyhow::Context;
use dashmap::DashMap;
use solana_api::solana_api::SolanaApi;
use tokio::sync::Notify;
use tokio::time::sleep;

use common::neon_lib::types::{Address, TxParams};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::transaction::TransactionError;
use common::solana_transaction_status::TransactionStatus;

use crate::convert::ToNeon;
use crate::neon_api::NeonApi;

use self::transactions::{OngoingTransaction, TransactionBuilder};

pub struct Executor {
    program_id: Pubkey,

    neon_api: NeonApi,
    solana_api: SolanaApi,

    builder: TransactionBuilder,

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

        let builder = TransactionBuilder::new(
            neon_pubkey,
            solana_api.clone(),
            operator,
            operator_addr,
            treasury_pool_count,
            treasury_pool_seed,
        );

        Ok(Self {
            program_id: neon_pubkey,
            neon_api,
            solana_api,
            builder,
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

        let tx = self
            .builder
            .start_execution(tx, emulate_result, chain_id)
            .await?;
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
        let sol_tx = tx.sign(&[self.builder.keypair()], blockhash)?;

        let signature = self
            .solana_api
            .send_transaction(sol_tx)
            .await
            .context("could not send transaction")?;

        tracing::info!(%signature, tx = ?tx.eth_tx(), "sent new transaction");
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
            let hash = *bail_if_absent!(self.pending_transactions.get(&signature)).blockhash();
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

    async fn handle_success(&self, signature: Signature, tx: OngoingTransaction, slot: u64) {
        // TODO: maybe add Instant to ongoing transaction.
        tracing::info!(%signature, slot, "transaction was confirmed");

        // TODO: follow up transactions
        let hash = tx.eth_tx().map(|tx| tx.signature_hash());
        match self.builder.next_step(tx) {
            Err(err) => {
                tracing::error!(?hash, %signature, %err, "failed executing next transaction step")
            }
            Ok(Some(tx)) => {
                if let Err(err) = self.sign_and_send_transaction(tx).await {
                    tracing::error!(%signature, ?hash, %err, "failed sending transaction next step");
                }
            }
            Ok(None) => (),
        }
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

    async fn init_operator_balance(&self, chain_id: u64) -> anyhow::Result<Option<Signature>> {
        let addr = self.builder.operator_balance(chain_id);
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

        let tx = self.builder.init_operator_balance(chain_id);
        let signature = self.sign_and_send_transaction(tx).await?;

        Ok(Some(signature))
    }
}
