#[cfg(test)]
mod tests;
mod transactions;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_signer_wallet::LocalWallet;
use anyhow::Context;
use clap::Args;
use dashmap::DashMap;
use solana_api::solana_api::SolanaApi;
use tokio::sync::Notify;
use tokio::time::sleep;

use common::neon_lib::types::Address;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{Keypair, Signature};
use common::solana_sdk::signer::Signer;
use common::solana_sdk::transaction::TransactionError;
use common::solana_transaction_status::TransactionStatus;

use crate::neon_api::NeonApi;

use self::transactions::{OngoingTransaction, TransactionBuilder};

#[derive(Args, Clone)]
pub struct Config {
    #[arg(long)]
    /// Path to operator keypair
    pub operator_keypair: Option<PathBuf>,

    #[arg(long, requires = "operator_keypair")]
    /// Operator ETH address
    pub operator_address: Option<Address>,

    #[arg(long, default_value_t = false)]
    /// Initialize operator balance accounts at service startup
    pub init_operator_balance: bool,
}

pub struct Executor {
    program_id: Pubkey,
    solana_api: SolanaApi,

    builder: TransactionBuilder,
    pending_transactions: DashMap<Signature, OngoingTransaction>,
    notify: Notify,

    #[cfg(test)]
    test_ext: test_ext::TestExtension,
}

impl Executor {
    pub async fn initialize_and_start(
        neon_api: NeonApi,
        solana_api: SolanaApi,
        neon_pubkey: Pubkey,
        operator: Keypair,
        operator_addr: Option<Address>,
        init_balances: bool,
    ) -> anyhow::Result<(Arc<Self>, impl Future<Output = anyhow::Result<()>>)> {
        let operator_addr = match operator_addr {
            Some(addr) => addr,
            None => {
                let operator_signer = LocalWallet::from_slice(operator.secret().as_ref())?;
                operator_signer.address().0 .0.into()
            }
        };
        tracing::info!(sol_key = %operator.pubkey(), eth_key = %operator_addr, "executor operator keys");

        let this = Self::initialize(
            neon_api,
            solana_api,
            neon_pubkey,
            operator,
            operator_addr,
            init_balances,
        )
        .await?;
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
        init_balances: bool,
    ) -> anyhow::Result<Self> {
        let notify = Notify::new();

        let builder = TransactionBuilder::new(
            neon_pubkey,
            solana_api.clone(),
            neon_api.clone(),
            operator,
            operator_addr,
        )
        .await?;

        let this = Self {
            program_id: neon_pubkey,
            solana_api,
            builder,
            pending_transactions: DashMap::new(),
            notify,

            #[cfg(test)]
            test_ext: test_ext::TestExtension::new(),
        };

        if init_balances {
            for chain in this.builder.chains() {
                tracing::info!(name = chain.name, id = chain.id, "initializing balance");
                this.init_operator_balance(chain.id).await?;
            }
        }

        Ok(this)
    }

    pub async fn handle_transaction(
        &self,
        tx: TxEnvelope,
        default_chain_id: u64,
    ) -> anyhow::Result<Signature> {
        let tx = self.builder.start_execution(tx, default_chain_id).await?;

        self.init_operator_balance(tx.chain_id().unwrap_or(default_chain_id))
            .await
            .context("cannot init operator balance")?;

        let signature = self.sign_and_send_transaction(tx).await?;

        Ok(signature)
    }

    /// Sign, send and register transaction to be confirmed.
    /// The only method in this module that can call `send_transaction`
    /// or insert into `pending_transactions` map.
    async fn sign_and_send_transaction(&self, tx: OngoingTransaction) -> anyhow::Result<Signature> {
        let blockhash = self
            .solana_api
            .get_recent_blockhash()
            .await
            .context("could not request blockhash")?; // TODO: force confirmed

        // This will replace bh and clear signatures in case it's a retry
        let sol_tx = tx.sign(&[self.builder.keypair()], blockhash)?;

        let signature = self
            .solana_api
            .send_transaction(&sol_tx)
            .await
            .context("could not send transaction")?;

        tracing::info!(%signature, ?tx, "sent new transaction");
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
                #[cfg(test)]
                self.test_ext.notify.notify_waiters();

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
                    tracing::error!(%signature, "missing pending transaction data");
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

        #[cfg(test)]
        self.test_ext.add(signature);
    }

    async fn handle_expired_transaction(&self, signature: Signature, tx: OngoingTransaction) {
        let tx_hash = tx.eth_tx().map(|tx| *tx.tx_hash());
        // TODO: retry counter
        tracing::warn!(?tx_hash, %signature, "transaction blockhash expired, retrying");
        if let Err(error) = self.sign_and_send_transaction(tx).await {
            tracing::error!(?tx_hash, %signature, %error, "failed retrying transaction");
        }
    }

    async fn handle_success(&self, signature: Signature, tx: OngoingTransaction, slot: u64) {
        let tx_hash = tx.eth_tx().map(|tx| *tx.tx_hash());
        // TODO: maybe add Instant to ongoing transaction.
        tracing::info!(?tx_hash, %signature, slot, "transaction was confirmed");

        // TODO: follow up transactions
        match self.builder.next_step(tx).await {
            Err(err) => {
                tracing::error!(?tx_hash, %signature, %err, "failed executing next transaction step")
            }
            Ok(Some(tx)) => {
                if let Err(err) = self.sign_and_send_transaction(tx).await {
                    tracing::error!(%signature, ?tx_hash, %err, "failed sending transaction next step");
                }
            }
            Ok(None) => (),
        }
    }

    async fn handle_error(
        &self,
        signature: Signature,
        tx: OngoingTransaction,
        slot: u64,
        err: TransactionError,
    ) {
        let tx_hash = tx.eth_tx().map(|tx| *tx.tx_hash());
        tracing::warn!(?tx_hash, %signature, slot, %err, "transaction was confirmed, but failed");

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

        tracing::info!(chain_id, "initializing operator balance");
        let tx = self.builder.init_operator_balance(chain_id);
        let signature = self.sign_and_send_transaction(tx).await?;

        Ok(Some(signature))
    }

    #[cfg(test)]
    async fn join_current_transactions(&self) -> Vec<Signature> {
        self.test_ext.take().await
    }
}

#[cfg(test)]
mod test_ext {
    use std::{mem, sync::Mutex};

    use super::*;

    pub struct TestExtension {
        pub notify: Notify,
        pub signatures: Mutex<Vec<Signature>>,
    }

    impl TestExtension {
        pub fn new() -> Self {
            Self {
                notify: Notify::new(),
                signatures: Mutex::new(Vec::new()),
            }
        }

        pub fn add(&self, signature: Signature) {
            self.signatures.lock().unwrap().push(signature);
        }

        pub async fn take(&self) -> Vec<Signature> {
            self.notify.notified().await;
            mem::take(self.signatures.lock().unwrap().as_mut())
        }
    }
}
