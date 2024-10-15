#[cfg(test)]
mod tests;
mod transactions;

use std::future::Future;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEnvelope;
use alloy_rlp::Decodable;
use alloy_signer_wallet::LocalWallet;
use anyhow::{anyhow, bail, Context};
use arc_swap::access::Access;
use clap::Args;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::time::sleep;

use common::neon_lib::types::Address;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::{Keypair, Signature};
use common::solana_sdk::signer::{EncodableKey, Signer};
use common::solana_sdk::transaction::TransactionError;
use common::solana_sdk::transaction::VersionedTransaction;
use common::solana_transaction_status::TransactionStatus;
use neon_api::NeonApi;
use solana_api::solana_api::SolanaApi;
use tracing::info;

use self::transactions::{OngoingTransaction, TransactionBuilder, TxErrorKind};

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

    #[arg(long, default_value_t = u8::MAX)]
    /// Maximum holder accounts
    pub max_holders: u8,
}

#[derive(Debug)]
pub struct ExecuteRequest {
    pub(crate) tx: TxEnvelope,
    pub(crate) fallback_chain_id: u64,
}

impl Deref for ExecuteRequest {
    type Target = TxEnvelope;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl ExecuteRequest {
    pub fn new(tx: TxEnvelope, fallback_chain_id: u64) -> Self {
        Self {
            tx,
            fallback_chain_id,
        }
    }

    pub fn from_bytes(bytes: impl AsRef<[u8]>, fallback_chain_id: u64) -> alloy_rlp::Result<Self> {
        let bytes_ref: &mut &[u8] = &mut bytes.as_ref();
        let tx = TxEnvelope::decode(bytes_ref)?;
        Ok(Self::new(tx, fallback_chain_id))
    }
}

#[derive(Debug)]
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
        config: Config,
        pg_pool: Option<db::PgPool>,
    ) -> anyhow::Result<(Arc<Self>, impl Future<Output = anyhow::Result<()>>)> {
        let Some(keypair_path) = config.operator_keypair else {
            bail!("missing operator keypair");
        };
        let operator = Keypair::read_from_file(&keypair_path).map_err(|err| anyhow!("{err}"))?;

        let address = match config.operator_address {
            Some(addr) => addr,
            None => {
                let operator_signer = LocalWallet::from_slice(operator.secret().as_ref())?;
                operator_signer.address().0 .0.into()
            }
        };
        tracing::info!(sol_key = %operator.pubkey(), eth_key = %address, "executor operator keys");

        let tx_builder_config = transactions::Config {
            program_id: neon_pubkey,
            operator,
            address,
            max_holders: config.max_holders,
            pg_pool,
        };

        let this = Self::initialize(
            neon_api,
            solana_api,
            tx_builder_config,
            config.init_operator_balance,
        )
        .await?;
        let this = Arc::new(this);
        let this_to_run = this.clone();
        Ok((this, this_to_run.run()))
    }

    async fn initialize(
        neon_api: NeonApi,
        solana_api: SolanaApi,
        config: transactions::Config,
        init_balances: bool,
    ) -> anyhow::Result<Self> {
        info!(operator = %config.operator.pubkey(), "started executor initialization");
        let notify = Notify::new();

        let program_id = config.program_id;
        let builder = TransactionBuilder::new(solana_api.clone(), neon_api.clone(), config).await?;
        let builder = builder;

        let mut this = Self {
            program_id,
            solana_api,
            builder,
            pending_transactions: DashMap::new(),
            notify,

            #[cfg(test)]
            test_ext: test_ext::TestExtension::new(),
        };

        if init_balances {
            for chain in this.builder.chains().load().deref() {
                tracing::info!(name = chain.name, id = chain.id, "initializing balance");
                this.init_operator_balance(chain.id).await?;
            }
        }

        // Recovery
        for tx in this.builder.recover().await? {
            this.sign_and_send_transaction(tx).await?;
        }

        info!(operator = %this.builder.pubkey(), "finished executor initialization");
        Ok(this)
    }

    pub async fn reload_config(&self) -> anyhow::Result<()> {
        self.builder.reload_config().await
    }

    pub async fn handle_transaction(&self, tx: ExecuteRequest) -> anyhow::Result<Signature> {
        let fallback_chain_id = tx.fallback_chain_id;
        let ongoing = self.builder.start_execution(tx).await?;

        self.init_operator_balance(ongoing.chain_id().unwrap_or(fallback_chain_id))
            .await
            .context("cannot init operator balance")?;

        let signature = self.sign_and_send_transaction(ongoing).await?;

        Ok(signature)
    }

    /// Sign, send and register transaction to be confirmed.
    /// The only method in this module that can call `send_transaction`
    /// or insert into `pending_transactions` map.
    async fn sign_and_send_transaction(&self, tx: OngoingTransaction) -> anyhow::Result<Signature> {
        let mut tx = tx;

        let sol_tx = self.sign_tx(&tx).await?;
        tracing::debug!(?sol_tx, "sending transaction");
        let signature = match self
            .solana_api
            .send_transaction(&sol_tx)
            .await
            .map_err(|err| (TxErrorKind::from_error(&err, &tx), err))
        {
            Ok(sign) => sign,
            Err((None, err)) => return Err(err.into()),
            Err((Some(err_kind), err)) => {
                let tx_hash = tx.eth_tx().map(|tx| tx.tx_hash()).copied();
                tx = self.builder.retry(tx, err_kind).await.map_err(|new_err| {
                    anyhow!("Transaction {tx_hash:?} cannot be retried: {new_err}, Initial error: {err}")
                })?;
                let sol_tx = self.sign_tx(&tx).await?;
                tracing::debug!(?sol_tx, "sending transaction");
                self.solana_api
                    .send_transaction(&sol_tx)
                    .await
                    .context("cannot send retried transaction: {err:?}")?
            }
        };

        tracing::info!(%signature, ?tx, "sent new transaction");
        let do_notify = self.pending_transactions.is_empty();
        self.pending_transactions.insert(signature, tx); // TODO: check none?
        if do_notify {
            self.notify.notify_waiters();
        }

        Ok(signature)
    }

    async fn sign_tx(&self, tx: &OngoingTransaction) -> anyhow::Result<VersionedTransaction> {
        let blockhash = self
            .solana_api
            .get_recent_blockhash()
            .await
            .context("could not request blockhash")?; // TODO: force confirmed

        // This will replace bh and clear signatures in case it's a retry
        tx.sign(&[self.builder.keypair()], blockhash)
    }

    async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        info!(operator = %self.builder.pubkey(), "started executor task");
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
                    tracing::warn!(?err, "could not request signature statuses");
                    continue;
                }
                Ok(res) => res,
            };

            for (signature, status) in signatures.drain(..).zip(result) {
                self.handle_signature_status(signature, status).await;
            }

            #[cfg(test)]
            if self.check_stop_after() {
                return Ok(());
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
                .inspect_err(|err| tracing::warn!(?err, "could not check blockhash validity"))
                .unwrap_or(true);
            if !is_valid {
                let (_, tx) = bail_if_absent!(self.pending_transactions.remove(&signature));
                self.handle_expired_transaction(signature, tx).await;
            }
            return;
        };

        let (_, tx) = bail_if_absent!(self.pending_transactions.remove(&signature));
        let slot = status.slot;

        #[cfg(test)]
        self.test_ext.add(signature);

        if let Some(err) = status.err {
            self.handle_error(signature, tx, slot, err).await;
        } else {
            self.handle_success(signature, tx, slot).await;
        }
    }

    async fn handle_expired_transaction(&self, signature: Signature, tx: OngoingTransaction) {
        let tx_hash = tx.eth_tx().map(|tx| *tx.tx_hash());
        // TODO: retry counter
        tracing::warn!(?tx_hash, %signature, "transaction blockhash expired, retrying");
        if let Err(error) = self.sign_and_send_transaction(tx).await {
            tracing::error!(?tx_hash, %signature, ?error, "failed retrying transaction");
        }
    }

    async fn handle_success(&self, signature: Signature, tx: OngoingTransaction, slot: u64) {
        let tx_hash = tx.eth_tx().map(|tx| *tx.tx_hash());
        // TODO: maybe add Instant to ongoing transaction.
        tracing::info!(?tx_hash, %signature, slot, "transaction was confirmed");

        #[cfg(test)]
        if self.check_stop_after() {
            return;
        }

        // TODO: follow up transactions
        match self.builder.next_step(tx).await {
            Err(err) => {
                tracing::error!(?tx_hash, %signature, ?err, "failed executing next transaction step")
            }
            Ok(Some(tx)) => {
                if let Err(err) = self.sign_and_send_transaction(tx).await {
                    tracing::error!(%signature, ?tx_hash, ?err, "failed sending transaction next step");
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
        tracing::warn!(?tx_hash, %signature, slot, ?err, "transaction was confirmed, but failed");

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
}

#[cfg(test)]
impl Executor {
    async fn join_current_transactions(&self) -> Vec<Signature> {
        self.test_ext.take().await
    }

    async fn stop_after(&self, n: usize) -> Vec<Signature> {
        self.test_ext.stop_after(n).await
    }

    fn check_stop_after(&self) -> bool {
        if let Some(max) = *self.test_ext.stop_after.lock().unwrap() {
            if self.test_ext.signatures.lock().unwrap().len() >= max {
                self.test_ext.notify.notify_waiters();
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod test_ext {
    use std::{mem, sync::Mutex};

    use super::*;

    #[derive(Debug)]
    pub struct TestExtension {
        pub notify: Notify,
        pub signatures: Mutex<Vec<Signature>>,
        pub stop_after: Mutex<Option<usize>>,
    }

    impl TestExtension {
        pub fn new() -> Self {
            Self {
                notify: Notify::new(),
                signatures: Mutex::new(Vec::new()),
                stop_after: Mutex::new(None),
            }
        }

        pub async fn stop_after(&self, n: usize) -> Vec<Signature> {
            self.stop_after.lock().unwrap().replace(n);
            self.take().await
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
