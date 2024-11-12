use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use dashmap::DashMap;
use futures_util::StreamExt;
use humantime::format_duration;
use reth_primitives::Address;
use solana_cli_config::CONFIG_FILE;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::time;
use tokio_util::time::DelayQueue;

use executor::Executor;
use neon_api::NeonApi;
use operator::Operator;
use solana_api::solana_api::SolanaApi;

const RELOAD_INTERVAL: Duration = Duration::from_secs(60);
const RECHECK_INTERVAL: Duration = Duration::from_secs(20);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("operator error: {0}")]
    Operator(#[from] operator::Error),
    #[error("executor error: {0}")]
    Executor(anyhow::Error),
    #[error("load error: {0}")]
    Load(#[from] std::io::Error),
    #[error("unknown operator: {0}")]
    UnknownOperator(Address),
}

fn default_kp_path() -> OsString {
    let config = solana_cli_config::Config::default();
    let path: &Path = config.keypair_path.as_ref();
    path.parent()
        .unwrap_or_else(|| panic!("invalid config path: {:?}", &*CONFIG_FILE))
        .as_os_str()
        .to_owned()
}

#[derive(Debug, Args, Clone)]
#[group(id = "OperatorConfig")]
pub struct Config {
    #[arg(long, default_value_os_t = default_kp_path())]
    /// Path to directory containing operator keypairs
    pub operator_keypair_path: OsString,

    #[arg(long)]
    /// Prefix filter for operator keys
    pub operator_keypair_prefix: Option<OsString>,

    #[arg(long, default_value_t = false)]
    /// Initialize operator balance accounts at service startup
    pub init_operator_balance: bool,

    #[arg(long, default_value_t = u8::MAX)]
    /// Maximum holder accounts
    pub max_holders: u8,

    #[arg(long, default_value_t = false)]
    /// Create all holders on service start
    pub init_holders: bool,

    #[arg(long, default_value_t = 9_000_000_000)]
    /// Balance below which a warning is issued
    pub operator_warn_balance: u64,

    #[arg(long, default_value_t = 1_000_000_000)]
    /// Minimum balance for operator to become activated
    pub operator_minimum_balance: u64,
}

#[derive(Debug, Clone)]
struct PoolEntry {
    operator: Arc<Operator>,
    executor: Arc<Executor>,
}

#[derive(Debug)]
struct Bootstrap {
    neon_pubkey: Pubkey,
    neon_api: NeonApi,
    solana_api: SolanaApi,
    pg_pool: db::PgPool,
    init_operator_balance: bool,
    max_holders: u8,
    init_holders: bool,
}

impl Bootstrap {
    async fn start_executor(&self, operator: Arc<Operator>) -> Result<PoolEntry> {
        let (executor, _executor_task) = Executor::builder()
            .neon_pubkey(self.neon_pubkey)
            .operator(operator.clone())
            .neon_api(self.neon_api.clone())
            .solana_api(self.solana_api.clone())
            .pg_pool(self.pg_pool.clone())
            .init_operator_balance(self.init_operator_balance)
            .max_holders(self.max_holders)
            .init_holders(self.init_holders)
            .prepare()
            .start()
            .await
            .map_err(Error::Executor)?;
        let entry = PoolEntry { operator, executor };
        Ok(entry)
    }
}

type Sender<T> = async_channel::Sender<T>;
type Receiver<T> = async_channel::Receiver<T>;

#[derive(Debug, Clone, Copy)]
enum OperatorHealth {
    Good,
    Warn,
    Bad,
}

impl OperatorHealth {
    /// Returns `true` if the operator health is [`Bad`].
    ///
    /// [`Bad`]: OperatorHealth::Bad
    #[must_use]
    fn is_bad(&self) -> bool {
        matches!(self, Self::Bad)
    }
}

#[derive(Debug)]
pub struct OperatorPool {
    map: DashMap<Address, PoolEntry>,
    queue: (Sender<Address>, Receiver<Address>),
    bootstrap: Bootstrap,
    deactivated: Sender<Address>,
    path: OsString,
    prefix: Option<OsString>,
    warn_balance: u64,
    min_balance: u64,
}

impl OperatorPool {
    pub async fn from_config(
        config: Config,
        neon_pubkey: Pubkey,
        neon_api: NeonApi,
        solana_api: SolanaApi,
        pg_pool: db::PgPool,
    ) -> Result<Arc<Self>> {
        assert!(config.operator_warn_balance > config.operator_minimum_balance);
        let operators = load_operators(
            &config.operator_keypair_path,
            config.operator_keypair_prefix.as_deref(),
        )?;

        let bootstrap = Bootstrap {
            neon_pubkey,
            neon_api,
            solana_api,
            pg_pool,
            init_operator_balance: config.init_operator_balance,
            max_holders: config.max_holders,
            init_holders: config.init_holders,
        };
        let (deactivated, deactivated_rx) = async_channel::bounded(32);
        let map = DashMap::new();
        let queue = async_channel::unbounded();
        let mut operator_order = Vec::new();
        for operator in operators {
            let operator = Arc::new(operator);
            let address = operator.address();
            let entry = bootstrap.start_executor(operator).await?;
            tracing::info!(operator = ?entry.operator, "loaded operator");
            operator_order.push(entry.operator.address());
            map.insert(entry.operator.address(), entry);
            queue.0.send(address).await.expect("never closed");
        }

        tracing::info!(order = ?operator_order, "loaded operators");

        let this = Arc::new(Self {
            map,
            queue,
            bootstrap,
            deactivated,
            path: config.operator_keypair_path,
            prefix: config.operator_keypair_prefix,
            warn_balance: config.operator_warn_balance,
            min_balance: config.operator_minimum_balance,
        });

        tokio::spawn(this.clone().run(deactivated_rx));

        Ok(this)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, address: &Address) -> Option<Arc<Operator>> {
        self.map.get(address).map(|entry| entry.operator.clone())
    }

    pub fn try_get(&self, address: &Address) -> Result<Arc<Operator>> {
        self.get(address).ok_or(Error::UnknownOperator(*address))
    }

    pub fn addresses(&self) -> Vec<Address> {
        self.map.iter().map(|ref_| *ref_.key()).collect()
    }

    async fn reload(&self) -> Result<()> {
        let mut to_add = load_operators(&self.path, self.prefix.as_deref())?;

        let to_remove = self
            .map
            .iter()
            .filter(|entry| !to_add.contains(&entry.operator))
            .map(|entry| entry.operator.address())
            .collect::<HashSet<_>>();
        to_add.retain(|operator| !self.map.contains_key(&operator.address()));
        if to_remove.is_empty() && to_add.is_empty() {
            tracing::debug!("operator set did not change, nothing to reload");
        } else {
            tracing::info!(?to_add, ?to_remove, "reloading operators");
            let mut map = HashMap::new();
            let mut operator_order = Vec::new();
            for address in to_remove {
                let Some((_, entry)) = self.map.remove(&address) else {
                    tracing::warn!("cannot remove operator, not found in map");
                    continue;
                };
                let operator = &entry.operator;
                tracing::info!(?operator, "deactivating operator");
                continue;
            }

            for operator in to_add {
                let operator = Arc::new(operator);
                let address = operator.address();
                let entry = self.bootstrap.start_executor(operator).await?;
                tracing::info!(operator = ?entry.operator, "loaded operator");
                operator_order.push(entry.operator.address());
                map.insert(entry.operator.address(), entry);
                self.queue.0.send(address).await.expect("never closed");
            }

            tracing::info!("reloaded operators");
        }
        Ok(())
    }

    fn balance_to_health(&self, balance: u64) -> OperatorHealth {
        match balance {
            _ if balance < self.min_balance => OperatorHealth::Bad,
            _ if balance < self.warn_balance => OperatorHealth::Warn,
            _ => OperatorHealth::Good,
        }
    }

    async fn get_operator_balance(&self, addr: &Address, key: &Pubkey) -> u64 {
        let balance = self
            .bootstrap
            .solana_api
            .get_balance(key)
            .await
            .inspect_err(
                |error| tracing::warn!(%key, ?error, "error while checking operator balance"),
            )
            .unwrap_or(0);
        tracing::debug!(%addr, %key, balance, "checking operator balance");
        balance
    }

    /// Returns true if there's no point in checking this operator again
    async fn try_reactivate(&self, address: Address) -> bool {
        let Some(key) = self.map.get(&address).map(|ref_| ref_.operator.pubkey()) else {
            tracing::debug!(%address, "operator will not be reactivated, not present in map");
            return true;
        };
        if self
            .balance_to_health(self.get_operator_balance(&address, &key).await)
            .is_bad()
        {
            false
        } else {
            self.queue.0.send(address).await.expect("never closed");
            tracing::info!(%address, "reactivated operator");
            true
        }
    }

    fn run(
        self: Arc<Self>,
        deactivate: Receiver<Address>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let this = Arc::downgrade(&self);
        drop(self);
        let mut interval = time::interval(RELOAD_INTERVAL);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut deactivated = DelayQueue::new();

        async move {
            tracing::info!(
                interval = %format_duration(RELOAD_INTERVAL),
                "starting operator pool reload task"
            );
            loop {
                tokio::select! {
                     _ = interval.tick() => {
                        let Some(this) = this.upgrade() else {
                            tracing::info!("stopping operator pool reload task");
                            break;
                        };
                        if let Err(error) = this.reload().await {
                            tracing::error!(?error, "could not reload operator pool");
                        }
                     }
                    Ok(addr) = deactivate.recv() => {
                        deactivated.insert(addr, RECHECK_INTERVAL);
                    },
                    Some(addr) = deactivated.next(), if !deactivated.is_empty() => {
                        let addr = addr.into_inner();
                        let Some(this) = this.upgrade() else {
                            tracing::info!("stopping operator pool reload task");
                            break;
                        };
                        if !this.try_reactivate(addr).await {
                            deactivated.insert(addr, RECHECK_INTERVAL);
                        }
                    }
                    else => break,
                }
            }
        }
    }
}

impl executor::Execute for OperatorPool {
    async fn handle_transaction(
        &self,
        tx: executor::ExecuteRequest,
        result_sender: Option<oneshot::Sender<executor::ExecuteResult>>,
    ) -> anyhow::Result<Signature> {
        let executor = loop {
            let address = self.queue.1.recv().await.expect("never closed");
            let Some(executor) = self.map.get(&address).map(|ref_| ref_.executor.clone()) else {
                // This is for removed and disabled operators
                continue;
            };
            let balance = self
                .get_operator_balance(&address, &executor.pubkey())
                .await;
            match self.balance_to_health(balance) {
                OperatorHealth::Bad => {
                    tracing::info!(%address, pubkey = %executor.pubkey(), balance, "deactivating operator");
                    self.deactivated
                        .send(address)
                        .await
                        .expect("cannot deactivate operator");
                    continue;
                }
                OperatorHealth::Warn => {
                    tracing::warn!(%address, pubkey = %executor.pubkey(), balance, "operator balance is low");
                }
                OperatorHealth::Good => (),
            }
            self.queue
                .0
                .send(address)
                .await
                .expect("never full and never closed");
            break executor;
        };
        executor.handle_transaction(tx, result_sender).await
    }
}

fn load_operators(path: impl AsRef<Path>, prefix: Option<&OsStr>) -> Result<HashSet<Operator>> {
    let path = path.as_ref();

    macro_rules! ok {
        ($result:expr) => {
            match $result {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::warn!(?error, dir = ?path, "error reading directory entry");
                    continue;
                }
            }
        }
    }

    tracing::info!(?path, "loading keys");
    let mut set = HashSet::new();
    // TODO: tokio read_dir??
    for entry in path.read_dir()? {
        let entry = ok!(entry);

        let fits_prefix = prefix.map_or(true, |prefix| {
            entry
                .file_name()
                .as_encoded_bytes()
                .starts_with(prefix.as_encoded_bytes())
        });
        if ok!(entry.file_type()).is_file()
            && entry.path().extension().map_or(false, |ext| ext == "json")
            && fits_prefix
        {
            let operator = ok!(Operator::read_from_file(entry.path()));
            tracing::debug!(sol = %operator.pubkey(), eth = %operator.address(), "loaded key");
            set.insert(operator);
        }
    }

    Ok(set)
}
