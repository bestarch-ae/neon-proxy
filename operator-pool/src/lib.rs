use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use clap::Args;
use reth_primitives::Address;
use solana_cli_config::CONFIG_FILE;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use thiserror::Error;
use tokio::sync::oneshot;

use executor::Executor;
use neon_api::NeonApi;
use operator::Operator;
use solana_api::solana_api::SolanaApi;
use tokio::task::JoinHandle;

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
}

#[derive(Debug)]
struct PoolEntry {
    operator: Arc<Operator>,
    executor: Arc<Executor>,
    _executor_task: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug)]
pub struct OperatorPool {
    map: HashMap<Address, PoolEntry>,
    operator_order: Box<[Address]>,
    index: AtomicUsize,
}

impl OperatorPool {
    pub async fn from_config(
        config: Config,
        neon_pubkey: Pubkey,
        neon_api: NeonApi,
        solana_api: SolanaApi,
        pg_pool: db::PgPool,
    ) -> Result<Self> {
        let operators = Self::load_from_path(
            config.operator_keypair_path,
            config.operator_keypair_prefix.as_deref(),
        )?;

        let mut map = HashMap::new();
        let mut operator_order = Vec::new();
        for operator in operators {
            let operator = Arc::new(operator);
            let (executor, _executor_task) = Executor::builder()
                .neon_pubkey(neon_pubkey)
                .operator(operator.clone())
                .neon_api(neon_api.clone())
                .solana_api(solana_api.clone())
                .pg_pool(pg_pool.clone())
                .init_operator_balance(config.init_operator_balance)
                .max_holders(config.max_holders)
                .init_holders(config.init_holders)
                .prepare()
                .start()
                .await
                .map_err(Error::Executor)?;
            operator_order.push(operator.address());
            let entry = PoolEntry {
                operator,
                executor,
                _executor_task,
            };
            map.insert(entry.operator.address(), entry);
        }

        Ok(Self {
            map,
            operator_order: operator_order.into_boxed_slice(),
            index: AtomicUsize::new(0),
        })
    }

    fn load_from_path(path: impl AsRef<Path>, prefix: Option<&OsStr>) -> Result<HashSet<Operator>> {
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
                tracing::info!(sol = %operator.pubkey(), eth = %operator.address(), "loaded key");
                set.insert(operator);
            }
        }

        Ok(set)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, address: &Address) -> Option<&Operator> {
        self.map.get(address).map(|entry| entry.operator.as_ref())
    }

    pub fn try_get(&self, address: &Address) -> Result<&Operator> {
        self.get(address).ok_or(Error::UnknownOperator(*address))
    }

    pub fn addresses(&self) -> impl Iterator<Item = &'_ Address> + '_ {
        self.map.keys()
    }
}

impl executor::Execute for OperatorPool {
    fn handle_transaction(
        &self,
        tx: executor::ExecuteRequest,
        result_sender: Option<oneshot::Sender<executor::ExecuteResult>>,
    ) -> impl Future<Output = anyhow::Result<Signature>> + Send {
        let idx = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let address = &self.operator_order[idx % self.operator_order.len()];
        self.map
            .get(address)
            .expect("must exist")
            .executor
            .handle_transaction(tx, result_sender)
    }
}
