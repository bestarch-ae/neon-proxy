mod operator;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::Arc;

use anyhow::bail;
use clap::Args;
use reth_primitives::Address;
use solana_cli_config::CONFIG_FILE;

pub use operator::Operator;

fn default_kp_path() -> OsString {
    let config = solana_cli_config::Config::default();
    let path: &Path = config.keypair_path.as_ref();
    path.parent()
        .unwrap_or_else(|| panic!("invalid config path: {:?}", &*CONFIG_FILE))
        .as_os_str()
        .to_owned()
}

#[derive(Args)]
#[group(id = "OperatorConfig")]
pub struct Config {
    #[arg(long, default_value_os_t = default_kp_path())]
    /// Path to directory containing operator keypairs
    pub operator_keypair_path: OsString,

    #[arg(long)]
    /// Prefix filter for operator keys
    pub operator_keypair_prefix: Option<OsString>,
}

#[derive(Debug, Clone)]
pub struct Operators {
    map: Arc<HashMap<Address, Arc<Operator>>>,
}

impl Operators {
    pub fn from_config(config: Config) -> anyhow::Result<Self> {
        Self::load_from_path(
            config.operator_keypair_path,
            config.operator_keypair_prefix.as_deref(),
        )
    }

    pub fn load_from_path(path: impl AsRef<Path>, prefix: Option<&OsStr>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.is_dir() {
            bail!("provided path ({path:?}) is not a directory")
        }

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
        let mut map = HashMap::new();
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
                map.insert(operator.address(), Arc::new(operator));
            }
        }

        Ok(Self { map: Arc::new(map) })
    }

    pub fn get(&self, address: &Address) -> Option<Arc<Operator>> {
        self.map.get(address).cloned()
    }

    pub fn addresses(&self) -> impl Iterator<Item = &'_ Address> + '_ {
        self.map.keys()
    }
}
