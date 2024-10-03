mod operator;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::bail;
use clap::Args;
use reth_primitives::Address;

pub use operator::Operator;

#[derive(Args)]
pub struct Config {
    #[arg(long)]
    /// Path to directory containing operator keypairs
    pub operator_keypair_path: PathBuf,

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
