use anyhow::Result;
use clap::Parser;
use solana::solana_api::SolanaApi;
use solana::traverse::TraverseLedger;

use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;

#[derive(Parser)]
struct Args {
    #[arg(value_name = "Pubkey")]
    /// Target pubkey
    target: Pubkey,

    #[arg(
        short,
        long,
        default_value = "https://api.mainnet-beta.solana.com",
        value_name = "URL"
    )]
    /// Solana endpoint
    url: String,

    #[arg(short, long, default_value = None, value_name = "SIGNATURE")]
    /// Transaction to start from
    from: Option<Signature>,

    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    pg_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Args::try_parse()?;

    let api = SolanaApi::new(opts.url);
    let mut traverse = TraverseLedger::new(api, opts.target, opts.from);
    let mut adb = accountsdb::DummyAdb::new(opts.target);
    let pool = db::connect(&opts.pg_url).await?;
    let tx_repo = db::TransactionRepo::new(pool);
    tracing::info!("connected");

    while let Some(result) = traverse.next().await {
        tracing::debug!(?result, "retrieved transaction");
        let tx = match result {
            Ok(tx) => tx,
            Err(err) => {
                tracing::warn!(?err, "failed to retrieve transaction");
                continue;
            }
        };
        let txs = match neon_parse::parse(tx, &mut adb) {
            Ok(txs) => txs,
            Err(err) => {
                tracing::warn!(?err, "failed to parse tx");
                continue;
            }
        };
        tracing::debug!(?txs, "parsed transactions");
        for tx in &txs {
            if let Err(err) = tx_repo.insert(tx).await {
                tracing::warn!(?err, "failed to save neon transaction");
            } else {
                tracing::info!(signature = tx.neon_signature, "saved transaction");
            }
        }
    }

    Ok(())
}

mod accountsdb {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    use common::solana_sdk::account_info::AccountInfo;
    use common::solana_sdk::pubkey::Pubkey;
    use neon_parse::AccountsDb;

    #[derive(Clone, Debug)]
    struct Data {
        data: Vec<u8>,
        lamports: u64,
    }

    #[derive(Clone, Debug)]
    pub struct DummyAdb {
        map: HashMap<Pubkey, Data>,
        neon_pubkey: Pubkey,
    }

    impl DummyAdb {
        pub fn new(neon: Pubkey) -> Self {
            DummyAdb {
                map: Default::default(),
                neon_pubkey: neon,
            }
        }
    }

    impl AccountsDb for DummyAdb {
        fn get_by_key<'a>(&'a mut self, pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
            tracing::debug!(%pubkey, "getting data for account");
            let data = self.map.get_mut(pubkey)?;

            let account_info = AccountInfo {
                key: pubkey,
                owner: &self.neon_pubkey,
                data: Rc::new(RefCell::new(data.data.as_mut())),
                lamports: Rc::new(RefCell::new(&mut data.lamports)),
                is_signer: false,
                is_writable: false,
                executable: false,
                rent_epoch: 0,
            };
            Some(account_info)
        }

        fn init_account(&mut self, pubkey: Pubkey) {
            tracing::debug!(%pubkey, "init account");
            self.map.entry(pubkey).or_insert_with(|| Data {
                data: vec![0; 1024 * 1024],
                lamports: 0,
            });
        }
    }
}
