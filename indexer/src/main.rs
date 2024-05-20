use anyhow::Result;
use clap::Parser;
use solana::solana_api::SolanaApi;
use solana::traverse::{LedgerItem, TraverseLedger};

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

    let pool = db::connect(&opts.pg_url).await?;
    let tx_repo = db::TransactionRepo::new(pool.clone());
    let sig_repo = db::SolanaSignaturesRepo::new(pool.clone());
    let block_repo = db::BlockRepo::new(pool);

    let last_signature = sig_repo.get_latest().await?;
    let from = opts.from.or(last_signature);

    tracing::info!("starting traversal from {:?}", from);

    let api = SolanaApi::new(opts.url);
    let mut traverse = TraverseLedger::new(api, opts.target, from);
    let mut adb = accountsdb::DummyAdb::new(opts.target);

    let mut last_written_slot = None;
    tracing::info!("connected");

    while let Some(result) = traverse.next().await {
        tracing::debug!(?result, "retrieved transaction");
        match result {
            Ok(LedgerItem::Transaction(tx)) => {
                let signature = tx.tx.signatures[0];
                let tx_idx = tx.tx_idx;
                let slot = tx.slot;

                let _span =
                    tracing::info_span!("solana transaction", signature = %signature).entered();

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
                if let Err(err) = sig_repo.insert(slot, tx_idx, signature).await {
                    tracing::warn!(?err, "failed to save solana transaction");
                }
            }
            Ok(LedgerItem::Block(block)) => {
                if let Err(err) = block_repo.insert(&block).await {
                    tracing::warn!(?err, slot = block.slot, "failed to save solana block");
                } else {
                    tracing::info!(slot = block.slot, "saved solana block");
                    last_written_slot.replace(block.slot);
                }
            }
            Err(err) => {
                tracing::warn!(?err, "failed to retrieve transaction");
                continue;
            }
        };
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
            tracing::info!(%pubkey, "init account");
            self.map.entry(pubkey).or_insert_with(|| Data {
                data: vec![0; 1024 * 1024],
                lamports: 0,
            });
        }
    }
}
