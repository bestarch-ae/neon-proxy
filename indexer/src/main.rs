use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use solana::solana_api::SolanaApi;
use solana::traverse::{LedgerItem, TraverseLedger};

use common::ethnum::U256;
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

    #[arg(long, default_value = None, value_name = "SECS")]
    /// Seconds to sleep between RPS limit rejected gSFA
    rps_limit_sleep: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Args::try_parse()?;

    let pool = db::connect(&opts.pg_url).await?;
    let tx_repo = db::TransactionRepo::new(pool.clone());
    let sig_repo = db::SolanaSignaturesRepo::new(pool.clone());
    let holder_repo = db::HolderRepo::new(pool.clone());
    let block_repo = db::BlockRepo::new(pool);

    let last_signature = sig_repo.get_latest().await?;
    let from = opts.from.or(last_signature);

    tracing::info!("starting traversal from {:?}", from);

    let api = SolanaApi::new(opts.url);
    let mut traverse = TraverseLedger::new(api, opts.target, from);
    traverse.set_rps_limit_sleep(opts.rps_limit_sleep.map(Duration::from_secs));
    let mut adb = accountsdb::DummyAdb::new(opts.target, holder_repo.clone());

    let mut last_written_slot = None;
    tracing::info!("connected");

    /* TODO: we should always start from the start of the block otherwise these would be wrong */
    let mut neon_tx_idx = 0;
    let mut block_gas_used = U256::new(0);

    while let Some(result) = traverse.next().await {
        tracing::debug!(?result, "retrieved transaction/block");

        match result {
            Ok(LedgerItem::Transaction(tx)) => {
                let signature = tx.tx.signatures[0];
                let tx_idx = tx.tx_idx as u32;
                let slot = tx.slot;

                let _span =
                    tracing::info_span!("solana transaction", signature = %signature).entered();

                adb.set_slot_idx(slot, tx_idx);

                let (txs, holders) = match neon_parse::parse(tx, &mut adb) {
                    Ok((txs, holders)) => (txs, holders),
                    Err(err) => {
                        tracing::warn!(?err, "failed to parse solana transaction");
                        continue;
                    }
                };
                tracing::debug!(?txs, "parsed transactions");
                for mut tx in txs {
                    tx.tx_idx = neon_tx_idx;
                    neon_tx_idx += 1;

                    block_gas_used += tx.gas_used;
                    tx.sum_gas_used = block_gas_used;

                    if let Err(err) = tx_repo.insert(&tx).await {
                        tracing::warn!(?err, "failed to save neon transaction");
                    } else {
                        tracing::info!(
                            signature = hex::encode(tx.neon_signature),
                            "saved transaction"
                        );
                    }
                }
                if let Err(err) = sig_repo.insert(slot, tx_idx, signature).await {
                    tracing::warn!(?err, "failed to save solana transaction");
                }
                for holder in &holders {
                    tracing::info!(slot = %slot, pubkey = %holder.pubkey(), "saving holder");
                    if let Err(err) = save_holder(&holder_repo, slot, tx_idx, holder).await {
                        tracing::warn!(?err, "failed to save neon holder");
                    }
                }
            }
            Ok(LedgerItem::Block(block)) => {
                neon_tx_idx = 0;
                block_gas_used = U256::new(0);

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

async fn save_holder(
    repo: &db::HolderRepo,
    slot: u64,
    tx_idx: u32,
    op: &neon_parse::HolderOperation,
) -> Result<(), anyhow::Error> {
    use neon_parse::HolderOperation;
    match op {
        HolderOperation::Create(pubkey) => {
            repo.insert(slot, tx_idx, false, None, pubkey, None, None)
                .await?
        }
        HolderOperation::Write {
            pubkey,
            tx_hash,
            offset,
            data,
        } => {
            repo.insert(
                slot,
                tx_idx,
                false,
                Some(&hex::encode(tx_hash)),
                pubkey,
                Some(*offset as u64),
                Some(data),
            )
            .await?
        }
        HolderOperation::Delete(pubkey) => {
            repo.insert(slot, tx_idx, false, None, pubkey, None, None)
                .await?
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
    use db::HolderRepo;
    use neon_parse::AccountsDb;
    use tokio::runtime::Handle;

    #[derive(Clone, Debug)]
    struct Data {
        data: Vec<u8>,
        lamports: u64,
    }

    #[derive(Clone, Debug)]
    pub struct DummyAdb {
        map: HashMap<Pubkey, Data>,
        neon_pubkey: Pubkey,
        db: HolderRepo,
        slot: u64,
        tx_idx: u32,
    }

    impl DummyAdb {
        pub fn new(neon: Pubkey, db: HolderRepo) -> Self {
            DummyAdb {
                map: Default::default(),
                neon_pubkey: neon,
                db,
                slot: 0,
                tx_idx: 0,
            }
        }

        pub fn set_slot_idx(&mut self, slot: u64, tx_idx: u32) {
            self.slot = slot;
            self.tx_idx = tx_idx;
        }

        pub fn get_from_db(
            db: HolderRepo,
            pubkey: &Pubkey,
            slot: u64,
            tx_idx: u32,
        ) -> Option<Vec<u8>> {
            let data = tokio::task::block_in_place(move || {
                Handle::current()
                    .block_on(async move { db.get_by_pubkey(pubkey, slot, tx_idx).await })
            });

            match data {
                Ok(Some(data)) => {
                    tracing::info!(%pubkey, "holder data found");
                    Some(data)
                }
                Ok(None) => {
                    tracing::info!(%pubkey, "holder not found in db");
                    None
                }
                Err(err) => {
                    tracing::warn!(%err, "db error");
                    None
                }
            }
        }
    }

    impl AccountsDb for DummyAdb {
        fn get_by_key<'a>(&'a mut self, pubkey: &'a Pubkey) -> Option<AccountInfo<'a>> {
            tracing::debug!(%pubkey, "getting data for account");
            self.init_account(*pubkey);
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
            let db = self.db.clone();
            let slot = self.slot;
            let tx_idx = self.tx_idx;

            self.map.entry(pubkey).or_insert_with(move || {
                use common::evm_loader::account::TAG_HOLDER;

                let data = Self::get_from_db(db, &pubkey, slot, tx_idx);
                let mut data = data.unwrap_or_else(|| vec![0; 1024 * 1024]);
                data[0] = TAG_HOLDER;

                Data { data, lamports: 0 }
            });
        }
    }
}
