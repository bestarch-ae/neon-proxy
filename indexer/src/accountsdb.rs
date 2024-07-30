use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use common::evm_loader::account::Holder;
use common::evm_loader::types::Transaction;
use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::pubkey::Pubkey;
use common::types::{utils, HolderOperation, TxHash};
use db::{HolderRepo, TransactionRepo};
use lru::LruCache;
use neon_parse::{AccountsDb, TransactionsDb};
use tokio::runtime::Handle;
use tokio_stream::StreamExt;

use crate::metrics::metrics;

#[derive(Clone, Debug)]
struct Data {
    data: Vec<u8>,
    lamports: u64,
}

#[derive(Clone, Debug)]
pub struct DummyAdb {
    map: LruCache<Pubkey, Data>,
    neon_pubkey: Pubkey,
    db: HolderRepo,
    slot: u64,
    tx_idx: u32,
}

impl DummyAdb {
    pub fn new(neon: Pubkey, db: HolderRepo) -> Self {
        DummyAdb {
            map: LruCache::new(NonZeroUsize::new(1_000).unwrap()),
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

    fn get_from_db(
        db: HolderRepo,
        pubkey: &Pubkey,
        slot: u64,
        tx_idx: u32,
    ) -> Option<Vec<HolderOperation>> {
        let data = tokio::task::block_in_place(move || {
            let timer = metrics().holder_fetch_time.start_timer();
            let res = Handle::current()
                .block_on(async move { db.get_by_pubkey(pubkey, slot, tx_idx).await });
            timer.stop_and_record();
            res
        });

        match data {
            Ok(ops) if ops.is_empty() => {
                tracing::info!(%pubkey, "holder not found in db");
                None
            }
            Ok(ops) => {
                tracing::info!(%pubkey, "holder data found");
                Some(ops)
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

        if self.map.contains(pubkey) {
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
            return Some(account_info);
        }

        if let Some(ops) = Self::get_from_db(self.db.clone(), pubkey, self.slot, self.tx_idx) {
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
            let account_info_2 = account_info.clone();
            let mut holder = Holder::from_account(&self.neon_pubkey, account_info).unwrap();
            for op in ops {
                match op {
                    HolderOperation::Write {
                        pubkey: _,
                        tx_hash,
                        offset,
                        data,
                    } => {
                        holder.update_transaction_hash(tx_hash);
                        holder.write(offset, &data).unwrap();
                    }
                    HolderOperation::Create(_pubkey) => {
                        holder.clear();
                    }
                    HolderOperation::Delete(_pubkey) => holder.clear(),
                }
            }
            tracing::info!(%pubkey, data = %common::solana_sdk::hash::hash(&account_info_2.data.borrow()), "holder data loaded");
            return Some(account_info_2);
        }

        None
    }

    fn init_account(&mut self, pubkey: Pubkey) {
        tracing::debug!(%pubkey, "init account");

        super::metrics()
            .holders_in_memory
            .set(self.map.len() as i64);

        self.map.get_or_insert(pubkey, move || {
            use common::evm_loader::account::TAG_HOLDER;

            let mut data = Data {
                data: vec![0; 1024 * 1024],
                lamports: 0,
            };
            data.data[0] = TAG_HOLDER;

            data
        });
    }

    fn delete_account(&mut self, pubkey: Pubkey) {
        self.map.demote(&pubkey);
        self.map.pop_lru();

        super::metrics()
            .holders_in_memory
            .set(self.map.len() as i64);
    }
}

#[derive(Debug)]
pub struct DummyTdb {
    map: RefCell<LruCache<TxHash, Transaction>>,
    db: TransactionRepo,
}

impl DummyTdb {
    pub fn new(db: TransactionRepo) -> Self {
        DummyTdb {
            map: RefCell::new(LruCache::new(NonZeroUsize::new(128).unwrap())),
            db,
        }
    }
}

impl TransactionsDb for DummyTdb {
    fn get_by_hash(&self, tx_hash: TxHash) -> Option<Transaction> {
        tracing::debug!(%tx_hash, "getting transaction");
        let db = self.db.clone();

        if let Some(tx) = self
            .map
            .borrow_mut()
            .get(&tx_hash)
            .map(utils::clone_evm_transaction)
        {
            return Some(tx);
        }

        // This is only used in case we don't find transaction
        // in cache, and shouldn't happen in 99% cases, when
        // transaction is processed. Basically happens only when
        // we were interrupted (process killed) in between steps
        // of the transaction.
        if let Some(tx) = tokio::task::block_in_place(move || {
            let res = Handle::current().block_on(async move {
                let mut stream = db.fetch_without_events(db::TransactionBy::Hash(tx_hash));
                stream.next().await
            });
            match res {
                Some(Ok(tx)) => Some(tx.inner.transaction),
                Some(Err(_)) => None,
                None => None,
            }
        }) {
            self.map
                .borrow_mut()
                .put(tx_hash, utils::clone_evm_transaction(&tx));
            return Some(tx);
        }
        None
    }

    fn insert(&mut self, tx: Transaction) {
        self.map.borrow_mut().put(TxHash::from(tx.hash), tx);
    }
}
