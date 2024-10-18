use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures_util::StreamExt;
use indexmap::IndexSet;
use solana_api::solana_api::SolanaApi;
use solana_sdk::address_lookup_table::{self, state, AddressLookupTableAccount};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use tracing::{debug, info, warn};

const ACCOUNTS_PER_TX: usize = 27;
const MAX_ACCOUNTS_PER_ALT: usize = 256;

pub type AltInfo = AddressLookupTableAccount;

#[derive(Debug)]
pub struct AltUpdateInfo {
    pubkey: Pubkey,
    accounts: VecDeque<Pubkey>,
    _guard: OwnedSemaphorePermit,
    /// Idx of last written account in vec
    idx: usize,
    written: usize,
}

impl AltUpdateInfo {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

#[derive(Debug, Clone)]
struct Alt {
    pubkey: Pubkey,
    accounts: IndexSet<Pubkey>,
    write_lock: Arc<Semaphore>,
}

impl Alt {
    fn new(pubkey: Pubkey, accounts: IndexSet<Pubkey>) -> Self {
        Self {
            pubkey,
            accounts,
            write_lock: Arc::new(Semaphore::new(1)),
        }
    }

    fn get_account(&self) -> AltInfo {
        AddressLookupTableAccount {
            key: self.pubkey,
            addresses: self.accounts.iter().copied().collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AltManager {
    operator: Pubkey,
    solana_api: SolanaApi,
    alts: DashMap<Pubkey, Alt>,
    repo: Option<db::AltRepo>,
}

#[derive(Debug)]
pub enum UpdateProgress {
    WriteChunk {
        ix: Instruction,
        info: AltUpdateInfo,
    },
    Ready(AltInfo),
}

impl AltManager {
    pub async fn new(operator: Pubkey, solana_api: SolanaApi, pg_pool: Option<db::PgPool>) -> Self {
        let mut this = Self {
            operator,
            solana_api,
            alts: DashMap::new(),
            repo: pg_pool.map(db::AltRepo::new),
        };
        this.recover().await;

        this
    }

    pub async fn acquire<I>(&self, accounts: I) -> anyhow::Result<UpdateProgress>
    where
        I: IntoIterator<Item = Pubkey>,
    {
        let new_accounts: IndexSet<_> = accounts.into_iter().collect();
        // TODO: Reverse address index
        let mut candidates = BTreeMap::new();
        for alt in self.alts.iter() {
            let intersection_len = alt.accounts.intersection(&new_accounts).count();
            if intersection_len == new_accounts.len() {
                return Ok(UpdateProgress::Ready(alt.get_account()));
            }
            // if intersection_len > 0
            //     && alt.accounts.len() + (new_accounts.len() - intersection_len)
            //         < MAX_ACCOUNTS_PER_ALT
            if alt.accounts.len() + (new_accounts.len() - intersection_len) < MAX_ACCOUNTS_PER_ALT {
                candidates.insert(intersection_len, alt.pubkey);
            }
        }

        const RETRIES: usize = 5;

        let candidates_len = candidates.len();
        for retry_idx in 0..RETRIES {
            if let Some((_, candidate)) = candidates.pop_last() {
                // WARN: Do not hold lock into the map while awaiting on semaphore,
                //     : so other tx chain can finish updating alt
                let alt = || self.alts.get(&candidate).expect("alts dont get deleted");
                let semaphore = alt().write_lock.clone();
                let _guard = semaphore.acquire_owned().await.unwrap();

                let alt = alt();
                let accounts: VecDeque<_> =
                    alt.accounts.difference(&new_accounts).copied().collect();

                // WARN: There is a small chance that while we were awaiting on the semaphore
                //     : this ALT got updated to the point where new accounts no longer fit .
                if alt.accounts.len() + accounts.len() > MAX_ACCOUNTS_PER_ALT {
                    debug!(
                        %self.operator,
                        alt_len = alt.accounts.len(),
                        extension_len = accounts.len(),
                        retry_idx,
                        key = %alt.pubkey,
                        "skipping ALT due to race"
                    );
                    continue;
                }

                let mut info = AltUpdateInfo {
                    pubkey: alt.pubkey,
                    accounts,
                    _guard,
                    idx: 0,
                    written: 0,
                };
                debug!(
                    %self.operator,
                    alt_len = alt.accounts.len(),
                    extension_len = info.accounts.len(),
                    retry_idx,
                    key = %alt.pubkey,
                    "updating ALT with new accounts"
                );
                let ix = self.write_next_chunk(&mut info);
                return Ok(UpdateProgress::WriteChunk { ix, info });
            } else {
                break;
            }
        }

        debug!(
            %self.operator,
            extension_len = new_accounts.len(),
            candidates_len,
            "no suitable ALT candidates"
        );
        // Otherwise create new ALT
        let (info, ix) = self.create_new_alt(new_accounts).await?;
        Ok(UpdateProgress::WriteChunk { ix, info })
    }

    pub fn update(&self, info: AltUpdateInfo) -> UpdateProgress {
        let mut info = info;
        if info.idx == 0 {
            let ix = self.write_next_chunk(&mut info);
            return UpdateProgress::WriteChunk { ix, info };
        }

        let mut alt = self
            .alts
            .get_mut(&info.pubkey)
            .expect("alts dont get deleted");
        assert_eq!(info.pubkey, alt.pubkey);
        alt.accounts.extend(info.accounts.drain(0..info.idx));
        let alt_length = alt.accounts.len();

        if info.is_empty() {
            UpdateProgress::Ready(alt.get_account())
        } else {
            drop(alt);
            let ix = self.write_next_chunk(&mut info);
            info!(
                %self.operator,
                extension_length = info.idx,
                alt_length,
                key = %info.pubkey,
                "extending ALT"
            );
            UpdateProgress::WriteChunk { ix, info }
        }
    }

    pub async fn recreate_alt(
        &self,
        info: AltUpdateInfo,
    ) -> anyhow::Result<(AltUpdateInfo, Instruction)> {
        let account_raw = self.solana_api.get_account(&info.pubkey).await;
        let account_data = account_raw.as_ref().ok().and_then(Option::as_ref);
        let account = account_data.map(|acc| state::AddressLookupTable::deserialize(&acc.data));
        warn!(%self.operator, key = %info.pubkey, ?account, "recreating ALT");
        if let Some((_key, old)) = self.alts.remove(&info.pubkey) {
            let accounts = old.accounts.into_iter().chain(info.accounts);
            self.create_new_alt(accounts).await
        } else {
            // This should be unreachable since we lock semaphore on update.
            // If this error occurs the logic is invalid.
            bail!("missing ALT ({}) for recreation", info.pubkey);
        }
    }

    async fn create_new_alt<I>(&self, accounts: I) -> anyhow::Result<(AltUpdateInfo, Instruction)>
    where
        I: IntoIterator<Item = Pubkey>,
    {
        let (ix, pubkey) = self.create_alt_ix().await?;
        let alt = Alt::new(pubkey, IndexSet::new());
        if let Err(err) = self.save_account(pubkey).await {
            warn!(%self.operator, %pubkey, ?err, "could not save ALT to db");
        }
        let _guard = alt.write_lock.clone().acquire_owned().await.unwrap();
        self.alts.insert(pubkey, alt);
        let update_info = AltUpdateInfo {
            pubkey,
            accounts: accounts.into_iter().collect(),
            _guard,
            idx: 0,
            written: 0,
        };
        info!(%self.operator, %pubkey, length = update_info.accounts.len(), "creating new alt");

        Ok((update_info, ix))
    }

    async fn create_alt_ix(&self) -> anyhow::Result<(Instruction, Pubkey)> {
        Ok(address_lookup_table::instruction::create_lookup_table(
            self.operator,
            self.operator,
            self.solana_api
                .get_slot(CommitmentLevel::Finalized)
                .await
                .context("failed requesting recent slot")?,
        ))
    }

    fn write_next_chunk(&self, info: &mut AltUpdateInfo) -> Instruction {
        let to = info.accounts.len().min(ACCOUNTS_PER_TX);
        info.written += info.idx;
        info.idx = to;

        address_lookup_table::instruction::extend_lookup_table(
            info.pubkey,
            self.operator,
            Some(self.operator),
            info.accounts.iter().copied().take(to).collect(),
        )
    }

    async fn recover(&mut self) {
        if let Some(repo) = self.repo.as_ref() {
            let alts_stream = repo.fetch();
            tokio::pin!(alts_stream);
            while let Some(item) = alts_stream.next().await {
                let Ok(key) =
                    item.inspect_err(|err| warn!(%self.operator, ?err, "invalid ALT address"))
                else {
                    continue;
                };

                if let Err(err) = self.load_account(key).await {
                    warn!(%self.operator, %key, ?err, "could not load account");
                } else if let Some(alt) = self.alts.get(&key) {
                    info!(%self.operator, %key, addresses = ?alt.accounts, "loaded ALT account");
                } else {
                    // Probably unreachable if recover is `&mut`
                    info!(%self.operator, %key, "loaded ALT account, but it is not present in map");
                }
            }
            info!(%self.operator, recovered = self.alts.len(), "finished ALT recovery");
        }
    }

    async fn load_account(&self, pubkey: Pubkey) -> anyhow::Result<()> {
        let Some(acc) = self.solana_api.get_account(&pubkey).await? else {
            bail!("account {pubkey} does not exist");
        };
        let state = state::AddressLookupTable::deserialize(&acc.data)?;
        let alt = Alt::new(pubkey, state.addresses.iter().copied().collect());
        self.alts.insert(pubkey, alt);
        Ok(())
    }

    async fn save_account(&self, pubkey: Pubkey) -> anyhow::Result<()> {
        if let Some(repo) = self.repo.as_ref() {
            repo.insert(pubkey).await?;
        }

        Ok(())
    }
}
