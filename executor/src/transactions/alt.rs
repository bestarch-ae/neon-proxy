use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Context;

use common::solana_sdk::address_lookup_table::{self, AddressLookupTableAccount};
use common::solana_sdk::commitment_config::CommitmentLevel;
use common::solana_sdk::instruction::Instruction;
use common::solana_sdk::pubkey::Pubkey;
use dashmap::DashMap;
use solana_api::solana_api::SolanaApi;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
    accounts: HashSet<Pubkey>,
    write_lock: Arc<Semaphore>,
}

impl Alt {
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
    pub fn new(operator: Pubkey, solana_api: SolanaApi) -> Self {
        Self {
            operator,
            solana_api,
            alts: DashMap::new(),
        }
    }

    pub async fn acquire<I>(&self, accounts: I) -> anyhow::Result<UpdateProgress>
    where
        I: IntoIterator<Item = Pubkey>,
    {
        let new_accounts: HashSet<_> = accounts.into_iter().collect();
        // TODO: Reverse address index
        let mut candidates = BTreeMap::new();
        for alt in self.alts.iter() {
            let intersection_len = alt.accounts.intersection(&new_accounts).count();
            if intersection_len == new_accounts.len() {
                return Ok(UpdateProgress::Ready(alt.get_account()));
            }
            if intersection_len > 0
                && alt.accounts.len() + (new_accounts.len() - intersection_len)
                    < MAX_ACCOUNTS_PER_ALT
            {
                candidates.insert(intersection_len, alt.pubkey);
            }
        }

        const RETRIES: usize = 5;

        for _ in 0..RETRIES {
            if let Some((_, candidate)) = candidates.pop_last() {
                // WARN: Do not hold lock into the map while awaiting on semaphore,
                //     : so other tx chain can finish updating alt
                let alt = || self.alts.get(&candidate).expect("alts dont get deleted");
                let semaphore = alt().write_lock.clone();
                let _guard = semaphore.acquire_owned().await.unwrap();

                let alt = alt();
                let accounts = alt.accounts.difference(&new_accounts).copied().collect();

                // WARN: There is a small chance that while we were awaiting on the semaphore
                //     : this ALT got updated to the point where new accounts no longer fit .
                if alt.accounts.len() + accounts.len() > MAX_ACCOUNTS_PER_ALT {
                    continue;
                }

                let mut info = AltUpdateInfo {
                    pubkey: alt_guard.pubkey,
                    accounts,
                    _guard,
                    idx: 0,
                    written: 0,
                };
                let ix = self.write_next_chunk(&mut info);
                return Ok(UpdateProgress::WriteChunk { ix, info });
            }
        }

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

        if info.is_empty() {
            UpdateProgress::Ready(alt.get_account())
        } else {
            let ix = self.write_next_chunk(&mut info);
            UpdateProgress::WriteChunk { ix, info }
        }
    }

    async fn create_new_alt<I>(&self, accounts: I) -> anyhow::Result<(AltUpdateInfo, Instruction)>
    where
        I: IntoIterator<Item = Pubkey>,
    {
        let (ix, pubkey) = self.create_alt_ix().await?;
        let alt = Alt {
            pubkey,
            accounts: HashSet::new(),
            write_lock: Arc::new(Semaphore::new(1)),
        };
        let _guard = alt.write_lock.clone().acquire_owned().await.unwrap();
        self.alts.insert(pubkey, alt);
        let update_info = AltUpdateInfo {
            pubkey,
            accounts: accounts.into_iter().collect(),
            _guard,
            idx: 0,
            written: 0,
        };

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
}
