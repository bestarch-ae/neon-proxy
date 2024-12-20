mod parse;

use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::Arc;

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Decodable2718;
use alloy_eips::eip2718::Encodable2718;
use anyhow::bail;
use anyhow::Context;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TrySendError;
use evm_loader::account;
use evm_loader::account::Holder;
use evm_loader::types::Transaction;
use reth_primitives::B256;
use solana_sdk::account_info::IntoAccountInfo;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction;
use tracing::debug;
use tracing::{error, info};

use common::neon_instruction::tag;
use solana_api::solana_api::SolanaApi;

use crate::transactions::holder::parse::parse_state;
use crate::transactions::holder::parse::StateData;

const HOLDER_DATA_LEN: usize = 256 * 1024; // neon_proxy.py default
const HOLDER_META_LEN: usize =
    account::ACCOUNT_PREFIX_LEN + mem::size_of::<account::HolderHeader>();
pub const HOLDER_SIZE: usize = HOLDER_META_LEN + HOLDER_DATA_LEN;
const PREFIX: &str = "holder";

#[derive(Debug, Clone, Copy)]
struct HolderMeta {
    idx: u8,
    pubkey: Pubkey,
}

fn holder_seed(idx: u8) -> String {
    format!("{PREFIX}{}", idx)
}

pub(super) struct HolderInfo {
    meta: HolderMeta,
    sender: Sender<HolderMeta>,
    data: Vec<u8>,
    current_offset: usize,
    hash: B256,
    recreate: bool,
}

impl Drop for HolderInfo {
    fn drop(&mut self) {
        match self.sender.try_send(self.meta) {
            Ok(()) | Err(TrySendError::Closed(_)) => (),
            Err(TrySendError::Full(meta)) => panic!("holder do not fit: {}", meta.pubkey),
        }
    }
}

impl fmt::Debug for HolderInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HolderInfo")
            .field("meta", &self.meta)
            .field("data", &hex::encode(&self.data))
            .field("current_offset", &self.current_offset)
            .field("hash", &self.hash)
            .finish()
    }
}

impl HolderInfo {
    pub fn is_empty(&self) -> bool {
        self.current_offset >= self.data.len()
    }

    pub fn pubkey(&self) -> &Pubkey {
        &self.meta.pubkey
    }

    pub fn offset(&self) -> usize {
        self.current_offset
    }

    pub fn hash(&self) -> &B256 {
        &self.hash
    }

    pub fn recreate(&self) -> bool {
        self.recreate
    }
}

#[derive(Debug)]
enum HolderState {
    Incomplete,
    Pending(TxEnvelope),
    State {
        tx_hash: B256,
        chain_id: Option<u64>,
        accounts: Vec<Pubkey>,
    },
    Finalized,
}

impl HolderState {
    fn recoverable(self) -> Option<RecoverableHolderState> {
        match self {
            Self::Incomplete | Self::Finalized => None,
            Self::Pending(tx) => Some(RecoverableHolderState::Pending(tx)),
            Self::State {
                tx_hash,
                chain_id,
                accounts,
            } => Some(RecoverableHolderState::State {
                tx_hash,
                chain_id,
                accounts,
            }),
        }
    }
}

#[derive(Debug)]
pub enum RecoverableHolderState {
    Recreate,
    Pending(TxEnvelope),
    State {
        tx_hash: B256,
        chain_id: Option<u64>,
        accounts: Vec<Pubkey>,
    },
}

#[derive(Debug)]
struct RecoveredHolder {
    meta: HolderMeta,
    state: HolderState,
    recreate: bool,
}

#[derive(Debug)]
pub struct HolderToFinalize {
    pub info: HolderInfo,
    pub state: RecoverableHolderState,
}

#[derive(Debug, Clone)]
pub struct HolderManager {
    operator: Pubkey,
    program_id: Pubkey,
    solana_api: SolanaApi,

    receiver: Receiver<HolderMeta>,
    sender: Sender<HolderMeta>,
    counter: Arc<AtomicU8>,
    max_holders: u8,
    holder_size: usize,
}

#[derive(Debug)]
pub struct AcquireHolder {
    pub info: HolderInfo,
    pub create_ixs: Option<[Instruction; 2]>,
}

impl HolderManager {
    pub fn new(
        operator: Pubkey,
        program_id: Pubkey,
        solana_api: SolanaApi,
        max_holders: u8,
        holder_size: usize,
    ) -> Self {
        let (sender, receiver) = async_channel::bounded(max_holders.into());

        Self {
            operator,
            program_id,
            solana_api,
            receiver,
            sender,
            counter: Arc::new(0.into()),
            max_holders,
            holder_size,
        }
    }

    pub async fn recover(&mut self) -> Vec<HolderToFinalize> {
        let mut output = Vec::new();
        let mut idx = self.counter.fetch_add(1, SeqCst);
        while idx < self.max_holders {
            match self.try_recover_holder(idx).await {
                Ok(None) => {
                    self.counter.store(idx, SeqCst);
                    break;
                }
                Ok(Some(recovered_holder)) => {
                    info!(%self.operator, ?recovered_holder, "discovered holder");
                    let mut info = self.attach_info(recovered_holder.meta, None);
                    info.recreate = recovered_holder.recreate;
                    match recovered_holder.state.recoverable() {
                        None if recovered_holder.recreate => {
                            output.push(HolderToFinalize {
                                info,
                                state: RecoverableHolderState::Recreate,
                            });
                        }
                        None => drop(info),
                        Some(state) => output.push(HolderToFinalize { info, state }),
                    }
                }
                Err(error) => error!(%self.operator, idx, ?error, "could not recover holder"),
            }
            idx = self.counter.fetch_add(1, SeqCst);
        }
        info!(
            %self.operator,
            counter = idx,
            recovered = self.receiver.len(),
            "finished holder recovery"
        );

        output
    }

    pub async fn create_all_remaining(&mut self) -> anyhow::Result<Vec<AcquireHolder>> {
        let mut result = Vec::new();
        while self.counter.load(SeqCst) < self.max_holders {
            let idx = self.counter.load(SeqCst);
            let pubkey = self.holder_key(idx);
            if self.solana_api.get_account(&pubkey).await?.is_some() {
                debug!(%self.operator, idx, %pubkey, "skipping");
                assert_eq!(self.counter.fetch_add(1, SeqCst,), idx);
                continue;
            }

            let info = self.new_holder_info(None);
            let ixs = self.create_holder(&info).await?;
            result.push(AcquireHolder {
                info,
                create_ixs: Some(ixs),
            })
        }

        Ok(result)
    }

    async fn try_recover_holder(&self, idx: u8) -> anyhow::Result<Option<RecoveredHolder>> {
        use common::evm_loader::account::{self, legacy, tag};

        let seed = holder_seed(idx);
        let pubkey = Pubkey::create_with_seed(&self.operator, &seed, &self.program_id)
            .expect("create with seed failed");
        debug!(%self.operator, idx, seed, %pubkey, "requesting holder");
        let Some(mut account) = self.solana_api.get_account(&pubkey).await? else {
            debug!(%self.operator, idx, seed, %pubkey, "holder does not exists");
            return Ok(None);
        };
        debug!(%self.operator, idx, seed, %pubkey, ?account, "holder exists");

        let meta = HolderMeta { idx, pubkey };
        let account_info = (&pubkey, &mut account).into_account_info();
        let holder_size = account_info.data.borrow().len();
        let recreate = holder_size != self.holder_size;
        let state = match tag(&self.program_id, &account_info).context("invalid holder account")? {
            account::TAG_STATE_FINALIZED | legacy::TAG_STATE_FINALIZED_DEPRECATED => {
                HolderState::Finalized
            }
            account::TAG_HOLDER | legacy::TAG_HOLDER_DEPRECATED => {
                let holder = Holder::from_account(&self.program_id, account_info)
                    .context("cannot parse holder")?;
                // [`Transaction::from_rlp`] can panic if first byte is invalid
                let state = if !common::has_valid_tx_first_byte(holder.transaction().as_ref()) {
                    HolderState::Incomplete
                } else {
                    match Transaction::from_rlp(holder.transaction().as_ref())
                        .and_then(|trx| holder.validate_transaction(&trx))
                    {
                        Ok(()) => {
                            let tx_buf = holder.transaction();
                            let tx = TxEnvelope::decode_2718(&mut tx_buf.as_ref())
                                .context("cannot decode transaction from holder")?;
                            HolderState::Pending(tx)
                        }
                        Err(_) => HolderState::Incomplete,
                    }
                };
                state
            }
            account::TAG_STATE | legacy::TAG_STATE_DEPRECATED => {
                let StateData {
                    tx_hash,
                    chain_id,
                    accounts,
                } = parse_state(&self.program_id, &account_info)?;
                HolderState::State {
                    tx_hash,
                    chain_id,
                    accounts,
                }
            }
            n => bail!("invalid holder tag: {n}"),
        };
        if recreate {
            info!(
                ?self.operator,
                idx,
                old_size = holder_size,
                new_size = self.holder_size,
                "holder will be recreated"
            );
        }
        Ok(Some(RecoveredHolder {
            meta,
            state,
            recreate,
        }))
    }

    pub async fn acquire_holder(&self, tx: Option<&TxEnvelope>) -> anyhow::Result<AcquireHolder> {
        let existing = |meta| {
            let info = self.attach_info(meta, tx);
            AcquireHolder {
                info,
                create_ixs: None,
            }
        };

        while let Ok(meta) = self.receiver.try_recv() {
            if self.can_acquire_holder(&meta).await? {
                return Ok(existing(meta));
            }
            debug!(%self.operator, idx = meta.idx, pubkey = %meta.pubkey, "skipping holder");
        }

        loop {
            // TODO: we should have a counter of available holders
            if self.counter.load(SeqCst) < self.max_holders {
                let info = self.new_holder_info(tx);
                let ixs = self.create_holder(&info).await?;
                return Ok(AcquireHolder {
                    info,
                    create_ixs: Some(ixs),
                });
            } else {
                let meta = self.receiver.recv().await.expect("Manager dropped?");
                if self.can_acquire_holder(&meta).await? {
                    return Ok(existing(meta));
                }
            }
        }
    }

    pub async fn is_holder_finalized(&self, key: &Pubkey) -> anyhow::Result<bool> {
        use common::evm_loader::account::{self, legacy, tag};

        let Some(mut account) = self.solana_api.get_account(key).await? else {
            bail!("account not found: {key}")
        };

        let account_info = (key, &mut account).into_account_info();
        let tag = tag(&self.program_id, &account_info).context("invalid holder account")?;
        Ok(matches!(
            tag,
            account::TAG_STATE_FINALIZED | legacy::TAG_STATE_FINALIZED_DEPRECATED
        ))
    }

    fn holder_key(&self, idx: u8) -> Pubkey {
        let seed = holder_seed(idx);
        Pubkey::create_with_seed(&self.operator, &seed, &self.program_id)
            .expect("cannot create holder address")
    }

    fn new_holder_info(&self, tx: Option<&TxEnvelope>) -> HolderInfo {
        let idx = self.counter.fetch_add(1, SeqCst);
        let pubkey = self.holder_key(idx);

        self.attach_info(HolderMeta { idx, pubkey }, tx)
    }

    fn attach_info(&self, meta: HolderMeta, tx: Option<&TxEnvelope>) -> HolderInfo {
        let (hash, data) = if let Some(tx) = tx {
            let mut data = Vec::with_capacity(tx.encode_2718_len());
            tx.encode_2718(&mut &mut data);
            (*tx.tx_hash(), data)
        } else {
            (Default::default(), Vec::new())
        };

        HolderInfo {
            meta,
            sender: self.sender.clone(),
            data,
            current_offset: 0,
            hash,
            recreate: false,
        }
    }

    pub async fn create_holder(&self, holder: &HolderInfo) -> anyhow::Result<[Instruction; 2]> {
        let seed = holder_seed(holder.meta.idx);
        let sp_ix = system_instruction::create_account_with_seed(
            &self.operator,
            &holder.meta.pubkey,
            &self.operator,
            &seed,
            self.solana_api
                .minimum_rent_for_exemption(self.holder_size)
                .await?,
            self.holder_size as u64,
            &self.program_id,
        );

        const TAG_IDX: usize = 0;
        const SEED_LEN_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const SEED_IDX: usize = SEED_LEN_IDX + mem::size_of::<u64>();
        let seed_len = seed.as_bytes().len();

        let mut data = vec![0; SEED_IDX + seed_len];
        data[TAG_IDX] = tag::HOLDER_CREATE;
        data[SEED_LEN_IDX..SEED_IDX].copy_from_slice(&(seed_len as u64).to_le_bytes());
        data[SEED_IDX..].copy_from_slice(seed.as_bytes());

        let accounts = vec![
            AccountMeta::new(holder.meta.pubkey, false),
            AccountMeta::new_readonly(self.operator, true),
        ];

        let neon_ix = Instruction {
            program_id: self.program_id,
            accounts,
            data,
        };

        Ok([sp_ix, neon_ix])
    }

    pub fn delete_holder(&self, holder: &HolderInfo) -> Instruction {
        let data = vec![tag::HOLDER_DELETE; 1];
        let accounts = vec![
            AccountMeta::new(holder.meta.pubkey, false),
            AccountMeta::new_readonly(self.operator, true),
        ];

        Instruction {
            program_id: self.program_id,
            accounts,
            data,
        }
    }

    async fn can_acquire_holder(&self, meta: &HolderMeta) -> anyhow::Result<bool> {
        use common::evm_loader::account::{self, legacy, tag};

        let Some(mut account) = self.solana_api.get_account(&meta.pubkey).await? else {
            error!(%self.operator, idx = meta.idx, pubkey = %meta.pubkey, "holder does not exists");
            return Ok(false);
        };
        let account_info = (&meta.pubkey, &mut account).into_account_info();
        let state = match tag(&self.program_id, &account_info) {
            Ok(tag) => tag,
            Err(err) => {
                error!(%self.operator, idx = meta.idx, pubkey = %meta.pubkey, ?account_info, ?err, "invalid holder state");
                return Ok(false);
            }
        };
        let can_acquire = state == account::TAG_HOLDER
            || state == legacy::TAG_HOLDER_DEPRECATED
            || state == account::TAG_STATE_FINALIZED
            || state == legacy::TAG_STATE_FINALIZED_DEPRECATED;
        if !can_acquire {
            tracing::warn!(%self.operator, idx = meta.idx, pubkey = %meta.pubkey, state, "holder cannot be acquired");
        }
        Ok(can_acquire)
    }

    pub(super) fn write_next_holder_chunk(&self, holder: &mut HolderInfo) -> Instruction {
        const CHUNK_LEN: usize = 930;

        if holder.is_empty() {
            panic!("attempt to write empty holder");
        }

        const TAG_IDX: usize = 0;
        const HASH_IDX: usize = TAG_IDX + mem::size_of::<u8>();
        const OFFSET_IDX: usize = HASH_IDX + mem::size_of::<B256>();
        const DATA_IDX: usize = OFFSET_IDX + mem::size_of::<u64>();

        let chunk_end = holder.data.len().min(holder.current_offset + CHUNK_LEN);
        let chunk_start = holder.current_offset;
        let chunk = &holder.data[chunk_start..chunk_end];
        holder.current_offset = chunk_end;

        let mut data = vec![0; DATA_IDX + chunk.len()];
        data[TAG_IDX] = tag::HOLDER_WRITE;
        data[HASH_IDX..OFFSET_IDX].copy_from_slice(holder.hash.as_slice());
        data[OFFSET_IDX..DATA_IDX].copy_from_slice(&chunk_start.to_le_bytes());
        data[DATA_IDX..].copy_from_slice(chunk);

        let accounts = vec![
            AccountMeta::new(holder.meta.pubkey, false),
            AccountMeta::new(self.operator, true),
        ];

        Instruction {
            program_id: self.program_id,
            accounts,
            data,
        }
    }
}
