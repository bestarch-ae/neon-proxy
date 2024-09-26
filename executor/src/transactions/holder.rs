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
use reth_primitives::B256;

use common::evm_loader::account;
use common::evm_loader::account::Holder;
use common::evm_loader::types::Transaction;
use common::neon_instruction::tag;
use common::solana_sdk::account_info::IntoAccountInfo;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::system_instruction;
use solana_api::solana_api::SolanaApi;

use crate::transactions::holder::parse::parse_state;
use crate::transactions::holder::parse::StateData;

const HOLDER_DATA_LEN: usize = 256 * 1024; // neon_proxy.py default
const HOLDER_META_LEN: usize =
    account::ACCOUNT_PREFIX_LEN + mem::size_of::<account::HolderHeader>();
const HOLDER_SIZE: usize = HOLDER_META_LEN + HOLDER_DATA_LEN;
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
    Pending(TxEnvelope),
    State {
        tx_hash: B256,
        chain_id: Option<u64>,
        accounts: Vec<Pubkey>,
    },
}

impl RecoverableHolderState {
    pub fn tx_hash(&self) -> &B256 {
        match self {
            Self::Pending(tx) => tx.tx_hash(),
            Self::State { tx_hash, .. } => tx_hash,
        }
    }
}

#[derive(Debug)]
struct RecoveredHolder {
    meta: HolderMeta,
    state: HolderState,
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
        }
    }

    pub async fn recover(&mut self) -> anyhow::Result<Vec<HolderToFinalize>> {
        let mut output = Vec::new();
        let mut idx = self.counter.fetch_add(1, SeqCst);
        while idx < self.max_holders {
            match self
                .try_recover_holder(idx)
                .await
                .inspect_err(|error| tracing::error!(idx, ?error, "could not recover holder"))?
            {
                None => {
                    tracing::debug!(idx, "stopping holder recovery");
                    self.counter.store(idx, SeqCst);
                    break;
                }
                Some(recovered_holder) => {
                    tracing::debug!(?recovered_holder, "discovered holder");
                    let info = self.attach_info(recovered_holder.meta, None);
                    match recovered_holder.state.recoverable() {
                        None => drop(info),
                        Some(state) => output.push(HolderToFinalize { info, state }),
                    }
                }
            }
            idx = self.counter.fetch_add(1, SeqCst);
        }

        Ok(output)
    }

    async fn try_recover_holder(&self, idx: u8) -> anyhow::Result<Option<RecoveredHolder>> {
        use common::evm_loader::account::{self, legacy, tag};

        let seed = holder_seed(idx);
        let pubkey = Pubkey::create_with_seed(&self.operator, &seed, &self.program_id)
            .expect("create with seed failed");
        let Some(mut account) = self.solana_api.get_account(&pubkey).await? else {
            return Ok(None);
        };

        let meta = HolderMeta { idx, pubkey };
        let account_info = (&pubkey, &mut account).into_account_info();
        let state = match tag(&self.program_id, &account_info).context("invalid holder account")? {
            account::TAG_STATE_FINALIZED | legacy::TAG_STATE_FINALIZED_DEPRECATED => {
                HolderState::Finalized
            }
            account::TAG_HOLDER | legacy::TAG_HOLDER_DEPRECATED => {
                let holder = Holder::from_account(&self.program_id, account_info)
                    .context("cannot parse holder")?;
                let state = match Transaction::from_rlp(holder.transaction().as_ref())
                    .and_then(|trx| holder.validate_transaction(&trx))
                {
                    Ok(()) => {
                        let tx_buf = holder.transaction();
                        let tx = TxEnvelope::decode_2718(&mut tx_buf.as_ref())
                            .context("cannot decode transaction from holder")?;
                        HolderState::Pending(tx)
                    }
                    Err(_) => HolderState::Incomplete,
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
            n => anyhow::bail!("invalid holder tag: {n}"),
        };
        Ok(Some(RecoveredHolder { meta, state }))
    }

    pub async fn acquire_holder(&self, tx: Option<&TxEnvelope>) -> anyhow::Result<AcquireHolder> {
        let existing = |meta| {
            let info = self.attach_info(meta, tx);
            AcquireHolder {
                info,
                create_ixs: None,
            }
        };

        if let Ok(meta) = self.receiver.try_recv() {
            return Ok(existing(meta));
        }

        if self.counter.load(SeqCst) < self.max_holders {
            let info = self.new_holder_info(tx);
            let ixs = self.create_holder(&info).await?;
            Ok(AcquireHolder {
                info,
                create_ixs: Some(ixs),
            })
        } else {
            let meta = self.receiver.recv().await.expect("Manager dropped?");
            Ok(existing(meta))
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

    fn new_holder_info(&self, tx: Option<&TxEnvelope>) -> HolderInfo {
        let idx = self.counter.fetch_add(1, SeqCst);
        let seed = holder_seed(idx);
        let pubkey = Pubkey::create_with_seed(&self.operator, &seed, &self.program_id)
            .expect("cannot create holder address");

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
        }
    }

    async fn create_holder(&self, holder: &HolderInfo) -> anyhow::Result<[Instruction; 2]> {
        let seed = holder_seed(holder.meta.idx);
        let sp_ix = system_instruction::create_account_with_seed(
            &self.operator,
            &holder.meta.pubkey,
            &self.operator,
            &seed,
            self.solana_api
                .minimum_rent_for_exemption(HOLDER_SIZE)
                .await?,
            HOLDER_SIZE as u64,
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
