use alloy_consensus::TxEnvelope;
use anyhow::Context;
use borsh::BorshDeserialize;
use common::EmulateResponseExt;
use neon_lib::commands::emulate::{EmulateResponse, SolanaAccount};
use reth_primitives::B256;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::compute_budget::{self, ComputeBudgetInstruction};
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{self, legacy, v0, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;

use super::alt::{AltInfo, AltUpdateInfo};
use super::emulator::{get_chain_id, IterInfo};
use super::holder::HolderInfo;
use crate::ExecuteRequest;

#[derive(Debug)]
pub(super) struct TxData {
    pub envelope: ExecuteRequest,
    pub emulate: EmulateResponse,
}

impl TxData {
    pub fn new(envelope: ExecuteRequest, emulate: EmulateResponse) -> Self {
        Self { envelope, emulate }
    }

    pub fn has_external_fail(&self) -> bool {
        self.emulate.has_external_call_fail()
    }
}

#[derive(Debug)]
pub(super) enum TxStage {
    HolderFill {
        info: HolderInfo,
        tx: ExecuteRequest,
    },
    AltFill {
        info: AltUpdateInfo,
        tx_data: TxData,
        holder: Option<HolderInfo>,
    },
    IterativeExecution {
        tx_data: TxData,
        holder: HolderInfo,
        iter_info: Option<IterInfo>,
        alt: Option<AltInfo>,
        from_data: bool,
    },
    DataExecution {
        tx_data: TxData,
        chain_id: u64,
        holder: HolderInfo,
        alt: Option<AltInfo>,
    },
    RecoveredHolder {
        tx_hash: B256,
        chain_id: u64,
        holder: HolderInfo,
        iter_info: IterInfo,
        accounts: Vec<SolanaAccount>,
    },
    Final {
        tx_data: Option<TxData>,
        // We need to hold holder occupied until the end
        holder: Option<HolderInfo>,
    },
    Cancel {
        tx_hash: B256,
        _holder: HolderInfo,
    },
    CreateHolder {
        info: HolderInfo,
    },
    DeleteHolder {
        info: HolderInfo,
    },
}

impl TxStage {
    pub fn ongoing(self, ixs: &[Instruction], payer: &Pubkey) -> OngoingTransaction {
        OngoingTransaction {
            payer: *payer,
            // TODO: by value
            instructions: ixs.to_vec(),
            alt: None,
            stage: self,
            message: VersionedMessage::Legacy(message::legacy::Message::new(ixs, Some(payer))),
        }
    }

    pub fn ongoing_alt(
        self,
        ixs: &[Instruction],
        payer: &Pubkey,
        alt: AddressLookupTableAccount,
    ) -> anyhow::Result<OngoingTransaction> {
        Ok(OngoingTransaction {
            payer: *payer,
            instructions: ixs.to_vec(),
            alt: Some(alt.clone()),
            stage: self,
            message: VersionedMessage::V0(message::v0::Message::try_compile(
                payer,
                ixs,
                &[alt],
                Hash::default(),
            )?),
        })
    }

    pub fn holder_fill(info: HolderInfo, tx: ExecuteRequest) -> Self {
        Self::HolderFill { info, tx }
    }

    pub fn alt_fill(info: AltUpdateInfo, tx_data: TxData, holder: Option<HolderInfo>) -> Self {
        Self::AltFill {
            info,
            tx_data,
            holder,
        }
    }

    pub fn data(tx_data: TxData, chain_id: u64, holder: HolderInfo, alt: Option<AltInfo>) -> Self {
        Self::DataExecution {
            tx_data,
            chain_id,
            holder,
            alt,
        }
    }

    pub fn final_data(tx_data: TxData) -> Self {
        Self::Final {
            tx_data: Some(tx_data),
            holder: None,
        }
    }

    pub fn final_holder(holder: HolderInfo, tx_data: TxData) -> Self {
        Self::Final {
            tx_data: Some(tx_data),
            holder: Some(holder),
        }
    }

    pub fn step_data(
        holder: HolderInfo,
        tx_data: TxData,
        iter_info: Option<IterInfo>,
        alt: Option<AltInfo>,
    ) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info,
            alt,
            from_data: true,
        }
    }

    pub fn step_holder(
        holder: HolderInfo,
        tx_data: TxData,
        iter_info: IterInfo,
        alt: Option<AltInfo>,
    ) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info: Some(iter_info),
            alt,
            from_data: false,
        }
    }

    pub fn recovered_holder(
        tx_hash: B256,
        chain_id: u64,
        holder: HolderInfo,
        iter_info: IterInfo,
        accounts: Vec<SolanaAccount>,
    ) -> Self {
        Self::RecoveredHolder {
            tx_hash,
            chain_id,
            holder,
            iter_info,
            accounts,
        }
    }

    pub fn operational() -> Self {
        Self::Final {
            tx_data: None,
            holder: None,
        }
    }

    pub fn cancel(tx_hash: B256, _holder: HolderInfo) -> Self {
        Self::Cancel { tx_hash, _holder }
    }
}

#[derive(Debug)]
pub struct OngoingTransaction {
    payer: Pubkey,
    instructions: Vec<Instruction>,
    alt: Option<AddressLookupTableAccount>,
    stage: TxStage,
    message: VersionedMessage,
}

impl OngoingTransaction {
    pub fn alt(&self) -> Option<&AltInfo> {
        self.alt.as_ref()
    }

    pub fn cu_limit(&self) -> Option<u32> {
        find_compute_budget(&self.instructions).cu_limit
    }

    pub fn heap_frame(&self) -> Option<u32> {
        find_compute_budget(&self.instructions).heap_frame
    }

    pub fn with_cu_limit(self, limit: u32) -> anyhow::Result<Self> {
        let filter = |ix: &Instruction| {
            matches!(
                ComputeBudgetInstruction::try_from_slice(&ix.data),
                Ok(ComputeBudgetInstruction::SetComputeUnitLimit(_))
            )
        };
        self.adjust_budget(filter, ComputeBudgetInstruction::SetComputeUnitLimit(limit))
    }

    pub fn with_heap_frame(self, limit: u32) -> anyhow::Result<Self> {
        let filter = |ix: &Instruction| {
            matches!(
                ComputeBudgetInstruction::try_from_slice(&ix.data),
                Ok(ComputeBudgetInstruction::RequestHeapFrame(_))
            )
        };
        self.adjust_budget(filter, ComputeBudgetInstruction::RequestHeapFrame(limit))
    }

    fn adjust_budget<F>(mut self, filter: F, ix: ComputeBudgetInstruction) -> anyhow::Result<Self>
    where
        F: FnMut(&Instruction) -> bool,
    {
        let new_ix = Instruction::new_with_borsh(compute_budget::id(), &ix, vec![]);
        let mut filter = filter;
        let Some((idx, target_tx)) = self
            .instructions
            .iter_mut()
            .enumerate()
            .find(|(_idx, ix)| ix.program_id == compute_budget::id() && filter(ix))
        else {
            self.instructions.insert(0, new_ix);
            return self.rebuild_message();
        };
        let data = new_ix.data;
        target_tx.data.clone_from(&data);
        match &mut self.message {
            VersionedMessage::Legacy(legacy::Message { instructions, .. })
            | VersionedMessage::V0(v0::Message { instructions, .. }) => {
                instructions[idx].data = data
            }
        }
        Ok(self)
    }

    pub fn eth_tx(&self) -> Option<&TxEnvelope> {
        match &self.stage {
            TxStage::HolderFill { tx: envelope, .. }
            | TxStage::IterativeExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::AltFill {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::DataExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::Final {
                tx_data: Some(TxData { envelope, .. }),
                ..
            } => Some(&envelope.tx),
            TxStage::Final { tx_data: None, .. }
            | TxStage::RecoveredHolder { .. }
            | TxStage::Cancel { .. }
            | TxStage::CreateHolder { .. }
            | TxStage::DeleteHolder { .. } => None,
        }
    }

    pub fn tx_hash(&self) -> Option<&B256> {
        match &self.stage {
            TxStage::HolderFill { tx: envelope, .. }
            | TxStage::IterativeExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::AltFill {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::DataExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::Final {
                tx_data: Some(TxData { envelope, .. }),
                ..
            } => Some(envelope.tx_hash()),
            TxStage::RecoveredHolder { tx_hash, .. } | TxStage::Cancel { tx_hash, .. } => {
                Some(tx_hash)
            }
            TxStage::Final { tx_data: None, .. }
            | TxStage::DeleteHolder { .. }
            | TxStage::CreateHolder { .. } => None,
        }
    }

    pub fn has_external_call_fail(&self) -> bool {
        match &self.stage {
            TxStage::IterativeExecution { tx_data, .. }
            | TxStage::AltFill { tx_data, .. }
            | TxStage::DataExecution { tx_data, .. }
            | TxStage::Final {
                tx_data: Some(tx_data),
                ..
            } => tx_data.has_external_fail(),
            TxStage::Final { tx_data: None, .. }
            | TxStage::Cancel { .. }
            | TxStage::RecoveredHolder { .. }
            | TxStage::HolderFill { .. }
            | TxStage::DeleteHolder { .. }
            | TxStage::CreateHolder { .. } => false,
        }
    }

    pub fn blockhash(&self) -> &Hash {
        self.message.recent_blockhash() // TODO: None if default?
    }

    pub fn chain_id(&self) -> Option<u64> {
        match self.stage {
            TxStage::IterativeExecution {
                tx_data: TxData {
                    envelope: ref tx, ..
                },
                ..
            }
            | TxStage::DataExecution {
                tx_data: TxData {
                    envelope: ref tx, ..
                },
                ..
            }
            | TxStage::Final {
                tx_data: Some(TxData {
                    envelope: ref tx, ..
                }),
                ..
            }
            | TxStage::AltFill {
                tx_data: TxData {
                    envelope: ref tx, ..
                },
                ..
            }
            | TxStage::HolderFill { ref tx, .. } => get_chain_id(&tx.tx),
            TxStage::RecoveredHolder { chain_id, .. } => Some(chain_id),
            TxStage::Final { tx_data: None, .. }
            | TxStage::Cancel { .. }
            | TxStage::DeleteHolder { .. }
            | TxStage::CreateHolder { .. } => None,
        }
    }

    pub fn sign(
        &mut self,
        signers: &[&impl Signer],
        blockhash: Hash,
    ) -> anyhow::Result<VersionedTransaction> {
        self.message.set_recent_blockhash(blockhash);
        let message = self.message.clone();
        VersionedTransaction::try_new(message, signers).context("could not sign transactions")
    }

    pub(super) fn disassemble(self) -> TxStage {
        self.stage
    }

    pub(super) fn is_alt(&self) -> bool {
        matches!(self.stage, TxStage::AltFill { .. })
    }

    fn rebuild_message(mut self) -> anyhow::Result<Self> {
        if let Some(alt) = self.alt.take() {
            self.stage.ongoing_alt(&self.instructions, &self.payer, alt)
        } else {
            Ok(self.stage.ongoing(&self.instructions, &self.payer))
        }
    }
}

#[derive(Debug, Default)]
struct ComputeBudget {
    cu_limit: Option<u32>,
    heap_frame: Option<u32>,
}

fn find_compute_budget(ixs: &[Instruction]) -> ComputeBudget {
    let mut budget = ComputeBudget::default();
    for ix in ixs
        .iter()
        .filter(|ix| ix.program_id == compute_budget::id())
    {
        match ComputeBudgetInstruction::try_from_slice(&ix.data) {
            Ok(ComputeBudgetInstruction::RequestHeapFrame(n)) => budget.heap_frame = Some(n),
            Ok(ComputeBudgetInstruction::SetComputeUnitLimit(n)) => budget.cu_limit = Some(n),
            Ok(ix) => tracing::warn!(?ix, "unexpected compute budget instruction"),
            Err(error) => {
                tracing::warn!(?error, ?ix, "cannot deserialize compute budget instruction")
            }
        }
    }

    budget
}
