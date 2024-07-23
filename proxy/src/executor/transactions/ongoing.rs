use alloy_consensus::TxEnvelope;
use anyhow::Context;

use common::neon_lib::commands::emulate::EmulateResponse;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::Instruction;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::transaction::Transaction;

use super::emulator::IterInfo;
use super::HolderInfo;

#[derive(Debug)]
pub(super) struct TxData {
    pub envelope: TxEnvelope,
    pub emulate: EmulateResponse,
}

impl TxData {
    pub fn new(envelope: TxEnvelope, emulate: EmulateResponse) -> Self {
        Self { envelope, emulate }
    }
}

#[derive(Debug)]
pub(super) enum TxStage {
    Operational,
    HolderFill {
        info: HolderInfo,
        tx: TxEnvelope,
    },
    IterativeExecution {
        tx_data: TxData,
        holder: Pubkey,
        iter_info: Option<IterInfo>,
        from_data: bool,
    },
    SingleExecution {
        tx_data: TxData,
        #[allow(dead_code)]
        holder: Option<Pubkey>,
    },
}

impl TxStage {
    pub fn ongoing(self, ixs: &[Instruction], payer: &Pubkey, chain_id: u64) -> OngoingTransaction {
        OngoingTransaction {
            stage: self,
            tx: Transaction::new_with_payer(ixs, Some(payer)),
            chain_id,
        }
    }

    pub fn holder_fill(info: HolderInfo, tx: TxEnvelope) -> Self {
        Self::HolderFill { info, tx }
    }

    pub fn execute_data(tx_data: TxData) -> Self {
        Self::SingleExecution {
            tx_data,
            holder: None,
        }
    }

    pub fn execute_holder(holder: Pubkey, tx_data: TxData) -> Self {
        Self::SingleExecution {
            tx_data,
            holder: Some(holder),
        }
    }

    pub fn step_data(holder: Pubkey, tx_data: TxData, iter_info: Option<IterInfo>) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info,
            from_data: true,
        }
    }

    pub fn step_holder(holder: Pubkey, tx_data: TxData, iter_info: IterInfo) -> Self {
        Self::IterativeExecution {
            tx_data,
            holder,
            iter_info: Some(iter_info),
            from_data: false,
        }
    }

    pub fn operational() -> Self {
        Self::Operational
    }
}

#[derive(Debug)]
pub struct OngoingTransaction {
    stage: TxStage,
    tx: Transaction,
    chain_id: u64,
}

impl OngoingTransaction {
    pub fn eth_tx(&self) -> Option<&TxEnvelope> {
        match &self.stage {
            TxStage::HolderFill { tx: envelope, .. }
            | TxStage::IterativeExecution {
                tx_data: TxData { envelope, .. },
                ..
            }
            | TxStage::SingleExecution {
                tx_data: TxData { envelope, .. },
                ..
            } => Some(envelope),
            TxStage::Operational => None,
        }
    }

    pub fn blockhash(&self) -> &Hash {
        &self.tx.message.recent_blockhash // TODO: None if default?
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn sign(&mut self, signers: &[&Keypair], blockhash: Hash) -> anyhow::Result<&Transaction> {
        self.tx
            .try_sign(signers, blockhash)
            .context("could not sign transactions")?;
        Ok(&self.tx)
    }

    pub(super) fn disassemble(self) -> (TxStage, u64) {
        (self.stage, self.chain_id)
    }
}
