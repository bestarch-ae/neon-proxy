use alloy_consensus::TxEnvelope;
use anyhow::Context;

use common::neon_lib::commands::emulate::EmulateResponse;
use common::solana_sdk::address_lookup_table::AddressLookupTableAccount;
use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::Instruction;
use common::solana_sdk::message::{self, VersionedMessage};
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::transaction::VersionedTransaction;

use crate::ExecuteRequest;

use super::alt::AltInfo;
use super::emulator::{get_chain_id, IterInfo};
use super::holder::HolderInfo;

#[derive(Debug)]
pub(super) struct TxData {
    pub envelope: ExecuteRequest,
    pub emulate: EmulateResponse,
}

impl TxData {
    pub fn new(envelope: ExecuteRequest, emulate: EmulateResponse) -> Self {
        Self { envelope, emulate }
    }
}

#[derive(Debug)]
pub(super) enum TxStage {
    HolderFill {
        info: HolderInfo,
        tx: ExecuteRequest,
    },
    AltFill {
        info: AltInfo,
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
    Final {
        tx_data: Option<TxData>,
        #[allow(dead_code)]
        // We need to hold holder occupied until the end
        holder: Option<HolderInfo>,
    },
}

impl TxStage {
    pub fn ongoing(self, ixs: &[Instruction], payer: &Pubkey) -> OngoingTransaction {
        OngoingTransaction {
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

    pub fn alt_fill(info: AltInfo, tx_data: TxData, holder: Option<HolderInfo>) -> Self {
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

    pub fn operational() -> Self {
        Self::Final {
            tx_data: None,
            holder: None,
        }
    }
}

#[derive(Debug)]
pub struct OngoingTransaction {
    stage: TxStage,
    message: VersionedMessage,
}

impl OngoingTransaction {
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
            TxStage::Final { tx_data: None, .. } => None,
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
            | TxStage::HolderFill { ref tx, .. } => get_chain_id(&tx.tx),
            _ => None,
        }
    }

    pub fn sign(
        &self,
        signers: &[&Keypair],
        blockhash: Hash,
    ) -> anyhow::Result<VersionedTransaction> {
        let mut message = self.message.clone();
        message.set_recent_blockhash(blockhash);
        VersionedTransaction::try_new(message, signers).context("could not sign transactions")
    }

    pub(super) fn disassemble(self) -> TxStage {
        self.stage
    }
}
