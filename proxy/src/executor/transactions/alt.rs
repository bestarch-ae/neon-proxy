use anyhow::{bail, Context};

use common::solana_sdk::address_lookup_table::{self, AddressLookupTableAccount};
use common::solana_sdk::commitment_config::CommitmentLevel;
use common::solana_sdk::instruction::Instruction;
use common::solana_sdk::pubkey::Pubkey;

use super::holder::HolderInfo;
use super::ongoing::{OngoingTransaction, TxData, TxStage};
use super::TransactionBuilder;

const ACCOUNTS_PER_TX: usize = 27;

#[derive(Debug, Clone)]
pub struct AltInfo {
    pubkey: Pubkey,
    accounts: Vec<Pubkey>,
    idx: usize,
}

impl AltInfo {
    pub fn is_empty(&self) -> bool {
        self.idx >= self.accounts.len()
    }

    /// TODO: This is only valid until ALT reuse is implemented
    pub fn into_account(self) -> AddressLookupTableAccount {
        AddressLookupTableAccount {
            key: self.pubkey,
            addresses: self.accounts,
        }
    }
}

impl TransactionBuilder {
    pub(super) async fn start_from_alt(
        &self,
        tx_data: TxData,
        holder: Option<HolderInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let (ix, pubkey) = self.create_alt_ix().await?;
        let info = AltInfo {
            pubkey,
            idx: 0,
            accounts: tx_data
                .emulate
                .solana_accounts
                .iter()
                .map(|acc| acc.pubkey)
                .collect(),
        };
        tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), ?info, ?holder, "creating new ALT");

        Ok(TxStage::alt_fill(info, tx_data, holder).ongoing(&[ix], &self.pubkey()))
    }

    pub(super) fn fill_alt(
        &self,
        info: AltInfo,
        tx_data: TxData,
        holder: Option<HolderInfo>,
    ) -> anyhow::Result<OngoingTransaction> {
        let idx_before = info.idx;
        let mut info = info;
        let ix = self.write_next_alt_chunk(&mut info)?;
        tracing::debug!(tx_hash = %tx_data.envelope.tx_hash(), ?info, idx_before, ?holder, "write next ALT chunk");
        Ok(TxStage::alt_fill(info, tx_data, holder).ongoing(&[ix], &self.pubkey()))
    }

    async fn create_alt_ix(&self) -> anyhow::Result<(Instruction, Pubkey)> {
        Ok(address_lookup_table::instruction::create_lookup_table(
            self.pubkey(),
            self.pubkey(),
            self.solana_api
                .get_slot(CommitmentLevel::Finalized)
                .await
                .context("failed requesting recent slot")?,
        ))
    }

    fn write_next_alt_chunk(&self, info: &mut AltInfo) -> anyhow::Result<Instruction> {
        if info.is_empty() {
            bail!("attempt to write filled ALT info: {info:?}");
        }

        let from = info.idx;
        let to = info.accounts.len().min(from + ACCOUNTS_PER_TX);
        info.idx = to;
        Ok(address_lookup_table::instruction::extend_lookup_table(
            info.pubkey,
            self.pubkey(),
            Some(self.pubkey()),
            info.accounts[from..to].to_vec(),
        ))
    }
}
