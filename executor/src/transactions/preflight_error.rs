use anyhow::{bail, Context, Result};

use neon_lib::commands::emulate::SolanaAccount;
use solana_api::solana_api::{ClientError, ClientErrorKind};
use solana_api::solana_rpc_client_api::{request, response};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::TransactionError;

use crate::transactions::emulator::get_chain_id;
use crate::transactions::ongoing::TxData;

use super::ongoing::{OngoingTransaction, TxStage};
use super::{TransactionBuilder, MAX_COMPUTE_UNITS, MAX_HEAP_SIZE};

/// Solana preflight error kind.
#[derive(Debug)]
pub enum TxErrorKind {
    CuMeterExceeded,
    TxSizeExceeded,
    AltFail,
    BadExternalCall,
    AlreadyProcessed,
    MissingAccount(Pubkey),
    Other,
}

impl TxErrorKind {
    pub fn from_error(err: &ClientError, tx: &OngoingTransaction) -> Option<Self> {
        match err.kind {
            ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
                code: -32602,
                ref message,
                ..
            }) if message.contains("Transaction too large:") => {
                return Some(TxErrorKind::TxSizeExceeded)
            }
            _ => (),
        };

        if matches!(
            extract_transaction_err(&err.kind),
            Some(TransactionError::AlreadyProcessed)
        ) {
            return Some(Self::AlreadyProcessed);
        }

        if tx.is_alt() {
            return Some(Self::AltFail);
        }

        if let Some(error) = extract_logs(&err.kind).iter().rev().find_map(|log| {
            if log.contains("exceeded CUs meter at BPF instruction") {
                return Some(Self::CuMeterExceeded);
            }
            if let Some(key) = try_extract_missing_account(log) {
                return Some(Self::MissingAccount(key));
            }
            None
        }) {
            return Some(error);
        }

        if tx.has_external_call_fail() {
            return Some(Self::BadExternalCall);
        }

        is_preflight(err.kind()).then_some(Self::Other)
    }
}

fn extract_transaction_err(err: &ClientErrorKind) -> Option<&TransactionError> {
    match err {
        ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
            data:
                request::RpcResponseErrorData::SendTransactionPreflightFailure(
                    response::RpcSimulateTransactionResult { err, .. },
                ),
            ..
        }) => err.as_ref(),
        _ => None,
    }
}

pub fn try_extract_missing_account(log: &str) -> Option<Pubkey> {
    let log = log.strip_prefix("Program log: panicked at 'address ")?;
    let end = log.find(" must be present in the transaction'")?;
    log[0..end].parse().ok()
}

fn extract_logs(err: &ClientErrorKind) -> &'_ [String] {
    match err {
        ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
            data:
                request::RpcResponseErrorData::SendTransactionPreflightFailure(
                    response::RpcSimulateTransactionResult {
                        logs: Some(ref logs),
                        ..
                    },
                ),
            ..
        }) => logs.as_slice(),
        _ => &[],
    }
}

fn is_preflight(err: &ClientErrorKind) -> bool {
    #[allow(clippy::match_like_matches_macro)] // In this case it's more readable
    match err {
        ClientErrorKind::RpcError(request::RpcError::RpcResponseError {
            data: request::RpcResponseErrorData::SendTransactionPreflightFailure(..),
            ..
        }) => true,
        _ => false,
    }
}

/// ## Preflight error handling implementations
impl TransactionBuilder {
    /// Retry exceeded CU Meter error
    pub(super) fn handle_cu_meter(&self, tx: OngoingTransaction) -> Result<OngoingTransaction> {
        let tx_hash = tx.eth_tx().map(|tx| tx.tx_hash()).copied();
        if tx.heap_frame().map_or(false, |n| n == MAX_HEAP_SIZE)
            && tx.cu_limit().map_or(false, |n| n == MAX_COMPUTE_UNITS)
        {
            bail!("CU Meter exceeded for transaction with maximum budget")
        }

        let mut tx = tx;
        if tx.heap_frame().map_or(true, |n| n != MAX_HEAP_SIZE) {
            tracing::warn!(?tx_hash, "retry with max heap");
            tx = tx
                .with_heap_frame(MAX_HEAP_SIZE)
                .context("cannot set heap frame")?;
        }

        if tx.cu_limit().map_or(true, |n| n != MAX_COMPUTE_UNITS) {
            tracing::warn!(?tx_hash, "retry with max CU");
            tx = tx
                .with_cu_limit(MAX_COMPUTE_UNITS)
                .context("cannot set compute units")?;
        }
        Ok(tx)
    }

    /// Retry exceeded Transaction size error
    ///
    /// First we try to save some tx len using holder and then we resort to using ALTs.
    // TODO: A better way would be to decide which method to use first depending on the size
    //     :of the excess, but this would require heavier refactoring which is not an option ATM.
    pub(super) async fn handle_tx_size(
        &self,
        tx: OngoingTransaction,
    ) -> Result<OngoingTransaction> {
        let tx_hash = tx.eth_tx().map(|tx| tx.tx_hash()).copied();
        let has_alt = tx.alt().is_some();
        match tx.disassemble() {
            // Try using holder if not used yet
            // TODO: Possible optimization is to check whether eth tx data size is greater than
            //     : sol tx size excess
            TxStage::DataExecution {
                tx_data,
                ..
            }
            | TxStage::IterativeExecution {
                tx_data,
                from_data: true,
                ..
            }
            | TxStage::Final {
                tx_data: Some(tx_data),
                holder: None,
            } => {
                tracing::warn!(?tx_hash, "Data tx retry from Holder");
                self.start_holder_execution(tx_data.envelope).await
            }

            // Switch to ALT strategy if holder does not help
            TxStage::Final {
                tx_data: Some(tx_data),
                holder: Some(holder),
            } if !has_alt => {
                tracing::warn!(?tx_hash, "tx retry with ALT");
                self.start_from_alt(tx_data, Some(holder)).await
            }
            TxStage::IterativeExecution {
                tx_data,
                holder,
                alt: None,
                from_data: false,
                ..
            } => {
                tracing::warn!(?tx_hash, "tx retry with ALT");
                self.start_from_alt(tx_data, Some(holder)).await
            }

            // Unrecoverable 
            stage @ TxStage::HolderFill { .. }
            | stage @ TxStage::AltFill { .. }
            // Has both holder and ALT
            | stage @ TxStage::IterativeExecution {
                alt: Some(_),
                from_data: false,
                ..
            }
            | stage @ TxStage::Final { tx_data: None, .. } // Not an ETH tx
            | stage @ TxStage::Final { tx_data: Some(_), holder: Some(_) } // Has both holder and ALT
            | stage @ TxStage::RecoveredHolder { .. }
            | stage @ TxStage::RecreateHolder { .. }
            | stage @ TxStage::CreateHolder { .. }
            | stage @ TxStage::DeleteHolder { .. }
            | stage @ TxStage::Cancel { .. } => bail!("cannot shorten tx size: {stage:?}"),
        }
    }

    /// Retry ALT program error
    pub(super) async fn handle_alt(&self, tx: OngoingTransaction) -> Result<OngoingTransaction> {
        match tx.disassemble() {
            TxStage::AltFill {
                info,
                tx_data,
                holder,
            } => {
                let (info, ix) = self.alt_mgr.recreate_alt(info).await?;
                Ok(TxStage::alt_fill(info, tx_data, holder).ongoing(&[ix], &self.pubkey()))
            }
            stage => unreachable!("only alt stages can fail with AltFail: {stage:?}"),
        }
    }

    /// Retry from emulate stage
    pub(super) async fn retry_with_account(
        &self,
        tx: OngoingTransaction,
        pubkey: Pubkey,
    ) -> Result<OngoingTransaction> {
        let add_account = |tx_data: &mut TxData| {
            tx_data.emulate.solana_accounts.push(SolanaAccount {
                pubkey,
                is_writable: true,
                is_legacy: false,
            })
        };
        let tx_hash = tx.eth_tx().map(|tx| tx.tx_hash()).copied();
        tracing::warn!(?tx_hash, "reemulate attempt");
        let alt = tx.alt().cloned();
        match tx.disassemble() {
            TxStage::Final {
                tx_data: Some(mut tx_data),
                holder: None,
            } => {
                add_account(&mut tx_data);
                if let Some(chain_id) = get_chain_id(&tx_data.envelope) {
                    self.dispatch_data_execution_by_version(tx_data, chain_id, alt)
                        .await
                } else {
                    // This is probably unreachable but still exists just in case
                    self.start_holder_execution(tx_data.envelope).await
                }
            }
            TxStage::IterativeExecution {
                mut tx_data,
                holder,
                iter_info: Some(iter_info),
                from_data,
                ..
            } if iter_info.unique_idx() == 1 => {
                add_account(&mut tx_data);
                if let Some(chain_id) = from_data.then(|| get_chain_id(&tx_data.envelope)).flatten()
                {
                    self.dispatch_data_execution_by_version(tx_data, chain_id, alt)
                        .await
                } else if from_data {
                    // This is probably unreachable but still exists just in case
                    self.start_holder_execution(tx_data.envelope).await
                } else {
                    self.execute_from_holder_emulated(holder, tx_data, alt)
                        .await
                }
            }
            TxStage::IterativeExecution {
                tx_data,
                holder,
                iter_info: Some(_),
                alt,
                ..
            } => {
                tracing::warn!(?tx_hash, "cancelling transaction");
                self.cancel(tx_data, holder, alt)
            }
            TxStage::Final {
                tx_data: Some(mut tx_data),
                holder: Some(holder),
            } => {
                add_account(&mut tx_data);
                self.execute_from_holder_emulated(holder, tx_data, alt)
                    .await
            }
            stage => bail!("invalid stage for reemulation: {stage:?}"),
        }
    }

    /// Generic preflight error handling logic.
    ///
    /// Fallback to iterative execution first and then proceed to cancel
    /// in case the error still persist.
    pub(super) async fn handle_preflight_error(
        &self,
        tx: OngoingTransaction,
    ) -> Result<OngoingTransaction> {
        let tx_hash = tx.eth_tx().map(|tx| tx.tx_hash()).copied();

        let alt = tx.alt().cloned();
        match tx.disassemble() {
            TxStage::DataExecution { tx_data, .. }
            | TxStage::Final {
                tx_data: Some(tx_data),
                holder: None,
            } => {
                // This will probably result in iterative fallback with cancel
                tracing::warn!(?tx_hash, "Data tx retry from Holder");
                // TODO: Maybe fallback to data iterative?
                //     : But we need to init holder anyway
                self.start_holder_execution(tx_data.envelope).await
            }
            TxStage::Final {
                tx_data: Some(tx_data),
                holder: Some(holder),
            } => {
                tracing::warn!(?tx_hash, "Fallback to iterative");
                self.step(None, tx_data, holder, false, alt.clone())
                    .await
                    .transpose()
                    .expect("always some for first iter")
            }
            TxStage::IterativeExecution {
                tx_data, holder, ..
            } => {
                tracing::warn!(?tx_hash, "cancelling transaction");
                self.cancel(tx_data, holder, alt)
            }
            stage @ TxStage::HolderFill { .. }
            | stage @ TxStage::AltFill { .. }
            | stage @ TxStage::Final { .. }
            | stage @ TxStage::RecoveredHolder { .. }
            | stage @ TxStage::Cancel { .. }
            | stage @ TxStage::RecreateHolder { .. }
            | stage @ TxStage::CreateHolder { .. }
            | stage @ TxStage::DeleteHolder { .. } => {
                bail!("{stage:?} cannot fallback to iterative")
            }
        }
    }
}
