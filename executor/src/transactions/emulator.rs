use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use alloy_consensus::TxEnvelope;
use anyhow::Context;
use neon_lib::commands::emulate::EmulateResponse;
use neon_lib::commands::simulate_solana::SimulateSolanaTransactionResult;
use neon_lib::types::TxParams;
use reth_primitives::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana_sdk::instruction::{Instruction, InstructionError};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::{Transaction, TransactionError};
use thiserror::Error;

use common::convert::ToNeon;
use neon_api::{NeonApi, SimulateConfig};

use crate::transactions::preflight_error::try_extract_missing_account;

use super::{MAX_COMPUTE_UNITS, MAX_HEAP_SIZE};

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("missing account: {0}")]
    MissingAccount(Pubkey),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

#[derive(Clone, Debug)]
pub(super) struct IterInfo {
    step_count: u32,
    iterations: u32,
    unique_idx: u32,
    cu_limit: u32,
}

impl IterInfo {
    pub fn new(step_count: u32, iterations: u32, cu_limit: u32) -> Self {
        Self {
            step_count,
            iterations,
            unique_idx: 0,
            cu_limit,
        }
    }

    pub fn max(step_count: u32) -> Self {
        Self {
            step_count,
            iterations: u32::MAX,
            unique_idx: 0,
            cu_limit: MAX_COMPUTE_UNITS,
        }
    }

    pub fn next_idx(&mut self) -> u32 {
        let out = self.unique_idx;
        self.unique_idx += 1;
        out
    }

    pub fn is_finished(&self) -> bool {
        self.unique_idx >= self.iterations
    }

    pub fn step_count(&self) -> u32 {
        self.step_count
    }

    pub fn cu_limit(&self) -> u32 {
        self.cu_limit
    }

    pub fn unique_idx(&self) -> u32 {
        self.unique_idx
    }
}

#[derive(Debug)]
pub struct Emulator {
    neon_api: NeonApi,
    evm_steps_min: AtomicU64,
    payer: Pubkey,
}

impl Emulator {
    pub fn new(neon_api: NeonApi, evm_steps_min: u64, payer: Pubkey) -> Self {
        Self {
            neon_api,
            evm_steps_min: evm_steps_min.into(),
            payer,
        }
    }

    pub async fn emulate(&self, tx: &TxEnvelope) -> anyhow::Result<EmulateResponse> {
        let request = get_neon_emulate_request(tx)?;
        let res = self.neon_api.emulate_raw(request).await.map_err(Into::into);
        tracing::info!(?res, tx_hash = %tx.tx_hash(), "neon emulation result");
        res
    }

    pub async fn simulate(
        &self,
        txs: &[Transaction],
        compute_units: Option<u64>,
        heap_size: Option<u32>,
    ) -> anyhow::Result<Vec<SimulateSolanaTransactionResult>> {
        let config = SimulateConfig {
            compute_units,
            heap_size,
            verify: false,
            ..SimulateConfig::default()
        };
        self.neon_api
            .simulate(config, txs)
            .await
            .map_err(Into::into)
    }

    pub fn needs_iterative_execution(&self, emulate: &EmulateResponse) -> bool {
        self.calculate_resize_iter_cnt(emulate) > 0
    }

    pub async fn check_single_execution(
        &self,
        tx_hash: &B256,
        ixs: &[Instruction],
    ) -> anyhow::Result<bool> {
        tracing::info!(%tx_hash, ?ixs, "check_single_execution");
        let tx = Transaction::new_with_payer(ixs, Some(&self.payer));
        let res = self
            .simulate(&[tx], Some(MAX_COMPUTE_UNITS.into()), Some(MAX_HEAP_SIZE))
            .await?
            .into_iter()
            .next()
            .context("empty simulation result")?;
        tracing::debug!(%tx_hash, result = ?res, "solana simulation result");

        if let Some(err) = res.error {
            match err {
                TransactionError::InstructionError(
                    _,
                    InstructionError::ProgramFailedToComplete,
                ) if res
                    .logs
                    .iter()
                    .any(|log| log.ends_with("exceeded CUs meter at BPF instruction")) =>
                {
                    Ok(false)
                }
                // error => Err(anyhow!(
                //     "transaction ({tx_hash}) preflight error: {error}, logs: {:#?}",
                //     res.logs
                // )),
                error => {
                    tracing::warn!(%tx_hash, ?error, logs = ?res.logs, "simulation failed");
                    Ok(true)
                }
            }
        } else {
            Ok(true)
        }
    }

    pub async fn calculate_iterations(
        &self,
        tx_hash: &B256,
        emulate: &EmulateResponse,
        f: impl FnMut(&mut IterInfo) -> anyhow::Result<Vec<Transaction>>,
    ) -> Result<IterInfo, Error> {
        const RETRIES: usize = 10;

        let mut f = f;
        let total_steps = emulate.steps_executed;
        let wrap_iter = self.calculate_wrap_iter_cnt(emulate);
        let mut iter_steps = self.evm_steps_min.load(Relaxed).max(emulate.steps_executed);
        let max_cu_limit: u64 = (dec!(0.95) * Decimal::from(MAX_COMPUTE_UNITS))
            .round()
            .try_into()
            .expect("rounded and fits");
        let mut exec_iter =
            (total_steps / iter_steps) + if total_steps % iter_steps > 1 { 1 } else { 0 };

        for retry in 0..RETRIES {
            if iter_steps <= self.evm_steps_min.load(Relaxed) {
                break;
            }

            let iterations = exec_iter + wrap_iter;
            tracing::debug!(%tx_hash, iter_steps, total_steps, iterations, "testing iter_info");

            let mut iter_info =
                IterInfo::new(iter_steps as u32, iterations as u32, MAX_COMPUTE_UNITS);
            let txs = f(&mut iter_info)?;
            let res = self
                .simulate(&txs, Some(MAX_COMPUTE_UNITS.into()), Some(MAX_HEAP_SIZE))
                .await?;

            let has_errored = res.iter().any(|res| res.error.is_some());
            if has_errored {
                tracing::debug!(%tx_hash, try_idx = retry, "simulation errored");
                for (tx_idx, res) in res.iter().enumerate() {
                    tracing::debug!(%tx_hash, tx_idx, try_idx = retry, ?res, "simulation report");
                    if let Some(key) = res
                        .logs
                        .iter()
                        .rev()
                        .find_map(|log| try_extract_missing_account(log))
                    {
                        return Err(Error::MissingAccount(key));
                    }
                }
            }

            let used_cu_limit = res
                .iter()
                .map(|res| res.executed_units)
                .max()
                .context("empty simulate response")?;

            if used_cu_limit <= max_cu_limit {
                let used_cu_limit =
                    (MAX_COMPUTE_UNITS as u64).min((used_cu_limit / 10_000) * 10_000 + 150_000);

                let iter_info =
                    IterInfo::new(iter_steps as u32, iterations as u32, used_cu_limit as u32);
                tracing::debug!(%tx_hash, retry, ?iter_info, "calculated optimal iterations");
                return Ok(iter_info);
            }

            exec_iter += 1;
            iter_steps =
                (total_steps / exec_iter) + if total_steps % exec_iter > 0 { 1 } else { 0 };
            iter_steps = self.evm_steps_min.load(Relaxed).max(iter_steps);
        }

        tracing::warn!(%tx_hash, RETRIES, "fallback to default iterations");
        Ok(self.default_iter_info(emulate))
    }

    pub(super) fn set_evm_steps_min(&self, evm_steps_min: u64) {
        self.evm_steps_min.store(evm_steps_min, Relaxed);
    }

    pub fn evm_steps_min(&self) -> u64 {
        self.evm_steps_min.load(Relaxed)
    }

    fn calculate_resize_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        self.calculate_wrap_iter_cnt(emulate).saturating_sub(2)
    }

    fn calculate_exec_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        (emulate.steps_executed + self.evm_steps_min.load(Relaxed)).saturating_sub(1)
            / self.evm_steps_min.load(Relaxed)
    }

    fn calculate_wrap_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        emulate.iterations - self.calculate_exec_iter_cnt(emulate)
    }

    pub fn default_iter_info(&self, emulate: &EmulateResponse) -> IterInfo {
        let steps_per_iteration = self.evm_steps_min.load(Relaxed);
        let iterations =
            self.calculate_exec_iter_cnt(emulate) + self.calculate_wrap_iter_cnt(emulate);
        IterInfo::new(
            steps_per_iteration as u32,
            iterations as u32,
            MAX_COMPUTE_UNITS,
        )
    }
}

pub fn get_neon_emulate_request(tx: &TxEnvelope) -> anyhow::Result<TxParams> {
    let from = tx
        .recover_signer()
        .context("could not recover signer")?
        .to_neon();
    let request = match &tx {
        TxEnvelope::Legacy(tx) => TxParams {
            nonce: Some(tx.tx().nonce),
            from,
            to: tx.tx().to.to().copied().map(ToNeon::to_neon),
            data: Some(tx.tx().input.0.to_vec()),
            value: Some(tx.tx().value.to_neon()),
            gas_limit: Some(tx.tx().gas_limit.into()),
            actual_gas_used: None,
            gas_price: Some(tx.tx().gas_price.into()),
            access_list: None,
            chain_id: tx.tx().chain_id,
        },
        TxEnvelope::Eip2930(tx) => TxParams {
            nonce: Some(tx.tx().nonce),
            from,
            to: tx.tx().to.to().copied().map(ToNeon::to_neon),
            data: Some(tx.tx().input.0.to_vec()),
            value: Some(tx.tx().value.to_neon()),
            gas_limit: Some(tx.tx().gas_limit.into()),
            actual_gas_used: None,
            gas_price: Some(tx.tx().gas_price.into()),
            access_list: Some(
                tx.tx()
                    .access_list
                    .iter()
                    .cloned()
                    .map(ToNeon::to_neon)
                    .collect(),
            ),
            chain_id: Some(tx.tx().chain_id),
        },
        tx => anyhow::bail!("unsupported transaction: {:?}", tx.tx_type()),
    };

    Ok(request)
}

pub fn get_chain_id(tx: &TxEnvelope) -> Option<u64> {
    match &tx {
        TxEnvelope::Legacy(tx) => tx.tx().chain_id,
        TxEnvelope::Eip2930(tx) => Some(tx.tx().chain_id),
        _ => None,
    }
}
