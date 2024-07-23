use alloy_consensus::TxEnvelope;
use anyhow::{anyhow, Context};
use common::neon_lib::commands::simulate_solana::SimulateSolanaTransactionResult;
use reth_primitives::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::types::TxParams;
use common::solana_sdk::instruction::Instruction;
use common::solana_sdk::instruction::InstructionError;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::transaction::Transaction;
use common::solana_sdk::transaction::TransactionError;

use crate::convert::ToNeon;
use crate::neon_api::{NeonApi, SimulateConfig};

use super::{MAX_COMPUTE_UNITS, MAX_HEAP_SIZE};

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
}

pub struct Emulator {
    neon_api: NeonApi,
    evm_steps_min: u64,
    payer: Pubkey,
}

impl Emulator {
    pub fn new(neon_api: NeonApi, evm_steps_min: u64, payer: Pubkey) -> Self {
        Self {
            neon_api,
            evm_steps_min,
            payer,
        }
    }

    pub async fn emulate(&self, tx: &TxEnvelope) -> anyhow::Result<EmulateResponse> {
        let request = get_neon_emulate_request(tx)?;
        self.neon_api.emulate(request).await.map_err(Into::into)
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
        let tx = Transaction::new_with_payer(ixs, Some(&self.payer));
        let res = self
            .simulate(&[tx], Some(MAX_COMPUTE_UNITS.into()), Some(MAX_HEAP_SIZE))
            .await?
            .into_iter()
            .next()
            .context("empty simulation result")?;
        if let Some(err) = res.error {
            match err {
                TransactionError::InstructionError(
                    _,
                    InstructionError::ProgramFailedToComplete,
                ) if res.logs.last().map_or(false, |log| {
                    log.ends_with("exceeded CUs meter at BPF instruction")
                }) =>
                {
                    Ok(false)
                }
                error => Err(anyhow!("transaction ({tx_hash}) preflight error: {error}")),
            }
        } else {
            Ok(true)
        }
    }

    pub async fn calculate_iterations(
        &self,
        emulate: &EmulateResponse,
        f: impl FnMut(&mut IterInfo) -> anyhow::Result<Vec<Transaction>>,
    ) -> anyhow::Result<IterInfo> {
        let mut f = f;
        let total_steps = emulate.steps_executed;
        let wrap_iter = self.calculate_wrap_iter_cnt(emulate);
        let mut iter_steps = self.evm_steps_min.max(emulate.steps_executed);
        let max_cu_limit: u64 = (dec!(0.95) * Decimal::from(MAX_COMPUTE_UNITS))
            .round()
            .try_into()
            .expect("rounded and fits");

        for _ in 0..5 {
            if iter_steps <= self.evm_steps_min {
                break;
            }

            let exec_iter =
                (total_steps / iter_steps) + if total_steps % iter_steps > 1 { 1 } else { 0 };
            let iterations = exec_iter + wrap_iter;

            let mut iter_info =
                IterInfo::new(iter_steps as u32, iterations as u32, MAX_COMPUTE_UNITS);
            let txs = f(&mut iter_info)?;
            let res = self.simulate(&txs, Some(u64::MAX), None).await?;

            if res.iter().any(|res| res.error.is_some()) {
                break;
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
                return Ok(iter_info);
            }

            let ratio = dec!(0.9).min(
                Decimal::from(max_cu_limit)
                    .checked_div(used_cu_limit.into())
                    .unwrap_or(Decimal::MAX),
            );
            iter_steps = self
                .evm_steps_min
                .max(ratio.saturating_mul(iter_steps.into()).try_into()?);
        }

        Ok(self.default_iter_info(emulate))
    }

    fn calculate_resize_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        self.calculate_wrap_iter_cnt(emulate).saturating_sub(2)
    }

    fn calculate_exec_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        (emulate.steps_executed + self.evm_steps_min).saturating_sub(1) / self.evm_steps_min
    }

    fn calculate_wrap_iter_cnt(&self, emulate: &EmulateResponse) -> u64 {
        emulate.iterations - self.calculate_exec_iter_cnt(emulate)
    }

    fn default_iter_info(&self, emulate: &EmulateResponse) -> IterInfo {
        let steps_per_iteration = self.evm_steps_min;
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
