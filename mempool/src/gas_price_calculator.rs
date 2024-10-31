use std::collections::VecDeque;

use clap::Args;
use reth_primitives::{U128, U64};
use rust_decimal::Decimal;

use crate::gas_prices::{GasPriceModel, PRICES_REFRESH_RATE_MS};

#[derive(Args, Debug, Clone)]
pub struct GasPriceCalculatorConfig {
    #[arg(long, default_value = "10000")]
    pub operator_fee: u128,
    #[arg(long, default_value = "1")]
    pub min_gas_price: u128,
    #[arg(long)]
    pub const_gas_price: Option<u128>,
    #[arg(long, default_value = "10")]
    pub gas_price_min_window: u64,
    #[arg(long, default_value = "1000000000")]
    pub min_acceptable_gas_price: u128,
    #[arg(long, default_value = "15000")]
    pub solana_cu_priority_fee: u128,
    #[arg(long, default_value = "10000")]
    pub solana_simple_cu_priority_fee: u128,
    #[arg(long, default_value = "1000000000")]
    pub min_wo_chain_id_acceptable_gas_price: u128,
}

impl GasPriceCalculatorConfig {
    pub fn min_exec_gas_price_cnt(&self) -> usize {
        (60000 / PRICES_REFRESH_RATE_MS * self.gas_price_min_window * 60000) as usize
    }
}

impl GasPriceCalculatorConfig {
    #[inline]
    /// Returns the `const_gas_price` or the `min_gas_price`, whichever is higher.
    pub fn const_gas_price(&self) -> Option<u128> {
        self.const_gas_price
            .map(|price| price.max(self.min_gas_price))
    }
}

/// Calculates the gas price based on the base token price and the token price.
#[derive(Debug, Clone)]
pub struct GasPriceCalculator {
    pub config: GasPriceCalculatorConfig,
    net_prices: VecDeque<Decimal>,
}

impl GasPriceCalculator {
    pub fn new(config: GasPriceCalculatorConfig) -> Self {
        Self {
            config,
            net_prices: VecDeque::new(),
        }
    }

    pub fn build_gas_price_model(
        &mut self,
        chain_id: u64,
        token_name: String,
        mut base_price_usd: Decimal,
        mut token_price_usd: Decimal,
    ) -> Option<GasPriceModel> {
        if let Some(const_gas_price) = self.config.const_gas_price() {
            base_price_usd.rescale(5);
            token_price_usd.rescale(5);
            return Some(build_gas_price_model(
                chain_id,
                token_name,
                true,
                const_gas_price,
                self.config.min_acceptable_gas_price,
                base_price_usd.mantissa().unsigned_abs(),
                token_price_usd.mantissa().unsigned_abs(),
                &self.config,
            ));
        }

        if token_price_usd == Decimal::ZERO {
            return None;
        }
        let net_price = base_price_usd * Decimal::from(1_000_000_000) / token_price_usd;
        self.net_prices.push_back(net_price);
        if self.net_prices.len() > self.config.min_exec_gas_price_cnt() {
            self.net_prices.pop_front();
        }
        let op_fee =
            Decimal::from(self.config.operator_fee) / Decimal::from(100_000) + Decimal::ONE;
        let suggested_gas_price = net_price * op_fee;
        let min_executable_gas_price = self
            .net_prices
            .iter()
            .min()
            .copied()
            .unwrap_or(Decimal::ZERO);

        base_price_usd.rescale(5);
        token_price_usd.rescale(5);
        // suggested_gas_price.trunc();
        Some(build_gas_price_model(
            chain_id,
            token_name,
            false,
            suggested_gas_price.trunc().mantissa().unsigned_abs(),
            min_executable_gas_price.trunc().mantissa().unsigned_abs(),
            base_price_usd.mantissa().unsigned_abs(),
            token_price_usd.mantissa().unsigned_abs(),
            &self.config,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn build_gas_price_model(
    chain_id: u64,
    token_name: String,
    is_const_gas_price: bool,
    suggested_gas_price: u128,
    min_executable_gas_price: u128,
    base_price_usd: u128,
    token_price_usd: u128,
    config: &GasPriceCalculatorConfig,
) -> GasPriceModel {
    GasPriceModel {
        token_name,
        chain_id: U64::from(chain_id),
        gas_price: U128::from(suggested_gas_price),
        suggested_gas_price: U128::from(suggested_gas_price),
        is_const_gas_price,
        min_acceptable_gas_price: U128::from(config.min_acceptable_gas_price),
        min_executable_gas_price: U128::from(min_executable_gas_price),
        chain_token_price_usd: U128::from(base_price_usd),
        token_price_usd: U128::from(token_price_usd),
        operator_fee: U128::from(config.operator_fee),
        solana_cu_priority_fee: U128::from(config.solana_cu_priority_fee),
        solana_simple_cu_priority_fee: U128::from(config.solana_simple_cu_priority_fee),
        min_wo_chain_id_acceptable_gas_price: U128::from(
            config.min_wo_chain_id_acceptable_gas_price,
        ),
    }
}
