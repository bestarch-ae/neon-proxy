#[derive(Debug, Clone)]
pub struct GasPriceCalculatorConfig {
    pub operator_fee: u128,
    pub min_gas_price: u128,
    pub const_gas_price: Option<u128>,
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
    config: GasPriceCalculatorConfig,
}

impl GasPriceCalculator {
    pub fn new(config: GasPriceCalculatorConfig) -> Self {
        Self { config }
    }

    /// Calculates the gas price based on the base token price and the token price.
    /// Returns `None` if the token price is zero.
    /// The formula is `net_price = base_price_usd / token_price_usd`.
    /// The suggested price is `suggested_price = net_price * (1 + operator_fee)`.
    /// If `const_gas_price` is set, it will be returned.
    #[inline]
    pub fn calculate_gas_price(&self, base_price_usd: u128, token_price_usd: u128) -> Option<u128> {
        tracing::info!(base_price_usd, token_price_usd, "calculate_gas_price");
        if let Some(const_gas_price) = self.config.const_gas_price() {
            return Some(const_gas_price);
        }

        if token_price_usd == 0 {
            return None;
        }

        let net_price = base_price_usd * 1_000_000_000 / token_price_usd;
        let suggested_price = net_price * (100_000 + self.config.operator_fee) / 100_000;
        tracing::info!(
            net_price,
            suggested_price,
            "suggested_price = ({base_price_usd} * 10^9 / {token_price_usd}) * (10^5 + {}) / 10^5",
            self.config.operator_fee
        );
        Some(suggested_price)
    }
}
