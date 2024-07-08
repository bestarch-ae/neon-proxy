use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use pyth_sdk_solana::Price;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tracing::{error, info, warn};

use common::neon_lib::commands;
use common::neon_lib::rpc::CloneRpcClient;
use common::solana_sdk::pubkey::Pubkey;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::mempool::gas_price_calculator::{GasPriceCalculator, GasPriceCalculatorConfig};
use crate::mempool::pyth_price_collector::PythPricesCollector;
use crate::mempool::MempoolError;

pub type Symbology = HashMap<String, Pubkey>;

const EVM_CONFIG_REFRESH_RATE_SEC: u64 = 60;
const RPC_CLIENT_MAX_RETRIES: usize = 10;
const TARGET_PREC: i32 = -9;

/// Gas prices for EVM transactions
#[derive(Clone)]
pub struct GasPrices {
    prices: Arc<DashMap<Pubkey, Price>>,
    price_calculator: GasPriceCalculator,
    base_token_pkey: Pubkey,
    default_token_pkey: Pubkey,
}

impl GasPrices {
    /// Create a new GasPrices instance, which will start a background thread to fetch prices
    /// from Pyth and update them in the `prices` map.
    /// It periodically fetches the EVM config and subscribes to the prices of the tokens in the
    /// config.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        url: &str,
        ws_url: &str,
        neon_pubkey: Pubkey,
        config_key: Pubkey,
        base_token: &str,
        default_token: &str,
        symbology: Symbology,
        calculator_config: GasPriceCalculatorConfig,
    ) -> Result<Self, MempoolError> {
        let prices = Arc::new(DashMap::new());
        let prices_thread = Arc::clone(&prices);
        let url_thread = url.to_owned();
        let ws_url_thread = ws_url.to_owned();

        let base_token_pkey = *symbology
            .get(base_token)
            .ok_or(MempoolError::BaseTokenNotFound(base_token.to_owned()))?;
        let default_token_pkey = *symbology
            .get(default_token)
            .ok_or(MempoolError::DefaultTokenNotFound(default_token.to_owned()))?;

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build rt");
            let local = LocalSet::new();

            local.spawn_local(async move {
                let rpc_client = CloneRpcClient {
                    rpc: Arc::new(RpcClient::new(url_thread)),
                    max_retries: RPC_CLIENT_MAX_RETRIES,
                    key_for_config: config_key,
                };
                let mut collector = PythPricesCollector::try_new(&ws_url_thread, prices_thread)
                    .await
                    .expect("failed to create PythPricesCollector");

                let refresh_rate = Duration::from_secs(EVM_CONFIG_REFRESH_RATE_SEC);
                let mut interval = tokio::time::interval(refresh_rate);
                let mut evm_tokens = HashSet::new();
                loop {
                    let evm_config =
                        match commands::get_config::execute(&rpc_client, neon_pubkey).await {
                            Ok(config) => config,
                            Err(err) => {
                                error!(?err, "failed to get EVM config");
                                interval.tick().await;
                                continue;
                            }
                        };

                    let new_tokens = evm_config
                        .chains
                        .iter()
                        .map(|chain| chain.name.to_uppercase())
                        .collect::<HashSet<_>>();
                    for token in new_tokens.difference(&evm_tokens) {
                        if let Some(token_pkey) = symbology.get(token) {
                            info!(?token, "subscribing to price");
                            if let Err(err) = collector.subscribe(*token_pkey).await {
                                error!(?err, "failed to subscribe to price");
                            }
                        } else {
                            warn!(?token, "subscribing to price: not found in symbology");
                        }
                    }
                    for token in evm_tokens.difference(&new_tokens) {
                        if let Some(token_pkey) = symbology.get(token) {
                            info!(?token, "unsubscribing from price");
                            collector.unsubscribe(*token_pkey).await;
                        } else {
                            warn!(
                                ?token,
                                "unsubscribing from to price: not found in symbology"
                            );
                        }
                    }
                    evm_tokens = new_tokens;
                    interval.tick().await;
                }
            });

            rt.block_on(local);
        });

        Ok(Self {
            prices,
            price_calculator: GasPriceCalculator::new(calculator_config),
            base_token_pkey,
            default_token_pkey,
        })
    }

    /// Get the gas price for the default token, or 0 if the price is not available.
    /// Precision is 18 decimal places.
    pub fn get_gas_price(&self) -> u128 {
        self.get_gas_for_token_pkey(&self.default_token_pkey)
            .unwrap_or(0)
    }

    fn get_gas_for_token_pkey(&self, token_pkey: &Pubkey) -> Option<u128> {
        let Some(base_price_usd) = self.prices.get(&self.base_token_pkey) else {
            warn!(?self.base_token_pkey, "get_gas_price: base token not found in prices");
            return None;
        };
        let Some(token_price_usd) = self.prices.get(token_pkey) else {
            warn!(
                ?token_pkey,
                "get_gas_price: token price not found in prices"
            );
            return None;
        };

        let base_price_usd = adjust_scale(
            base_price_usd.price as u128,
            base_price_usd.expo,
            TARGET_PREC,
        );
        let token_price_usd = adjust_scale(
            token_price_usd.price as u128,
            token_price_usd.expo,
            TARGET_PREC,
        );

        self.price_calculator
            .calculate_gas_price(base_price_usd, token_price_usd)
    }
}

#[allow(clippy::comparison_chain)]
fn adjust_scale(n: u128, old_expo: i32, new_expo: i32) -> u128 {
    if new_expo > old_expo {
        let factor = 10u128.pow((new_expo - old_expo) as u32);
        n / factor
    } else if new_expo < old_expo {
        let factor = 10u128.pow((old_expo - new_expo) as u32);
        n * factor
    } else {
        n
    }
}
