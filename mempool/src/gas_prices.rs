use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::DashMap;
use neon_api::NeonApi;
use pyth_sdk_solana::Price;
use reth_primitives::{U128, U64};
use serde::Serialize;
use solana_api::solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tracing::{error, info, warn};

use common::solana_sdk::pubkey::Pubkey;

use crate::error::MempoolError;
use crate::gas_price_calculator::{GasPriceCalculator, GasPriceCalculatorConfig};
use crate::pyth_price_collector::PythPricesCollector;

pub type Symbology = HashMap<String, Pubkey>;

const EVM_CONFIG_REFRESH_RATE_SEC: u64 = 60;
const TARGET_PREC: i32 = -9;
const ONE_BLOCK_MS: u64 = 400;
pub const PRICES_REFRESH_RATE_MS: u64 = 16 * ONE_BLOCK_MS;

#[derive(Debug, Clone)]
pub struct GasPricesConfig {
    /// URL of the Pyth websocket endpoint (could be solana mainnet)
    pub ws_url: String,
    /// Base token symbol in the Pyth symbology
    pub base_token: String,
    /// Default token symbol in the Pyth symbology
    pub default_token: String,
    /// Default chain id
    pub default_chain_id: u64,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GasPriceModel {
    pub token_name: String,
    pub chain_id: U64,
    pub gas_price: U128,
    pub suggested_gas_price: U128,
    pub is_const_gas_price: bool,
    pub min_acceptable_gas_price: U128,
    pub min_executable_gas_price: U128,
    pub chain_token_price_usd: U128,
    pub token_price_usd: U128,
    pub operator_fee: U128,
    #[serde(rename = "solanaCUPriorityFee")]
    pub solana_cu_priority_fee: U128,
    #[serde(rename = "solanaSimpleCUPriorityFee")]
    pub solana_simple_cu_priority_fee: U128,
    #[serde(rename = "minWoChainIDAcceptableGasPrice")]
    pub min_wo_chain_id_acceptable_gas_price: U128,
}

pub trait GasPricesTrait: Clone + Send + Sync + 'static {
    fn get_gas_price(&self, chain_id: Option<u64>) -> u128;
    fn get_gas_price_for_chain(&self, chain_id: u64) -> Option<u128>;
}

/// Gas prices for EVM transactions
#[derive(Clone)]
pub struct GasPrices {
    #[allow(dead_code)]
    prices: Arc<DashMap<Pubkey, Price>>,
    gas_price_models: Arc<DashMap<u64, GasPriceModel>>,
    default_chain_id: u64,
}

impl GasPrices {
    /// Create a new GasPrices instance, which will start a background thread to fetch prices
    /// from Pyth and update them in the `prices` map.
    /// It periodically fetches the EVM config and subscribes to the prices of the tokens in the
    /// config.
    pub fn try_new(
        config: GasPricesConfig,
        neon_api: NeonApi,
        rpc_client: RpcClient,
        symbology: Symbology,
        calculator_config: GasPriceCalculatorConfig,
        chain_token_map: HashMap<u64, String>,
    ) -> Result<Self, MempoolError> {
        let is_const_gas_price = calculator_config.const_gas_price.is_some();

        let base_token_pkey = if is_const_gas_price {
            info!("using const_gas_price");
            symbology
                .get(&config.base_token)
                .copied()
                .unwrap_or_default()
        } else {
            *symbology
                .get(&config.base_token)
                .ok_or(MempoolError::BaseTokenNotFound(config.base_token))?
        };

        let chain_token_map = chain_token_map
            .iter()
            .filter_map(|(&chain_id, token)| {
                let token_pkey = symbology.get(token).copied();
                if is_const_gas_price {
                    token_pkey.map(|tk| Ok((chain_id, (tk, token.clone()))))
                } else {
                    match token_pkey {
                        Some(tk) => Some(Ok((chain_id, (tk, token.clone())))),
                        None => Some(Err(MempoolError::TokenNotFound(token.clone()))),
                    }
                }
            })
            .collect::<Result<HashMap<_, _>, MempoolError>>()?;

        let prices = Arc::new(DashMap::new());

        if !symbology.is_empty() {
            let ws_url = config.ws_url;
            tokio::spawn(update_prices_loop(
                ws_url,
                Arc::downgrade(&prices),
                rpc_client,
                neon_api,
                symbology,
            ));
        }

        let gas_price_models = Arc::new(DashMap::new());
        tokio::spawn(update_price_models_loop(
            calculator_config,
            Arc::downgrade(&prices),
            Arc::downgrade(&gas_price_models),
            base_token_pkey,
            chain_token_map,
        ));

        Ok(Self {
            prices,
            gas_price_models,
            default_chain_id: config.default_chain_id,
        })
    }

    pub fn get_gas_price_model(&self, chain_id: Option<u64>) -> Option<GasPriceModel> {
        let chain_id = chain_id.unwrap_or(self.default_chain_id);
        self.gas_price_models
            .get(&chain_id)
            .map(|r| r.value().clone())
    }
}

impl GasPricesTrait for GasPrices {
    /// Get the gas price for the given chain_id token (or default if not present), or 0 if the
    /// price is not available. Precision is 18 decimal places.
    fn get_gas_price(&self, chain_id: Option<u64>) -> u128 {
        info!(chain_id = ?chain_id, "get gas price");
        let chain_id = chain_id.unwrap_or(self.default_chain_id);
        self.get_gas_price_for_chain(chain_id).unwrap_or(0)
    }

    fn get_gas_price_for_chain(&self, chain_id: u64) -> Option<u128> {
        info!(chain_id, gas_price_models = ?self.gas_price_models, "get gas price for chain");
        self.gas_price_models
            .get(&chain_id)
            .map(|r| r.value().suggested_gas_price.try_into().unwrap())
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

async fn update_prices_loop(
    ws_url: String,
    prices: Weak<DashMap<Pubkey, Price>>,
    rpc_client: RpcClient,
    neon_api: NeonApi,
    symbology: Symbology,
) {
    let mut collector = PythPricesCollector::try_new(&ws_url, prices, rpc_client)
        .await
        .expect("failed to create PythPricesCollector");

    let refresh_rate = Duration::from_secs(EVM_CONFIG_REFRESH_RATE_SEC);
    let mut interval = tokio::time::interval(refresh_rate);
    let mut evm_tokens = HashSet::new();
    loop {
        let evm_config = match neon_api.get_config().await {
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
}

async fn update_price_models_loop(
    calculator_config: GasPriceCalculatorConfig,
    prices: Weak<DashMap<Pubkey, Price>>,
    gas_price_models: Weak<DashMap<u64, GasPriceModel>>,
    base_token_pkey: Pubkey,
    chain_token_map: HashMap<u64, (Pubkey, String)>,
) {
    let mut price_calculator = GasPriceCalculator::new(calculator_config);
    let refresh_rate = Duration::from_millis(PRICES_REFRESH_RATE_MS);
    let mut interval = tokio::time::interval(refresh_rate);

    loop {
        let Some(prices) = prices.upgrade() else {
            info!("prices map dropped, exiting");
            break;
        };
        let Some(gas_price_models) = gas_price_models.upgrade() else {
            info!("gas_price_models dropped, exiting");
            break;
        };
        let Some(base_price_usd) = prices.get(&base_token_pkey) else {
            continue;
        };
        let base_price_usd = adjust_scale(
            base_price_usd.price as u128,
            base_price_usd.expo,
            TARGET_PREC,
        );
        for (&chain_id, (token_pkey, token_name)) in chain_token_map.iter() {
            let Some(token_price_usd) = prices.get(token_pkey) else {
                continue;
            };
            let token_price_usd = adjust_scale(
                token_price_usd.price as u128,
                token_price_usd.expo,
                TARGET_PREC,
            );
            if let Some(gas_model) = price_calculator.build_gas_price_model(
                chain_id,
                token_name.clone(),
                base_price_usd,
                token_price_usd,
            ) {
                gas_price_models.insert(chain_id, gas_model);
            } else {
                warn!(
                    ?chain_id,
                    ?token_price_usd,
                    ?base_price_usd,
                    "get_gas_price: failed to build gas price model"
                );
            }
        }
        interval.tick().await;
    }
}
