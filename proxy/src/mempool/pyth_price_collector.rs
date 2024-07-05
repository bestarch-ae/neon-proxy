use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use pyth_sdk_solana::state::{load_mapping_account, load_product_account};
use pyth_sdk_solana::{load_price_feed_from_account, Price, PriceFeed};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_config::RpcAccountInfoConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use common::solana_account_decoder::UiAccountEncoding;
use common::solana_sdk::account::Account;
use common::solana_sdk::commitment_config::CommitmentConfig;
use common::solana_sdk::pubkey::Pubkey;

use super::gas_prices::Symbology;
use super::MempoolError;

const ASSET_TYPE_KEY: &str = "asset_type";
const ASSET_TYPE_CRYPTO: &str = "Crypto";
const QUOTE_KEY: &str = "quote_currency";
const BASE_KEY: &str = "base";
const USD: &str = "USD";

const PRICE_AGE_SECS: u64 = 60;

type UnsubscribeFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Collects the symbology from the Pyth mapping account and returns a map of token names to
/// their Pyth price account public keys.
/// Tokens are filtered by the following criteria:
/// - asset_type == "Crypto"
/// - quote_currency == "USD"
/// - base token name is not empty
pub async fn pyth_collect_symbology(
    mapping_addr: &Pubkey,
    rpc_client: &RpcClient,
) -> Result<Symbology, MempoolError> {
    let mut symbology = HashMap::new();
    // request pyth mapping account with available products
    let mapping_raw = rpc_client.get_account_data(mapping_addr).await?;
    let mapping_acct = load_mapping_account(&mapping_raw)?;

    for product_pkey in mapping_acct
        .products
        .iter()
        .filter(|&&pkey| pkey != Pubkey::default())
    {
        let product_raw = rpc_client.get_account_data(product_pkey).await?;
        let product_acct = match load_product_account(&product_raw) {
            Ok(product_acct) => product_acct,
            Err(err) => {
                warn!(?err, "error loading product account");
                continue;
            }
        };

        if product_acct.px_acc == Pubkey::default() {
            continue;
        }

        // we're only interested in crypto tokens priced in USD
        let mut is_crypto = false;
        let mut is_usd = false;
        let mut token_name = None;

        for (key, val) in product_acct.iter() {
            if key == ASSET_TYPE_KEY && val == ASSET_TYPE_CRYPTO {
                is_crypto = true;
            } else if key == QUOTE_KEY && val == USD {
                is_usd = true;
            } else if key == BASE_KEY {
                token_name = Some(val.to_owned());
            }
        }

        if is_crypto && is_usd {
            if let Some(token_name) = token_name {
                symbology.insert(token_name, product_acct.px_acc);
            }
        }
    }

    Ok(symbology)
}

/// Collects the Pyth prices for the tokens in the symbology map.
pub struct PythPricesCollector {
    client: Arc<PubsubClient>,
    prices: Arc<DashMap<Pubkey, Price>>,
    subscriptions: HashMap<Pubkey, JoinHandle<Result<(), MempoolError>>>,
    unsubscribe_fns: HashMap<Pubkey, UnsubscribeFn>,
}

impl PythPricesCollector {
    /// Creates a new PythPricesCollector instance.
    pub async fn try_new(
        ws_url: &str,
        prices: Arc<DashMap<Pubkey, Price>>,
    ) -> Result<Self, MempoolError> {
        let client = Arc::new(PubsubClient::new(ws_url).await?);

        Ok(Self {
            client,
            prices,
            subscriptions: HashMap::new(),
            unsubscribe_fns: HashMap::new(),
        })
    }

    /// Subscribes to the token account and starts collecting the prices.
    pub async fn subscribe(&mut self, token_pkey: Pubkey) -> Result<(), MempoolError> {
        if self.subscriptions.contains_key(&token_pkey) {
            return Ok(());
        }
        let (unsub_tx, mut unsub_rx) = unbounded_channel();
        let subs_handler = tokio::task::spawn_local({
            // From the `PubsubClient docs:
            //   The subscriptions have to be made from the tasks that will receive the subscription
            //   messages, because the subscription streams hold a reference to the `PubsubClient`.
            //   Otherwise, we would just subscribe on the main task and send the receivers out
            //   to other tasks.
            let unsub_tx = unsub_tx.clone();
            let client = Arc::clone(&self.client);
            let token_key = token_pkey;
            let prices = Arc::clone(&self.prices);
            async move {
                let account_info_config = RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..RpcAccountInfoConfig::default()
                };
                // subscribe to the token account for price updates
                let (mut notif, unsub_fn) = client
                    .account_subscribe(&token_key, Some(account_info_config))
                    .await?;
                info!(?token_key, "subscribed to pyth token account");
                unsub_tx
                    .send(unsub_fn)
                    .map_err(|err| MempoolError::TokioSendError(err.to_string()))?;
                drop(unsub_tx);

                // this loop will end once the main task unsubscribes
                while let Some(update) = notif.next().await {
                    let Some(mut sol_acct) = update.value.decode::<Account>() else {
                        warn!(?update.value, "error decoding account data");
                        continue;
                    };
                    let price_feed: PriceFeed =
                        load_price_feed_from_account(&token_key, &mut sol_acct)?;
                    let current_time =
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
                    let Some(price) =
                        price_feed.get_price_no_older_than(current_time, PRICE_AGE_SECS)
                    else {
                        warn!(?token_key, "stale or no price");
                        continue;
                    };
                    prices.insert(token_key, price);
                }
                Ok::<_, MempoolError>(())
            }
        });

        drop(unsub_tx);

        self.subscriptions.insert(token_pkey, subs_handler);
        if let Some(unsub_fn) = unsub_rx.recv().await {
            self.unsubscribe_fns.insert(token_pkey, unsub_fn);
        }

        drop(unsub_rx);

        Ok(())
    }

    /// Unsubscribes from the token account and stops collecting the prices.
    pub async fn unsubscribe(&mut self, token_pkey: Pubkey) {
        if let Some(unsub_fn) = self.unsubscribe_fns.remove(&token_pkey) {
            unsub_fn().await;
        }
        // TODO: do we want to wait the handler?
        self.subscriptions.remove(&token_pkey);
        self.prices.remove(&token_pkey);
    }
}
