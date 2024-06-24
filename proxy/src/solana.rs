use std::sync::Arc;

use common::ethnum::U256;
use common::neon_lib::rpc::{CloneRpcClient, RpcEnum};
use common::neon_lib::types::BalanceAddress;
use common::neon_lib::{commands, NeonError};
use common::solana_sdk::pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::LocalSet;

#[derive(Debug)]
enum Task {
    GetBalance {
        addr: BalanceAddress,
        response: oneshot::Sender<Result<U256, NeonError>>,
    },

    GetTransactionCount {
        addr: BalanceAddress,
        response: oneshot::Sender<Result<u64, NeonError>>,
    },
}

#[derive(Clone)]
struct Context {
    client: CloneRpcClient,
    neon_pubkey: Pubkey,
}

#[derive(Debug, Clone)]
pub struct Solana {
    channel: Sender<Task>,
}

impl Solana {
    pub fn new(url: String, neon_pubkey: Pubkey) -> Self {
        let (tx, mut rx) = mpsc::channel(128);

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            let local = LocalSet::new();

            local.spawn_local(async move {
                let client = RpcClient::new(url);
                let client = CloneRpcClient {
                    rpc: Arc::new(client),
                    max_retries: 10,
                    key_for_config: neon_pubkey, // TODO: wrong key,
                };
                let ctx = Context {
                    client,
                    neon_pubkey,
                };
                while let Some(task) = rx.recv().await {
                    let ctx = ctx.clone();
                    tokio::task::spawn_local(Self::execute(task, ctx));
                }
            });

            rt.block_on(local);
        });

        Self { channel: tx }
    }

    pub async fn get_balance(&self, addr: BalanceAddress) -> Result<U256, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::GetBalance { addr, response: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn get_transaction_count(&self, addr: BalanceAddress) -> Result<u64, NeonError> {
        let (tx, rx) = oneshot::channel();
        self.channel
            .send(Task::GetTransactionCount { addr, response: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    async fn execute(task: Task, ctx: Context) {
        let rpc = RpcEnum::CloneRpcClient(ctx.client.clone());
        match task {
            Task::GetBalance { addr, response } => {
                let resp = commands::get_balance::execute(&rpc, &ctx.neon_pubkey, &[addr]).await;
                tracing::info!(?resp, "get_balance");
                let resp = match resp {
                    Ok(resp) => {
                        let mut balance = common::ethnum::U256::default();
                        for resp in resp {
                            balance = resp.balance;
                        }
                        Ok(balance)
                    }
                    Err(err) => Err(err),
                };
                response.send(resp).unwrap();
            }

            Task::GetTransactionCount { addr, response } => {
                let resp =
                    commands::get_balance::execute(&*ctx.client, &ctx.neon_pubkey, &[addr]).await;
                tracing::info!(?resp, "get_transaction_count");
                let resp = match resp {
                    Ok(resp) => {
                        let mut count = 0;
                        for resp in resp {
                            count = resp.trx_count;
                        }
                        Ok(count)
                    }
                    Err(err) => Err(err),
                };
                response.send(resp).unwrap();
            }
        }
    }
}
