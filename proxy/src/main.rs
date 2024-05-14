use clap::Parser;
use rpc_api::EthApiServer;

mod db;
mod rpc;

use rpc::EthApiImpl;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    /// Postgres url
    pg_url: String,

    #[arg(short, long, default_value = "127.0.0.1:8888", value_name = "LISTEN_ADDR")]
    listen: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let opts = Args::parse();

    let pool = db::connect(&opts.pg_url)
        .await
        .unwrap();

    let transactions = db::TransactionRepo::new(pool);
    let eth = EthApiImpl::new(transactions);
    let server = jsonrpsee::server::Server::builder()
        .build(&opts.listen)
        .await
        .unwrap();
    tracing::info!("Listening on {}", opts.listen);
    let handle = server.start(eth.into_rpc());
    handle.stopped().await;
}