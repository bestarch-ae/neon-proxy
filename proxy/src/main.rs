use clap::Parser;
use jsonrpsee::types::ErrorCode;
use rpc_api::EthApiServer;
use thiserror::Error;

mod convert;
mod rpc;

use rpc::EthApiImpl;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    DB(#[from] db::Error),
    #[error("parse error: {0}")]
    Parse(#[from] anyhow::Error),
}

impl From<Error> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: Error) -> Self {
        match value {
            Error::DB(..) | Error::Parse(..) => ErrorCode::InternalError.into(),
        }
    }
}

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = None, value_name = "POSTGRES_URL")]
    /// Postgres url
    pg_url: String,

    #[arg(
        short,
        long,
        default_value = "127.0.0.1:8888",
        value_name = "LISTEN_ADDR"
    )]
    listen: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let opts = Args::parse();

    let pool = db::connect(&opts.pg_url).await.unwrap();

    let eth = EthApiImpl::new(pool);
    let server = jsonrpsee::server::Server::builder()
        .build(&opts.listen)
        .await
        .unwrap();
    tracing::info!("Listening on {}", opts.listen);
    let handle = server.start(eth.into_rpc());
    handle.stopped().await;
}
