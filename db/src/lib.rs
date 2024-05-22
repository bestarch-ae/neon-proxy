mod block;
mod transaction;

use std::str::FromStr;

use anyhow::Context;
use common::solana_sdk::signature::Signature;
use sqlx::PgPool;
use thiserror::Error;

pub use block::{BlockBy, BlockRepo};
pub use transaction::{TransactionBy, TransactionRepo};

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(url).await
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("parsing error: {0}")]
    Parse(#[from] anyhow::Error),
}

pub struct SolanaSignaturesRepo {
    pool: sqlx::PgPool,
}

impl SolanaSignaturesRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(
        &self,
        slot: u64,
        tx_idx: u64,
        signature: Signature,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO solana_transaction_signatures
            (
                block_slot,
                tx_idx,
                signature
            ) VALUES($1, $2, $3)
            "#,
            slot as i64,
            tx_idx as i32,
            signature.to_string()
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_latest(&self) -> Result<Option<Signature>, Error> {
        let row = sqlx::query!(
            r#"
            SELECT signature as "signature!" FROM solana_transaction_signatures
            ORDER BY block_slot DESC, tx_idx DESC
            LIMIT 1
            "#
        )
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => Ok(Some(
                Signature::from_str(&row.signature).context("signature")?,
            )),
            None => Ok(None),
        }
    }
}
