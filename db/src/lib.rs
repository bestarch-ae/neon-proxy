mod block;
mod transaction;

use std::str::FromStr;

use anyhow::Context;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::signature::Signature;
use sqlx::PgPool;
use thiserror::Error;

pub use block::{BlockBy, BlockRepo};
pub use transaction::{TransactionBy, TransactionRepo, WithBlockhash};

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
        tx_idx: u32,
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
            ON CONFLICT DO NOTHING
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

#[derive(Clone, Debug)]
pub struct HolderRepo {
    pool: sqlx::PgPool,
}

#[allow(dead_code)]
struct HolderRow {
    block_slot: i64,
    start_block_slot: Option<i64>,
    last_block_slot: Option<i64>,
    is_stuck: bool,
    neon_sig: Option<String>,
    pubkey: String,
    data_offset: Option<i64>,
    data: Option<Vec<u8>>,
}

impl HolderRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_by_pubkey(
        &self,
        pubkey: &Pubkey,
        before_slot: u64,
        tx_idx: u32,
    ) -> Result<Option<Vec<u8>>, sqlx::Error> {
        let rows = sqlx::query_as!(
            HolderRow,
            "SELECT
                 block_slot, start_block_slot, last_block_slot,
                 is_stuck, neon_sig, pubkey, data_offset,
                 data
             FROM neon_holder_log
             WHERE
              pubkey = $1 AND
              block_slot <= $2 AND
              tx_idx <= $3",
            pubkey.to_string(),
            before_slot as i64,
            tx_idx as i32
        )
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut holder_data = Vec::new();
        for row in rows {
            if let (Some(offset), Some(data)) = (row.data_offset, row.data) {
                let offset = offset as usize;
                if offset + data.len() > holder_data.len() {
                    holder_data.resize(offset + data.len(), 0);
                }
                holder_data[offset..offset + data.len()].copy_from_slice(&data);
            } else {
                holder_data.clear();
            }
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert(
        &self,
        slot: u64,
        tx_idx: u32,
        is_stuck: bool,
        neon_sig: Option<&str>,
        pubkey: &Pubkey,
        offset: Option<u64>,
        data: Option<&[u8]>,
    ) -> Result<(), sqlx::Error> {
        let mut txn = self.pool.begin().await?;
        sqlx::query!(
            r#"
            INSERT INTO neon_holder_log
            (
                block_slot,
                tx_idx,
                neon_sig,
                pubkey,
                is_stuck,
                data_offset,
                data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            slot as i64,
            tx_idx as i32,
            neon_sig,
            pubkey.to_string(),
            is_stuck,
            offset.map(|o| o as i64),
            data
        )
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }
}
