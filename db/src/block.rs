use anyhow::Context;
use common::solana_sdk::hash::Hash;
use common::types::SolanaBlock;
use futures_util::FutureExt;
use sqlx::postgres::Postgres;

use crate::PgSolanaBlockHash;

use super::Error;

#[derive(Debug, Clone)]
pub struct BlockRepo {
    pool: sqlx::PgPool,
}

#[derive(Debug)]
pub enum BlockBy {
    Slot(u64),
    Hash(Hash),
}

impl BlockRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(
        &self,
        block: &SolanaBlock,
        txn: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"INSERT INTO solana_blocks (
                block_slot,
                block_hash,
                block_time,
                parent_block_slot,
                parent_block_hash,
                is_finalized,
                is_active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            block.slot as i64,
            PgSolanaBlockHash::from(block.hash) as PgSolanaBlockHash,
            block.time.unwrap_or(0),
            block.parent_slot as i64,
            PgSolanaBlockHash::from(block.parent_hash) as PgSolanaBlockHash,
            block.is_finalized,
            true,
        )
        .execute(&mut **txn)
        .await?;
        Ok(())
    }

    pub async fn finalize(&self, slot: u64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE solana_blocks SET is_finalized = true WHERE block_slot = $1"#,
            slot as i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn purge(&self, slot: u64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"DELETE FROM solana_blocks WHERE block_slot = $1"#,
            slot as i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_by(&self, by: BlockBy) -> Result<Option<SolanaBlock>, Error> {
        let (slot, hash) = match by {
            BlockBy::Slot(num) => (Some(num as i64), None),
            BlockBy::Hash(hash) => (None, Some(PgSolanaBlockHash::from(hash))),
        };
        sqlx::query_as!(
            BlockRow,
            r#"WITH
                current_block_slot AS
                (SELECT block_slot FROM solana_blocks WHERE (block_slot = $1 OR $2) AND (block_hash = $3 OR $4))
               SELECT
                L.block_slot as "block_slot!",
                L.block_hash as "block_hash!: PgSolanaBlockHash",
                L.block_time as "block_time!",
                coalesce(R.block_slot, L.parent_block_slot) as "parent_block_slot!",
                coalesce(R.block_hash, L.parent_block_hash) as "parent_block_hash!: PgSolanaBlockHash",
                L.is_finalized
               FROM solana_blocks L
               LEFT JOIN solana_blocks R ON R.block_slot = L.block_slot - 1
               WHERE L.block_slot in (select block_slot from current_block_slot);
            "#,
            slot.unwrap_or(0),
            slot.is_none(),
            hash.as_ref().map(|hash| hash.0.as_slice()),
            hash.is_none(),
        )
        .map(TryInto::try_into)
        .fetch_optional(&self.pool)
        .map(|res| Ok(res.map(Option::transpose)??))
        .await
    }

    pub async fn latest_number(&self, is_finalized: bool) -> Result<u64, Error> {
        let num = sqlx::query!(
            r#"
                SELECT max(block_slot) as "slot!"
                FROM solana_blocks
                WHERE is_finalized = $1 OR $2
            "#,
            is_finalized,
            !is_finalized
        )
        .fetch_one(&self.pool)
        .await?
        .slot as u64;
        Ok(num)
    }

    pub async fn latest_block_time(&self) -> Result<Option<(u64, u64)>, Error> {
        let num = sqlx::query!(
            r#"SELECT block_slot,block_time as "block_time!"
               FROM solana_blocks
               WHERE block_time IS NOT NULL
               ORDER BY block_slot DESC
               LIMIT 1
              "#
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| (row.block_slot as u64, row.block_time as u64));
        Ok(num)
    }

    pub async fn earliest_slot(&self) -> Result<u64, Error> {
        let num = sqlx::query!(r#"SELECT min(block_slot) as "slot!" FROM solana_blocks"#)
            .fetch_one(&self.pool)
            .await?
            .slot as u64;
        Ok(num)
    }
}

struct BlockRow {
    block_slot: i64,
    block_hash: PgSolanaBlockHash,
    block_time: i64,
    parent_block_slot: i64,
    parent_block_hash: PgSolanaBlockHash,
    is_finalized: bool,
}

impl TryFrom<BlockRow> for SolanaBlock {
    type Error = anyhow::Error;

    fn try_from(value: BlockRow) -> Result<Self, anyhow::Error> {
        let BlockRow {
            block_slot,
            block_hash,
            block_time,
            parent_block_slot,
            parent_block_hash,
            is_finalized,
        } = value;
        Ok(SolanaBlock {
            slot: block_slot.try_into().context("slot")?,
            hash: block_hash.into(),
            parent_slot: parent_block_slot.try_into().context("parent_slot")?,
            parent_hash: parent_block_hash.into(),
            time: Some(block_time),
            is_finalized,
        })
    }
}
