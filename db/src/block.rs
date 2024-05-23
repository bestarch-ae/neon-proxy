use anyhow::Context;
use common::types::SolanaBlock;
use futures_util::FutureExt;

use super::Error;

#[derive(Debug, Clone)]
pub struct BlockRepo {
    pool: sqlx::PgPool,
}

#[derive(Debug)]
pub enum BlockBy<'a> {
    Slot(u64),
    Hash(&'a str),
}

impl BlockRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, block: &SolanaBlock) -> Result<(), sqlx::Error> {
        let mut txn = self.pool.begin().await?;
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
            block.hash,
            block.time.unwrap_or(0),
            block.parent_slot as i64,
            block.parent_hash,
            true,
            true,
        )
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn fetch_by(&self, by: BlockBy<'_>) -> Result<Option<SolanaBlock>, Error> {
        let (slot, hash) = match by {
            BlockBy::Slot(num) => (Some(num as i64), None),
            BlockBy::Hash(hash) => (None, Some(hash)),
        };
        sqlx::query_as!(
            BlockRow,
            r#"SELECT
                block_slot as "block_slot!",
                block_hash as "block_hash!",
                block_time as "block_time!",
                parent_block_slot as "parent_block_slot!",
                parent_block_hash as "parent_block_hash!"
               FROM solana_blocks
               WHERE (block_slot = $1 OR $2) AND (block_hash = $3 OR $4)
            "#,
            slot.unwrap_or(0),
            slot.is_none(),
            hash.unwrap_or(""),
            hash.is_none(),
        )
        .map(TryInto::try_into)
        .fetch_optional(&self.pool)
        .map(|res| Ok(res.map(Option::transpose)??))
        .await
    }
}

struct BlockRow {
    block_slot: i64,
    block_hash: String,
    block_time: i64,
    parent_block_slot: i64,
    parent_block_hash: String,
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
        } = value;
        Ok(SolanaBlock {
            slot: block_slot.try_into().context("slot")?,
            hash: block_hash,
            parent_slot: parent_block_slot.try_into().context("parent_slot")?,
            parent_hash: parent_block_hash,
            time: Some(block_time),
        })
    }
}
