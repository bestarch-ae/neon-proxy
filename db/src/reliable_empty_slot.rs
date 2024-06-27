use super::Error;

#[derive(Debug, Clone)]
pub struct ReliableEmptySlotRepo {
    pool: sqlx::PgPool,
}

impl ReliableEmptySlotRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn update_or_insert(&self, slot: u64) -> Result<(), sqlx::Error> {
        let mut txn = self.pool.begin().await?;
        sqlx::query!("DELETE FROM reliable_empty_slot")
            .execute(&mut *txn)
            .await?;
        sqlx::query!(
            r#"INSERT INTO reliable_empty_slot (block_slot) VALUES ($1)"#,
            slot as i64
        )
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn latest_number(&self, is_finalized: bool) -> Result<u64, Error> {
        let num = sqlx::query!(
            r#"
                SELECT MAX(slot) AS "slot!"
                FROM (
                    SELECT MAX(block_slot) AS slot
                    FROM solana_blocks
                    WHERE is_finalized = $1 OR $2
                    UNION ALL
                    SELECT MAX(block_slot) AS slot
                    FROM reliable_empty_slot
                ) AS combined_slots
            "#,
            is_finalized,
            !is_finalized
        )
        .fetch_one(&self.pool)
        .await?
        .slot as u64;
        Ok(num)
    }
}
