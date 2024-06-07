use std::str::FromStr;

use anyhow::Context;
use common::solana_sdk::signature::Signature;

use super::Error;

pub struct SolanaSignaturesRepo {
    pool: sqlx::PgPool,
}

impl SolanaSignaturesRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_processed(
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
                signature,
                is_processed
            ) VALUES($1, $2, $3, TRUE)
            ON CONFLICT (block_slot, tx_idx)
            DO UPDATE SET is_processed = TRUE
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
            SELECT signature as "signature!"
            FROM solana_transaction_signatures
            WHERE is_processed
            ORDER BY (block_slot, tx_idx) DESC
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
