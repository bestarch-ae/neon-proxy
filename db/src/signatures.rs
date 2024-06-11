use std::str::FromStr;

use anyhow::Context;
use common::{solana_sdk::signature::Signature, types::Candidate};

use super::Error;

#[derive(Debug, Clone)]
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

    pub async fn insert_candidates(&self, candidates: Vec<Candidate>) -> Result<(), Error> {
        let slots: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.slot as i64)
            .collect();
        let idxs: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.idx as i32)
            .collect();
        let signs: Vec<_> = candidates
            .into_iter()
            .map(|candidate| candidate.signature)
            .collect();
        sqlx::query!(
            r#"
            INSERT INTO solana_transaction_signatures (
                block_slot,
                tx_idx,
                signature,
                is_processed
            ) VALUES (
                UNNEST($1::bigint[]),
                UNNEST($2::int[]),
                UNNEST($3::text[]),
                FALSE
            )
            "#,
            &slots,
            &idxs,
            &signs
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_candidates(&self, limit: Option<i64>) -> Result<Vec<Candidate>, Error> {
        sqlx::query!(
            r#"
            SELECT block_slot, tx_idx, signature
            FROM solana_transaction_signatures
            WHERE is_processed = FALSE
            ORDER BY (block_slot, tx_idx) ASC
            LIMIT $1
            "#,
            limit.unwrap_or(i64::MAX)
        )
        .map(|row| Candidate {
            signature: row.signature,
            slot: row.block_slot as u64,
            idx: row.block_slot as usize,
        })
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }
}
