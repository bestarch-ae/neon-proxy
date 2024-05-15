use std::str::FromStr;

use common::solana_sdk::signature::Signature;
use common::types::{NeonTxInfo, SolanaBlock};
use sqlx::PgPool;

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(url).await
}

pub struct TransactionRepo {
    pool: sqlx::PgPool,
}

impl TransactionRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, tx: &NeonTxInfo) -> Result<(), sqlx::Error> {
        let block_slot = tx.sol_slot as i64;
        let tx_hash = format!("0x{}", tx.neon_signature);
        let tx_idx = tx.sol_tx_idx as i32;
        let sol_sig = &tx.sol_signature.to_string();
        let sol_idx = tx.sol_ix_idx as i32;
        let sol_inner_idx = tx.sol_ix_inner_idx as i32;

        let mut txn = self.pool.begin().await?;

        let mut tx_log_idx = 0;

        for log in &tx.events {
            /* not a real eth event */
            if log.is_hidden || log.topic_list.is_empty() {
                continue;
            }

            let topic1 = log
                .topic_list
                .first()
                .map(|t| format!("{:#0x}", t))
                .unwrap_or_default();
            let topic2 = log
                .topic_list
                .get(1)
                .map(|t| format!("{:#0x}", t))
                .unwrap_or_default();
            let topic3 = log
                .topic_list
                .get(2)
                .map(|t| format!("{:#0x}", t))
                .unwrap_or_default();
            let topic4 = log
                .topic_list
                .get(3)
                .map(|t| format!("{:#0x}", t))
                .unwrap_or_default();

            sqlx::query!(
                r#"
            INSERT INTO neon_transaction_logs
            (
                address,
                block_slot,
                tx_hash,
                tx_idx,
                tx_log_idx,
                log_idx,
                event_level,
                event_order,
                sol_sig,
                idx,
                inner_idx,
                log_topic1,
                log_topic2,
                log_topic3,
                log_topic4,
                log_topic_cnt,
                log_data
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            "#,
                log.address.map(|a| a.to_string()).unwrap_or_default(),
                block_slot,
                tx_hash,
                tx_idx,
                tx_log_idx,
                log.log_idx as i64,
                log.level as i64,
                log.order as i64,
                &sol_sig,
                sol_idx,
                sol_inner_idx,
                topic1,
                topic2,
                topic3,
                topic4,
                log.topic_list.len() as i32,
                hex::encode(&log.data)
            )
            .execute(&mut *txn)
            .await?;

            tx_log_idx += 1;
        }

        sqlx::query!(
            r#"
            INSERT INTO neon_transactions
            (
                neon_sig,
                tx_type,
                from_addr,
                sol_sig,
                sol_ix_idx,
                sol_ix_inner_idx,
                block_slot,
                tx_idx,
                nonce,
                gas_price,
                gas_limit,
                value,
                gas_used,
                sum_gas_used,
                to_addr,
                contract,
                status,
                is_canceled,
                is_completed,
                v, r, s,
                calldata,
                logs
            )
            VALUES($1, $2, $3, $4, $5, $6,
                   $7, $8, $9, $10, $11, $12,
                   $13, $14, $15, $16, $17, $18,
                   $19, $20, $21, $22, $23, $24)
            "#,
            tx_hash,
            tx.tx_type as i32,
            tx.from.to_string(),
            sol_sig,
            tx.sol_ix_idx as i64,
            tx.sol_ix_inner_idx as i64,
            block_slot,
            tx.sol_tx_idx as i64,
            format!("{:#0x}", tx.transaction.nonce()),
            format!("{:#0x}", tx.transaction.gas_price()),
            format!("{:#0x}", tx.transaction.gas_limit()),
            format!("{:#0x}", tx.transaction.value()),
            format!("{:#0x}", tx.gas_used),
            format!("{:#0x}", tx.sum_gas_used),
            tx.transaction
                .target()
                .map(|x| x.to_string())
                .unwrap_or_default(),
            tx.contract.map(|c| c.to_string()),
            format!("{:#0x}", tx.status),
            tx.is_cancelled,
            tx.is_completed,
            format!("{:#0x}", tx.transaction.recovery_id()),
            format!("{:#0x}", tx.transaction.r()),
            format!("{:#0x}", tx.transaction.s()),
            format!("0x{}", hex::encode(tx.transaction.call_data())),
            &[] /* logs */
        )
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }
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

    pub async fn get_latest(&self) -> Result<Option<Signature>, sqlx::Error> {
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
            Some(row) => Ok(Some(Signature::from_str(&row.signature).unwrap())),
            None => Ok(None),
        }
    }
}

pub struct BlockRepo {
    pool: sqlx::PgPool,
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
}
