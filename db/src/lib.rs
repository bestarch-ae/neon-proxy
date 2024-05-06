use common::types::NeonTxInfo;

pub struct TransactionRepo {
    pool: sqlx::PgPool,
}

impl TransactionRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, tx: &NeonTxInfo) -> Result<(), sqlx::Error> {
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
            format!("0x{}", tx.neon_signature),
            tx.tx_type as i8,
            tx.from.to_string(),
            tx.sol_signature,
            tx.sol_ix_idx as i64,
            tx.sol_ix_inner_idx as i64,
            tx.sol_slot as i64,
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
            tx.contract.map(|c| format!("0x{}", c)),
            format!("{:#0x}", tx.status),
            tx.is_cancelled,
            tx.is_completed,
            "", /* v */
            format!("{:#0x}", tx.transaction.r()),
            format!("{:#0x}", tx.transaction.s()),
            format!("0x{}", hex::encode(tx.transaction.call_data())),
            &[] /* logs */
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
