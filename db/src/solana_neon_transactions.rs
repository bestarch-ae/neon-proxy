use futures_util::{Stream, StreamExt};
use sqlx::postgres::PgRow;
use sqlx::FromRow;

use common::types::TxHash;

use crate::{Error, PgPubkey, PgU256};

#[derive(Debug, Clone)]
pub struct SolanaNeonTransactionRepo {
    pool: sqlx::PgPool,
}

impl SolanaNeonTransactionRepo {
    pub async fn fetch_with_costs(
        &self,
        tx_hash: TxHash,
    ) -> impl Stream<Item = Result<SolanaNeonTransactionRowWithCost, Error>> + '_ {
        let tx_hash_array = *tx_hash.as_array();
        sqlx::query_as::<_, SolanaNeonTransactionRowWithCost>(
            r#"
                SELECT DISTINCT
                    t.sol_sig,
                    t.block_slot,
                    t.idx,
                    t.inner_idx,
                    t.ix_code,
                    t.is_success,
                    t.neon_sig,
                    t.neon_step_cnt,
                    t.neon_gas_used,
                    t.neon_total_gas_used,
                    t.max_heap_size,
                    t.used_heap_size,
                    t.max_bpf_cycle_cnt,
                    t.used_bpf_cycle_cnt,
                    c.operator,
                    c.sol_spent
                FROM
                    solana_neon_transactions t
                INNER JOIN
                    solana_blocks AS b
                    ON b.block_slot = t.block_slot
                    AND b.is_active = True
                INNER JOIN
                    solana_transaction_costs AS c
                    ON c.sol_sig = t.sol_sig
                WHERE
                    t.neon_sig = $1
                ORDER BY
                    t.block_slot, t.neon_total_gas_used, t.sol_sig, t.idx, t.inner_idx
            "#,
        )
        .bind(tx_hash_array)
        .fetch(&self.pool)
        .map(|row| row.map_err(Error::from))
    }
}

impl SolanaNeonTransactionRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SolanaNeonTransactionRow {
    pub sol_sig: Vec<u8>,
    pub block_slot: i64,
    pub idx: i32,
    pub inner_idx: Option<i64>,
    pub ix_code: i32,
    pub is_success: bool,
    pub neon_sig: Vec<u8>,
    pub neon_step_cnt: i64,
    pub neon_gas_used: i64,
    pub neon_total_gas_used: i64,
    pub max_heap_size: i32,
    pub used_heap_size: i32,
    pub max_bpf_cycle_cnt: i32,
    pub used_bpf_cycle_cnt: i32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SolanaTransactionCostRow {
    pub sol_sig: Vec<u8>,
    pub block_slot: i64,
    pub operator: PgPubkey,
    pub sol_spent: i64,
}

#[derive(Debug, Clone)]
pub struct SolanaNeonTransactionRowWithCost {
    pub transaction: SolanaNeonTransactionRow,
    pub cost: SolanaTransactionCostRow,
}

impl FromRow<'_, PgRow> for SolanaNeonTransactionRowWithCost {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let transaction = SolanaNeonTransactionRow::from_row(row)?;
        let cost = SolanaTransactionCostRow::from_row(row)?;
        Ok(SolanaNeonTransactionRowWithCost { transaction, cost })
    }
}
