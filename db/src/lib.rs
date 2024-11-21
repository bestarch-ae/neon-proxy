mod alt;
mod block;
mod reliable_empty_slot;
mod solana_neon_transactions;
mod transaction;

use std::str::FromStr;

use anyhow::Context;
use num_traits::ToPrimitive;
use sqlx::postgres::Postgres;
use thiserror::Error;

use common::ethnum::U256;
use common::evm_loader::types::Address;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::{hash::Hash, pubkey::Pubkey};
use common::types::HolderOperation;

pub use alt::AltRepo;
pub use block::{BlockBy, BlockRepo};
pub use reliable_empty_slot::ReliableEmptySlotRepo;
pub use solana_neon_transactions::SolanaNeonTransactionRepo;
pub use sqlx::PgPool;
pub use transaction::{RichLogBy, TransactionBy, TransactionRepo, WithBlockhash};

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    tracing::info!(%url, "connecting to database");

    let pool = PgPool::connect(url).await?;
    let migrations = sqlx::migrate!("../schemas/migrations");
    tracing::info!("running migrations");
    migrations.run(&pool).await?;
    tracing::info!("migrations successful");
    Ok(pool)
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("parsing error: {0}")]
    Parse(#[from] anyhow::Error),
}

#[derive(sqlx::Type, Copy, Clone, Debug, Default)]
#[sqlx(type_name = "SolanaBlockHash", transparent)]
pub(crate) struct PgSolanaBlockHash([u8; 32]);

impl From<Hash> for PgSolanaBlockHash {
    fn from(value: Hash) -> Self {
        Self(value.to_bytes())
    }
}

impl From<PgSolanaBlockHash> for Hash {
    fn from(value: PgSolanaBlockHash) -> Self {
        Hash::new_from_array(value.0)
    }
}

pub struct SolanaSignaturesRepo {
    pool: sqlx::PgPool,
}

fn u256_to_bytes(val: &U256) -> [u8; 32] {
    let (hi, lo) = val.into_words();
    let mut buf = [0; 32];
    buf[0..16].copy_from_slice(&lo.to_le_bytes());
    buf[16..32].copy_from_slice(&hi.to_le_bytes());
    buf
}

#[derive(sqlx::Type, Clone, Debug, Default)]
#[sqlx(type_name = "U256", transparent)]
pub struct PgU256(sqlx::types::BigDecimal);

impl PgU256 {
    pub fn as_u64(&self) -> u64 {
        self.0.to_u64().expect("value exceeds u64 range")
    }
}

impl From<sqlx::types::BigDecimal> for PgU256 {
    fn from(val: sqlx::types::BigDecimal) -> Self {
        Self(val)
    }
}

impl From<U256> for PgU256 {
    fn from(val: U256) -> Self {
        let buf = u256_to_bytes(&val);
        let bigint = num_bigint::BigInt::from_bytes_le(num_bigint::Sign::Plus, &buf);

        PgU256(sqlx::types::BigDecimal::new(bigint, 0))
    }
}

impl From<PgU256> for U256 {
    fn from(value: PgU256) -> Self {
        use std::str::FromStr;
        U256::from_str(&value.0.to_string()).unwrap()
    }
}

#[derive(sqlx::Type, Copy, Clone, Debug, Default)]
#[sqlx(type_name = "Pubkey", transparent, no_pg_array)]
pub struct PgPubkey([u8; 32]);

impl From<Pubkey> for PgPubkey {
    fn from(pubkey: Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl From<PgPubkey> for Pubkey {
    fn from(value: PgPubkey) -> Self {
        value.0.into()
    }
}

#[derive(sqlx::Type, Copy, Clone, Debug, Default)]
#[sqlx(type_name = "Address", transparent, no_pg_array)]
pub struct PgAddress([u8; 20]);

impl From<Address> for PgAddress {
    fn from(addr: Address) -> Self {
        Self(addr.0)
    }
}

impl From<PgAddress> for Address {
    fn from(value: PgAddress) -> Self {
        Address(value.0)
    }
}

impl From<Vec<u8>> for PgAddress {
    fn from(val: Vec<u8>) -> Self {
        assert_eq!(val.len(), 20);
        let mut buf = [0; 20];
        buf.copy_from_slice(&val);
        PgAddress(buf)
    }
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
        txn: &mut sqlx::Transaction<'_, Postgres>,
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
        .execute(&mut **txn)
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

    pub async fn get_stuck(&self) {}

    pub async fn get_by_pubkey(
        &self,
        pubkey: &Pubkey,
        before_slot: u64,
        tx_idx: u32,
    ) -> Result<Vec<HolderOperation>, sqlx::Error> {
        tracing::info!(%pubkey, before_slot, tx_idx, "looking for holder");
        let rows = sqlx::query_as!(
            HolderRow,
            "SELECT
                 block_slot, start_block_slot, last_block_slot,
                 is_stuck, neon_sig, pubkey, data_offset,
                 data
             FROM neon_holder_log
             WHERE
              pubkey = $1 AND
              ((block_slot < $2) OR (block_slot = $2 AND tx_idx <= $3))
             ORDER BY block_slot",
            pubkey.to_string(),
            before_slot as i64,
            tx_idx as i32
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            tracing::info!(%pubkey, before_slot, tx_idx, "holder not found");
            return Ok(Vec::new());
        }
        let mut operations = Vec::new();
        for row in rows {
            let pubkey = row.pubkey.parse().unwrap();
            if let (Some(offset), Some(data), Some(neon_sig)) =
                (row.data_offset, row.data, row.neon_sig)
            {
                let mut tx_hash = [0; 32];
                hex::decode_to_slice(neon_sig, &mut tx_hash).unwrap();
                let offset = offset as usize;
                let op = HolderOperation::Write {
                    pubkey,
                    tx_hash,
                    offset,
                    data,
                };
                operations.push(op);
            } else {
                let op = HolderOperation::Create(pubkey); // TODO: how to distinguish create/delete
                operations.push(op);
            }
        }
        Ok(operations)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert(
        &self,
        slot: u64,
        tx_idx: u32,
        is_stuck: bool,
        op: &HolderOperation,
        txn: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let (pubkey, neon_sig, offset, data) = match op {
            HolderOperation::Create(pubkey) => (pubkey, None, None, None),
            HolderOperation::Delete(pubkey) => (pubkey, None, None, None),
            HolderOperation::Write {
                pubkey,
                tx_hash,
                offset,
                data,
            } => (pubkey, Some(tx_hash), Some(offset), Some(data)),
        };
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
            neon_sig.map(hex::encode),
            pubkey.to_string(),
            is_stuck,
            offset.map(|o| *o as i64),
            data
        )
        .execute(&mut **txn)
        .await?;
        Ok(())
    }

    pub async fn set_is_stuck(
        &self,
        pubkey: Pubkey,
        block_slot: u64,
        is_stuck: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE neon_holder_log
            SET is_stuck = $1
            WHERE pubkey = $2 AND block_slot = $3
            "#,
            is_stuck,
            pubkey.to_string(),
            block_slot as i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
