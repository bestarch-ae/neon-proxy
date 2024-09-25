use anyhow::{bail, Context};
use futures_util::{stream, Stream, StreamExt, TryStreamExt};
use sqlx::postgres::{PgRow, Postgres};
use sqlx::FromRow;

use common::ethnum::U256;
use common::evm_loader::types::vector::{VectorVecExt, VectorVecSlowExt};
use common::evm_loader::types::{AccessListTx, Address, LegacyTx, Transaction, TransactionPayload};
use common::solana_sdk::hash::Hash;
use common::solana_sdk::signature::Signature;
use common::types::{CanceledNeonTxInfo, EventKind, EventLog, NeonTxInfo, TxHash};

use crate::PgSolanaBlockHash;

use super::Error;

#[derive(Debug, Clone)]
struct EventFilter<'a> {
    address: &'a [Address],
    topics: [&'a [Vec<u8>]; 4],
}

#[derive(Debug, Clone)]
/// [`EventLog`] with additional block and transaction data
pub struct RichLog {
    pub blockhash: Hash,
    pub slot: u64,
    pub timestamp: i64,
    pub tx_idx: u64,
    pub tx_hash: TxHash,
    pub event: EventLog,
}

#[derive(Debug, Clone, Copy)]
pub enum RichLogBy {
    Hash([u8; 32]),
    SlotRange { from: Option<u64>, to: Option<u64> },
}

#[derive(sqlx::Type, Copy, Clone, Debug, Default)]
#[sqlx(type_name = "Address", transparent, no_pg_array)]
struct PgAddress([u8; 20]);

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

fn u256_to_bytes(val: &U256) -> [u8; 32] {
    let (hi, lo) = val.into_words();
    let mut buf = [0; 32];
    buf[0..16].copy_from_slice(&lo.to_le_bytes());
    buf[16..32].copy_from_slice(&hi.to_le_bytes());
    buf
}

#[derive(sqlx::Type, Clone, Debug, Default)]
#[sqlx(type_name = "U256", transparent)]
struct PgU256(sqlx::types::BigDecimal);

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

#[test]
fn from_to_256() {
    let x =
        U256::from_str_hex("0xc7f505b2f371ae2175ee4913f4499e1f2633a7b5936321eed1cdaeb6115181d2")
            .unwrap();
    let big = PgU256::from(x);
    let y: U256 = big.clone().into();
    println!("x={} big={} y = {}", x, big.0, y);
    assert_eq!(x, y);
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionBy {
    Hash(TxHash),
    Slot(u64),
    BlockNumberAndIndex(u64, u64),
    BlockHashAndIndex(Hash, Option<u64>),
    SenderNonce {
        address: Address,
        nonce: u64,
        chain_id: u64,
    },
    SlotRange {
        from: Option<u64>,
        to: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, Default)]
struct TransactionByParams {
    from_slot: Option<u64>,
    to_slot: Option<u64>,
    tx_hash: Option<TxHash>,
    tx_idx: Option<u64>,
    block_hash: Option<Hash>,
    sender: Option<PgAddress>,
    nonce: Option<i64>,
    chain_id: Option<i64>,
}

impl TransactionBy {
    fn id(&self) -> i32 {
        match self {
            TransactionBy::Hash(_) => 1,
            TransactionBy::Slot(_) | TransactionBy::SlotRange { .. } => 2,
            TransactionBy::BlockNumberAndIndex(_, _) => 3,
            TransactionBy::BlockHashAndIndex(_, _) => 4,
            TransactionBy::SenderNonce { .. } => 5,
        }
    }

    fn params(self) -> TransactionByParams {
        match self {
            TransactionBy::Hash(hash) => TransactionByParams {
                tx_hash: Some(hash),
                ..Default::default()
            },
            TransactionBy::Slot(slot) => TransactionByParams {
                from_slot: Some(slot),
                to_slot: Some(slot),
                ..Default::default()
            },
            TransactionBy::BlockNumberAndIndex(slot, idx) => TransactionByParams {
                from_slot: Some(slot),
                to_slot: Some(slot),
                tx_idx: Some(idx),
                ..Default::default()
            },
            TransactionBy::BlockHashAndIndex(hash, idx) => TransactionByParams {
                block_hash: Some(hash),
                tx_idx: idx,
                ..Default::default()
            },
            TransactionBy::SenderNonce {
                address,
                nonce,
                chain_id,
            } => TransactionByParams {
                sender: Some(PgAddress::from(address)),
                nonce: Some(nonce as i64),
                chain_id: Some(chain_id as i64),
                ..Default::default()
            },
            TransactionBy::SlotRange { from, to } => TransactionByParams {
                from_slot: from,
                to_slot: to,
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct WithBlockhash<T> {
    pub inner: T,
    pub blockhash: Option<Hash>,
}

#[derive(Debug, Clone)]
pub struct TransactionRepo {
    pool: sqlx::PgPool,
}

impl TransactionRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn set_canceled(
        &self,
        info: &CanceledNeonTxInfo,
        txn: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE neon_transactions
            SET
               is_canceled = true,
               is_completed = true,
               block_slot = $1,
               tx_idx = $2,
               gas_used = $3,
               sum_gas_used = $4
            WHERE neon_sig = $5 AND is_completed = false
            "#,
            info.sol_slot as i64,
            info.tx_idx as i32,
            PgU256::from(info.gas_used) as PgU256,
            PgU256::from(info.sum_gas_used) as PgU256,
            info.neon_signature.as_slice()
        )
        .execute(&mut **txn)
        .await?;
        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<sqlx::Transaction<'_, Postgres>, sqlx::Error> {
        self.pool.begin().await
    }

    pub async fn insert(
        &self,
        tx: &NeonTxInfo,
        txn: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let block_slot = tx.sol_slot as i64;
        let tx_hash = tx.neon_signature;
        let tx_idx = tx.tx_idx as i32;
        let sol_sig = &tx.sol_signature;
        let sol_idx = tx.sol_ix_idx as i32;
        let sol_inner_idx = tx.sol_ix_inner_idx as i32;
        let neon_step_cnt = tx.neon_steps as i64;
        let total_gas_used = tx.sum_gas_used;

        let v = match &tx.transaction.transaction {
            TransactionPayload::Legacy(legacy) => legacy.v,
            TransactionPayload::AccessList(tx) => tx.chain_id * 2 + 35 + u128::from(tx.recovery_id),
        };
        let chain_id = tx.transaction.chain_id();

        sqlx::query!(
            r#"
                UPDATE
                 neon_transaction_logs L
                SET
                 is_reverted = TRUE
                WHERE L.tx_hash IN (
                    SELECT neon_sig
                    FROM neon_transactions
                    WHERE
                      neon_sig = $1 AND
                      (neon_step_cnt > $2 OR (neon_step_cnt = $2 AND sum_gas_used > $3))
                )
            "#,
            tx_hash.as_slice(),
            neon_step_cnt,
            PgU256::from(total_gas_used) as PgU256,
        )
        .execute(&mut **txn)
        .await?;

        for log in &tx.events {
            /* not a real eth event */
            if log.is_hidden || log.topic_list.is_empty() {
                continue;
            }

            let topic1 = log.topic_list.first().map(u256_to_bytes);
            let topic2 = log.topic_list.get(1).map(u256_to_bytes);
            let topic3 = log.topic_list.get(2).map(u256_to_bytes);
            let topic4 = log.topic_list.get(3).map(u256_to_bytes);

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
                log.address.map(PgAddress::from).unwrap_or_default() as PgAddress, // 1
                block_slot,                                                        // 2
                tx_hash.as_slice(),                                                // 3
                tx_idx,                                                            // 4
                log.tx_log_idx as i64,                                             // 5
                log.blk_log_idx as i64,                                            // 6
                log.level as i64,                                                  // 7
                log.order as i64,                                                  // 8
                sol_sig.as_ref(),                                                  // 9
                sol_idx,                                                           // 10
                sol_inner_idx,                                                     // 11
                topic1.as_ref().map(|x| x.as_slice()),                             // 12
                topic2.as_ref().map(|x| x.as_slice()),                             // 13
                topic3.as_ref().map(|x| x.as_slice()),                             // 14
                topic4.as_ref().map(|x| x.as_slice()),                             // 15
                log.topic_list.len() as i32,                                       // 16
                hex::encode(&log.data)                                             // 17
            )
            .execute(&mut **txn)
            .await?;
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
                chain_id,
                calldata,
                logs,
                neon_step_cnt
            )
            VALUES($1, $2, $3, $4, $5, $6,
                   $7, $8, $9, $10, $11, $12,
                   $13, $14, $15, $16, $17, $18,
                   $19, $20, $21, $22, $23, $24, $25, $26)
            ON CONFLICT (neon_sig)
            DO UPDATE SET
               block_slot = $7,
               is_completed = $19,
               is_canceled = $18,
               status = $17,
               tx_idx = $8,
               sol_ix_idx = $5,
               sol_ix_inner_idx = $6,
               gas_used = $13,
               sum_gas_used = $14,
               neon_step_cnt = $26
            "#,
            tx_hash.as_slice(),                                                // 1
            tx.tx_type as i32,                                                 // 2
            PgAddress::from(tx.from) as PgAddress,                             // 3
            sol_sig.as_ref(),                                                  // 4
            tx.sol_ix_idx as i64,                                              // 5
            tx.sol_ix_inner_idx as i64,                                        // 6
            block_slot,                                                        // 7
            tx.tx_idx as i64,                                                  // 8
            tx.transaction.nonce() as i64,                                     // 9
            PgU256::from(tx.transaction.gas_price()) as PgU256,                // 10
            PgU256::from(tx.transaction.gas_limit()) as PgU256,                // 11
            PgU256::from(tx.transaction.value()) as PgU256,                    // 12
            PgU256::from(tx.gas_used) as PgU256,                               // 13
            PgU256::from(tx.sum_gas_used) as PgU256,                           // 14
            tx.transaction.target().map(PgAddress::from) as Option<PgAddress>, // 15
            tx.contract.map(PgAddress::from) as Option<PgAddress>,             // 16
            tx.status as i16,                                                  // 17
            tx.is_cancelled,                                                   // 18
            tx.is_completed,                                                   // 19
            PgU256::from(v) as PgU256,                                         // 20: TODO
            PgU256::from(tx.transaction.r()) as PgU256,                        // 21
            PgU256::from(tx.transaction.s()) as PgU256,                        // 22
            chain_id.map(|x| x as i64),                                        // 23
            tx.transaction.call_data(),                                        // 24
            &[],                                                               /* 25 logs */
            neon_step_cnt                                                      // 26
        )
        .execute(&mut **txn)
        .await?;
        Ok(())
    }

    pub fn fetch_with_events(
        &self,
        by: TransactionBy,
    ) -> impl Stream<Item = Result<WithBlockhash<NeonTxInfo>, Error>> + '_ {
        self.fetch_with_events_inner(by, None)
    }

    fn fetch_with_events_inner(
        &self,
        by: TransactionBy,
        filter: Option<EventFilter<'_>>,
    ) -> impl Stream<Item = Result<WithBlockhash<NeonTxInfo>, Error>> + '_ {
        let case = by.id();
        tracing::info!(?by, %case, ?filter, "fetching transactions with events");

        let address_ref: Vec<_> = filter
            .as_ref()
            .map(|f| f.address.iter().map(|addr| addr.0.to_vec()).collect())
            .unwrap_or_default();
        let TransactionByParams {
            from_slot,
            to_slot,
            tx_hash,
            block_hash,
            tx_idx,
            sender,
            nonce,
            chain_id,
        } = by.params();
        sqlx::query_as::<_, NeonTransactionRowWithLogs>(
            r#"SELECT * FROM
                   (WITH tx_block_slot AS
                    (
                     SELECT neon_sig,block_slot
                     FROM neon_transactions
                     WHERE neon_sig = $1 OR ($2 AND block_slot between $3 AND $4)
                    )
                    SELECT
                      T.neon_sig, tx_type, from_addr,
                      T.sol_sig, sol_ix_idx,
                      sol_ix_inner_idx, T.block_slot,
                      CAST
                        (DENSE_RANK() OVER (
                          PARTITION BY T.block_slot ORDER BY T.block_slot,T.tx_idx,sol_ix_idx
                        )-1 AS INTEGER) as tx_idx,
                      nonce, gas_price,
                      gas_limit, value, gas_used,
                      sum_gas_used, to_addr, contract,
                      status, is_canceled, is_completed,
                      v, r, s, chain_id,
                      calldata, neon_step_cnt,
                      B.block_hash,

                      L.address, L.tx_log_idx,
                      (row_number() OVER (PARTITION BY T.block_slot ORDER BY T.block_slot,T.tx_idx,L.block_slot,L.tx_idx,L.tx_log_idx))-1 as log_idx,
                      L.event_level, L.event_order,
                      L.log_topic1, L.log_topic2,
                      L.log_topic3, L.log_topic4,
                      L.log_topic_cnt, L.log_data
                    FROM neon_transactions T
                    LEFT JOIN tx_block_slot S on T.block_slot = S.block_slot AND T.neon_sig = S.neon_sig
                    LEFT JOIN (
                        SELECT * FROM neon_transaction_logs WHERE NOT COALESCE(is_reverted, FALSE)
                        ) L ON L.tx_hash = T.neon_sig AND T.is_canceled = FALSE
                    LEFT JOIN solana_blocks B ON B.block_slot = T.block_slot
                    WHERE NOT COALESCE(L.is_reverted, FALSE)) TL
                    WHERE
                        (TL.is_completed OR TL.is_canceled) AND
                        CASE
                            -- signature match
                            WHEN $5 = 1 THEN TL.neon_sig = $1
                            -- slot match
                            WHEN $5 = 2 THEN block_slot BETWEEN $3 AND $4
                            -- block number and index
                            WHEN $5 = 3 THEN block_slot BETWEEN $3 AND $4 AND TL.tx_idx = $6
                            -- block hash and maybe index
                            WHEN $5 = 4 THEN TL.block_hash = $7 AND (TL.tx_idx = $6 OR $8)
                            -- sender + nonce + chain_id
                            WHEN $5 = 5 THEN TL.from_addr = $9 AND
                                             TL.nonce = $10 AND
                                             TL.chain_id = $11
                        END
                        AND CASE
                            -- additionally filter by address & logs
                            WHEN $12 THEN
                                (address = ANY($13) OR $13 = '{}') AND
                                (log_topic1 = ANY($14) OR $14 = '{}') AND
                                (log_topic2 = ANY($15) OR $15 = '{}') AND
                                (log_topic3 = ANY($16) OR $16 = '{}') AND
                                (log_topic4 = ANY($17) OR $17 = '{}')
                            ELSE TRUE
                        END
                    ORDER BY TL.block_slot,TL.tx_idx,tx_log_idx
           "#,
        )
        .bind(tx_hash.map(|hash| *hash.as_array()).unwrap_or_default()) // 1
        .bind(tx_hash.is_none()) // 2
        .bind(from_slot.unwrap_or(0) as i64) // 3
        .bind(to_slot.unwrap_or(i64::MAX as u64) as i64) // 4
        .bind(case) // 5
        .bind(tx_idx.unwrap_or(0) as i64) // 6
        .bind(block_hash.map(PgSolanaBlockHash::from).unwrap_or_default()) // 7
        .bind(tx_idx.is_none()) // 8
        .bind(sender.unwrap_or_default()) // 9
        .bind(nonce.unwrap_or(0)) // 10
        .bind(chain_id.unwrap_or(0)) // 11
        .bind(filter.is_some()) // 12
        .bind(address_ref) // 13
        .bind(filter.as_ref().map(|f| f.topics[0]).unwrap_or_default().to_vec()) // 14
        .bind(filter.as_ref().map(|f| f.topics[1]).unwrap_or_default().to_vec()) // 15
        .bind(filter.as_ref().map(|f| f.topics[2]).unwrap_or_default().to_vec()) // 16
        .bind(filter.as_ref().map(|f| f.topics[3]).unwrap_or_default().to_vec()) // 17
        .fetch(&self.pool)
        .map(move |row| row.map(|row: NeonTransactionRowWithLogs| row.with_logs()))
        .map(move |res| Ok(res??))
    }

    pub async fn fetch_last_log_idx(&self, by: TxHash) -> Result<Option<i32>, sqlx::Error> {
        sqlx::query!(
            r#"
            SELECT MAX(tx_log_idx) as "log_idx?"
            FROM neon_transaction_logs
            WHERE tx_hash = $1
            "#,
            by.as_slice()
        )
        .fetch_one(&self.pool)
        .await
        .map(|row| row.log_idx)
    }

    pub fn fetch_solana_signatures(
        &self,
        hash: [u8; 32],
    ) -> impl Stream<Item = Result<Signature, Error>> + '_ {
        struct Row {
            sol_sig: Option<Vec<u8>>,
        }
        sqlx::query_as!(
            Row,
            r#"SELECT sol_sig
               FROM neon_transactions
               WHERE neon_sig = $1 ORDER BY block_slot,tx_idx"#,
            &hash,
        )
        .map(|row| {
            let sol_sig = row
                .sol_sig
                .ok_or_else(|| anyhow::anyhow!("missing sol_sig"))?;
            let sol_sig = Signature::from(
                <[u8; 64]>::try_from(sol_sig).map_err(|_| anyhow::anyhow!("bad signature"))?,
            );
            Ok::<_, Error>(sol_sig)
        })
        .fetch(&self.pool)
        .map(|res| res?)
    }

    pub fn fetch_rich_logs(
        &self,
        by: RichLogBy,
        address: &[Address],
        topics: [&[Vec<u8>]; 4],
    ) -> impl Stream<Item = Result<RichLog, Error>> + '_ {
        tracing::debug!(?by, ?address, ?topics, "fetching logs");
        let filter = EventFilter { address, topics };
        let by = match by {
            RichLogBy::Hash(hash) => TransactionBy::BlockHashAndIndex(hash.into(), None),
            RichLogBy::SlotRange { from, to } => TransactionBy::SlotRange { from, to },
        };
        self.fetch_with_events_inner(by, Some(filter))
            .map(|tx| {
                tx.map(|tx| {
                    let block_hash = tx.blockhash.unwrap_or_default();
                    let slot = tx.inner.sol_slot;
                    let tx_hash = tx.inner.neon_signature;

                    stream::iter(tx.inner.events.into_iter().map(move |ev| {
                        Ok(RichLog {
                            blockhash: block_hash,
                            slot,
                            timestamp: 0,
                            tx_idx: tx.inner.tx_idx,
                            tx_hash,
                            event: ev,
                        })
                    }))
                })
            })
            .try_flatten()
    }

    pub async fn fetch(&self, by: TransactionBy) -> Result<Vec<WithBlockhash<NeonTxInfo>>, Error> {
        tracing::info!(?by, "fetching transactions");

        let mut transactions: Vec<WithBlockhash<NeonTxInfo>> = Vec::new();
        let mut stream = self.fetch_with_events(by);
        while let Some(tx) = stream.try_next().await? {
            tracing::debug!(?tx, "found transaction");
            if let Some(current_tx) = transactions.last_mut() {
                if current_tx.inner.neon_signature == tx.inner.neon_signature {
                    current_tx.inner.events.extend(tx.inner.events);
                    continue;
                }
            }
            transactions.push(tx);
        }

        Ok(transactions)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct NeonTransactionRow {
    neon_sig: Vec<u8>,
    tx_type: i32,
    from_addr: PgAddress,

    sol_sig: Vec<u8>,
    sol_ix_idx: i32,
    sol_ix_inner_idx: i32,
    block_slot: i64,
    tx_idx: i32,

    nonce: i64,
    gas_price: PgU256,
    gas_limit: PgU256,
    value: PgU256,
    gas_used: PgU256,
    sum_gas_used: PgU256,

    to_addr: Option<PgAddress>,
    contract: Option<PgAddress>,

    status: i16,
    is_canceled: bool,
    is_completed: bool,

    v: PgU256,
    r: PgU256,
    s: PgU256,
    chain_id: Option<i64>,

    calldata: Vec<u8>,
    neon_step_cnt: i64,
    block_hash: Option<PgSolanaBlockHash>,
}

#[derive(Debug)]
struct NeonTransactionRowWithLogs {
    transaction: NeonTransactionRow,
    log: Option<NeonTransactionLogRow>,
}

impl FromRow<'_, PgRow> for NeonTransactionRowWithLogs {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let transaction = NeonTransactionRow::from_row(row)?;
        let val: Option<i32> = row.try_get("tx_log_idx")?;
        let log = if val.is_none() {
            None
        } else {
            Some(NeonTransactionLogRow::from_row(row)?)
        };
        Ok(NeonTransactionRowWithLogs { transaction, log })
    }
}

impl NeonTransactionRowWithLogs {
    fn with_logs(self) -> anyhow::Result<WithBlockhash<NeonTxInfo>> {
        let mut tx = self.transaction.neon_tx_info_with_empty_logs()?;
        if !tx.inner.is_cancelled {
            if let Some(log) = self.log {
                tx.inner.events.push(log.try_into()?);
            }
        }
        Ok(tx)
    }
}

impl NeonTransactionRow {
    fn transaction(&self) -> anyhow::Result<Transaction> {
        #[derive(Debug)]
        enum TxKind {
            Legacy,
            AccessList,
            DynamicFee,
        }

        let tx_kind = match self.tx_type {
            0x00 => TxKind::Legacy,
            0x01 => TxKind::AccessList,
            0x02 => TxKind::DynamicFee,
            byte => bail!("Unsupported EIP-2718 Transaction type | First byte: {byte}"),
        };

        let nonce = self.nonce as u64;
        let gas_price = self.gas_price.clone().into();
        let gas_limit = self.gas_limit.clone().into();
        let value = self.value.clone().into();
        let v: U256 = self.v.clone().into();
        let r = self.r.clone().into();
        let s = self.s.clone().into();
        let chain_id = self.chain_id.map(|x| U256::from(x as u128));
        let recovery_id = match v.as_u64() {
            _legacy if v <= 35 => v.as_u8() - 27,
            _access_list => {
                let chain_id = chain_id.ok_or_else(|| anyhow::anyhow!("missing chain_id"))?;
                (v.as_u64() - 35 - 2 * chain_id.as_u64()) as u8
            }
        };

        let target = self.to_addr.map(Address::from);
        let call_data = self.calldata.clone();

        let payload = match tx_kind {
            TxKind::Legacy => TransactionPayload::Legacy(LegacyTx {
                nonce,
                gas_price,
                gas_limit,
                target,
                value,
                call_data: call_data.into_vector(),
                v,
                r,
                s,
                chain_id,
                recovery_id,
            }),
            TxKind::AccessList => TransactionPayload::AccessList(AccessListTx {
                nonce,
                gas_price,
                gas_limit,
                target,
                value,
                call_data: call_data.into_vector(),
                r,
                s,
                chain_id: chain_id.unwrap_or(U256::new(0)), // TODO
                recovery_id,
                access_list: Vec::new().elementwise_copy_into_vector(), // TODO
            }),
            kind => bail!("unsupported tx kind: {kind:?}"),
        };

        Ok(Transaction {
            transaction: payload,
            hash: [0; 32], // TODO
            byte_len: 0,   // TODO
            signed_hash: self
                .neon_sig
                .clone()
                .try_into()
                .map_err(|_| anyhow::anyhow!("signed_hash"))?,
        })
    }

    fn neon_tx_info_with_empty_logs(self) -> anyhow::Result<WithBlockhash<NeonTxInfo>> {
        let transaction = self.transaction()?;
        let tx_type = match transaction.transaction {
            TransactionPayload::Legacy(..) => 0x00,
            TransactionPayload::AccessList(..) => 0x01,
        };

        let tx = NeonTxInfo {
            tx_type,
            neon_signature: self
                .neon_sig
                .try_into()
                .map_err(|_| anyhow::anyhow!("neon signature"))?,
            from: Address::from(self.from_addr),
            contract: self.contract.map(Address::from),
            transaction,
            events: Vec::new(),
            gas_used: U256::from(self.gas_used),
            sum_gas_used: U256::from(self.sum_gas_used),
            sol_signature: common::solana_sdk::signature::Signature::from(
                <[u8; 64]>::try_from(self.sol_sig).map_err(|_| anyhow::anyhow!("sol signature"))?,
            ),
            sol_slot: self.block_slot.try_into().context("sol_slot")?,
            tx_idx: self.tx_idx.try_into().context("sol_tx_idx")?,
            sol_ix_idx: self.sol_ix_idx.try_into().context("sol_ix_idx")?,
            sol_ix_inner_idx: self
                .sol_ix_inner_idx
                .try_into()
                .context("sol_ix_inner_idx")?,
            status: self.status as u8,
            is_completed: self.is_completed,
            is_cancelled: self.is_canceled,
            neon_steps: self.neon_step_cnt as u64,
        };

        Ok(WithBlockhash {
            inner: tx,
            blockhash: self.block_hash.map(Into::into),
        })
    }
}

#[derive(Debug, FromRow)]
struct NeonTransactionLogRow {
    address: Option<PgAddress>,
    tx_log_idx: i32,
    log_idx: i64,

    event_level: i32,
    event_order: i32,

    log_topic1: Option<Vec<u8>>,
    log_topic2: Option<Vec<u8>>,
    log_topic3: Option<Vec<u8>>,
    log_topic4: Option<Vec<u8>>,
    log_topic_cnt: i32,

    log_data: String,
}

impl TryFrom<NeonTransactionLogRow> for EventLog {
    type Error = anyhow::Error;

    fn try_from(value: NeonTransactionLogRow) -> Result<Self, Self::Error> {
        {
            let address = value.address.map(Into::into);
            let mut topics = Vec::new();
            for topic in [
                &value.log_topic1,
                &value.log_topic2,
                &value.log_topic3,
                &value.log_topic4,
            ]
            .iter()
            .take(value.log_topic_cnt as usize)
            {
                let topic = U256::from_le_bytes(
                    topic.as_ref().context("topic")?[0..32]
                        .try_into()
                        .context("topic len")?,
                );
                topics.push(topic);
            }
            Result::<_, Self::Error>::Ok(EventLog {
                event_type: EventKind::Log, // TODO: insert to DB
                is_hidden: false,
                is_reverted: false,
                address,
                topic_list: topics,
                data: hex::decode(value.log_data).context("data")?,
                tx_log_idx: value.tx_log_idx.try_into().context("tx_log_idx")?,
                blk_log_idx: value.log_idx.try_into().context("blk_log_idx")?,
                level: value.event_level.try_into().context("event_level")?,
                order: value.event_order.try_into().context("event_order")?,
            })
        }
        .context("event log")
    }
}

#[derive(Debug, FromRow)]
struct NeonRichLogRow {
    block_hash: PgSolanaBlockHash,
    block_slot: i64,
    block_time: i64,

    // NeonTransactionLogRow
    address: Option<PgAddress>,
    tx_hash: Vec<u8>,
    tx_log_idx: i32,
    log_idx: i64,

    event_level: i32,
    event_order: i32,

    log_topic1: Option<Vec<u8>>,
    log_topic2: Option<Vec<u8>>,
    log_topic3: Option<Vec<u8>>,
    log_topic4: Option<Vec<u8>>,
    log_topic_cnt: i32,

    log_data: String,
    tx_idx: i64,
}

impl TryFrom<NeonRichLogRow> for RichLog {
    type Error = anyhow::Error;

    fn try_from(value: NeonRichLogRow) -> Result<Self, Self::Error> {
        let log = NeonTransactionLogRow {
            address: value.address,
            tx_log_idx: value.tx_log_idx,
            log_idx: value.log_idx,
            event_level: value.event_level,
            event_order: value.event_order,
            log_topic1: value.log_topic1,
            log_topic2: value.log_topic2,
            log_topic3: value.log_topic3,
            log_topic4: value.log_topic4,
            log_topic_cnt: value.log_topic_cnt,
            log_data: value.log_data,
        };

        let tx_hash = TxHash::try_from(value.tx_hash)
            .map_err(|_| anyhow::anyhow!("failed to parse tx_hash"))?;

        Ok(RichLog {
            blockhash: value.block_hash.into(),
            slot: value.block_slot.try_into().context("block_slot")?,
            timestamp: value.block_time,
            tx_idx: value.tx_idx.try_into().context("tx_idx")?,
            tx_hash,
            event: log.try_into()?,
        })
    }
}
