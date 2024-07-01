use anyhow::{bail, Context};
use common::solana_sdk::hash::Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};
use sqlx::postgres::PgRow;
use sqlx::FromRow;

use common::ethnum::U256;
use common::evm_loader::types::{AccessListTx, Address, LegacyTx, Transaction, TransactionPayload};
use common::types::{EventKind, EventLog, NeonTxInfo, TxHash};

use crate::PgSolanaBlockHash;

use super::Error;

#[derive(Debug, Clone)]
/// [`EventLog`] with additional block and transaction data
pub struct RichLog {
    pub blockhash: Hash,
    pub slot: u64,
    pub timestamp: i64,
    pub tx_idx: u64,
    pub tx_hash: Vec<u8>,
    pub event: EventLog,
}

#[derive(Debug, Clone, Copy)]
pub enum RichLogBy {
    Hash([u8; 32]),
    SlotRange { from: Option<u64>, to: Option<u64> },
}

impl RichLogBy {
    fn bounds(self) -> (Option<u64>, Option<u64>, Option<[u8; 32]>) {
        match self {
            Self::SlotRange { from, to } => (from, to, None),
            Self::Hash(hash) => (None, None, Some(hash)),
        }
    }
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
}

impl TransactionBy {
    pub fn slot_hash(self) -> (Option<u64>, Option<TxHash>) {
        match self {
            TransactionBy::Hash(hash) => (None, Some(hash)),
            TransactionBy::Slot(slot) => (Some(slot), None),
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
        hash: &TxHash,
        gas_used: U256,
        slot: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE neon_transactions
            SET
               is_canceled = true,
               block_slot = $1,
               gas_used = $2,
               sum_gas_used = $2
            WHERE neon_sig = $3 AND is_completed = false
            "#,
            slot as i64,
            PgU256::from(gas_used) as PgU256,
            hash.as_slice()
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert(&self, tx: &NeonTxInfo) -> Result<(), sqlx::Error> {
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

        let mut txn = self.pool.begin().await?;

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
        .execute(&mut *txn)
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
            .execute(&mut *txn)
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
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub fn fetch_without_events(
        &self,
        by: TransactionBy,
    ) -> impl Stream<Item = Result<WithBlockhash<NeonTxInfo>, Error>> + '_ {
        let (slot, hash) = by.slot_hash();
        sqlx::query_as!(
            NeonTransactionRow,
            r#"SELECT
                 neon_sig as "neon_sig!", tx_type as "tx_type!", from_addr as "from_addr!: PgAddress",
                 sol_sig as "sol_sig!", sol_ix_idx as "sol_ix_idx!",
                 sol_ix_inner_idx as "sol_ix_inner_idx!", T.block_slot as "block_slot!",
                 tx_idx as "tx_idx!", nonce as "nonce!", gas_price as "gas_price!",
                 gas_limit as "gas_limit!", value as "value!", gas_used as "gas_used!",
                 sum_gas_used as "sum_gas_used!", to_addr as "to_addr?: PgAddress", contract as "contract?: PgAddress",
                 status "status!", is_canceled as "is_canceled!", is_completed as "is_completed!", 
                 v "v!", r as "r!", s as "s!", chain_id,
                 calldata as "calldata!", neon_step_cnt as "neon_step_cnt!",
                 B.block_hash as "block_hash!: PgSolanaBlockHash"
               FROM neon_transactions T
               LEFT JOIN solana_blocks B ON B.block_slot = T.block_slot
               WHERE (neon_sig = $1 OR $2) AND (T.block_slot = $3 OR $4)
               ORDER BY (T.block_slot, tx_idx) ASC
           "#,
            hash.as_ref().map(|x| x.as_slice()).unwrap_or_default(),
            hash.is_none(),
            slot.unwrap_or(0) as i64,
            slot.is_none(),
        )
        .map(|row| row.neon_tx_info_with_empty_logs())
        .fetch(&self.pool)
        .map(|res| Ok(res??))
    }

    fn fetch_with_events(
        &self,
        by: TransactionBy,
    ) -> impl Stream<Item = Result<WithBlockhash<NeonTxInfo>, Error>> + '_ {
        let (slot, hash) = by.slot_hash();
        sqlx::query_as::<_, NeonTransactionRowWithLogs>(
            r#"SELECT * FROM
                   (WITH tx_block_slot AS
                    (
                     SELECT block_slot
                     FROM neon_transactions
                     WHERE neon_sig = $1 OR ($2 AND block_slot = $3)
                    )
                    SELECT
                      neon_sig, tx_type, from_addr,
                      T.sol_sig, sol_ix_idx,
                      sol_ix_inner_idx, T.block_slot,
                      T.tx_idx, nonce, gas_price,
                      gas_limit, value, gas_used,
                      sum_gas_used, to_addr, contract,
                      status, is_canceled, is_completed, 
                      v, r, s, chain_id,
                      calldata, neon_step_cnt,
                      B.block_hash,

                      L.address, L.tx_log_idx,
                      (row_number() OVER (PARTITION BY T.block_slot ORDER BY L.block_slot,L.tx_idx,L.tx_log_idx))-1 as log_idx,
                      L.event_level, L.event_order,
                      L.log_topic1, L.log_topic2,
                      L.log_topic3, L.log_topic4,
                      L.log_topic_cnt, L.log_data
                    FROM neon_transactions T
                    LEFT JOIN tx_block_slot S on T.block_slot = S.block_slot
                    LEFT JOIN (
                        SELECT *  FROM neon_transaction_logs WHERE NOT COALESCE(is_reverted, FALSE)
                        ) L ON L.tx_hash = T.neon_sig
                    LEFT JOIN solana_blocks B ON B.block_slot = T.block_slot
                    WHERE NOT COALESCE(L.is_reverted, FALSE)
                    ORDER BY T.block_slot,T.tx_idx,tx_log_idx) TL
                    WHERE TL.neon_sig = $1 OR ($2 AND block_slot = $3)
           "#,
        )
        .bind(hash.map(|hash| *hash.as_array()).unwrap_or_default())
        .bind(hash.is_none())
        .bind(slot.unwrap_or(0) as i64)
        .bind(slot.is_none())
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

    pub fn fetch_rich_logs(
        &self,
        by: RichLogBy,
        address: &[Address],
        topics: [&[Vec<u8>]; 4],
    ) -> impl Stream<Item = Result<RichLog, Error>> + '_ {
        tracing::debug!(?by, ?address, ?topics, "fetching logs");
        let (from, to, hash) = by.bounds();
        let address_ref: Vec<_> = address.iter().map(|addr| addr.0.to_vec()).collect();

        sqlx::query_as!(
            NeonRichLogRow,
            r#"SELECT
                address as "address?: PgAddress", tx_hash as "tx_hash!",
                row_number() over (partition by T.block_slot ORDER BY T.block_slot, T.tx_idx, tx_log_idx) as "log_idx!",
                tx_log_idx as "tx_log_idx!", L.block_slot as "block_slot!",
                event_level as "event_level!", event_order as "event_order!",
                log_topic1 as "log_topic1?", log_topic2 as "log_topic2?",
                log_topic3 as "log_topic3?", log_topic4 as "log_topic4?",
                log_topic_cnt as "log_topic_cnt!", log_data as "log_data!",
                block_hash as "block_hash!: PgSolanaBlockHash", block_time as "block_time!",
                T.tx_idx as "tx_idx!"
               FROM neon_transactions T
               LEFT JOIN solana_blocks B ON B.block_slot = T.block_slot
               INNER JOIN neon_transaction_logs L ON tx_hash = neon_sig
               WHERE T.is_completed
                   AND (T.block_slot >= $1) AND (T.block_slot <= $2)
                   AND (block_hash = $3 OR $4)
                   AND (address = ANY($5) OR $6)
                   AND (log_topic1 = ANY($7) OR $8)
                   AND (log_topic2 = ANY($9) OR $10)
                   AND (log_topic3 = ANY($11) OR $12)
                   AND (log_topic4 = ANY($13) OR $14)
                   AND is_reverted = FALSE
               ORDER BY (T.block_slot, T.tx_idx, tx_log_idx) ASC
            "#,
            from.unwrap_or(0) as i64,                // 1
            to.map_or(i64::MAX, |to| to as i64),     // 2
            hash.as_ref().map(<[u8; 32]>::as_slice), // 3
            hash.is_none(),                          // 4
            address_ref.as_slice(),                  // 5
            address.is_empty(),                      // 6
            topics[0],                               // 7
            topics[0].is_empty(),                    // 8
            topics[1],                               // 9
            topics[1].is_empty(),                    // 10
            topics[2],                               // 11
            topics[2].is_empty(),                    // 12
            topics[3],                               // 13
            topics[3].is_empty(),                    // 14
        )
        .map(TryInto::try_into)
        .fetch(&self.pool)
        .map(|res| Ok(res??))
    }

    pub async fn fetch(&self, by: TransactionBy) -> Result<Vec<WithBlockhash<NeonTxInfo>>, Error> {
        tracing::info!(?by, "fetching transactions");

        let mut transactions: Vec<WithBlockhash<NeonTxInfo>> = Vec::new();
        let mut stream = self.fetch_with_events(by);
        while let Some(tx) = stream.try_next().await? {
            tracing::info!(?tx, "found transaction");
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
                call_data,
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
                call_data,
                r,
                s,
                chain_id: chain_id.unwrap_or(U256::new(0)), // TODO
                recovery_id,
                access_list: Vec::new(), // TODO
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

        Ok(RichLog {
            blockhash: value.block_hash.into(),
            slot: value.block_slot.try_into().context("block_slot")?,
            timestamp: value.block_time,
            tx_idx: value.tx_idx.try_into().context("tx_idx")?,
            tx_hash: value.tx_hash,
            event: log.try_into()?,
        })
    }
}
