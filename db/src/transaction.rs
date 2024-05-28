use anyhow::{bail, Context};
use common::ethnum::U256;
use common::evm_loader::types::{AccessListTx, Address, LegacyTx, Transaction, TransactionPayload};
use common::types::{EventKind, EventLog, NeonTxInfo};
use futures_util::{Stream, StreamExt, TryStreamExt};

use super::Error;

#[derive(Debug, Clone, Copy)]
pub enum TransactionBy {
    Hash([u8; 32]),
    Slot(u64),
}

impl TransactionBy {
    pub fn slot_hash(self) -> (Option<u64>, Option<String>) {
        match self {
            TransactionBy::Hash(hash) => (None, Some(format!("0x{}", hex::encode(hash)))),
            TransactionBy::Slot(slot) => (Some(slot), None),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WithBlockhash<T> {
    pub inner: T,
    pub blockhash: Option<String>,
}

#[derive(Debug, Clone)]
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
        let tx_idx = tx.tx_idx as i32;
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
            ON CONFLICT (neon_sig)
            DO UPDATE SET
               block_slot = $7,
               /* sum_gas_used = EXCLUDED.sum_gas_used + $13, TODO */
               is_completed = $19,
               is_canceled = $18,
               status = $17,
               tx_idx = $8,
               sol_ix_idx = $5,
               sol_ix_inner_idx = $6
            "#,
            tx_hash,                                        // 1
            tx.tx_type as i32,                              // 2
            tx.from.to_string(),                            // 3
            sol_sig,                                        // 4
            tx.sol_ix_idx as i64,                           // 5
            tx.sol_ix_inner_idx as i64,                     // 6
            block_slot,                                     // 7
            tx.tx_idx as i64,                               // 8
            format!("{:#0x}", tx.transaction.nonce()),      // 9
            format!("{:#0x}", tx.transaction.gas_price()),  // 10
            format!("{:#0x}", tx.transaction.gas_limit()),  // 11
            format!("{:#0x}", tx.transaction.value()),      // 12
            format!("{:#0x}", tx.gas_used),                 // 13
            format!("{:#0x}", tx.sum_gas_used),             // 14
            tx.transaction.target().map(|x| x.to_string()), // 15
            tx.contract.map(|c| c.to_string()),             // 16
            format!("{:#0x}", tx.status),                   // 17
            tx.is_cancelled,                                // 18
            tx.is_completed,                                // 19
            // TODO: Fix this for legacy
            format!("{:#0x}", tx.transaction.recovery_id()), // 20
            format!("{:#0x}", tx.transaction.r()),           // 21
            format!("{:#0x}", tx.transaction.s()),           // 22
            format!("0x{}", hex::encode(tx.transaction.call_data())), // 23
            &[]                                              /* 24 logs */
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
                 neon_sig as "neon_sig!", tx_type as "tx_type!", from_addr as "from_addr!",
                 sol_sig as "sol_sig!", sol_ix_idx as "sol_ix_idx!",
                 sol_ix_inner_idx as "sol_ix_inner_idx!", T.block_slot as "block_slot!",
                 tx_idx as "tx_idx!", nonce as "nonce!", gas_price as "gas_price!",
                 gas_limit as "gas_limit!", value as "value!", gas_used as "gas_used!",
                 sum_gas_used as "sum_gas_used!", to_addr as "to_addr?", contract as "contract?",
                 status "status!", is_canceled as "is_canceled!", is_completed as "is_completed!", 
                 v "v!", r as "r!", s as "s!", 
                 calldata as "calldata!",
                 B.block_hash
               FROM neon_transactions T
               LEFT JOIN solana_blocks B ON B.block_slot = T.block_slot
               WHERE (neon_sig = $1 OR $2) AND (T.block_slot = $3 OR $4)
               ORDER BY (T.block_slot, tx_idx) ASC
           "#,
            hash.as_ref().map_or("", String::as_str),
            hash.is_none(),
            slot.unwrap_or(0) as i64,
            slot.is_none(),
        )
        .map(|row| row.neon_tx_info_with_empty_logs())
        .fetch(&self.pool)
        .map(|res| Ok(res??))
    }

    fn fetch_logs(
        &self,
        by: TransactionBy,
    ) -> impl Stream<Item = Result<(String, EventLog), Error>> + '_ {
        tracing::info!(?by, "fetching logs");
        let (slot, hash) = by.slot_hash();
        sqlx::query_as!(
            NeonTransactionLogRow,
            r#"SELECT
                address as "address!", tx_hash as "tx_hash!",
                log_idx as "log_idx!",
                event_level as "event_level!", event_order as "event_order!",
                log_topic1 as "log_topic1!", log_topic2 as "log_topic2!",
                log_topic3 as "log_topic3!", log_topic4 as "log_topic4!",
                log_topic_cnt as "log_topic_cnt!", log_data as "log_data!"
               FROM neon_transaction_logs
               WHERE (tx_hash = $1 OR $2) AND (block_slot = $3 OR $4)
               ORDER BY (block_slot, tx_idx, tx_log_idx) ASC
            "#,
            hash.as_ref().map_or("", String::as_str),
            hash.is_none(),
            slot.unwrap_or(0) as i64,
            slot.is_none(),
        )
        .map(|row| {
            let hash = row.tx_hash.clone();
            row.try_into().map(|log| (hash, log))
        })
        .fetch(&self.pool)
        .map(|res| Ok(res??))
    }

    pub async fn fetch(&self, by: TransactionBy) -> Result<Vec<WithBlockhash<NeonTxInfo>>, Error> {
        tracing::info!(?by, "fetching transactions");
        let mut transactions: Vec<_> = self.fetch_without_events(by).try_collect().await?;
        if transactions.is_empty() {
            return Ok(transactions);
        }
        let mut tx_iter = transactions.iter_mut();
        let mut current_tx = tx_iter.next().expect("checked not empty");

        let mut logs = self.fetch_logs(by);

        while let Some((tx_hash, log)) = logs.try_next().await? {
            while current_tx.inner.neon_signature != tx_hash {
                current_tx = tx_iter.next().expect("inconsistent log and tx order");
            }

            current_tx.inner.events.push(log);
        }

        Ok(transactions)
    }
}

#[derive(Debug, Clone)]
struct NeonTransactionRow {
    neon_sig: String,
    tx_type: i32,
    from_addr: String,

    sol_sig: String,
    sol_ix_idx: i32,
    sol_ix_inner_idx: i32,
    block_slot: i64,
    tx_idx: i32,

    nonce: String,
    gas_price: String,
    gas_limit: String,
    value: String,
    gas_used: String,
    sum_gas_used: String,

    to_addr: Option<String>,
    contract: Option<String>,

    status: String,
    is_canceled: bool,
    is_completed: bool,

    v: String,
    r: String,
    s: String,

    calldata: String,
    block_hash: Option<String>,
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

        let nonce = u64::from_str_radix(self.nonce.strip_prefix("0x").context("nonce")?, 16)
            .context("nonce")?;
        let recovery_id = u8::from_str_radix(self.v.strip_prefix("0x").context("recovery_id")?, 16)
            .context("recovery_id")?;
        let gas_price = U256::from_str_hex(&self.gas_price).context("gas_price")?;
        let gas_limit = U256::from_str_hex(&self.gas_limit).context("gas_limit")?;
        let value = U256::from_str_hex(&self.value).context("value")?;
        let v = U256::from_str_hex(&self.v).context("v")?;
        let r = U256::from_str_hex(&self.r).context("r")?;
        let s = U256::from_str_hex(&self.s).context("s")?;

        let target = self
            .to_addr
            .as_ref()
            .map(|s| Address::from_hex(s).context("target"))
            .transpose()?;
        let call_data = hex::decode(self.calldata.strip_prefix("0x").context("calldata")?)
            .context("calldata")?;

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
                chain_id: None,
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
                chain_id: U256::new(0), // TODO
                recovery_id,
                access_list: Vec::new(), // TODO
            }),
            kind => bail!("unsupported tx kind: {kind:?}"),
        };

        Ok(Transaction {
            transaction: payload,
            hash: [0; 32], // TODO
            byte_len: 0,   // TODO
            signed_hash: hex::decode(self.neon_sig.strip_prefix("0x").unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
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
            neon_signature: self.neon_sig,
            from: Address::from_hex(&self.from_addr).context("from")?,
            contract: self
                .contract
                .as_ref()
                .map(|s| Address::from_hex(s))
                .transpose()
                .context("contract")?,
            transaction,
            events: Vec::new(),
            gas_used: U256::from_str_hex(&self.gas_used).context("gas_used")?,
            sum_gas_used: U256::from_str_hex(&self.sum_gas_used).context("sum_gas_used")?,
            sol_signature: self.sol_sig,
            sol_slot: self.block_slot.try_into().context("sol_slot")?,
            tx_idx: self.tx_idx.try_into().context("sol_tx_idx")?,
            sol_ix_idx: self.sol_ix_idx.try_into().context("sol_ix_idx")?,
            sol_ix_inner_idx: self
                .sol_ix_inner_idx
                .try_into()
                .context("sol_ix_inner_idx")?,
            status: u8::from_str_radix(self.status.strip_prefix("0x").context("status")?, 16)
                .context("status")?,
            is_completed: self.is_completed,
            is_cancelled: self.is_canceled,
        };

        Ok(WithBlockhash {
            inner: tx,
            blockhash: self.block_hash,
        })
    }
}

struct NeonTransactionLogRow {
    address: String,
    tx_hash: String,
    log_idx: i32,

    event_level: i32,
    event_order: i32,

    log_topic1: String,
    log_topic2: String,
    log_topic3: String,
    log_topic4: String,
    log_topic_cnt: i32,

    log_data: String,
}

impl TryFrom<NeonTransactionLogRow> for EventLog {
    type Error = anyhow::Error;

    fn try_from(value: NeonTransactionLogRow) -> Result<Self, Self::Error> {
        {
            let address = if value.address.is_empty() {
                None // TODO: Insert null
            } else {
                Some(Address::from_hex(&value.address).context("address")?)
            };
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
                let topic = U256::from_str_hex(topic).context("topic")?;
                topics.push(topic);
            }
            Result::<_, Self::Error>::Ok(EventLog {
                event_type: EventKind::Log, // TODO: insert to DB
                is_hidden: false,
                address,
                topic_list: topics,
                data: hex::decode(value.log_data).context("data")?,
                log_idx: value.log_idx.try_into().context("log_idx")?,
                level: value.event_level.try_into().context("event_level")?,
                order: value.event_order.try_into().context("event_order")?,
            })
        }
        .context("event log")
    }
}
