use std::str::FromStr;

use reth_primitives::revm_primitives::LogData;
use reth_primitives::{Address, Bytes, Log as PrimitiveLog, B256, U256};
use rpc_api_types::{
    AnyReceiptEnvelope, AnyTransactionReceipt, Log, Receipt, TransactionReceipt,
};
use rpc_api_types::{ReceiptWithBloom, Transaction, WithOtherFields};
use sqlx::PgPool;

type Result<T> = std::result::Result<T, sqlx::Error>;

pub async fn connect(url: &str) -> Result<PgPool> {
    PgPool::connect(url).await
}

#[derive(Clone)]
pub struct TransactionRepo {
    pool: PgPool,
}

#[derive(Debug)]
#[allow(dead_code)]
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

    to_addr: String,
    contract: String,

    status: String,
    is_canceled: bool,
    is_completed: bool,

    v: String,
    r: String,
    s: String,

    calldata: String,
    logs: Vec<u8>,
}

struct TransactionWithLogs {
    tx: NeonTransactionRow,
    logs: Vec<NeonTransactionLogRow>,
}

impl From<TransactionWithLogs> for TransactionReceipt<AnyReceiptEnvelope<Log>> {
    fn from(tx_with_logs: TransactionWithLogs) -> Self {
        let TransactionWithLogs { tx: row, logs } = tx_with_logs;
        let receipt = Receipt {
            status: true,
            cumulative_gas_used: u128::from_str_radix(
                row.sum_gas_used.strip_prefix("0x").unwrap(),
                16,
            )
            .unwrap(),
            logs: logs.into_iter().map(Into::into).collect(),
        };
        let receipt = ReceiptWithBloom::new(receipt, Default::default() /* TODO: fix this */);
        let envelope = AnyReceiptEnvelope {
            inner: receipt,
            r#type: 0, /* TODO */
        };

        TransactionReceipt {
            inner: envelope,
            transaction_hash: B256::from_str(&row.neon_sig).unwrap(),
            transaction_index: Some(row.tx_idx as u64),
            block_hash: None,
            block_number: Some(row.block_slot as u64),
            gas_used: u128::from_str_radix(row.gas_used.strip_prefix("0x").unwrap(), 16).unwrap(),
            effective_gas_price: u128::from_str_radix(
                row.gas_price.strip_prefix("0x").unwrap(),
                16,
            )
            .unwrap(),
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::from_str(&row.from_addr).unwrap(),
            to: Some(Address::from_str(&row.to_addr).unwrap()),
            contract_address: Address::from_str(&row.contract).ok(),
            state_root: None,
        }
    }
}

impl From<NeonTransactionRow> for Transaction {
    fn from(row: NeonTransactionRow) -> Transaction {
        Transaction {
            hash: B256::from_str(&row.neon_sig).unwrap(),
            nonce: u64::from_str_radix(row.nonce.strip_prefix("0x").unwrap(), 16).unwrap(),
            block_hash: None,                          /* TODO */
            block_number: Some(row.block_slot as u64), /* TODO: not sure if correct */
            transaction_index: Some(row.tx_idx as u64),
            from: Address::from_str(&row.from_addr).unwrap(),
            to: Some(Address::from_str(&row.to_addr).unwrap()),
            value: U256::from_str(&row.value).unwrap(),
            gas_price: Some(
                u128::from_str_radix(row.gas_price.strip_prefix("0x").unwrap(), 16).unwrap(),
            ),
            gas: u128::from_str_radix(row.gas_limit.strip_prefix("0x").unwrap(), 16).unwrap(),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            input: Bytes::from_str(&row.calldata).unwrap(),
            signature: None, /* TODO: what this */
            chain_id: None,  /* TODO: fill in */
            blob_versioned_hashes: None,
            access_list: None,
            transaction_type: Some(row.tx_type as u8),
            other: Default::default(),
        }
    }
}

#[allow(dead_code)]
struct NeonTransactionLogRow {
    address: String,
    block_slot: i64,
    tx_hash: String,
    tx_idx: i32,
    tx_log_idx: i32,
    log_idx: i32,

    event_level: i32,
    event_order: i32,

    sol_sig: String,
    idx: i32,
    inner_idx: i32,

    log_topic1: String,
    log_topic2: String,
    log_topic3: String,
    log_topic4: String,
    log_topic_cnt: i32,

    log_data: String,
}

impl From<NeonTransactionLogRow> for PrimitiveLog {
    fn from(value: NeonTransactionLogRow) -> Self {
        let mut topics = Vec::<B256>::new();
        for topic in [
            &value.log_topic1,
            &value.log_topic2,
            &value.log_topic3,
            &value.log_topic4,
        ]
        .iter()
        .take(value.log_topic_cnt as usize)
        {
            let topic = B256::from_str(topic).unwrap();
            topics.push(topic);
        }
        let data = LogData::new(topics, Bytes::from_str(&value.log_data).unwrap()).unwrap();
        PrimitiveLog {
            address: Address::from_str(&value.address).unwrap(),
            data,
        }
    }
}

impl From<NeonTransactionLogRow> for Log {
    fn from(value: NeonTransactionLogRow) -> Self {
        let mut topics = Vec::<B256>::new();
        for topic in [
            &value.log_topic1,
            &value.log_topic2,
            &value.log_topic3,
            &value.log_topic4,
        ]
        .iter()
        .take(value.log_topic_cnt as usize)
        {
            let topic = B256::from_str(topic).unwrap();
            topics.push(topic);
        }
        let data = LogData::new(topics, Bytes::from_str(&value.log_data).unwrap()).unwrap();
        Log {
            inner: PrimitiveLog {
                address: Address::from_str(&value.address).unwrap(),
                data,
            },
            transaction_index: Some(value.tx_idx as u64),
            block_hash: None,
            block_number: Some(value.block_slot as u64),
            block_timestamp: None,
            transaction_hash: Some(B256::from_str(&value.tx_hash).unwrap()),
            log_index: Some(value.log_idx as u64),
            removed: false,
        }
    }
}

impl TransactionRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn get_by_hash_inner(&self, hash: B256) -> Result<Option<NeonTransactionRow>> {
        let hash = &hash.to_string();
        let row = sqlx::query_as!(NeonTransactionRow,
             r#"SELECT
                 neon_sig as "neon_sig!", tx_type as "tx_type!", from_addr as "from_addr!",
                 sol_sig as "sol_sig!", sol_ix_idx as "sol_ix_idx!", sol_ix_inner_idx as "sol_ix_inner_idx!", block_slot as "block_slot!", tx_idx as "tx_idx!",
                 nonce as "nonce!", gas_price as "gas_price!", gas_limit as "gas_limit!", value as "value!", gas_used as "gas_used!", sum_gas_used as "sum_gas_used!",
                 to_addr as "to_addr!", contract as "contract!",
                 status "status!", is_canceled as "is_canceled!", is_completed as "is_completed!", 
                 v "v!", r as "r!", s as "s!", 
                 calldata as "calldata!",
                 logs as "logs!"
               FROM neon_transactions WHERE neon_sig = $1"#, hash).fetch_optional(&self.pool).await?;
        Ok(row)
    }

    pub async fn get_by_hash(&self, hash: B256) -> Result<Option<Transaction>> {
        let row = self.get_by_hash_inner(hash).await?;
        Ok(row.map(Into::into))
    }

    pub async fn receipt_by_hash(&self, hash: B256) -> Result<Option<AnyTransactionReceipt>> {
        let Some(tx) = self.get_by_hash_inner(hash).await? else {
            return Ok(None);
        };
        let hash = &hash.to_string();
        let logs = sqlx::query_as!(NeonTransactionLogRow,
            r#"SELECT
                address as "address!", block_slot as "block_slot!", tx_hash as "tx_hash!", tx_idx as "tx_idx!", tx_log_idx as "tx_log_idx!", log_idx as "log_idx!",
                event_level as "event_level!", event_order as "event_order!",
                sol_sig as "sol_sig!", idx as "idx!", inner_idx as "inner_idx!",
                log_topic1 as "log_topic1!", log_topic2 as "log_topic2!", log_topic3 as "log_topic3!", log_topic4 as "log_topic4!", log_topic_cnt as "log_topic_cnt!",
                log_data as "log_data!"
              FROM neon_transaction_logs WHERE tx_hash = $1"#, hash).fetch_all(&self.pool).await?;
        let receipt: TransactionReceipt<AnyReceiptEnvelope<Log>> =
            TransactionWithLogs { tx, logs }.into();
        Ok(Some(WithOtherFields::new(receipt)))
    }
}
