use anyhow::{anyhow, Context, Error};
use common::evm_loader::types::{Address as NeonAddress, TransactionPayload};
use common::solana_sdk::hash::Hash;
use common::types::{EventLog, NeonTxInfo, SolanaBlock};
use db::{RichLog, RichLogBy};
use hex_literal::hex;
use reth_primitives::revm_primitives::LogData;
use reth_primitives::trie::EMPTY_ROOT_HASH;
use reth_primitives::{Address, Bloom, Bytes, Log as PrimitiveLog, B256, B64, U256};
use rpc_api_types::other::OtherFields;
use rpc_api_types::{
    AnyReceiptEnvelope, AnyTransactionReceipt, Block, BlockNumberOrTag, BlockTransactions, Filter,
    FilterBlockOption, FilterSet, Header, Log, Receipt, ReceiptWithBloom, Transaction,
    TransactionReceipt, ValueOrArray, WithOtherFields,
};

use crate::rpc::NeonLog;

pub type NeonTransactionReceipt =
    WithOtherFields<TransactionReceipt<AnyReceiptEnvelope<Log<NeonLogData>>>>;
pub type NeonLogData = WithOtherFields<LogData>;
pub type EthNeonLog = Log<NeonLogData>;

pub fn to_neon_receipt(rec: AnyTransactionReceipt) -> NeonTransactionReceipt {
    let WithOtherFields { inner, other } = rec;
    WithOtherFields {
        other,
        inner: inner.map_inner(to_neon_envelope),
    }
}

fn to_neon_envelope(env: AnyReceiptEnvelope<Log>) -> AnyReceiptEnvelope<Log<NeonLogData>> {
    let inner: Vec<Log<_>> = env
        .inner
        .receipt
        .logs
        .into_iter()
        .map(to_eth_neon_log)
        .collect();
    let receipt = Receipt {
        status: env.inner.receipt.status,
        cumulative_gas_used: env.inner.receipt.cumulative_gas_used,
        logs: inner,
    };
    AnyReceiptEnvelope {
        inner: ReceiptWithBloom {
            receipt,
            logs_bloom: env.inner.logs_bloom,
        },
        r#type: env.r#type,
    }
}

pub fn to_eth_neon_log(log: Log) -> EthNeonLog {
    Log {
        inner: to_neon_log_inner(log.inner),
        transaction_index: log.transaction_index,
        block_hash: log.block_hash,
        block_number: log.block_number,
        block_timestamp: log.block_timestamp,
        transaction_hash: log.transaction_hash,
        log_index: log.log_index,
        removed: log.removed,
    }
}

fn to_neon_log_inner(log: PrimitiveLog) -> PrimitiveLog<WithOtherFields<LogData>> {
    let mut neon_fields = OtherFields::default();
    neon_fields.insert(
        "address".to_owned(),
        serde_json::to_value(log.address.to_string()).unwrap(),
    );
    PrimitiveLog {
        address: log.address,
        data: WithOtherFields {
            inner: log.data,
            other: neon_fields,
        },
    }
}

fn neon_extra_fields(tx: &NeonTxInfo) -> Result<OtherFields, Error> {
    let mut neon_fields = std::collections::BTreeMap::new();

    let v = match &tx.transaction.transaction {
        TransactionPayload::Legacy(legacy) => legacy.v,
        TransactionPayload::AccessList(tx) => tx.chain_id * 2 + 35 + u128::from(tx.recovery_id),
    };
    neon_fields.insert(
        "v".to_string(),
        serde_json::to_value(v).context("extra fields")?,
    );

    neon_fields.insert(
        "r".to_string(),
        serde_json::to_value(tx.transaction.r()).context("extra fields")?,
    );

    neon_fields.insert(
        "s".to_string(),
        serde_json::to_value(tx.transaction.s()).context("extra fields")?,
    );

    Ok(OtherFields::new(neon_fields))
}

fn add_checksum_fields(other: &mut OtherFields, tx: &NeonTxInfo) -> Result<(), Error> {
    other.insert(
        "from".to_string(),
        serde_json::to_value(Address::new(tx.from.0).to_string())?,
    );
    if let Some(to) = tx.transaction.target() {
        other.insert(
            "to".to_string(),
            serde_json::to_value(Address::new(to.0).to_string())?,
        );
    }
    if let Some(contract) = tx.contract {
        other.insert(
            "contractAddress".to_string(),
            serde_json::to_value(Address::new(contract.0).to_string())?,
        );
    }

    Ok(())
}

pub fn neon_to_eth(tx: NeonTxInfo, blockhash: Option<Hash>) -> Result<Transaction, Error> {
    let mut other = neon_extra_fields(&tx)?;
    add_checksum_fields(&mut other, &tx)?;

    Ok(Transaction {
        hash: B256::from(tx.neon_signature.as_array()),
        nonce: tx.transaction.nonce(),
        block_hash: blockhash.map(sol_blockhash_into_hex),
        block_number: Some(tx.sol_slot), /* TODO: not sure if correct */
        transaction_index: Some(tx.tx_idx),
        from: tx.from.0.into(),
        to: tx.transaction.target().map(|addr| addr.0.into()),
        value: U256::from_be_bytes(tx.transaction.value().to_be_bytes()),
        gas_price: Some(tx.transaction.gas_price().as_u128()),
        gas: tx.transaction.gas_limit().as_u128(),
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
        max_fee_per_blob_gas: None,
        input: Bytes::copy_from_slice(tx.transaction.call_data()),
        signature: None, /* TODO: what this */
        chain_id: tx.transaction.chain_id(),
        blob_versioned_hashes: None,
        access_list: None,
        transaction_type: Some(tx.tx_type),
        other,
    })
}

pub fn neon_to_eth_receipt(
    tx: NeonTxInfo,
    blockhash: Option<Hash>,
) -> Result<AnyTransactionReceipt, Error> {
    let receipt = Receipt {
        status: tx.status > 0x0,
        cumulative_gas_used: tx.sum_gas_used.as_u128(),
        logs: tx
            .events
            .iter()
            .map(|event| {
                Result::<_, Error>::Ok(Log {
                    inner: neon_event_to_log(event),
                    // TODO: Do we really need all these fields
                    transaction_index: Some(tx.tx_idx),
                    block_hash: blockhash.map(sol_blockhash_into_hex),
                    block_number: Some(tx.sol_slot),
                    block_timestamp: None,
                    transaction_hash: Some(B256::from(tx.neon_signature.as_array())),
                    log_index: Some(event.blk_log_idx),
                    removed: false,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?,
    };

    let mut bloom = Bloom::default();
    for log in &receipt.logs {
        bloom.accrue_log(&log.inner);
    }

    let envelope = AnyReceiptEnvelope {
        inner: ReceiptWithBloom::new(receipt, bloom),
        r#type: tx.tx_type,
    };

    let mut other = OtherFields::default();
    add_checksum_fields(&mut other, &tx)?;

    let receipt = TransactionReceipt {
        inner: envelope,
        transaction_hash: B256::from(tx.neon_signature.as_array()),
        transaction_index: Some(tx.tx_idx),
        block_hash: blockhash.map(sol_blockhash_into_hex),
        block_number: Some(tx.sol_slot),
        gas_used: tx.gas_used.as_u128(),
        effective_gas_price: tx.transaction.gas_price().as_u128(),
        from: tx.from.0.into(),
        to: tx.transaction.target().map(|addr| addr.0.into()),
        contract_address: tx.contract.map(|addr| addr.0.into()),

        blob_gas_used: None,
        blob_gas_price: None,
        state_root: None,
    };

    Ok(WithOtherFields {
        inner: receipt,
        other,
    })
}

pub fn neon_event_to_log(event: &EventLog) -> PrimitiveLog {
    let topics = event
        .topic_list
        .iter()
        .map(|topic| B256::new(topic.to_ne_bytes()))
        .collect();
    PrimitiveLog {
        address: Address(event.address.unwrap_or_default().0.into()),
        data: LogData::new(topics, Bytes::copy_from_slice(&event.data)).unwrap(),
    }
}

fn build_block_header(block: SolanaBlock, txs: &[NeonTxInfo]) -> Result<Header, Error> {
    tracing::debug!("Building header for block {:?} txs: {}", block, txs.len());
    // Consts taken from here:
    // https://github.com/neonlabsorg/proxy-model.py/blob/149298b924d7cbf5e02eb85eac041b63e21e59d5/proxy/neon_rpc_api_model/neon_rpc_api_worker.py#L626C25-L626C89
    const UNCLES_HASH: B256 = B256::new(hex!(
        "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
    ));
    const MAX_BPF_CYCLES: u128 = 48_000_000_000_000;

    let SolanaBlock {
        slot,
        hash,
        parent_hash,
        time,
        ..
    } = block;
    let hash = B256::new(hash.to_bytes());
    let parent_hash = B256::new(parent_hash.to_bytes());

    let root = if txs.is_empty() {
        EMPTY_ROOT_HASH
    } else {
        B256::with_last_byte(1)
    };

    let mut logs_bloom = Bloom::default();
    let mut gas_used = 0;

    for tx in txs {
        gas_used += tx.gas_used.as_u128();
        for event in &tx.events {
            let topics: Vec<_> = event
                .topic_list
                .iter()
                .map(|topic| B256::new(topic.to_ne_bytes()))
                .collect();
            let address = Address(event.address.unwrap_or_default().0.into());
            logs_bloom.accrue_raw_log(address, &topics);
        }
    }

    Ok(Header {
        number: Some(slot),
        hash: Some(hash),
        parent_hash,
        timestamp: time.unwrap_or(0).try_into().context("timestamp")?,
        uncles_hash: UNCLES_HASH,
        transactions_root: root,
        receipts_root: B256::with_last_byte(1),
        state_root: B256::with_last_byte(1),
        mix_hash: Some(B256::with_last_byte(1)),
        gas_used,
        gas_limit: MAX_BPF_CYCLES.max(gas_used),
        logs_bloom,
        nonce: Some(B64::ZERO),
        total_difficulty: Some(U256::ZERO),
        ..Default::default()
    })
}

pub fn build_block(block: SolanaBlock, txs: Vec<NeonTxInfo>, full: bool) -> Result<Block, Error> {
    let hash = block.hash;
    let header = build_block_header(block, &txs).context("block header")?;
    let transactions = if full {
        let txs = txs
            .into_iter()
            .map(|tx| neon_to_eth(tx, Some(hash)))
            .collect::<Result<_, Error>>()
            .context("transactions")?;
        BlockTransactions::Full(txs)
    } else {
        let txs = txs
            .into_iter()
            .map(|tx| B256::from(tx.neon_signature.as_array()))
            .collect();
        BlockTransactions::Hashes(txs)
    };

    Ok(Block {
        header,
        transactions,
        size: Some(U256::from(1)),
        ..Default::default()
    })
}

fn sol_blockhash_into_hex(hash: Hash) -> B256 {
    hash.to_bytes().into()
}

pub fn convert_rich_log(log: RichLog) -> Result<NeonLog, Error> {
    let inner_log = Log {
        inner: neon_event_to_log(&log.event),
        transaction_index: Some(log.tx_idx),
        block_hash: Some(sol_blockhash_into_hex(log.blockhash)),
        block_number: Some(log.slot),
        block_timestamp: None,
        transaction_hash: Some(B256::try_from(log.tx_hash.as_slice()).context("transaction hash")?),
        log_index: Some(log.event.blk_log_idx),
        removed: false,
    };
    Ok(NeonLog {
        log: inner_log,
        removed: false,
        solana_transaction_signature: log.sol_signature,
        solana_instruction_index: log.sol_ix_idx,
        solana_inner_instruction_index: log.sol_ix_inner_idx,
        solana_address: None, //log.event.address,
        neon_event_type: log.event.event_type,
        neon_event_level: log.event.level,
        neon_event_order: log.event.order,
        neon_is_hidden: false,
        neon_is_reverted: false,
    })
}

#[derive(Debug, Clone)]
pub struct LogFilters {
    pub block: RichLogBy,
    pub address: Vec<NeonAddress>,
    pub topics: [Vec<Vec<u8>>; 4],
}

pub fn convert_filters(filters: Filter) -> Result<LogFilters, Error> {
    let extract_block_number = |block| match block {
        BlockNumberOrTag::Number(block) => Ok(block),
        tag => Err(anyhow!("block tag {tag} not supported")),
    };
    let block = match filters.block_option {
        FilterBlockOption::Range {
            from_block,
            to_block,
        } => {
            let from = from_block.map(extract_block_number).transpose()?;
            let to = to_block.map(extract_block_number).transpose()?;
            RichLogBy::SlotRange { from, to }
        }
        FilterBlockOption::AtBlockHash(hash) => RichLogBy::Hash(hash.0),
    };

    fn extract_filter_set<T, U, F>(filter_set: FilterSet<T>, mut f: F) -> Vec<U>
    where
        T: Eq + std::hash::Hash + Clone + ToString,
        F: FnMut(&T) -> U,
    {
        match filter_set.to_value_or_array() {
            None => Vec::new(),
            Some(ValueOrArray::Value(val)) => vec![f(&val)],
            Some(ValueOrArray::Array(vec)) => vec.iter().map(f).collect(),
        }
    }

    let address = extract_filter_set(filters.address, |addr| NeonAddress(addr.0 .0));
    let topics = filters
        .topics
        .map(|topics| extract_filter_set(topics, |topic| topic.0.to_vec()));

    Ok(LogFilters {
        block,
        address,
        topics,
    })
}
