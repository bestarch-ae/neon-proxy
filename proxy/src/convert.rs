use anyhow::{anyhow, Context, Error};
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

use common::evm_loader::types::Address as NeonAddress;
use common::solana_sdk::hash::Hash;
use common::types::{EventLog, NeonTxInfo, SolanaBlock};

fn neon_extra_fields(tx: &NeonTxInfo) -> Result<OtherFields, Error> {
    let mut neon_fields = std::collections::BTreeMap::new();

    // TODO: implement proper recovery id calculation (EIP-155)
    neon_fields.insert(
        "v".to_string(),
        serde_json::to_value(U256::from(tx.transaction.recovery_id())).context("extra fields")?,
    );

    neon_fields.insert(
        "r".to_string(),
        serde_json::to_value(tx.transaction.r()).context("extra fields")?,
    );

    neon_fields.insert(
        "s".to_string(),
        serde_json::to_value(tx.transaction.s()).context("extra fields")?,
    );

    // TODO: get chainId from somewhere if None
    let chain_id = tx.transaction.chain_id();

    neon_fields.insert(
        "chainID".to_string(),
        serde_json::to_value(chain_id).context("chainID")?,
    );

    Ok(OtherFields::new(neon_fields))
}

pub fn neon_to_eth(tx: NeonTxInfo, blockhash: Option<Hash>) -> Result<Transaction, Error> {
    let other = neon_extra_fields(&tx)?;

    Ok(Transaction {
        hash: B256::from(&tx.neon_signature),
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
                    transaction_hash: Some(B256::from(&tx.neon_signature)),
                    log_index: Some(event.log_idx),
                    removed: false,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?,
    };

    let envelope = AnyReceiptEnvelope {
        inner: ReceiptWithBloom::new(receipt, Default::default()),
        r#type: tx.tx_type,
    };

    let receipt = TransactionReceipt {
        inner: envelope,
        transaction_hash: B256::from(&tx.neon_signature),
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

    Ok(WithOtherFields::new(receipt))
}

pub fn neon_event_to_log(event: &EventLog) -> PrimitiveLog {
    let topics = event
        .topic_list
        .iter()
        .map(|topic| B256::new(topic.to_be_bytes())) // TODO: Is this ok?
        .collect();
    PrimitiveLog {
        address: Address(event.address.unwrap_or_default().0.into()),
        data: LogData::new(topics, Bytes::copy_from_slice(&event.data)).unwrap(),
    }
}

fn build_block_header(block: SolanaBlock, txs: &[NeonTxInfo]) -> Result<Header, Error> {
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

    let logs_bloom = Bloom::default();
    let mut gas_used = 0;

    for tx in txs {
        // logs_bloom |= tx.events.inner.bloom(); TODO
        gas_used += tx.gas_used.as_u128();
    }

    Ok(Header {
        number: Some(slot),
        hash: Some(hash),
        parent_hash,
        timestamp: time.unwrap_or(0).try_into().context("timestamp")?,
        uncles_hash: UNCLES_HASH,
        transactions_root: root,
        receipts_root: root,
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
            .map(|tx| B256::from(&tx.neon_signature))
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

pub fn convert_rich_log(log: RichLog) -> Result<Log, Error> {
    Ok(Log {
        inner: neon_event_to_log(&log.event),
        transaction_index: Some(log.tx_idx),
        block_hash: Some(sol_blockhash_into_hex(log.blockhash)),
        block_number: Some(log.slot),
        block_timestamp: Some(log.timestamp as u64),
        transaction_hash: Some(B256::try_from(log.tx_hash.as_slice()).context("transaction hash")?),
        log_index: Some(log.event.log_idx),
        removed: false,
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
