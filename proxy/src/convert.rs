use hex_literal::hex;
use reth_primitives::trie::EMPTY_ROOT_HASH;
use reth_primitives::{Bloom, B256, U256};
use rpc_api_types::{AnyTransactionReceipt, Block, BlockTransactions, Header, Transaction};
use thiserror::Error;

use common::solana_sdk::hash::Hash;
use common::types::SolanaBlock;

#[derive(Debug, Error)]
pub enum Error {}

fn build_block_header(block: SolanaBlock, txs: &[AnyTransactionReceipt]) -> Header {
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
    let hash: Hash = hash.parse().expect("TODO: replace String in type");
    let parent_hash: Hash = parent_hash.parse().expect("TODO: replace String in type");
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
        logs_bloom |= tx.inner.inner.bloom();
        gas_used += tx.gas_used;
    }

    Header {
        number: Some(slot),
        hash: Some(hash),
        parent_hash,
        timestamp: time.unwrap_or(0) as u64,
        uncles_hash: UNCLES_HASH,
        transactions_root: root,
        receipts_root: root,
        state_root: B256::with_last_byte(1),
        mix_hash: Some(B256::with_last_byte(1)),
        gas_used,
        gas_limit: MAX_BPF_CYCLES.max(gas_used),
        logs_bloom,
        ..Default::default()
    }
}

pub fn build_block(
    block: SolanaBlock,
    tx_receipts: Vec<AnyTransactionReceipt>,
    txs: Vec<Transaction>,
    full: bool,
) -> Block {
    let header = build_block_header(block, &tx_receipts);
    let transactions = if full {
        BlockTransactions::Full(txs)
    } else {
        BlockTransactions::Hashes(txs.into_iter().map(|tx| tx.hash).collect())
    };

    Block {
        header,
        transactions,
        size: Some(U256::from(1)),
        ..Default::default()
    }
}
