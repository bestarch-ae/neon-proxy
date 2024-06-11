use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use solana_client::client_error::ClientError;
use solana_client::rpc_config::{RpcBlockConfig, RpcSignaturesForAddressConfig};
use solana_client::rpc_config::{RpcEncodingConfigWrapper, RpcTransactionConfig};
use solana_client::rpc_request::RpcRequest;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature as RpcTxStatus;
use solana_client::rpc_sender::{RpcSender, RpcTransportStats};
use tracing_subscriber::EnvFilter;

use common::solana_sdk::hash::Hash;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::message::v0::LoadedAddresses;
use common::solana_sdk::message::{legacy::Message as LegacyMessage, VersionedMessage};
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signer::Signer;
use common::solana_sdk::slot_history::Slot;
use common::solana_sdk::transaction::VersionedTransaction;
use common::solana_transaction_status::ConfirmedTransactionWithStatusMeta;
use common::solana_transaction_status::VersionedTransactionWithStatusMeta;
use common::solana_transaction_status::{Rewards, TransactionStatusMeta};
use common::solana_transaction_status::{TransactionWithStatusMeta, UiTransactionEncoding};

use crate::solana_api::SIGNATURES_LIMIT;

use super::*;

struct TransactionDB {
    statuses: Arc<RwLock<Vec<RpcTxStatus>>>,
    txs: Arc<DashMap<Signature, ConfirmedTransactionWithStatusMeta>>,
    blocks: Arc<DashMap<u64, UiConfirmedBlock>>,
    address: Pubkey,
}

impl TransactionDB {
    pub fn get_statuses(
        &self,
        min: Option<Signature>,
        max: Option<Signature>,
        limit: Option<usize>,
    ) -> Vec<RpcTxStatus> {
        eprintln!(
            "request from: {:?} to: {:?}",
            min.map(sign_to_num),
            max.map(sign_to_num)
        );
        let limit = limit.unwrap_or(SIGNATURES_LIMIT);
        let statuses = self.statuses.read().unwrap();

        let check_max = max.is_none() && min.is_some();
        let mut max = max.map_or(statuses.len(), |sign| {
            statuses
                .iter()
                .position(|s| Signature::from_str(&s.signature).unwrap() == sign)
                .expect("could not find max signature")
        });

        let min = min.map_or_else(
            || max.saturating_sub(limit),
            |sign| {
                (statuses
                    .iter()
                    .position(|s| Signature::from_str(&s.signature).unwrap() == sign)
                    .expect("could not find min signature")
                    + 1)
                .max(max.saturating_sub(limit))
            },
        );

        if check_max && max - min > SIGNATURES_LIMIT {
            max = min + SIGNATURES_LIMIT;
        }

        let out = &statuses[min..max];
        eprintln!("min: {}, max: {}, len: {}", min, max, out.len());
        out.iter().cloned().rev().collect()
    }
}

type RequestResult = Result<Value, ClientError>;

#[async_trait]
impl RpcSender for TransactionDB {
    async fn send(&self, request: RpcRequest, params: Value) -> RequestResult {
        tracing::info!(?request, ?params, "outgoing request");

        match request {
            RpcRequest::GetSignaturesForAddress => {
                let (address, config): (String, RpcSignaturesForAddressConfig) =
                    serde_json::from_value(params).expect("could not parse getSignatures params");
                let address = Pubkey::from_str(&address).unwrap();
                assert_eq!(address, self.address);
                let statuses = self.get_statuses(
                    config.until.map(|s| Signature::from_str(&s).unwrap()),
                    config.before.map(|s| Signature::from_str(&s).unwrap()),
                    config.limit,
                );

                let statuses = serde_json::to_value(statuses).unwrap();
                Ok(statuses)
            }
            RpcRequest::GetVersion => {
                let version: Value =
                    serde_json::from_str(r#"{"feature-set":4192065167,"solana-core":"1.10.31"}"#)
                        .unwrap();
                Ok(version)
            }
            RpcRequest::GetTransaction => {
                let (signature, config): (String, RpcEncodingConfigWrapper<RpcTransactionConfig>) =
                    serde_json::from_value(params).expect("could not parse transaction request");
                let signature: Signature = signature.parse().expect("cannot parse signature");
                let transaction = self.txs.get(&signature).expect("unknown signature").clone();
                let encoding = config
                    .convert_to_current()
                    .encoding
                    .unwrap_or(UiTransactionEncoding::Json);
                let encoded = transaction.encode(encoding, None).unwrap();
                let transaction =
                    serde_json::to_value(&encoded).expect("cannot serialize transaction");
                Ok(transaction)
            }
            RpcRequest::GetBlock => {
                let (slot, _config): (u64, RpcBlockConfig) =
                    serde_json::from_value(params).expect("could not parse transaction request");
                let block = self.blocks.get(&slot).unwrap();
                let block = serde_json::to_value(block.value()).expect("cannot serialize block");
                Ok(block)
            }
            RpcRequest::GetBlocks => Ok(serde_json::to_value(Vec::<()>::new()).unwrap()),
            RpcRequest::GetSlot => Ok(serde_json::to_value(u64::MAX).unwrap()),
            request => panic!("unsupported request: {} - {}", request, params),
        }
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }

    fn url(&self) -> String {
        String::new()
    }
}

fn sign_to_num(sign: Signature) -> u64 {
    u64::from_ne_bytes(sign.as_ref()[56..64].try_into().unwrap())
}

fn dummy_transaction() -> VersionedTransaction {
    let keypair = Keypair::new();
    let keypair1 = Keypair::new();
    // Copied from solana tests
    let program_id0 = Pubkey::new_unique();
    let program_id1 = Pubkey::new_unique();
    let id0 = keypair.pubkey();
    let id1 = keypair1.pubkey();
    let id2 = Pubkey::new_unique();
    let id3 = Pubkey::new_unique();
    let instructions = vec![
        Instruction::new_with_bincode(program_id0, &0, vec![AccountMeta::new(id0, false)]),
        Instruction::new_with_bincode(program_id0, &0, vec![AccountMeta::new(id1, true)]),
        Instruction::new_with_bincode(program_id1, &0, vec![AccountMeta::new_readonly(id2, false)]),
        Instruction::new_with_bincode(program_id1, &0, vec![AccountMeta::new_readonly(id3, false)]),
    ];

    let mut message = LegacyMessage::new(&instructions, Some(&id0));
    message.recent_blockhash = Hash::new_unique();

    VersionedTransaction::try_new(VersionedMessage::Legacy(message), &[&keypair, &keypair1])
        .unwrap()
}

fn dummy_transaction_meta() -> TransactionStatusMeta {
    TransactionStatusMeta {
        status: Ok(()),
        fee: 42,
        pre_balances: Vec::new(),
        post_balances: Vec::new(),
        inner_instructions: Some(Vec::new()),
        log_messages: Some(Vec::new()),
        pre_token_balances: Some(Vec::new()),
        post_token_balances: Some(Vec::new()),
        rewards: Some(Rewards::default()),
        loaded_addresses: LoadedAddresses::default(),
        return_data: None,
        compute_units_consumed: Some(42),
    }
}

fn dummy_confirmed_tx_with_meta(slot: Slot) -> (Signature, ConfirmedTransactionWithStatusMeta) {
    let transaction = dummy_transaction();
    let signature = *transaction.signatures.first().unwrap();
    (
        signature,
        ConfirmedTransactionWithStatusMeta {
            tx_with_meta: TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta: dummy_transaction_meta(),
            }),
            slot,
            block_time: Some(12),
        },
    )
}

fn create_block() -> UiConfirmedBlock {
    let signatures = std::iter::repeat_with(Signature::new_unique)
        .take(100)
        .map(|s| s.to_string())
        .collect();

    UiConfirmedBlock {
        previous_blockhash: Hash::new_unique().to_string(),
        blockhash: Hash::new_unique().to_string(),
        parent_slot: 0,
        transactions: None,
        signatures: Some(signatures),
        rewards: None,
        block_time: None,
        block_height: None,
    }
}

fn insert_txs_in_block(block: &mut UiConfirmedBlock, txs: &[Signature]) {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut prev_idx = 0;
    for sign in txs {
        prev_idx = rng.gen_range(prev_idx..block.signatures.as_ref().unwrap().len()) + 1;
        block
            .signatures
            .as_mut()
            .unwrap()
            .insert(prev_idx, sign.to_string());
    }
}

#[tokio::test]
async fn correct_order() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let address = Pubkey::new_unique();

    const TXS_PER_SLOT_1: u64 = 3;
    let idx_to_slot = |per_slot, idx| (idx - 1) / per_slot;
    let (txs, mut signatures): (DashMap<_, _>, VecDeque<_>) = (1..3568_u64)
        .map(|idx| dummy_confirmed_tx_with_meta(idx_to_slot(TXS_PER_SLOT_1, idx)))
        .map(|(sign, tx)| ((sign, tx), sign))
        .unzip();
    let sign_map: DashMap<_, _> = signatures
        .iter()
        .cloned()
        .enumerate()
        .map(|(idx, sign)| (sign, idx + 1))
        .collect();
    let signatures2 = signatures
        .iter()
        .enumerate()
        .map(|(idx, &sign)| (idx as u64 + 1, sign))
        .collect::<DashMap<_, _>>();

    let txs = Arc::new(txs);
    let blocks = Arc::new(DashMap::new());
    let populate_blocks = |per_slot, signatures: &[Signature]| {
        for signs in signatures.chunks(per_slot) {
            let slot = txs.get(signs.first().unwrap()).unwrap().value().slot;
            assert!(signs
                .iter()
                .all(|sign| { txs.get(sign).unwrap().value().slot == slot }));
            insert_txs_in_block(
                blocks.entry(slot).or_insert_with(create_block).value_mut(),
                signs,
            )
        }
    };
    populate_blocks(TXS_PER_SLOT_1 as usize, signatures.make_contiguous());

    let sign_map = sign_map;
    let sign_to_num = |sign: &'_ Signature| *sign_map.get(sign).unwrap();
    let num_to_sign = |idx| *signatures2.get(&idx).unwrap().value();

    let statuses = signatures
        .iter()
        .enumerate()
        .map(|(idx, sign)| RpcTxStatus {
            signature: sign.to_string(),
            slot: idx as u64 / TXS_PER_SLOT_1,
            err: None,
            memo: None,
            block_time: None,
            confirmation_status: None,
        })
        .collect::<Vec<_>>();
    let statuses = Arc::new(RwLock::new(statuses));
    let tx_db = TransactionDB {
        statuses: statuses.clone(),
        txs: txs.clone(),
        blocks: blocks.clone(),
        address,
    };
    let api = SolanaApi::with_sender(tx_db);
    let mut traverse = TraverseLedger::new_with_api(
        api,
        TraverseConfig {
            target_key: address,
            signature_buffer_limit: Some(3000),
            ..Default::default()
        },
        None,
    );

    const TXS_PER_SLOT_2: u64 = 2;
    const ADD_NEW_AT: usize = 1337;
    let mut counter = 0;

    while !signatures.is_empty() {
        if counter == ADD_NEW_AT {
            let mut new_signatures = Vec::new();
            let mut statuses = statuses.write().unwrap();
            for (idx, (sign, tx)) in
                (5000..15995).map(|idx| (idx, dummy_confirmed_tx_with_meta(idx / TXS_PER_SLOT_2)))
            {
                new_signatures.push(sign);
                signatures.push_back(sign);
                signatures2.insert(idx, sign);
                sign_map.insert(sign, idx as usize);
                statuses.push(RpcTxStatus {
                    signature: num_to_sign(idx).to_string(),
                    slot: idx / TXS_PER_SLOT_2,
                    err: None,
                    memo: None,
                    block_time: None,
                    confirmation_status: None,
                });
                txs.insert(sign, tx);
            }
            populate_blocks(TXS_PER_SLOT_2 as usize, &new_signatures);
        }
        counter += 1;

        let sign = signatures.pop_front().unwrap();
        let num = sign_to_num(&sign);
        println!("waiting for {sign}, idx: {num},  len: {}", signatures.len());
        let tx = loop {
            match traverse.next().await.unwrap().unwrap() {
                LedgerItem::Transaction(tx) => break tx,
                _item => continue, // Skip blocks for now
            }
        };
        assert_eq!(
            *tx.tx.signatures.first().unwrap(),
            sign,
            "{} != {}",
            sign_to_num(tx.tx.signatures.first().unwrap()),
            sign_to_num(&sign)
        );
    }
}
