use std::sync::{Arc, RwLock};

use async_trait::async_trait;
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
use dashmap::DashMap;
use serde_json::Value;
use solana_client::client_error::ClientError;
use solana_client::rpc_config::RpcSignaturesForAddressConfig;
use solana_client::rpc_config::{RpcEncodingConfigWrapper, RpcTransactionConfig};
use solana_client::rpc_request::RpcRequest;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature as RpcTxStatus;
use solana_client::rpc_sender::{RpcSender, RpcTransportStats};

use super::*;

struct TransactionDB {
    statuses: Arc<RwLock<Vec<RpcTxStatus>>>,
    txs: Arc<DashMap<Signature, ConfirmedTransactionWithStatusMeta>>,
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
                println!("{:?}", encoding);
                let encoded = transaction.encode(encoding, None).unwrap();
                let transaction =
                    serde_json::to_value(&encoded).expect("cannot serialize transaction");
                println!("tx: {:?}", transaction);
                Ok(transaction)
            }
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

#[tokio::test]
async fn correct_order() {
    let address = Pubkey::new_unique();

    let txs = (1..3568_u64)
        .map(dummy_confirmed_tx_with_meta)
        .collect::<DashMap<_, _>>();
    let (sign_map, mut signatures): (DashMap<_, _>, VecDeque<_>) = txs
        .iter()
        .map(|ref_| *ref_.key())
        .enumerate()
        .map(|(idx, sign)| ((sign, idx + 1), sign))
        .unzip();
    let signatures2 = signatures
        .iter()
        .enumerate()
        .map(|(idx, &sign)| (idx as u64 + 1, sign))
        .collect::<DashMap<_, _>>();
    let sign_map = sign_map;
    let sign_to_num = |sign| *sign_map.get(sign).unwrap();
    let num_to_sign = |idx| *signatures2.get(&idx).unwrap().value();

    let statuses = signatures
        .iter()
        .enumerate()
        .map(|(idx, sign)| RpcTxStatus {
            signature: sign.to_string(),
            slot: idx as u64,
            err: None,
            memo: None,
            block_time: None,
            confirmation_status: None,
        })
        .collect::<Vec<_>>();
    let statuses = Arc::new(RwLock::new(statuses));
    let txs = Arc::new(txs);
    let tx_db = TransactionDB {
        statuses: statuses.clone(),
        txs: txs.clone(),
        address,
    };
    let api = SolanaApi::with_sender(tx_db);
    let mut traverse = TraverseLedger::new(api, address, None);

    const ADD_NEW_AT: usize = 1337;
    let mut counter = 0;

    while !signatures.is_empty() {
        if counter == ADD_NEW_AT {
            let mut statuses = statuses.write().unwrap();
            for (idx, (sign, tx)) in
                (5000..15995).map(|idx| (idx, dummy_confirmed_tx_with_meta(idx)))
            {
                signatures.push_back(sign);
                signatures2.insert(idx, sign);
                statuses.push(RpcTxStatus {
                    signature: num_to_sign(idx).to_string(),
                    slot: idx,
                    err: None,
                    memo: None,
                    block_time: None,
                    confirmation_status: None,
                });
                txs.insert(sign, tx);
            }
        }
        counter += 1;

        let sign = signatures.pop_front().unwrap();
        let tx = traverse.next().await.unwrap().unwrap();
        assert_eq!(
            *tx.tx.signatures.first().unwrap(),
            sign,
            "{} != {}",
            sign_to_num(tx.tx.signatures.first().unwrap()),
            sign_to_num(&sign)
        );
    }
}
