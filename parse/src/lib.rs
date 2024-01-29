use common::types::{NeonIxReceipt, NeonTxInfo};
use solana_sdk::signature::Signature;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use thiserror::Error;

mod log;

#[derive(Debug, Error)]
pub enum InstructionParseError {}

#[derive(Debug, Error)]
pub enum TransactionParseError {
    #[error("Failed to decode transaction")]
    InvalidTransaction,
    #[error("Failed to decode instruction")]
    InstructionParseError(#[from] InstructionParseError),
}

fn parse_instruction(_data: &[u8]) -> Result<NeonIxReceipt, InstructionParseError> {
    todo!()
}

#[derive(Debug)]
struct SolTxSigSlotInfo {
    pub signature: Signature,
    pub block_slot: u64,
}

#[derive(Debug)]
struct SolTxMetaInfo {
    pub ident: SolTxSigSlotInfo,
}

// TODO: return NeonTxInfo
pub fn parse(
    transaction: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<(), TransactionParseError> {
    let EncodedConfirmedTransactionWithStatusMeta {
        slot, transaction, ..
    } = transaction;
    let tx = transaction
        .transaction
        .decode()
        .ok_or(TransactionParseError::InvalidTransaction)?;
    let sig_slot_info = SolTxSigSlotInfo {
        signature: tx.signatures[0],
        block_slot: slot,
    };
    let meta_info = SolTxMetaInfo {
        ident: sig_slot_info,
    };
    for ix in tx.message.instructions() {
        //let ix = parse_instruction(&ix.data)?;
    }
    if let Some(OptionSerializer::Some(msgs)) = transaction.meta.map(|meta| meta.log_messages) {
        if let Err(err) = log::parse(msgs) {
            println!("log parsing error {:?}", err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use solana_rpc_client::rpc_client::RpcClient;
    use solana_sdk::signature::Signature;
    use solana_transaction_status::UiTransactionEncoding;

    use super::*;
    use std::str::FromStr;
    use test_log::test;

    // use this to retrieve some transaction from network and store it locally
    #[allow(unused)]
    fn get_transaction_net(signature: &str) -> EncodedConfirmedTransactionWithStatusMeta {
        use std::path::PathBuf;
        let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
        let signature = Signature::from_str(signature).unwrap();
        let tx = client
            .get_transaction(&signature, UiTransactionEncoding::Base64)
            .unwrap();
        let mut path = PathBuf::from_str("./tests/data").unwrap();
        path.push(format!("{}.json", signature));
        std::fs::write(path, serde_json::to_vec(&tx).unwrap()).unwrap();
        tx
    }

    fn get_transaction(signature: &str) -> EncodedConfirmedTransactionWithStatusMeta {
        use std::path::PathBuf;
        let mut path = PathBuf::from_str("./tests/data").unwrap();
        path.push(format!("{}.json", signature));
        let buf = std::fs::read_to_string(&path).unwrap();
        let tx: EncodedConfirmedTransactionWithStatusMeta = serde_json::from_str(&buf).unwrap();
        tx
    }

    #[test]
    pub fn parse_4y() {
        let tx = get_transaction("4YcHMcHwXkpqTfuqafaJigL9SKoYcRhUD9LimTHKjbkhJeLSpdjdsJCirjTqrM7VZC4RBrDJZdrjW5ZAUbqHqhq5");
        parse(tx).unwrap();
    }

    #[test]
    pub fn parse_2y() {
        let tx = get_transaction("2yUjfHPDAEiMnZPFgj4YgEMa9yQ91zjoCx82i6JuZpnWJjXS3UMKNHnvZCCsjdicg5nfTq2CCUfvcpQkiKL6yCss");
        parse(tx).unwrap();
    }
}
