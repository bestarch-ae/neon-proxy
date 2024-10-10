use std::fmt;
use std::path::Path;

use alloy_consensus::SignableTransaction;
use alloy_network::{TxSigner, TxSignerSync};
use alloy_signer::{Signature as EthSignature, SignerSync};
use alloy_signer_wallet::LocalWallet;
use anyhow::anyhow;
use async_trait::async_trait;
use reth_primitives::Address;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::{EncodableKey, Signer, SignerError};

pub struct Operator {
    sol_keypair: Keypair,
    eth_keypair: LocalWallet,
}

impl Operator {
    pub fn read_from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let sol_keypair = Keypair::read_from_file(path)
            .map_err(|err| anyhow!("cannot read keypair from file: {err:?}"))?;
        Self::from_keypair(sol_keypair)
    }

    pub fn address(&self) -> Address {
        self.eth_keypair.address()
    }

    pub fn pubkey(&self) -> Pubkey {
        self.sol_keypair.pubkey()
    }

    fn from_keypair(sol_keypair: Keypair) -> anyhow::Result<Self> {
        let eth_keypair = LocalWallet::from_field_bytes(sol_keypair.secret().as_bytes().into())?;
        Ok(Self {
            sol_keypair,
            eth_keypair,
        })
    }

    pub fn sign_message(&self, msg: &[u8]) -> anyhow::Result<EthSignature> {
        self.eth_keypair.sign_message_sync(msg).map_err(Into::into)
    }

    pub fn sign_eth_transaction(
        &self,
        tx: &mut dyn SignableTransaction<EthSignature>,
    ) -> anyhow::Result<EthSignature> {
        self.sign_transaction_sync(tx).map_err(Into::into)
    }
}

impl fmt::Debug for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operator")
            .field("sol_keypair", &self.sol_keypair.pubkey())
            .field("eth_keypair", &self.eth_keypair.address())
            .finish()
    }
}

impl AsRef<Keypair> for Operator {
    fn as_ref(&self) -> &Keypair {
        &self.sol_keypair
    }
}

impl Signer for Operator {
    fn try_pubkey(&self) -> Result<Pubkey, SignerError> {
        self.sol_keypair.try_pubkey()
    }

    fn try_sign_message(&self, message: &[u8]) -> Result<Signature, SignerError> {
        self.sol_keypair.try_sign_message(message)
    }

    fn is_interactive(&self) -> bool {
        false
    }
}

#[async_trait]
impl TxSigner<EthSignature> for Operator {
    fn address(&self) -> Address {
        self.eth_keypair.address()
    }

    async fn sign_transaction(
        &self,
        tx: &mut dyn SignableTransaction<EthSignature>,
    ) -> alloy_signer::Result<EthSignature> {
        self.sign_transaction_sync(tx)
    }
}

impl TxSignerSync<EthSignature> for Operator {
    fn address(&self) -> Address {
        self.eth_keypair.address()
    }

    fn sign_transaction_sync(
        &self,
        tx: &mut dyn SignableTransaction<EthSignature>,
    ) -> alloy_signer::Result<EthSignature> {
        self.eth_keypair.sign_transaction_sync(tx)
    }
}
