use reth_primitives::alloy_primitives::SignatureError;
use reth_primitives::{alloy_primitives, U128};
use thiserror::Error;

use common::convert::ToNeon;
use common::ethnum::{u256, U256};
use common::neon_lib::types::BalanceAddress;
use common::types::TxEnvelopeExt;
use executor::ExecuteRequest;
use neon_api::NeonApi;

use crate::GasPriceModel;

pub const MAX_TX_SIZE: usize = 127 * 1024;
pub const GAS_LIMIT_LOWER_BOUND: u128 = 21_000;

#[derive(Debug, Error)]
pub enum PreFlightError {
    #[error(transparent)]
    InternalError(#[from] anyhow::Error),
    #[error("transaction size is too big")]
    TxSizeTooBig,
    #[error("gas limit multiplier is missing in evm config")]
    GasLimitMultiplierMissing,
    #[error("gas uint64 overflow")]
    GasU64Overflow,
    #[error("gas limit reached")]
    GasLimitReached,
    #[error("max fee per gas higher than 2^256-1")]
    MaxFeePerGasU256Overflow,
    #[error("{0}: address {1} have {2} want {3}")]
    InsufficientFundsForTransfer(&'static str, alloy_primitives::Address, U256, U256),
    #[error(transparent)]
    SignatureError(#[from] SignatureError),
    #[error(transparent)]
    NeonApiError(#[from] neon_api::NeonApiError),
    #[error("already known")]
    TxAlreadyKnown,
    #[error("wrong chain id")]
    WrongChainId,
    #[error("not an eoa")]
    NotEOA,
    #[error(transparent)]
    DbError(#[from] db::Error),
    #[error("transaction underpriced: have {0} want {1}")]
    Underpriced(U128, U128),
    #[error("proxy configuration doesn't allow underpriced transaction without chain-id")]
    UnderpricedWoChainId,
}

impl PreFlightError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::NeonApiError(err) => err.error_code(),
            Self::TxAlreadyKnown => -32000,
            _ => jsonrpsee::types::ErrorCode::InternalError.code(),
        }
    }
}

impl From<PreFlightError> for jsonrpsee::types::ErrorObjectOwned {
    fn from(value: PreFlightError) -> Self {
        let error_code = value.error_code();
        Self::owned(error_code, value.to_string(), None::<String>)
    }
}

pub struct PreFlightValidator;

impl PreFlightValidator {
    pub async fn validate(
        tx: &ExecuteRequest,
        neon_api: &NeonApi,
        txs: &db::TransactionRepo,
        price: Option<GasPriceModel>,
    ) -> Result<(), PreFlightError> {
        Self::validate_tx_is_new(tx, txs).await?;

        let evm_config = neon_api.get_config().await?;
        let chain_id = tx.chain_id()?;
        let tx_input_len = tx.input_len()?;
        let gas_limit_multiplier_wo_chain_id = evm_config
            .config
            .get("NEON_GAS_LIMIT_MULTIPLIER_NO_CHAINID")
            .ok_or(PreFlightError::GasLimitMultiplierMissing)
            .map(|s| {
                s.parse::<u128>()
                    .map_err(|_| PreFlightError::GasLimitMultiplierMissing)
            })??;
        let tx_gas_limit = tx.gas_limit()?;
        let tx_gas_price = tx.gas_price()?;

        Self::validate_chain_id(chain_id, tx.fallback_chain_id())?;
        Self::validate_sender_eoa(tx, neon_api).await?;
        Self::validate_tx_size(tx_input_len)?;
        Self::validate_gas_limit(
            chain_id,
            tx_input_len,
            tx_gas_limit,
            tx_gas_price,
            gas_limit_multiplier_wo_chain_id,
        )?;
        Self::validate_sender_balance(tx, tx_input_len, tx_gas_limit, tx_gas_price, neon_api)
            .await?;
        if let Some(price) = price {
            Self::validate_tx_gas_price(chain_id, tx_gas_price, &price)?;
            Self::validate_underpriced_tx_wo_chain_id(chain_id, tx_gas_price, &price)?;
        }

        Ok(())
    }

    async fn validate_sender_eoa(
        tx: &ExecuteRequest,
        neon_api: &NeonApi,
    ) -> Result<(), PreFlightError> {
        let code = neon_api
            .get_code(tx.recover_signer()?.to_neon(), None)
            .await?;

        if let Some(code) = code {
            if !code.is_empty() {
                return Err(PreFlightError::NotEOA);
            }
        }

        Ok(())
    }

    fn validate_chain_id(
        chain_id: Option<u64>,
        fallback_chain_id: u64,
    ) -> Result<(), PreFlightError> {
        if let Some(chain_id) = chain_id {
            if chain_id == fallback_chain_id {
                return Ok(());
            }
            return Err(PreFlightError::WrongChainId);
        }

        Ok(())
    }

    async fn validate_tx_is_new(
        tx: &ExecuteRequest,
        txs: &db::TransactionRepo,
    ) -> Result<(), PreFlightError> {
        if txs
            .fetch(db::TransactionBy::Hash(tx.tx_hash().0.into()))
            .await?
            .is_empty()
        {
            return Ok(());
        }

        Err(PreFlightError::TxAlreadyKnown)
    }

    fn validate_tx_size(tx_input_len: usize) -> Result<(), PreFlightError> {
        if tx_input_len > (MAX_TX_SIZE) {
            return Err(PreFlightError::TxSizeTooBig);
        }

        Ok(())
    }

    fn validate_gas_limit(
        chain_id: Option<u64>,
        tx_input_len: usize,
        tx_gas_limit: u128,
        tx_gas_price: Option<u128>,
        gas_limit_multiplier_wo_chain_id: u128,
    ) -> Result<(), PreFlightError> {
        let gas_limit = if chain_id.is_some() || tx_input_len == 0 {
            tx_gas_limit
        } else {
            match tx_gas_limit.checked_mul(gas_limit_multiplier_wo_chain_id) {
                Some(v) => v,
                None => return Err(PreFlightError::GasU64Overflow),
            }
        };

        if gas_limit < GAS_LIMIT_LOWER_BOUND {
            return Err(PreFlightError::GasLimitReached);
        }

        if gas_limit > u64::MAX as u128 {
            return Err(PreFlightError::GasU64Overflow);
        }

        let gas_limit_u256 = u256::new(gas_limit);
        let gas_price = u256::new(tx_gas_price.unwrap_or(0));
        gas_limit_u256
            .checked_mul(gas_price)
            .ok_or(PreFlightError::MaxFeePerGasU256Overflow)?;

        Ok(())
    }

    async fn validate_sender_balance(
        tx: &ExecuteRequest,
        tx_input_len: usize,
        tx_gas_limit: u128,
        tx_gas_price: Option<u128>,
        neon_api: &NeonApi,
    ) -> Result<(), PreFlightError> {
        let sender_address = tx.recover_signer()?;
        let balance_address = BalanceAddress {
            address: sender_address.to_neon(),
            chain_id: tx.fallback_chain_id(),
        };
        let sender_balance = neon_api.get_balance(balance_address, None).await?; // todo: which commitment to use?
        let required_balance =
            U256::from(tx_gas_price.unwrap_or(0)) * U256::from(tx_gas_limit) + to_u256(tx.value()?);

        if required_balance <= sender_balance {
            return Ok(());
        }

        let message = if tx_input_len == 0 {
            "insufficient funds for transfer"
        } else {
            "insufficient funds for gas * price + value"
        };

        Err(PreFlightError::InsufficientFundsForTransfer(
            message,
            sender_address,
            sender_balance,
            required_balance,
        ))
    }

    fn validate_tx_gas_price(
        chain_id: Option<u64>,
        tx_gas_price: Option<u128>,
        price: &GasPriceModel,
    ) -> Result<(), PreFlightError> {
        let min_gas_price = price.min_acceptable_gas_price;
        if let Some(gas_price) = tx_gas_price {
            let gas_price = U128::from(gas_price);
            if gas_price < min_gas_price && chain_id.is_some() {
                return Err(PreFlightError::Underpriced(gas_price, min_gas_price));
            }
        }

        Ok(())
    }

    fn validate_underpriced_tx_wo_chain_id(
        chain_id: Option<u64>,
        tx_gas_price: Option<u128>,
        price: &GasPriceModel,
    ) -> Result<(), PreFlightError> {
        if chain_id.is_some() {
            return Ok(());
        }
        if let Some(gas_price) = tx_gas_price {
            let gas_price = U128::from(gas_price);
            if gas_price < price.min_wo_chain_id_acceptable_gas_price {
                return Err(PreFlightError::UnderpricedWoChainId);
            }
        }

        Ok(())
    }
}

fn to_u256(v: alloy_primitives::U256) -> U256 {
    let limbs = v.as_limbs();
    let limb0 = (limbs[1] as u128) << 64 | (limbs[0] as u128); // Lower u128
    let limb1 = (limbs[3] as u128) << 64 | (limbs[2] as u128); // Upper u128

    U256([limb0, limb1])
}
