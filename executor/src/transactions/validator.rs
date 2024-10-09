use alloy_consensus::{Transaction, TxEnvelope};
use reth_primitives::alloy_primitives;
use reth_primitives::alloy_primitives::SignatureError;
use thiserror::Error;

use common::convert::ToNeon;
use common::ethnum::{u256, U256};
use common::neon_lib::commands::get_config::GetConfigResponse;
use common::neon_lib::types::BalanceAddress;
use neon_api::NeonApi;

use crate::ExecuteRequest;

pub const MAX_TX_SIZE: usize = 127 * 1024;
pub const GAS_LIMIT_LOWER_BOUND: u128 = 21_000;

#[derive(Debug, Error)]
pub enum PreFlightError {
    #[error("unsupported tx type")]
    UnsupportedTxType,
    #[error("transaction size is too big")]
    TxSizeTooBig,
    #[error("gas limit multiplier is missing in evm config")]
    GasLimitMultiplierMissing,
    #[error("gas u64 overflow")]
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
}

impl PreFlightError {
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::NeonApiError(err) => err.error_code(),
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

pub struct PreFlightValidator {
    evm_config: GetConfigResponse,
    neon_api: NeonApi,
}

impl PreFlightValidator {
    pub async fn try_new(neon_api: NeonApi) -> Result<Self, PreFlightError> {
        let evm_config = neon_api.get_config().await?;
        Ok(Self {
            evm_config,
            neon_api,
        })
    }

    pub async fn validate(&self, tx: &ExecuteRequest) -> Result<(), PreFlightError> {
        Self::validate_tx_size(tx)?;
        self.validate_gas_limit(tx)?;
        self.validate_sender_balance(tx).await
    }

    fn validate_tx_size(tx: &ExecuteRequest) -> Result<(), PreFlightError> {
        let len = tx_input_len(tx)?;

        if len > (MAX_TX_SIZE) {
            return Err(PreFlightError::TxSizeTooBig);
        }

        Ok(())
    }

    fn validate_gas_limit(&self, tx: &ExecuteRequest) -> Result<(), PreFlightError> {
        let chain_id = chain_id(tx)?;
        let len = tx_input_len(tx)?;
        let tx_gas_limit = tx_gas_limit(tx)?;

        let gas_limit = if chain_id.is_some() || len == 0 {
            tx_gas_limit
        } else {
            let gas_limit_multiplier_wo_chain_id = self
                .evm_config
                .config
                .get("NEON_GAS_LIMIT_MULTIPLIER_NO_CHAINID")
                .ok_or(PreFlightError::GasLimitMultiplierMissing)
                .map(|s| {
                    s.parse::<u128>()
                        .map_err(|_| PreFlightError::GasLimitMultiplierMissing)
                })??;
            match tx_gas_limit.checked_mul(gas_limit_multiplier_wo_chain_id) {
                Some(v) => v,
                None => return Err(PreFlightError::GasU64Overflow),
            }
        };

        if gas_limit < GAS_LIMIT_LOWER_BOUND {
            return Err(PreFlightError::GasLimitMultiplierMissing);
        }

        if gas_limit > u64::MAX as u128 {
            return Err(PreFlightError::GasU64Overflow);
        }

        let gas_limit_u256 = u256::new(gas_limit);
        let gas_price = u256::new(tx_gas_price(tx)?.unwrap_or(0));
        gas_limit_u256
            .checked_mul(gas_price)
            .ok_or(PreFlightError::MaxFeePerGasU256Overflow)?;

        Ok(())
    }

    async fn validate_sender_balance(&self, tx: &ExecuteRequest) -> Result<(), PreFlightError> {
        let sender_address = tx.recover_signer()?;
        let balance_address = BalanceAddress {
            address: sender_address.to_neon(),
            chain_id: tx.fallback_chain_id, // todo: we should make sure that tx.chain_id matches the fallback_chain_id
        };
        let sender_balance = self.neon_api.get_balance(balance_address, None).await?; // todo: which commitment to use?
        let required_balance = U256::from(tx_gas_price(tx)?.unwrap_or(0))
            * U256::from(tx_gas_limit(tx)?)
            + to_u256(tx_value(tx)?);

        if required_balance <= sender_balance {
            return Ok(());
        }

        let message = if tx_input_len(tx)? == 0 {
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
}

fn to_u256(v: alloy_primitives::U256) -> U256 {
    let limbs = v.as_limbs();
    let limb0 = (limbs[1] as u128) << 64 | (limbs[0] as u128); // Lower u128
    let limb1 = (limbs[3] as u128) << 64 | (limbs[2] as u128); // Upper u128

    U256([limb0, limb1])
}

fn tx_input_len(tx: &TxEnvelope) -> Result<usize, PreFlightError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().input.len()),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().input.len()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().input.len()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().tx().input.len()),
        _ => Err(PreFlightError::UnsupportedTxType),
    }
}

fn chain_id(tx: &TxEnvelope) -> Result<Option<u64>, PreFlightError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().chain_id),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().chain_id()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().chain_id()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().chain_id()),
        _ => Err(PreFlightError::UnsupportedTxType),
    }
}

fn tx_gas_limit(tx: &TxEnvelope) -> Result<u128, PreFlightError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().gas_limit),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().gas_limit()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().gas_limit()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().gas_limit()),
        _ => Err(PreFlightError::UnsupportedTxType),
    }
}

fn tx_gas_price(tx: &TxEnvelope) -> Result<Option<u128>, PreFlightError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().gas_price()),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().gas_price()),
        _ => Err(PreFlightError::UnsupportedTxType),
    }
}

fn tx_value(tx: &TxEnvelope) -> Result<alloy_primitives::U256, PreFlightError> {
    match tx {
        TxEnvelope::Legacy(signed) => Ok(signed.tx().value),
        TxEnvelope::Eip1559(signed) => Ok(signed.tx().value),
        TxEnvelope::Eip2930(signed) => Ok(signed.tx().value),
        TxEnvelope::Eip4844(signed) => Ok(signed.tx().tx().value),
        _ => Err(PreFlightError::UnsupportedTxType),
    }
}
