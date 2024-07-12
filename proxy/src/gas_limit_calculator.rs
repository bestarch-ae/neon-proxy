use alloy_rlp::Encodable;
use reth_primitives::{Address, Signature, Transaction, TransactionSigned, TxKind, TxLegacy, U256};
use thiserror::Error;
use tracing::debug;

use common::neon_lib::commands::emulate::EmulateResponse;
use common::neon_lib::types::TxParams;

/// openzeppelin gas-limit check
const OZ_GAS_LIMIT: u64 = 30_000;
/// minimal gas limit for NeonTx: start (10k), execute (10k), finalization (5k)
const MIN_GAS_LIMIT: u64 = 25_000;
/// holder, payer, treasury-pool-address, payer-token-address, SolSysProg.ID, NeonProg.ID, CbProg.ID
const BASE_ACCOUNT_CNT: usize = 7;
const SOL_ALT_PROG_MAX_TX_ACCOUNT_CNT: usize = 27;

#[derive(Error, Debug)]
pub enum GasLimitError {
    #[error("too many accounts: {0} > {1}")]
    TooManyAccounts(usize, usize),
}

#[derive(Debug, Copy, Clone)]
pub struct GasLimitCalculator {
    max_tx_account_cnt: usize,
}

impl GasLimitCalculator {
    pub fn new(max_tx_account_cnt: usize) -> Self {
        Self { max_tx_account_cnt }
    }

    /// Estimate gas limit for the transaction
    pub fn estimate(
        &self,
        tx: &TxParams,
        emul_resp: &EmulateResponse,
        holder_msg_size: u64,
    ) -> Result<u64, GasLimitError> {
        let execution_const = emul_resp.used_gas;
        let tx_size_cost = Self::tx_size_cost(tx, emul_resp.used_gas, holder_msg_size);
        let alt_cost = self.alt_cost(emul_resp)?;

        let total_cost = std::cmp::max(execution_const + tx_size_cost + alt_cost, MIN_GAS_LIMIT);

        debug!(
            ?total_cost,
            ?execution_const,
            ?tx_size_cost,
            ?alt_cost,
            "estimated gas"
        );

        Ok(total_cost)
    }

    fn tx_size_cost(tx: &TxParams, used_gas: u64, holder_msg_size: u64) -> u64 {
        if used_gas < OZ_GAS_LIMIT {
            return 0;
        }

        let to = tx
            .to
            .as_ref()
            .map(|to| TxKind::Call(Address::from(to.as_bytes())))
            .unwrap_or(TxKind::Create);
        let eth_tx = Transaction::from(TxLegacy {
            chain_id: Some(245022934 * 512),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0, // we can take gas_limit from the tx, but it won't affect the transaction size
            to,
            value: U256::ZERO, // we can take value from the tx, but it won't affect the transaction size
            input: tx.data.clone().unwrap_or_default().into(),
        });
        let signature = Signature {
            r: U256::ZERO,
            s: U256::ZERO,
            odd_y_parity: false,
        };

        let eth_tx_len =
            TransactionSigned::from_transaction_and_signature(eth_tx, signature).length() as u64;
        (eth_tx_len / holder_msg_size + 1) * 5000
    }

    fn alt_cost(&self, emul_resp: &EmulateResponse) -> Result<u64, GasLimitError> {
        let acc_cnt = emul_resp.solana_accounts.len() + BASE_ACCOUNT_CNT;
        if acc_cnt > self.max_tx_account_cnt {
            return Err(GasLimitError::TooManyAccounts(
                acc_cnt,
                self.max_tx_account_cnt,
            ));
        }
        if acc_cnt >= SOL_ALT_PROG_MAX_TX_ACCOUNT_CNT {
            // ALT ix: create + ceil(256/30) extend + deactivate + close
            return Ok(5000 * 12);
        }
        Ok(0)
    }
}
