use anyhow::anyhow;
use tokio::sync::oneshot;

use crate::transactions::OngoingTransaction;
use crate::ExecuteResult;

type ResultSender = oneshot::Sender<ExecuteResult>;

/// The rule is simple: The last owner must disarm the Silencer.
#[derive(Debug)]
#[must_use = "must be disarmed before drop"]
pub struct Silencer(Option<ResultSender>);

impl Silencer {
    pub fn disarm(&mut self, result: ExecuteResult) -> bool {
        if let Some(sender) = self.0.take() {
            sender.send(result).is_ok()
        } else {
            false
        }
    }
}

impl From<Option<ResultSender>> for Silencer {
    fn from(value: Option<ResultSender>) -> Self {
        Silencer(value)
    }
}

impl From<ResultSender> for Silencer {
    fn from(value: ResultSender) -> Self {
        Silencer(Some(value))
    }
}

impl Drop for Silencer {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(ExecuteResult::Error(anyhow!(
                "sender dropped without disarm"
            )));
        }
    }
}

/// The rule is simple: The last owner must disarm the TransactionEntry or inner Silencer.
#[derive(Debug)]
#[must_use = "must be disarmed before drop"]
pub struct TransactionEntry {
    tx: OngoingTransaction,
    sender: Silencer,
}

impl TransactionEntry {
    pub fn new(tx: OngoingTransaction, sender: Silencer) -> Self {
        Self { tx, sender }
    }

    pub fn no_feedback(tx: OngoingTransaction) -> Self {
        Self::new(tx, None.into())
    }

    pub fn tx(&self) -> &OngoingTransaction {
        &self.tx
    }

    pub fn disarm(&mut self, result: ExecuteResult) -> bool {
        self.sender.disarm(result)
    }

    #[must_use = "must be disarmed before drop"]
    pub fn destruct(self) -> (OngoingTransaction, Silencer) {
        let Self { tx, sender } = self;
        (tx, sender)
    }
}
