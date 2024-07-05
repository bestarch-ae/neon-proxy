use std::str::FromStr;
use std::str::Utf8Error;

use arrayref::array_ref;
use base64::prelude::{Engine, BASE64_STANDARD as BASE64};
use common::solana_sdk::pubkey::Pubkey;
use thiserror::Error;
use tracing::{error, warn};

use common::ethnum::U256;
use common::types::{EventKind, EventLog as NeonLogTxEvent, TxHash};

const LOG_TRUNCATED_MSG: &str = "Log truncated";
const ALREADY_FINALIZED_MSG: &str = "Program log: Storage Account is finalized";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid base64")]
    InvalidBase64(#[from] base64::DecodeError),
    #[error("Invalid base64")]
    InvalidBase64Slice(#[from] base64::DecodeSliceError),
    #[error("Invalid utf8")]
    InvalidUtf8(#[from] Utf8Error),
    #[error("Invalid mnemonic")]
    InvalidMnemonic(#[from] BadMnemonic),
    #[error("Invalid event")]
    InvalidEvent,
    #[error("Invalid hash")]
    InvalidHash,
    #[error("Invalid log line")]
    InvalidLog,
}

// Copy-paste from evm_log_decoder.py
#[allow(dead_code)]
#[derive(Debug)]
pub struct NeonLogInfo {
    pub sig: Option<TxHash>,
    pub ix: Option<NeonLogTxIx>,
    pub ret: Option<NeonLogTxReturn>,
    pub steps: Option<NeonLogTxSteps>,
    pub event_list: Vec<NeonLogTxEvent>,
    pub is_truncated: bool,
    pub is_already_finalized: bool,
}

impl NeonLogInfo {
    pub fn gas_used(&self) -> U256 {
        if let Some(gas) = self.ret.as_ref().map(|ret| ret.gas_used) {
            tracing::debug!("returning gas_used from ret: {}", gas);
            return gas;
        }
        let gas = self.ix.as_ref().map(|ix| ix.gas_used).unwrap_or_default();
        tracing::debug!("ix gas_used: {}", gas);
        gas
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct NeonLogTxSteps {
    pub steps: u64,
    pub total_steps: u64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct NeonLogTxReturn {
    pub gas_used: U256,
    pub status: u8,
    pub is_canceled: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct NeonLogTxCancel {
    pub gas_used: u64,
}

#[derive(Debug)]
pub struct NeonLogTxIx {
    pub gas_used: U256,
    pub total_gas_used: U256,
}

#[derive(Debug, Copy, Clone)]
enum Mnemonic {
    Miner,
    Hash,
    Return,
    Log(u8),
    Enter,
    Exit,
    Gas,
    Steps,
    Reset,
}

impl Mnemonic {
    const fn requires_arg(&self) -> bool {
        !matches!(self, Self::Reset)
    }

    fn decode_hash(s: &str) -> Result<TxHash, Error> {
        // TODO: figure it out, seems weird.
        // With 32 byte buf it reports OutputSliceTooSmall
        let mut buf = [0u8; 33];
        let len = BASE64.decode_slice(s.as_bytes(), &mut buf)?;
        if len != 32 {
            return Err(Error::InvalidHash);
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&buf[0..32]);
        Ok(hash.into())
    }

    /// Unpacks base64-encoded event data:
    /// LOG0 address [0] data
    /// LOG1 address [1] topic1 data
    /// LOG2 address [2] topic1 topic2 data
    /// LOG3 address [3] topic1 topic2 topic3 data
    /// LOG4 address [4] topic1 topic2 topic3 topic4 data
    fn decode_tx_event(topics_count: u8, s: &str) -> Result<NeonLogTxEvent, Error> {
        use common::evm_loader::types::Address;

        let parts = s.split(' ').collect::<Vec<_>>();
        if parts.len() < 3 {
            error!("Invalid event: less than 3 parts");
            return Err(Error::InvalidEvent);
        }
        if topics_count > 4 {
            error!("Invalid event: topics count > 4");
            return Err(Error::InvalidEvent);
        }

        let mut buf = [0u8; 4];
        let msg_topics_count = parts[1];
        let _len = BASE64.decode_slice(msg_topics_count.as_bytes(), &mut buf)?;
        let msg_topics_count = u32::from_le_bytes(buf);
        if msg_topics_count != topics_count as u32 {
            error!("Invalid event: topics count mismatch");
            return Err(Error::InvalidEvent);
        }
        let mut buf = [0u8; 32];
        let _len = BASE64.decode_slice_unchecked(parts[0].as_bytes(), &mut buf)?;
        let address = Address::from(*array_ref![buf, 0, 20]);
        let topic_list = parts[2..2 + (msg_topics_count as usize)]
            .iter()
            .map(|part| {
                let mut buf = [0u8; 32];
                let _ = BASE64.decode_slice_unchecked(part, &mut buf)?;
                Ok::<_, Error>(U256::from_le_bytes(buf))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = BASE64.decode(parts[2 + topics_count as usize])?;
        Ok(NeonLogTxEvent {
            event_type: EventKind::Log,
            is_hidden: false,
            is_reverted: false,
            address: Some(address),
            topic_list,
            data,

            tx_log_idx: 0,
            blk_log_idx: 0,
            level: 0,
            order: 0,
        })
    }

    // Unpacks base64-encoded event data:
    // ENTER CALL <20 bytes contract address>
    // ENTER CALLCODE <20 bytes contract address>
    // ENTER STATICCALL <20 bytes contract address>
    // ENTER DELEGATECALL <20 bytes contract address>
    // ENTER CREATE <20 bytes contract address>
    // ENTER CREATE2 <20 bytes contract address>
    fn decode_tx_enter(input: &str) -> Result<NeonLogTxEvent, Error> {
        use common::evm_loader::types::Address;

        let parts = input.split(' ').collect::<Vec<_>>();
        if parts.len() != 2 {
            error!("Invalid ENTER event: must contain 2 elements");
            return Err(Error::InvalidEvent);
        }

        let mut buf = [0u8; 32]; // should be more than enough
        let len = BASE64.decode_slice(parts[0].as_bytes(), &mut buf)?;
        let event_kind = match &buf[0..len] {
            b"CALL" => EventKind::EnterCall,
            b"CALLCODE" => EventKind::EnterCallCode,
            b"STATICCALL" => EventKind::EnterStaticCall,
            b"DELEGATECALL" => EventKind::EnterDelegateCall,
            b"CREATE" => EventKind::EnterCreate,
            b"CREATE2" => EventKind::EnterCreate2,
            e => {
                error!(kind = ?e, "Invalid ENTER event: unknown event kind");
                return Err(Error::InvalidEvent);
            }
        };
        let mut buf = [0u8; 20];
        let len = BASE64.decode_slice_unchecked(parts[1].as_bytes(), &mut buf)?;
        if len != 20 {
            error!("Invalid ENTER event: address must be 20 bytes");
            return Err(Error::InvalidEvent);
        }
        let address = Address::from(buf);

        Ok(NeonLogTxEvent {
            event_type: event_kind,
            is_hidden: true,
            is_reverted: false,
            address: Some(address),
            topic_list: Vec::new(),
            data: Vec::new(),

            tx_log_idx: 0,
            blk_log_idx: 0,
            level: 0,
            order: 0,
        })
    }

    /// Unpacks base64-encoded event data:
    /// EXIT STOP
    /// EXIT RETURN
    /// EXIT SELFDESTRUCT
    /// EXIT REVERT data
    fn decode_tx_exit(input: &str) -> Result<NeonLogTxEvent, Error> {
        let split = input.split(' ').collect::<Vec<_>>();
        let event = split[0];

        let mut buf = [0u8; 32];
        let len = BASE64.decode_slice(event.as_bytes(), &mut buf)?;
        let mut data = Vec::new();

        let event_kind = match &buf[..len] {
            b"STOP" => EventKind::ExitStop,
            b"RETURN" => EventKind::ExitReturn,
            b"SELFDESTRUCT" => EventKind::ExitSelfDestruct,
            b"SENDALL" => EventKind::ExitSendAll,
            b"REVERT" => {
                data = BASE64.decode(split[1])?;
                EventKind::ExitRevert
            }
            e => {
                error!(kind = ?e, "Invalid EXIT event: unknown event kind");
                return Err(Error::InvalidEvent);
            }
        };
        Ok(NeonLogTxEvent {
            event_type: event_kind,
            is_hidden: true,
            is_reverted: false,
            address: None,
            topic_list: Vec::new(),
            data,

            tx_log_idx: 0,
            blk_log_idx: 0,
            level: 0,
            order: 0,
        })
    }

    /// Unpacks gas
    /// GAS <32 bytes le iteration gas> <32 bytes le total gas>
    fn decode_tx_gas(input: &str) -> Result<NeonLogTxIx, Error> {
        let parts = input.split(' ').collect::<Vec<_>>();
        if parts.len() != 2 {
            error!("Invalid GAS: must contain 2 elements");
            return Err(Error::InvalidEvent);
        }
        let mut buf = [0u8; 32];
        let _len = BASE64.decode_slice_unchecked(parts[0].as_bytes(), &mut buf)?;
        let gas_used = U256::from_le_bytes(buf);

        let _len = BASE64.decode_slice_unchecked(parts[1].as_bytes(), &mut buf)?;
        let total_gas_used = U256::from_le_bytes(buf);

        tracing::debug!("gas decoded {} {}", gas_used, total_gas_used);

        Ok(NeonLogTxIx {
            gas_used,
            total_gas_used,
        })
    }

    /// Unpacks base64-encoded return data
    fn decode_tx_return(ix: Option<&NeonLogTxIx>, input: &str) -> Result<NeonLogTxReturn, Error> {
        if input.is_empty() {
            error!("Invalid RETURN");
            return Err(Error::InvalidEvent);
        }
        let mut buf = [0u8; 8];
        let len = BASE64.decode_slice(input, &mut buf)?;
        assert_eq!(len, 1); // TODO: unclear how long should it be
        let exit_status = buf[0];
        let exit_status = u8::from(exit_status < 0xd0);

        let gas_used = ix.map(|ix| ix.total_gas_used).ok_or(Error::InvalidEvent)?;

        Ok(NeonLogTxReturn {
            gas_used,
            status: exit_status,
            is_canceled: false,
        })
    }

    // TODO
    fn decode_miner(_input: &str) {
        warn!("miner opcode not implemented yet");
    }

    // Unpacks number of evm steps:
    // STEP <bytes-le - the number of iteration EVM steps> <bytes-le - the total number of EVM steps>
    fn decode_steps(input: &str) -> Result<NeonLogTxSteps, Error> {
        let words = input.split(' ').collect::<Vec<_>>();
        if words.len() < 2 {
            error!("Invalid STEPS");
            return Err(Error::InvalidEvent);
        }
        let mut buf = [0u8; 32];
        let len = BASE64.decode_slice(words[0].as_bytes(), &mut buf)?;
        assert_eq!(len, 8);
        let steps = u64::from_le_bytes(*array_ref![buf, 0, 8]);

        let len = BASE64.decode_slice(words[1].as_bytes(), &mut buf)?;
        assert_eq!(len, 8);
        let total_steps = u64::from_le_bytes(*array_ref![buf, 0, 8]);
        tracing::info!("steps: {} {}", steps, total_steps);

        Ok(NeonLogTxSteps { steps, total_steps })
    }
}

#[derive(Debug, Error)]
#[error("Invalid mnemonic")]
pub struct BadMnemonic;

impl FromStr for Mnemonic {
    type Err = BadMnemonic;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HASH" => Ok(Self::Hash),
            "MINER" => Ok(Self::Miner),
            "RETURN" => Ok(Self::Return),
            "ENTER" => Ok(Self::Enter),
            "EXIT" => Ok(Self::Exit),
            "GAS" => Ok(Self::Gas),
            "STEPS" => Ok(Self::Steps),
            "RESET" => Ok(Self::Reset),
            _ if s.starts_with("LOG") => {
                let n = s.strip_prefix("LOG").unwrap();
                let n = n.parse().map_err(|_| BadMnemonic)?;
                Ok(Self::Log(n))
            }
            s => {
                tracing::warn!("Invalid mnemonic {}", s);
                Err(BadMnemonic)
            }
        }
    }
}

fn parse_mnemonic(line: &str) -> Result<(Mnemonic, Option<&str>), Error> {
    tracing::debug!(?line, "parsing mnemonic");

    let parts: Vec<_> = line.splitn(2, ' ').collect();
    let mnemonic = parts.first().ok_or(Error::InvalidLog)?;
    let mnemonic = BASE64.decode(mnemonic.as_bytes())?;
    let mnemonic = std::str::from_utf8(&mnemonic)?.parse()?;
    let rest = parts.get(1).copied();
    Ok((mnemonic, rest))
}

pub fn parse(
    lines: impl IntoIterator<Item = impl AsRef<str>>,
    neon_pubkey: Pubkey,
) -> Result<NeonLogInfo, Error> {
    tracing::debug!("parsing log");
    let neon_pubkey = neon_pubkey.to_string();

    let mut is_truncated = false;
    let mut is_already_finalized = false;

    let mut neon_tx_sig = None;
    let mut neon_tx_ix = None;
    let mut neon_tx_return = None;
    let mut neon_tx_steps = None;
    let mut event_list = Vec::new();
    let mut inside_neon = false;
    let mut invoke_depth = 0;
    let mut neon_depth = 0;

    for line in lines {
        let line = line.as_ref();
        match line {
            ALREADY_FINALIZED_MSG => {
                is_already_finalized = true;
                continue;
            }
            LOG_TRUNCATED_MSG => {
                is_truncated = true;
                continue;
            }
            _ => {}
        }

        // deal with programs called from inside the neon program
        {
            let words: Vec<_> = line.splitn(4, ' ').collect(); // TODO: smallvec
            if words.len() >= 3 {
                if words[0] == "Program" && words[2] == "invoke" {
                    tracing::debug!("invoke {}", words[1]);
                    invoke_depth += 1;
                    if words[1] == neon_pubkey {
                        inside_neon = true;
                        neon_depth = invoke_depth;
                    }
                }

                if words[0] == "Program" && words[2] == "success" {
                    tracing::debug!("invoked {} return", words[1]);
                    invoke_depth -= 1;
                }
            }
        }

        if inside_neon && neon_depth == invoke_depth && line.starts_with("Program data: ") {
            let line = line.strip_prefix("Program data: ").unwrap();
            let (mnemonic, rest) = parse_mnemonic(line)?;
            tracing::debug!(?mnemonic, "parsing mnemonic");
            let rest = mnemonic
                .requires_arg()
                .then_some(rest)
                .flatten()
                .ok_or(Error::InvalidLog)?;
            match mnemonic {
                Mnemonic::Reset => {
                    tracing::warn!("reset mnemonic not supported");
                }
                Mnemonic::Hash => {
                    if neon_tx_sig.is_some() {
                        warn!("HASH already exists");
                        continue;
                    }
                    neon_tx_sig = Some(Mnemonic::decode_hash(rest)?);
                }
                Mnemonic::Return => {
                    if neon_tx_return.is_some() {
                        warn!("RETURN already exists");
                        continue;
                    }
                    neon_tx_return = Some(Mnemonic::decode_tx_return(neon_tx_ix.as_ref(), rest)?);
                }
                Mnemonic::Log(n) => {
                    let event = Mnemonic::decode_tx_event(n, rest)?;
                    event_list.push(event);
                }
                Mnemonic::Enter => {
                    let event = Mnemonic::decode_tx_enter(rest)?;
                    event_list.push(event);
                }
                Mnemonic::Exit => {
                    let event = Mnemonic::decode_tx_exit(rest)?;
                    event_list.push(event);
                }
                Mnemonic::Gas => {
                    if neon_tx_ix.is_some() {
                        warn!("GAS already exists");
                        continue;
                    }
                    neon_tx_ix = Some(Mnemonic::decode_tx_gas(rest)?);
                }
                Mnemonic::Miner => {
                    // TODO
                    Mnemonic::decode_miner(rest);
                }
                Mnemonic::Steps => {
                    neon_tx_steps = Some(Mnemonic::decode_steps(rest)?);
                }
            }
        }
    }

    // complete event list
    let mut current_level: u64 = 0;
    let mut current_order = 0;
    let mut addr_stack = Vec::new();
    let mut event_level;
    let mut event_addr;

    for event in &mut event_list {
        if event.event_type.is_start() {
            current_level += 1;
            event_level = current_level;
            event_addr = event.address;
            addr_stack.push(event.address.unwrap());
        } else if event.event_type.is_exit() {
            event_level = current_level;
            //current_level -= 1;
            current_level = current_level.saturating_sub(1); // TODO
            event_addr = addr_stack.pop();
        } else {
            event_level = current_level;
            if addr_stack.is_empty() {
                event_addr = None;
            } else {
                event_addr = Some(*addr_stack.last().unwrap());
            }
        }
        current_order += 1;
        event.level = event_level;
        event.order = current_order;
        event.address = event.address.or(event_addr);
    }

    let mut revert_level = None;
    let is_failed = neon_tx_return.as_ref().is_some_and(|ret| ret.status == 0);

    for event in event_list.iter_mut().rev() {
        if event.event_type.is_start() && revert_level == Some(event.level) {
            revert_level = None;
        }
        if event.event_type == EventKind::ExitRevert && revert_level.is_none() {
            revert_level = Some(event.level);
        }
        let is_reverted = revert_level.is_some() || is_failed;
        event.is_reverted = is_reverted;
        event.is_hidden |= is_reverted;
    }

    let log_info = NeonLogInfo {
        sig: neon_tx_sig,
        ix: neon_tx_ix,
        ret: neon_tx_return,
        steps: neon_tx_steps,
        event_list,
        is_truncated,
        is_already_finalized,
    };
    Ok(log_info)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use test_log::test;

    #[derive(Debug, Deserialize)]
    struct Meta {
        #[serde(rename = "logMessages")]
        log_messages: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct DumbTx {
        meta: Meta,
    }

    #[test]
    fn parse_logs() {
        let neon_pubkey = "eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU"
            .parse()
            .unwrap();
        let path = "tests/data/";
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                println!("Parsing: {:?}", entry.path());
                let buf = std::fs::read(entry.path()).unwrap();
                let tx: DumbTx = serde_json::from_slice(&buf).unwrap();
                let log_info = super::parse(tx.meta.log_messages, neon_pubkey).unwrap();
                println!("Parsed: {:?} got {:#?}", entry.path(), log_info);
            }
        }
    }
}
