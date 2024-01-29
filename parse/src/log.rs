use std::str::FromStr;
use std::str::Utf8Error;

use base64::prelude::{Engine, BASE64_STANDARD as BASE64};
use thiserror::Error;
use tracing::{error, warn};

use common::ethnum::U256;
use common::types::{EventKind, EventLog as NeonLogTxEvent};

type NeonLogSignature = [u8; 32];

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
}

// Copy-paste from evm_log_decoder.py
#[derive(Debug)]
pub struct NeonLogInfo {
    pub sig: Option<NeonLogSignature>,
    pub ix: Option<NeonLogTxIx>,
    pub ret: Option<NeonLogTxReturn>,
    pub event_list: Vec<NeonLogTxEvent>,
    pub is_truncated: bool,
    pub is_already_finalized: bool,
}

#[derive(Debug)]
pub struct NeonLogTxReturn {
    pub gas_used: U256,
    pub status: u8,
    pub is_canceled: bool,
}

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
    Hash,
    Return,
    Log(u8),
    Enter,
    Exit,
    Gas,
}

impl Mnemonic {
    fn decode_hash(s: &str) -> Result<[u8; 32], Error> {
        // TODO: figure it out, seems weird.
        // With 32 byte buf it reports OutputSliceTooSmall
        let mut buf = [0u8; 33];
        let len = BASE64.decode_slice(s.as_bytes(), &mut buf)?;
        if len != 32 {
            return Err(Error::InvalidHash);
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&buf[0..32]);
        Ok(hash)
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
        let mut buf = [0u8; 20];
        let _len = BASE64.decode_slice_unchecked(parts[0].as_bytes(), &mut buf)?;
        let address = Address::from(buf);
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
            address: Some(address),
            topic_list,
            data,

            // TODO: unclear what this is
            log_idx: 0,
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
            address: Some(address),
            topic_list: Vec::new(),
            data: Vec::new(),

            // TODO: unclear what this is
            log_idx: 0,
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
        let mut buf = [0u8; 32];
        let len = BASE64.decode_slice(input.as_bytes(), &mut buf)?;
        let mut data = Vec::new();

        let event_kind = match &buf[..len] {
            b"STOP" => EventKind::ExitStop,
            b"RETURN" => EventKind::ExitReturn,
            b"SELFDESTRUCT" => EventKind::ExitSelfDestruct,
            e if e.starts_with(b"REVERT") => {
                for (i, part) in e.split(|c| *c == b' ').enumerate() {
                    if i == 0 && part != b"REVERT" {
                        error!(kind = ?part, "Invalid EXIT event: unknown event kind");
                        return Err(Error::InvalidEvent);
                    }
                    if i == 1 {
                        data = BASE64.decode(part)?;
                    }
                }
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
            address: None,
            topic_list: Vec::new(),
            data,

            // TODO: unclear what this is
            log_idx: 0,
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
        let gas_used = ix.map(|ix| ix.total_gas_used).ok_or(Error::InvalidEvent)?;

        Ok(NeonLogTxReturn {
            gas_used,
            status: exit_status,
            is_canceled: false,
        })
    }
}

#[derive(Debug, Error)]
#[error("Invalid mnemonic")]
pub struct BadMnemonic;

impl FromStr for Mnemonic {
    type Err = BadMnemonic;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HASH" => Ok(Mnemonic::Hash),
            "RETURN" => Ok(Mnemonic::Return),
            "ENTER" => Ok(Mnemonic::Enter),
            "EXIT" => Ok(Mnemonic::Exit),
            "GAS" => Ok(Mnemonic::Gas),
            _ if s.starts_with("LOG") => {
                let n = s.strip_prefix("LOG").unwrap();
                let n = n.parse().map_err(|_| BadMnemonic)?;
                Ok(Mnemonic::Log(n))
            }
            _ => Err(BadMnemonic),
        }
    }
}

fn parse_mnemonic(line: &str) -> Result<(Mnemonic, &str), Error> {
    let (mnemonic, rest) = line.split_once(' ').unwrap();
    let mnemonic = BASE64.decode(mnemonic.as_bytes())?;
    let mnemonic = std::str::from_utf8(&mnemonic)?.parse()?;
    Ok((mnemonic, rest))
}

pub fn parse(lines: impl IntoIterator<Item = impl AsRef<str>>) -> Result<NeonLogInfo, Error> {
    let mut is_truncated = false;
    let mut is_already_finalized = false;

    let mut neon_tx_sig = None;
    let mut neon_tx_ix = None;
    let mut neon_tx_return = None;
    let mut event_list = Vec::new();

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
        if line.starts_with("Program data: ") {
            let line = line.strip_prefix("Program data: ").unwrap();
            let (mnemonic, rest) = parse_mnemonic(line)?;
            match mnemonic {
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
            }
        }
    }
    let log_info = NeonLogInfo {
        sig: neon_tx_sig,
        ix: neon_tx_ix,
        ret: neon_tx_return,
        event_list,
        is_truncated,
        is_already_finalized,
    };
    Ok(log_info)
}
