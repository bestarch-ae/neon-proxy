use std::mem;

use anyhow::{bail, ensure, Context};
use bytemuck::pod_read_unaligned;
use common::ethnum::U256;
use reth_primitives::B256;

use common::evm_loader::account::TAG_STATE;
use common::evm_loader::{self};
use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::pubkey::Pubkey;

// #[repr(C, packed)]
// pub struct Header {
//     pub executor_state_offset: usize,
//     pub evm_machine_offset: usize,
//     pub data_offset: usize,
// }

// The following two consts taken from [`evm_loader::allocator`]
/// See [`solana_program::entrypoint::deserialize`] for more details.
const FIRST_ACCOUNT_DATA_OFFSET: usize =
    /* number of accounts */
    mem::size_of::<u64>() +
    /* duplication marker */ mem::size_of::<u8>() +
    /* is signer? */ mem::size_of::<u8>() +
    /* is writable? */ mem::size_of::<u8>() +
    /* is executable? */ mem::size_of::<u8>() +
    /* original_data_len */ mem::size_of::<u32>() +
    /* key */ mem::size_of::<Pubkey>() +
    /* owner */ mem::size_of::<Pubkey>() +
    /* lamports */ mem::size_of::<u64>() +
    /* factual_data_len */ mem::size_of::<u64>();
/// See <https://solana.com/docs/programs/faq#memory-map> for more details.
const PROGRAM_DATA_INPUT_PARAMETERS_OFFSET: usize = 0x0004_0000_0000_usize;

#[derive(Debug)]
pub struct StateData {
    pub tx_hash: B256,
    pub chain_id: Option<u64>,
    pub accounts: Vec<Pubkey>,
}

pub fn parse_state(program_id: &Pubkey, account: &AccountInfo<'_>) -> anyhow::Result<StateData> {
    evm_loader::account::validate_tag(program_id, account, TAG_STATE)?;

    const HEADER_START: usize = evm_loader::account::ACCOUNT_PREFIX_LEN;
    const DATA_OFFSET_OFFSET: usize = HEADER_START
        // NEON uses `usize` for all three fields which is unfortunate
        // but we have to rely on the fact that `u64` is `usize` in SBF
        + mem::size_of::<u64>() // executor_state_offset 
        + mem::size_of::<u64>(); // evm_machine_offset;

    let data = account.data.borrow();
    let data_offset: u64 =
        pod_read_unaligned(&data[DATA_OFFSET_OFFSET..(DATA_OFFSET_OFFSET + mem::size_of::<u64>())]);

    // Structs layout (selected fields taken from program logs):
    // | Field name           | Type                                | SBF Size | SBF Offset |
    // | -------------------- | ----------------------------------- | -------- | ---------- |
    // | -------------------- | -----------`Data` layout----------- | -------- | ---------- |
    // | `owner`              | `Pubkey`                            | 32       | 0          |
    // | `transaction`        | `Transaction`                       | 376      | 32         |
    // | `origin`             | `Address`                           | 20       | 408        |
    // | `revisions`          | `Vector<(Pubkey, AccountRevision)>` | 24       | 432        |
    // | `touched_accounts`   | `Vector<Pubkey, u64>`               | 24       | 456        |
    // | `gas_used`           | `U256`                              | 32       | 480        |
    // | `steps_executed`     | `u64`                               | 8        | 512        |
    // | -------------------- | -`(Pubkey,AccountRevision)` layout- | -------- | ---------- |
    // | 0                    | `Pubkey`                            | 32       | 1 (align)  |
    // | 1                    | `AccountRevision`                   | 36       | 4 (align)  |
    // | total                |                                     | 68       | 4 (align)  |
    // | -------------------- | -------`Transaction` layout-------- | -------- | ---------- |
    // | `transaction`        | `TransactionPayload`                | 304      | 0          |
    // | `hash`               | `[u8; 32]`                          | 32       | 312        |
    // | -------------------- | ----`TransactionPayload` layout---- | -------- | ---------- |
    // | <tag>                | `u8`                                | 1        | 0          |
    // | <data>               |                                     |          | 8          |
    // | -------------------- | ------Payload fields (Legacy)------ | -------- | ---------- |
    // | `chain_id`           | `Option<U256>`                      | 40       | 248        |
    // | -------------------- | ----Payload fields (AccessList)---- | -------- | ---------- |
    // | `chain_id`           | `U256`                              | 32       | 216        |

    const REVISIONS_OFFSET: usize = 432;
    const TRX_OFFSET: usize = 32;

    const PAYLOAD_TAG_OFFSET: usize = TRX_OFFSET;
    const PAYLOAD_DATA_OFFSET: usize = TRX_OFFSET + 8;

    const LEGACY_CHAIN_ID_OFFSET: usize = PAYLOAD_DATA_OFFSET + 248;
    const LEGACY_CHAIN_ID_END: usize = LEGACY_CHAIN_ID_OFFSET + 40;
    const ACCESS_LIST_CHAIN_ID_OFFSET: usize = PAYLOAD_DATA_OFFSET + 216;
    const ACCESS_LIST_CHAIN_ID_END: usize = ACCESS_LIST_CHAIN_ID_OFFSET + 32;

    const HASH_OFFSET: usize = TRX_OFFSET + 312;
    const HASH_LEN: usize = 32;

    let offset = |offset| data_offset as usize + offset;

    let mut hash = [0; HASH_LEN];
    hash.copy_from_slice(&data[offset(HASH_OFFSET)..offset(HASH_OFFSET + HASH_LEN)]);
    let tx_hash = B256::from(hash);

    let tag = data[offset(PAYLOAD_TAG_OFFSET)];
    let chain_id = match tag {
        // Legacy
        0 => (data[offset(LEGACY_CHAIN_ID_OFFSET)] == 1)
            .then(|| {
                pod_read_unaligned(
                    &data[offset(LEGACY_CHAIN_ID_OFFSET + 8)..offset(LEGACY_CHAIN_ID_END)],
                )
            })
            .map(U256::from_le_bytes),
        // AccessList
        1 => Some(U256::from_le_bytes(pod_read_unaligned(
            &data[offset(ACCESS_LIST_CHAIN_ID_OFFSET + 8)..offset(ACCESS_LIST_CHAIN_ID_END)],
        ))),
        n => bail!("invalid transaction payload tag: {}", n),
    }
    .map(U256::as_u64);

    const STATE_ACCOUNT_DATA_OFFSET: isize =
        -((FIRST_ACCOUNT_DATA_OFFSET + PROGRAM_DATA_INPUT_PARAMETERS_OFFSET) as isize);

    let data = StateData {
        tx_hash,
        chain_id,
        accounts: read_vec(&data, offset(REVISIONS_OFFSET), STATE_ACCOUNT_DATA_OFFSET)?,
    };

    Ok(data)
}

fn read_vec(data: &[u8], start: usize, offset: isize) -> anyhow::Result<Vec<Pubkey>> {
    // 1. The Vector's memory layout consists of three usizes: ptr to the buffer, capacity and length.
    // 2. There's no alignment between the fields, the Vector occupies exactly the 3*sizeof<usize> bytes.
    // 3. The order of those fields in the memory is unspecified (no repr is set on the vec struct).
    // => The len is the smallest of those three usizes, because it can't realistically be more than the buffer
    // ptr value and it's no more than capacity.
    // => The buffer ptr is the biggest among them.
    ensure!(data.len() >= start + 3 * mem::size_of::<u64>());
    let read_u64 = |at| pod_read_unaligned(&data[at..(at + mem::size_of::<u64>())]);
    let (fst, snd, trd): (u64, u64, u64) = (
        read_u64(start),
        read_u64(start + mem::size_of::<u64>()),
        read_u64(start + 2 * mem::size_of::<u64>()),
    );
    let vec_len = fst.min(snd).min(trd) as usize;
    let buf_ptr_unadjusted = fst.max(snd).max(trd) as usize;
    let buf_ptr_adjusted = match offset.is_negative() {
        true => buf_ptr_unadjusted
            .checked_sub(offset.unsigned_abs())
            .context("underflow during state vec parsing: {buf_ptr_unadjusted} + {offset}")?,
        false => buf_ptr_unadjusted
            .checked_add(offset.unsigned_abs())
            .context("overflow during state vec parsing: {buf_ptr_unadjusted} + {offset}")?,
    };

    const TUPLE_SIZE: usize = 68;
    ensure!(data.len() >= buf_ptr_adjusted + TUPLE_SIZE * vec_len);

    let mut output = Vec::with_capacity(vec_len);
    let read_pubkey = |at| pod_read_unaligned(&data[at..(at + mem::size_of::<Pubkey>())]);
    for idx in 0..vec_len {
        let key = Pubkey::new_from_array(read_pubkey(buf_ptr_adjusted + idx * TUPLE_SIZE));
        output.push(key);
    }

    Ok(output)
}
