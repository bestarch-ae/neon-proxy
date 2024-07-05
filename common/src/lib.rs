pub mod types;

// ===== Reexports =====
pub use ethnum;
pub use evm_loader;
pub use neon_lib;
pub use solana_account_decoder;
pub use solana_sdk;
pub use solana_transaction_status;

pub mod neon_instruction {
    pub mod tag {
        pub const COLLECT_TREASURE: u8 = 0x1e;

        pub const HOLDER_CREATE: u8 = 0x24;
        pub const HOLDER_DELETE: u8 = 0x25;
        pub const HOLDER_WRITE: u8 = 0x26;
        pub const CREATE_MAIN_TREASURY: u8 = 0x29;

        pub const ACCOUNT_CREATE_BALANCE: u8 = 0x30;
        pub const DEPOSIT: u8 = 0x31;

        pub const TX_EXEC_FROM_DATA: u8 = 0x32;
        pub const TX_EXEC_FROM_ACCOUNT: u8 = 0x33;
        pub const TX_STEP_FROM_DATA: u8 = 0x34;
        pub const TX_STEP_FROM_ACCOUNT: u8 = 0x35;
        pub const TX_STEP_FROM_ACCOUNT_NO_CHAINID: u8 = 0x36;
        pub const CANCEL: u8 = 0x37;
        pub const TX_EXEC_FROM_DATA_SOLANA_CALL: u8 = 0x38;
        pub const TX_EXEC_FROM_ACCOUNT_SOLANA_CALL: u8 = 0x39;

        pub const OPERATOR_BALANCE_CREATE: u8 = 0x3a;
        pub const OPERATOR_BALANCE_DELETE: u8 = 0x3b;
        pub const OPERATOR_BALANCE_WITHDRAW: u8 = 0x3c;

        pub const DEPOSIT_DEPRECATED: u8 = 0x27;
        pub const TX_EXEC_FROM_DATA_DEPRECATED: u8 = 0x1f;
        pub const TX_EXEC_FROM_ACCOUNT_DEPRECATED: u8 = 0x2a;
        pub const TX_STEP_FROM_DATA_DEPRECATED: u8 = 0x20;
        pub const TX_STEP_FROM_ACCOUNT_DEPRECATED: u8 = 0x21;
        pub const TX_STEP_FROM_ACCOUNT_NO_CHAINID_DEPRECATED: u8 = 0x22;
    }
}
