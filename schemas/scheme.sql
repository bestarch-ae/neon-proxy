    ---- Prepare stage # TODO: remove
    CREATE OR REPLACE FUNCTION does_table_have_column(t_name TEXT, c_name TEXT)
        RETURNS BOOLEAN
        LANGUAGE plpgsql
    AS
    $$
    DECLARE
        column_count INT;
    BEGIN
        SELECT COUNT(t.column_name)
          INTO column_count
          FROM information_schema.columns AS t
         WHERE t.table_name=t_name AND t.column_name=c_name;
        RETURN column_count > 0;
    END;
    $$;

    CREATE OR REPLACE FUNCTION does_table_exist(t_name TEXT)
        RETURNS BOOLEAN
        LANGUAGE plpgsql
    AS
    $$
    DECLARE
        column_count INT;
    BEGIN
        SELECT COUNT(t.column_name)
          INTO column_count
          FROM information_schema.columns AS t
         WHERE t.table_name=t_name;
        RETURN column_count > 0;
    END;
    $$;

    CREATE DOMAIN U256 AS NUMERIC
    CHECK (VALUE >= 0 AND VALUE < 2^256)
    CHECK (SCALE(VALUE) = 0);

    CREATE DOMAIN Address AS BYTEA
    CHECK (octet_length(VALUE) = 20 OR VALUE IS NULL);

    CREATE DOMAIN SolanaBlockHash AS BYTEA
    CHECK (octet_length(VALUE) = 32 OR VALUE IS NULL);

    --- Initialize stage

    CREATE TABLE IF NOT EXISTS constants (
        key TEXT UNIQUE,
        value BYTEA
    );

    CREATE TABLE IF NOT EXISTS gas_less_accounts (
        address TEXT,
        contract TEXT,
        nonce BIGINT,
        block_slot BIGINT,
        neon_sig TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_gas_less_accounts ON gas_less_accounts(address, contract, nonce);

    CREATE TABLE IF NOT EXISTS gas_less_usages(
        address TEXT,
        block_slot BIGINT,
        neon_sig TEXT,
        nonce BIGINT,
        to_addr TEXT,
        neon_total_gas_usage BIGINT,
        operator TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_gas_less_usages ON gas_less_usages(address);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_gas_less_usages_neon_sig ON gas_less_usages(neon_sig);

    CREATE TABLE IF NOT EXISTS solana_blocks (
        block_slot BIGINT PRIMARY KEY,
        block_hash SolanaBlockHash NOT NULL,
        block_time BIGINT,
        parent_block_slot BIGINT NOT NULL,
        parent_block_hash SolanaBlockHash NOT NULL,
        is_finalized BOOL NOT NULL,
        is_active BOOL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_solana_blocks_hash ON solana_blocks(block_hash);
    CREATE INDEX IF NOT EXISTS idx_solana_blocks_slot_active ON solana_blocks(block_slot, is_active);

    CREATE TABLE IF NOT EXISTS neon_transaction_logs (
        address Address,
        block_slot BIGINT NOT NULL, -- TODO: Reference solana_blocks. Need `solana_api::traverse` block/tx order change to work.

        tx_hash BYTEA NOT NULL CHECK (octet_length(tx_hash) = 32),
        tx_idx INT NOT NULL,
        tx_log_idx INT NOT NULL,
        log_idx INT NOT NULL,

        event_level INT NOT NULL,
        event_order INT NOT NULL,

        sol_sig BYTEA CHECK (octet_length(sol_sig) = 64),
        idx INT NOT NULL,
        inner_idx INT NOT NULL,

        log_topic1 BYTEA CHECK (octet_length(log_topic1) = 32 OR log_topic1 IS NULL),
        log_topic2 BYTEA CHECK (octet_length(log_topic1) = 32 OR log_topic2 IS NULL),
        log_topic3 BYTEA CHECK (octet_length(log_topic1) = 32 OR log_topic3 IS NULL),
        log_topic4 BYTEA CHECK (octet_length(log_topic1) = 32 OR log_topic4 IS NULL),
        log_topic_cnt INT NOT NULL,

        log_data TEXT NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_neon_transaction_logs_block_tx_log ON neon_transaction_logs(block_slot, tx_hash, tx_log_idx);
    -- TODO: CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_block_tx_idx_log ON neon_transaction_logs(block_slot, tx_idx, tx_log_idx);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_address ON neon_transaction_logs(address);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_block_slot ON neon_transaction_logs(block_slot);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_topic1 ON neon_transaction_logs(log_topic1);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_topic2 ON neon_transaction_logs(log_topic2);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_topic3 ON neon_transaction_logs(log_topic3);
    CREATE INDEX IF NOT EXISTS idx_neon_transaction_logs_topic4 ON neon_transaction_logs(log_topic4);


    CREATE TABLE IF NOT EXISTS solana_neon_transactions (
        sol_sig TEXT,
        block_slot BIGINT,
        idx INT,
        inner_idx INT,
        ix_code INT,
        is_success BOOLEAN,

        neon_sig TEXT,
        neon_step_cnt INT,
        neon_gas_used BIGINT,
        neon_total_gas_used BIGINT,

        max_heap_size INT,
        used_heap_size INT,

        max_bpf_cycle_cnt INT,
        used_bpf_cycle_cnt INT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_neon_transactions_neon_sol_idx_inner ON solana_neon_transactions(sol_sig, block_slot, idx, inner_idx);
    CREATE INDEX IF NOT EXISTS idx_solana_neon_transactions_neon_sig ON solana_neon_transactions(neon_sig, block_slot);
    CREATE INDEX IF NOT EXISTS idx_solana_neon_transactions_neon_block ON solana_neon_transactions(block_slot);

    CREATE TABLE IF NOT EXISTS neon_transactions (
        neon_sig BYTEA NOT NULL CHECK (octet_length(neon_sig) = 32),
        tx_type INT NOT NULL,
        from_addr Address NOT NULL,

        sol_sig BYTEA CHECK (octet_length(sol_sig) = 64),
        sol_ix_idx INT NOT NULL,
        sol_ix_inner_idx INT,
        block_slot BIGINT NOT NULL,
        tx_idx INT NOT NULL,

        nonce BIGINT NOT NULL,
        gas_price U256 NOT NULL,
        gas_limit U256 NOT NULL,
        value U256 NOT NULL,
        gas_used U256 NOT NULL,
        sum_gas_used U256 NOT NULL,

        to_addr Address,
        contract Address,

        status SMALLINT NOT NULL,
        is_canceled BOOLEAN NOT NULL,
        is_completed BOOLEAN NOT NULL,

        v U256 NOT NULL,
        r U256 NOT NULL,
        s U256 NOT NULL,
        chain_id BIGINT, /* NULL for legacy transactions */

        calldata BYTEA NOT NULL,
        logs BYTEA NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_neon_transactions_sol_sig_block ON neon_transactions(sol_sig, block_slot);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_neon_transactions_neon_sig ON neon_transactions(neon_sig);
    CREATE INDEX IF NOT EXISTS idx_neon_transactions_neon_sig_block ON neon_transactions(neon_sig, block_slot);
    CREATE INDEX IF NOT EXISTS idx_neon_transactions_sender_nonce_block ON neon_transactions(from_addr, nonce, block_slot);
    CREATE INDEX IF NOT EXISTS idx_neon_transactions_block_slot_tx_idx ON neon_transactions(block_slot, tx_idx);

    CREATE TABLE IF NOT EXISTS solana_alt_transactions (
        sol_sig TEXT,
        block_slot BIGINT,
        idx INT,
        inner_idx INT DEFAULT -1,
        ix_code INT,
        alt_address TEXT,
        is_success BOOLEAN,

        neon_sig TEXT
    );
    ALTER TABLE solana_alt_transactions ADD COLUMN IF NOT EXISTS inner_idx INT DEFAULT -1;
    DROP INDEX IF EXISTS idx_solana_alt_transactions_sig_slot_idx;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_alt_transactions_sig_slot_idx_inner ON solana_alt_transactions(sol_sig, block_slot, idx, inner_idx);
    CREATE INDEX IF NOT EXISTS idx_solana_alt_transactions_neon_sig ON solana_alt_transactions(neon_sig, block_slot);
    CREATE INDEX IF NOT EXISTS idx_solana_alt_transactions_slot ON solana_alt_transactions(block_slot);

    CREATE TABLE IF NOT EXISTS solana_transaction_costs (
        sol_sig TEXT,
        block_slot BIGINT,

        operator TEXT,
        sol_spent BIGINT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_transaction_costs_sig ON solana_transaction_costs(sol_sig, block_slot);
    CREATE INDEX IF NOT EXISTS idx_solana_transaction_costs_slot ON solana_transaction_costs(block_slot);
    CREATE INDEX IF NOT EXISTS idx_solana_transaction_costs_operator ON solana_transaction_costs(operator, block_slot);

    CREATE TABLE IF NOT EXISTS solana_transaction_signatures (
        block_slot  BIGINT,
        tx_idx      INT,
        signature   TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_transaction_signatures_sig ON solana_transaction_signatures(block_slot, tx_idx);

    CREATE TABLE IF NOT EXISTS stuck_neon_holders (
        block_slot BIGINT,
        json_data_list TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stuck_neon_holders_block ON stuck_neon_holders(block_slot);

    CREATE TABLE IF NOT EXISTS stuck_neon_transactions (
        is_finalized BOOLEAN,
        block_slot BIGINT,
        json_data_list TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stuck_neon_transactions_block ON stuck_neon_transactions(is_finalized, block_slot);

    CREATE TABLE IF NOT EXISTS solana_alt_infos (
        block_slot BIGINT,
        json_data_list TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_alt_infos_block ON solana_alt_infos(block_slot);

    CREATE TABLE IF NOT EXISTS neon_holder_log (
        block_slot BIGINT NOT NULL,
        tx_idx INT NOT NULL,
        start_block_slot BIGINT,
        last_block_slot BIGINT,
        is_stuck BOOLEAN NOT NULL,
        neon_sig TEXT,
        pubkey TEXT NOT NULL,
        data_offset BIGINT,
        data BYTEA
    );
    CREATE INDEX IF NOT EXISTS pubkey_slot ON neon_holder_log(pubkey, block_slot, tx_idx);-- Add migration script here
