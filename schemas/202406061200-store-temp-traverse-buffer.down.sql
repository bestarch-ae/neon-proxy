DROP INDEX idx_solana_transaction_signatures_is_processed;
ALTER TABLE solana_transaction_signatures
    DROP COLUMN is_processed,
    ALTER COLUMN block_slot DROP NOT NULL,
    ALTER COLUMN tx_idx DROP NOT NULL,
    ALTER COLUMN signature DROP NOT NULL;
