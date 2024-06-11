ALTER TABLE solana_transaction_signatures
    ADD COLUMN is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    ALTER COLUMN block_slot SET NOT NULL,
    ALTER COLUMN tx_idx SET NOT NULL,
    ALTER COLUMN signature SET NOT NULL;
UPDATE solana_transaction_signatures SET is_processed = TRUE WHERE TRUE;
CREATE INDEX idx_solana_transaction_signatures_is_processed ON solana_transaction_signatures(is_processed);
