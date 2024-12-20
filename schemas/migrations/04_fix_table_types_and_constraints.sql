ALTER TABLE solana_alt_transactions
ALTER COLUMN sol_sig TYPE BYTEA USING sol_sig::BYTEA,
ADD CONSTRAINT sol_sig_length_check CHECK (octet_length(sol_sig) = 64);

ALTER TABLE solana_transaction_costs
ALTER COLUMN sol_sig TYPE BYTEA USING sol_sig::BYTEA,
ALTER COLUMN operator TYPE BYTEA USING operator::BYTEA,
ADD CONSTRAINT sol_sig_length_check CHECK (octet_length(sol_sig) = 64),
ADD CONSTRAINT unique_sol_sig UNIQUE (sol_sig);

ALTER TABLE solana_neon_transactions
ALTER COLUMN sol_sig TYPE BYTEA USING sol_sig::BYTEA,
ALTER COLUMN neon_sig TYPE BYTEA USING neon_sig::BYTEA,
ALTER COLUMN neon_step_cnt TYPE BIGINT USING neon_step_cnt::BIGINT,
ADD CONSTRAINT sol_sig_length_check CHECK (octet_length(sol_sig) = 64),
ADD CONSTRAINT neon_sig_length_check CHECK (octet_length(sol_sig) = 64);

ALTER TABLE neon_transaction_logs
ALTER COLUMN inner_idx DROP NOT NULL;
