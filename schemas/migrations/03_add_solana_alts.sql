CREATE TABLE IF NOT EXISTS solana_alts (
    address BYTEA CHECK (octet_length(address) = 32) PRIMARY KEY
);
