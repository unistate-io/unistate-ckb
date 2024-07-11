-- Add optional fields to token_info table
ALTER TABLE token_info
ADD COLUMN udt_hash BYTEA,
ADD COLUMN expected_supply NUMERIC(39, 0),
ADD COLUMN mint_limit NUMERIC(39, 0),
ADD COLUMN mint_status SMALLINT;
