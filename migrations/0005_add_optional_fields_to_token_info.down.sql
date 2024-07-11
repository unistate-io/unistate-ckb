-- Remove optional fields from token_info table
ALTER TABLE token_info
DROP COLUMN IF EXISTS udt_hash,
DROP COLUMN IF EXISTS expected_supply,
DROP COLUMN IF EXISTS mint_limit,
DROP COLUMN IF EXISTS mint_status;
