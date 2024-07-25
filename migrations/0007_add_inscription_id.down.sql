-- Remove indexes from the fields
DROP INDEX IF EXISTS idx_token_info_inscription_id;

-- Remove inscription_id field from token_info table
ALTER TABLE token_info
DROP COLUMN IF EXISTS inscription_id;