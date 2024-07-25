-- Drop foreign key constraint from xudt_cell.inscription_id to token_info.inscription_id
ALTER TABLE xudt_cell
DROP CONSTRAINT fk_xudt_cell_inscription_id;

-- Drop inscription_id field from xudt_cell table
ALTER TABLE xudt_cell
DROP COLUMN inscription_id;

-- Drop inscription_id field from token_info table
ALTER TABLE token_info
DROP COLUMN inscription_id;

-- Drop indexes on the dropped fields
DROP INDEX idx_token_info_inscription_id;
DROP INDEX idx_xudt_cell_inscription_id;
