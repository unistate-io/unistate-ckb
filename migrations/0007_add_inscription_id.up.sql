-- Add inscription_id field to token_info table
ALTER TABLE token_info
ADD COLUMN inscription_id BYTEA;

-- Add inscription_id field to xudt_cell table
ALTER TABLE xudt_cell
ADD COLUMN inscription_id BYTEA;

-- Create a foreign key constraint from xudt_cell.inscription_id to token_info.inscription_id
ALTER TABLE xudt_cell
ADD CONSTRAINT fk_xudt_cell_inscription_id
FOREIGN KEY (inscription_id)
REFERENCES token_info(inscription_id);

-- Add indexes to the new fields
CREATE INDEX idx_token_info_inscription_id ON token_info (inscription_id);
CREATE INDEX idx_xudt_cell_inscription_id ON xudt_cell (inscription_id);
