-- Add inscription_id field to token_info table with UNIQUE constraint
ALTER TABLE token_info
ADD COLUMN inscription_id VARCHAR REFERENCES addresses(id);

-- Add indexes to the new fields
CREATE INDEX idx_token_info_inscription_id ON token_info (inscription_id);