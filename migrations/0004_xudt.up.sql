-- Create xudt_cell table
CREATE TABLE xudt_cell (
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  lock_id VARCHAR NOT NULL REFERENCES addresses(id),
  type_id VARCHAR NOT NULL REFERENCES addresses(id),
  amount NUMERIC(39, 0) NOT NULL,
  -- ScriptVec ids
  xudt_args VARCHAR [],
  xudt_data VARCHAR [],
  xudt_data_lock BYTEA,
  xudt_owner_lock_script_hash BYTEA,
  is_consumed BOOLEAN NOT NULL DEFAULT FALSE, -- 新增布尔字段
  PRIMARY KEY (transaction_hash, transaction_index)
);

-- Create xudt_status_cell table
CREATE TABLE transaction_outputs_status (
  output_transaction_hash BYTEA NOT NULL,
  output_transaction_index INTEGER NOT NULL,
  consumed_input_transaction_hash BYTEA,
  consumed_input_transaction_index INTEGER,
  PRIMARY KEY (output_transaction_hash, output_transaction_index)
);
-- Add a trigger to update is_consumed field in xudt_cell when a record is inserted into transaction_outputs_status
CREATE OR REPLACE FUNCTION update_is_consumed()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE xudt_cell
  SET is_consumed = TRUE
  WHERE transaction_hash = NEW.output_transaction_hash
    AND transaction_index = NEW.output_transaction_index;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_is_consumed
AFTER INSERT ON transaction_outputs_status
FOR EACH ROW
EXECUTE FUNCTION update_is_consumed();

-- Create token_info table
CREATE TABLE token_info (
  type_id VARCHAR NOT NULL PRIMARY KEY REFERENCES addresses(id),
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  decimal SMALLINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  symbol VARCHAR(255) NOT NULL
);

-- Add indexes to xudt_cell fields
CREATE INDEX idx_xudt_cell_lock_id ON xudt_cell (lock_id);
CREATE INDEX idx_xudt_cell_type_id ON xudt_cell (type_id);
-- Add indexes to token_info fields
CREATE INDEX idx_token_info_type_id ON token_info (type_id);