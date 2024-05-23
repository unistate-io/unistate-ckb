-- Create xudt_cell table
CREATE TABLE xudt_cell (
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  lock_id VARCHAR NOT NULL,
  type_id VARCHAR NOT NULL,
  amount NUMERIC(39, 0) NOT NULL,
  -- ScriptVec ids
  xudt_args VARCHAR [],
  xudt_data VARCHAR [],
  xudt_data_lock BYTEA,
  xudt_owner_lock_script_hash BYTEA,
  PRIMARY KEY (transaction_hash, transaction_index)
);

-- Create xudt_status_cell table
CREATE TABLE xudt_status_cell (
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  input_transaction_hash BYTEA,
  input_transaction_index INTEGER,
  PRIMARY KEY (transaction_hash, transaction_index)
);

CREATE TABLE token_info (
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  type_id VARCHAR NOT NULL,
  decimal SMALLINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  symbol VARCHAR(255) NOT NULL,
  PRIMARY KEY (transaction_hash, transaction_index)
);