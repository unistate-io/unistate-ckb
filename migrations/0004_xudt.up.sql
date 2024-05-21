-- Create enum for cell status
CREATE TYPE cell_status AS ENUM ('live', 'dead');

-- Create XudtCell table
CREATE TABLE XudtCell (
  transaction_hash BYTEA NOT NULL,
  transaction_index INTEGER NOT NULL,
  lock_id VARCHAR NOT NULL REFERENCES addresses(id),
  type_id VARCHAR NOT NULL REFERENCES addresses(id),
  status cell_status NOT NULL DEFAULT 'live',
  amount NUMERIC(39, 0) NOT NULL,
  -- ScriptVec ids
  xudt_args VARCHAR [],
  xudt_data VARCHAR [],
  xudt_data_lock BYTEA,
  xudt_owner_lock_script_hash BYTEA,
  PRIMARY KEY (transaction_hash, transaction_index)
);