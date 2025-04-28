-- Add up migration script here
-- Refactored Schema UP Migration
-- Core Tables
CREATE TABLE addresses (
    address_id VARCHAR PRIMARY KEY, -- CKB address string (ckb..., ckt...)
    script_code_hash BYTEA NOT NULL,
    script_hash_type SMALLINT NOT NULL, -- 0: data, 1: type, 2: data1
    script_args BYTEA NOT NULL,
    first_seen_block_number BIGINT NOT NULL,
    first_seen_tx_hash BYTEA NOT NULL,
    first_seen_tx_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE addresses IS 'Stores unique CKB script addresses and their components.';

COMMENT ON COLUMN addresses.address_id IS 'CKB address string (e.g., ckb..., ckt...). Primary key.';

CREATE INDEX idx_addresses_script_hash ON addresses (script_code_hash, script_hash_type);

CREATE TABLE block_height (
    id SERIAL PRIMARY KEY,
    height BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT single_row CHECK (id = 1) -- Enforce single row
);

COMMENT ON TABLE block_height IS 'Tracks the last processed block height by the indexer.';

INSERT INTO
    block_height (id, height)
VALUES
    (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE transaction_outputs_status (
    output_tx_hash BYTEA NOT NULL,
    output_tx_index INTEGER NOT NULL,
    consumed_by_tx_hash BYTEA, -- Hash of the tx that consumed this output (NULL if unspent)
    consumed_by_input_index INTEGER, -- Index of the input in the consuming tx (NULL if unspent)
    consuming_block_number BIGINT, -- Block number where consumption happened
    consuming_tx_timestamp TIMESTAMP, -- Timestamp of the consuming block
    PRIMARY KEY (output_tx_hash, output_tx_index)
);

COMMENT ON TABLE transaction_outputs_status IS 'Tracks the consumption status of transaction outputs (UTXOs).';

CREATE INDEX idx_tx_outputs_status_consuming_tx ON transaction_outputs_status (consumed_by_tx_hash);

-- Spore/Cluster Tables
CREATE TYPE spore_action_type AS ENUM (
    'MintSpore',
    'TransferSpore',
    'BurnSpore',
    'MintCluster',
    'TransferCluster',
    'MintProxy',
    'TransferProxy',
    'BurnProxy',
    'MintAgent',
    'TransferAgent',
    'BurnAgent'
);

CREATE TABLE clusters (
    cluster_id BYTEA PRIMARY KEY, -- Unique identifier for the cluster
    cluster_name VARCHAR,
    cluster_description TEXT,
    mutant_id BYTEA, -- Optional: Identifier for associated mutants
    owner_address_id VARCHAR, -- Stores the owner's address string (No FK)
    type_address_id VARCHAR NOT NULL, -- Stores the type script's address string (No FK)
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    -- Creation context
    created_at_block_number BIGINT NOT NULL,
    created_at_tx_hash BYTEA NOT NULL,
    created_at_output_index INTEGER NOT NULL,
    created_at_timestamp TIMESTAMP NOT NULL,
    -- Last update context (transfer, burn)
    last_updated_at_block_number BIGINT NOT NULL,
    last_updated_at_tx_hash BYTEA NOT NULL,
    last_updated_at_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE clusters IS 'Stores state information for Spore Clusters.';

COMMENT ON COLUMN clusters.cluster_id IS 'Unique ID of the cluster (e.g., CKB NFT ID derived from first input).';

CREATE INDEX idx_clusters_owner_address_id ON clusters (owner_address_id);

CREATE INDEX idx_clusters_type_address_id ON clusters (type_address_id);

CREATE INDEX idx_clusters_last_updated_block ON clusters (last_updated_at_block_number);

CREATE TABLE spores (
    spore_id BYTEA PRIMARY KEY, -- Unique identifier for the spore
    content_type VARCHAR,
    content BYTEA,
    cluster_id BYTEA, -- Stores the associated cluster ID (No FK)
    owner_address_id VARCHAR, -- Stores the owner's address string (No FK)
    type_address_id VARCHAR NOT NULL, -- Stores the type script's address string (No FK)
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    -- Creation context
    created_at_block_number BIGINT NOT NULL,
    created_at_tx_hash BYTEA NOT NULL,
    created_at_output_index INTEGER NOT NULL,
    created_at_timestamp TIMESTAMP NOT NULL,
    -- Last update context (transfer, burn)
    last_updated_at_block_number BIGINT NOT NULL,
    last_updated_at_tx_hash BYTEA NOT NULL,
    last_updated_at_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE spores IS 'Stores state information for Spore NFTs.';

COMMENT ON COLUMN spores.spore_id IS 'Unique ID of the spore (e.g., CKB NFT ID derived from first input).';

CREATE INDEX idx_spores_owner_address_id ON spores (owner_address_id);

CREATE INDEX idx_spores_cluster_id ON spores (cluster_id);

CREATE INDEX idx_spores_type_address_id ON spores (type_address_id);

CREATE INDEX idx_spores_last_updated_block ON spores (last_updated_at_block_number);

CREATE TABLE spore_actions (
    tx_hash BYTEA PRIMARY KEY, -- Transaction hash is the primary key for the action event
    block_number BIGINT NOT NULL,
    action_type spore_action_type NOT NULL,
    spore_id BYTEA, -- Spore ID involved (No FK)
    cluster_id BYTEA, -- Cluster ID involved (No FK)
    proxy_id BYTEA, -- Proxy ID involved, if applicable
    from_address_id VARCHAR, -- Sender address string (No FK)
    to_address_id VARCHAR, -- Receiver address string (No FK)
    action_data JSONB, -- Optional: Store action-specific data
    tx_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE spore_actions IS 'Records transactional events related to Spores and Clusters.';

COMMENT ON COLUMN spore_actions.tx_hash IS 'Transaction hash where the action occurred. Primary Key.';

CREATE INDEX idx_spore_actions_block_number ON spore_actions (block_number);

CREATE INDEX idx_spore_actions_action_type ON spore_actions (action_type);

CREATE INDEX idx_spore_actions_spore_id ON spore_actions (spore_id)
WHERE
    spore_id IS NOT NULL;

CREATE INDEX idx_spore_actions_cluster_id ON spore_actions (cluster_id)
WHERE
    cluster_id IS NOT NULL;

CREATE INDEX idx_spore_actions_from_address_id ON spore_actions (from_address_id)
WHERE
    from_address_id IS NOT NULL;

CREATE INDEX idx_spore_actions_to_address_id ON spore_actions (to_address_id)
WHERE
    to_address_id IS NOT NULL;

-- RGBPP Tables
CREATE TABLE rgbpp_locks (
    lock_args_hash BYTEA PRIMARY KEY, -- Hash of lock script args
    tx_hash BYTEA NOT NULL, -- CKB tx containing this lock output
    output_index INTEGER NOT NULL,
    lock_script_code_hash BYTEA NOT NULL,
    lock_script_hash_type SMALLINT NOT NULL,
    btc_txid BYTEA NOT NULL, -- Bitcoin txid related to this lock
    block_number BIGINT NOT NULL,
    tx_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE rgbpp_locks IS 'Records RGBPP lock events based on output lock scripts.';

COMMENT ON COLUMN rgbpp_locks.lock_args_hash IS 'Hash of the RGBPP lock script arguments, used as PK.';

CREATE INDEX idx_rgbpp_locks_tx_hash ON rgbpp_locks (tx_hash);

CREATE INDEX idx_rgbpp_locks_btc_txid ON rgbpp_locks (btc_txid);

CREATE INDEX idx_rgbpp_locks_block_number ON rgbpp_locks (block_number);

CREATE TABLE rgbpp_unlocks (
    unlock_witness_hash BYTEA PRIMARY KEY, -- Hash of the unlock witness data
    tx_hash BYTEA NOT NULL, -- CKB tx containing this unlock witness
    version SMALLINT NOT NULL,
    input_len SMALLINT NOT NULL,
    output_len SMALLINT NOT NULL,
    btc_tx BYTEA NOT NULL,
    btc_tx_proof BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    tx_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE rgbpp_unlocks IS 'Records RGBPP unlock events based on witness data.';

COMMENT ON COLUMN rgbpp_unlocks.unlock_witness_hash IS 'Hash of the RGBPP unlock witness data, used as PK.';

CREATE INDEX idx_rgbpp_unlocks_tx_hash ON rgbpp_unlocks (tx_hash);

CREATE INDEX idx_rgbpp_unlocks_block_number ON rgbpp_unlocks (block_number);

-- XUDT Tables
CREATE TABLE xudt_cells (
    tx_hash BYTEA NOT NULL, -- Tx hash where this cell was created
    output_index INTEGER NOT NULL, -- Output index in the creation tx
    lock_address_id VARCHAR NOT NULL, -- Lock script address string (No FK)
    type_address_id VARCHAR NOT NULL, -- Type script address string (No FK)
    amount NUMERIC(39, 0) NOT NULL,
    xudt_extension_args JSONB, -- Parsed extension args (e.g., list of addresses) as JSONB
    xudt_extension_data JSONB, -- Parsed data field (e.g., list of strings) as JSONB
    xudt_data_lock_hash BYTEA, -- Hash of the lock script in XudtData, if present
    owner_lock_hash BYTEA, -- Hash of the owner lock script in Type args, if present
    block_number BIGINT NOT NULL,
    tx_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (tx_hash, output_index)
);

COMMENT ON TABLE xudt_cells IS 'Stores state of XUDT cells (UTXOs). State is determined by joining with transaction_outputs_status.';

CREATE INDEX idx_xudt_cells_lock_address_id ON xudt_cells (lock_address_id);

CREATE INDEX idx_xudt_cells_type_address_id ON xudt_cells (type_address_id);

CREATE INDEX idx_xudt_cells_block_number ON xudt_cells (block_number);

CREATE TABLE token_info (
    type_address_id VARCHAR PRIMARY KEY, -- Type script address string is the natural key
    defining_tx_hash BYTEA NOT NULL, -- Tx where this info was defined
    defining_output_index INTEGER NOT NULL, -- Output index where info was defined
    decimal SMALLINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    udt_hash BYTEA, -- Optional: Hash associated with the token (e.g., from inscription)
    expected_supply NUMERIC(39, 0), -- Optional: For mintable tokens
    mint_limit NUMERIC(39, 0), -- Optional: For mintable tokens
    mint_status SMALLINT, -- Optional: For mintable tokens
    inscription_address_id VARCHAR, -- Optional: Associated inscription script address string (No FK)
    block_number BIGINT NOT NULL,
    tx_timestamp TIMESTAMP NOT NULL
);

COMMENT ON TABLE token_info IS 'Stores metadata about specific token types (XUDT, Inscription, etc.).';

COMMENT ON COLUMN token_info.type_address_id IS 'Address string of the type script defining the token. Primary Key.';

CREATE INDEX idx_token_info_inscription_address_id ON token_info (inscription_address_id)
WHERE
    inscription_address_id IS NOT NULL;

CREATE INDEX idx_token_info_block_number ON token_info (block_number);
