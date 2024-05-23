-- Create enumeration type for spore action types
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

-- Create addresses table
CREATE TABLE addresses (
    id VARCHAR PRIMARY KEY,
    script_code_hash BYTEA NOT NULL,
    script_hash_type SMALLINT NOT NULL,
    script_args BYTEA NOT NULL
);

-- Create spore_actions table
CREATE TABLE spore_actions (
    id SERIAL PRIMARY KEY,
    tx BYTEA NOT NULL,
    action_type spore_action_type NOT NULL,
    spore_id BYTEA,
    cluster_id BYTEA,
    proxy_id BYTEA,
    from_address_id VARCHAR,
    to_address_id VARCHAR,
    data_hash BYTEA,
    content_type VARCHAR,
    content BYTEA,
    cluster_name VARCHAR,
    cluster_description TEXT,
    mutant_id BYTEA,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE clusters (
    id BYTEA PRIMARY KEY,
    cluster_name VARCHAR,
    cluster_description TEXT,
    mutant_id BYTEA,
    owner_address VARCHAR,
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE spores (
    id BYTEA PRIMARY KEY,
    content_type VARCHAR,
    content BYTEA,
    cluster_id BYTEA,
    owner_address VARCHAR,
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);