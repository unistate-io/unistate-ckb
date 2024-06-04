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

CREATE TABLE clusters (
    id BYTEA PRIMARY KEY,
    cluster_name VARCHAR,
    cluster_description TEXT,
    mutant_id BYTEA,
    owner_address VARCHAR REFERENCES addresses(id),
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE spores (
    id BYTEA PRIMARY KEY,
    content_type VARCHAR,
    content BYTEA,
    cluster_id BYTEA REFERENCES clusters(id),
    owner_address VARCHAR REFERENCES addresses(id),
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Create spore_actions table
CREATE TABLE spore_actions (
    id SERIAL PRIMARY KEY,
    tx BYTEA NOT NULL,
    action_type spore_action_type NOT NULL,
    spore_id BYTEA REFERENCES spores(id),
    cluster_id BYTEA REFERENCES clusters(id),
    proxy_id BYTEA,
    from_address_id VARCHAR REFERENCES addresses(id),
    to_address_id VARCHAR REFERENCES addresses(id),
    data_hash BYTEA,
    content_type VARCHAR,
    content BYTEA,
    cluster_name VARCHAR,
    cluster_description TEXT,
    mutant_id BYTEA,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_clusters_owner_address ON clusters (owner_address);

CREATE INDEX idx_spores_cluster_id ON spores (cluster_id);

CREATE INDEX idx_spores_owner_address ON spores (owner_address);

CREATE INDEX idx_spore_actions_spore_id ON spore_actions (spore_id);

CREATE INDEX idx_spore_actions_cluster_id ON spore_actions (cluster_id);

CREATE INDEX idx_spore_actions_from_address_id ON spore_actions (from_address_id);

CREATE INDEX idx_spore_actions_to_address_id ON spore_actions (to_address_id);