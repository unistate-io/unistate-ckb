-- 创建 addresses 表
CREATE TABLE addresses (
    address_id BYTEA PRIMARY KEY,
    script_code_hash BYTEA NOT NULL,
    script_hash_type SMALLINT NOT NULL,
    script_args BYTEA
);
CREATE INDEX ON addresses (script_code_hash, script_hash_type);

-- 创建 spores 表
CREATE TABLE spores (
    spore_id BYTEA PRIMARY KEY,
    owner_address_id BYTEA NOT NULL REFERENCES addresses(address_id),
    data_hash BYTEA NOT NULL,
    content_type BYTEA NOT NULL,
    content BYTEA NOT NULL,
    cluster_id BYTEA,
    is_burned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON spores (owner_address_id);
CREATE INDEX ON spores (cluster_id);

-- 创建 clusters 表
CREATE TABLE clusters (
    cluster_id BYTEA PRIMARY KEY,
    owner_address_id BYTEA NOT NULL REFERENCES addresses(address_id),
    data_hash BYTEA NOT NULL,
    name BYTEA NOT NULL,
    description BYTEA NOT NULL,
    mutant_id BYTEA,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON clusters (owner_address_id);

-- 创建 spore_actions 表
CREATE TABLE spore_actions (
    action_id SERIAL PRIMARY KEY,
    action_type SMALLINT NOT NULL,
    spore_id BYTEA NOT NULL REFERENCES spores(spore_id),
    from_address_id BYTEA REFERENCES addresses(address_id),
    to_address_id BYTEA REFERENCES addresses(address_id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON spore_actions (spore_id);
CREATE INDEX ON spore_actions (from_address_id);
CREATE INDEX ON spore_actions (to_address_id);

-- 创建 cluster_actions 表
CREATE TABLE cluster_actions (
    action_id SERIAL PRIMARY KEY,
    action_type SMALLINT NOT NULL,
    cluster_id BYTEA NOT NULL REFERENCES clusters(cluster_id),
    from_address_id BYTEA REFERENCES addresses(address_id),
    to_address_id BYTEA REFERENCES addresses(address_id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON cluster_actions (cluster_id);
CREATE INDEX ON cluster_actions (from_address_id);
CREATE INDEX ON cluster_actions (to_address_id);