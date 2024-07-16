-- Drop the trigger
DROP TRIGGER IF EXISTS trigger_update_is_burned_and_updated_at ON spore_actions;

-- Drop the trigger function
DROP FUNCTION IF EXISTS update_is_burned_and_updated_at();

-- Drop the existing spore_actions table
DROP TABLE spore_actions;

-- Recreate the original spore_actions table with the old structure
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

-- Recreate the indexes
CREATE INDEX idx_spore_actions_spore_id ON spore_actions (spore_id);
CREATE INDEX idx_spore_actions_cluster_id ON spore_actions (cluster_id);
CREATE INDEX idx_spore_actions_from_address_id ON spore_actions (from_address_id);
CREATE INDEX idx_spore_actions_to_address_id ON spore_actions (to_address_id);
