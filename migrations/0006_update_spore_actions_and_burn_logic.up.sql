-- Drop the existing spore_actions table
DROP TABLE spore_actions;

-- Recreate the spore_actions table with tx as the primary key and without the specified columns
CREATE TABLE spore_actions (
    tx BYTEA PRIMARY KEY,
    action_type spore_action_type NOT NULL,
    spore_id BYTEA REFERENCES spores(id),
    cluster_id BYTEA REFERENCES clusters(id),
    proxy_id BYTEA,
    from_address_id VARCHAR REFERENCES addresses(id),
    to_address_id VARCHAR REFERENCES addresses(id),
    data_hash BYTEA,
    created_at TIMESTAMP NOT NULL
);

-- Create a trigger function to update is_burned and updated_at in spores or clusters
CREATE OR REPLACE FUNCTION update_is_burned_and_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.action_type = 'BurnSpore' AND NEW.spore_id IS NOT NULL THEN
        UPDATE spores 
        SET is_burned = TRUE, updated_at = NEW.created_at 
        WHERE id = NEW.spore_id;
    END IF;
    IF NEW.action_type IN ('BurnProxy', 'BurnAgent') AND NEW.cluster_id IS NOT NULL THEN
        UPDATE clusters 
        SET is_burned = TRUE, updated_at = NEW.created_at 
        WHERE id = NEW.cluster_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER trigger_update_is_burned_and_updated_at
AFTER INSERT ON spore_actions
FOR EACH ROW
EXECUTE FUNCTION update_is_burned_and_updated_at();
-- Drop the existing spore_actions table
DROP TABLE spore_actions;

-- Recreate the spore_actions table with tx as the primary key and without the specified columns
CREATE TABLE spore_actions (
    tx BYTEA PRIMARY KEY,
    action_type spore_action_type NOT NULL,
    spore_id BYTEA REFERENCES spores(id),
    cluster_id BYTEA REFERENCES clusters(id),
    proxy_id BYTEA,
    from_address_id VARCHAR REFERENCES addresses(id),
    to_address_id VARCHAR REFERENCES addresses(id),
    data_hash BYTEA,
    created_at TIMESTAMP NOT NULL
);

-- Create a trigger function to update is_burned and updated_at in spores or clusters
CREATE OR REPLACE FUNCTION update_is_burned_and_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.action_type = 'BurnSpore' AND NEW.spore_id IS NOT NULL THEN
        UPDATE spores 
        SET is_burned = TRUE, updated_at = NEW.created_at 
        WHERE id = NEW.spore_id;
    END IF;
    IF NEW.action_type IN ('BurnProxy', 'BurnAgent') AND NEW.cluster_id IS NOT NULL THEN
        UPDATE clusters 
        SET is_burned = TRUE, updated_at = NEW.created_at 
        WHERE id = NEW.cluster_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER trigger_update_is_burned_and_updated_at
AFTER INSERT ON spore_actions
FOR EACH ROW
EXECUTE FUNCTION update_is_burned_and_updated_at();
