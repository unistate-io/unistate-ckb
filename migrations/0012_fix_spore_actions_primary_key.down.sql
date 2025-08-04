-- Revert the spore_actions primary key fix
-- Remove action_index column and restore original primary key

-- Drop the composite primary key constraint
ALTER TABLE spore_actions DROP CONSTRAINT spore_actions_pkey;

-- Drop the action_index column
ALTER TABLE spore_actions DROP COLUMN action_index;

-- Restore the original primary key on tx_hash only
ALTER TABLE spore_actions ADD CONSTRAINT spore_actions_pkey PRIMARY KEY (tx_hash);

-- Drop the action_index index
DROP INDEX IF EXISTS idx_spore_actions_action_index;

-- Restore original table comment
COMMENT ON TABLE spore_actions IS 'Records transactional events related to Spores and Clusters.';

-- Restore original tx_hash column comment
COMMENT ON COLUMN spore_actions.tx_hash IS 'Transaction hash where the action occurred. Primary Key.';
