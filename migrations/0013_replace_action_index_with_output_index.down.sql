-- Revert spore_actions primary key from output_index back to action_index

-- First, drop the existing primary key constraint
ALTER TABLE spore_actions DROP CONSTRAINT spore_actions_pkey;

-- Drop the existing output_index column
ALTER TABLE spore_actions DROP COLUMN output_index;

-- Add action_index column back
ALTER TABLE spore_actions ADD COLUMN action_index INTEGER NOT NULL DEFAULT 0;

-- Create composite primary key using tx_hash and action_index
ALTER TABLE spore_actions ADD CONSTRAINT spore_actions_pkey PRIMARY KEY (tx_hash, action_index);

-- Add index on action_index for better query performance
CREATE INDEX idx_spore_actions_action_index ON spore_actions (action_index);

-- Update comment for the table
COMMENT ON TABLE spore_actions IS 'Records transactional events related to Spores and Clusters. Supports multiple actions per transaction.';

-- Update comment for tx_hash column
COMMENT ON COLUMN spore_actions.tx_hash IS 'Transaction hash where the action occurred. Part of composite primary key.';

-- Add comment for action_index column
COMMENT ON COLUMN spore_actions.action_index IS 'Index distinguishing multiple actions within the same transaction. Part of composite primary key.';
