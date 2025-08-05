-- Replace action_index with output_index as primary key for spore_actions table
-- This change makes the primary key more meaningful for queries

-- First, drop the existing primary key constraint
ALTER TABLE spore_actions DROP CONSTRAINT spore_actions_pkey;

-- Add output_index column allowing NULL initially for existing data
ALTER TABLE spore_actions ADD COLUMN output_index INTEGER;

-- Update existing data: for now set output_index to a default value (we'll delete and reindex anyway)
UPDATE spore_actions SET output_index = action_index WHERE output_index IS NULL;

-- Now make the column NOT NULL
ALTER TABLE spore_actions ALTER COLUMN output_index SET NOT NULL;

-- Drop the existing action_index column since it's no longer needed
ALTER TABLE spore_actions DROP COLUMN action_index;

-- Create composite primary key using tx_hash and output_index
ALTER TABLE spore_actions ADD CONSTRAINT spore_actions_pkey PRIMARY KEY (tx_hash, output_index);

-- Add index on output_index for better query performance
CREATE INDEX idx_spore_actions_output_index ON spore_actions (output_index);

-- Update comment for the table
COMMENT ON TABLE spore_actions IS 'Records transactional events related to Spores and Clusters. Uses output_index as primary key for meaningful queries.';

-- Update comment for tx_hash column
COMMENT ON COLUMN spore_actions.tx_hash IS 'Transaction hash where the action occurred. Part of composite primary key with output_index.';

-- Add comment for output_index column
COMMENT ON COLUMN spore_actions.output_index IS 'Index of the output in the transaction where this action occurred. Part of composite primary key.';
