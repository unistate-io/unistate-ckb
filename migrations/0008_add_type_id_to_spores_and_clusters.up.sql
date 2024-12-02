-- Add type_id column to spores table
ALTER TABLE spores ADD COLUMN IF NOT EXISTS type_id VARCHAR REFERENCES addresses(id);

-- Create index on type_id column in spores table
CREATE INDEX IF NOT EXISTS idx_spores_type_id ON spores (type_id);

-- Add type_id column to clusters table
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS type_id VARCHAR REFERENCES addresses(id);

-- Create index on type_id column in clusters table
CREATE INDEX IF NOT EXISTS idx_clusters_type_id ON clusters (type_id);
