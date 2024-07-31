-- Remove index on type_id column in clusters table
DROP INDEX IF EXISTS idx_clusters_type_id;

-- Remove type_id column from clusters table
ALTER TABLE clusters
DROP COLUMN IF EXISTS type_id;

-- Remove index on type_id column in spores table
DROP INDEX IF EXISTS idx_spores_type_id;

-- Remove type_id column from spores table
ALTER TABLE spores
DROP COLUMN IF EXISTS type_id;