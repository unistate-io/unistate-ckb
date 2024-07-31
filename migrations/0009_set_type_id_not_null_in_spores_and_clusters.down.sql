-- Alter clusters table to allow type_id to be NULL
ALTER TABLE IF EXISTS clusters
ALTER COLUMN IF EXISTS type_id DROP NOT NULL;

-- Alter spores table to allow type_id to be NULL
ALTER TABLE IF EXISTS spores
ALTER COLUMN IF EXISTS type_id DROP NOT NULL;