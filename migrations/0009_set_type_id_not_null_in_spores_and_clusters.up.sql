-- Alter spores table to set type_id as NOT NULL

ALTER TABLE spores

ALTER COLUMN type_id SET NOT NULL;

-- Alter clusters table to set type_id as NOT NULL

ALTER TABLE clusters

ALTER COLUMN type_id SET NOT NULL;

