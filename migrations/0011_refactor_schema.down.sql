-- Add down migration script here
-- Refactored Schema DOWN Migration
-- Drop XUDT Tables
DROP TABLE IF EXISTS token_info;

DROP TABLE IF EXISTS xudt_cells;

-- Drop RGBPP Tables
DROP TABLE IF EXISTS rgbpp_unlocks;

DROP TABLE IF EXISTS rgbpp_locks;

-- Drop Spore/Cluster Tables
DROP TABLE IF EXISTS spore_actions;

DROP TABLE IF EXISTS spores;

DROP TABLE IF EXISTS clusters;

DROP TYPE IF EXISTS spore_action_type;

-- Drop enum type after tables using it
-- Drop Core Tables
DROP TABLE IF EXISTS transaction_outputs_status;

DROP TABLE IF EXISTS block_height;

DROP TABLE IF EXISTS addresses;
