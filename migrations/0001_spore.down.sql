-- 删除 cluster_actions 表索引
DROP INDEX IF EXISTS cluster_actions_cluster_id_idx;
DROP INDEX IF EXISTS cluster_actions_from_address_id_idx;
DROP INDEX IF EXISTS cluster_actions_to_address_id_idx;

-- 删除 spore_actions 表索引
DROP INDEX IF EXISTS spore_actions_spore_id_idx;
DROP INDEX IF EXISTS spore_actions_from_address_id_idx;
DROP INDEX IF EXISTS spore_actions_to_address_id_idx;

-- 删除 clusters 表索引
DROP INDEX IF EXISTS clusters_owner_address_id_idx;

-- 删除 spores 表索引
DROP INDEX IF EXISTS spores_owner_address_id_idx;
DROP INDEX IF EXISTS spores_cluster_id_idx;

-- 删除 addresses 表索引
DROP INDEX IF EXISTS addresses_script_code_hash_script_hash_type_idx;

-- 删除 cluster_actions 表
DROP TABLE IF EXISTS cluster_actions;

-- 删除 spore_actions 表
DROP TABLE IF EXISTS spore_actions;

-- 删除 clusters 表
DROP TABLE IF EXISTS clusters;

-- 删除 spores 表
DROP TABLE IF EXISTS spores;

-- 删除 addresses 表
DROP TABLE IF EXISTS addresses;