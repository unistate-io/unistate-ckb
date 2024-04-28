-- 删除 spore_actions 表中的外键列
ALTER TABLE spore_actions DROP COLUMN IF EXISTS rgbpp_lock_id;
ALTER TABLE spore_actions DROP COLUMN IF EXISTS rgbpp_unlock_id;

-- 删除 cluster_actions 表中的外键列
ALTER TABLE cluster_actions DROP COLUMN IF EXISTS rgbpp_lock_id;
ALTER TABLE cluster_actions DROP COLUMN IF EXISTS rgbpp_unlock_id;

-- 删除 rgbpp_unlocks 表
DROP TABLE IF EXISTS rgbpp_unlocks;

-- 删除 rgbpp_locks 表
DROP TABLE IF EXISTS rgbpp_locks;

