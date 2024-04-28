-- 创建 rgbpp_locks 表
CREATE TABLE rgbpp_locks (
    lock_id BYTEA PRIMARY KEY,
    out_index INTEGER NOT NULL,
    btc_txid BYTEA NOT NULL
);

-- 创建 rgbpp_unlocks 表
CREATE TABLE rgbpp_unlocks (
    unlock_id BYTEA PRIMARY KEY,
    version SMALLINT NOT NULL,
    input_len SMALLINT NOT NULL,
    output_len SMALLINT NOT NULL,
    btc_tx BYTEA NOT NULL,
    btc_tx_proof BYTEA NOT NULL
);

-- 在 spore_actions 表中添加外键引用
ALTER TABLE spore_actions ADD COLUMN rgbpp_lock_id BYTEA REFERENCES rgbpp_locks(lock_id);
ALTER TABLE spore_actions ADD COLUMN rgbpp_unlock_id BYTEA REFERENCES rgbpp_unlocks(unlock_id);

-- 在 cluster_actions 表中添加外键引用
ALTER TABLE cluster_actions ADD COLUMN rgbpp_lock_id BYTEA REFERENCES rgbpp_locks(lock_id);
ALTER TABLE cluster_actions ADD COLUMN rgbpp_unlock_id BYTEA REFERENCES rgbpp_unlocks(unlock_id);