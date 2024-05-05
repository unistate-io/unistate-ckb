-- 创建 rgbpp_locks 表
CREATE TABLE rgbpp_locks (
    lock_id BYTEA PRIMARY KEY,
    tx BYTEA NOT NULL,
    out_index INTEGER NOT NULL,
    btc_txid BYTEA NOT NULL
);

-- 创建 rgbpp_unlocks 表
CREATE TABLE rgbpp_unlocks (
    unlock_id BYTEA PRIMARY KEY,
    tx BYTEA NOT NULL,
    version SMALLINT NOT NULL,
    input_len SMALLINT NOT NULL,
    output_len SMALLINT NOT NULL,
    btc_tx BYTEA NOT NULL,
    btc_tx_proof BYTEA NOT NULL
);