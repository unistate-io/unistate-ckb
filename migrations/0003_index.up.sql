-- Add up migration script here
CREATE TABLE block_height (
    id SERIAL PRIMARY KEY,
    height BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT single_row CHECK (id = 1)
);

INSERT INTO
    block_height (id, height)
VALUES
    (1, 0);