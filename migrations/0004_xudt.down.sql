-- Drop indexes from token_info fields
DROP INDEX idx_token_info_type_id;

-- Drop indexes from xudt_cell fields
DROP INDEX idx_xudt_cell_type_id;
DROP INDEX idx_xudt_cell_lock_id;

-- Drop token_info table
DROP TABLE token_info;

-- Drop trigger and function for updating is_consumed field
DROP TRIGGER trg_update_is_consumed ON transaction_outputs_status;
DROP FUNCTION update_is_consumed;

-- Drop transaction_outputs_status table
DROP TABLE transaction_outputs_status;

-- Drop xudt_cell table
DROP TABLE xudt_cell;
