-- Drop tables in reverse order to avoid dependencies
-- Drop trigger trg_update_is_consumed
DROP TRIGGER trg_update_is_consumed ON xudt_status_cell;

-- Drop function update_is_consumed
DROP FUNCTION update_is_consumed();

-- Drop table xudt_status_cell
DROP TABLE xudt_status_cell;

-- Drop table xudt_cell
DROP TABLE xudt_cell;

-- Drop table token_info
DROP TABLE token_info;