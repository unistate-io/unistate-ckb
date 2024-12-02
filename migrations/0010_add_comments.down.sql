-- Remove comments for addresses table
COMMENT ON TABLE addresses IS NULL;
COMMENT ON COLUMN addresses.id IS NULL;
COMMENT ON COLUMN addresses.script_code_hash IS NULL;
COMMENT ON COLUMN addresses.script_hash_type IS NULL;
COMMENT ON COLUMN addresses.script_args IS NULL;

-- Remove comments for clusters table
COMMENT ON TABLE clusters IS NULL;
COMMENT ON COLUMN clusters.id IS NULL;
COMMENT ON COLUMN clusters.cluster_name IS NULL;
COMMENT ON COLUMN clusters.cluster_description IS NULL;
COMMENT ON COLUMN clusters.mutant_id IS NULL;
COMMENT ON COLUMN clusters.owner_address IS NULL;
COMMENT ON COLUMN clusters.is_burned IS NULL;
COMMENT ON COLUMN clusters.created_at IS NULL;
COMMENT ON COLUMN clusters.updated_at IS NULL;

-- Remove comments for spores table
COMMENT ON TABLE spores IS NULL;
COMMENT ON COLUMN spores.id IS NULL;
COMMENT ON COLUMN spores.content_type IS NULL;
COMMENT ON COLUMN spores.content IS NULL;
COMMENT ON COLUMN spores.cluster_id IS NULL;
COMMENT ON COLUMN spores.owner_address IS NULL;
COMMENT ON COLUMN spores.is_burned IS NULL;
COMMENT ON COLUMN spores.created_at IS NULL;
COMMENT ON COLUMN spores.updated_at IS NULL;

-- Remove comments for spore_actions table
COMMENT ON TABLE spore_actions IS NULL;
COMMENT ON COLUMN spore_actions.tx IS NULL;
COMMENT ON COLUMN spore_actions.action_type IS NULL;
COMMENT ON COLUMN spore_actions.spore_id IS NULL;
COMMENT ON COLUMN spore_actions.cluster_id IS NULL;
COMMENT ON COLUMN spore_actions.proxy_id IS NULL;
COMMENT ON COLUMN spore_actions.from_address_id IS NULL;
COMMENT ON COLUMN spore_actions.to_address_id IS NULL;
COMMENT ON COLUMN spore_actions.data_hash IS NULL;
COMMENT ON COLUMN spore_actions.created_at IS NULL;

-- Remove comments for rgbpp_locks table
COMMENT ON TABLE rgbpp_locks IS NULL;
COMMENT ON COLUMN rgbpp_locks.lock_id IS NULL;
COMMENT ON COLUMN rgbpp_locks.tx IS NULL;
COMMENT ON COLUMN rgbpp_locks.out_index IS NULL;
COMMENT ON COLUMN rgbpp_locks.btc_txid IS NULL;

-- Remove comments for rgbpp_unlocks table
COMMENT ON TABLE rgbpp_unlocks IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.unlock_id IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.tx IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.version IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.input_len IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.output_len IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.btc_tx IS NULL;
COMMENT ON COLUMN rgbpp_unlocks.btc_tx_proof IS NULL;

-- Remove comments for xudt_cell table
COMMENT ON TABLE xudt_cell IS NULL;
COMMENT ON COLUMN xudt_cell.transaction_hash IS NULL;
COMMENT ON COLUMN xudt_cell.transaction_index IS NULL;
COMMENT ON COLUMN xudt_cell.lock_id IS NULL;
COMMENT ON COLUMN xudt_cell.type_id IS NULL;
COMMENT ON COLUMN xudt_cell.amount IS NULL;
COMMENT ON COLUMN xudt_cell.xudt_args IS NULL;
COMMENT ON COLUMN xudt_cell.xudt_data IS NULL;
COMMENT ON COLUMN xudt_cell.xudt_data_lock IS NULL;
COMMENT ON COLUMN xudt_cell.xudt_owner_lock_script_hash IS NULL;
COMMENT ON COLUMN xudt_cell.is_consumed IS NULL;

-- Remove comments for transaction_outputs_status table
COMMENT ON TABLE transaction_outputs_status IS NULL;
COMMENT ON COLUMN transaction_outputs_status.output_transaction_hash IS NULL;
COMMENT ON COLUMN transaction_outputs_status.output_transaction_index IS NULL;
COMMENT ON COLUMN transaction_outputs_status.consumed_input_transaction_hash IS NULL;
COMMENT ON COLUMN transaction_outputs_status.consumed_input_transaction_index IS NULL;

-- Remove comments for token_info table
COMMENT ON TABLE token_info IS NULL;
COMMENT ON COLUMN token_info.type_id IS NULL;
COMMENT ON COLUMN token_info.transaction_hash IS NULL;
COMMENT ON COLUMN token_info.transaction_index IS NULL;
COMMENT ON COLUMN token_info.decimal IS NULL;
COMMENT ON COLUMN token_info.name IS NULL;
COMMENT ON COLUMN token_info.symbol IS NULL;
COMMENT ON COLUMN token_info.udt_hash IS NULL;
COMMENT ON COLUMN token_info.expected_supply IS NULL;
COMMENT ON COLUMN token_info.mint_limit IS NULL;
COMMENT ON COLUMN token_info.mint_status IS NULL;

-- Remove comments for block_height table
COMMENT ON TABLE block_height IS NULL;
COMMENT ON COLUMN block_height.id IS NULL;
COMMENT ON COLUMN block_height.height IS NULL;