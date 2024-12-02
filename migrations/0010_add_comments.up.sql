-- Comments for addresses table
COMMENT ON TABLE addresses IS 'Stores unique identifiers for script addresses used in transactions.';
COMMENT ON COLUMN addresses.id IS 'Unique identifier for the address, typically a string representation.';
COMMENT ON COLUMN addresses.script_code_hash IS 'The code hash of the script associated with this address.';
COMMENT ON COLUMN addresses.script_hash_type IS 'The hash type of the script, indicating how the script is interpreted.';
COMMENT ON COLUMN addresses.script_args IS 'Arguments passed to the script.';

-- Comments for clusters table
COMMENT ON TABLE clusters IS 'Tracks cluster information for Spore assets, including their names, descriptions, mutation IDs, ownership, and burning status.';
COMMENT ON COLUMN clusters.id IS 'Unique identifier for the cluster.';
COMMENT ON COLUMN clusters.cluster_name IS 'Name of the cluster.';
COMMENT ON COLUMN clusters.cluster_description IS 'Description of the cluster.';
COMMENT ON COLUMN clusters.mutant_id IS 'Identifier for mutations related to this cluster.';
COMMENT ON COLUMN clusters.owner_address IS 'Reference to the address that owns this cluster.';
COMMENT ON COLUMN clusters.is_burned IS 'Indicates whether the cluster has been burned (transferred to a null address).';
COMMENT ON COLUMN clusters.created_at IS 'Timestamp when the cluster was created.';
COMMENT ON COLUMN clusters.updated_at IS 'Timestamp when the cluster information was last updated.';

-- Comments for spores table
COMMENT ON TABLE spores IS 'Holds details about individual Spore assets, including their type, content, associated cluster, owner, and burning status.';
COMMENT ON COLUMN spores.id IS 'Unique identifier for the spore.';
COMMENT ON COLUMN spores.content_type IS 'Type of content associated with the spore (e.g., text, image).';
COMMENT ON COLUMN spores.content IS 'The actual content data of the spore.';
COMMENT ON COLUMN spores.cluster_id IS 'Reference to the cluster this spore belongs to.';
COMMENT ON COLUMN spores.owner_address IS 'Reference to the address that owns this spore.';
COMMENT ON COLUMN spores.is_burned IS 'Indicates whether the spore has been burned.';
COMMENT ON COLUMN spores.created_at IS 'Timestamp when the spore was created.';
COMMENT ON COLUMN spores.updated_at IS 'Timestamp when the spore information was last updated.';

-- Comments for spore_actions table
COMMENT ON TABLE spore_actions IS 'Records transactional actions performed on Spore or Cluster assets, such as minting, transferring, or burning.';
COMMENT ON COLUMN spore_actions.tx IS 'The transaction hash that includes this action.';
COMMENT ON COLUMN spore_actions.action_type IS 'Type of action performed (e.g., MintSpore, TransferCluster).';
COMMENT ON COLUMN spore_actions.spore_id IS 'Reference to the Spore involved in this action, if applicable.';
COMMENT ON COLUMN spore_actions.cluster_id IS 'Reference to the Cluster involved in this action, if applicable.';
COMMENT ON COLUMN spore_actions.proxy_id IS 'Identifier for proxy-related actions.';
COMMENT ON COLUMN spore_actions.from_address_id IS 'Reference to the address from which the asset was transferred.';
COMMENT ON COLUMN spore_actions.to_address_id IS 'Reference to the address to which the asset was transferred.';
COMMENT ON COLUMN spore_actions.data_hash IS 'Hash of additional data related to this action.';
COMMENT ON COLUMN spore_actions.created_at IS 'Timestamp when this action occurred.';

-- Comments for rgbpp_locks table
COMMENT ON TABLE rgbpp_locks IS 'Indexes lock scripts associated with RGBPP assets, linking them to specific transaction outputs and Bitcoin transaction IDs.';
COMMENT ON COLUMN rgbpp_locks.lock_id IS 'Unique identifier for the lock script.';
COMMENT ON COLUMN rgbpp_locks.tx IS 'The CKB transaction hash that includes this lock.';
COMMENT ON COLUMN rgbpp_locks.out_index IS 'Index of the output in the transaction that contains this lock.';
COMMENT ON COLUMN rgbpp_locks.btc_txid IS 'Bitcoin transaction ID associated with this lock.';

-- Comments for rgbpp_unlocks table
COMMENT ON TABLE rgbpp_unlocks IS 'Records unlock scripts or conditions for spending RGBPP assets, including version information, input and output lengths, and related Bitcoin transactions and proofs.';
COMMENT ON COLUMN rgbpp_unlocks.unlock_id IS 'Unique identifier for the unlock script.';
COMMENT ON COLUMN rgbpp_unlocks.tx IS 'The CKB transaction hash that includes this unlock.';
COMMENT ON COLUMN rgbpp_unlocks.version IS 'Version of the unlock script.';
COMMENT ON COLUMN rgbpp_unlocks.input_len IS 'Length of inputs required by this unlock script.';
COMMENT ON COLUMN rgbpp_unlocks.output_len IS 'Length of outputs produced by this unlock script.';
COMMENT ON COLUMN rgbpp_unlocks.btc_tx IS 'Bitcoin transaction data associated with this unlock.';
COMMENT ON COLUMN rgbpp_unlocks.btc_tx_proof IS 'Proof verifying the Bitcoin transaction.';

-- Comments for xudt_cell table
COMMENT ON TABLE xudt_cell IS 'Indexes XUDT cells, recording transaction details, addresses involved, token amounts, and additional script arguments or data related to the tokens.';
COMMENT ON COLUMN xudt_cell.transaction_hash IS 'Hash of the transaction that includes this cell.';
COMMENT ON COLUMN xudt_cell.transaction_index IS 'Index of the output in the transaction.';
COMMENT ON COLUMN xudt_cell.lock_id IS 'Reference to the address that locks this cell.';
COMMENT ON COLUMN xudt_cell.type_id IS 'Reference to the type script defining this token.';
COMMENT ON COLUMN xudt_cell.amount IS 'Amount of tokens in this cell.';
COMMENT ON COLUMN xudt_cell.xudt_args IS 'Additional arguments related to XUDT.';
COMMENT ON COLUMN xudt_cell.xudt_data IS 'Extra data associated with XUDT.';
COMMENT ON COLUMN xudt_cell.xudt_data_lock IS 'Lock script for the XUDT data.';
COMMENT ON COLUMN xudt_cell.xudt_owner_lock_script_hash IS 'Hash of the owner lock script for XUDT.';
COMMENT ON COLUMN xudt_cell.is_consumed IS 'Indicates whether this cell has been spent in a subsequent transaction.';

-- Comments for transaction_outputs_status table
COMMENT ON TABLE transaction_outputs_status IS 'Tracks the consumption status of transaction outputs, indicating whether they have been spent in subsequent transactions.';
COMMENT ON COLUMN transaction_outputs_status.output_transaction_hash IS 'Hash of the transaction that produced this output.';
COMMENT ON COLUMN transaction_outputs_status.output_transaction_index IS 'Index of the output in the transaction.';
COMMENT ON COLUMN transaction_outputs_status.consumed_input_transaction_hash IS 'Hash of the transaction that consumed this output, if applicable.';
COMMENT ON COLUMN transaction_outputs_status.consumed_input_transaction_index IS 'Index of the input in the consuming transaction that spent this output.';

-- Comments for token_info table
COMMENT ON TABLE token_info IS 'Holds essential information about token definitions, aiding in the identification and tracking of different token types within the blockchain.';
COMMENT ON COLUMN token_info.type_id IS 'Reference to the address defining this token type.';
COMMENT ON COLUMN token_info.transaction_hash IS 'Hash of the transaction that defined or updated this token information.';
COMMENT ON COLUMN token_info.transaction_index IS 'Index of the output in the transaction that contains this token definition.';
COMMENT ON COLUMN token_info.decimal IS 'Number of decimal places for the token.';
COMMENT ON COLUMN token_info.name IS 'Name of the token.';
COMMENT ON COLUMN token_info.symbol IS 'Symbol or shorthand for the token name.';
COMMENT ON COLUMN token_info.udt_hash IS 'Hash of the UDT (User-Defined Token) contract, if applicable.';
COMMENT ON COLUMN token_info.expected_supply IS 'Expected total supply of the token.';
COMMENT ON COLUMN token_info.mint_limit IS 'Limit on the amount that can be minted.';
COMMENT ON COLUMN token_info.mint_status IS 'Status indicating the current minting situation.';

-- Comments for block_height table
COMMENT ON TABLE block_height IS 'Maintains the record of the current block height processed by the indexer, ensuring synchronization with the blockchain''s progression.';
COMMENT ON COLUMN block_height.id IS 'Primary key, should always be 1 due to the single-row constraint.';
COMMENT ON COLUMN block_height.height IS 'The latest block height processed by the indexer.';
