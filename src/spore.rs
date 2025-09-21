use bigdecimal::ToPrimitive as _;
use ckb_jsonrpc_types::{CellOutput, TransactionView};
use ckb_types::H256;
use molecule::prelude::Reader as _;
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelIterator as _, IntoParallelRefIterator as _,
    ParallelIterator,
};

use chrono::DateTime;
use sea_orm::ActiveValue::NotSet;
use sea_orm::Set;
use tokio::sync::mpsc;
use tracing::{debug, error};
use utils::network::NetworkType;

use crate::helper::{to_timestamp_naive, upsert_address};
use crate::{
    database::Operations,
    entity::{clusters, sea_orm_active_enums, spore_actions, spores},
    schemas::{action, spore_v1, spore_v2, top_level},
};

use constants::Constants;

/// Analysis result for Spore transaction processing
#[derive(Debug, Clone)]
pub struct SporeAnalysis {
    pub has_burn_operations: bool,
    pub has_mint_operations: bool,
    pub has_transfer_operations: bool,
    pub actions: Vec<action::SporeActionUnion>,
}

/// Analyze Spore-related aspects of a transaction
pub fn analyze_spore_transaction(tx: &ckb_jsonrpc_types::TransactionView) -> SporeAnalysis {
    use molecule::prelude::Reader as _;

    // Extract Spore actions from witnesses (moved from spore.rs)
    let spore_actions: Vec<action::SporeActionUnion> = tx
        .inner
        .witnesses
        .par_iter()
        .filter_map(|witness| {
            let witness_bytes = witness.as_bytes();
            top_level::WitnessLayoutReader::from_slice(witness_bytes)
                .ok()
                .and_then(|r| match r.to_enum() {
                    top_level::WitnessLayoutUnionReader::SighashAll(s) => {
                        let actions = s
                            .message()
                            .to_entity()
                            .actions()
                            .into_iter()
                            .filter_map(|action| {
                                let action_data = action.data();
                                action::SporeActionReader::from_slice(&action_data.raw_data())
                                    .ok()
                                    .map(|reader| reader.to_entity().to_enum())
                            })
                            .collect::<Vec<_>>();
                        Some(actions)
                    }
                    _ => None,
                })
        })
        .flatten()
        .collect();

    // Analyze action types
    let (has_burn_operations, has_mint_operations, has_transfer_operations) = spore_actions
        .iter()
        .fold(
            (false, false, false),
            |(burn, mint, transfer), action| match action {
                action::SporeActionUnion::BurnSpore(_) => (true, mint, transfer),
                action::SporeActionUnion::BurnProxy(_) => (true, mint, transfer),
                action::SporeActionUnion::BurnAgent(_) => (true, mint, transfer),
                action::SporeActionUnion::MintSpore(_) => (burn, true, transfer),
                action::SporeActionUnion::MintCluster(_) => (burn, true, transfer),
                action::SporeActionUnion::TransferSpore(_) => (burn, mint, true),
                action::SporeActionUnion::TransferCluster(_) => (burn, mint, true),
                action::SporeActionUnion::MintProxy(_) => (burn, true, transfer),
                action::SporeActionUnion::TransferProxy(_) => (burn, mint, true),
                action::SporeActionUnion::MintAgent(_) => (burn, true, transfer),
                action::SporeActionUnion::TransferAgent(_) => (burn, mint, true),
            },
        );

    SporeAnalysis {
        has_burn_operations,
        has_mint_operations,
        has_transfer_operations,
        actions: spore_actions,
    }
}

/// Main function that uses pre-analyzed results to avoid duplicate processing
pub fn try_index_spore(
    tx: TransactionView,
    timestamp: u64,
    block_number: u64,
    network: NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
    spore_analysis: &SporeAnalysis,
    prev_outputs: Option<Vec<CellOutput>>,
) -> Result<(), anyhow::Error> {
    // Skip processing if no spore-related actions found
    if !spore_analysis.has_burn_operations
        && !spore_analysis.has_mint_operations
        && !spore_analysis.has_transfer_operations
    {
        return Ok(());
    }

    // Use pre-analyzed results for more efficient processing
    index_spore_with_analysis(
        tx,
        timestamp,
        block_number,
        network,
        op_sender,
        constants,
        spore_analysis,
        &prev_outputs,
    )
}

// Internal data structures
enum DataVariant {
    Spore(spore_v1::SporeData),
    ClusterV2(spore_v2::ClusterDataV2),
    ClusterV1(spore_v1::ClusterData),
}

/// Optimized indexing function that uses pre-analyzed results
fn index_spore_with_analysis(
    tx: TransactionView,
    timestamp: u64,
    block_number: u64,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
    constants: Constants,
    spore_analysis: &SporeAnalysis,
    prev_outputs: &Option<Vec<CellOutput>>,
) -> anyhow::Result<()> {
    debug!("Indexing Spore with analysis for tx: {}", tx.hash);

    let tx_hash = tx.hash.clone();

    // Pre-compute output pairs to enable better parallel processing
    let output_pairs: Vec<_> = tx
        .inner
        .outputs_data
        .iter()
        .zip(tx.inner.outputs.iter())
        .enumerate()
        .collect();

    // Process outputs in parallel using pre-analyzed mappings
    output_pairs
        .into_par_iter()
        .filter_map(|(index, (output_data, output_cell))| {
            let type_script = output_cell.type_.as_ref()?;
            let id = type_script.args.as_bytes().get(..32)?.to_vec();

            let variant = if constants.is_spore_type(type_script) {
                spore_v1::SporeDataReader::from_compatible_slice(output_data.as_bytes())
                    .ok()
                    .map(|reader| reader.to_entity())
                    .map(DataVariant::Spore)
            } else if constants.is_cluster_type(type_script) {
                spore_v2::ClusterDataV2Reader::from_compatible_slice(output_data.as_bytes())
                    .ok()
                    .map(|reader| reader.to_entity())
                    .map(DataVariant::ClusterV2)
                    .or_else(|| {
                        spore_v1::ClusterDataReader::from_compatible_slice(output_data.as_bytes())
                            .ok()
                            .map(|reader| reader.to_entity())
                            .map(DataVariant::ClusterV1)
                    })
            } else {
                None
            };

            variant.map(|v| OutputData {
                variant: v,
                to_address_script: action::AddressUnion::from_json_script(output_cell.lock.clone()),
                type_id_script: action::AddressUnion::from_json_script(type_script.clone()),
                id,
                output_index: index.to_i32().unwrap(),
            })
        })
        .try_for_each(|output_data| {
            upsert_spore_or_cluster(
                output_data,
                network,
                block_number,
                &tx_hash,
                timestamp,
                op_sender.clone(),
            )
        })?;

    // Process burn actions using pre-analyzed results
    if spore_analysis.has_burn_operations {
        let burn_actions: Vec<_> = spore_analysis
            .actions
            .iter()
            .filter(|action| {
                matches!(action, action::SporeActionUnion::BurnSpore(_))
                    || matches!(action, action::SporeActionUnion::BurnProxy(_))
                    || matches!(action, action::SporeActionUnion::BurnAgent(_))
            })
            .collect();

        // Process burn actions using pre-built mappings
        for action in burn_actions {
            process_burn_action_with_mappings(
                action,
                &tx_hash,
                block_number,
                timestamp,
                &op_sender,
                prev_outputs,
                &constants,
            )?;
        }
    }

    // Process non-burn actions (mint/transfer) using pre-analyzed results
    if spore_analysis.has_mint_operations || spore_analysis.has_transfer_operations {
        let non_burn_actions: Vec<_> = spore_analysis
            .actions
            .iter()
            .filter(|action| {
                !matches!(action, action::SporeActionUnion::BurnSpore(_))
                    && !matches!(action, action::SporeActionUnion::BurnProxy(_))
                    && !matches!(action, action::SporeActionUnion::BurnAgent(_))
            })
            .collect();

        // Build spore_id to output_index map for action processing
        let spore_id_to_output_index: std::collections::HashMap<Vec<u8>, i32> = tx
            .inner
            .outputs
            .par_iter()
            .enumerate()
            .filter_map(|(index, output)| {
                let type_script = output.type_.as_ref()?;
                if constants.is_spore_type(type_script) || constants.is_cluster_type(type_script) {
                    type_script
                        .args
                        .as_bytes()
                        .get(..32)
                        .map(|bytes| (bytes.to_vec(), index as i32))
                } else {
                    None
                }
            })
            .collect();

        // Process non-burn actions
        for action in non_burn_actions {
            // Find the output index for this action based on spore/cluster ID
            let output_index = action
                .spore_id()
                .or_else(|| action.cluster_id())
                .and_then(|id| spore_id_to_output_index.get(&id).cloned())
                .unwrap_or(0); // Default to 0 if not found

            insert_action(
                action.clone(),
                &tx_hash,
                output_index,
                network,
                block_number,
                timestamp,
                op_sender.clone(),
            )?;
        }
    }

    Ok(())
}

/// Process burn action with input validation
fn process_burn_action_with_mappings(
    action: &action::SporeActionUnion,
    tx_hash: &H256,
    block_number: u64,
    timestamp: u64,
    op_sender: &mpsc::UnboundedSender<Operations>,
    prev_outputs: &Option<Vec<CellOutput>>,
    constants: &Constants,
) -> Result<(), anyhow::Error> {
    match action {
        action::SporeActionUnion::BurnSpore(_) => {
            if let Some(spore_id) = action.spore_id() {
                let spore_id_hex = hex::encode(&spore_id);
                tracing::debug!(
                    "Processing BurnSpore action for spore_id: {} in tx: {}",
                    spore_id_hex,
                    hex::encode(tx_hash)
                );

                // Validate that the spore_id exists in the inputs being consumed AND can be parsed as a valid Spore
                let spore_id_in_inputs = if let Some(prev_outputs) = prev_outputs {
                    prev_outputs.iter().any(|output| {
                        if let Some(type_script) = &output.type_ {
                            // First check if this is a spore type script and contains the matching spore_id
                            type_script.args.as_bytes().get(..32) == Some(&spore_id) &&
                            constants.is_spore_type(type_script)
                        } else {
                            false
                        }
                    })
                } else {
                    tracing::warn!(
                        "No previous outputs available for BurnSpore validation in tx: {}",
                        hex::encode(tx_hash)
                    );
                    false
                };

                if spore_id_in_inputs {
                    let burn_model = spores::ActiveModel {
                        spore_id: Set(spore_id),
                        is_burned: Set(true),
                        last_updated_at_block_number: Set(block_number as i64),
                        last_updated_at_tx_hash: Set(tx_hash.as_bytes().to_vec()),
                        last_updated_at_timestamp: Set(DateTime::from_timestamp(
                            timestamp as i64,
                            0,
                        )
                        .map(|dt| dt.naive_utc())
                        .unwrap_or_default()),
                        // All other fields should be NotSet to preserve existing data
                        owner_address_id: NotSet,
                        content_type: NotSet,
                        content: NotSet,
                        cluster_id: NotSet,
                        type_address_id: NotSet,
                        created_at_block_number: NotSet,
                        created_at_tx_hash: NotSet,
                        created_at_output_index: NotSet,
                        created_at_timestamp: NotSet,
                    };

                    op_sender
                        .send(Operations::BurnSpore(burn_model))
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to send BurnSpore operation: {}", e)
                        })?;

                    tracing::debug!(
                        "Successfully processed BurnSpore for spore_id: {}",
                        spore_id_hex
                    );
                } else {
                    tracing::warn!(
                        "BurnSpore action for spore_id: {} in tx: {} - spore_id not found in inputs, skipping",
                        spore_id_hex,
                        hex::encode(tx_hash)
                    );
                }
            }
        }
        action::SporeActionUnion::BurnProxy(_) | action::SporeActionUnion::BurnAgent(_) => {
            if let Some(cluster_id) = action.cluster_id() {
                let cluster_id_hex = hex::encode(&cluster_id);
                let action_type = match action {
                    action::SporeActionUnion::BurnProxy(_) => "BurnProxy",
                    action::SporeActionUnion::BurnAgent(_) => "BurnAgent",
                    _ => "Unknown",
                };

                tracing::debug!(
                    "Processing {} action for cluster_id: {} in tx: {}",
                    action_type,
                    cluster_id_hex,
                    hex::encode(tx_hash)
                );

                // Validate that the cluster_id exists in the inputs being consumed AND can be parsed as a valid Cluster
                let cluster_id_in_inputs = if let Some(prev_outputs) = prev_outputs {
                    prev_outputs.iter().any(|output| {
                        if let Some(type_script) = &output.type_ {
                            // First check if this is a cluster type script and contains the matching cluster_id
                            type_script.args.as_bytes().get(..32) == Some(&cluster_id) &&
                            constants.is_cluster_type(type_script)
                        } else {
                            false
                        }
                    })
                } else {
                    tracing::warn!(
                        "No previous outputs available for {} validation in tx: {}",
                        action_type,
                        hex::encode(tx_hash)
                    );
                    false
                };

                if cluster_id_in_inputs {
                    let burn_model = clusters::ActiveModel {
                        cluster_id: Set(cluster_id),
                        is_burned: Set(true),
                        last_updated_at_block_number: Set(block_number as i64),
                        last_updated_at_tx_hash: Set(tx_hash.as_bytes().to_vec()),
                        last_updated_at_timestamp: Set(DateTime::from_timestamp(
                            timestamp as i64,
                            0,
                        )
                        .map(|dt| dt.naive_utc())
                        .unwrap_or_default()),
                        // All other fields should be NotSet to preserve existing data
                        owner_address_id: NotSet,
                        cluster_name: NotSet,
                        cluster_description: NotSet,
                        mutant_id: NotSet,
                        type_address_id: NotSet,
                        created_at_block_number: NotSet,
                        created_at_tx_hash: NotSet,
                        created_at_output_index: NotSet,
                        created_at_timestamp: NotSet,
                    };

                    op_sender
                        .send(Operations::BurnCluster(burn_model))
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to send BurnCluster operation: {}", e)
                        })?;

                    tracing::debug!(
                        "Successfully processed {} for cluster_id: {}",
                        action_type,
                        cluster_id_hex
                    );
                } else {
                    tracing::warn!(
                        "{} action for cluster_id: {} in tx: {} - cluster_id not found in inputs, skipping",
                        action_type,
                        cluster_id_hex,
                        hex::encode(tx_hash)
                    );
                }
            }
        }
        _ => {}
    }

    Ok(())
}

struct OutputData {
    id: Vec<u8>, // Spore or Cluster ID
    type_id_script: action::AddressUnion,
    to_address_script: action::AddressUnion,
    variant: DataVariant,
    output_index: i32, // Need the index for creation context
}

/// Insert spore action into database (restored function)
fn insert_action(
    action_union: action::SporeActionUnion,
    tx_hash: &H256,
    output_index: i32,
    network: NetworkType,
    block_number: u64,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    // Upsert related addresses in parallel for better performance
    let (from_id_result, to_id_result) = rayon::join(
        || {
            action_union.from_address_id(
                network,
                block_number,
                tx_hash,
                timestamp,
                op_sender.clone(),
            )
        },
        || action_union.to_address_id(network, block_number, tx_hash, timestamp, op_sender.clone()),
    );
    let from_id = from_id_result;
    let to_id = to_id_result;

    // Create the action model
    let action_model = spore_actions::ActiveModel {
        tx_hash: Set(tx_hash.0.to_vec()),
        block_number: Set(block_number as i64),
        action_type: Set(action_union.action_type()),
        spore_id: Set(action_union.spore_id()),
        cluster_id: Set(action_union.cluster_id()),
        proxy_id: Set(action_union.proxy_id()),
        from_address_id: Set(from_id),
        to_address_id: Set(to_id),
        action_data: Set(action_union.to_action_data_json()), // Store raw hex in JSON
        tx_timestamp: Set(to_timestamp_naive(timestamp)),     // Use helper
        output_index: Set(output_index),
    };

    debug!("insert action: {:?}", action_model);

    // Send operation
    op_sender
        .send(Operations::UpsertActions(action_model))
        .map_err(|e| anyhow::anyhow!("Failed to send UpsertActions operation: {}", e))?;

    Ok(())
}

// Helper methods for SporeActionUnion to get address IDs
trait SporeActionHelpers {
    fn from_address_id(
        &self,
        network: NetworkType,
        block_number: u64,
        tx_hash: &H256,
        timestamp: u64,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Option<String>;

    fn to_address_id(
        &self,
        network: NetworkType,
        block_number: u64,
        tx_hash: &H256,
        timestamp: u64,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Option<String>;
}

impl SporeActionHelpers for action::SporeActionUnion {
    fn from_address_id(
        &self,
        network: NetworkType,
        block_number: u64,
        tx_hash: &H256,
        timestamp: u64,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Option<String> {
        self.from_address().and_then(|address| {
            match upsert_address(
                &address,
                network,
                block_number,
                tx_hash,
                timestamp,
                op_sender,
            ) {
                Ok(id) => Some(id),
                Err(e) => {
                    error!("Failed to upsert 'from' address for action: {:?}", e);
                    None
                }
            }
        })
    }

    fn to_address_id(
        &self,
        network: NetworkType,
        block_number: u64,
        tx_hash: &H256,
        timestamp: u64,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Option<String> {
        self.to_address().and_then(|address| {
            match upsert_address(
                &address,
                network,
                block_number,
                tx_hash,
                timestamp,
                op_sender,
            ) {
                Ok(id) => Some(id),
                Err(e) => {
                    error!("Failed to upsert 'to' address for action: {:?}", e);
                    None
                }
            }
        })
    }
}

// --- Action related methods ---
impl action::SporeActionUnion {
    fn action_type(&self) -> sea_orm_active_enums::SporeActionType {
        use sea_orm_active_enums::SporeActionType;
        match self {
            action::SporeActionUnion::MintSpore(_) => SporeActionType::MintSpore,
            action::SporeActionUnion::TransferSpore(_) => SporeActionType::TransferSpore,
            action::SporeActionUnion::BurnSpore(_) => SporeActionType::BurnSpore,
            action::SporeActionUnion::MintCluster(_) => SporeActionType::MintCluster,
            action::SporeActionUnion::TransferCluster(_) => SporeActionType::TransferCluster,
            action::SporeActionUnion::MintProxy(_) => SporeActionType::MintProxy,
            action::SporeActionUnion::TransferProxy(_) => SporeActionType::TransferProxy,
            action::SporeActionUnion::BurnProxy(_) => SporeActionType::BurnProxy,
            action::SporeActionUnion::MintAgent(_) => SporeActionType::MintAgent,
            action::SporeActionUnion::TransferAgent(_) => SporeActionType::TransferAgent,
            action::SporeActionUnion::BurnAgent(_) => SporeActionType::BurnAgent,
        }
    }

    fn spore_id(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintSpore(raw) => Some(raw.spore_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferSpore(raw) => {
                Some(raw.spore_id().raw_data().to_vec())
            }
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.spore_id().raw_data().to_vec()),
            _ => None,
        }
    }

    fn cluster_id(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintCluster(raw) => {
                Some(raw.cluster_id().raw_data().to_vec())
            }
            action::SporeActionUnion::TransferCluster(raw) => {
                Some(raw.cluster_id().raw_data().to_vec())
            }
            action::SporeActionUnion::MintProxy(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferProxy(raw) => {
                Some(raw.cluster_id().raw_data().to_vec())
            }
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferAgent(raw) => {
                Some(raw.cluster_id().raw_data().to_vec())
            }
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            _ => None,
        }
    }

    fn proxy_id(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintProxy(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferProxy(raw) => {
                Some(raw.proxy_id().raw_data().to_vec())
            }
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            _ => None,
        }
    }

    fn to_action_data_json(&self) -> Option<sea_orm::prelude::Json> {
        use molecule::bytes::Bytes;
        use serde_json::json;
        let bytes: Bytes = self.as_bytes();
        let hex_string = hex::encode(bytes.as_ref());
        Some(json!({ "raw_hex": hex_string }))
    }

    fn from_address(&self) -> Option<action::AddressUnion> {
        match self {
            action::SporeActionUnion::TransferSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferCluster(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferAgent(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.from().to_enum()),
            _ => None,
        }
    }

    fn to_address(&self) -> Option<action::AddressUnion> {
        match self {
            action::SporeActionUnion::MintSpore(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferSpore(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::MintCluster(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferCluster(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::MintProxy(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferProxy(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferAgent(raw) => Some(raw.to().to_enum()),
            _ => None,
        }
    }
}

// --- Data parsing methods ---
impl spore_v1::SporeData {
    fn content_type_op(&self) -> Option<String> {
        Some(self.content_type().into_string())
    }

    fn content_op(&self) -> Option<Vec<u8>> {
        Some(self.content().raw_data().to_vec())
    }

    fn cluster_id_op(&self) -> Option<Vec<u8>> {
        self.cluster_id()
            .to_opt()
            .map(|bytes| bytes.raw_data().to_vec())
    }
}

impl spore_v1::Bytes {
    #[allow(clippy::wrong_self_convention)]
    pub fn into_string(&self) -> String {
        String::from_utf8(self.raw_data().to_vec()).unwrap_or_else(|e| {
            error!("into_string failed for bytes {:?}: {e:?}", self.raw_data());
            String::new()
        })
    }
}

impl spore_v2::ClusterDataV2 {
    fn description_op(&self) -> Option<String> {
        Some(self.description().into_string())
    }

    fn name_op(&self) -> Option<String> {
        Some(self.name().into_string())
    }

    fn mutant_id_op(&self) -> Option<Vec<u8>> {
        self.mutant_id()
            .to_opt()
            .map(|bytes| bytes.raw_data().to_vec())
    }
}

impl spore_v1::ClusterData {
    fn description_op(&self) -> Option<String> {
        Some(self.description().into_string())
    }

    fn name_op(&self) -> Option<String> {
        Some(self.name().into_string())
    }
}

// Upsert Spore or Cluster state based on output data
fn upsert_spore_or_cluster(
    data: OutputData,
    network: NetworkType,
    block_number: u64,
    tx_hash: &H256,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let now_naive = to_timestamp_naive(timestamp); // Use helper

    let OutputData {
        id, // Spore or Cluster ID
        type_id_script,
        to_address_script,
        variant,
        output_index,
    } = data;

    // Upsert addresses involved in parallel for better performance
    let (type_address_id_result, owner_address_id_result) = rayon::join(
        || {
            upsert_address(
                &type_id_script,
                network,
                block_number,
                tx_hash,
                timestamp,
                op_sender.clone(),
            )
        },
        || {
            upsert_address(
                &to_address_script, // Use the 'to' address as the owner
                network,
                block_number,
                tx_hash,
                timestamp,
                op_sender.clone(),
            )
        },
    );
    let type_address_id = type_address_id_result?;
    let owner_address_id = owner_address_id_result?;

    let block_num_i64 = block_number as i64;
    let tx_hash_vec = tx_hash.0.to_vec();
    let output_idx_i32 = output_index as i32;

    match variant {
        DataVariant::Spore(spore_data) => {
            let model = spores::ActiveModel {
                spore_id: Set(id),
                owner_address_id: Set(Some(owner_address_id)),
                type_address_id: Set(type_address_id),
                content_type: Set(spore_data.content_type_op()),
                content: Set(spore_data.content_op()),
                cluster_id: Set(spore_data.cluster_id_op()),
                is_burned: Set(false),
                created_at_block_number: Set(block_num_i64),
                created_at_tx_hash: Set(tx_hash_vec.clone()),
                created_at_output_index: Set(output_idx_i32),
                created_at_timestamp: Set(now_naive),
                last_updated_at_block_number: Set(block_num_i64), // Initially same
                last_updated_at_tx_hash: Set(tx_hash_vec),        // Initially same
                last_updated_at_timestamp: Set(now_naive),        // Initially same
            };
            op_sender.send(Operations::UpsertSpores(model))?;
        }
        DataVariant::ClusterV2(cluster_data) => {
            let model = clusters::ActiveModel {
                cluster_id: Set(id),
                owner_address_id: Set(Some(owner_address_id)),
                type_address_id: Set(type_address_id),
                cluster_name: Set(cluster_data.name_op()),
                cluster_description: Set(cluster_data.description_op()),
                mutant_id: Set(cluster_data.mutant_id_op()),
                is_burned: Set(false),
                created_at_block_number: Set(block_num_i64),
                created_at_tx_hash: Set(tx_hash_vec.clone()),
                created_at_output_index: Set(output_idx_i32),
                created_at_timestamp: Set(now_naive),
                last_updated_at_block_number: Set(block_num_i64),
                last_updated_at_tx_hash: Set(tx_hash_vec),
                last_updated_at_timestamp: Set(now_naive),
            };
            op_sender.send(Operations::UpsertCluster(model))?;
        }
        DataVariant::ClusterV1(cluster_data) => {
            let model = clusters::ActiveModel {
                cluster_id: Set(id),
                owner_address_id: Set(Some(owner_address_id)),
                type_address_id: Set(type_address_id),
                cluster_name: Set(cluster_data.name_op()),
                cluster_description: Set(cluster_data.description_op()),
                mutant_id: Set(None), // V1 doesn't have mutant_id
                is_burned: Set(false),
                created_at_block_number: Set(block_num_i64),
                created_at_tx_hash: Set(tx_hash_vec.clone()),
                created_at_output_index: Set(output_idx_i32),
                created_at_timestamp: Set(now_naive),
                last_updated_at_block_number: Set(block_num_i64),
                last_updated_at_tx_hash: Set(tx_hash_vec),
                last_updated_at_timestamp: Set(now_naive),
            };
            op_sender.send(Operations::UpsertCluster(model))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use ckb_jsonrpc_types::TransactionView;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::{
        database::Operations,
        spore::{analyze_spore_transaction, try_index_spore},
    };
    use constants::Constants;

    #[tokio::test]
    async fn debug_index_spore() {
        use tracing::Level;
        use tracing_subscriber::FmtSubscriber;

        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        use hex;
        use tracing::debug;
        let urls = vec![
            "https://testnet.ckbapp.dev/".to_string(),
            "https://testnet.ckb.dev/".to_string(),
        ];

        let featcher = fetcher::HttpFetcher::http_client(
            urls,
            std::time::Duration::from_secs(600), // evaluation_interval (sort_interval_secs from default)
            5,                                   // call_max_retries (max_retries from default)
            std::time::Duration::from_millis(500), // call_retry_interval (retry_interval from default)
            100 * 1024 * 1024,                     // max_response_size (100MB from default)
            100 * 1024 * 1024,                     // max_request_size (100MB from default)
        )
        .await
        .expect("failed to create HTTP fetcher");
        let hash =
            ckb_types::h256!("0x855064475ebf279c30d8ebb20b75874eea378664ce7ea7ded101a1b95c10fd63");
        let inner = featcher
            .get_txs_by_hashes(vec![hash.clone()])
            .await
            .expect("failed to fetch transaction")
            .into_values()
            .next()
            .expect("no transaction found in response");
        let tx = TransactionView { inner, hash };
        debug!("tx: {tx:?}");
        let (sender, mut recver) = mpsc::unbounded_channel();
        // Analyze the transaction to create SporeAnalysis
        let spore_analysis = analyze_spore_transaction(&tx);

        // Real prev_outputs fetching like in index.rs
        let prev_outputs = if spore_analysis.has_burn_operations && !tx.inner.inputs.is_empty() {
            match featcher.get_outputs(tx.inner.inputs.clone()).await {
                Ok(prev_outputs) => Some(prev_outputs),
                Err(e) => {
                    debug!(
                        "Failed to fetch previous outputs for tx {}: {:?}",
                        hex::encode(&tx.hash),
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        try_index_spore(
            tx,
            0,
            0,
            utils::network::NetworkType::Testnet,
            Constants::Testnet,
            sender,
            &spore_analysis,
            prev_outputs,
        )
        .unwrap();
        while let Some(op) = recver.recv().await {
            if let Operations::UpsertSpores(spore) = &op {
                if let Some(spore_id) = spore.spore_id.try_as_ref() {
                    let hex_id = hex::encode(spore_id);
                    debug!("spore_id (hex) from UpsertSpores: {}", hex_id);
                }
            }
            if let Operations::UpsertActions(action) = &op {
                if let Some(Some(spore_id)) = &action.spore_id.try_as_ref() {
                    let hex_id = hex::encode(spore_id);
                    debug!("spore_id (hex) from UpsertActions: {}", hex_id);
                }
            }
            if let Operations::UpsertCluster(cluster) = &op {
                if let Some(cluster_id) = cluster.cluster_id.try_as_ref() {
                    let hex_id = hex::encode(cluster_id);
                    debug!("cluster_id (hex) from UpsertCluster: {}", hex_id);
                }
            }
            if let Operations::UpsertAddress(address) = &op {
                if let Some(address_id) = address.address_id.try_as_ref() {
                    debug!("address_id from UpsertAddress: {}", address_id);
                }
            }
            debug!("op: {op:?}");
        }

        let id = [
            9, 232, 185, 221, 127, 140, 95, 78, 226, 187, 141, 69, 70, 252, 171, 211, 4, 67, 78,
            63, 149, 139, 50, 6, 243, 45, 206, 152, 249, 179, 82, 101,
        ];
        let id_hex = hex::encode(id);
        debug!("id_hex: {}", id_hex);

        debug!("done!");
    }

    #[test]
    fn test_negative_index_logic() {
        // Test the core logic of negative index mapping
        // This test verifies that input indices are correctly mapped to negative values

        // Simulate different input indices
        let input_indices = vec![0, 1, 2, 5, 10];

        // Test the mapping logic: input_index -> -(input_index + 1)
        let expected_negative_indices = vec![-1, -2, -3, -6, -11];

        for (i, &input_index) in input_indices.iter().enumerate() {
            let negative_index = -(input_index as i32 + 1);
            assert_eq!(
                negative_index, expected_negative_indices[i],
                "Input {} should map to {}, got {}",
                input_index, expected_negative_indices[i], negative_index
            );

            // Verify that negative indices are indeed negative
            assert!(
                negative_index < 0,
                "Index should be negative for input {}",
                input_index
            );

            // Verify that we can recover the original input index
            let recovered_input = (-negative_index - 1) as usize;
            assert_eq!(
                recovered_input, input_index,
                "Should recover input {} from negative index {}, got {}",
                input_index, negative_index, recovered_input
            );
        }

        // Test that all negative indices are unique
        let negative_indices: Vec<i32> = input_indices.iter().map(|&i| -(i as i32 + 1)).collect();

        let unique_indices: std::collections::HashSet<_> = negative_indices.iter().collect();
        assert_eq!(
            unique_indices.len(),
            negative_indices.len(),
            "All negative indices should be unique"
        );

        println!("Negative index logic test passed!");
    }
}
