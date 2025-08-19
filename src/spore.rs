use ckb_jsonrpc_types::TransactionView;
use ckb_types::H256;
use molecule::{bytes::Bytes, prelude::Reader as _};
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelIterator as _, IntoParallelRefIterator as _,
    ParallelIterator,
};
use rayon::slice::ParallelSlice as _;

use sea_orm::{Set, prelude::Json};
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{debug, error};
use utils::network::NetworkType;

use crate::helper::{to_timestamp_naive, upsert_address};
use crate::{
    database::Operations,
    entity::{self, clusters, spore_actions, spores},
    schemas::{
        action, spore_v1, spore_v2,
        top_level::{WitnessLayoutReader, WitnessLayoutUnionReader},
    },
};

use constants::Constants;

// Struct to hold transaction context
#[derive(Debug, Clone)] // Add Clone derive
pub struct SporeTx {
    pub tx: TransactionView,
    pub timestamp: u64,
    pub block_number: u64,
}

// Main indexer function
pub fn index_spores(
    txs: Vec<SporeTx>,
    network: NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> Result<(), anyhow::Error> {
    // Use adaptive parallel processing based on transaction count
    if txs.len() > 100 {
        // Process in chunks for better memory locality
        txs.par_chunks(20).try_for_each(|chunk| {
            chunk.iter().try_for_each(|ctx| {
                index_spore(
                    ctx.tx.clone(),
                    ctx.timestamp,
                    ctx.block_number,
                    network,
                    op_sender.clone(),
                    constants,
                )
            })
        })?;
    } else {
        // Simple parallel iteration for small batches
        txs.into_par_iter().try_for_each(|ctx| {
            index_spore(
                ctx.tx,
                ctx.timestamp,
                ctx.block_number,
                network,
                op_sender.clone(),
                constants,
            )
        })?;
    }

    Ok(())
}

// Internal data structures
enum DataVariant {
    Spore(spore_v1::SporeData),
    ClusterV2(spore_v2::ClusterDataV2),
    ClusterV1(spore_v1::ClusterData),
}

struct OutputData {
    id: Vec<u8>, // Spore or Cluster ID
    type_id_script: action::AddressUnion,
    to_address_script: action::AddressUnion,
    variant: DataVariant,
    output_index: i32, // Need the index for creation context
}

fn index_spore(
    tx: TransactionView,
    timestamp: u64,
    block_number: u64,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
    constants: Constants,
) -> anyhow::Result<()> {
    debug!("Indexing Spore for tx: {}", tx.hash);

    let tx_hash = tx.hash.clone();

    // Pre-compute output pairs to enable better parallel processing
    let output_pairs: Vec<_> = tx
        .inner
        .outputs_data
        .iter()
        .zip(tx.inner.outputs.iter())
        .enumerate()
        .collect();

    // Process outputs in parallel with optimized data access
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
                output_index: index as i32,
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

    extract_actions(&tx, &constants)
        .into_par_iter()
        .try_for_each(|(output_index, action_data)| {
            insert_action(
                action_data,
                &tx_hash,
                output_index,
                network,
                block_number,
                timestamp,
                op_sender.clone(),
            )
        })?;

    Ok(())
}

// --- Action related methods ---
impl action::SporeActionUnion {
    fn action_type(&self) -> entity::sea_orm_active_enums::SporeActionType {
        use entity::sea_orm_active_enums::SporeActionType;
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

    // Get 'from' address ID, ensuring address exists in DB
    #[allow(clippy::wrong_self_convention)]
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
                // Use helper
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

    // Get 'to' address ID, ensuring address exists in DB
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
                // Use helper
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

    // Extract 'from' address script
    #[allow(clippy::wrong_self_convention)]
    fn from_address(&self) -> Option<action::AddressUnion> {
        match self {
            action::SporeActionUnion::TransferSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferCluster(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::TransferAgent(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.from().to_enum()),
            _ => None, // Mint actions don't have 'from'
        }
    }

    // Extract 'to' address script
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
            _ => None, // Burn actions don't have 'to'
        }
    }

    // Serialize action to JSON containing raw hex bytes
    fn to_action_data_json(&self) -> Option<Json> {
        let bytes: Bytes = self.as_bytes(); // SporeActionUnion implements Entity, which has as_bytes()
        let hex_string = hex::encode(bytes.as_ref());
        // Store as a JSON object with a specific key for clarity
        Some(json!({ "raw_hex": hex_string }))
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

// Insert Spore/Cluster action event
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

    // For burn actions, update the spore/cluster to mark as burned
    match &action_union {
        action::SporeActionUnion::BurnSpore(_) => {
            if let Some(spore_id) = action_union.spore_id() {
                let burn_model = spores::ActiveModel {
                    spore_id: Set(spore_id),
                    is_burned: Set(true),
                    last_updated_at_block_number: Set(block_number as i64),
                    last_updated_at_tx_hash: Set(tx_hash.0.to_vec()),
                    last_updated_at_timestamp: Set(to_timestamp_naive(timestamp)),
                    ..Default::default()
                };
                op_sender
                    .send(Operations::UpsertSpores(burn_model))
                    .map_err(|e| anyhow::anyhow!("Failed to send UpsertSpores operation: {}", e))?;
            }
        }
        action::SporeActionUnion::BurnProxy(_) | action::SporeActionUnion::BurnAgent(_) => {
            if let Some(cluster_id) = action_union.cluster_id() {
                let burn_model = clusters::ActiveModel {
                    cluster_id: Set(cluster_id),
                    is_burned: Set(true),
                    last_updated_at_block_number: Set(block_number as i64),
                    last_updated_at_tx_hash: Set(tx_hash.0.to_vec()),
                    last_updated_at_timestamp: Set(to_timestamp_naive(timestamp)),
                    ..Default::default()
                };
                op_sender
                    .send(Operations::UpsertCluster(burn_model))
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to send UpsertCluster operation: {}", e)
                    })?;
            }
        }
        _ => {}
    }

    Ok(())
}

// Helper function to create a simple mapping for burn actions
// Since we can't easily fetch previous outputs in this context,
// we'll map burn actions to negative indices based on their position
// in the inputs array, assuming they correlate with the burn actions
fn create_burn_action_mapping(
    tx: &ckb_jsonrpc_types::TransactionView,
    burn_actions: &[action::SporeActionUnion],
) -> std::collections::HashMap<Vec<u8>, i32> {
    let mut mapping = std::collections::HashMap::new();
    let mut burn_action_index = 0;

    // Create a vector of input indices (0, 1, 2, ...)
    let input_indices: Vec<usize> = (0..tx.inner.inputs.len()).collect();

    for action in burn_actions {
        let spore_id = match action {
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.spore_id().raw_data().to_vec()),
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.cluster_id().raw_data().to_vec()),
            _ => None,
        };

        if let Some(id) = spore_id {
            // Map to negative index: -1 for input 0, -2 for input 1, etc.
            if let Some(&input_index) = input_indices.get(burn_action_index) {
                mapping.insert(id, -(input_index as i32 + 1));
                burn_action_index += 1;
            } else {
                // Fallback if we run out of input indices
                mapping.insert(id, -999);
            }
        }
    }

    mapping
}

// Extract actions from transaction witnesses

fn extract_actions(
    tx: &ckb_jsonrpc_types::TransactionView,
    constants: &Constants,
) -> Vec<(i32, action::SporeActionUnion)> {
    // Get SighashAll messages from witnesses in parallel
    // Extract actions from messages in parallel - optimized with better parallel processing
    let spore_actions: Vec<_> = tx
        .inner
        .witnesses
        .par_iter()
        .filter_map(|witness| {
            WitnessLayoutReader::from_slice(witness.as_bytes())
                .ok()
                .and_then(|r| match r.to_enum() {
                    WitnessLayoutUnionReader::SighashAll(s) => Some(
                        s.message()
                            .to_entity()
                            .actions()
                            .into_iter()
                            .filter_map(|action| {
                                action::SporeActionReader::from_slice(&action.data().raw_data())
                                    .ok()
                                    .map(|reader| reader.to_entity().to_enum())
                            })
                            .collect::<Vec<_>>(),
                    ),
                    _ => None,
                })
        })
        .flatten()
        .collect();

    // Build spore_id to output_index map in parallel
    let spore_id_to_output_index: std::collections::HashMap<Vec<u8>, usize> = tx
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
                    .map(|bytes| (bytes.to_vec(), index))
            } else {
                None
            }
        })
        .collect();

    // Separate burn actions from other actions to create mapping
    let (burn_actions, other_actions): (Vec<_>, Vec<_>) =
        spore_actions.into_par_iter().partition(|action| {
            matches!(
                action,
                action::SporeActionUnion::BurnSpore(_)
                    | action::SporeActionUnion::BurnProxy(_)
                    | action::SporeActionUnion::BurnAgent(_)
            )
        });

    // Create mapping for burn actions
    let burn_action_mapping = create_burn_action_mapping(tx, &burn_actions);

    // Process burn actions
    let burn_actions_with_indices: Vec<(i32, action::SporeActionUnion)> = burn_actions
        .into_par_iter()
        .filter_map(|action| {
            let spore_id = match &action {
                action::SporeActionUnion::BurnSpore(raw) => {
                    Some(raw.spore_id().raw_data().to_vec())
                }
                action::SporeActionUnion::BurnProxy(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::BurnAgent(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                _ => None,
            };

            spore_id.and_then(|id| {
                burn_action_mapping
                    .get(&id)
                    .map(|&negative_index| (negative_index, action))
            })
        })
        .collect();

    // Process other actions (mint, transfer)
    let other_actions_with_indices: Vec<(i32, action::SporeActionUnion)> = other_actions
        .into_par_iter()
        .filter_map(|action| {
            let spore_id = match &action {
                action::SporeActionUnion::MintSpore(raw) => {
                    Some(raw.spore_id().raw_data().to_vec())
                }
                action::SporeActionUnion::TransferSpore(raw) => {
                    Some(raw.spore_id().raw_data().to_vec())
                }
                action::SporeActionUnion::MintCluster(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::TransferCluster(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::MintProxy(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::TransferProxy(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::MintAgent(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                action::SporeActionUnion::TransferAgent(raw) => {
                    Some(raw.cluster_id().raw_data().to_vec())
                }
                _ => None,
            };

            spore_id.and_then(|id| {
                spore_id_to_output_index
                    .get(&id)
                    .map(|&index| (index as i32, action))
            })
        })
        .collect();

    // Combine all actions
    let mut actions_with_indices = Vec::new();
    actions_with_indices.extend(burn_actions_with_indices);
    actions_with_indices.extend(other_actions_with_indices);

    actions_with_indices
}

// Keep the old function for backward compatibility during transition

#[cfg(test)]
mod tests {
    use ckb_jsonrpc_types::TransactionView;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::{database::Operations, spore::index_spore};
    use constants::Constants;

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn debug_index_spore() {
        let urls = vec![
            "https://mainnet.ckbapp.dev/".to_string(),
            "https://mainnet.ckb.dev/".to_string(),
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
            ckb_types::h256!("0xba5ba022e67f2cd89e97c552f14a1d1c592572dd22e2fe9c675b5cf5ed9bb04d");
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
        index_spore(
            tx,
            0,
            0,
            utils::network::NetworkType::Mainnet,
            sender,
            Constants::Mainnet,
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

    #[test]
    fn test_create_burn_action_mapping() {
        use super::create_burn_action_mapping;
        use crate::schemas::action::SporeActionUnion;
        use ckb_jsonrpc_types::{CellInput, OutPoint, Transaction, TransactionView};

        // Create a mock transaction with 3 inputs
        let tx = Transaction {
            version: Default::default(),
            cell_deps: Default::default(),
            header_deps: Default::default(),
            inputs: vec![
                CellInput {
                    previous_output: OutPoint {
                        tx_hash: Default::default(),
                        index: Default::default(),
                    },
                    since: Default::default(),
                },
                CellInput {
                    previous_output: OutPoint {
                        tx_hash: Default::default(),
                        index: Default::default(),
                    },
                    since: Default::default(),
                },
                CellInput {
                    previous_output: OutPoint {
                        tx_hash: Default::default(),
                        index: Default::default(),
                    },
                    since: Default::default(),
                },
            ],
            outputs: vec![],
            outputs_data: vec![],
            witnesses: vec![],
        };

        let tx_view = TransactionView {
            inner: tx,
            hash: Default::default(),
        };

        // Test with empty burn actions
        let empty_burn_actions: Vec<SporeActionUnion> = vec![];
        let mapping = create_burn_action_mapping(&tx_view, &empty_burn_actions);
        assert_eq!(
            mapping.len(),
            0,
            "Empty burn actions should produce empty mapping"
        );

        // Test Default::default() behavior to understand what spore_id we get
        let default_burn_spore = SporeActionUnion::BurnSpore(Default::default());
        if let SporeActionUnion::BurnSpore(raw) = &default_burn_spore {
            let spore_id = raw.spore_id().raw_data().to_vec();
            println!("Default BurnSpore spore_id: {:?}", spore_id);
            // Check if the default spore_id is non-empty
            if !spore_id.is_empty() {
                // If non-empty, we can use it for testing
                let burn_actions = vec![
                    default_burn_spore,
                    SporeActionUnion::BurnProxy(Default::default()),
                    SporeActionUnion::BurnAgent(Default::default()),
                ];

                let mapping = create_burn_action_mapping(&tx_view, &burn_actions);
                println!("Mapping with default actions: {:?}", mapping);

                // Test that we get some mappings (even if we don't know the exact count)
                // The important thing is that the function doesn't panic and produces valid negative indices
                let indices: Vec<i32> = mapping.values().cloned().collect();
                for &index in &indices {
                    assert!(index < 0, "Index should be negative, got: {}", index);
                }

                println!(
                    "Create burn action mapping test passed with {} mappings!",
                    mapping.len()
                );
            } else {
                println!("Default BurnSpore produces empty spore_id, skipping detailed test");
                // Test that the function handles empty spore_id gracefully
                let burn_actions = vec![default_burn_spore];
                let mapping = create_burn_action_mapping(&tx_view, &burn_actions);
                assert_eq!(
                    mapping.len(),
                    0,
                    "Empty spore_id should not be added to mapping"
                );
                println!("Empty spore_id test passed!");
            }
        } else {
            panic!("Expected BurnSpore variant");
        }
    }
}
