use ckb_jsonrpc_types::TransactionView;
use ckb_types::H256;
use molecule::{bytes::Bytes, prelude::Reader as _};
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelIterator as _, IntoParallelRefIterator as _,
    ParallelIterator,
};
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
    // Iterate over contexts
    txs.into_par_iter().try_for_each(|ctx| {
        index_spore(
            ctx.tx,           // Pass tx from context
            ctx.timestamp,    // Pass timestamp from context
            ctx.block_number, // Pass block_number from context
            network,
            op_sender.clone(),
            constants,
        )
    })?;

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
    output_index: usize, // Need the index for creation context
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

    tx.inner
        .outputs_data
        .par_iter()
        .zip(tx.inner.outputs.par_iter())
        .enumerate()
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
                output_index: index,
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

    extract_actions(&tx)
        .into_par_iter()
        .try_for_each(|action_data| {
            insert_action(
                action_data,
                &tx_hash,
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

    // Upsert addresses involved and get their string IDs
    let type_address_id = upsert_address(
        // Use helper
        &type_id_script,
        network,
        block_number,
        tx_hash,
        timestamp,
        op_sender.clone(),
    )?;
    let owner_address_id = upsert_address(
        // Use helper
        &to_address_script, // Use the 'to' address as the owner
        network,
        block_number,
        tx_hash,
        timestamp,
        op_sender.clone(),
    )?;

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
    network: NetworkType,
    block_number: u64,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    // Upsert related addresses to ensure they exist before referencing
    let from_id =
        action_union.from_address_id(network, block_number, tx_hash, timestamp, op_sender.clone());
    let to_id =
        action_union.to_address_id(network, block_number, tx_hash, timestamp, op_sender.clone());

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
    };

    debug!("insert action: {:?}", action_model);

    // Send operation
    op_sender
        .send(Operations::UpsertActions(action_model))
        .map_err(|e| anyhow::anyhow!("Failed to send UpsertActions operation: {}", e))?;

    Ok(())
}

// Extract actions from transaction witnesses
fn extract_actions(tx: &ckb_jsonrpc_types::TransactionView) -> Vec<action::SporeActionUnion> {
    // Get SighashAll messages from witnesses
    let msgs = tx
        .inner
        .witnesses
        .par_iter() // Parallel iteration over witnesses
        .filter_map(|witness| {
            WitnessLayoutReader::from_slice(witness.as_bytes())
                .ok()
                .and_then(|r| match r.to_enum() {
                    WitnessLayoutUnionReader::SighashAll(s) => Some(s.message().to_entity()),
                    _ => None, // Ignore other witness types
                })
        })
        .collect::<Vec<_>>(); // Collect messages

    // debug!(
    //     "Extracted {} SighashAll messages from witnesses for tx {}",
    //     msgs.len(),
    //     tx.hash
    // );

    // Extract actions from messages
    let spore_actions: Vec<_> = msgs
        .par_iter() // Parallel iterator over messages
        .map(|msg| {
            // Sequentially process actions within each message
            msg.actions()
                .into_iter()
                .filter_map(|action| {
                    action::SporeActionReader::from_slice(&action.data().raw_data())
                        .ok()
                        .map(|reader| reader.to_entity().to_enum())
                })
                .collect::<Vec<_>>() // Collect actions for this message
        })
        // Flatten the Vec<Vec<Action>> into a ParallelIterator<Action>
        .flatten()
        // Collect all actions into the final Vec
        .collect();

    // debug!("Extracted {} spore actions for tx {}", spore_actions.len(), tx.hash);

    spore_actions
}

#[cfg(test)]
mod tests {
    use ckb_jsonrpc_types::TransactionView;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::spore::index_spore;
    use constants::Constants;

    #[test]
    #[tracing_test::traced_test]
    fn debug_index_spore() {
        let tx: TransactionView = serde_json::from_str(TEST_JSON).expect("deserialize failed.");
        debug!("tx: {tx:?}");
        let (sender, mut recver) = mpsc::unbounded_channel();
        index_spore(
            tx,
            0,
            0,
            utils::network::NetworkType::Testnet,
            sender,
            Constants::Testnet,
        )
        .unwrap();
        while let Some(op) = recver.blocking_recv() {
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

    const TEST_JSON: &'static str = r#"
    {
      "cell_deps": [
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x96b198fb5ddbd1eed57ed667068f1f1e55d07907b4c0dbd38675a69ea1b69824"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xe464b7fb9311c5e2820e61c99afc615d6b98bdefbe318c34868c010cbd0dc938"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x40449b80339a6cbb2fd74e3f511b367d82aa888040648c712e5000b5286f2e8b"
          }
        },
        {
          "dep_type": "dep_group",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xf05188e5f3a6767fc4687faf45ba5f1a6e25d3ada6129dae8722cb282f262493"
          }
        }
      ],
      "hash": "0x49e22275cbd55bce814192b8f8913d3a977d0f9dca41828eb384836d40066237",
      "header_deps": [],
      "inputs": [
        {
          "previous_output": {
            "index": "0x1",
            "tx_hash": "0x40449b80339a6cbb2fd74e3f511b367d82aa888040648c712e5000b5286f2e8b"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x0",
            "tx_hash": "0x40449b80339a6cbb2fd74e3f511b367d82aa888040648c712e5000b5286f2e8b"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x0",
            "tx_hash": "0xa19d7aa51d60b92cab30b9b85f72db8ec5691a6a3175edd2c0cab71d4a088cb4"
          },
          "since": "0x0"
        }
      ],
      "outputs": [
        {
          "capacity": "0x52b391e00",
          "lock": {
            "args": "0x0001fd155e7c2e7348f93cdd5e4ab3922c68d859834d",
            "code_hash": "0xd00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e318323",
            "hash_type": "type"
          },
          "type": {
            "args": "0x09e8b9dd7f8c5f4ee2bb8d4546fcabd304434e3f958b3206f32dce98f9b35265",
            "code_hash": "0x4a4dce1df3dffff7f8b2cd7dff7303df3b6150c9788cb75dcf6747247132b9f5",
            "hash_type": "data1"
          }
        },
        {
          "capacity": "0xb8c63f000",
          "lock": {
            "args": "0x0001fd155e7c2e7348f93cdd5e4ab3922c68d859834d",
            "code_hash": "0xd00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e318323",
            "hash_type": "type"
          },
          "type": {
            "args": "0xd7c32f5bc9b7b76efcbd455e58933dd449ccc7e7150e0f36fa71b44c7272362b",
            "code_hash": "0x7366a61534fa7c7e6225ecc0d828ea3b5366adec2b58206f2ee84995fe030075",
            "hash_type": "data1"
          }
        },
        {
          "capacity": "0xe703ba360e",
          "lock": {
            "args": "0x0001fd155e7c2e7348f93cdd5e4ab3922c68d859834d",
            "code_hash": "0xd00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e318323",
            "hash_type": "type"
          },
          "type": null
        }
      ],
      "outputs_data": [
        "0x5e00000010000000190000003a00000005000000646f622f301d0000007b2022646e61223a20223363326139626563323063393738313522207d20000000d7c32f5bc9b7b76efcbd455e58933dd449ccc7e7150e0f36fa71b44c7272362b",
        "0x70010000100000001f000000700100000b00000053696d706c65206c6f6f744d0100007b226465736372697074696f6e223a22412073696d706c65206c6f6f7420636c7573746572222c22646f62223a7b22766572223a302c226465636f646572223a7b2274797065223a22636f64655f68617368222c2268617368223a22307831336361633738616438343832323032663138663964663465613730373631316333356639393433373566613033616537393132313331326464613939323563227d2c227061747465726e223a5b5b224261636b67726f756e64436f6c6f72222c22537472696e67222c302c312c226f7074696f6e73222c5b22726564222c22626c7565222c22677265656e222c22626c61636b222c227768697465225d5d2c5b2254797065222c224e756d626572222c312c312c2272616e6765222c5b31302c35305d5d2c5b2254696d657374616d70222c224e756d626572222c322c342c227261774e756d626572225d5d7d7d",
        "0x"
      ],
      "version": "0x0",
      "witnesses": [
        "0xd501000010000000d5010000d5010000c1010000019facc2de8a6664813665eee45eebdc01182ccb055a9a8113a920c3028ff4ccf021eb388fc2f7680b77c4060e15f2e1eaf2ed844ba101c9639ed5c781f0894df8e5fbca5bb1f8b0bf1a65ccebf2841ae4aa485e40b1bac4fc893b593d15b75ddfabe187bd0630d4663605328deb43158b78b6c61481e813c99c97db8507896486d280d9320d7862ad09b32103900596a08ba01a51863a8aac3f5ac1969360ae301d000000007b2274797065223a22776562617574686e2e676574222c226368616c6c656e6765223a224e7a45775a544e684d7a49354f57466b4f5752685a6a67784d4455354f57526c596d46684f5755334d3245334d44426d4e546b354d4449315a574d354e57526a4d6d49324e6a6b794e574e68596a51784e546c684d77222c226f726967696e223a2268747470733a2f2f6170702e6a6f792e6964222c2263726f73734f726967696e223a66616c73652c226f746865725f6b6579735f63616e5f62655f61646465645f68657265223a22646f206e6f7420636f6d7061726520636c69656e74446174614a534f4e20616761696e737420612074656d706c6174652e205365652068747470733a2f2f676f6f2e676c2f796162506578227d",
        "0x010000ff410200000c00000010000000000000003102000008000000290200000c00000003010000f700000010000000300000005000000023cde495cfd125019140ea933e76e3cbf16189483232ab5a05d88fb34d1023e1e09b872be9b58bdb8f32dae653ebffdf84e5bcdadad08f862ac2bf8306065537a3000000000000009f00000010000000300000007f00000009e8b9dd7f8c5f4ee2bb8d4546fcabd304434e3f958b3206f32dce98f9b35265000000004b000000100000003000000031000000d00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e31832301160000000001fd155e7c2e7348f93cdd5e4ab3922c68d859834d5935cf04746622869f8bc8b5609e38ff2b0ae9f7e591dd3c0f93772fd3e5faf22601000010000000300000005000000023cde495cfd125019140ea933e76e3cbf16189483232ab5a05d88fb34d1023e13f16e12dd7bb8b790fb4141c0bf6a17d33ee55919856e7f4bed103b30d9207f8d200000004000000ce00000010000000300000007f000000d7c32f5bc9b7b76efcbd455e58933dd449ccc7e7150e0f36fa71b44c7272362b000000004b000000100000003000000031000000d00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e31832301160000000001fd155e7c2e7348f93cdd5e4ab3922c68d859834d000000004b000000100000003000000031000000d00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e31832301160000000001fd155e7c2e7348f93cdd5e4ab3922c68d859834d"
      ]
    }"#;
}
