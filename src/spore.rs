use ckb_jsonrpc_types::TransactionView;
use ckb_types::{packed, H256};
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator as _, IntoParallelRefIterator, ParallelIterator,
};
use sea_orm::Set;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{
    constants::Constants,
    database::Operations,
    entity::{self, addresses},
    schemas::{
        action, spore_v1, spore_v2,
        top_level::{WitnessLayoutReader, WitnessLayoutUnionReader},
    },
};

pub struct SporeTx {
    pub tx: TransactionView,
    pub timestamp: u64,
}

pub struct SporeIndexer {
    txs: Vec<SporeTx>,
    network: ckb_sdk::NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl SporeIndexer {
    pub fn new(
        txs: Vec<SporeTx>,
        network: ckb_sdk::NetworkType,
        constants: Constants,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Self {
        Self {
            txs,
            network,
            constants,
            op_sender,
        }
    }

    pub fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            txs,
            network,
            constants,
            op_sender,
        } = self;

        txs.into_par_iter().try_for_each(|tx| {
            index_spore(tx.tx, tx.timestamp, network, op_sender.clone(), constants)
        })?;

        Ok(())
    }
}

enum DataVariant {
    Spore(spore_v1::SporeData),
    ClusterV2(spore_v2::ClusterDataV2),
    ClusterV1(spore_v1::ClusterData),
}

struct Data {
    id: Vec<u8>,
    type_id: action::AddressUnion,
    to: action::AddressUnion,
    variant: DataVariant,
}

fn index_spore(
    tx: TransactionView,
    timestamp: u64,
    network: ckb_sdk::NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
    constants: Constants,
) -> anyhow::Result<()> {
    debug!("tx: {}", hex::encode(tx.hash.as_bytes()));

    tx.inner
        .outputs_data
        .par_iter()
        .zip(tx.inner.outputs.par_iter())
        .filter_map(|(output_data, output_cell)| {
            output_cell
                .type_
                .as_ref()
                .and_then(|s| s.args.as_bytes().get(..32).map(|v| v.to_vec()))
                .zip(output_cell.type_.as_ref())
                .and_then(|(id, type_script)| {
                    if constants.is_spore_type(type_script) {
                        debug!("is spore!");
                        spore_v1::SporeDataReader::from_compatible_slice(output_data.as_bytes())
                            .ok()
                            .map(|reader| reader.to_entity())
                            .map(DataVariant::Spore)
                    } else if constants.is_cluster_type(type_script) {
                        debug!("is cluster!");
                        spore_v2::ClusterDataV2Reader::from_compatible_slice(output_data.as_bytes())
                            .ok()
                            .map(|reader| reader.to_entity())
                            .map(DataVariant::ClusterV2)
                            .or(spore_v1::ClusterDataReader::from_compatible_slice(
                                output_data.as_bytes(),
                            )
                            .ok()
                            .map(|reader| reader.to_entity())
                            .map(DataVariant::ClusterV1))
                    } else {
                        None
                    }
                    .map(|variant| Data {
                        variant,
                        to: action::AddressUnion::from_json_script(output_cell.lock.clone()),
                        type_id: action::AddressUnion::from_json_script(type_script.clone()),
                        id,
                    })
                })
        })
        .try_for_each(|data| upsert_spores(data, network, timestamp, op_sender.clone()))?;

    extract_actions(&tx)
        .into_par_iter()
        .try_for_each(|action| {
            insert_action(
                action,
                tx.hash.clone(),
                network,
                timestamp,
                op_sender.clone(),
            )
        })?;

    Ok(())
}

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
            action::SporeActionUnion::MintCluster(_) => None,
            action::SporeActionUnion::TransferCluster(_) => None,
            action::SporeActionUnion::MintProxy(_) => None,
            action::SporeActionUnion::TransferProxy(_) => None,
            action::SporeActionUnion::BurnProxy(_) => None,
            action::SporeActionUnion::MintAgent(_) => None,
            action::SporeActionUnion::TransferAgent(_) => None,
            action::SporeActionUnion::BurnAgent(_) => None,
        }
    }

    fn cluster_id(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintSpore(_) => None,
            action::SporeActionUnion::TransferSpore(_) => None,
            action::SporeActionUnion::BurnSpore(_) => None,
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
        }
    }

    fn proxy_id(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintSpore(_) => None,
            action::SporeActionUnion::TransferSpore(_) => None,
            action::SporeActionUnion::BurnSpore(_) => None,
            action::SporeActionUnion::MintCluster(_) => None,
            action::SporeActionUnion::TransferCluster(_) => None,
            action::SporeActionUnion::MintProxy(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferProxy(raw) => {
                Some(raw.cluster_id().raw_data().to_vec())
            }
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.proxy_id().raw_data().to_vec()),
            action::SporeActionUnion::TransferAgent(_) => None,
            action::SporeActionUnion::BurnAgent(_) => None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_address_id(&self, network: ckb_sdk::NetworkType) -> Option<String> {
        self.from_address()
            .map(|address| address.to_string(network))
    }

    fn to_address_id(&self, network: ckb_sdk::NetworkType) -> Option<String> {
        self.to_address().map(|address| address.to_string(network))
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_address(&self) -> Option<action::AddressUnion> {
        match self {
            action::SporeActionUnion::MintSpore(_) => None,
            action::SporeActionUnion::TransferSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::MintCluster(_) => None,
            action::SporeActionUnion::TransferCluster(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::MintProxy(_) => None,
            action::SporeActionUnion::TransferProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::MintAgent(_) => None,
            action::SporeActionUnion::TransferAgent(raw) => Some(raw.from().to_enum()),
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.from().to_enum()),
        }
    }

    fn to_address(&self) -> Option<action::AddressUnion> {
        match self {
            action::SporeActionUnion::MintSpore(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferSpore(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::BurnSpore(_) => None,
            action::SporeActionUnion::MintCluster(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferCluster(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::MintProxy(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferProxy(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::BurnProxy(_) => None,
            action::SporeActionUnion::MintAgent(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::TransferAgent(raw) => Some(raw.to().to_enum()),
            action::SporeActionUnion::BurnAgent(_) => None,
        }
    }

    fn data_hash(&self) -> Option<Vec<u8>> {
        match self {
            action::SporeActionUnion::MintSpore(raw) => Some(raw.data_hash().raw_data().to_vec()),
            action::SporeActionUnion::TransferSpore(_) => None,
            action::SporeActionUnion::BurnSpore(_) => None,
            action::SporeActionUnion::MintCluster(_) => None,
            action::SporeActionUnion::TransferCluster(_) => None,
            action::SporeActionUnion::MintProxy(_) => None,
            action::SporeActionUnion::TransferProxy(_) => None,
            action::SporeActionUnion::BurnProxy(_) => None,
            action::SporeActionUnion::MintAgent(_) => None,
            action::SporeActionUnion::TransferAgent(_) => None,
            action::SporeActionUnion::BurnAgent(_) => None,
        }
    }
}

impl action::AddressUnion {
    pub fn from_json_script(script: ckb_jsonrpc_types::Script) -> Self {
        use ckb_types::prelude::Entity;
        Self::Script(action::Script::new_unchecked(
            packed::Script::from(script).as_bytes(),
        ))
    }
    fn to_string(&self, network: ckb_sdk::NetworkType) -> String {
        use ckb_types::prelude::Entity as _;
        let script = ckb_gen_types::packed::Script::new_unchecked(self.script().as_bytes());
        let addr = ckb_sdk::AddressPayload::from(script);
        addr.display_with_network(network, true)
    }

    fn script(&self) -> &action::Script {
        match self {
            action::AddressUnion::Script(script) => script,
        }
    }
}

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
        let res = String::from_utf8(self.raw_data().to_vec());
        match res {
            Ok(s) => s,
            Err(e) => {
                error!("into_string error: {e:?}");
                String::new()
            }
        }
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

fn to_timestamp(timestamp: u64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .expect("Invalid timestamp")
        .naive_utc()
}

pub fn upsert_address(
    address: &action::AddressUnion,
    network: ckb_sdk::NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<String> {
    let address_id = address.to_string(network);

    let script = address.script();
    // Insert address
    let address = addresses::ActiveModel {
        id: Set(address_id.clone()),
        script_code_hash: Set(script.code_hash().raw_data().to_vec()),
        script_hash_type: Set(script.hash_type().as_bytes().get_u8() as i16),
        script_args: Set(script.args().raw_data().to_vec()),
    };

    op_sender.send(Operations::UpsertAddress(address))?;

    Ok(address_id)
}

fn upsert_spores(
    data: Data,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    use entity::{clusters, spores};

    let now = to_timestamp(timestamp);

    let Data {
        id,
        type_id,
        to,
        variant,
    } = data;
    let type_id = upsert_address(&type_id, network, op_sender.clone())?;
    let to = upsert_address(&to, network, op_sender.clone())?;

    match variant {
        DataVariant::Spore(spore_data) => {
            let model = spores::ActiveModel {
                id: Set(id),
                owner_address: Set(Some(to)),
                content_type: Set(spore_data.content_type_op()),
                content: Set(spore_data.content_op()),
                cluster_id: Set(spore_data.cluster_id_op()),
                is_burned: Set(false),
                created_at: Set(now),
                updated_at: Set(now),
                type_id: Set(type_id),
            };
            op_sender.send(Operations::UpsertSpores(model))?;
        }
        DataVariant::ClusterV2(cluster_data) => {
            let model = clusters::ActiveModel {
                id: Set(id),
                owner_address: Set(Some(to)),
                cluster_name: Set(cluster_data.name_op()),
                cluster_description: Set(cluster_data.description_op()),
                mutant_id: Set(cluster_data.mutant_id_op()),
                is_burned: Set(false),
                created_at: Set(now),
                updated_at: Set(now),
                type_id: Set(type_id),
            };

            op_sender.send(Operations::UpsertCluster(model))?;
        }
        DataVariant::ClusterV1(cluster_data) => {
            let model = clusters::ActiveModel {
                id: Set(id),
                owner_address: Set(Some(to)),
                cluster_name: Set(cluster_data.name_op()),
                cluster_description: Set(cluster_data.description_op()),
                mutant_id: Set(None),
                is_burned: Set(false),
                created_at: Set(now),
                updated_at: Set(now),
                type_id: Set(type_id),
            };

            op_sender.send(Operations::UpsertCluster(model))?;
        }
    }

    Ok(())
}

fn insert_action(
    action: action::SporeActionUnion,
    tx: H256,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    if let Some(from) = action.from_address() {
        upsert_address(&from, network, op_sender.clone())?;
    }

    let action = entity::spore_actions::ActiveModel {
        tx: Set(tx.0.to_vec()),
        action_type: Set(action.action_type()),
        spore_id: Set(action.spore_id()),
        cluster_id: Set(action.cluster_id()),
        proxy_id: Set(action.proxy_id()),
        from_address_id: Set(action.from_address_id(network)),
        to_address_id: Set(action.to_address_id(network)),
        data_hash: Set(action.data_hash()),
        created_at: Set(to_timestamp(timestamp)),
    };

    debug!("insert action: {:?}", action);

    op_sender.send(Operations::UpsertActions(action))?;

    Ok(())
}

fn extract_actions(tx: &ckb_jsonrpc_types::TransactionView) -> Vec<action::SporeActionUnion> {
    let msgs = tx
        .inner
        .witnesses
        .par_iter()
        .filter_map(|witness| {
            WitnessLayoutReader::from_slice(witness.as_bytes())
                .ok()
                .and_then(|r| match r.to_enum() {
                    WitnessLayoutUnionReader::SighashAll(s) => Some(s.message().to_entity()),
                    _ => None,
                })
        })
        .collect::<Vec<_>>();

    debug!("msgs: {msgs:?}");

    let spore_action = msgs
        .par_iter()
        .map(|msg| {
            msg.actions().into_iter().filter_map(|action| {
                action::SporeActionReader::from_slice(&action.data().raw_data())
                    .ok()
                    .map(|reader| reader.to_entity().to_enum())
            })
        })
        .flatten_iter()
        .collect::<Vec<_>>();

    debug!("spore_actions: {spore_action:?}");

    spore_action
}

#[cfg(test)]
mod tests {
    use ckb_jsonrpc_types::TransactionView;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::{constants::Constants, spore::index_spore};

    #[test]
    #[tracing_test::traced_test]
    fn debug_index_spore() {
        let tx: TransactionView = serde_json::from_str(TEST_JSON).expect("deserialize failed.");
        debug!("tx: {tx:?}");
        let (sender, mut recver) = mpsc::unbounded_channel();
        index_spore(
            tx,
            0,
            ckb_sdk::NetworkType::Testnet,
            sender,
            Constants::Testnet,
        )
        .unwrap();
        while let Some(op) = recver.blocking_recv() {
            debug!("op: {op:?}");
        }
        debug!("done!");
    }

    const TEST_JSON: &'static str = r#"
    {
        "cell_deps": [
            {
            "dep_type": "code",
            "out_point": {
                "index": "0x0",
                "tx_hash": "0xfbceb70b2e683ef3a97865bb88e082e3e5366ee195a9c826e3c07d1026792fcd"
            }
            },
            {
            "dep_type": "dep_group",
            "out_point": {
                "index": "0x0",
                "tx_hash": "0xf8de3bb47d055cdf460d93a2a6e1b05f7432f9777c8c474abf4eec1d4aee5d37"
            }
            }
        ],
        "hash": "0x9b92dfee04c23944fa7d9e7b603d647113eb26f7a72c36c82336020fa7ffef54",
        "header_deps": [],
        "inputs": [
            {
            "previous_output": {
                "index": "0x0",
                "tx_hash": "0xe11d4528ef96b9b77379085af0a0adc0cacfba86ee0cdf9d3a2789af414f433e"
            },
            "since": "0x0"
            },
            {
            "previous_output": {
                "index": "0x2",
                "tx_hash": "0x10103972c03f93c5b1ccf9248e6c56f8d86328c7a5770e60f2572fe30b480e4f"
            },
            "since": "0x0"
            },
            {
            "previous_output": {
                "index": "0x0",
                "tx_hash": "0x3522355b8304a240d3222647a28cf535094b0ab4b82c31dfbc5b2d3c7adc1a0f"
            },
            "since": "0x0"
            }
        ],
        "outputs": [
            {
            "capacity": "0x41f009100",
            "lock": {
                "args": "0x6cd8ae51f91bacd7910126f880138b30ac5d3015",
                "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
                "hash_type": "type"
            },
            "type": {
                "args": "0xbb25fc0ac36dcaa47de2e4971d8a59d9f9f601b70bb35fee821ddc29d72e5ed3",
                "code_hash": "0x7366a61534fa7c7e6225ecc0d828ea3b5366adec2b58206f2ee84995fe030075",
                "hash_type": "data1"
            }
            },
            {
            "capacity": "0x46686f3ae",
            "lock": {
                "args": "0x6cd8ae51f91bacd7910126f880138b30ac5d3015",
                "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
                "hash_type": "type"
            },
            "type": null
            }
        ],
        "outputs_data": [
            "0x320000001000000022000000320000000e000000546573746e65742053706f7265730c00000054657374696e67206f6e6c79",
            "0x"
        ],
        "version": "0x0",
        "witnesses": [
            "0x5500000010000000550000005500000041000000bc293371bd6e82d61a3348a9e4f60ec20083b8ff8a1de67fa144dafd06ff10f1256ec7a83406b716854ff043789d70add0102d402b6d124ce4280035a810f6d801",
            "0x",
            "0x",
            "0x010000ff150100000c00000010000000000000000501000008000000fd00000008000000f50000001000000030000000500000009d6307a36896705fec8c8965792a1a57a9f299f9927d27db444bb4a1cd43a85e1c90a05f434d40898da7f3cf88f77b0eeacdf16e5ac62a13d220c567a3482b83a1000000030000009d00000010000000300000007d000000bb25fc0ac36dcaa47de2e4971d8a59d9f9f601b70bb35fee821ddc29d72e5ed300000000490000001000000030000000310000009bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce801140000006cd8ae51f91bacd7910126f880138b30ac5d3015755814a280af21d1105e863be04387215227ad4f6aca47e6a9a3932b00d9bcd2"
        ]
    }"#;
}
