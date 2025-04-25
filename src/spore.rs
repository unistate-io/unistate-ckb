use ckb_jsonrpc_types::TransactionView;
use ckb_types::{H256, packed};
use molecule::{
    bytes::Buf,
    prelude::{Entity as _, Reader as _},
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator as _, IntoParallelRefIterator, ParallelIterator,
};
use sea_orm::Set;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{
    database::Operations,
    entity::{self, addresses},
    schemas::{
        action, spore_v1, spore_v2,
        top_level::{WitnessLayoutReader, WitnessLayoutUnionReader},
    },
};

use constants::Constants;

pub struct SporeTx {
    pub tx: TransactionView,
    pub timestamp: u64,
}

pub struct SporeIndexer {
    txs: Vec<SporeTx>,
    network: utils::network::NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl SporeIndexer {
    pub fn new(
        txs: Vec<SporeTx>,
        network: utils::network::NetworkType,
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
    network: utils::network::NetworkType,
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
    fn from_address_id(&self, network: utils::network::NetworkType) -> Option<String> {
        self.from_address()
            .and_then(|address| address.to_string(network))
    }

    fn to_address_id(&self, network: utils::network::NetworkType) -> Option<String> {
        self.to_address()
            .and_then(|address| address.to_string(network))
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

    fn to_string(&self, network: utils::network::NetworkType) -> Option<String> {
        let script_bytes = self.script().as_bytes();
        match utils::address::script_bytes_to_address(script_bytes.as_ref(), network) {
            Ok(s) => Some(s),
            Err(e) => {
                error!("Failed to convert address: {:?}", e);
                None
            }
        }
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
    network: utils::network::NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<String> {
    let Some(address_id) = address.to_string(network) else {
        return Err(anyhow::anyhow!(
            "Failed to convert script to address string. Script bytes: {:?}, Network: {:?}",
            address.script().as_bytes(),
            network
        ));
    };

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
    network: utils::network::NetworkType,
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
    network: utils::network::NetworkType,
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
