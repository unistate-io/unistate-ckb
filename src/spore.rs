use ckb_jsonrpc_types::TransactionView;
use ckb_types::H256;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator as _, IntoParallelRefIterator, ParallelIterator,
};
use sea_orm::{ActiveValue::NotSet, Set};
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

pub struct SporeTx {
    pub tx: TransactionView,
    pub timestamp: u64,
}

pub struct SporeIndexer {
    txs: Vec<SporeTx>,
    network: ckb_sdk::NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl SporeIndexer {
    pub fn new(
        txs: Vec<SporeTx>,
        network: ckb_sdk::NetworkType,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Self {
        Self {
            txs,
            network,
            op_sender,
        }
    }

    pub fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            txs,
            network,
            op_sender,
        } = self;

        txs.into_par_iter()
            .try_for_each(|tx| index_spore(tx.tx, tx.timestamp, network, op_sender.clone()))?;

        Ok(())
    }
}

fn index_spore(
    tx: TransactionView,
    timestamp: u64,
    network: ckb_sdk::NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let actions = extract_actions(&tx);

    if actions.is_empty() {
        return Ok(());
    }

    debug!("tx: {}", hex::encode(tx.hash.as_bytes()));

    let outpus = tx
        .inner
        .outputs_data
        .par_iter()
        .filter_map(|output_data| {
            let spore_data_op =
                spore_v1::SporeDataReader::from_compatible_slice(output_data.as_bytes()).ok();
            let cluster_data_op =
                spore_v2::ClusterDataV2Reader::from_compatible_slice(output_data.as_bytes()).ok();
            if spore_data_op.is_none() && cluster_data_op.is_none() {
                None
            } else {
                Some((
                    spore_data_op.map(|reader| reader.to_entity()),
                    cluster_data_op.map(|reader| reader.to_entity()),
                ))
            }
        })
        .collect::<Vec<_>>();

    actions
        .into_par_iter()
        .zip(outpus.into_par_iter())
        .try_for_each(
            |(action, (spore_data, cluster_data))| -> anyhow::Result<()> {
                let (spore_data, cluster_data) =
                    extract_data(action.action_type(), spore_data, cluster_data);

                upsert_spores(
                    &action,
                    spore_data.as_ref(),
                    cluster_data.as_ref(),
                    network,
                    timestamp,
                    op_sender.clone(),
                )?;

                insert_action(
                    action,
                    spore_data.as_ref(),
                    cluster_data.as_ref(),
                    tx.hash.clone(),
                    network,
                    timestamp,
                    op_sender.clone(),
                )?;
                Ok(())
            },
        )?;

    Ok(())
}

fn extract_data(
    action: entity::sea_orm_active_enums::SporeActionType,
    spore_data: Option<spore_v1::SporeData>,
    cluster_data: Option<spore_v2::ClusterDataV2>,
) -> (Option<spore_v1::SporeData>, Option<spore_v2::ClusterDataV2>) {
    use entity::sea_orm_active_enums::SporeActionType::{BurnSpore, MintSpore, TransferSpore};
    match action {
        BurnSpore | MintSpore | TransferSpore => (spore_data, None),
        _ => (None, cluster_data),
    }
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

    fn cluster_id(&self, spore_data: Option<&spore_v1::SporeData>) -> Option<Vec<u8>> {
        let action_cluster_id = match self {
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
        };

        let spore_data_cluster_id = spore_v1::SporeData::cluster_id_op(spore_data);

        action_cluster_id.xor(spore_data_cluster_id)
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

    fn from_address_id(&self, network: ckb_sdk::NetworkType) -> Option<String> {
        self.from_address()
            .map(|address| address.to_string(network))
    }

    fn to_address_id(&self, network: ckb_sdk::NetworkType) -> Option<String> {
        self.to_address().map(|address| address.to_string(network))
    }

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
    fn content_type_op(op: Option<&Self>) -> Option<String> {
        debug!("content_type.");

        op.map(|data| data.content_type().into_string())
    }

    fn content_op(op: Option<&Self>) -> Option<Vec<u8>> {
        debug!("content.");

        op.map(|data| data.content().raw_data().to_vec())
    }

    fn cluster_id_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.and_then(|data| data.cluster_id().to_opt()).map(|bytes| {
            let id = bytes.raw_data().to_vec();

            debug!("cluster id: {}", hex::encode(&id));

            id
        })
    }
}

impl spore_v1::Bytes {
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
    fn description_op(op: Option<&Self>) -> Option<String> {
        debug!("description.");

        op.map(|data| data.description().into_string())
    }

    fn name_op(op: Option<&Self>) -> Option<String> {
        debug!("name.");

        op.map(|data| data.name().into_string())
    }

    fn mutant_id_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.and_then(|data| data.mutant_id().to_opt())
            .map(|bytes| bytes.raw_data().to_vec())
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
    action: &action::SporeActionUnion,
    spore_data: Option<&spore_v1::SporeData>,
    cluster_data: Option<&spore_v2::ClusterDataV2>,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let now = to_timestamp(timestamp);

    let spore_id = action.spore_id();

    let cluster_id = action.cluster_id(spore_data);

    use entity::{clusters, spores};

    if let Some(address) = action.to_address() {
        upsert_address(&address, network, op_sender.clone())?;
    }

    let to = action.to_address_id(network);
    let is_burned = to.is_none();

    if let Some((cluster_id, cluster_data)) = cluster_id.zip(cluster_data) {
        let model = clusters::ActiveModel {
            id: Set(cluster_id.clone()),
            owner_address: Set(to.clone()),
            cluster_name: Set(spore_v2::ClusterDataV2::name_op(Some(cluster_data))),
            cluster_description: Set(spore_v2::ClusterDataV2::description_op(Some(cluster_data))),
            mutant_id: Set(spore_v2::ClusterDataV2::mutant_id_op(Some(cluster_data))),
            is_burned: Set(is_burned),
            created_at: Set(now),
            updated_at: Set(now),
        };

        op_sender.send(Operations::UpsertCluster(model))?;
    }

    if let Some(spore_id) = spore_id {
        let model = spores::ActiveModel {
            id: Set(spore_id),
            owner_address: Set(to),
            content_type: Set(spore_v1::SporeData::content_type_op(spore_data)),
            content: Set(spore_v1::SporeData::content_op(spore_data)),
            cluster_id: Set(spore_v1::SporeData::cluster_id_op(spore_data)),
            is_burned: Set(is_burned),
            created_at: Set(now),
            updated_at: Set(now),
        };
        op_sender.send(Operations::UpsertSpores(model))?;
    }

    Ok(())
}

fn insert_action(
    action: action::SporeActionUnion,
    spore_data: Option<&spore_v1::SporeData>,
    cluster_data: Option<&spore_v2::ClusterDataV2>,
    tx: H256,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    if let Some(from) = action.from_address() {
        upsert_address(&from, network, op_sender.clone())?;
    }

    let action = entity::spore_actions::ActiveModel {
        id: NotSet,
        tx: Set(tx.0.to_vec()),
        action_type: Set(action.action_type()),
        spore_id: Set(action.spore_id()),
        cluster_id: Set(action.cluster_id(spore_data)),
        proxy_id: Set(action.proxy_id()),
        from_address_id: Set(action.from_address_id(network)),
        to_address_id: Set(action.to_address_id(network)),
        data_hash: Set(action.data_hash()),
        content_type: Set(spore_v1::SporeData::content_type_op(spore_data)),
        content: Set(spore_v1::SporeData::content_op(spore_data)),
        cluster_name: Set(spore_v2::ClusterDataV2::name_op(cluster_data)),
        cluster_description: Set(spore_v2::ClusterDataV2::description_op(cluster_data)),
        mutant_id: Set(spore_v2::ClusterDataV2::mutant_id_op(cluster_data)),
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

    spore_action
}
