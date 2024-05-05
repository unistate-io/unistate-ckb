use anyhow::Ok;
use ckb_jsonrpc_types::TransactionView;
use ckb_types::H256;
use molecule::bytes::Buf;
use molecule::prelude::{Entity, Reader as _};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use sea_orm::DbConn;
use sea_orm::{prelude::ActiveModelTrait as _, ActiveValue::NotSet, EntityTrait, Set};
use tracing::debug;

use crate::schemas::spore_v1;
use crate::schemas::spore_v2;
use crate::schemas::top_level::WitnessLayoutReader;
use crate::schemas::top_level::WitnessLayoutUnionReader;
use crate::{entity, schemas::action};

pub async fn index_spore(
    db: &DbConn,
    tx: &TransactionView,
    timestamp: u64,
    network: ckb_sdk::NetworkType,
) -> anyhow::Result<()> {
    let actions = extract_actions(tx);

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

    for (action, (spore_data, cluster_data)) in actions.into_iter().zip(outpus.into_iter()) {
        upsert_spores(
            db,
            &action,
            spore_data.as_ref(),
            cluster_data.as_ref(),
            network,
            timestamp,
        )
        .await?;

        insert_action(
            db,
            action,
            spore_data.as_ref(),
            cluster_data.as_ref(),
            tx.hash.clone(),
            network,
            timestamp,
        )
        .await?;
    }

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
            action::SporeActionUnion::MintSpore(raw) => Some(raw.spore_id().as_bytes().to_vec()),
            action::SporeActionUnion::TransferSpore(raw) => {
                Some(raw.spore_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::BurnSpore(raw) => Some(raw.spore_id().as_bytes().to_vec()),
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
                Some(raw.cluster_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::TransferCluster(raw) => {
                Some(raw.cluster_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::MintProxy(raw) => Some(raw.cluster_id().as_bytes().to_vec()),
            action::SporeActionUnion::TransferProxy(raw) => {
                Some(raw.cluster_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.cluster_id().as_bytes().to_vec()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.cluster_id().as_bytes().to_vec()),
            action::SporeActionUnion::TransferAgent(raw) => {
                Some(raw.cluster_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::BurnAgent(raw) => Some(raw.cluster_id().as_bytes().to_vec()),
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
            action::SporeActionUnion::MintProxy(raw) => Some(raw.proxy_id().as_bytes().to_vec()),
            action::SporeActionUnion::TransferProxy(raw) => {
                Some(raw.cluster_id().as_bytes().to_vec())
            }
            action::SporeActionUnion::BurnProxy(raw) => Some(raw.proxy_id().as_bytes().to_vec()),
            action::SporeActionUnion::MintAgent(raw) => Some(raw.proxy_id().as_bytes().to_vec()),
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
            action::SporeActionUnion::MintSpore(raw) => Some(raw.data_hash().as_bytes().to_vec()),
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
    fn content_type_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.map(|data| data.content_type().as_bytes().to_vec())
    }

    fn content_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.map(|data| data.content().as_bytes().to_vec())
    }

    fn cluster_id_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.and_then(|data| data.cluster_id().to_opt())
            .map(|bytes| bytes.as_bytes().to_vec())
    }
}

impl spore_v2::ClusterDataV2 {
    fn description_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.map(|data| data.description().as_bytes().to_vec())
    }

    fn name_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.map(|data| data.name().as_bytes().to_vec())
    }

    fn mutant_id_op(op: Option<&Self>) -> Option<Vec<u8>> {
        op.and_then(|data| data.mutant_id().to_opt())
            .map(|bytes| bytes.as_bytes().to_vec())
    }
}

fn to_timestamp(timestamp: u64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .expect("Invalid timestamp")
        .naive_utc()
}

async fn upsert_address(
    db: &DbConn,
    address: &action::AddressUnion,
    network: ckb_sdk::NetworkType,
) -> anyhow::Result<()> {
    use entity::addresses;

    let address_id = address.to_string(network);

    let address_exists = addresses::Entity::find_by_id(&address_id)
        .one(db)
        .await?
        .is_some();

    if !address_exists {
        let script = address.script();
        // Insert address
        addresses::ActiveModel {
            id: Set(address_id),
            script_code_hash: Set(script.code_hash().as_bytes().to_vec()),
            script_hash_type: Set(script.hash_type().as_bytes().get_u8() as i16),
            script_args: Set(script.args().as_bytes().to_vec()),
        }
        .insert(db)
        .await?;
    }

    Ok(())
}

async fn upsert_spores(
    db: &DbConn,
    action: &action::SporeActionUnion,
    spore_data: Option<&spore_v1::SporeData>,
    cluster_data: Option<&spore_v2::ClusterDataV2>,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
) -> anyhow::Result<()> {
    let now = to_timestamp(timestamp);

    let spore_id = action.spore_id();

    let cluster_id = action.cluster_id(spore_data);

    use entity::{clusters, spores};

    if let Some(address) = action.to_address() {
        upsert_address(db, &address, network).await?;
    }

    let to = action.to_address_id(network);
    let is_burned = to.is_none();

    if let Some((cluster_id, cluster_data)) = cluster_id.zip(cluster_data) {
        let cluster_exists = clusters::Entity::find_by_id(cluster_id.clone())
            .one(db)
            .await?
            .is_some();

        if cluster_exists {
            // Update cluster
            clusters::ActiveModel {
                id: Set(cluster_id.clone()),
                owner_address: Set(to.clone()),
                cluster_name: Set(spore_v2::ClusterDataV2::name_op(Some(cluster_data))),
                cluster_description: Set(spore_v2::ClusterDataV2::description_op(Some(
                    cluster_data,
                ))),
                mutant_id: Set(spore_v2::ClusterDataV2::mutant_id_op(Some(cluster_data))),
                is_burned: Set(is_burned),
                updated_at: Set(now),
                ..Default::default()
            }
            .update(db)
            .await?;
        } else {
            // Insert cluster
            clusters::ActiveModel {
                id: Set(cluster_id.clone()),
                owner_address: Set(to.clone()),
                cluster_name: Set(spore_v2::ClusterDataV2::name_op(Some(cluster_data))),
                cluster_description: Set(spore_v2::ClusterDataV2::description_op(Some(
                    cluster_data,
                ))),
                mutant_id: Set(spore_v2::ClusterDataV2::mutant_id_op(Some(cluster_data))),
                is_burned: Set(is_burned),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(db)
            .await?;
        }
    }

    if let Some(spore_id) = spore_id {
        let spore_exists = spores::Entity::find_by_id(spore_id.clone())
            .one(db)
            .await?
            .is_some();

        if spore_exists {
            // Update spore
            spores::ActiveModel {
                id: Set(spore_id),
                owner_address: Set(to),
                content_type: Set(spore_v1::SporeData::content_type_op(spore_data)),
                content: Set(spore_v1::SporeData::content_op(spore_data)),
                cluster_id: Set(spore_v1::SporeData::cluster_id_op(spore_data)),
                is_burned: Set(is_burned),
                updated_at: Set(now),
                ..Default::default()
            }
            .update(db)
            .await?;
        } else {
            // Insert spore
            spores::ActiveModel {
                id: Set(spore_id),
                owner_address: Set(to),
                content_type: Set(spore_v1::SporeData::content_type_op(spore_data)),
                content: Set(spore_v1::SporeData::content_op(spore_data)),
                cluster_id: Set(spore_v1::SporeData::cluster_id_op(spore_data)),
                is_burned: Set(is_burned),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(db)
            .await?;
        }
    }

    Ok(())
}

async fn insert_action(
    db: &DbConn,
    action: action::SporeActionUnion,
    spore_data: Option<&spore_v1::SporeData>,
    cluster_data: Option<&spore_v2::ClusterDataV2>,
    tx: H256,
    network: ckb_sdk::NetworkType,
    timestamp: u64,
) -> anyhow::Result<()> {
    if let Some(from) = action.from_address() {
        upsert_address(db, &from, network).await?;
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
    }
    .insert(db)
    .await?;

    debug!("insert action: {:?}", action);

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
