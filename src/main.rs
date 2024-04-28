use core::time::Duration;

use async_std::task::sleep;
use ckb_sdk::{rpc::ResponseFormatGetter, CkbRpcClient};

use constants::mainnet_info::{
    get_cluster_type_script, get_spore_type_script, get_unique_type_script, get_xudttype_script,
    is_rgbpp_lock_script, CLUSTER_TYPE_SCRIPT, RGBPP_LOCK_DEP, SPORE_TYPE_SCRIPT,
    START_RGBPP_HEIGHT, XUDTTYPE_DEP,
};

use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader},
};
use schemas::{
    action,
    blockchain::WitnessArgsReader,
    spore_v1::SporeDataReader,
    top_level::{WitnessLayoutReader, WitnessLayoutUnionReader},
};
use sea_orm::{ActiveModelTrait, ActiveValue::NotSet, ConnectOptions, Database, Set};

use crate::schemas::spore_v2::ClusterDataV2Reader;

mod constants;
mod entity;
mod schemas;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opt =
        ConnectOptions::new("postgres://unistate_dev:unistate_dev@localhost:5432/unistate_dev");
    let db = Database::connect(opt).await?;
    // let client = CkbRpcClient::new("http://127.0.0.1:8114");
    let client = CkbRpcClient::new("https://rpc.ankr.com/nervos_ckb/0b0417ab9bc09b426a1e617d9761d5262610241ee275708775ceba0d7f6e99c9");
    let mut height = START_RGBPP_HEIGHT;
    // let mut height = 12826479u64;

    loop {
        let block = client.get_block_by_number(height.into())?;
        match block {
            Some(block) => {
                for tx in block.transactions {
                    if tx
                        .inner
                        .cell_deps
                        .iter()
                        .find(|cd| {
                            cd.out_point
                                .tx_hash
                                .as_bytes()
                                .eq(RGBPP_LOCK_DEP.out_point.tx_hash.as_bytes())
                        })
                        .is_none()
                    {
                        continue;
                    }
                    // 暂时过滤掉xudt
                    if tx
                        .inner
                        .cell_deps
                        .iter()
                        .find(|cd| {
                            cd.out_point
                                .tx_hash
                                .as_bytes()
                                .eq(XUDTTYPE_DEP.out_point.tx_hash.as_bytes())
                        })
                        .is_some()
                    {
                        continue;
                    }
                    let mut rgbpp_unlock_op = None;
                    for (index, i) in tx.inner.inputs.iter().enumerate() {
                        if i.previous_output.tx_hash.as_bytes().iter().all(|i| 0.eq(i)) {
                            continue;
                        }
                        println!("hash: {:?}", tx.hash.to_string());

                        let Some(pre_tx) = client
                            .get_transaction(i.previous_output.tx_hash.clone())?
                            .and_then(|t| t.transaction)
                            .and_then(|t| t.get_value().ok())
                        else {
                            println!(
                                "not found previous output tx: {}",
                                i.previous_output.tx_hash.to_string()
                            );
                            continue;
                        };

                        let idx = i.previous_output.index.value() as usize;

                        let pre_output = pre_tx.inner.outputs.get(idx).unwrap();

                        if is_rgbpp_lock_script(&pre_output.lock) {
                            // rgbpp
                            println!(
                                "input rgbpp_args: {}",
                                hex::encode(pre_output.lock.args.as_bytes())
                            );
                            let rgbpp_lock = schemas::rgbpp::RGBPPLockReader::from_slice(
                                pre_output.lock.args.as_bytes(),
                            )?;

                            let rgbpp_lock = entity::rgbpp_locks::ActiveModel {
                                lock_id: NotSet,
                                btc_txid: Set(rgbpp_lock.btc_txid().raw_data().to_vec()),
                                out_index: Set(rgbpp_lock.out_index().raw_data().get_u32() as i32),
                            }
                            .insert(&db)
                            .await?;

                            if let Some(witness_args) =
                                pre_tx.inner.witnesses.get(index).and_then(|witness| {
                                    WitnessArgsReader::from_slice(witness.as_bytes()).ok()
                                })
                            {
                                let rgbpp_unlock = witness_args.lock().to_opt().map(|args| {
                                    schemas::rgbpp::RGBPPUnlockReader::from_slice(
                                        args.raw_data().as_ref(),
                                    )
                                });

                                match rgbpp_unlock {
                                    Some(Ok(rgbpp_unlock)) => {
                                        println!("input rgbpp_unlock: {rgbpp_unlock:#}");
                                        let extra_data = rgbpp_unlock.extra_data().to_entity();
                                        let rgbpp_unlock = entity::rgbpp_unlocks::ActiveModel {
                                            unlock_id: NotSet,
                                            version: Set(
                                                rgbpp_unlock.version().raw_data().get_u16() as i16,
                                            ),
                                            input_len: Set(extra_data
                                                .input_len()
                                                .as_bytes()
                                                .get_i16()),
                                            output_len: Set(extra_data
                                                .output_len()
                                                .as_bytes()
                                                .get_i16()),
                                            btc_tx: Set(rgbpp_unlock.btc_tx().raw_data().to_vec()),
                                            btc_tx_proof: Set(rgbpp_unlock
                                                .btc_tx_proof()
                                                .raw_data()
                                                .to_vec()),
                                        }
                                        .insert(&db)
                                        .await?;
                                        rgbpp_unlock_op = Some(rgbpp_unlock.unlock_id);
                                    }
                                    Some(Err(e)) => eprintln!("rgbpp_unlock error: {e:?}"),
                                    None => eprintln!("not found rgbpp_unlock"),
                                }
                            };

                            if let Some(xudt) = get_xudttype_script(pre_output.type_.as_ref()) {
                                // todo
                                println!("xudt!");
                            } else if let Some(spore) =
                                get_spore_type_script(pre_output.type_.as_ref())
                            {
                                // todo
                                println!("spore!");

                                let raw_spore_data = &pre_tx.inner.outputs_data[idx];
                                let spore_data = SporeDataReader::from_compatible_slice(
                                    raw_spore_data.as_bytes(),
                                );

                                match spore_data {
                                    Ok(spore_data) => println!("input spore_data: {spore_data:#}"),
                                    Err(e) => eprintln!("spore_data error: {e:?}"),
                                }

                                let spore_action = extract_spore_action(&tx, SPORE_TYPE_SCRIPT);
                                match spore_action {
                                    Some(action::SporeActionUnion::BurnSpore(burn)) => {
                                        println!("burn spore: {burn:#}");
                                        let action::AddressUnion::Script(owner) =
                                            burn.from().to_enum();
                                        let owner = entity::addresses::ActiveModel {
                                            address_id: Set(owner.as_bytes().to_vec()),
                                            script_code_hash: Set(owner
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(Into::<u8>::into(
                                                owner.hash_type(),
                                            )
                                                as i16),
                                            script_args: Set(Some(
                                                owner.args().raw_data().to_vec(),
                                            )),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let timestamp = chrono::DateTime::from_timestamp_millis(
                                            block.header.inner.timestamp.value() as i64,
                                        )
                                        .unwrap()
                                        .naive_utc();

                                        let spore = entity::spores::ActiveModel {
                                            spore_id: Set(burn.spore_id().raw_data().to_vec()),
                                            is_burned: Set(true),
                                            updated_at: Set(timestamp),
                                            ..Default::default()
                                        }
                                        .insert(&db)
                                        .await?;

                                        let action = entity::spore_actions::ActiveModel {
                                            action_id: NotSet,
                                            // 0代表mint
                                            action_type: Set(3),
                                            spore_id: Set(spore.spore_id),
                                            from_address_id: Set(Some(
                                                owner.address_id.as_ref().clone(),
                                            )),
                                            to_address_id: Set(None),
                                            created_at: Set(timestamp),
                                            rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id)),
                                            rgbpp_unlock_id: Set(rgbpp_unlock_op.clone()),
                                        }
                                        .insert(&db)
                                        .await?;
                                    }
                                    Some(unknow_action) => {
                                        eprintln!("Unknow spore action: {unknow_action:#}");
                                    }
                                    None => {
                                        eprintln!("Not found any action.");
                                    }
                                }
                            } else if let Some(cluster) =
                                get_cluster_type_script(pre_output.type_.as_ref())
                            {
                                println!("cluster!");

                                // todo
                            } else if let Some(unique) =
                                get_unique_type_script(pre_output.type_.as_ref())
                            {
                                println!("unique!");

                                // todo
                            }
                        }
                    }

                    for (idx, output) in tx.inner.outputs.iter().enumerate() {
                        if is_rgbpp_lock_script(&output.lock) {
                            // rgbpp
                            let rgbpp_lock =
                                schemas::rgbpp::RGBPPLockReader::from_compatible_slice(
                                    output.lock.args.as_bytes(),
                                )?;

                            let rgbpp_lock = entity::rgbpp_locks::ActiveModel {
                                lock_id: NotSet,
                                btc_txid: Set(rgbpp_lock.btc_txid().raw_data().to_vec()),
                                out_index: Set(rgbpp_lock.out_index().raw_data().get_u32() as i32),
                            }
                            .insert(&db)
                            .await?;

                            if let Some(xudt) = get_xudttype_script(output.type_.as_ref()) {
                                // todo
                                println!("xudt!");
                            } else if let Some(spore) = get_spore_type_script(output.type_.as_ref())
                            {
                                // todo
                                println!("spore!");

                                let raw_spore_data = &tx.inner.outputs_data[idx];
                                let spore_data = SporeDataReader::from_compatible_slice(
                                    raw_spore_data.as_bytes(),
                                )?;

                                let spore_action = extract_spore_action(&tx, SPORE_TYPE_SCRIPT);
                                match spore_action {
                                    Some(action::SporeActionUnion::MintSpore(mint)) => {
                                        println!("mint spore: {mint:#}");
                                        let action::AddressUnion::Script(owner) =
                                            mint.to().to_enum();
                                        let owner = entity::addresses::ActiveModel {
                                            address_id: Set(owner.as_bytes().to_vec()),
                                            script_code_hash: Set(owner
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(Into::<u8>::into(
                                                owner.hash_type(),
                                            )
                                                as i16),
                                            script_args: Set(Some(
                                                owner.args().raw_data().to_vec(),
                                            )),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let timestamp = chrono::DateTime::from_timestamp_millis(
                                            block.header.inner.timestamp.value() as i64,
                                        )
                                        .unwrap()
                                        .naive_utc();

                                        let spore = entity::spores::ActiveModel {
                                            spore_id: Set(mint.spore_id().raw_data().to_vec()),
                                            content_type: Set(spore_data
                                                .content_type()
                                                .raw_data()
                                                .to_vec()),
                                            content: Set(spore_data.content().raw_data().to_vec()),
                                            cluster_id: Set(spore_data
                                                .cluster_id()
                                                .to_opt()
                                                .map(|b| b.raw_data().to_vec())),
                                            owner_address_id: Set(owner
                                                .address_id
                                                .as_ref()
                                                .clone()),
                                            data_hash: Set(mint.data_hash().raw_data().to_vec()),
                                            created_at: Set(timestamp),
                                            updated_at: Set(timestamp),
                                            is_burned: Set(false),
                                        }
                                        .insert(&db)
                                        .await?;

                                        let action = entity::spore_actions::ActiveModel {
                                            action_id: NotSet,
                                            // 0代表mint
                                            action_type: Set(0),
                                            spore_id: Set(spore.spore_id),
                                            from_address_id: Set(None),
                                            to_address_id: Set(Some(
                                                owner.address_id.as_ref().clone(),
                                            )),
                                            created_at: Set(timestamp),
                                            rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id)),
                                            rgbpp_unlock_id: Set(rgbpp_unlock_op.clone()),
                                        }
                                        .insert(&db)
                                        .await?;
                                    }
                                    Some(action::SporeActionUnion::TransferSpore(transfer)) => {
                                        println!("transfer spore: {transfer:#}");

                                        let action::AddressUnion::Script(from) =
                                            transfer.from().to_enum();
                                        let action::AddressUnion::Script(to) =
                                            transfer.to().to_enum();
                                        let from = entity::addresses::ActiveModel {
                                            address_id: Set(from.as_bytes().to_vec()),
                                            script_code_hash: Set(from
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(
                                                Into::<u8>::into(from.hash_type()) as i16,
                                            ),
                                            script_args: Set(Some(from.args().raw_data().to_vec())),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let to = entity::addresses::ActiveModel {
                                            address_id: Set(to.as_bytes().to_vec()),
                                            script_code_hash: Set(to
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(
                                                Into::<u8>::into(to.hash_type()) as i16
                                            ),
                                            script_args: Set(Some(to.args().raw_data().to_vec())),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let timestamp = chrono::DateTime::from_timestamp_millis(
                                            block.header.inner.timestamp.value() as i64,
                                        )
                                        .unwrap()
                                        .naive_utc();

                                        let spore = entity::spores::ActiveModel {
                                            spore_id: Set(transfer.spore_id().raw_data().to_vec()),
                                            content_type: Set(spore_data
                                                .content_type()
                                                .raw_data()
                                                .to_vec()),
                                            content: Set(spore_data.content().raw_data().to_vec()),
                                            cluster_id: Set(spore_data
                                                .cluster_id()
                                                .to_opt()
                                                .map(|b| b.raw_data().to_vec())),
                                            owner_address_id: Set(to.address_id.as_ref().clone()),
                                            updated_at: Set(timestamp),
                                            ..Default::default()
                                        }
                                        .update(&db)
                                        .await?;

                                        let action = entity::spore_actions::ActiveModel {
                                            action_id: NotSet,
                                            // 0代表mint
                                            action_type: Set(1),
                                            spore_id: Set(spore.spore_id),
                                            from_address_id: Set(Some(
                                                from.address_id.as_ref().clone(),
                                            )),
                                            to_address_id: Set(Some(
                                                to.address_id.as_ref().clone(),
                                            )),
                                            created_at: Set(timestamp),
                                            rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id)),
                                            rgbpp_unlock_id: Set(rgbpp_unlock_op.clone()),
                                        }
                                        .insert(&db)
                                        .await?;
                                    }
                                    Some(unknow_action) => {
                                        eprintln!("Unknow spore action: {unknow_action:#}");
                                    }
                                    None => {
                                        eprintln!("Not found any action.");
                                    }
                                }
                            } else if let Some(cluster) =
                                get_cluster_type_script(output.type_.as_ref())
                            {
                                // todo
                                println!("cluster!");

                                let raw_cluster_data = &tx.inner.outputs_data[idx];
                                let cluster_data = ClusterDataV2Reader::from_compatible_slice(
                                    raw_cluster_data.as_bytes(),
                                )
                                .unwrap();

                                println!("clusfer data: {cluster_data}");

                                let spore_action = extract_spore_action(&tx, CLUSTER_TYPE_SCRIPT);
                                match spore_action {
                                    Some(action::SporeActionUnion::MintCluster(mint)) => {
                                        let action::AddressUnion::Script(owner) =
                                            mint.to().to_enum();
                                        let owner = entity::addresses::ActiveModel {
                                            script_code_hash: Set(owner
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(Into::<u8>::into(
                                                owner.hash_type(),
                                            )
                                                as i16),
                                            script_args: Set(Some(
                                                owner.args().raw_data().to_vec(),
                                            )),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let timestamp = chrono::DateTime::from_timestamp_millis(
                                            block.header.inner.timestamp.value() as i64,
                                        )
                                        .unwrap()
                                        .naive_utc();

                                        let cluster = entity::clusters::ActiveModel {
                                            cluster_id: Set(mint.cluster_id().raw_data().to_vec()),
                                            owner_address_id: Set(owner
                                                .address_id
                                                .as_ref()
                                                .clone()),
                                            data_hash: Set(mint.data_hash().raw_data().to_vec()),
                                            name: Set(cluster_data.name().raw_data().to_vec()),
                                            description: Set(cluster_data
                                                .description()
                                                .raw_data()
                                                .to_vec()),
                                            mutant_id: Set(cluster_data
                                                .mutant_id()
                                                .to_opt()
                                                .map(|m| m.raw_data().to_vec())),
                                            created_at: Set(timestamp),
                                            updated_at: Set(timestamp),
                                        }
                                        .insert(&db)
                                        .await?;

                                        let action = entity::cluster_actions::ActiveModel {
                                            action_id: NotSet,
                                            // 0代表mint
                                            action_type: Set(0),
                                            cluster_id: Set(cluster.cluster_id),
                                            from_address_id: Set(None),
                                            to_address_id: Set(Some(
                                                owner.address_id.as_ref().clone(),
                                            )),
                                            created_at: Set(timestamp),
                                            rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id)),
                                            rgbpp_unlock_id: Set(rgbpp_unlock_op.clone()),
                                        }
                                        .insert(&db)
                                        .await?;

                                        println!("mint cluster: {mint:#}");
                                    }
                                    Some(action::SporeActionUnion::TransferCluster(transfer)) => {
                                        let action::AddressUnion::Script(from) =
                                            transfer.from().to_enum();
                                        let action::AddressUnion::Script(to) =
                                            transfer.to().to_enum();
                                        let from = entity::addresses::ActiveModel {
                                            script_code_hash: Set(from
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(
                                                Into::<u8>::into(from.hash_type()) as i16,
                                            ),
                                            script_args: Set(Some(from.args().raw_data().to_vec())),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let to = entity::addresses::ActiveModel {
                                            script_code_hash: Set(to
                                                .code_hash()
                                                .raw_data()
                                                .to_vec()),
                                            script_hash_type: Set(
                                                Into::<u8>::into(to.hash_type()) as i16
                                            ),
                                            script_args: Set(Some(to.args().raw_data().to_vec())),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let timestamp = chrono::DateTime::from_timestamp_millis(
                                            block.header.inner.timestamp.value() as i64,
                                        )
                                        .unwrap()
                                        .naive_utc();

                                        let cluster = entity::clusters::ActiveModel {
                                            cluster_id: Set(transfer
                                                .cluster_id()
                                                .raw_data()
                                                .to_vec()),
                                            owner_address_id: Set(to.address_id.as_ref().clone()),
                                            name: Set(cluster_data.name().raw_data().to_vec()),
                                            description: Set(cluster_data
                                                .description()
                                                .raw_data()
                                                .to_vec()),
                                            mutant_id: Set(cluster_data
                                                .mutant_id()
                                                .to_opt()
                                                .map(|m| m.raw_data().to_vec())),
                                            created_at: NotSet,
                                            updated_at: Set(timestamp),
                                            ..Default::default()
                                        }
                                        .save(&db)
                                        .await?;

                                        let action = entity::cluster_actions::ActiveModel {
                                            action_id: NotSet,
                                            // 0代表mint
                                            action_type: Set(1),
                                            cluster_id: Set(cluster.cluster_id.unwrap()),
                                            from_address_id: Set(Some(
                                                from.address_id.as_ref().clone(),
                                            )),
                                            to_address_id: Set(Some(
                                                to.address_id.as_ref().clone(),
                                            )),
                                            created_at: Set(timestamp),
                                            rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id)),
                                            rgbpp_unlock_id: Set(rgbpp_unlock_op.clone()),
                                        }
                                        .insert(&db)
                                        .await?;
                                        println!("transfer cluster: {transfer:#}");
                                    }
                                    Some(unknow_action) => {
                                        eprintln!("Unknow spore action: {unknow_action:#}");
                                    }
                                    None => {
                                        eprintln!("Not found any action.");
                                    }
                                }
                            } else if let Some(unique) =
                                get_unique_type_script(output.type_.as_ref())
                            {
                                // todo
                                println!("unique!");
                            }
                        }
                    }
                }

                height += 1;
            }
            None => {
                println!("sleeping..");
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

fn extract_spore_action(
    tx: &ckb_jsonrpc_types::TransactionView,
    script: ckb_jsonrpc_types::Script,
) -> Option<action::SporeActionUnion> {
    let message = tx.inner.witnesses.iter().find_map(|witness| {
        WitnessLayoutReader::from_slice(witness.as_bytes())
            .ok()
            .and_then(|r| match r.to_enum() {
                WitnessLayoutUnionReader::SighashAll(s) => Some(s.message().to_entity()),
                _ => None,
            })
    })?;

    let spore_action = message
        .actions()
        .into_iter()
        // TODO
        // .filter(|action| action.script_hash().as_slice() == script.code_hash.as_bytes())
        .find_map(|action| {
            // println!("{}", action);
            // println!("{:?}", script);
            action::SporeActionReader::from_slice(&action.data().raw_data())
                .ok()
                .map(|reader| reader.to_entity().to_enum())
        })?;

    Some(spore_action)
}
