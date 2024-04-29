use core::time::Duration;

use async_std::task::sleep;
use ckb_sdk::{rpc::ResponseFormatGetter, CkbRpcClient};

use constants::mainnet_info::{
    get_cluster_type_script, get_spore_type_script, get_unique_type_script, get_xudttype_script,
    is_rgbpp_lock_script, CLUSTER_TYPE_SCRIPT, RGBPP_LOCK_DEP, SPORE_TYPE_SCRIPT, XUDTTYPE_DEP,
};

use molecule::prelude::Reader;
use schemas::{
    action,
    blockchain::WitnessArgsReader,
    spore_v1::SporeDataReader,
    top_level::{WitnessLayoutReader, WitnessLayoutUnionReader},
};
use sea_orm::{ConnectOptions, Database};

use crate::{
    operations::{
        burn_spore, mint_cluster, mint_spore, transfer_cluster, transfer_spore, upsert_rgbpp_lock,
        upsert_rgbpp_unlock,
    },
    schemas::spore_v2::ClusterDataV2Reader,
};

mod constants;
mod entity;
mod operations;
mod schemas;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opt =
        ConnectOptions::new("postgres://unistate_dev:unistate_dev@localhost:5432/unistate_dev");
    let db = Database::connect(opt).await?;
    // let client = CkbRpcClient::new("http://127.0.0.1:8114");
    let client = CkbRpcClient::new("https://rpc.ankr.com/nervos_ckb/0b0417ab9bc09b426a1e617d9761d5262610241ee275708775ceba0d7f6e99c9");
    // let mut height = START_RGBPP_HEIGHT;
    // let mut height = 12826479u64;
    let mut height = 12799324u64;

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

                            let rgbpp_lock = upsert_rgbpp_lock(&db, pre_output, rgbpp_lock).await?;

                            if let Some(rgbpp_unlock) =
                                pre_tx.inner.witnesses.iter().find_map(|witness| {
                                    WitnessArgsReader::from_slice(witness.as_bytes())
                                        .ok()
                                        .and_then(|witness_args| {
                                            witness_args.lock().to_opt().and_then(|args| {
                                                schemas::rgbpp::RGBPPUnlockReader::from_slice(
                                                    args.raw_data().as_ref(),
                                                )
                                                .ok()
                                            })
                                        })
                                })
                            {
                                println!("input rgbpp_unlock: {rgbpp_unlock:#}");
                                let rgbpp_unlock =
                                    upsert_rgbpp_unlock(&db, &rgbpp_lock, rgbpp_unlock).await?;
                                rgbpp_unlock_op = Some(rgbpp_unlock);
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
                                        burn_spore(
                                            &db,
                                            block.header.inner.timestamp.value(),
                                            burn,
                                            &rgbpp_lock,
                                            rgbpp_unlock_op.as_ref(),
                                        )
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

                            let rgbpp_lock = upsert_rgbpp_lock(&db, output, rgbpp_lock).await?;

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
                                        mint_spore(
                                            &db,
                                            block.header.inner.timestamp.value(),
                                            spore_data,
                                            mint,
                                            &rgbpp_lock,
                                            rgbpp_unlock_op.as_ref(),
                                        )
                                        .await?;
                                    }
                                    Some(action::SporeActionUnion::TransferSpore(transfer)) => {
                                        println!("transfer spore: {transfer:#}");
                                        transfer_spore(
                                            &db,
                                            block.header.inner.timestamp.value(),
                                            transfer,
                                            &rgbpp_lock,
                                            rgbpp_unlock_op.as_ref(),
                                        )
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
                                        println!("mint cluster: {mint:#}");
                                        mint_cluster(
                                            &db,
                                            block.header.inner.timestamp.value(),
                                            cluster_data,
                                            mint,
                                            &rgbpp_lock,
                                            rgbpp_unlock_op.as_ref(),
                                        )
                                        .await?;
                                    }
                                    Some(action::SporeActionUnion::TransferCluster(transfer)) => {
                                        println!("transfer cluster: {transfer:#}");
                                        transfer_cluster(
                                            &db,
                                            block.header.inner.timestamp.value(),
                                            transfer,
                                            &rgbpp_lock,
                                            rgbpp_unlock_op.as_ref(),
                                        )
                                        .await?;
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
                println!("height: {height}");
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
