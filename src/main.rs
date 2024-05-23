use core::time::Duration;
use std::env;

use ckb_jsonrpc_types::{BlockNumber, TransactionView};
use ckb_sdk::NetworkType;

use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use sea_orm::{ActiveModelTrait, ConnectOptions, Database, EntityTrait};

use tokio::{select, task::JoinHandle, time::sleep};
use tracing::{debug, info, Level};
use tracing_subscriber::{
    filter::FilterFn, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};

use crate::constants::mainnet_info::{
    CLUSTER_TYPE_DEP, RGBPP_LOCK_DEP, SPORE_TYPE_DEP, XUDTTYPE_DEP,
};
use entity::block_height;

mod constants;
mod entity;
mod error;
mod fetcher;
mod rgbpp;
mod schemas;
mod spore;
mod unique;
mod xudt;

struct TxWithStates {
    is_spore: bool,
    is_xudt: bool,
    is_rgbpp: bool,
    tx: TransactionView,
    timestamp: u64,
    height: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;

    let filter = FilterFn::new(|metadata| {
        // Only enable spans or events with the target "interesting_things"
        metadata.level() <= &Level::DEBUG
            && metadata
                .module_path()
                .map(|p| !(p.starts_with("h2::codec") || p.starts_with("sqlx::query")))
                .unwrap_or(true)
    });

    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_level(true);

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
        .init();

    let opt = ConnectOptions::new(env::var("DATABASE_URL")?);
    let db = Database::connect(opt).await?;
    // let client =
    //     fetcher::Fetcher::http_client("http://127.0.0.1:8114", 500, 5, 104857600, 104857600)?;
    let client =
        fetcher::Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 104857600, 104857600)?;

    let block_height_value = block_height::Entity::find().one(&db).await?.unwrap().height as u64;

    let network = NetworkType::Mainnet;
    let mut height = constants::mainnet_info::DEFAULT_START_HEIGHT.max(block_height_value);

    let target = client.get_tip_block_number().await?.value();
    let max_batch_size = 100;
    let mut batch_size = (target - height).min(max_batch_size);

    let (spore_indexer, spore_sender) = spore::SporeIndexer::new(&db, network);

    let (xudt_indexer, xudt_sender) =
        xudt::XudtIndexer::new(&db, network, spore_indexer.sender.clone());

    let spore_task = tokio::spawn(spore_indexer.index());

    let (rgbpp_indexer, rgbpp_sender) = rgbpp::RgbppIndexer::new(&db, &client);

    let rgbpp_task = tokio::spawn(rgbpp_indexer.index());

    let xudt_task = tokio::spawn(xudt_indexer.index());

    let (block_sender, mut block_receiver) = tokio::sync::mpsc::channel(100);

    let fetch_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        loop {
            info!("height: {height}");

            let numbers = (0..batch_size)
                .into_par_iter()
                .map(|i| BlockNumber::from(height + i))
                .collect::<Vec<_>>();

            let blocks = client.get_blocks(numbers).await?;

            // Send blocks to the processing task
            block_sender.send((blocks, height)).await?;

            height += batch_size;
            batch_size =
                (client.get_tip_block_number().await?.value() - height).min(max_batch_size);

            if batch_size == 0 {
                info!("sleeping...");
                tokio::time::sleep(Duration::from_secs(6)).await;
            }
        }
    });

    let process_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        loop {
            if let Some((blocks, height)) = block_receiver.recv().await {
                let txs = blocks
                    .into_par_iter()
                    .enumerate()
                    .flat_map(|(i, block)| {
                        block.transactions.into_par_iter().filter_map(move |tx| {
                            let rgbpp = tx
                                .inner
                                .cell_deps
                                .par_iter()
                                .find_any(|cd| RGBPP_LOCK_DEP.out_point.eq(&cd.out_point));

                            let spore = tx.inner.cell_deps.par_iter().find_any(|cd| {
                                SPORE_TYPE_DEP.out_point.eq(&cd.out_point)
                                    || CLUSTER_TYPE_DEP.out_point.eq(&cd.out_point)
                            });

                            let xudt = tx
                                .inner
                                .cell_deps
                                .par_iter()
                                .find_any(|cd| XUDTTYPE_DEP.out_point.eq(&cd.out_point));
                            let is_spore = spore.is_some();
                            let is_xudt = xudt.is_some();
                            let is_rgbpp = rgbpp.is_some();

                            if is_rgbpp || is_xudt || is_spore {
                                Some(TxWithStates {
                                    is_spore,
                                    is_xudt,
                                    is_rgbpp,
                                    tx,
                                    timestamp: block.header.inner.timestamp.value(),
                                    height: height + i as u64,
                                })
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<Vec<_>>();

                info!("will handle {} txs", txs.len());

                let mut rgbpp_txs = vec![];
                let mut spore_txs = vec![];
                let mut xudt_txs = vec![];

                for tx in txs.into_iter() {
                    if tx.is_rgbpp {
                        rgbpp_txs.push(tx.tx.clone());
                    }

                    if tx.is_spore {
                        spore_txs.push(spore::SporeTx {
                            tx: tx.tx.clone(),
                            timestamp: tx.timestamp,
                        });
                    }

                    if tx.is_xudt {
                        xudt_txs.push(xudt::XudtTx { tx: tx.tx });
                    }
                }

                let rgbpp_sender = rgbpp_sender.clone();

                let rgbpp_sender_task = tokio::spawn(async move {
                    for tx in rgbpp_txs {
                        rgbpp_sender.send(tx).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                });

                let spore_sender = spore_sender.clone();

                let spore_sender_task = tokio::spawn(async move {
                    for tx in spore_txs {
                        spore_sender.send(tx).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                });

                let xudt_sender = xudt_sender.clone();

                let xudt_sender_task = tokio::spawn(async move {
                    for tx in xudt_txs {
                        xudt_sender.send(tx).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                });

                let _ = tokio::try_join!(rgbpp_sender_task, spore_sender_task, xudt_sender_task)?;

                block_height::ActiveModel {
                    id: sea_orm::Set(1),
                    height: sea_orm::Set(height as i64),
                }
                .update(&db)
                .await?;
            } else {
                sleep(Duration::from_secs(2)).await;
            }
        }
    });

    select! {
        res = spore_task => {
            debug!("spore res: {res:?}");
        }
        res = rgbpp_task => {
            debug!("rgbpp res: {res:?}");
        }
        res = xudt_task => {
            debug!("xudt res: {res:?}");
        }
        res = fetch_task => {
            debug!("featch res: {res:?}");
        }
        res = process_task => {
            debug!("process res: {res:?}");
        }
    }

    Ok(())
}
