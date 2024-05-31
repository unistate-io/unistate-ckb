use core::time::Duration;
use std::env;

use ckb_jsonrpc_types::{BlockNumber, TransactionView};
use ckb_sdk::NetworkType;

use database::DatabaseProcessor;
use rayon::iter::{
    IntoParallelIterator, IntoParallelRefIterator, ParallelExtend, ParallelIterator,
};
use sea_orm::{ConnectOptions, Database, EntityTrait};

use spore::SporeTx;
use tokio::task::JoinHandle;
use tracing::{info, Level};
use tracing_subscriber::{
    filter::FilterFn, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};
use xudt::XudtTx;

use crate::constants::mainnet_info::{
    CLUSTER_TYPE_DEP, RGBPP_LOCK_DEP, SPORE_TYPE_DEP, XUDTTYPE_DEP,
};
use entity::block_height;

mod constants;
mod database;
mod entity;
mod error;
mod fetcher;
mod rgbpp;
mod schemas;
mod spore;
mod unique;
mod xudt;

const MB: u32 = 1048576;

struct CategorizedTxs {
    spore_txs: Vec<SporeTx>,
    xudt_txs: Vec<XudtTx>,
    rgbpp_txs: Vec<TransactionView>,
}

impl CategorizedTxs {
    fn new() -> Self {
        Self {
            spore_txs: Vec::new(),
            xudt_txs: Vec::new(),
            rgbpp_txs: Vec::new(),
        }
    }

    fn merge(mut self, other: Self) -> Self {
        self.rgbpp_txs.par_extend(other.rgbpp_txs.into_par_iter());
        self.spore_txs.par_extend(other.spore_txs.into_par_iter());
        self.xudt_txs.par_extend(other.xudt_txs.into_par_iter());
        self
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;

    let filter = FilterFn::new(|metadata| {
        metadata.level() <= &Level::INFO && metadata.module_path().is_some()
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
        fetcher::Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 1000 * MB, 100 * MB)?;

    let block_height_value = block_height::Entity::find().one(&db).await?.unwrap().height as u64;

    let network = NetworkType::Mainnet;

    let mut height = constants::mainnet_info::DEFAULT_START_HEIGHT.max(block_height_value);

    let initial_target = client.get_tip_block_number().await?.value();
    let max_batch_size = 1000;
    let mut batch_size = (initial_target - height).min(max_batch_size);
    let mut target_height = initial_target;
    let mut pre_handle = None;

    loop {
        info!("height: {height}");

        let numbers = (0..batch_size)
            .into_par_iter()
            .map(|i| BlockNumber::from(height + i))
            .collect::<Vec<_>>();

        height += batch_size;
        batch_size = (target_height - height).min(max_batch_size);

        let blocks = client.get_blocks(numbers).await?;

        let (database_processor, op_sender, height_sender) = DatabaseProcessor::new(db.clone());

        let processor_handle = tokio::spawn(database_processor.handle());

        let fetcher = client.clone();

        let pre_handle_take = pre_handle.take();
        let main_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let categorized_txs = blocks
                .into_par_iter()
                .fold(CategorizedTxs::new, |acc, block| {
                    let new = block
                        .transactions
                        .into_par_iter()
                        .fold(CategorizedTxs::new, move |mut categorized, tx| {
                            let rgbpp = tx
                                .inner
                                .cell_deps
                                .par_iter()
                                .any(|cd| RGBPP_LOCK_DEP.out_point.eq(&cd.out_point));

                            let spore = tx.inner.cell_deps.par_iter().any(|cd| {
                                SPORE_TYPE_DEP.out_point.eq(&cd.out_point)
                                    || CLUSTER_TYPE_DEP.out_point.eq(&cd.out_point)
                            });

                            let xudt = tx
                                .inner
                                .cell_deps
                                .par_iter()
                                .any(|cd| XUDTTYPE_DEP.out_point.eq(&cd.out_point));

                            if spore {
                                categorized.spore_txs.push(SporeTx {
                                    tx: tx.clone(),
                                    timestamp: block.header.inner.timestamp.value(),
                                });
                            }
                            if xudt {
                                categorized.xudt_txs.push(XudtTx { tx: tx.clone() });
                            }
                            if rgbpp {
                                categorized.rgbpp_txs.push(tx);
                            }

                            categorized
                        })
                        .reduce(CategorizedTxs::new, |pre, next| pre.merge(next));
                    acc.merge(new)
                })
                .reduce(CategorizedTxs::new, |acc, txs| acc.merge(txs));

            let CategorizedTxs {
                spore_txs,
                xudt_txs,
                rgbpp_txs,
            } = categorized_txs;

            let spore_idxer = spore::SporeIndexer::new(spore_txs, network, op_sender.clone());

            spore_idxer.index()?;

            let xudt_idxer = xudt::XudtIndexer::new(xudt_txs, network, op_sender.clone());

            xudt_idxer.index()?;

            let rgbpp_idxer = rgbpp::RgbppIndexer::new(rgbpp_txs, fetcher, op_sender.clone());

            rgbpp_idxer.index().await?;

            if let Some(pre_handle) = pre_handle_take {
                pre_handle.await??;
            }

            if height_sender
                .send(block_height::ActiveModel {
                    id: sea_orm::Set(1),
                    height: sea_orm::Set(height as i64),
                })
                .is_err()
            {
                tracing::error!("send height model failed: {:?}", height)
            };

            processor_handle.await??;

            Ok(())
        });

        pre_handle = Some(main_handle);

        if batch_size == 0 {
            let new_target = client.get_tip_block_number().await?.value();
            if new_target != target_height {
                target_height = new_target;
                batch_size = (target_height - height).min(max_batch_size);
            } else {
                info!("sleeping...");
                tokio::time::sleep(Duration::from_secs(6)).await;
            }
        }
    }
}
