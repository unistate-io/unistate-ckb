use core::time::Duration;

use ckb_jsonrpc_types::BlockNumber;
use ckb_sdk::NetworkType;

use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use sea_orm::{ActiveModelTrait, ConnectOptions, Database, EntityTrait};

use tracing::info;
use tracing_subscriber::{
    filter::FilterFn, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};

use crate::constants::mainnet_info::{CLUSTER_TYPE_DEP, SPORE_TYPE_DEP};
use entity::block_height;

mod constants;
mod entity;
mod error;
mod fetcher;
mod rgbpp;
mod schemas;
mod spore;
mod xudt;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let filter = FilterFn::new(|metadata| {
        // Only enable spans or events with the target "interesting_things"
        metadata
            .module_path()
            .map(|p| p.starts_with("unistate_ckb"))
            .unwrap_or(false)
    });

    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_level(true);

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
        .init();

    let opt =
        ConnectOptions::new("postgres://unistate_dev:unistate_dev@localhost:5432/unistate_dev");
    let db = Database::connect(opt).await?;
    // let client = CkbRpcClient::new("http://127.0.0.1:8114");
    let client =
        fetcher::Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 104857600, 104857600)?;

    let block_height_value = block_height::Entity::find().one(&db).await?.unwrap().height as u64;

    let network = NetworkType::Mainnet;
    // let mut height = constants::mainnet_info::START_RGBPP_HEIGHT;
    // let mut height = 12826479u64;
    // let mut height = 12799324u64;
    // let mut height = 12801007u64;
    // let mut height = 12801313u64;
    // let mut height = 12800136u64;
    // let mut height = 12000082u64.max(block_height_value);
    // let mut height = 12429082u64;
    let mut height = 12800082u64.max(block_height_value);

    let mut target = client.get_tip_block_number().await?.value();
    let max_batch_size = 1000;
    let mut batch_size = (target - height).min(max_batch_size);

    loop {
        let numbers = (0..batch_size)
            .into_par_iter()
            .map(|i| BlockNumber::from(height + i))
            .collect::<Vec<_>>();

        let blocks = client.get_blocks(numbers).await?;

        let txs = blocks
            .into_par_iter()
            .map(|block| {
                block
                    .transactions
                    .into_par_iter()
                    .filter(|tx| {
                        // let rgbpp = tx.inner.cell_deps.par_iter().find_any(|cd| {
                        //     cd.out_point
                        //         .tx_hash
                        //         .as_bytes()
                        //         .eq(RGBPP_LOCK_DEP.out_point.tx_hash.as_bytes())
                        // });

                        let spore = tx.inner.cell_deps.par_iter().find_any(|cd| {
                            cd.out_point
                                .tx_hash
                                .as_bytes()
                                .eq(SPORE_TYPE_DEP.out_point.tx_hash.as_bytes())
                                || cd
                                    .out_point
                                    .tx_hash
                                    .as_bytes()
                                    .eq(CLUSTER_TYPE_DEP.out_point.tx_hash.as_bytes())
                        });

                        spore.is_some() // || rgbpp.is_some()
                    })
                    .map(|tx| (tx, block.header.inner.timestamp.value()))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();

        height += batch_size;
        target = client.get_tip_block_number().await?.value();
        batch_size = (target - height).min(max_batch_size);

        info!("height: {height}");

        for (tx, timestamp) in txs {
            rgbpp::index_rgbpp_lock(&client, &db, &tx).await?;
            spore::index_spore(&db, &tx, timestamp, network).await?;
        }

        block_height::ActiveModel {
            id: sea_orm::Set(1),
            height: sea_orm::Set(height as i64),
        }
        .update(&db)
        .await?;

        if batch_size == 0 {
            tokio::time::sleep(Duration::from_secs(6)).await;
        }
    }
}
