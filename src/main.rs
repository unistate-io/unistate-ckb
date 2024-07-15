use core::time::Duration;

use ckb_jsonrpc_types::{BlockNumber, TransactionView};

use config::Config;
use database::DatabaseProcessor;
use figment::{
    providers::{Format as _, Toml},
    Figment,
};
use rayon::iter::{
    IntoParallelIterator, IntoParallelRefIterator, ParallelExtend, ParallelIterator,
};
use sea_orm::{ConnectOptions, Database, EntityTrait};

use spore::SporeTx;
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::{
    filter::FilterFn, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};
use xudt::XudtTx;

use entity::block_height;

#[allow(clippy::all)]
#[allow(unused_imports)]
mod entity;
#[allow(clippy::all)]
mod schemas;

mod config;
mod constants;
mod database;
mod error;
mod fetcher;
mod inscription;
mod rgbpp;
mod spore;
mod unique;
mod xudt;

const MB: u32 = 1048576;

struct CategorizedTxs {
    spore_txs: Vec<SporeTx>,
    xudt_txs: Vec<XudtTx>,
    rgbpp_txs: Vec<TransactionView>,
    inscription_txs: Vec<TransactionView>,
}

impl CategorizedTxs {
    fn new() -> Self {
        Self {
            spore_txs: Vec::new(),
            xudt_txs: Vec::new(),
            rgbpp_txs: Vec::new(),
            inscription_txs: Vec::new(),
        }
    }

    fn merge(mut self, other: Self) -> Self {
        self.rgbpp_txs.par_extend(other.rgbpp_txs.into_par_iter());
        self.spore_txs.par_extend(other.spore_txs.into_par_iter());
        self.xudt_txs.par_extend(other.xudt_txs.into_par_iter());
        self.inscription_txs
            .par_extend(other.inscription_txs.into_par_iter());
        self
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;

    let config: Config = Figment::new()
        .join(Toml::file("unistate.toml"))
        .merge(figment::providers::Env::raw().only(&["DATABASE_URL"]))
        .extract()?;

    let config_level: tracing::Level = config.unistate.optional_config.level.into();

    let filter = FilterFn::new(move |metadata| metadata.level() <= &config_level);

    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_level(true);

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
        .init();

    info!("config: {config:#?}");

    let opt = ConnectOptions::new(&config.database_url);
    let db = Database::connect(opt).await?;

    let client = fetcher::Fetcher::from_config(&config.unistate)?;

    let block_height_value = block_height::Entity::find().one(&db).await?.unwrap().height as u64;
    let network = config.unistate.optional_config.network;
    let constants = constants::Constants::from_config(network);

    let mut height = config
        .unistate
        .optional_config
        .initial_height
        .max(block_height_value);

    let initial_target = client.get_tip_block_number().await?.value();
    let max_batch_size = config.unistate.optional_config.batch_size;
    let interval = config.unistate.optional_config.interval;
    let mut batch_size = (initial_target - height).min(max_batch_size);
    let mut target_height = initial_target;
    let mut pre_handle = None;
    let mut handles = JoinSet::new();
    let fetch_size = config.unistate.optional_config.fetch_size;
    loop {
        info!("Fetching batch: {batch_size} items | Progress: {height}/{target_height}");

        let numbers = (0..batch_size)
            .into_par_iter()
            .map(|i| BlockNumber::from(height + i))
            .collect::<Vec<_>>();

        height += batch_size;
        batch_size = (target_height - height).min(max_batch_size);

        let blocks = client.get_blocks(numbers).await?;

        let (database_processor, op_sender, commited) = DatabaseProcessor::new(db.clone(), height);

        let processor_handle = tokio::spawn(database_processor.handle());

        let fetcher = client.clone();

        let pre_handle_take = pre_handle.take();
        handles.spawn(async move {
            let categorized_txs = blocks
                .into_par_iter()
                .fold(CategorizedTxs::new, |acc, block| {
                    let new =
                        block
                            .transactions
                            .into_par_iter()
                            .fold(CategorizedTxs::new, move |mut categorized, tx| {
                                let rgbpp = tx.inner.cell_deps.par_iter().any(|cd| {
                                    constants.rgbpp_lock_dep().out_point.eq(&cd.out_point)
                                });

                                let spore = tx
                                    .inner
                                    .cell_deps
                                    .par_iter()
                                    .any(|cd| constants.is_spore(cd));

                                let xudt = tx.inner.cell_deps.par_iter().any(|cd| {
                                    constants.xudt_type_dep().out_point.eq(&cd.out_point)
                                });

                                let inscription = tx.inner.cell_deps.par_iter().any(|cd| {
                                    constants.inscription_info_dep().out_point.eq(&cd.out_point)
                                });

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
                                    categorized.rgbpp_txs.push(tx.clone());
                                }

                                if inscription {
                                    categorized.inscription_txs.push(tx);
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
                inscription_txs,
            } = categorized_txs;

            let spore_idxer = spore::SporeIndexer::new(spore_txs, network, op_sender.clone());

            spore_idxer.index()?;

            tracing::debug!("drop spore idxer");

            let xudt_idxer = xudt::XudtIndexer::new(xudt_txs, network, op_sender.clone());

            xudt_idxer.index()?;

            tracing::debug!("drop xudt idxer");

            let rgbpp_idxer = rgbpp::RgbppIndexer::new(rgbpp_txs, fetcher, op_sender.clone());

            rgbpp_idxer.index().await?;

            tracing::debug!("drop rgbpp idxer");

            let inscription_idxer =
                inscription::InscriptionInfoIndexer::new(inscription_txs, network, op_sender);

            inscription_idxer.index()?;

            if let Some(pre) = pre_handle_take {
                pre.await??;
            }

            commited
                .send(())
                .map_err(|_| anyhow::anyhow!("commited failed."))?;

            Result::<(), anyhow::Error>::Ok(())
        });

        pre_handle = Some(processor_handle);

        while let Some(res) = handles.try_join_next() {
            res??;
        }

        let pre_handle_len = handles.len();

        if pre_handle_len >= fetch_size {
            if let Some(res) = handles.join_next().await {
                res??;
            }
        }

        if batch_size == 0 {
            let new_target = client.get_tip_block_number().await?.value();
            if new_target != target_height {
                target_height = new_target;
                batch_size = (target_height - height).min(max_batch_size);
            } else {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        info!("sleeping...");
        tokio::time::sleep(Duration::from_secs_f32(interval)).await;
    }
}
