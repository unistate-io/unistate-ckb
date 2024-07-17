use core::time::Duration;

use ckb_jsonrpc_types::{BlockNumber, TransactionView};

use ckb_sdk::NetworkType;
use config::Config;
use database::{DatabaseProcessor, Operations};
use figment::{
    providers::{Format as _, Toml},
    Figment,
};
use futures::Future;
use jsonrpsee::http_client::HttpClient;
use rayon::iter::{IntoParallelIterator, ParallelExtend, ParallelIterator};
use sea_orm::{ConnectOptions, Database, DbConn, EntityTrait};

use spore::SporeTx;
use tokio::{
    sync::{mpsc, oneshot},
    task::{self, JoinHandle, JoinSet},
};
use tracing::info;
use tracing_subscriber::{
    filter::FilterFn, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};

use anyhow::Result;
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
    xudt_txs: Vec<TransactionView>,
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
async fn main() -> Result<()> {
    setup_environment()?;
    let config = load_config()?;
    setup_logging(&config)?;

    info!("config: {config:#?}");

    let db = setup_database(&config).await?;
    let client = setup_fetcher(&config)?;

    let (initial_height, network, constants) = initialize_blockchain_data(&db, &config).await?;

    let mut indexer = Indexer::new(initial_height, &config, client, network, constants, db);
    indexer.run().await?;

    Ok(())
}

fn setup_environment() -> Result<()> {
    dotenvy::dotenv()?;
    Ok(())
}

fn load_config() -> Result<Config> {
    let config: Config = Figment::new()
        .join(Toml::file("unistate.toml"))
        .merge(figment::providers::Env::raw().only(&["DATABASE_URL"]))
        .extract()?;
    Ok(config)
}

fn setup_logging(config: &Config) -> Result<()> {
    let config_level: tracing::Level = config.unistate.optional_config.level.into();
    let filter = FilterFn::new(move |metadata| metadata.level() <= &config_level);
    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_level(true);

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
        .init();

    Ok(())
}
async fn setup_database(config: &Config) -> Result<DbConn> {
    let mut opt = ConnectOptions::new(&config.database_url);
    let pool = &config.pool;

    if let Some(max_connections) = pool.max_connections {
        opt.max_connections(max_connections);
    }

    if let Some(min_connections) = pool.min_connections {
        opt.min_connections(min_connections);
    }

    if let Some(connection_timeout) = pool.connection_timeout {
        opt.connect_timeout(Duration::from_secs(connection_timeout));
    }

    if let Some(acquire_timeout) = pool.acquire_timeout {
        opt.acquire_timeout(Duration::from_secs(acquire_timeout));
    }

    if let Some(idle_timeout) = pool.idle_timeout {
        opt.idle_timeout(Duration::from_secs(idle_timeout));
    }

    if let Some(max_lifetime) = pool.max_lifetime {
        opt.max_lifetime(Duration::from_secs(max_lifetime));
    }

    opt.sqlx_logging(pool.sqlx_logging);

    let db = Database::connect(opt).await?;
    Ok(db)
}

fn setup_fetcher(config: &Config) -> Result<fetcher::Fetcher<HttpClient>> {
    Ok(fetcher::Fetcher::from_config(&config.unistate)?)
}

async fn initialize_blockchain_data(
    db: &DbConn,
    config: &Config,
) -> Result<(u64, NetworkType, constants::Constants)> {
    let mut initial_height = config.unistate.optional_config.initial_height;
    let apply_init_height = config.unistate.optional_config.apply_initial_height;

    if !apply_init_height {
        if let Some(block_height_entity) = block_height::Entity::find().one(db).await? {
            initial_height = initial_height.max(block_height_entity.height as u64);
        }
    }

    let network = config.unistate.optional_config.network;
    let constants = constants::Constants::from_config(network);

    Ok((initial_height, network, constants))
}

struct Indexer {
    height: u64,
    target_height: u64,
    batch_size: u64,
    max_batch_size: u64,
    interval: f32,
    fetch_size: usize,
    client: fetcher::Fetcher<HttpClient>,
    db: DbConn,
    constants: constants::Constants,
    network: NetworkType,
}

impl Indexer {
    fn new(
        initial_height: u64,
        config: &Config,
        client: fetcher::Fetcher<HttpClient>,
        network: NetworkType,
        constants: constants::Constants,
        db: DbConn,
    ) -> Self {
        let max_batch_size = config.unistate.optional_config.batch_size;
        let interval = config.unistate.optional_config.interval;
        let fetch_size = config.unistate.optional_config.fetch_size;

        Self {
            height: initial_height,
            target_height: 0,
            batch_size: 0,
            max_batch_size,
            interval,
            fetch_size,
            client,
            db,
            constants,
            network,
        }
    }

    async fn run(&mut self) -> Result<()> {
        self.update_target_height().await?;

        let mut pre_handle = None;
        let mut handles = JoinSet::new();

        loop {
            self.log_progress();
            let numbers = self.get_block_numbers();
            self.update_height();

            let (database_processor, op_sender, commited) =
                DatabaseProcessor::new(self.db.clone(), self.height);
            let processor_handle = tokio::spawn(database_processor.handle());

            let handle = self.spawn_indexing_task(numbers, op_sender, commited, pre_handle.take());
            handles.spawn(handle);

            pre_handle = Some(processor_handle);

            self.manage_handles(&mut handles).await?;

            if self.batch_size == 0 {
                tokio::time::sleep(Duration::from_secs(6)).await;
                self.update_target_height().await?;
            }

            tokio::time::sleep(Duration::from_secs_f32(self.interval)).await;
        }
    }

    fn log_progress(&self) {
        info!(
            "Fetching batch: {} items | Progress: {}/{}",
            self.batch_size, self.height, self.target_height
        );
    }

    fn get_block_numbers(&self) -> Vec<BlockNumber> {
        (0..self.batch_size)
            .into_par_iter()
            .map(|i| BlockNumber::from(self.height + i))
            .collect()
    }

    fn update_height(&mut self) {
        self.height += self.batch_size;
        self.batch_size = (self.target_height - self.height).min(self.max_batch_size);
    }

    async fn update_target_height(&mut self) -> Result<()> {
        let new_target = self.client.get_tip_block_number().await?.value();
        if new_target != self.target_height {
            self.target_height = new_target;
            self.batch_size = (self.target_height - self.height).min(self.max_batch_size);
        }
        Ok(())
    }

    fn spawn_indexing_task(
        &self,
        numbers: Vec<BlockNumber>,
        op_sender: mpsc::UnboundedSender<Operations>,
        commited: oneshot::Sender<()>,
        pre_handle: Option<JoinHandle<Result<()>>>,
    ) -> impl Future<Output = Result<()>> {
        let fetcher = self.client.clone();
        let constants = self.constants;
        let network = self.network;

        async move {
            let categorized_txs =
                fetch_and_categorize_transactions(&fetcher, numbers, &constants).await?;

            index_transactions(categorized_txs, network, constants, op_sender, fetcher).await?;

            if let Some(pre) = pre_handle {
                pre.await??;
            }

            commited
                .send(())
                .map_err(|_| anyhow::anyhow!("commited failed."))?;

            Ok(())
        }
    }

    async fn manage_handles(&self, handles: &mut JoinSet<Result<()>>) -> Result<()> {
        while let Some(res) = handles.try_join_next() {
            res??;
        }

        if handles.len() >= self.fetch_size {
            if let Some(res) = handles.join_next().await {
                res??;
            }
        }

        Ok(())
    }
}

async fn fetch_and_categorize_transactions(
    fetcher: &fetcher::Fetcher<HttpClient>,
    numbers: Vec<BlockNumber>,
    constants: &constants::Constants,
) -> Result<CategorizedTxs> {
    let categorize_txs = fetcher
        .get_blocks(numbers)
        .await?
        .into_par_iter()
        .fold(CategorizedTxs::new, |txs, block| {
            let timestamp = block.header.inner.timestamp.value();
            txs.merge(
                block
                    .transactions
                    .into_par_iter()
                    .fold(CategorizedTxs::new, |txs, tx| {
                        categorize_transaction(txs, tx, constants, timestamp)
                    })
                    .reduce(CategorizedTxs::new, CategorizedTxs::merge),
            )
        })
        .reduce(CategorizedTxs::new, CategorizedTxs::merge);
    Ok(categorize_txs)
}

fn categorize_transaction(
    mut txs: CategorizedTxs,
    tx: TransactionView,
    constants: &constants::Constants,
    timestamp: u64,
) -> CategorizedTxs {
    macro_rules! define_categories {
        ($($item:ident),*) => {{
            struct Categories {
                $($item: bool),*
            }

            tx.inner.cell_deps.iter().fold(
                Categories {
                    $($item: false),*
                },
                |categories, cd| (
                    Categories {
                        $($item: categories.$item || constants.is_spore(cd)),*
                    }
                )
            )
        }};
    }

    let categories = define_categories!(is_spore, is_xudt, is_rgbpp, is_inscription);

    if categories.is_spore {
        txs.spore_txs.push(SporeTx {
            tx: tx.clone(),
            timestamp,
        });
    }

    if categories.is_xudt {
        txs.xudt_txs.push(tx.clone());
    }

    if categories.is_rgbpp {
        txs.rgbpp_txs.push(tx.clone());
    }

    if categories.is_inscription {
        txs.inscription_txs.push(tx);
    }

    txs
}
async fn index_transactions(
    categorized_txs: CategorizedTxs,
    network: NetworkType,
    constants: constants::Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
    fetcher: fetcher::Fetcher<HttpClient>,
) -> Result<()> {
    let (spore_sender, xudt_sender, rgbpp_sender) =
        (op_sender.clone(), op_sender.clone(), op_sender.clone());
    let (spore_result, xudt_result, rgbpp_result, inscription_result) = tokio::join!(
        task::spawn_blocking(move || {
            let spore_idxer = spore::SporeIndexer::new(
                categorized_txs.spore_txs,
                network,
                constants,
                spore_sender,
            );
            spore_idxer.index()
        }),
        task::spawn_blocking(move || {
            let xudt_idxer = xudt::XudtIndexer::new(categorized_txs.xudt_txs, network, xudt_sender);
            xudt_idxer.index()
        }),
        task::spawn(async move {
            let rgbpp_idxer =
                rgbpp::RgbppIndexer::new(categorized_txs.rgbpp_txs, fetcher, rgbpp_sender);
            rgbpp_idxer.index().await
        }),
        task::spawn_blocking(move || {
            let inscription_idxer = inscription::InscriptionInfoIndexer::new(
                categorized_txs.inscription_txs,
                network,
                op_sender,
            );
            inscription_idxer.index()
        })
    );

    spore_result??;
    xudt_result??;
    rgbpp_result??;
    inscription_result??;

    Ok(())
}
