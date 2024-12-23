use async_scoped::spawner::use_tokio::Tokio;
use clap::Parser;
use core::time::Duration;

use ckb_jsonrpc_types::{BlockNumber, TransactionView};

use ckb_sdk::NetworkType;
use config::Config;
use database::{DatabaseProcessor, Operations};
use figment::{
    providers::{Format as _, Toml},
    Figment,
};
use futures::{Future, StreamExt, TryStreamExt};
use rayon::iter::{IntoParallelIterator, ParallelExtend, ParallelIterator};
use sea_orm::{ConnectOptions, Database, DbConn, EntityTrait};

use spore::SporeTx;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::{error, info};
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
mod database;
mod error;
mod inscription;
mod rgbpp;
mod spore;
mod unique;
mod xudt;

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
    let apply_init_height = parse_args()?;

    setup_environment()?;
    let config = load_config()?;
    setup_logging(&config)?;

    info!("config: {config:#?}");

    let db = setup_database(&config).await?;
    let client = config.http_fetcher().await?;

    let (initial_height, network, constants) =
        initialize_blockchain_data(&db, &config, apply_init_height).await?;

    let mut indexer = Indexer::new(initial_height, &config, client, network, constants, db);

    run_with_watchdog(&mut indexer).await?;

    Ok(())
}

async fn run_with_watchdog(indexer: &mut Indexer) -> Result<()> {
    let mut last_error: Option<anyhow::Error> = None;
    let mut last_failure_time: Option<time::Instant> = None;

    loop {
        match indexer.run().await {
            Ok(_) => {
                info!("Indexer run completed successfully.");
                return Ok(());
            }
            Err(e) => {
                error!("Indexer run encountered an error: {:?}", e);

                if let Some(last_error) = last_error.as_ref() {
                    if last_error.to_string() == e.to_string() {
                        if let Some(last_failure_time) = last_failure_time {
                            if last_failure_time.elapsed() < Duration::from_secs(10) {
                                error!("Repeated error within 10 seconds: {:?}", e);
                                return Err(e);
                            }
                        }
                    }
                }

                last_error = Some(e);
                last_failure_time = Some(time::Instant::now());
                info!("Restarting indexer after error...");
                time::sleep(Duration::from_secs(6)).await;
            }
        }
    }
}

#[derive(Parser)]
#[command(name = "unistate-ckb", about = "Unistate CKB Indexer", long_about = None, version)]
struct Cli {
    /// Apply initial height (true/false)
    #[arg(short, long, value_parser)]
    apply_init_height: Option<bool>,
}

fn parse_args() -> Result<Option<bool>> {
    let cli = Cli::parse();
    Ok(cli.apply_init_height)
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

async fn initialize_blockchain_data(
    db: &DbConn,
    config: &Config,
    apply_init_height: Option<bool>,
) -> Result<(u64, NetworkType, constants::Constants)> {
    let mut initial_height = config.unistate.optional_config.initial_height;
    let apply_init_height =
        apply_init_height.unwrap_or(config.unistate.optional_config.apply_initial_height);

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
    client: fetcher::HttpFetcher,
    db: DbConn,
    constants: constants::Constants,
    network: NetworkType,
}

impl Indexer {
    fn new(
        initial_height: u64,
        config: &Config,
        client: fetcher::HttpFetcher,
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
        let mut scope = unsafe { async_scoped::TokioScope::create(Tokio) };
        loop {
            self.log_progress();
            let numbers = self.get_block_numbers();
            self.update_height();

            let (database_processor, op_sender, commited) =
                DatabaseProcessor::new(self.db.clone(), self.height);
            let processor_handle = tokio::spawn(database_processor.handle());

            let handle = self.spawn_indexing_task(numbers, op_sender, commited, pre_handle.take());
            scope.spawn(handle);
            pre_handle = Some(processor_handle);

            while let Ok(Some(res)) = scope.try_next().await {
                res?;
            }

            if scope.remaining() >= self.fetch_size {
                if let Some(res) = scope.next().await {
                    res??;
                }
            }

            if self.batch_size == 0 {
                time::sleep(Duration::from_secs(6)).await;
                self.update_target_height().await?;
            }

            time::sleep(Duration::from_secs_f32(self.interval)).await;
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

    fn spawn_indexing_task<'i, 'r>(
        &'i self,
        numbers: Vec<BlockNumber>,
        op_sender: mpsc::UnboundedSender<Operations>,
        commited: oneshot::Sender<()>,
        pre_handle: Option<JoinHandle<Result<()>>>,
    ) -> impl Future<Output = Result<()>> + use<'r> {
        let fetcher = self.client.clone();
        let constants = self.constants;
        let network = self.network;

        async move {
            let categorized_txs =
                fetch_and_categorize_transactions(&fetcher, numbers, &constants).await?;

            let (spore_sender, xudt_sender, rgbpp_sender) =
                (op_sender.clone(), op_sender.clone(), op_sender.clone());

            let (mut scope, _) = unsafe {
                async_scoped::TokioScope::scope(|scope| {
                    scope.spawn_blocking(move || {
                        let spore_idxer = spore::SporeIndexer::new(
                            categorized_txs.spore_txs,
                            network,
                            constants,
                            spore_sender,
                        );
                        spore_idxer.index()
                    });
                    scope.spawn_blocking(move || {
                        let xudt_idxer =
                            xudt::XudtIndexer::new(categorized_txs.xudt_txs, network, xudt_sender);
                        xudt_idxer.index()
                    });
                    scope.spawn(async move {
                        let rgbpp_idxer = rgbpp::RgbppIndexer::new(
                            categorized_txs.rgbpp_txs,
                            fetcher,
                            rgbpp_sender,
                        );
                        rgbpp_idxer.index().await
                    });
                    scope.spawn_blocking(move || {
                        let inscription_idxer = inscription::InscriptionInfoIndexer::new(
                            categorized_txs.inscription_txs,
                            network,
                            op_sender,
                        );
                        inscription_idxer.index()
                    });
                })
            };

            while let Some(res) = scope.next().await {
                res??;
            }

            drop(scope);

            if let Some(pre) = pre_handle {
                pre.await??;
            }

            commited
                .send(())
                .map_err(|_| anyhow::anyhow!("commited failed."))?;

            Ok(())
        }
    }
}

async fn fetch_and_categorize_transactions(
    fetcher: &fetcher::HttpFetcher,
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
                    .fold(CategorizedTxs::new, |mut txs, tx| {
                        let categorizes = categorize_transaction(&tx, constants);
                        if categorizes.is_spore {
                            txs.spore_txs.push(SporeTx {
                                tx: tx.clone(),
                                timestamp,
                            });
                        }
                        if categorizes.is_xudt {
                            txs.xudt_txs.push(tx.clone());
                        }

                        if categorizes.is_rgbpp {
                            txs.rgbpp_txs.push(tx.clone());
                        }

                        if categorizes.is_inscription {
                            txs.inscription_txs.push(tx.clone());
                        }

                        if let Some(db) = fetcher.db.clone() {
                            let db = db.read();
                            let _ = fetcher::cache_transaction(&*db, tx);
                        }

                        txs
                    })
                    .reduce(CategorizedTxs::new, CategorizedTxs::merge),
            )
        })
        .reduce(CategorizedTxs::new, CategorizedTxs::merge);
    Ok(categorize_txs)
}

macro_rules! define_categories {
    ($($item:ident),*) => {
        struct Categories {
            $($item: bool),*
        }

        fn categorize_transaction(tx: &TransactionView, constants: &constants::Constants) -> Categories {
            tx.inner.cell_deps.iter().fold(
                Categories {
                    $($item: false),*
                },
                |categories, cd| (
                    Categories {
                        $($item: categories.$item || constants.$item(cd)),*
                    }
                )
            )
        }
    };
}

define_categories! { is_spore, is_xudt, is_rgbpp, is_inscription }
