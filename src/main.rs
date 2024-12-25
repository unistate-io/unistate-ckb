use celldep_height_finder::fetch_and_print_dep_heights;
use ckb_sdk::NetworkType;
use clap::{Parser, Subcommand};
use config::Config;
use constants::Constants;
use core::time::Duration;

use figment::{
    providers::{Format as _, Toml},
    Figment,
};

use sea_orm::{ConnectOptions, Database, DbConn, EntityTrait};

use tokio::time;
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

mod categorization;
mod config;
mod database;
mod error;
mod index;
mod inscription;
mod rgbpp;
mod spore;
mod unique;
mod xudt;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let _ = dotenvy::dotenv();
    let config = load_config()?;

    match &cli.command {
        Commands::Run { apply_init_height } => {
            setup_logging(&config)?;

            info!("Version: {}", env!("CARGO_PKG_VERSION"));
            info!("config: {config:#?}");

            let db = setup_database(&config).await?;
            let client = config.http_fetcher().await?;

            let (initial_height, network, constants) =
                initialize_blockchain_data(&db, &config, *apply_init_height).await?;

            let mut indexer =
                index::Indexer::new(initial_height, &config, client, network, constants, db);

            run_with_watchdog(&mut indexer).await?;
        }
        Commands::FetchDepHeights => {
            let client = config.http_fetcher_without_redb().await?;
            let constants = Constants::from_config(config.unistate.optional_config.network);

            fetch_and_print_dep_heights(constants, &client).await?;
        }
    }

    Ok(())
}

async fn run_with_watchdog(indexer: &mut index::Indexer) -> Result<()> {
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
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the indexer
    #[command(name = "run")]
    Run {
        /// Apply initial height (true/false)
        #[arg(short, long, value_parser)]
        apply_init_height: Option<bool>,
    },
    /// Fetch and print dependency heights
    #[command(name = "fetch-dep-heights")]
    FetchDepHeights,
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
) -> Result<(u64, NetworkType, Constants)> {
    let mut initial_height = config.unistate.optional_config.initial_height;
    let apply_init_height =
        apply_init_height.unwrap_or(config.unistate.optional_config.apply_initial_height);

    if !apply_init_height {
        if let Some(block_height_entity) = block_height::Entity::find().one(db).await? {
            initial_height = initial_height.max(block_height_entity.height as u64);
        }
    }

    let network = config.unistate.optional_config.network;
    let constants = Constants::from_config(network);

    Ok((initial_height, network, constants))
}
