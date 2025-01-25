use std::path::PathBuf;

use ckb_sdk::NetworkType;
use constants::MB;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct Config {
    pub(crate) unistate: UnistateConfig,
    #[serde(default)]
    pub(crate) database_url: String,
    #[serde(default)]
    pub(crate) pool: PoolConfig,
}

impl Config {
    #[inline]
    fn init_redb(&self) -> Result<Option<fetcher::Database>, crate::error::Error> {
        let fc = &self.unistate.featcher;
        if !fc.disable_cached
            && fc.redb_path.extension().and_then(|ext| ext.to_str()) == Some("redb")
        {
            Ok(Some(
                fetcher::Database::create(fc.redb_path.as_path())
                    .map_err(|e| fetcher::Error::Database(fetcher::RedbError::from(e)))?,
            ))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn create_http_fetcher(
        &self,
        redb: Option<fetcher::Database>,
    ) -> Result<fetcher::HttpFetcher, crate::error::Error> {
        let fc = &self.unistate.featcher;
        let fetcher = fetcher::HttpFetcher::http_client(
            &self.unistate.urls,
            fc.retry_interval,
            fc.max_retries,
            fc.max_response_size,
            fc.max_request_size,
            redb,
        )
        .await?;
        Ok(fetcher)
    }

    pub async fn http_fetcher(&self) -> Result<fetcher::HttpFetcher, crate::error::Error> {
        let redb = self.init_redb()?;
        self.create_http_fetcher(redb).await
    }

    pub async fn http_fetcher_without_redb(
        &self,
    ) -> Result<fetcher::HttpFetcher, crate::error::Error> {
        self.create_http_fetcher(None).await
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct UnistateConfig {
    pub(crate) urls: Vec<String>,
    #[serde(flatten, default)]
    pub(crate) optional_config: UnistateConfigOptional,
    #[serde(default)]
    pub(crate) featcher: FeatcherConfig,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default)]
pub(crate) struct FeatcherConfig {
    pub(crate) retry_interval: u64,
    pub(crate) max_retries: usize,
    pub(crate) max_response_size: u32, // 默认是 10485760 即 10mb
    pub(crate) max_request_size: u32,
    pub(crate) redb_path: PathBuf,
    pub(crate) disable_cached: bool,
}

impl Default for FeatcherConfig {
    fn default() -> Self {
        Self {
            retry_interval: 500,
            max_retries: 5,
            max_request_size: 100 * MB,
            max_response_size: 100 * MB,
            redb_path: PathBuf::from("unistate.redb"),
            disable_cached: true,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default)]
pub(crate) struct UnistateConfigOptional {
    pub(crate) initial_height: u64,
    pub(crate) batch_size: u64,
    pub(crate) fetch_size: usize,
    pub(crate) interval: f32,
    pub(crate) level: Level,
    pub(crate) network: NetworkType,
    pub(crate) apply_initial_height: bool,
}

#[derive(Debug, PartialEq, Deserialize, Clone, Copy)]
pub(crate) enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<Level> for tracing::Level {
    fn from(val: Level) -> Self {
        match val {
            Level::Trace => Self::TRACE,
            Level::Debug => Self::DEBUG,
            Level::Info => Self::INFO,
            Level::Warn => Self::WARN,
            Level::Error => Self::ERROR,
        }
    }
}

impl Default for UnistateConfigOptional {
    fn default() -> Self {
        Self {
            initial_height: 1,
            batch_size: 200,
            fetch_size: 5,
            interval: 1.0,
            level: Level::Info,
            network: NetworkType::Mainnet,
            apply_initial_height: false,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Default)]
#[serde(default)]
pub(crate) struct PoolConfig {
    pub(crate) max_connections: Option<u32>,
    pub(crate) min_connections: Option<u32>,
    pub(crate) connection_timeout: Option<u64>,
    pub(crate) acquire_timeout: Option<u64>,
    pub(crate) idle_timeout: Option<u64>,
    pub(crate) max_lifetime: Option<u64>,
    pub(crate) sqlx_logging: bool,
}

#[cfg(test)]
mod tests {
    use figment::{
        providers::{Format as _, Toml},
        Figment,
    };

    use super::*;

    #[test]
    fn test_config() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "Config.toml",
                r#"
                    [unistate]
                    initial_height = 1000
                    urls = ["testurl"]
                    network = "Testnet"
                    # auth = { user = "user", password = "password" }
                    featcher.max_retries = 3
                    [pool]
                    max_connections = 11
                "#,
            )?;

            let config: Config = Figment::new().merge(Toml::file("Config.toml")).extract()?;

            println!("{config:?}");

            assert_eq!(
                config,
                Config {
                    database_url: "".into(),
                    unistate: UnistateConfig {
                        urls: vec!["testurl".into()],
                        optional_config: UnistateConfigOptional {
                            initial_height: 1000,
                            network: NetworkType::Testnet,
                            ..Default::default()
                        },
                        featcher: FeatcherConfig {
                            max_retries: 3,
                            ..Default::default()
                        }
                    },
                    pool: PoolConfig {
                        max_connections: Some(11),
                        ..Default::default()
                    }
                }
            );

            Ok(())
        });
    }
}
