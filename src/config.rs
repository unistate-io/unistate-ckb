use ckb_sdk::NetworkType;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct Config {
    pub(crate) unistate: UnistateConfig,
    #[serde(default)]
    pub(crate) database_url: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct UnistateConfig {
    pub(crate) url: String,
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
}

impl Default for FeatcherConfig {
    fn default() -> Self {
        Self {
            retry_interval: 500,
            max_retries: 5,
            max_request_size: 100,
            max_response_size: 100,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default)]
pub(crate) struct UnistateConfigOptional {
    pub(crate) initial_height: u64,
    pub(crate) batch_size: u64,
    pub(crate) interval: f32,
    pub(crate) level: Level,
    pub(crate) network: NetworkType,
}

#[derive(Debug, PartialEq, Deserialize, Clone, Copy)]
pub(crate) enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Into<tracing::Level> for Level {
    fn into(self) -> tracing::Level {
        match self {
            Level::Trace => tracing::Level::TRACE,
            Level::Debug => tracing::Level::DEBUG,
            Level::Info => tracing::Level::INFO,
            Level::Warn => tracing::Level::WARN,
            Level::Error => tracing::Level::ERROR,
        }
    }
}

impl Default for UnistateConfigOptional {
    fn default() -> Self {
        Self {
            initial_height: 1,
            batch_size: 200,
            interval: 5.0,
            level: Level::Info,
            network: NetworkType::Mainnet,
        }
    }
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
                    url = "testurl"
                    # auth = { user = "user", password = "password" } 
                    featcher.max_retries = 3
                "#,
            )?;

            let config: Config = Figment::new().merge(Toml::file("Config.toml")).extract()?;

            println!("{config:?}");

            assert_eq!(
                config,
                Config {
                    database_url: "".into(),
                    unistate: UnistateConfig {
                        url: "testurl".into(),
                        optional_config: UnistateConfigOptional {
                            initial_height: 1000,
                            ..Default::default()
                        },
                        featcher: FeatcherConfig {
                            max_retries: 3,
                            ..Default::default()
                        }
                    }
                }
            );

            Ok(())
        });
    }
}
