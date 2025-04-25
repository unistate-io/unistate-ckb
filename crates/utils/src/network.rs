use std::fmt;

use serde::{Deserialize, Serialize};

pub const PREFIX_MAINNET: &str = "ckb";
pub const PREFIX_TESTNET: &str = "ckt";

pub const NETWORK_MAINNET: &str = "ckb";
pub const NETWORK_TESTNET: &str = "ckb_testnet";

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NetworkType {
    Mainnet,
    Testnet,
}

impl NetworkType {
    pub fn from_prefix(value: &str) -> Option<NetworkType> {
        match value {
            PREFIX_MAINNET => Some(NetworkType::Mainnet),
            PREFIX_TESTNET => Some(NetworkType::Testnet),
            _ => None,
        }
    }

    pub fn to_prefix(self) -> &'static str {
        match self {
            NetworkType::Mainnet => PREFIX_MAINNET,
            NetworkType::Testnet => PREFIX_TESTNET,
        }
    }

    pub fn from_raw_str(value: &str) -> Option<NetworkType> {
        match value {
            NETWORK_MAINNET => Some(NetworkType::Mainnet),
            NETWORK_TESTNET => Some(NetworkType::Testnet),
            _ => None,
        }
    }

    pub fn to_str(self) -> &'static str {
        match self {
            NetworkType::Mainnet => NETWORK_MAINNET,
            NetworkType::Testnet => NETWORK_TESTNET,
        }
    }
}

impl fmt::Display for NetworkType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_str())
    }
}

#[derive(Debug, Clone)]
pub struct NetworkInfo {
    pub network_type: NetworkType,
    pub url: String,
}

impl NetworkInfo {
    pub fn new(network_type: NetworkType, url: String) -> Self {
        Self { network_type, url }
    }
    pub fn from_network_type(network_type: NetworkType) -> Option<Self> {
        match network_type {
            NetworkType::Mainnet => Some(Self::mainnet()),
            NetworkType::Testnet => Some(Self::testnet()),
        }
    }
    pub fn mainnet() -> Self {
        Self {
            network_type: NetworkType::Mainnet,
            url: "https://mainnet.ckb.dev".to_string(),
        }
    }
    pub fn testnet() -> Self {
        Self {
            network_type: NetworkType::Testnet,
            url: "https://testnet.ckb.dev".to_string(),
        }
    }
}
