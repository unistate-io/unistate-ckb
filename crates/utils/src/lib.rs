use ckb_fixed_hash_core::H160;
use ckb_jsonrpc_types::{JsonBytes, ResponseFormat};

pub mod address;
pub mod network;

pub fn blake160(message: &[u8]) -> H160 {
    let r = ckb_hash::blake2b_256(message);
    H160::from_slice(&r[..20]).unwrap()
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseFormatError {
    #[error("It's a JsonBytes, can't get the inner value directly")]
    NotValue,
    #[error("It's not a JsonBytes, can't get the json bytes directly")]
    NotJsonBytes,
}

pub trait ResponseFormatGetter<V> {
    fn get_value(self) -> Result<V, ResponseFormatError>;
    fn get_json_bytes(self) -> Result<JsonBytes, ResponseFormatError>;
}

impl<V> ResponseFormatGetter<V> for ResponseFormat<V> {
    fn get_value(self) -> Result<V, ResponseFormatError> {
        match self.inner {
            ckb_jsonrpc_types::Either::Left(v) => Ok(v),
            ckb_jsonrpc_types::Either::Right(_) => Err(ResponseFormatError::NotValue),
        }
    }

    fn get_json_bytes(self) -> Result<JsonBytes, ResponseFormatError> {
        match self.inner {
            ckb_jsonrpc_types::Either::Left(_v) => Err(ResponseFormatError::NotJsonBytes),
            ckb_jsonrpc_types::Either::Right(json_bytes) => Ok(json_bytes),
        }
    }
}
