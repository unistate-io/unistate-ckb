use ckb_types::H256;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("JSON RPC client error: {0}")]
    JsonRpcClientError(#[from] jsonrpsee::core::client::Error),
    #[error("Deserialization error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("Failed to fetch raw transaction. Error code: {code}, Message: {message}")]
    FailedFetch { code: i32, message: String },
    #[error("Result for batched JSON-RPC response is not valid hex: {0}")]
    FromHexError(#[from] hex::FromHexError),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Previous output not found: {tx_hash:?}, {index:?}")]
    PreviousOutputNotFound { tx_hash: H256, index: u32 },
    #[error("Previous output data not found: {tx_hash:?}, {index:?}")]
    PreviousOutputDataNotFound { tx_hash: H256, index: u32 },
}
