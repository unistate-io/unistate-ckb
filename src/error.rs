use ckb_types::H256;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Encountered an issue with the JSON RPC client. Details: {0}")]
    JsonRpcClientError(#[from] jsonrpsee::core::client::Error),
    #[error("There was a problem deserializing data. Details: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("Failed to fetch the transaction. Error Code: {code}, Message: '{message}'")]
    FailedFetch { code: i32, message: String },
    #[error("The data received in a batched JSON-RPC response is not valid hexadecimal. Details: {0}")]
    FromHexError(#[from] hex::FromHexError),
    #[error("An I/O error occurred. Details: {0}")]
    IoError(#[from] std::io::Error),
    #[error("The previous output could not be found for the transaction '{tx_hash:?}' at index {index}. Please check your data.")]
    PreviousOutputNotFound { tx_hash: H256, index: u32 },
    #[error("The data for the previous output of the transaction '{tx_hash:?}' at index {index} could not be found. Please verify your inputs.")]
    PreviousOutputDataNotFound { tx_hash: H256, index: u32 },
}
