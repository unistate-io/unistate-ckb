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
    // #[error("Failed to fetch raw transactions after {retries} with {error}")]
    // FetchRetryError {
    //     error: jsonrpsee::core::client::Error,
    //     retries: usize,
    // },
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    // #[error("Invalid auth cookie file. Please check the file permissions and contents")]
    // InvalidCookieFile,
    // #[error(
    //     "Invalid height. Target height {target_height} is smaller than the current height {height}"
    // )]
    // InvalidHeight { target_height: u32, height: u32 },
    // #[error("Invalid json resopnse.")]
    // InvalidJsonResponse,
    // #[error("blcok chain reorg unrecoverable!")]
    // Unrecoverable,
}
