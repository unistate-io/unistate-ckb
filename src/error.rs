use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("There was a problem deserializing data. Details: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error(
        "The data received in a batched JSON-RPC response is not valid hexadecimal. Details: {0}"
    )]
    FromHex(#[from] hex::FromHexError),
    #[error("An I/O error occurred. Details: {0}")]
    Io(#[from] std::io::Error),
    #[error("An error occurred related to inscriptions: {0}")]
    Inscription(#[from] crate::inscription::InscriptionError),
    #[error("An error occurred while fetching data: {0}")]
    Fetcher(#[from] fetcher::Error),
    #[error("An error occurred with redb: {0}")]
    Redb(#[from] fetcher::RedbError),
}
