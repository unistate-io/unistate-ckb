#![allow(clippy::type_complexity)]
mod wrapper;

use ckb_jsonrpc_types::{
    BlockNumber, BlockView, CellInput, CellOutput, JsonBytes, OutPoint, Transaction,
    TransactionWithStatusResponse,
};
use ckb_types::H256;
use futures::FutureExt; // Required for .boxed()
use jsonrpsee::{
    core::{client::Error as JsonRpseeError, params::BatchRequestBuilder},
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use redb::{ReadableTable as _, TableDefinition};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    ops::{Bound, RangeBounds},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use utils::ResponseFormatGetter as _; // Import the trait for use
use wrapper::Bincode;

pub use jsonrpsee::core::client::ClientT;
pub use redb::{Database, Error as RedbError};

const TX_TABLE: TableDefinition<Bincode<H256>, Bincode<Transaction>> =
    TableDefinition::new("transactions");

const TX_COUNT_TABLE: TableDefinition<Bincode<H256>, u64> =
    TableDefinition::new("transaction_counts");

static DB: OnceLock<Database> = OnceLock::new();

// --- Error Enum ---
#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to fetch data from RPC: code={code}, message={message}")]
    FailedFetch { code: i32, message: String },

    #[error(
        "Previous output not found: tx_hash={tx_hash:#x}, index={index}, outputs_len={outputs_len}"
    )]
    PreviousOutputNotFound {
        tx_hash: H256,
        index: u32,
        outputs_len: usize,
    },

    #[error(
        "Previous output data not found: tx_hash={tx_hash:#x}, index={index}, outputs_data_len={outputs_data_len}"
    )]
    PreviousOutputDataNotFound {
        tx_hash: H256,
        index: u32,
        outputs_data_len: usize,
    },

    #[error("Encountered an issue with the JSON RPC client: {0}")]
    JsonRpcClient(#[from] JsonRpseeError), // Can't clone this easily

    #[error("There was a problem serializing/deserializing data: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Database error: {0}")]
    Database(#[from] RedbError),

    #[error("Database not initialized")]
    DatabaseNotInitialized,

    #[error("Database already initialized")]
    DatabaseAlreadyInitialized,

    #[error("Fetcher already initialized")]
    FetcherAlreadyInitialized,

    #[error("Fetcher not initialized")]
    FetcherNotInitialized,

    #[error("No RPC clients configured")]
    NoClientsConfigured,

    // Store specific error string for display, as JsonRpseeError isn't easily cloneable/storable
    #[error("All retry attempts failed for RPC call '{method}': Last error: {last_error_message}")]
    RetryAttemptsExhausted {
        method: &'static str,
        last_error_message: String,
    },

    #[error("All retry attempts failed for batch RPC request: Last error: {last_error_message}")]
    BatchRetryAttemptsExhausted { last_error_message: String },

    #[error("Failed to build batch request: {0}")]
    BatchBuildFailed(String), // Store error message string
}

// --- Fetcher Struct ---
#[derive(Debug)]
pub struct Fetcher<C> {
    clients: SmallVec<[C; 2]>,
    current_index: Arc<AtomicUsize>,
    retry_interval: u64,
    max_retries: usize,
}

// --- Statics and Initialization ---
static FETCHER: OnceLock<Fetcher<HttpClient>> = OnceLock::new();

pub async fn init_http_fetcher(
    urls: impl IntoIterator<Item = impl AsRef<str>>,
    retry_interval: u64,
    max_retries: usize,
    max_response_size: u32,
    max_request_size: u32,
    sort_interval_secs: Option<u64>,
) -> Result<(), Error> {
    let fetcher = Fetcher::http_client(
        urls,
        retry_interval,
        max_retries,
        max_response_size,
        max_request_size,
        sort_interval_secs,
    )
    .await?;

    FETCHER
        .set(fetcher)
        .map_err(|_| Error::FetcherAlreadyInitialized)
}

pub fn get_fetcher() -> Result<&'static Fetcher<HttpClient>, Error> {
    FETCHER.get().ok_or(Error::FetcherNotInitialized)
}

pub fn init_db(db: Database) -> Result<(), Error> {
    let write_txn = db.begin_write().map_err(redb::Error::from)?;
    {
        let _ = write_txn.open_table(TX_TABLE).map_err(redb::Error::from)?;
        let _ = write_txn
            .open_table(TX_COUNT_TABLE)
            .map_err(redb::Error::from)?;
    }
    write_txn.commit().map_err(redb::Error::from)?;

    // Use map_err to prevent setting if already initialized
    DB.set(db).map_err(|_| Error::DatabaseAlreadyInitialized)
}

pub fn get_db() -> Result<&'static Database, Error> {
    DB.get().ok_or(Error::DatabaseNotInitialized)
}

// --- Cache Module ---
mod cache {
    use super::*;
    use ckb_jsonrpc_types::TransactionView;

    pub fn clear_transactions_below_count(
        db: &redb::Database,
        threshold: u64,
    ) -> Result<(), RedbError> {
        let write_txn = db.begin_write()?;
        {
            let mut tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

            let to_remove: Vec<H256> = count_table
                .iter()?
                .filter_map(|result| match result {
                    Ok((hash_guard, count_guard)) if count_guard.value() < threshold => {
                        Some(hash_guard.value())
                    }
                    _ => None,
                })
                .collect();

            for hash in to_remove {
                tx_table.remove(&hash)?;
                count_table.remove(&hash)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_cached_transactions(
        db: &redb::Database,
        hashes: &[H256],
    ) -> Result<HashMap<H256, Transaction>, RedbError> {
        let write_txn = db.begin_write()?;
        let mut cached_txs = HashMap::new();
        {
            let tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

            for hash in hashes {
                if let Some(tx_guard) = tx_table.get(hash)? {
                    let current_count = count_table.get(hash)?.map_or(0, |ag| ag.value()); // Use map_or instead of unwrap_or_default
                    // Increment count on retrieval
                    count_table.insert(hash, &(current_count + 1))?;
                    cached_txs.insert(hash.clone(), tx_guard.value());
                }
            }
        }
        write_txn.commit()?;
        Ok(cached_txs)
    }

    pub fn cache_transactions(
        db: &redb::Database,
        txs: &[TransactionWithStatusResponse], // Use slice
    ) -> Result<HashMap<H256, Transaction>, RedbError> {
        let write_txn = db.begin_write()?;
        let mut fetched_txs = HashMap::new();
        {
            let mut tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

            for tx_response in txs {
                if let Some(tx_view) = &tx_response.transaction {
                    // Use ResponseFormatGetter trait, handle potential error from get_value
                    match tx_view.clone().get_value() {
                        Ok(tx) => {
                            let hash = tx.hash.clone();
                            let inner_tx = tx.inner.clone();
                            // Insert tx, check if it was new
                            if tx_table.insert(&hash, &inner_tx)?.is_none() {
                                // Only initialize count if it was newly inserted
                                count_table.insert(&hash, &0u64)?;
                            }
                            fetched_txs.insert(hash, inner_tx);
                        }
                        Err(e) => {
                            // Log or handle error if get_value fails (e.g., unexpected format)
                            warn!("Failed to get transaction value during caching: {}", e);
                        }
                    }
                }
            }
        }
        write_txn.commit()?;
        Ok(fetched_txs)
    }

    pub fn cache_transaction(db: &redb::Database, tx: &TransactionView) -> Result<(), RedbError> {
        let write_txn = db.begin_write()?;
        {
            let mut tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;
            let hash = tx.hash.clone();
            let inner_tx = tx.inner.clone();
            if tx_table.insert(&hash, &inner_tx)?.is_none() {
                count_table.insert(&hash, &0u64)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_max_count_transaction(
        db: &redb::Database,
    ) -> Result<Option<(H256, u64)>, RedbError> {
        let read_txn = db.begin_read()?;
        let count_table = read_txn.open_table(TX_COUNT_TABLE)?;

        let max_entry = count_table
            .iter()?
            .filter_map(|result| result.ok()) // Filter out potential iteration errors
            .max_by_key(|(_, count)| count.value());

        Ok(max_entry.map(|(hash, count)| (hash.value(), count.value())))
    }
}
pub use cache::*;

/// Helper to check if a jsonrpsee error is retryable
#[inline]
fn is_retryable_error(err: &JsonRpseeError) -> bool {
    matches!(
        err,
        JsonRpseeError::RequestTimeout
            | JsonRpseeError::Transport(_)
            | JsonRpseeError::RestartNeeded(_)
    )
}

// --- Fetcher Implementation ---
impl<C> Fetcher<C>
where
    C: ClientT + Send + Clone + 'static + Sync,
{
    pub fn new(clients: SmallVec<[C; 2]>, retry_interval: u64, max_retries: usize) -> Self {
        Self {
            clients,
            retry_interval,
            max_retries,
            current_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Selects the fastest client and updates the shared index.
    pub async fn sort_clients_by_speed(&self) -> Result<usize, Error> {
        if self.clients.is_empty() {
            return Err(Error::NoClientsConfigured);
        }

        // 创建可并行的 Future 列表（包含超时控制）
        let futures = self.clients.iter().enumerate().map(|(index, client)| {
            let client = client.clone();
            async move {
                let start = tokio::time::Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_secs(2), // 添加超时控制
                    client.request::<BlockNumber, _>("get_tip_block_number", rpc_params!()),
                )
                .await;

                match result {
                    Ok(Ok(_)) => Ok((index, start.elapsed())),
                    Ok(Err(e)) => {
                        warn!("Client {} failed: {}", index, e);
                        Err(e)
                    }
                    Err(_) => {
                        warn!("Client {} timed out", index);
                        Err(JsonRpseeError::RequestTimeout)
                    }
                }
            }
            .boxed()
        });

        // 使用 select_ok 选择第一个成功的响应
        match futures::future::select_ok(futures).await {
            Ok(((fastest_index, duration), _remaining_futures)) => {
                info!(
                    "Selected fastest client {} in {:?}",
                    fastest_index, duration
                );
                self.current_index.store(fastest_index, Ordering::Relaxed);
                Ok(fastest_index)
            }
            Err(e) => {
                error!("All clients failed speed test: {}", e);
                Err(Error::RetryAttemptsExhausted {
                    method: "get_tip_block_number",
                    last_error_message: e.to_string(),
                })
            }
        }
    }

    /// Starts a background task that periodically sorts clients by speed.
    pub fn start_background_sorting(&self, interval_secs: u64) {
        if self.clients.len() <= 1 {
            info!("Background sorting skipped: only one or zero clients configured.");
            return;
        }
        let clients = self.clients.clone();
        let current_index_arc = self.current_index.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let mut measurements = Vec::with_capacity(clients.len());

                for (index, client) in clients.iter().enumerate() {
                    let start = std::time::Instant::now();
                    let result = client
                        .request::<BlockNumber, _>("get_tip_block_number", rpc_params!())
                        .await;
                    let duration = start.elapsed();

                    if result.is_ok() {
                        measurements.push((index, duration));
                    } // Don't log failures here, they are expected sometimes
                }

                // Find the fastest index among successful responses
                if let Some((fastest_idx, _)) = measurements.iter().min_by_key(|(_, d)| *d) {
                    let current_fastest = current_index_arc.load(Ordering::Relaxed);
                    if *fastest_idx != current_fastest {
                        // Update only if the fastest client has changed
                        current_index_arc.store(*fastest_idx, Ordering::Relaxed);
                        debug!(
                            "Background sort updated fastest client from {} to {}",
                            current_fastest, *fastest_idx
                        );
                    } else {
                        debug!(
                            "Background sort confirmed fastest client is still {}",
                            current_fastest
                        );
                    }
                } else {
                    // Only log if NO client responded, which might indicate a larger issue
                    warn!(
                        "Background sort failed to determine fastest client (no successful responses). Keeping current index."
                    );
                }
            }
        });
    }

    /// Makes a single RPC call with retry and client switching logic.
    #[inline]
    fn call<Params, R>(
        &self,
        method: &'static str,
        params: Params,
    ) -> impl Future<Output = Result<R, Error>> + Send + 'static
    where
        Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + 'static,
        R: jsonrpsee::core::DeserializeOwned + Send + 'static,
    {
        let clients = self.clients.clone();
        let current_index_arc = self.current_index.clone();
        let retry_interval = self.retry_interval;
        let max_retries = self.max_retries;
        let params_clone = params;

        async move {
            if clients.is_empty() {
                return Err(Error::NoClientsConfigured);
            }

            let mut active_client_index = current_index_arc.load(Ordering::Relaxed);
            let mut retries_done = 0;
            let mut switched_client = false;

            // Store the error *message* string for the final error report if needed
            #[allow(unused_assignments)]
            let mut last_error_message: String = "No error recorded".to_string();

            loop {
                // Ensure index is valid before using unsafe block
                if active_client_index >= clients.len() {
                    warn!(
                        "Invalid client index {} detected during call, resetting to 0",
                        active_client_index
                    );
                    active_client_index = 0;
                    current_index_arc.store(active_client_index, Ordering::Relaxed);
                }
                // Safety: active_client_index is guaranteed to be < clients.len() here.
                let client = unsafe { clients.get_unchecked(active_client_index) }.clone();

                debug!(
                    "Attempting call '{}' on client {} (Attempt {}/{}, Switched: {})",
                    method,
                    active_client_index,
                    retries_done + 1,
                    max_retries + 1,
                    switched_client
                );

                match client.request::<R, _>(method, params_clone.clone()).await {
                    Ok(response) => return Ok(response),
                    Err(rpc_error) => {
                        // Store the current error message immediately
                        last_error_message = rpc_error.to_string();

                        if !is_retryable_error(&rpc_error) {
                            warn!(
                                "Non-retryable error on call '{}' with client {}: {}. Failing.",
                                method, active_client_index, last_error_message
                            );
                            // Return the non-retryable error directly
                            return Err(Error::JsonRpcClient(rpc_error));
                        }

                        // Is a retryable error, proceed with retry logic
                        retries_done += 1;

                        if retries_done > max_retries {
                            if !switched_client {
                                // Exhausted retries on the current client, try switching
                                warn!(
                                    "Call '{}' failed after {} retries on client {}. Finding and switching to the fastest client.",
                                    method,
                                    max_retries + 1,
                                    active_client_index
                                );

                                // --- Find the new fastest client (inline check) ---
                                let mut measurements = Vec::with_capacity(clients.len());
                                for (index, c) in clients.iter().enumerate() {
                                    if index == active_client_index {
                                        continue;
                                    } // Skip the already failed one
                                    let start = std::time::Instant::now();
                                    let result = c
                                        .request::<BlockNumber, _>(
                                            "get_tip_block_number",
                                            rpc_params!(),
                                        )
                                        .await;
                                    let duration = start.elapsed();
                                    if result.is_ok() {
                                        measurements.push((index, duration));
                                    }
                                }

                                if let Some((fastest_index, _)) =
                                    measurements.iter().min_by_key(|(_, d)| *d)
                                {
                                    // Switch client successfully
                                    current_index_arc.store(*fastest_index, Ordering::Relaxed);
                                    active_client_index = *fastest_index;
                                    retries_done = 0; // Reset retries for the new client
                                    switched_client = true;
                                    warn!(
                                        "Switched to new fastest client {} for call '{}'. Resetting retries.",
                                        active_client_index, method
                                    );
                                    // Loop continues to try the new client immediately
                                } else {
                                    // Switching failed (no other client responded)
                                    warn!(
                                        "Call '{}' failed on client {}, and no other clients responded. Failing.",
                                        method, active_client_index
                                    );
                                    // Return the specific exhaustion error with the last message
                                    return Err(Error::RetryAttemptsExhausted {
                                        method,
                                        last_error_message,
                                    });
                                }
                            } else {
                                // Exhausted retries even after switching
                                warn!(
                                    "Call '{}' failed after {} retries even on the switched fastest client {}. Giving up.",
                                    method,
                                    max_retries + 1,
                                    active_client_index
                                );
                                // Return the specific exhaustion error with the last message
                                return Err(Error::RetryAttemptsExhausted {
                                    method,
                                    last_error_message,
                                });
                            }
                        } else {
                            // Retryable error, within limits for the *current* client
                            warn!(
                                "Retryable error on call '{}' with client {} (Attempt {}/{}): {}. Retrying after {} ms...",
                                method,
                                active_client_index,
                                retries_done,
                                max_retries + 1,
                                last_error_message,
                                retry_interval
                            );
                            tokio::time::sleep(Duration::from_millis(retry_interval)).await;
                            // Loop continues to retry on the same client
                        }
                    }
                }
            }
        }
    }

    /// Makes a batch RPC request with retry and client switching logic.
    #[inline]
    fn batch_request<R>(
        &self,
        batch: BatchRequestBuilder<'static>, // Take builder directly
    ) -> impl Future<Output = Result<Vec<R>, Error>> + Send + 'static
    where
        R: jsonrpsee::core::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    {
        let clients = self.clients.clone();
        let current_index_arc = self.current_index.clone();
        let retry_interval = self.retry_interval;
        let max_retries = self.max_retries;
        let initial_batch = batch; // Clone builder once initially

        async move {
            if clients.is_empty() {
                return Err(Error::NoClientsConfigured);
            }
            // Note: jsonrpsee handles empty batch, no need for check here.

            let mut active_client_index = current_index_arc.load(Ordering::Relaxed);
            let mut retries_done = 0;
            let mut switched_client = false;
            #[allow(unused_assignments)]
            let mut last_error_message: String = "No error recorded".to_string();

            loop {
                // Ensure index is valid
                if active_client_index >= clients.len() {
                    warn!(
                        "Invalid client index {} detected during batch, resetting to 0",
                        active_client_index
                    );
                    active_client_index = 0;
                    current_index_arc.store(active_client_index, Ordering::Relaxed);
                }
                // Safety: active_client_index is guaranteed to be < clients.len() here.
                let client = unsafe { clients.get_unchecked(active_client_index) }.clone();
                let current_batch = initial_batch.clone(); // Clone builder for this attempt

                debug!(
                    "Attempting batch request on client {} (Attempt {}/{}, Switched: {})",
                    active_client_index,
                    retries_done + 1,
                    max_retries + 1,
                    switched_client
                );

                // Use .boxed() to help the compiler with async trait method futures
                match client.batch_request::<R>(current_batch).boxed().await {
                    Ok(response) => {
                        // Process results, converting individual item errors
                        let results = response
                            .into_iter()
                            .map(|res| {
                                res.map_err(|err| Error::FailedFetch {
                                    code: err.code(),
                                    message: err.message().into(),
                                })
                            })
                            .collect::<Result<Vec<R>, Error>>();
                        // Propagates the first FailedFetch error if any occurred
                        return results;
                    }
                    Err(rpc_error) => {
                        last_error_message = rpc_error.to_string();

                        if !is_retryable_error(&rpc_error) {
                            warn!(
                                "Non-retryable error on batch request with client {}: {}. Failing.",
                                active_client_index, last_error_message
                            );
                            return Err(Error::JsonRpcClient(rpc_error));
                        }

                        retries_done += 1;

                        if retries_done > max_retries {
                            if !switched_client {
                                warn!(
                                    "Batch request failed after {} retries on client {}. Finding and switching.",
                                    max_retries + 1,
                                    active_client_index
                                );
                                // --- Find fastest *other* client ---
                                let mut measurements = Vec::with_capacity(clients.len());
                                for (index, c) in clients.iter().enumerate() {
                                    if index == active_client_index {
                                        continue;
                                    }
                                    let start = std::time::Instant::now();
                                    let result = c
                                        .request::<BlockNumber, _>(
                                            "get_tip_block_number",
                                            rpc_params!(),
                                        )
                                        .await;
                                    let duration = start.elapsed();
                                    if result.is_ok() {
                                        measurements.push((index, duration));
                                    }
                                }

                                if let Some((fastest_index, _)) =
                                    measurements.iter().min_by_key(|(_, d)| *d)
                                {
                                    current_index_arc.store(*fastest_index, Ordering::Relaxed);
                                    active_client_index = *fastest_index;
                                    retries_done = 0;
                                    switched_client = true;
                                    warn!(
                                        "Switched to new fastest client {} for batch request. Resetting retries.",
                                        active_client_index
                                    );
                                    // Loop continues
                                } else {
                                    warn!(
                                        "Batch request failed on client {}, and no other clients responded. Failing.",
                                        active_client_index
                                    );
                                    return Err(Error::BatchRetryAttemptsExhausted {
                                        last_error_message,
                                    });
                                }
                            } else {
                                warn!(
                                    "Batch request failed after {} retries even on the switched client {}. Giving up.",
                                    max_retries + 1,
                                    active_client_index
                                );
                                return Err(Error::BatchRetryAttemptsExhausted {
                                    last_error_message,
                                });
                            }
                        } else {
                            warn!(
                                "Retryable error on batch request with client {} (Attempt {}/{}): {}. Retrying after {} ms...",
                                active_client_index,
                                retries_done,
                                max_retries + 1,
                                last_error_message,
                                retry_interval
                            );
                            tokio::time::sleep(Duration::from_millis(retry_interval)).await;
                            // Loop continues
                        }
                    }
                }
            }
        }
    }

    // --- High-Level Data Fetching Methods ---

    pub async fn get_txs_by_hashes(
        &self,
        hashes: Vec<H256>,
    ) -> Result<HashMap<H256, Transaction>, Error> {
        debug!("Getting transactions by hashes (count: {})", hashes.len());
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result_map = HashMap::new();
        let mut hashes_to_fetch = hashes; // Start with all hashes

        // 1. Attempt cache lookup
        if let Ok(db) = get_db() {
            match cache::get_cached_transactions(db, &hashes_to_fetch) {
                Ok(cached_txs) => {
                    let found_count = cached_txs.len();
                    debug!("Found {} transactions in cache", found_count);
                    result_map.extend(cached_txs);
                    // Filter out hashes that were found in cache
                    hashes_to_fetch = hashes_to_fetch
                        .into_iter()
                        .filter(|h| !result_map.contains_key(h))
                        .collect();
                }
                Err(e) => {
                    // Log DB error but continue without cache
                    warn!(
                        "Failed to get cached transactions: {}. Proceeding via RPC.",
                        e
                    );
                }
            }
        } else {
            debug!("Database not initialized, fetching all transactions via RPC.");
        }

        // 2. Fetch remaining hashes via RPC
        if !hashes_to_fetch.is_empty() {
            debug!(
                "Fetching {} missing transactions via RPC",
                hashes_to_fetch.len()
            );
            // get_txs returns Vec<Option<TxWithStatusResp>>
            let fetched_responses_opts = self.get_txs(hashes_to_fetch).await?;

            // Filter out None results (tx not found on node)
            let valid_responses: Vec<TransactionWithStatusResponse> =
                fetched_responses_opts.into_iter().flatten().collect();

            if !valid_responses.is_empty() {
                let mut newly_fetched_txs = HashMap::new();

                // Attempt to cache newly fetched transactions
                if let Ok(db) = get_db() {
                    if !newly_fetched_txs.is_empty() {
                        // Only cache if we actually got some
                        match cache::cache_transactions(db, &valid_responses) {
                            Ok(cached_map) => {
                                // The map returned by cache_transactions should match newly_fetched_txs
                                debug_assert_eq!(cached_map.len(), newly_fetched_txs.len());
                                debug!("Cached {} newly fetched transactions", cached_map.len());
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to cache {} fetched transactions: {}",
                                    newly_fetched_txs.len(),
                                    e
                                );
                            }
                        }
                    }
                }

                // Extract transactions from valid responses
                for tx_response in valid_responses {
                    if let Some(tx_view) = tx_response.transaction {
                        match tx_view.get_value() {
                            Ok(tx) => {
                                newly_fetched_txs.insert(tx.hash, tx.inner);
                            }
                            Err(e) => warn!("Failed to get tx value from RPC response: {}", e),
                        }
                    }
                }

                // Add newly fetched transactions to the final result
                result_map.extend(newly_fetched_txs);
            }
        }

        debug!(
            "Finished get_txs_by_hashes. Total found: {}",
            result_map.len()
        );
        Ok(result_map)
    }

    pub async fn get_outputs_ignore_not_found(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<CellOutput>, Error> {
        debug!(
            "Getting outputs (ignore not found) for {} inputs",
            inputs.len()
        );
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        // Deduplicate transaction hashes needed
        let unique_hashes: HashSet<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect();

        // Exclude the zero hash (cellbase input)
        let unique_hashes_vec: Vec<H256> = unique_hashes
            .into_iter()
            .filter(|h| *h != H256::default())
            .collect();

        if unique_hashes_vec.is_empty() {
            return Ok(Vec::new());
        } // All inputs were cellbase

        // Fetch the required transactions (potentially from cache or RPC)
        debug!(
            "Fetching transactions for {} unique non-cellbase hashes",
            unique_hashes_vec.len()
        );
        let tx_map = self.get_txs_by_hashes(unique_hashes_vec).await?;

        // Find the corresponding outputs, filtering out misses
        let outputs = inputs
            .into_par_iter()
            .filter_map(|input| {
                let idx = input.previous_output.index.value() as usize;
                // Get the transaction, then get the output at the index
                tx_map
                    .get(&input.previous_output.tx_hash)
                    .and_then(|tx| tx.outputs.get(idx).cloned())
            })
            .collect::<Vec<_>>();

        debug!("Got {} outputs (ignore not found)", outputs.len());
        Ok(outputs)
    }

    pub async fn get_outputs(&self, inputs: Vec<CellInput>) -> Result<Vec<CellOutput>, Error> {
        debug!("Getting outputs for {} inputs", inputs.len());
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let unique_hashes: HashSet<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect();

        let unique_hashes_vec: Vec<H256> = unique_hashes
            .into_iter()
            .filter(|h| *h != H256::default())
            .collect();

        if unique_hashes_vec.is_empty() {
            return Ok(Vec::new());
        } // All inputs were cellbase

        debug!(
            "Fetching transactions for {} unique non-cellbase hashes",
            unique_hashes_vec.len()
        );
        let tx_map = self.get_txs_by_hashes(unique_hashes_vec).await?;

        // Map inputs to outputs, returning error on the first miss
        let outputs = inputs
            .into_par_iter()
            .map(|input| {
                let tx_hash = input.previous_output.tx_hash.clone();
                let index = input.previous_output.index.value();
                let idx = index as usize;

                // Handle cellbase inputs explicitly if they weren't filtered
                if tx_hash == H256::default() {
                    // This case shouldn't be reached if unique_hashes_vec was filtered correctly,
                    // but handle defensively. Return an error indicating invalid input.
                    return Err(Error::PreviousOutputNotFound {
                        tx_hash,
                        index,
                        outputs_len: 0, // Indicate invalid input
                    });
                }

                // Find the transaction in the map
                match tx_map.get(&tx_hash) {
                    Some(tx) => {
                        // Find the output within the transaction
                        tx.outputs
                            .get(idx)
                            .cloned()
                            .ok_or_else(|| Error::PreviousOutputNotFound {
                                tx_hash: tx_hash.clone(),
                                index,
                                outputs_len: tx.outputs.len(),
                            })
                    }
                    None => {
                        // Transaction itself wasn't found (e.g., pruned or never existed)
                        Err(Error::PreviousOutputNotFound {
                            tx_hash: tx_hash.clone(),
                            index,
                            outputs_len: 0, // Indicate Tx not found
                        })
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?; // Collect results, propagating the first error

        debug!("Got {} outputs", outputs.len());
        Ok(outputs)
    }

    pub async fn get_outputs_with_data(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!("Getting outputs with data for {} inputs", inputs.len());
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let unique_hashes: HashSet<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect();

        let unique_hashes_vec: Vec<H256> = unique_hashes
            .into_iter()
            .filter(|h| *h != H256::default())
            .collect();

        if unique_hashes_vec.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Fetching transactions for {} unique non-cellbase hashes",
            unique_hashes_vec.len()
        );
        let tx_map = self.get_txs_by_hashes(unique_hashes_vec).await?;

        let outputs_with_data = inputs
            .into_par_iter()
            .map(|input| {
                let out_point = input.previous_output.clone();
                let tx_hash = &out_point.tx_hash;
                let index = out_point.index.value();
                let idx = index as usize;

                if *tx_hash == H256::default() {
                    // Should not happen if filtered
                    return Err(Error::PreviousOutputNotFound {
                        tx_hash: tx_hash.clone(),
                        index,
                        outputs_len: 0,
                    });
                }

                match tx_map.get(tx_hash) {
                    Some(tx) => {
                        // Use ? to propagate errors early
                        let output = tx.outputs.get(idx).cloned().ok_or_else(|| {
                            Error::PreviousOutputNotFound {
                                tx_hash: tx_hash.clone(),
                                index,
                                outputs_len: tx.outputs.len(),
                            }
                        })?;
                        let data = tx.outputs_data.get(idx).cloned().ok_or_else(|| {
                            Error::PreviousOutputDataNotFound {
                                tx_hash: tx_hash.clone(),
                                index,
                                outputs_data_len: tx.outputs_data.len(),
                            }
                        })?;
                        Ok((out_point, output, data))
                    }
                    None => Err(Error::PreviousOutputNotFound {
                        // Transaction itself wasn't found
                        tx_hash: tx_hash.clone(),
                        index,
                        outputs_len: 0,
                    }),
                }
            })
            .collect::<Result<Vec<_>, _>>()?; // Collect results, propagating the first error

        debug!("Got {} outputs with data", outputs_with_data.len());
        Ok(outputs_with_data)
    }

    pub async fn get_outputs_with_data_ignore_not_found(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!(
            "Getting outputs with data (ignore not found) for {} inputs",
            inputs.len()
        );
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let unique_hashes: HashSet<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect();

        let unique_hashes_vec: Vec<H256> = unique_hashes
            .into_iter()
            .filter(|h| *h != H256::default())
            .collect();

        if unique_hashes_vec.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Fetching transactions for {} unique non-cellbase hashes",
            unique_hashes_vec.len()
        );
        let tx_map = self.get_txs_by_hashes(unique_hashes_vec).await?;

        let outputs_with_data = inputs
            .into_par_iter()
            .filter_map(|input| {
                // Use filter_map to ignore errors/misses
                let out_point = input.previous_output; // No clone needed if moved
                let idx = out_point.index.value() as usize;
                // Chain and_then calls to handle misses gracefully
                tx_map.get(&out_point.tx_hash).and_then(|tx| {
                    tx.outputs.get(idx).and_then(|output| {
                        tx.outputs_data
                            .get(idx)
                            .map(|data| (out_point, output.clone(), data.clone()))
                    }) // Clone output/data for result
                })
            })
            .collect::<Vec<_>>();

        debug!(
            "Got {} outputs with data (ignore not found)",
            outputs_with_data.len()
        );
        Ok(outputs_with_data)
    }

    // --- Direct RPC Method Wrappers ---

    pub async fn get_tip_block_number(&self) -> Result<BlockNumber, Error> {
        self.call("get_tip_block_number", rpc_params!()).await
    }

    /// Fetches a block by number. Returns `Ok(None)` if the block is not found on the node.
    pub async fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> Result<Option<BlockView>, Error> {
        self.call("get_block_by_number", rpc_params!(number)).await
    }

    /// Fetches multiple blocks by number using a batch request.
    /// The result vector contains `None` for blocks not found on the node.
    pub async fn get_blocks(
        &self,
        numbers: Vec<BlockNumber>,
    ) -> Result<Vec<Option<BlockView>>, Error> {
        if numbers.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch_request = BatchRequestBuilder::new();
        for number in numbers {
            // Handle potential error from insert (though unlikely for valid params)
            if let Err(e) = batch_request.insert("get_block_by_number", rpc_params!(number)) {
                return Err(Error::BatchBuildFailed(e.to_string()));
            }
        }
        // The type parameter R is Option<BlockView> because get_block_by_number RPC can return null.
        self.batch_request::<Option<BlockView>>(batch_request).await
    }

    /// Fetches blocks within a given range (inclusive start, inclusive end).
    pub async fn get_blocks_range<R>(&self, range: R) -> Result<Vec<Option<BlockView>>, Error>
    where
        R: RangeBounds<BlockNumber>,
    {
        // Determine start block number
        let start_num = match range.start_bound() {
            Bound::Included(n) => n.value(),
            Bound::Excluded(n) => n.value().saturating_add(1), // Add 1 for excluded lower bound
            Bound::Unbounded => 0,                             // Default to genesis block
        };

        // Determine end block number
        let end_num = match range.end_bound() {
            Bound::Included(n) => n.value(),
            Bound::Excluded(n) => n.value().saturating_sub(1), // Subtract 1 for excluded upper bound
            Bound::Unbounded => self.get_tip_block_number().await?.value(), // Fetch tip if unbounded
        };

        // Handle invalid or empty ranges
        if start_num > end_num {
            return Ok(Vec::new());
        }

        // Consider adding a limit to prevent excessive requests
        // const MAX_RANGE_SIZE: u64 = 1000; // Example limit
        // if end_num.saturating_sub(start_num) >= MAX_RANGE_SIZE {
        //     return Err(Error::...?("Range too large")); // Define an appropriate error
        // }

        // Generate the list of block numbers for the batch request
        let block_numbers: Vec<BlockNumber> =
            (start_num..=end_num).map(BlockNumber::from).collect();

        // Fetch blocks using the batch method
        self.get_blocks(block_numbers).await
    }

    /// Fetches multiple transactions by hash using a batch request.
    /// The result vector contains `None` for transactions not found on the node.
    /// This method directly reflects the RPC result. Use `get_txs_by_hashes`
    /// for integrated cache lookup and a `HashMap` result.
    pub async fn get_txs(
        &self,
        hashes: Vec<H256>,
    ) -> Result<Vec<Option<TransactionWithStatusResponse>>, Error> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch_request = BatchRequestBuilder::new();
        for hash in hashes {
            if let Err(e) = batch_request.insert("get_transaction", rpc_params!(hash)) {
                return Err(Error::BatchBuildFailed(e.to_string()));
            }
        }
        // R = Option<TransactionWithStatusResponse> because get_transaction RPC can return null.
        self.batch_request::<Option<TransactionWithStatusResponse>>(batch_request)
            .await
    }
}

// --- HTTP Specific Implementation ---

pub type HttpFetcher = Fetcher<HttpClient>;

impl Fetcher<HttpClient> {
    pub async fn http_client(
        urls: impl IntoIterator<Item = impl AsRef<str>>,
        retry_interval: u64,
        max_retries: usize,
        max_response_size: u32,
        max_request_size: u32,
        sort_interval_secs: Option<u64>,
    ) -> Result<Self, Error> {
        let mut clients = SmallVec::new();
        // Collect URLs first to check if empty and avoid partial client creation
        let url_list: Vec<_> = urls.into_iter().collect();

        if url_list.is_empty() {
            return Err(Error::NoClientsConfigured);
        }

        for url_ref in url_list {
            let url_str = url_ref.as_ref(); // Get &str from AsRef<str>
            let builder = HttpClientBuilder::default()
                .max_response_size(max_response_size)
                .max_request_size(max_request_size)
                // Consider adding a reasonable default timeout
                .request_timeout(Duration::from_secs(30));
            // Build client, propagating potential URL parsing or connection errors
            match builder.build(url_str) {
                Ok(client) => clients.push(client),
                Err(e) => {
                    // Log the problematic URL
                    error!("Failed to build HTTP client for URL '{}': {}", url_str, e);
                    // Return the error, stopping further client creation
                    return Err(Error::JsonRpcClient(e));
                }
            }
        }

        // Proceed only if client creation was successful
        let fetcher = Self::new(clients, retry_interval, max_retries);

        // Perform initial sort to set a sensible starting client
        match fetcher.sort_clients_by_speed().await {
            Ok(idx) => info!("Initial fastest client: {}", idx),
            Err(e) => {
                error!("Failed to determine initial fastest client: {}", e);
                return Err(e);
            }
        }

        // Start background sorting if requested and interval > 0
        if let Some(interval) = sort_interval_secs {
            if interval > 0 {
                info!(
                    "Starting background client sorting every {} seconds",
                    interval
                );
                fetcher.start_background_sorting(interval);
            }
        }

        Ok(fetcher)
    }
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*;
    use ckb_jsonrpc_types::BlockNumber; // Removed Either as it wasn't used directly in test logic
    use std::path::Path;

    // Helper for setting up fetcher and DB for tests
    async fn setup_fetcher_and_db(
        db_path: &str,
    ) -> Result<Fetcher<HttpClient>, Box<dyn std::error::Error>> {
        // Ensure clean state for DB
        if Path::new(db_path).exists() {
            std::fs::remove_file(db_path)?;
        }
        let database = redb::Database::create(db_path)?;
        // Use init_db, handle potential DatabaseAlreadyInitialized gracefully if needed
        match init_db(database) {
            Ok(_) => {}
            Err(Error::DatabaseAlreadyInitialized) => {
                // This might happen if tests run concurrently and DB static is shared, unlikely with OnceLock unless reset logic is added.
                warn!("Database '{}' already initialized, reusing.", db_path);
            }
            Err(e) => return Err(e.into()), // Propagate other init errors
        }

        let fetcher = Fetcher::http_client(
            // Use a reliable public node for testing (consider alternatives like local node or mocks)
            ["https://mainnet.ckb.dev/rpc"],
            500,              // retry_interval_ms
            3,                // max_retries per client before switch/fail
            15 * 1024 * 1024, // Increased size slightly
            15 * 1024 * 1024,
            None, // No background sorting in basic test
        )
        .await?; // Use ? to propagate fetcher creation errors

        Ok(fetcher)
    }

    // Helper to clean up DB file after test
    fn cleanup_db(db_path: &str) {
        if Path::new(db_path).exists() {
            match std::fs::remove_file(db_path) {
                Ok(_) => info!("Cleaned up DB file: {}", db_path),
                Err(e) => error!("Error cleaning up DB file {}: {}", db_path, e),
            }
        }
    }

    // Function signature for checking Send + Sync bounds (compile-time check)
    const fn check_send_and_sync<T: Send + Sync>() {}

    #[tokio::test]
    #[tracing_test::traced_test] // Requires `tracing-test` dev-dependency
    async fn test_fetcher_basic_methods() -> Result<(), Box<dyn std::error::Error>> {
        const TEST_DB_PATH: &str = "fetcher_basic_test.redb";
        let fetcher = setup_fetcher_and_db(TEST_DB_PATH).await?;

        // Compile-time check for Send + Sync
        check_send_and_sync::<Fetcher<HttpClient>>();

        let tip_block_number = fetcher.get_tip_block_number().await?;
        info!("Tip block number: {}", tip_block_number);
        assert!(tip_block_number.value() > 0);

        // Test getting a specific early block (likely to exist)
        let block_opt = fetcher.get_block_by_number(1u64.into()).await?;
        assert!(
            block_opt.is_some(),
            "Block 1 not found (should exist on mainnet)"
        );
        if let Some(block) = block_opt {
            info!("Block 1 Header Hash: {:#x}", block.header.hash);
        }

        // Test getting a non-existent block (should return Ok(None))
        let future_block_num = tip_block_number.value() + 1_000_000;
        let non_existent_block = fetcher.get_block_by_number(future_block_num.into()).await?;
        assert!(
            non_existent_block.is_none(),
            "Fetching a future block number should return None"
        );

        cleanup_db(TEST_DB_PATH);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_fetcher_batch_and_tx_methods() -> Result<(), Box<dyn std::error::Error>> {
        const TEST_DB_PATH: &str = "fetcher_batch_test.redb";
        let fetcher = setup_fetcher_and_db(TEST_DB_PATH).await?;

        let tip = fetcher.get_tip_block_number().await?;
        let start_num = tip.value().saturating_sub(4); // Get last 5 blocks
        let end_num = tip.value();
        let block_numbers: Vec<BlockNumber> = (start_num..=end_num).map(Into::into).collect();

        info!("Fetching blocks {} to {}", start_num, end_num);
        // get_blocks returns Vec<Option<BlockView>>
        let block_opts = fetcher.get_blocks(block_numbers.clone()).await?;
        assert_eq!(
            block_opts.len(),
            block_numbers.len(),
            "Mismatched number of blocks returned"
        );

        // Flatten to get only the successfully retrieved blocks
        let blocks: Vec<BlockView> = block_opts.into_iter().flatten().collect();
        if blocks.is_empty() {
            warn!(
                "No blocks were successfully retrieved in the range {}..={}. Test might be less comprehensive.",
                start_num, end_num
            );
            // Don't assert !is_empty() as it might be a valid state near genesis or if node errors occurred
        } else {
            info!("Successfully retrieved {} blocks", blocks.len());
        }

        // --- Transaction Fetching ---
        let mut tx_hashes = HashSet::new();
        for block in &blocks {
            // Skip cellbase, focus on regular transactions if any
            for tx_view in block.transactions.iter().skip(1) {
                tx_hashes.insert(tx_view.hash.clone());
            }
        }

        if tx_hashes.is_empty() {
            info!(
                "No non-cellbase transactions found in the fetched blocks to test transaction methods."
            );
            cleanup_db(TEST_DB_PATH);
            return Ok(()); // Nothing more to test for transactions
        }

        let tx_hashes_vec: Vec<H256> = tx_hashes.into_iter().collect();
        info!(
            "Attempting to fetch {} non-cellbase transactions by hash",
            tx_hashes_vec.len()
        );

        // 1. Test get_txs (batch RPC call) -> Vec<Option<TxWithStatusResp>>
        let tx_opts = fetcher.get_txs(tx_hashes_vec.clone()).await?;
        assert_eq!(
            tx_opts.len(),
            tx_hashes_vec.len(),
            "get_txs returned wrong number of results"
        );
        let found_tx_responses: Vec<_> = tx_opts.into_iter().flatten().collect();
        info!(
            "get_txs found {} non-None responses",
            found_tx_responses.len()
        );
        // We expect most (if not all) to be found since they came from recent blocks
        assert_eq!(
            found_tx_responses.len(),
            tx_hashes_vec.len(),
            "Not all requested transactions were found by get_txs"
        );

        // 2. Test get_txs_by_hashes (uses cache + get_txs)
        info!("Testing get_txs_by_hashes (first call - RPC & cache)");
        let tx_map = fetcher.get_txs_by_hashes(tx_hashes_vec.clone()).await?;
        // Should contain all transactions that were valid in found_tx_responses
        let expected_count = found_tx_responses
            .iter()
            .filter(|resp| resp.transaction.is_some()) // Double check transaction exists in response
            .count();
        assert_eq!(
            tx_map.len(),
            expected_count,
            "get_txs_by_hashes (first call) count mismatch"
        );
        assert!(
            tx_map.contains_key(&tx_hashes_vec[0]),
            "First tx hash not found in map"
        );

        // Second call - should hit cache
        info!("Testing get_txs_by_hashes (second call - cache)");
        let tx_map_cached = fetcher.get_txs_by_hashes(tx_hashes_vec.clone()).await?;
        assert_eq!(
            tx_map_cached.len(),
            expected_count,
            "get_txs_by_hashes (second call) count mismatch"
        );
        assert!(
            tx_map_cached.contains_key(&tx_hashes_vec[0]),
            "First tx hash not found in cached map"
        );

        // 3. Test output fetching using the successfully found transactions
        let mut inputs = Vec::new();
        for tx_resp in found_tx_responses {
            if let Some(tx_view) = tx_resp.transaction {
                match tx_view.get_value() {
                    Ok(tx) => {
                        for input in &tx.inner.inputs {
                            // Filter out cellbase inputs again for output fetching
                            if input.previous_output.tx_hash != H256::default() {
                                inputs.push(input.clone());
                            }
                        }
                    }
                    Err(e) => warn!("Error getting tx value for output test: {}", e), // Log error but continue
                }
            }
        }

        if inputs.is_empty() {
            info!(
                "No non-cellbase inputs found from successfully fetched txs to test output methods."
            );
        } else {
            info!(
                "Testing output fetching for {} non-cellbase inputs",
                inputs.len()
            );

            // Test get_outputs_ignore_not_found
            let outputs_ignored = fetcher.get_outputs_ignore_not_found(inputs.clone()).await?;
            info!(
                "get_outputs_ignore_not_found returned {} outputs",
                outputs_ignored.len()
            );
            // Can't assert exact count, depends on whether outputs are spent etc.

            // Test get_outputs_with_data_ignore_not_found
            let outputs_data_ignored = fetcher
                .get_outputs_with_data_ignore_not_found(inputs.clone())
                .await?;
            info!(
                "get_outputs_with_data_ignore_not_found returned {} outputs with data",
                outputs_data_ignored.len()
            );
            assert!(outputs_data_ignored.len() <= inputs.len());

            // Test get_outputs (expect potential failures for spent outputs)
            match fetcher.get_outputs(inputs.clone()).await {
                Ok(outputs) => info!(
                    "get_outputs succeeded with {} outputs (might be fewer than inputs)",
                    outputs.len()
                ),
                // Expect PreviousOutputNotFound if some are spent/pruned
                Err(Error::PreviousOutputNotFound { tx_hash, index, .. }) => {
                    info!(
                        "get_outputs failed as expected with PreviousOutputNotFound for tx {:#x} index {}",
                        tx_hash, index
                    );
                }
                Err(e) => {
                    // Fail test for unexpected errors
                    error!("get_outputs failed UNEXPECTEDLY: {}", e);
                    return Err(e.into());
                }
            }
        }

        // 4. Test cache functions (if DB is available)
        if let Ok(db) = get_db() {
            let initial_max = cache::get_max_count_transaction(db)?;
            info!("Initial max count tx (after fetches): {:?}", initial_max);

            // Clear transactions with count < 1 (i.e., fetched only once by get_txs_by_hashes above)
            cache::clear_transactions_below_count(db, 1)?;
            info!("Cleared transactions with fetch count < 1");

            let cleared_max = cache::get_max_count_transaction(db)?;
            info!("Max count tx after clearing count < 1: {:?}", cleared_max);
            // Assertions depend heavily on access patterns, difficult to make robust here.
            // Example: if initial_max was Some((_, 0)), cleared_max should be None or have count > 0.
            if let Some((_, count)) = initial_max {
                if count == 0 {
                    assert!(
                        cleared_max.map_or(true, |(_, c)| c > 0),
                        "Expected max count to be None or > 0 after clearing 0 counts"
                    );
                }
            }
        }

        cleanup_db(TEST_DB_PATH);
        Ok(())
    }
}
