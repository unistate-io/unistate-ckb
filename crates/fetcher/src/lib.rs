mod wrapper;

use ckb_jsonrpc_types::{
    BlockNumber,
    BlockView,
    CellInput,
    CellOutput,
    JsonBytes,
    OutPoint,
    Transaction,
    TransactionView, // Added TransactionView
    TransactionWithStatusResponse,
};
use ckb_types::H256;
use futures::{FutureExt, future::BoxFuture}; // Added BoxFuture
use jsonrpsee::{
    core::{client::ClientT, client::Error as JsonRpseeError, params::BatchRequestBuilder}, // Added ClientT explicitly
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
pub use redb::Database; // Added Database, RedbError
pub use redb::Error as RedbError;
use redb::{ReadableTable as _, TableDefinition};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},

    ops::{Bound, RangeBounds},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration, // Added Duration
};
use thiserror::Error;
use tracing::{debug, info, warn}; // Added warn
use utils::ResponseFormatGetter; // Assuming this comes from elsewhere in your project
use wrapper::Bincode;

// Constants for Database Tables
const TX_TABLE: TableDefinition<Bincode<H256>, Bincode<Transaction>> =
    TableDefinition::new("transactions");
const TX_COUNT_TABLE: TableDefinition<Bincode<H256>, u64> =
    TableDefinition::new("transaction_counts");

// Static singletons for Database and Fetcher
static DB: OnceLock<Database> = OnceLock::new();
static FETCHER: OnceLock<Fetcher<HttpClient>> = OnceLock::new();

/// Custom error type for the Fetcher module.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to fetch data: code={code}, message={message}")]
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

    #[error("Encountered an issue with the JSON RPC client. Details: {0}")]
    JsonRpcClient(#[from] JsonRpseeError), // Corrected Error type

    #[error("There was a problem deserializing data. Details: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Database error: {0}")]
    Database(#[from] redb::Error),

    #[error("Database not initialized")]
    DatabaseNotInitialized,

    #[error("Database already initialized")] // Added for consistency
    DatabaseAlreadyInitialized,

    #[error("Fetcher already initialized")]
    FetcherAlreadyInitialized,

    #[error("Fetcher not initialized")]
    FetcherNotInitialized,
}

/// Main struct for fetching data from CKB nodes.
#[derive(Debug)]
pub struct Fetcher<C> {
    clients: SmallVec<[C; 2]>,
    current_index: Arc<AtomicUsize>,
    retry_interval: u64,
    max_retries: usize,
}

// --- Initialization and Access Functions ---

/// Initializes the global HTTP Fetcher instance.
///
/// Must be called only once.
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

/// Gets a reference to the initialized global HTTP Fetcher.
///
/// Returns `Error::FetcherNotInitialized` if `init_http_fetcher` has not been called.
pub fn get_fetcher() -> Result<&'static Fetcher<HttpClient>, Error> {
    FETCHER.get().ok_or(Error::FetcherNotInitialized)
}

/// Initializes the global Database instance.
///
/// Opens the required tables. Must be called only once.
pub fn init_db(db: Database) -> Result<(), Error> {
    // Check if already initialized before potentially expensive write txn
    if DB.get().is_some() {
        return Err(Error::DatabaseAlreadyInitialized);
    }

    let write_txn = db.begin_write().map_err(redb::Error::from)?;
    {
        let _ = write_txn.open_table(TX_TABLE).map_err(redb::Error::from)?;
        let _ = write_txn
            .open_table(TX_COUNT_TABLE)
            .map_err(redb::Error::from)?;
    }
    write_txn.commit().map_err(redb::Error::from)?;

    // Use `set` which returns Err if already set.
    DB.set(db).map_err(|_| Error::DatabaseAlreadyInitialized) // Should ideally not happen due to check above, but safe.
}

/// Gets a reference to the initialized global Database.
///
/// Returns `Error::DatabaseNotInitialized` if `init_db` has not been called.
pub fn get_db() -> Result<&'static Database, Error> {
    DB.get().ok_or(Error::DatabaseNotInitialized)
}

// --- Database Caching Module ---
pub mod cache {
    use super::*; // Import necessary items from parent module

    pub fn clear_transactions_below_count(db: &Database, threshold: u64) -> Result<(), RedbError> {
        let write_txn = db.begin_write()?;
        {
            let mut tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

            let to_remove: Vec<H256> = count_table
                .iter()?
                .filter_map(|result| {
                    result.ok().and_then(|(hash, count)| {
                        if count.value() < threshold {
                            Some(hash.value())
                        } else {
                            None
                        }
                    })
                })
                .collect();

            debug!(
                "Removing {} transactions with count below {}",
                to_remove.len(),
                threshold
            );
            for hash in to_remove {
                tx_table.remove(&hash)?;
                count_table.remove(&hash)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_cached_transactions(
        db: &Database,
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

// --- Fetcher Implementation ---

impl<C> Fetcher<C>
where
    C: ClientT + Send + Sync + Clone + 'static, // Added Sync constraint
{
    /// Returns the currently selected client based on the atomic index.
    fn client(&self) -> &C {
        // Relaxed ordering is sufficient for round-robin or fastest-client selection
        let index = self.current_index.load(Ordering::Relaxed);
        // Use checked access in production code unless performance is absolutely critical
        // and bounds are guaranteed elsewhere.
        self.clients.get(index).unwrap_or_else(|| {
            // Fallback to the first client if index is somehow out of bounds
            warn!(
                "current_index {} out of bounds for clients len {}, falling back to index 0",
                index,
                self.clients.len()
            );
            &self.clients[0]
        })
        // Unsafe version (use with extreme caution):
        // unsafe { self.clients.get_unchecked(index) }
    }

    /// Creates a new Fetcher instance. Internal use, prefer `init_http_fetcher`.
    pub fn new(clients: SmallVec<[C; 2]>, retry_interval: u64, max_retries: usize) -> Self {
        assert!(!clients.is_empty(), "Fetcher requires at least one client");
        Self {
            clients,
            retry_interval,
            max_retries,
            current_index: Arc::new(AtomicUsize::new(0)), // Start with the first client
        }
    }

    /// Fetches transactions by their hashes, utilizing the cache if available.
    pub async fn get_txs_by_hashes(
        &self,
        hashes: Vec<H256>,
    ) -> Result<HashMap<H256, Transaction>, Error> {
        debug!("Getting {} transactions by hashes...", hashes.len());

        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        match get_db() {
            Ok(db) => {
                // --- Cached Path ---
                let mut result = cache::get_cached_transactions(db, &hashes)?;

                let missing_hashes: Vec<H256> =
                    hashes // No need for par_iter on the filter check
                        .into_iter() // Use standard iterator
                        .filter(|hash| !result.contains_key(hash))
                        .collect();

                if !missing_hashes.is_empty() {
                    debug!(
                        "Cache miss for {} hashes. Fetching from node...",
                        missing_hashes.len()
                    );
                    let fetched_tx_responses = self.get_txs(missing_hashes).await?;

                    // Cache the newly fetched transactions
                    let newly_cached_txs = cache::cache_transactions(db, &fetched_tx_responses)?;
                    result.extend(newly_cached_txs);
                } else {
                    debug!("All {} transactions found in cache.", result.len());
                }
                Ok(result)
            }
            Err(Error::DatabaseNotInitialized) => {
                // --- Non-Cached Path ---
                warn!("Database not initialized, fetching all transactions without cache.");
                let tx_responses = self.get_txs(hashes).await?;
                let mut result = HashMap::new();
                for tx_response in tx_responses {
                    if let Some(tx_view) = tx_response.transaction {
                        if let Ok(tx) = tx_view.get_value() {
                            result.insert(tx.hash, tx.inner);
                        }
                    }
                }
                Ok(result)
            }
            Err(e) => Err(e), // Propagate other DB errors (e.g., initialization failed)
        }
    }

    /// Internal helper for single RPC calls with retry logic.
    #[inline]
    async fn call<Params, R>(&self, method: &'static str, params: Params) -> Result<R, Error>
    where
        Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + Sync + 'static, // Added Sync + 'static
        R: jsonrpsee::core::DeserializeOwned + Send + 'static,
    {
        let mut current_retries = 0;

        loop {
            let client = self.client(); // Get the current client
            let result = client.request::<R, Params>(method, params.clone()).await;

            match result {
                Ok(response) => return Ok(response),
                Err(err) => {
                    warn!(
                        "RPC call '{}' failed (attempt {}/{}): {}. Retrying in {}ms...",
                        method,
                        current_retries + 1,
                        self.max_retries + 1, // +1 because we try once initially
                        err,
                        self.retry_interval
                    );
                    current_retries += 1;
                    if current_retries > self.max_retries {
                        // Optionally try switching client after max retries on the current one
                        // This adds complexity, consider if simple failure is better.
                        // let new_idx = self.sort_clients().await?; // Ensure sort_clients exists and works
                        // if new_idx != initial_client_index {
                        //     warn!("Switching client after max retries failed on index {}", initial_client_index);
                        //     self.current_index.store(new_idx, Ordering::Relaxed);
                        //     current_retries = 0; // Reset retries for the new client
                        //     continue; // Try again with the new client immediately
                        // } else {
                        //     // If sorting didn't find a better client or failed, return the error
                        return Err(Error::JsonRpcClient(err));
                        // }
                    }
                    tokio::time::sleep(Duration::from_millis(self.retry_interval)).await;
                }
            }
        }
    }

    /// Internal helper for batch RPC calls with retry logic.
    #[inline]
    async fn batch_request<R>(
        &self,
        batch_request: BatchRequestBuilder<'static>, // Takes pre-built request
    ) -> Result<Vec<R>, Error>
    where
        R: jsonrpsee::core::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static, // Added Sync + 'static
    {
        let mut current_retries = 0;

        loop {
            let client = self.client().clone(); // Clone client for the request future
            let current_batch = batch_request.clone(); // Clone the builder for the request future

            // NOTE: This `.boxed()` is kept as per the original code's comment.
            // Evaluate if it's truly necessary for your specific Rust version/deps.
            let result = client.batch_request::<R>(current_batch).boxed().await;

            match result {
                Ok(response) => {
                    // Attempt to parse results, handling individual errors
                    let results: Result<Vec<_>, _> = response
                        .into_iter()
                        .map(|res| {
                            res.map_err(|err| {
                                // Map individual item error within the batch
                                Error::FailedFetch {
                                    code: err.code(),
                                    message: err.message().into(),
                                }
                            })
                        })
                        .collect();
                    return results; // Return Vec<R> or the first FailedFetch error
                }
                Err(err) => {
                    // This error means the *entire batch request* failed (network, server error, or root parse error)
                    warn!(
                        "Batch request failed (attempt {}/{}): {}. Retrying in {}ms...",
                        current_retries + 1,
                        self.max_retries + 1,
                        err, // Log the specific jsonrpsee error
                        self.retry_interval
                    );

                    current_retries += 1;
                    if current_retries > self.max_retries {
                        // Consider switching client here as well, similar to `call`
                        // let new_idx = self.sort_clients().await?;
                        // if new_idx != initial_client_index { ... }
                        return Err(Error::JsonRpcClient(err));
                    }
                    tokio::time::sleep(Duration::from_millis(self.retry_interval)).await;
                }
            }
        }
    }

    /// Sorts clients by pinging `get_tip_block_number` and returns the index of the fastest *responding* client.
    /// This method modifies the internal `current_index`.
    pub async fn sort_clients(&self) -> Result<usize, Error> {
        let (idx, duration) = sort_clients_by_speed_static(&self.clients).await?;
        debug!(
            "Fastest client is index {} with duration {:?}. Updating current index.",
            idx, duration
        );
        self.current_index.store(idx, Ordering::Relaxed);
        Ok(idx)
    }

    /// Spawns a background task that periodically sorts clients.
    /// Requires the `Fetcher` to live for the 'static lifetime implicitly via `OnceLock` or be `Arc`ed.
    pub fn start_background_sorting(&self, interval_secs: u64) {
        info!(
            "Starting background client sorting task with interval {}s",
            interval_secs
        );
        // Clone data needed by the background task BEFORE spawning.
        let clients_clone = self.clients.clone(); // Requires C: Clone
        let current_index_clone = Arc::clone(&self.current_index);

        tokio::spawn(async move {
            // Use the cloned data, not `self` or the static `FETCHER`.
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await; // Consume the initial immediate tick

            loop {
                interval.tick().await;
                debug!("Background task: Sorting clients...");
                match sort_clients_by_speed_static(&clients_clone).await {
                    Ok((idx, duration)) => {
                        let old_idx = current_index_clone.swap(idx, Ordering::Relaxed);
                        if old_idx != idx {
                            info!(
                                "Background sort: Switched active client from index {} to {} (latency: {:?})",
                                old_idx, idx, duration
                            );
                        } else {
                            debug!(
                                "Background sort: Fastest client remains index {} (latency: {:?})",
                                idx, duration
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Background client sorting failed: {}", e);
                        // Decide how to handle errors: continue, retry later, log?
                        // For now, just log and continue the loop.
                    }
                }
            }
        });
    }

    // --- Public API Methods ---

    pub async fn get_outputs_ignore_not_found(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<CellOutput>, Error> {
        debug!(
            "Getting outputs for {} inputs (ignore not found)",
            inputs.len()
        );
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<H256> = inputs // Collect unique tx hashes needed
            .par_iter() // Use parallel iterator for mapping
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>() // Collect into HashSet to get unique hashes
            .into_par_iter() // Convert back for parallel collection (optional)
            .collect();

        let txs = self.get_txs_by_hashes(hashes).await?; // Fetch (potentially cached) transactions

        let outputs = inputs
            .into_par_iter() // Parallel processing of inputs
            .filter_map(|input| {
                let index = input.previous_output.index.value() as usize;
                // Check if tx exists and index is valid
                txs.get(&input.previous_output.tx_hash)
                    .and_then(|tx| tx.outputs.get(index))
                    .cloned() // Clone the output if found
            })
            .collect::<Vec<_>>();

        debug!("Found {} outputs (ignore not found).", outputs.len());
        Ok(outputs)
    }

    pub async fn get_outputs(&self, inputs: Vec<CellInput>) -> Result<Vec<CellOutput>, Error> {
        debug!("Getting outputs for {} inputs (strict)", inputs.len());
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect();

        let txs = self.get_txs_by_hashes(hashes).await?;

        inputs
            .into_par_iter() // Use parallel map for potential performance gain
            .map(|input| {
                let tx_hash = input.previous_output.tx_hash.clone();
                let index = input.previous_output.index.value();
                let tx_opt = txs.get(&tx_hash);

                tx_opt
                    .and_then(|tx| tx.outputs.get(index as usize).cloned())
                    .ok_or_else(|| Error::PreviousOutputNotFound {
                        tx_hash, // Use cloned hash
                        index,
                        outputs_len: tx_opt.map_or(0, |tx| tx.outputs.len()),
                    })
            })
            .collect::<Result<Vec<_>, _>>() // Collect into a Result<Vec<CellOutput>, Error>
    }

    pub async fn get_outputs_with_data(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!(
            "Getting outputs with data for {} inputs (strict)",
            inputs.len()
        );
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect();

        let txs = self.get_txs_by_hashes(hashes).await?;

        inputs
            .into_par_iter()
            .map(|input| {
                let out_point = input.previous_output.clone();
                let tx_hash = &out_point.tx_hash;
                let index = out_point.index.value() as usize;
                let tx_opt = txs.get(tx_hash);

                let output = tx_opt
                    .and_then(|tx| tx.outputs.get(index).cloned())
                    .ok_or_else(|| Error::PreviousOutputNotFound {
                        tx_hash: tx_hash.clone(),
                        index: index as u32,
                        outputs_len: tx_opt.map_or(0, |tx| tx.outputs.len()),
                    })?; // Early return if output not found

                let data = tx_opt
                    .and_then(|tx| tx.outputs_data.get(index).cloned())
                    .ok_or_else(|| Error::PreviousOutputDataNotFound {
                        tx_hash: tx_hash.clone(),
                        index: index as u32,
                        outputs_data_len: tx_opt.map_or(0, |tx| tx.outputs_data.len()),
                    })?; // Early return if data not found

                Ok((out_point, output, data))
            })
            .collect::<Result<Vec<_>, Error>>() // Collect results, propagating the first error
    }

    pub async fn get_outputs_with_data_ignore_not_found(
        &self,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!(
            "Getting outputs with data for {} inputs (ignore not found)",
            inputs.len()
        );
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<H256> = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect();

        let txs = self.get_txs_by_hashes(hashes).await?;

        let results = inputs
            .into_par_iter()
            .filter_map(|input| {
                let out_point = input.previous_output; // No need to clone yet
                let tx_hash = &out_point.tx_hash;
                let index = out_point.index.value() as usize;
                let tx_opt = txs.get(tx_hash);

                match (
                    tx_opt.and_then(|tx| tx.outputs.get(index)),
                    tx_opt.and_then(|tx| tx.outputs_data.get(index)),
                ) {
                    (Some(output), Some(data)) => {
                        Some((out_point.clone(), output.clone(), data.clone())) // Clone only if found
                    }
                    _ => None, // Ignore if either output or data is missing
                }
            })
            .collect::<Vec<_>>();
        debug!(
            "Found {} outputs with data (ignore not found).",
            results.len()
        );
        Ok(results)
    }

    // --- Direct RPC Passthrough Methods ---

    pub async fn get_tip_block_number(&self) -> Result<BlockNumber, Error> {
        self.call("get_tip_block_number", rpc_params!()).await
    }

    pub async fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> Result<Option<BlockView>, Error> {
        self.call("get_block_by_number", rpc_params!(number)).await
    }

    pub async fn get_blocks(
        &self,
        numbers: Vec<BlockNumber>,
    ) -> Result<Vec<Option<BlockView>>, Error> {
        if numbers.is_empty() {
            return Ok(Vec::new());
        }
        debug!("Fetching {} blocks by number", numbers.len());
        let mut batch_request = BatchRequestBuilder::new();
        for number in numbers {
            batch_request
                .insert("get_block_by_number", rpc_params!(number))
                // Panic is acceptable here as it indicates a programming error
                .expect("Bug: Failed to insert get_block_by_number into batch");
        }
        // Batch request returns Vec<Result<T, Error>>, we map internal errors
        // Note: get_block_by_number returns Option<BlockView>
        self.batch_request(batch_request).await
    }

    pub async fn get_blocks_range<R, N>(&self, range: R) -> Result<Vec<BlockView>, Error>
    where
        R: RangeBounds<N>,
        N: Into<BlockNumber> + Copy,
    {
        // Determine start block number
        let start = match range.start_bound() {
            Bound::Included(n) => (*n).into(),
            Bound::Excluded(n) => {
                let num = (*n).into().value();
                // Check for potential overflow if N is already u64::MAX
                BlockNumber::from(num.saturating_add(1))
            }
            Bound::Unbounded => BlockNumber::from(0), // Assuming start from genesis
        };

        // Determine end block number
        let end = match range.end_bound() {
            Bound::Included(n) => (*n).into(),
            Bound::Excluded(n) => {
                let num = (*n).into().value();
                // Check for potential underflow if N is 0
                BlockNumber::from(num.saturating_sub(1))
            }
            Bound::Unbounded => self.get_tip_block_number().await?,
        };

        // Handle invalid or empty range
        if start > end {
            debug!("get_blocks_range: empty range (start > end)");
            return Ok(Vec::new());
        }

        // Generate the sequence of block numbers (inclusive)
        let block_numbers: Vec<BlockNumber> = (start.value()..=end.value())
            .map(BlockNumber::from)
            .collect();

        if block_numbers.is_empty() {
            // This can happen if start == end and the range was exclusive end, etc.
            debug!("get_blocks_range: calculated empty block number list");
            return Ok(Vec::new());
        }

        debug!(
            "Fetching blocks range: {}..={} ({} blocks)",
            start.value(),
            end.value(),
            block_numbers.len()
        );

        // Fetch the blocks using the batch method
        let block_options = self.get_blocks(block_numbers).await?;

        // Filter out None results (blocks not found, which shouldn't happen in a range unless tip changed)
        let blocks: Vec<BlockView> = block_options.into_iter().filter_map(|opt| opt).collect();

        // Optional: Check if the number of blocks matches requested, could indicate issues.
        // if blocks.len() != block_options.len() {
        //     warn!("get_blocks_range: Mismatch between requested and found blocks. Tip might have changed or node issue.");
        // }

        Ok(blocks)
    }

    pub async fn get_txs(
        &self,
        hashes: Vec<H256>,
    ) -> Result<Vec<TransactionWithStatusResponse>, Error> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        debug!("Fetching {} transactions by hash", hashes.len());
        let mut batch_request = BatchRequestBuilder::new();
        for hash in hashes {
            batch_request
                .insert("get_transaction", rpc_params!(hash))
                .expect("Bug: Failed to insert get_transaction into batch");
        }
        // Note: get_transaction returns TransactionWithStatusResponse which includes Option<TransactionView>
        self.batch_request(batch_request).await
    }
}

// --- Specific HTTP Fetcher Implementation ---

pub type HttpFetcher = Fetcher<HttpClient>;

impl Fetcher<HttpClient> {
    /// Creates and initializes an HTTP Fetcher instance.
    /// Internal use by `init_http_fetcher`.
    pub async fn http_client(
        urls: impl IntoIterator<Item = impl AsRef<str>>,
        retry_interval: u64,
        max_retries: usize,
        max_response_size: u32,
        max_request_size: u32,
        sort_interval_secs: Option<u64>,
    ) -> Result<Self, Error> {
        let mut clients = SmallVec::new();
        let urls_vec: Vec<_> = urls.into_iter().collect(); // Collect to check emptiness

        if urls_vec.is_empty() {
            // Or return a specific error like `Error::NoClientsProvided`
            panic!("Fetcher requires at least one URL");
        }

        for url_ref in urls_vec {
            let url_str = url_ref.as_ref();
            debug!("Building HTTP client for URL: {}", url_str);
            let builder = HttpClientBuilder::default()
                .max_response_size(max_response_size)
                .max_request_size(max_request_size)
                // Add other configurations like timeouts if needed
                // .request_timeout(Duration::from_secs(30))
                ;
            // Use `?` to propagate jsonrpsee::core::Error, mapped via From trait in Error enum
            let client = builder.build(url_str)?;
            clients.push(client);
        }

        let fetcher = Self::new(clients, retry_interval, max_retries);

        // Perform initial sort to select the best starting client
        match fetcher.sort_clients().await {
            Ok(idx) => info!("Selected fastest initial client at index {}", idx),
            Err(e) => warn!("Initial client sort failed: {}. Using index 0.", e), // Continue with default index 0
        }

        // Start background sorting if interval is specified
        if let Some(interval) = sort_interval_secs {
            // Pass self to the method, it will clone necessary data
            fetcher.start_background_sorting(interval);
        }

        Ok(fetcher)
    }
}

// --- Helper function for sorting ---

/// Sorts a list of clients by pinging `get_tip_block_number` and returns the index
/// and duration of the fastest *responding* client. Does not modify any state.
async fn sort_clients_by_speed_static<C>(clients: &[C]) -> Result<(usize, Duration), Error>
where
    C: ClientT + Send + Sync + Clone + 'static,
{
    if clients.is_empty() {
        // This case should be prevented by checks in `new` and `http_client`
        panic!("Attempted to sort empty client list");
    }

    let mut futures: Vec<BoxFuture<Result<(usize, Duration), JsonRpseeError>>> =
        Vec::with_capacity(clients.len());

    for (idx, client) in clients.iter().enumerate() {
        let client_clone = client.clone(); // Clone client for the async block
        let fut = async move {
            let start = std::time::Instant::now();
            // Use a simple, quick request like get_tip_block_number
            let _result = client_clone
                .request::<BlockNumber, _>("get_tip_block_number", rpc_params!())
                .await?; // Propagate jsonrpsee error if request fails
            Ok((idx, start.elapsed()))
        }
        .boxed(); // Box the future
        futures.push(fut);
    }

    // Wait for the first successful future to complete
    match futures::future::select_ok(futures).await {
        Ok(((idx, duration), _remaining_futures)) => Ok((idx, duration)),
        Err(e) => {
            // This error means *all* client checks failed.
            warn!("All clients failed health check during sorting: {}", e);
            Err(Error::JsonRpcClient(e)) // Return the error from the last future that failed in select_ok
        }
    }
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use redb::ReadableTableMetadata;

    use super::*;
    use std::path::Path; // For path manipulation

    // Helper to create a fetcher for tests, ensuring DB is cleaned up
    async fn setup_test_fetcher(db_path: &str, sort_interval: Option<u64>) -> Result<(), Error> {
        // Ensure clean state
        if Path::new(db_path).exists() {
            std::fs::remove_file(db_path).expect("Failed to delete test DB file");
        }

        let database = Database::create(db_path).expect("Failed to create test DB");
        init_db(database)?; // Initialize global DB

        // Use real mainnet nodes for integration testing
        init_http_fetcher(
            [
                "https://mainnet.ckb.dev/", // Prioritize known good ones
                "https://mainnet.ckbapp.dev/",
                // "http://51.15.217.238:8114", // This one might be less reliable
            ],
            500,              // retry_interval
            3,                // max_retries
            10 * 1024 * 1024, // max_response_size (10MB)
            10 * 1024 * 1024, // max_request_size (10MB)
            sort_interval,    // Optional background sorting
        )
        .await
    }

    fn cleanup_test_db(db_path: &str) {
        // Drop the static DB instance if it was initialized
        // This is tricky with OnceLock. Best effort is to just delete the file.
        if Path::new(db_path).exists() {
            std::fs::remove_file(db_path).expect("Failed to delete test DB file post-test");
        }
        // Resetting OnceLock for subsequent tests is not directly possible without unsafe code or feature flags.
        // Tests relying on clean OnceLock state might interfere if run in the same process.
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore] // Mark as ignore as it hits external network and takes time
    async fn test_fetcher_methods_with_cache() {
        let db_path = "test_fetcher_methods_with_cache.redb";
        setup_test_fetcher(db_path, Some(60)).await.unwrap(); // Enable sorting for test

        let fetcher = get_fetcher().expect("Fetcher should be initialized");
        let db = get_db().expect("DB should be initialized");

        // 1. Get Tip Block Number
        let tip_block_number = fetcher.get_tip_block_number().await.unwrap();
        println!("Tip block number: {}", tip_block_number.value());
        assert!(tip_block_number.value() > 0);

        // 2. Get Single Block
        let block_num_to_get = tip_block_number.value().saturating_sub(10); // Get a recent block
        let single_block_opt = fetcher
            .get_block_by_number(block_num_to_get.into())
            .await
            .unwrap();
        assert!(
            single_block_opt.is_some(),
            "Block {} not found",
            block_num_to_get
        );
        let single_block = single_block_opt.unwrap();
        println!(
            "Got single block: {}",
            single_block.header.inner.number.value()
        );

        // 3. Get Blocks Batch
        let start_block = tip_block_number.value().saturating_sub(5);
        let end_block = tip_block_number.value().saturating_sub(1);
        let block_numbers: Vec<BlockNumber> = (start_block..=end_block).map(Into::into).collect();
        let blocks_batch_opts = fetcher.get_blocks(block_numbers.clone()).await.unwrap();
        assert_eq!(
            blocks_batch_opts.len(),
            block_numbers.len(),
            "Batch block count mismatch"
        );
        let blocks_batch: Vec<_> = blocks_batch_opts.into_iter().filter_map(|b| b).collect();
        assert_eq!(
            blocks_batch.len(),
            block_numbers.len(),
            "Not all blocks in batch were found"
        );
        println!("Got batch of {} blocks", blocks_batch.len());

        // 4. Get Blocks Range
        let range_start = tip_block_number.value().saturating_sub(8);
        let range_end = tip_block_number.value().saturating_sub(6); // 3 blocks: 8, 7, 6 down from tip
        let blocks_range = fetcher
            .get_blocks_range(range_start..=range_end)
            .await
            .unwrap();
        assert_eq!(blocks_range.len(), 3, "Range block count mismatch");
        println!("Got range of {} blocks", blocks_range.len());

        // 5. Extract Hashes and Get Transactions
        let tx_hashes: Vec<H256> = blocks_batch // Use blocks from batch fetch
            .iter()
            .flat_map(|block| block.transactions.iter().map(|tx| tx.hash.clone()))
            .collect::<HashSet<_>>() // Unique hashes
            .into_iter()
            .collect();

        if tx_hashes.is_empty() {
            println!("No transactions found in test blocks, skipping TX tests.");
        } else {
            println!("Extracted {} unique transaction hashes", tx_hashes.len());

            // 6. Get TXs (Batch)
            let tx_responses = fetcher.get_txs(tx_hashes.clone()).await.unwrap();
            assert_eq!(
                tx_responses.len(),
                tx_hashes.len(),
                "TX response count mismatch"
            );

            // 7. Get TXs by Hashes (Cached) - First call (populate cache)
            println!("Fetching TXs by hash (1st time - cache miss expected)");
            let txs_map1 = fetcher.get_txs_by_hashes(tx_hashes.clone()).await.unwrap();
            assert_eq!(
                txs_map1.len(),
                tx_hashes.len(),
                "TX map count mismatch (1st fetch)"
            );

            // 8. Get TXs by Hashes (Cached) - Second call (cache hit expected)
            println!("Fetching TXs by hash (2nd time - cache hit expected)");
            // Before second fetch, clear counts to verify increment
            let write_txn = db.begin_write().unwrap();
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE).unwrap();
            for hash in &tx_hashes {
                count_table.insert(hash.clone(), 0u64).unwrap();
            }
            drop(count_table);
            write_txn.commit().unwrap();

            let txs_map2 = fetcher.get_txs_by_hashes(tx_hashes.clone()).await.unwrap();
            assert_eq!(
                txs_map2.len(),
                tx_hashes.len(),
                "TX map count mismatch (2nd fetch)"
            );

            // Verify counts incremented
            let read_txn = db.begin_read().unwrap();
            let count_table = read_txn.open_table(TX_COUNT_TABLE).unwrap();
            for hash in &tx_hashes {
                let count = count_table
                    .get(hash.clone())
                    .unwrap()
                    .map(|c| c.value())
                    .unwrap_or(0);
                assert_eq!(count, 1, "Usage count for hash {} did not increment", hash);
            }
            drop(read_txn); // Release read lock

            // 9. Extract Inputs and Get Outputs
            let inputs: Vec<CellInput> = txs_map1 // Use transactions from first fetch
                .values()
                .flat_map(|tx| tx.inputs.iter().cloned())
                .collect();

            if inputs.is_empty() {
                println!("No inputs found in test transactions, skipping output tests.");
            } else {
                println!("Extracted {} inputs", inputs.len());
                // 10. Get Outputs (Ignore Not Found)
                let outputs_ignored = fetcher
                    .get_outputs_ignore_not_found(inputs.clone())
                    .await
                    .unwrap();
                println!("Got {} outputs (ignore not found)", outputs_ignored.len());
                assert!(
                    !outputs_ignored.is_empty(),
                    "Expected some outputs (ignore not found)"
                );

                // 11. Get Outputs (Strict) - This might fail if inputs are cellbase or from pruned txs
                match fetcher.get_outputs(inputs.clone()).await {
                    Ok(outputs_strict) => {
                        println!("Got {} outputs (strict)", outputs_strict.len());
                        // Length might not match inputs if some were unresolvable
                        assert!(outputs_strict.len() <= inputs.len());
                    }
                    Err(Error::PreviousOutputNotFound { tx_hash, index, .. }) => {
                        println!(
                            "Strict get_outputs failed as expected for some inputs (e.g., {:?}, index {})",
                            tx_hash, index
                        );
                    }
                    Err(e) => panic!("Unexpected error in get_outputs: {}", e),
                }

                // 12. Get Outputs With Data (Ignore Not Found)
                let outputs_data_ignored = fetcher
                    .get_outputs_with_data_ignore_not_found(inputs.clone())
                    .await
                    .unwrap();
                println!(
                    "Got {} outputs with data (ignore not found)",
                    outputs_data_ignored.len()
                );
                assert!(
                    !outputs_data_ignored.is_empty(),
                    "Expected some outputs with data (ignore not found)"
                );
                assert_eq!(outputs_data_ignored.len(), outputs_ignored.len()); // Should match non-data version

                // 13. Get Outputs With Data (Strict) - Might fail like get_outputs
                match fetcher.get_outputs_with_data(inputs.clone()).await {
                    Ok(outputs_data_strict) => {
                        println!(
                            "Got {} outputs with data (strict)",
                            outputs_data_strict.len()
                        );
                        assert!(outputs_data_strict.len() <= inputs.len());
                    }
                    Err(Error::PreviousOutputNotFound { tx_hash, index, .. })
                    | Err(Error::PreviousOutputDataNotFound { tx_hash, index, .. }) => {
                        println!(
                            "Strict get_outputs_with_data failed as expected for some inputs (e.g., {:?}, index {})",
                            tx_hash, index
                        );
                    }
                    Err(e) => panic!("Unexpected error in get_outputs_with_data: {}", e),
                }
            }

            // 14. Test Cache Cleanup
            println!("Testing cache cleanup...");
            // Ensure some transactions exist with count 1
            let read_txn = db.begin_read().unwrap();
            let count_table = read_txn.open_table(TX_COUNT_TABLE).unwrap();
            let count_one_txs = count_table
                .iter()
                .unwrap()
                .filter(|res| res.as_ref().ok().map_or(false, |(_, c)| c.value() == 1))
                .count();
            let total_txs = read_txn.open_table(TX_TABLE).unwrap().len().unwrap();
            drop(read_txn);

            assert!(
                count_one_txs > 0,
                "Expected some txs with count 1 for cleanup test"
            );

            cache::clear_transactions_below_count(db, 2).unwrap(); // Clear txs with count < 2 (i.e., count 0 and 1)

            let read_txn = db.begin_read().unwrap();
            let count_table = read_txn.open_table(TX_COUNT_TABLE).unwrap();
            let remaining_count_one_txs = count_table
                .iter()
                .unwrap()
                .filter(|res| res.as_ref().ok().map_or(false, |(_, c)| c.value() == 1))
                .count();
            let remaining_total_txs = read_txn.open_table(TX_TABLE).unwrap().len().unwrap();
            drop(read_txn);

            assert_eq!(
                remaining_count_one_txs, 0,
                "Cleanup should remove txs with count 1"
            );
            assert!(
                remaining_total_txs < total_txs,
                "Total TXs should decrease after cleanup"
            );
            println!("Cache cleanup successful.");
        }

        cleanup_test_db(db_path);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_blocks_range_variants() {
        let db_path = "test_range_variants.redb";
        setup_test_fetcher(db_path, None).await.unwrap(); // No sorting needed here
        let fetcher = get_fetcher().expect("Fetcher should be initialized");

        let tip = fetcher.get_tip_block_number().await.unwrap().value();
        println!("Testing get_blocks_range with tip block number: {}", tip);

        // Ensure we have enough blocks for the test ranges
        assert!(
            tip >= 10,
            "Need at least 10 blocks on the chain for this test"
        );

        let start = tip - 5; // e.g., tip=100, start=95
        let end = tip - 2; // e.g., tip=100, end=98

        // Test with included range: start..=end (95, 96, 97, 98) -> 4 blocks
        let blocks_incl = fetcher.get_blocks_range(start..=end).await.unwrap();
        assert_eq!(blocks_incl.len(), 4);
        assert_eq!(blocks_incl[0].header.inner.number.value(), start);
        assert_eq!(blocks_incl.last().unwrap().header.inner.number.value(), end);

        // Test with excluded end range: start..end (95, 96, 97) -> 3 blocks
        let blocks_excl_end = fetcher.get_blocks_range(start..end).await.unwrap();
        assert_eq!(blocks_excl_end.len(), 3);
        assert_eq!(blocks_excl_end[0].header.inner.number.value(), start);
        assert_eq!(
            blocks_excl_end.last().unwrap().header.inner.number.value(),
            end - 1
        );

        // Test with unbounded end: start.. (95..tip)
        let blocks_unb_end = fetcher.get_blocks_range(start..).await.unwrap();
        assert_eq!(blocks_unb_end.len() as u64, tip - start + 1);
        assert_eq!(blocks_unb_end[0].header.inner.number.value(), start);
        assert_eq!(
            blocks_unb_end.last().unwrap().header.inner.number.value(),
            tip
        );

        // Test with unbounded start: ..=end (0..98)
        // Note: Fetching from 0 can be very slow and return huge data. Limit the range.
        let limited_end = 5u64; // Fetch only blocks 0 to 5
        if tip >= limited_end {
            let blocks_unb_start = fetcher.get_blocks_range(..=limited_end).await.unwrap();
            assert_eq!(blocks_unb_start.len() as u64, limited_end + 1); // 0 to 5 inclusive is 6 blocks
            assert_eq!(blocks_unb_start[0].header.inner.number.value(), 0);
            assert_eq!(
                blocks_unb_start.last().unwrap().header.inner.number.value(),
                limited_end
            );
        } else {
            println!(
                "Skipping unbounded start test as tip ({}) is less than {}",
                tip, limited_end
            );
        }

        // Test empty range: end..start (98..95)
        let blocks_empty = fetcher.get_blocks_range(end..start).await.unwrap();
        assert!(blocks_empty.is_empty());

        // Test single block range: start..=start (95..=95) -> 1 block
        let blocks_single = fetcher.get_blocks_range(start..=start).await.unwrap();
        assert_eq!(blocks_single.len(), 1);
        assert_eq!(blocks_single[0].header.inner.number.value(), start);

        cleanup_test_db(db_path);
    }
}
