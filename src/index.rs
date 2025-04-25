use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ckb_jsonrpc_types::{BlockNumber, TransactionView};
use config::Config;
use constants::Constants;
use database::{DatabaseProcessor, Operations};
use fetcher::{HttpFetcher, get_db};
use futures::future::try_join_all;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_orm::DbConn;
use spore::SporeTx;
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::{self, JoinHandle},
    time,
};
use tracing::{error, info, warn};
use utils::network::NetworkType;

use crate::{categorization, config, database, inscription, rgbpp, spore, xudt};

use categorization::{CategorizedTxs, categorize_transaction};

// Consider making this configurable
const MAX_CONCURRENT_BATCHES: usize = 4;

pub struct Indexer {
    height: u64,
    target_height: u64,
    batch_size: u64,
    max_batch_size: u64,
    interval: f32,
    client: HttpFetcher,
    db: DbConn,
    constants: Constants,
    network: NetworkType,
    // State for pipelined processing
    db_commit_handles: VecDeque<JoinHandle<Result<()>>>,
    inflight_semaphore: Arc<Semaphore>,
}

impl Indexer {
    pub fn new(
        initial_height: u64,
        config: &Config,
        client: HttpFetcher,
        network: NetworkType,
        constants: Constants,
        db: DbConn,
    ) -> Self {
        let max_batch_size = config.unistate.optional_config.batch_size;
        let interval = config.unistate.optional_config.interval;
        Self {
            height: initial_height,
            target_height: 0,
            batch_size: 0,
            max_batch_size,
            interval,
            client,
            db,
            constants,
            network,
            db_commit_handles: VecDeque::with_capacity(MAX_CONCURRENT_BATCHES),
            inflight_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCHES)),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.update_target_height().await?;

        loop {
            // --- Cleanup finished DB tasks ---
            while let Some(handle) = self.db_commit_handles.front() {
                if handle.is_finished() {
                    let finished_handle = self.db_commit_handles.pop_front().unwrap();
                    match finished_handle.await {
                        Ok(Ok(())) => {
                            info!("Background DB commit task finished successfully.")
                        }
                        Ok(Err(db_err)) => {
                            // Critical error: Decide whether to stop the indexer
                            error!(
                                "Background DB commit task failed: {:?}. Halting may be necessary.",
                                db_err
                            );
                            return Err(db_err).context("Background DB commit task failed");
                        }
                        Err(join_err) => {
                            // Critical error: Decide whether to stop the indexer
                            error!(
                                "Error joining background DB task: {:?}. Halting may be necessary.",
                                join_err
                            );
                            return Err(join_err).context("Failed to join background DB task");
                        }
                    }
                } else {
                    // The first handle is not finished, so subsequent ones likely aren't either.
                    break;
                }
            }

            // --- Determine batch size ---
            self.batch_size =
                (self.target_height.saturating_sub(self.height)).min(self.max_batch_size);

            if self.batch_size > 0 {
                // --- Acquire semaphore permit ---
                // This limits the number of concurrent processing+committing batches.
                let _permit = match self.inflight_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => {
                        // This should not happen if semaphore is used correctly
                        error!("Failed to acquire semaphore permit: {}", e);
                        return Err(anyhow::anyhow!("Failed to acquire semaphore permit: {}", e));
                    }
                };

                let batch_start_height = self.height;
                let batch_end_height = batch_start_height + self.batch_size;
                let inclusive_end_height = batch_end_height.saturating_sub(1);

                info!(
                    "Dispatching batch: blocks {}..={} ({} items) | Current Height: {} | Target Height: {} | Inflight: {}",
                    batch_start_height,
                    inclusive_end_height,
                    self.batch_size,
                    self.height,
                    self.target_height,
                    MAX_CONCURRENT_BATCHES - self.inflight_semaphore.available_permits(),
                );

                // --- Setup DB Processor for this batch ---
                // The commit_signal_sender is used to tell the DB processor it's safe to commit.
                let (database_processor, op_sender, commit_signal_sender) =
                    DatabaseProcessor::new(self.db.clone(), inclusive_end_height);
                // Spawn the DB processor task itself
                let db_processor_task_handle = task::spawn(database_processor.handle());

                // --- Spawn Batch Processing Task ---
                let fetcher_clone = self.client.clone();
                let constants_clone = self.constants;
                let network_clone = self.network;

                task::spawn(async move {
                    let result = process_batch_inner(
                        fetcher_clone,
                        constants_clone,
                        network_clone,
                        batch_start_height,
                        batch_end_height,
                        op_sender,            // op_sender is moved here
                        commit_signal_sender, // commit_signal_sender is moved here
                        batch_start_height,
                        inclusive_end_height,
                    )
                    .await;

                    if let Err(e) = result {
                        error!(
                            "Error processing batch {}..={}: {:?}",
                            batch_start_height, inclusive_end_height, e
                        );
                        // Don't send commit signal if processing failed.
                        // The DB processor will eventually time out or exit gracefully
                        // when op_sender is dropped if not already done.
                        // Permit is dropped automatically when this task scope ends.
                    }
                    // If processing succeeded, the commit_signal_sender was sent inside process_batch_inner
                    // Permit is dropped automatically when this task scope ends.
                });

                // --- Spawn a separate task to manage DB commit and release semaphore ---
                let db_commit_waiter_handle = task::spawn(async move {
                    // Directly wait for the DB processor task to finish.
                    // The DB processor internally waits for the commit signal sent by process_batch_inner.
                    let db_result = db_processor_task_handle.await; // Wait for DB commit task

                    // Permit is dropped automatically when this task scope ends.
                    // Return the result of the DB operation.
                    match db_result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(db_err)) => Err(db_err).context("Database processor task failed"),
                        Err(join_err) => {
                            Err(join_err).context("Failed to join Database processor task")
                        }
                    }
                });

                // Store the handle for the waiter task (which includes DB commit)
                self.db_commit_handles.push_back(db_commit_waiter_handle);

                // --- Update height immediately to allow next iteration ---
                self.update_height();
            } else {
                // --- Caught up, wait for remaining commits and new blocks ---
                if !self.db_commit_handles.is_empty() {
                    info!(
                        "Caught up to target height {}. Waiting for {} pending DB commit(s)...",
                        self.target_height,
                        self.db_commit_handles.len()
                    );
                    while let Some(handle) = self.db_commit_handles.pop_front() {
                        match handle.await {
                            Ok(Ok(())) => info!("Final DB commit finished successfully."),
                            Ok(Err(db_err)) => error!("Final DB commit failed: {:?}", db_err), // Potentially halt
                            Err(join_err) => error!("Error joining final DB task: {:?}", join_err), // Potentially halt
                        }
                        // Ensure semaphore permits are released if awaited here (they should be by the waiter task)
                    }
                    info!("All pending DB commits finished.");
                } else {
                    info!(
                        "Caught up to target height {}. Waiting for new blocks...",
                        self.target_height
                    );
                }

                // Wait for new blocks before checking again
                time::sleep(Duration::from_secs(6)).await;
                self.update_target_height().await?;

                // Optional interval sleep only if fully caught up AND interval > 0
                if self.height >= self.target_height && self.interval > 0.0 {
                    time::sleep(Duration::from_secs_f32(self.interval)).await;
                }
            }
        }
    }

    fn update_height(&mut self) {
        self.height += self.batch_size;
    }

    async fn update_target_height(&mut self) -> Result<()> {
        match self.client.get_tip_block_number().await {
            Ok(tip_number) => {
                let new_target = tip_number.value();
                if new_target > self.target_height {
                    info!("New tip height detected: {}", new_target);
                    self.target_height = new_target;
                } else if new_target < self.target_height {
                    // This might happen during a reorg, allow target to decrease.
                    warn!(
                        "Node tip height ({}) is lower than current target height ({}). Possible reorg. Adjusting target.",
                        new_target, self.target_height
                    );
                    self.target_height = new_target;
                    // NOTE: A robust indexer would need explicit reorg handling logic here or elsewhere.
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to get tip block number: {:?}. Retaining previous target height {}.",
                    e, self.target_height
                );
                // Don't update target_height on error, retry next cycle.
                // Return error to potentially stop the indexer if desired.
                Err(e).context("Failed to get tip block number")
            }
        }
    }
}

async fn process_batch_inner(
    fetcher: HttpFetcher,
    constants: Constants,
    network: NetworkType,
    batch_start_height: u64,
    batch_end_height: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
    commit_tx: oneshot::Sender<()>, // Signal that processing is done
    processing_log_start: u64,
    processing_log_end: u64,
) -> Result<()> {
    info!(
        "Starting processing task for batch: {} - {}",
        processing_log_start, processing_log_end
    );

    // --- Fetch and Categorize ---
    let (categorized_txs, txs_to_cache) = fetch_and_categorize_transactions(
        &fetcher,
        batch_start_height,
        batch_end_height,
        &constants, // Pass by reference
    )
    .await
    .with_context(|| {
        format!(
            "Failed to fetch and categorize transactions for batch {} - {}",
            processing_log_start, processing_log_end
        )
    })?;

    // --- Cache Transactions (Optional) ---
    // Moved out of the parallel categorization loop
    if !txs_to_cache.is_empty() {
        if let Ok(db) = get_db() {
            for tx in txs_to_cache {
                // Clone the hash before tx is moved
                let tx_hash = tx.hash.clone();
                if let Err(e) = fetcher::cache_transaction(db, tx) {
                    // Pass guard ref
                    warn!(
                        "Failed to cache transaction {} in batch {} - {}: {}",
                        tx_hash, processing_log_start, processing_log_end, e
                    );
                }
            }
        }
    }

    // --- Spawn Sub-Indexer Tasks ---
    let mut sub_task_futures: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = Vec::new();

    let spore_sender = op_sender.clone();
    let xudt_sender = op_sender.clone();
    let rgbpp_sender = op_sender.clone();
    let inscription_sender = op_sender.clone();
    let rgbpp_fetcher = fetcher.clone();

    let spore_txs = categorized_txs.spore_txs;
    let xudt_txs = categorized_txs.xudt_txs;
    let rgbpp_txs = categorized_txs.rgbpp_txs;
    let inscription_txs = categorized_txs.inscription_txs;

    if !spore_txs.is_empty() {
        info!(
            "Spawning Spore indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            spore_txs.len()
        );
        let constants_clone = constants; // Constants is Copy
        let network_clone = network; // NetworkType is Copy
        let handle = task::spawn_blocking(move || {
            spore::SporeIndexer::new(spore_txs, network_clone, constants_clone, spore_sender)
                .index()
        });
        sub_task_futures.push(Box::pin(async move {
            handle.await.map_err(anyhow::Error::from)? // Flatten JoinError
        }));
    }
    if !xudt_txs.is_empty() {
        info!(
            "Spawning XUDT indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            xudt_txs.len()
        );
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            xudt::XudtIndexer::new(xudt_txs, network_clone, xudt_sender).index()
        });
        sub_task_futures.push(Box::pin(async move {
            handle.await.map_err(anyhow::Error::from)?
        }));
    }
    if !rgbpp_txs.is_empty() {
        info!(
            "Spawning RGB++ indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            rgbpp_txs.len()
        );

        let rgbpp_future = task::spawn(rgbpp::run_rgbpp_indexing_logic(
            rgbpp_txs,
            rgbpp_fetcher,
            rgbpp_sender,
        ));

        sub_task_futures.push(Box::pin(async move {
            rgbpp_future
                .await
                .map_err(anyhow::Error::from)?
                .context("RGB++ indexer task failed")
        }));
    }
    if !inscription_txs.is_empty() {
        info!(
            "Spawning Inscription indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            inscription_txs.len()
        );
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            inscription::InscriptionInfoIndexer::new(
                inscription_txs,
                network_clone,
                inscription_sender,
            )
            .index()
        });
        sub_task_futures.push(Box::pin(async move {
            handle.await.map_err(anyhow::Error::from)?
        }));
    }

    // --- Wait for Sub-Indexers ---
    if !sub_task_futures.is_empty() {
        info!(
            "Waiting for {} sub-indexer task(s) for batch {} - {}...",
            sub_task_futures.len(),
            processing_log_start,
            processing_log_end
        );
        try_join_all(sub_task_futures).await.with_context(|| {
            format!(
                "One or more sub-indexer tasks failed for batch {} - {}",
                processing_log_start, processing_log_end
            )
        })?;
        info!(
            "Sub-indexer tasks finished successfully for batch {} - {}.",
            processing_log_start, processing_log_end
        );
    } else {
        info!(
            "No sub-indexer tasks to run for batch {} - {}.",
            processing_log_start, processing_log_end
        );
    }

    // --- Signal DB Processor to Commit ---
    // Send the signal *after* all sub-tasks (which send Ops) are complete.
    commit_tx.send(()).map_err(|_| {
        anyhow::anyhow!(
            "Failed to send commit signal: DB processor receiver dropped for batch ending {}.",
            processing_log_end
        )
    })?;
    info!(
        "Commit signal sent for batch {} - {}",
        processing_log_start, processing_log_end
    );

    Ok(())
}

async fn fetch_and_categorize_transactions(
    fetcher: &HttpFetcher,
    start_number: u64,
    end_number: u64,
    constants: &Constants, // Pass Constants by reference
) -> Result<(CategorizedTxs, Vec<TransactionView>)> {
    // Return txs for caching
    let count = end_number.saturating_sub(start_number);
    let inclusive_end_number = end_number.saturating_sub(1);

    if count == 0 {
        info!(
            "Fetch/Categorize: Skipping empty range {}..{}",
            start_number, end_number
        );
        return Ok((CategorizedTxs::new(), Vec::new()));
    }

    info!(
        "Fetching and categorizing {} blocks: {} - {}",
        count, start_number, inclusive_end_number
    );

    // --- Fetch Blocks ---
    let block_views = fetcher
        .get_blocks_range(BlockNumber::from(start_number)..BlockNumber::from(end_number))
        .await
        .with_context(|| {
            format!(
                "Failed to fetch blocks range {}..{}",
                start_number, end_number
            )
        })?;

    if block_views.len() != count as usize {
        warn!(
            "Fetched {} blocks for range {}..{}, but expected {}. Node might be missing blocks or lagging.",
            block_views.len(),
            start_number,
            end_number,
            count
        );
        // Continue processing with the blocks received.
        if block_views.is_empty() {
            return Ok((CategorizedTxs::new(), Vec::new()));
        }
    }

    // --- Parallel Categorization (Over Blocks) ---
    let results = block_views
        .into_par_iter() // Parallelize over blocks
        .filter_map(|block| {
            // Use filter_map to handle potential errors or skips gracefully
            let block_num = block.header.inner.number.value();
            // Strict check: only process blocks within the *requested* range.
            // get_blocks_range *should* guarantee this, but belts and suspenders.
            if block_num < start_number || block_num >= end_number {
                error!(
                    "Received block {} outside of requested range {}..{}. Skipping.",
                    block_num, start_number, end_number
                );
                return None; // Skip this block
            }

            let timestamp = block.header.inner.timestamp.value();
            let mut block_categorized_txs = CategorizedTxs::new();
            let mut block_txs_to_cache = Vec::with_capacity(block.transactions.len());

            // Process transactions sequentially *within* this block's task
            for tx in block.transactions {
                let categorizes = categorize_transaction(&tx, constants); // Pass tx by reference

                if categorizes.is_spore {
                    block_categorized_txs.spore_txs.push(SporeTx {
                        tx: tx.clone(), // Clone only when needed
                        timestamp,
                    });
                }
                if categorizes.is_xudt {
                    block_categorized_txs.xudt_txs.push(tx.clone());
                }
                if categorizes.is_rgbpp {
                    block_categorized_txs.rgbpp_txs.push(tx.clone());
                }
                if categorizes.is_inscription {
                    block_categorized_txs.inscription_txs.push(tx.clone());
                }

                // Collect for caching after parallel processing
                if get_db().is_ok() {
                    // Only collect if caching is enabled
                    block_txs_to_cache.push(tx); // Clones the TransactionView
                }
            }
            Some((block_categorized_txs, block_txs_to_cache))
        })
        .reduce(
            || (CategorizedTxs::new(), Vec::new()), // Identity element for reduction
            |mut a, b| {
                // Reducer function
                a.0 = a.0.merge(b.0); // Merge CategorizedTxs
                a.1.extend(b.1); // Extend Vec<TransactionView>
                a
            },
        );

    info!(
        "Finished fetching and categorizing blocks: {} - {}. Found: {} Spore, {} XUDT, {} RGB++, {} Inscription. {} Txs to cache.",
        start_number,
        inclusive_end_number,
        results.0.spore_txs.len(),
        results.0.xudt_txs.len(),
        results.0.rgbpp_txs.len(),
        results.0.inscription_txs.len(),
        results.1.len()
    );
    Ok(results)
}
