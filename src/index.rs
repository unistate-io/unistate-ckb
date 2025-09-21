use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ckb_jsonrpc_types::{BlockNumber, CellOutput, TransactionView};
use ckb_types::H256;
use config::Config;
use constants::Constants;
use database::{DatabaseProcessor, Operations};
use fetcher::{get_db, get_fetcher};
use futures::future::try_join_all;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_orm::DbConn;
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::{self, JoinHandle},
    time,
};
use tracing::{debug, error, info, warn};
use utils::network::NetworkType;

use crate::transaction_context::TxContext;
use crate::{config, database, inscription, rgbpp, spore, xudt};

/// Indexer types for unified transaction processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerType {
    Spore,
    Xudt,
    Rgbpp,
    Inscription,
}

/// Pre-analysis results for transaction processing
#[derive(Debug, Clone)]
pub struct TransactionAnalysis {
    pub spore: spore::SporeAnalysis,
    pub rgbpp: rgbpp::RgbppAnalysis,
}

impl IndexerType {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexerType::Spore => "Spore",
            IndexerType::Xudt => "XUDT",
            IndexerType::Rgbpp => "RGB++",
            IndexerType::Inscription => "Inscription",
        }
    }
}

pub struct TxContexts {
    pub txs: Vec<TxContext>,
}

/// Analyze transaction to determine processing requirements
pub fn analyze_transaction(tx: &ckb_jsonrpc_types::TransactionView) -> TransactionAnalysis {
    // Parallel analysis of Spore and RGB++ components
    let (spore_analysis, rgbpp_analysis) = rayon::join(
        || spore::analyze_spore_transaction(tx),
        || rgbpp::analyze_rgbpp_transaction(tx),
    );

    TransactionAnalysis {
        spore: spore_analysis,
        rgbpp: rgbpp_analysis,
    }
}

/// Determine if transaction needs previous outputs pre-fetching
pub fn needs_prev_outputs_prefetch(analysis: &TransactionAnalysis) -> bool {
    analysis.spore.has_burn_operations || analysis.rgbpp.has_unlock_operations
}

impl TxContexts {
    pub fn new() -> Self {
        Self { txs: Vec::new() }
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.txs.extend(other.txs);
        self
    }
}

pub struct Indexer {
    height: u64,
    target_height: u64,
    batch_size: u64,
    max_batch_size: u64,
    interval: f32,
    db: DbConn,
    constants: Constants,
    network: NetworkType,
    db_commit_handles: VecDeque<(u64, JoinHandle<Result<()>>)>,
    inflight_semaphore: Arc<Semaphore>,
    max_concurrent_batches: usize,
}

impl Indexer {
    pub fn new(
        initial_height: u64,
        config: &Config,
        network: NetworkType,
        constants: Constants,
        db: DbConn,
    ) -> Self {
        let max_batch_size = config.unistate.optional_config.batch_size;
        let interval = config.unistate.optional_config.interval;
        let max_concurrent_batches = config.unistate.optional_config.max_concurrent_batches;
        Self {
            height: initial_height,
            target_height: 0,
            batch_size: 0,
            max_batch_size,
            interval,
            db,
            constants,
            network,
            db_commit_handles: VecDeque::with_capacity(max_concurrent_batches),
            inflight_semaphore: Arc::new(Semaphore::new(max_concurrent_batches)),
            max_concurrent_batches,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.update_target_height().await?;
        info!(
            "Initial height: {}, Target height: {}",
            self.height, self.target_height
        );

        loop {
            let mut last_successfully_committed_height = self.height;

            while let Some((_, handle)) = self.db_commit_handles.front() {
                if !handle.is_finished() {
                    break;
                }

                let (finished_batch_next_height, finished_handle) =
                    self.db_commit_handles.pop_front().unwrap();

                match finished_handle.await {
                    Ok(Ok(())) => {
                        debug!(
                            "DB commit task for batch ending at {} finished successfully.",
                            finished_batch_next_height - 1
                        );
                        last_successfully_committed_height = finished_batch_next_height;
                    }
                    Ok(Err(db_err)) => {
                        error!(
                            "DB commit task for batch ending at {} failed: {:?}. Halting indexer.",
                            finished_batch_next_height - 1,
                            db_err
                        );
                        return Err(db_err).context(format!(
                            "DB commit task failed for batch ending {}",
                            finished_batch_next_height - 1
                        ));
                    }
                    Err(join_err) => {
                        error!(
                            "Error joining DB commit task for batch ending at {}: {:?}. Halting indexer.",
                            finished_batch_next_height - 1,
                            join_err
                        );
                        return Err(join_err).context(format!(
                            "Failed to join DB commit task for batch ending {}",
                            finished_batch_next_height - 1
                        ));
                    }
                }
            }

            if last_successfully_committed_height > self.height {
                debug!(
                    "Updating indexer height from {} to {}",
                    self.height, last_successfully_committed_height
                );
                self.height = last_successfully_committed_height;
            }

            self.batch_size =
                (self.target_height.saturating_sub(self.height)).min(self.max_batch_size);

            if self.batch_size > 0 {
                while self.db_commit_handles.len() >= self.max_concurrent_batches {
                    info!(
                        "Commit pipeline full ({} pending). Waiting for oldest commit task to complete...",
                        self.db_commit_handles.len()
                    );
                    if let Some((oldest_batch_next_height, oldest_handle)) =
                        self.db_commit_handles.pop_front()
                    {
                        match oldest_handle.await {
                            Ok(Ok(())) => {
                                info!("Oldest DB commit task finished successfully while waiting.");
                                last_successfully_committed_height = oldest_batch_next_height;
                                if last_successfully_committed_height > self.height {
                                    debug!(
                                        "Updating indexer height from {} to {} while waiting",
                                        self.height, last_successfully_committed_height
                                    );
                                    self.height = last_successfully_committed_height;
                                }
                            }
                            Ok(Err(db_err)) => {
                                error!(
                                    "Oldest DB commit task failed while waiting: {:?}. Halting indexer.",
                                    db_err
                                );
                                return Err(db_err)
                                    .context("Oldest background DB commit task failed");
                            }
                            Err(join_err) => {
                                error!(
                                    "Error joining oldest DB task while waiting: {:?}. Halting indexer.",
                                    join_err
                                );
                                return Err(join_err)
                                    .context("Failed to join oldest background DB task");
                            }
                        }
                    } else {
                        warn!("Commit handle queue was unexpectedly empty while waiting.");
                        time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                }

                if self.db_commit_handles.len() >= self.max_concurrent_batches {
                    warn!("Pipeline still full after waiting, skipping dispatch this cycle.");
                    time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                let permit = match self.inflight_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to acquire semaphore permit: {}", e);
                        return Err(anyhow::anyhow!("Failed to acquire semaphore permit: {}", e));
                    }
                };

                let batch_start_height = self.height;
                let batch_end_height = batch_start_height + self.batch_size;
                let inclusive_end_height = batch_end_height.saturating_sub(1);
                let next_batch_start_height = inclusive_end_height + 1;

                info!(
                    "Dispatching batch: blocks {}..={} ({} items) | Current Height: {} | Target Height: {} | Inflight: {}/{}",
                    batch_start_height,
                    inclusive_end_height,
                    self.batch_size,
                    self.height,
                    self.target_height,
                    self.max_concurrent_batches - self.inflight_semaphore.available_permits(),
                    self.max_concurrent_batches
                );

                let (database_processor, op_sender, commit_signal_sender) =
                    DatabaseProcessor::new(self.db.clone(), next_batch_start_height);
                let db_processor_task_handle = task::spawn(database_processor.handle());

                let constants_clone = self.constants;
                let network_clone = self.network;
                let op_sender_clone = op_sender.clone();

                let processing_task_handle = task::spawn(async move {
                    let result = process_batch_inner(
                        constants_clone,
                        network_clone,
                        batch_start_height,
                        batch_end_height,
                        op_sender_clone,
                        commit_signal_sender,
                        batch_start_height,
                        inclusive_end_height,
                    )
                    .await;

                    if let Err(e) = &result {
                        error!(
                            "Error processing batch {}..={}: {:?}",
                            batch_start_height, inclusive_end_height, e
                        );
                    }
                    result
                });

                let db_commit_waiter_handle = task::spawn(async move {
                    match processing_task_handle.await {
                        Ok(Ok(())) => {}
                        Ok(Err(processing_err)) => {
                            error!(
                                "Processing task for batch {}..={} failed: {:?}",
                                batch_start_height, inclusive_end_height, processing_err
                            );
                            drop(db_processor_task_handle);
                            return Err(processing_err).context(format!(
                                "Processing task failed for batch {}..={}",
                                batch_start_height, inclusive_end_height
                            ));
                        }
                        Err(join_err) => {
                            error!(
                                "Processing task for batch {}..={} panicked or was cancelled: {:?}",
                                batch_start_height, inclusive_end_height, join_err
                            );
                            drop(db_processor_task_handle);
                            return Err(anyhow::Error::from(join_err).context(format!(
                                "Processing task join error for batch {}..={}",
                                batch_start_height, inclusive_end_height
                            )));
                        }
                    };

                    let db_result = db_processor_task_handle.await;
                    drop(permit);

                    match db_result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(db_err)) => Err(db_err).context(format!(
                            "Database processor task failed for batch ending {}",
                            inclusive_end_height
                        )),
                        Err(join_err) => Err(anyhow::Error::from(join_err)).context(format!(
                            "Failed to join Database processor task for batch ending {}",
                            inclusive_end_height
                        )),
                    }
                });

                self.db_commit_handles
                    .push_back((next_batch_start_height, db_commit_waiter_handle));
            } else {
                if !self.db_commit_handles.is_empty() {
                    info!(
                        "Caught up to target height {}. Waiting for {} pending DB commit(s) to finish...",
                        self.target_height,
                        self.db_commit_handles.len()
                    );
                    while let Some((finished_batch_next_height, handle)) =
                        self.db_commit_handles.pop_front()
                    {
                        match handle.await {
                            Ok(Ok(())) => {
                                info!("Final batch DB commit finished successfully.");
                                last_successfully_committed_height = finished_batch_next_height;
                            }
                            Ok(Err(db_err)) => {
                                error!(
                                    "Final batch DB commit failed: {:?}. Halting indexer.",
                                    db_err
                                );
                                return Err(db_err).context("Final batch DB commit task failed");
                            }
                            Err(join_err) => {
                                error!(
                                    "Error joining final DB task: {:?}. Halting indexer.",
                                    join_err
                                );
                                return Err(join_err).context("Failed to join final DB task");
                            }
                        }
                    }
                    info!("All pending DB commits finished.");
                    if last_successfully_committed_height > self.height {
                        info!(
                            "Final height update from {} to {}",
                            self.height, last_successfully_committed_height
                        );
                        self.height = last_successfully_committed_height;
                    }
                } else {
                    info!(
                        "Caught up to target height {}. No pending commits. Waiting for new blocks...",
                        self.target_height
                    );
                }

                time::sleep(Duration::from_secs(6)).await;
                self.update_target_height().await?;

                if self.height >= self.target_height
                    && self.interval > 0.0
                    && self.db_commit_handles.is_empty()
                {
                    debug!("Sleeping for interval: {}s", self.interval);
                    time::sleep(Duration::from_secs_f32(self.interval)).await;
                }
            }
        }
    }

    async fn update_target_height(&mut self) -> Result<()> {
        match get_fetcher()?.get_tip_block_number().await {
            Ok(tip_number) => {
                let new_target = tip_number.value();
                if new_target > self.target_height {
                    info!("New tip height detected: {}", new_target);
                    self.target_height = new_target;
                } else if new_target < self.target_height {
                    warn!(
                        "Node tip height ({}) is lower than current target height ({}). Possible reorg. Adjusting target.",
                        new_target, self.target_height
                    );
                    self.target_height = new_target;
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to get tip block number: {:?}. Retaining previous target height {}.",
                    e, self.target_height
                );
                Err(e).context("Failed to get tip block number")
            }
        }
    }
}

async fn process_batch_inner(
    constants: Constants,
    network: NetworkType,
    batch_start_height: u64,
    batch_end_height: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
    commit_tx: oneshot::Sender<()>,
    processing_log_start: u64,
    processing_log_end: u64,
) -> Result<(), anyhow::Error> {
    let _sender_guard = SenderGuard(op_sender.clone());

    info!(
        "Starting processing task for batch: {} - {}",
        processing_log_start, processing_log_end
    );

    let (tx_contexts, txs_to_cache) =
        fetch_and_process_transactions(batch_start_height, batch_end_height)
            .await
            .with_context(|| {
                format!(
                    "Failed to fetch and process transactions for batch {} - {}",
                    processing_log_start, processing_log_end
                )
            })?;

    if !txs_to_cache.is_empty() {
        if let Ok(db) = get_db() {
            let cache_result = task::spawn_blocking(move || -> Result<(), fetcher::Error> {
                for tx in txs_to_cache {
                    let tx_hash = tx.hash.clone();
                    if let Err(e) = fetcher::cache_transaction(db, &tx) {
                        warn!(
                            "Failed to cache transaction {} in batch {} - {}: {}",
                            tx_hash, processing_log_start, processing_log_end, e
                        );
                    }
                }
                Ok(())
            })
            .await;

            if let Err(e) = cache_result {
                error!("Error during background transaction caching task: {:?}", e);
            } else if let Ok(Err(cache_err)) = cache_result {
                error!(
                    "Error within background transaction caching: {:?}",
                    cache_err
                );
            }
        }
    }

    let mut sub_task_futures: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = Vec::new();

    let spore_sender = _sender_guard.0.clone();
    let xudt_sender = _sender_guard.0.clone();
    let rgbpp_sender = _sender_guard.0.clone();
    let inscription_sender = _sender_guard.0.clone();

    let tx_contexts_data = tx_contexts.txs;

    if !tx_contexts_data.is_empty() {
        info!(
            "Spawning unified indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            tx_contexts_data.len()
        );

        let constants_clone = constants;
        let network_clone = network;
        let handle = task::spawn(async move {
            index_all_transactions(
                tx_contexts_data,
                network_clone,
                constants_clone,
                spore_sender,
                xudt_sender,
                rgbpp_sender,
                inscription_sender,
            )
            .await
        });
        sub_task_futures.push(Box::pin(async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e).context("Unified indexer task failed"),
                Err(e) => Err(anyhow::Error::from(e)).context("Unified indexer task join error"),
            }
        }));
    }

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

    commit_tx.send(()).map_err(|_| {
        error!(
            "Failed to send commit signal: DB processor receiver dropped for batch ending {}.",
            processing_log_end
        );
        anyhow::anyhow!(
            "DB processor receiver dropped before commit signal for batch ending {}",
            processing_log_end
        )
    })?;
    info!(
        "Commit signal sent for batch {} - {}",
        processing_log_start, processing_log_end
    );

    drop(_sender_guard);

    Ok(())
}

struct SenderGuard(mpsc::UnboundedSender<Operations>);

impl Drop for SenderGuard {
    fn drop(&mut self) {
        debug!("Operation sender dropped.");
    }
}

async fn index_all_transactions(
    tx_contexts: Vec<TxContext>,
    network: NetworkType,
    constants: Constants,
    spore_sender: mpsc::UnboundedSender<Operations>,
    xudt_sender: mpsc::UnboundedSender<Operations>,
    rgbpp_sender: mpsc::UnboundedSender<Operations>,
    inscription_sender: mpsc::UnboundedSender<Operations>,
) -> Result<(), anyhow::Error> {
    use rayon::prelude::*;

    // Phase 1: Parallel analysis of all transactions
    let analysis_results: std::collections::HashMap<H256, TransactionAnalysis> = tx_contexts
        .par_iter()
        .map(|ctx| {
            let analysis = analyze_transaction(&ctx.tx);
            (ctx.tx.hash.clone(), analysis)
        })
        .collect();

    // Phase 2: Determine which transactions need prev_outputs and fetch them
    let txs_need_prefetch: Vec<H256> = analysis_results
        .iter()
        .filter_map(|(tx_hash, analysis)| {
            if needs_prev_outputs_prefetch(analysis)
                && tx_contexts
                    .iter()
                    .any(|ctx| ctx.tx.hash == *tx_hash && !ctx.tx.inner.inputs.is_empty())
            {
                Some(tx_hash.clone())
            } else {
                None
            }
        })
        .collect();

    let prev_outputs_map = if !txs_need_prefetch.is_empty() {
        let prev_outputs_futures: Vec<_> = txs_need_prefetch
            .iter()
            .filter_map(|tx_hash| {
                tx_contexts
                    .iter()
                    .find(|ctx| ctx.tx.hash == *tx_hash)
                    .map(|ctx| {
                        let inputs = ctx.tx.inner.inputs.clone();
                        let tx_hash = ctx.tx.hash.clone();
                        async move {
                            match get_fetcher()?.get_outputs(inputs).await {
                                Ok(prev_outputs) => {
                                    Ok::<Option<(H256, Vec<CellOutput>)>, anyhow::Error>(Some((
                                        tx_hash,
                                        prev_outputs,
                                    )))
                                }
                                Err(e) => {
                                    debug!(
                                        "Failed to fetch previous outputs for tx {}: {:?}",
                                        tx_hash, e
                                    );
                                    Ok(None)
                                }
                            }
                        }
                    })
            })
            .collect();

        let prev_outputs_results = try_join_all(prev_outputs_futures).await?;
        let mut map = std::collections::HashMap::new();
        for result in prev_outputs_results {
            if let Some((tx_hash, prev_outputs)) = result {
                map.insert(tx_hash, prev_outputs);
            }
        }
        map
    } else {
        std::collections::HashMap::new()
    };

    // Phase 3: Process all transactions with pre-analysis results
    if tx_contexts.len() > 100 {
        // Process in chunks for better memory locality and resource management
        tx_contexts.par_chunks(25).try_for_each(|chunk| {
            chunk.into_par_iter().try_for_each(|ctx| {
                let analysis = analysis_results.get(&ctx.tx.hash).unwrap();
                let prev_outputs = prev_outputs_map.get(&ctx.tx.hash).cloned();
                process_transaction_with_all_indexers(
                    &ctx,
                    network,
                    constants,
                    &spore_sender,
                    &xudt_sender,
                    &rgbpp_sender,
                    &inscription_sender,
                    analysis,
                    prev_outputs,
                )
            })
        })
    } else {
        // Simple parallel iteration for small batches
        tx_contexts.into_par_iter().try_for_each(|ctx| {
            let analysis = analysis_results.get(&ctx.tx.hash).unwrap();
            let prev_outputs = prev_outputs_map.get(&ctx.tx.hash).cloned();
            process_transaction_with_all_indexers(
                &ctx,
                network,
                constants,
                &spore_sender,
                &xudt_sender,
                &rgbpp_sender,
                &inscription_sender,
                analysis,
                prev_outputs,
            )
        })
    }
}

/// Helper function to process a single transaction with all indexers
fn process_transaction_with_all_indexers(
    ctx: &TxContext,
    network: NetworkType,
    constants: Constants,
    spore_sender: &mpsc::UnboundedSender<Operations>,
    xudt_sender: &mpsc::UnboundedSender<Operations>,
    rgbpp_sender: &mpsc::UnboundedSender<Operations>,
    inscription_sender: &mpsc::UnboundedSender<Operations>,
    analysis: &TransactionAnalysis,
    prev_outputs: Option<Vec<CellOutput>>,
) -> Result<(), anyhow::Error> {
    let tx_hash = ctx.tx.hash.clone(); // Store hash for logging
    let tx_hash_hex = hex::encode(&tx_hash);

    tracing::debug!(
        "Processing transaction {} at block {} with {} inputs, {} outputs, {} witnesses",
        tx_hash_hex,
        ctx.block_number,
        ctx.tx.inner.inputs.len(),
        ctx.tx.inner.outputs.len(),
        ctx.tx.inner.witnesses.len()
    );

    // Clone transaction data for parallel processing
    let tx_spore = ctx.tx.clone();
    let tx_xudt = ctx.tx.clone();
    let tx_rgbpp = ctx.tx.clone();
    let tx_inscription = ctx.tx.clone(); // Clone for the closure

    let timestamp = ctx.timestamp;
    let block_number = ctx.block_number;

    // Run all four indexers in parallel
    let results: Vec<Result<(), anyhow::Error>> = vec![
        (IndexerType::Spore, spore_sender.clone(), tx_spore),
        (IndexerType::Xudt, xudt_sender.clone(), tx_xudt),
        (IndexerType::Rgbpp, rgbpp_sender.clone(), tx_rgbpp),
        (
            IndexerType::Inscription,
            inscription_sender.clone(),
            tx_inscription,
        ),
    ]
    .into_par_iter()
    .map(|(indexer_type, sender, tx)| {
        let result = match indexer_type {
            IndexerType::Spore => spore::try_index_spore(
                tx,
                timestamp,
                block_number,
                network,
                constants,
                sender,
                &analysis.spore,
                prev_outputs.clone(),
            ),
            IndexerType::Xudt => xudt::try_index_xudt_transaction(
                tx,
                timestamp,
                block_number,
                network,
                constants,
                sender,
            ),
            IndexerType::Rgbpp => rgbpp::try_index_rgbpp_transaction(
                tx,
                timestamp,
                block_number,
                prev_outputs.clone(),
                sender,
                &analysis.rgbpp,
            ),
            IndexerType::Inscription => inscription::try_index_inscription_transaction(
                tx,
                timestamp,
                block_number,
                network,
                sender,
            ),
        };

        if let Err(ref e) = result {
            debug!(
                "{} indexer error for tx {}: {:?}",
                indexer_type.as_str(),
                tx_hash,
                e
            );
        } else {
            debug!(
                "{} indexer completed for tx {}",
                indexer_type.as_str(),
                tx_hash_hex
            );
        }
        result
    })
    .collect();

    // Check if any indexer had a hard error that should stop processing
    for (i, result) in results.iter().enumerate() {
        let indexer_name = match i {
            0 => "Spore",
            1 => "XUDT",
            2 => "RGB++",
            3 => "Inscription",
            _ => "Unknown",
        };
        if let Err(e) = result {
            // Most indexers return Ok(()) when transaction doesn't match their type
            // So we only care about actual errors
            debug!(
                "{} indexer error for tx {}: {:?}",
                indexer_name, tx_hash_hex, e
            );
        }
    }

    Ok(())
}

async fn fetch_and_process_transactions(
    start_number: u64,
    end_number: u64,
) -> Result<(TxContexts, Vec<TransactionView>)> {
    let count = end_number.saturating_sub(start_number);
    let inclusive_end_number = end_number.saturating_sub(1);

    if count == 0 {
        info!(
            "Fetch/Categorize: Skipping empty range {}..{}",
            start_number, end_number
        );
        return Ok((TxContexts::new(), Vec::new()));
    }

    info!(
        "Fetching and processing {} blocks: {} - {}",
        count, start_number, inclusive_end_number
    );

    let block_views = get_fetcher()?
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
        if block_views.is_empty() && count > 0 {
            error!(
                "Fetched 0 blocks for non-empty range {}..{}. Cannot proceed.",
                start_number, end_number
            );
            return Err(anyhow::anyhow!(
                "Failed to fetch any blocks in range {}..{}",
                start_number,
                end_number
            ));
        }
        if block_views.is_empty() {
            return Ok((TxContexts::new(), Vec::new()));
        }
    }

    let results = block_views
        .into_par_iter()
        .filter_map(|block| {
            let block_num = block.header.inner.number.value();
            if block_num < start_number || block_num >= end_number {
                error!(
                    "Received block {} outside of requested range {}..{}. Skipping.",
                    block_num, start_number, end_number
                );
                return None;
            }

            let timestamp = block.header.inner.timestamp.value();
            let mut block_tx_contexts = TxContexts::new();
            let mut block_txs_to_cache = Vec::with_capacity(block.transactions.len());

            for tx in block.transactions {
                // All transactions are processed without pre-categorization
                block_tx_contexts
                    .txs
                    .push(TxContext::new(tx.clone(), block_num, timestamp));

                if get_db().is_ok() {
                    block_txs_to_cache.push(tx);
                }
            }
            Some((block_tx_contexts, block_txs_to_cache))
        })
        .reduce(
            || (TxContexts::new(), Vec::new()),
            |mut a, b| {
                a.0 = a.0.merge(b.0);
                a.1.extend(b.1);
                a
            },
        );

    info!(
        "Finished fetching and processing blocks: {} - {}. Total transactions: {}. Txs to cache: {}.",
        start_number,
        inclusive_end_number,
        results.0.txs.len(),
        results.1.len()
    );
    Ok(results)
}
