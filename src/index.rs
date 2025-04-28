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

use crate::categorization::categorize_transaction;
use crate::inscription::InscriptionTxContext;
use crate::rgbpp::RgbppTxContext;
use crate::spore::SporeTx;
use crate::xudt::XudtTxContext;
use crate::{config, database, inscription, rgbpp, spore, xudt};

pub struct CategorizedTxContexts {
    pub spore_txs: Vec<SporeTx>,
    pub xudt_txs: Vec<XudtTxContext>,
    pub rgbpp_txs: Vec<RgbppTxContext>,
    pub inscription_txs: Vec<InscriptionTxContext>,
}

impl CategorizedTxContexts {
    pub fn new() -> Self {
        Self {
            spore_txs: Vec::new(),
            xudt_txs: Vec::new(),
            rgbpp_txs: Vec::new(),
            inscription_txs: Vec::new(),
        }
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.spore_txs.extend(other.spore_txs);
        self.xudt_txs.extend(other.xudt_txs);
        self.rgbpp_txs.extend(other.rgbpp_txs);
        self.inscription_txs.extend(other.inscription_txs);
        self
    }
}

const MAX_CONCURRENT_BATCHES: usize = 10;

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
        Self {
            height: initial_height,
            target_height: 0,
            batch_size: 0,
            max_batch_size,
            interval,
            db,
            constants,
            network,
            db_commit_handles: VecDeque::with_capacity(MAX_CONCURRENT_BATCHES),
            inflight_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCHES)),
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
                while self.db_commit_handles.len() >= MAX_CONCURRENT_BATCHES {
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

                if self.db_commit_handles.len() >= MAX_CONCURRENT_BATCHES {
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
                    MAX_CONCURRENT_BATCHES - self.inflight_semaphore.available_permits(),
                    MAX_CONCURRENT_BATCHES
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

    let (categorized_data, txs_to_cache) =
        fetch_and_categorize_transactions(batch_start_height, batch_end_height, &constants)
            .await
            .with_context(|| {
                format!(
                    "Failed to fetch and categorize transactions for batch {} - {}",
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

    let spore_tx_contexts = categorized_data.spore_txs;
    let xudt_tx_contexts = categorized_data.xudt_txs;
    let rgbpp_tx_contexts = categorized_data.rgbpp_txs;
    let inscription_tx_contexts = categorized_data.inscription_txs;

    if !spore_tx_contexts.is_empty() {
        info!(
            "Spawning Spore indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            spore_tx_contexts.len()
        );
        let constants_clone = constants;
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            spore::index_spores(
                spore_tx_contexts,
                network_clone,
                constants_clone,
                spore_sender,
            )
        });
        sub_task_futures.push(Box::pin(async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e).context("Spore indexer task failed"),
                Err(e) => Err(anyhow::Error::from(e)).context("Spore indexer task join error"),
            }
        }));
    }
    if !xudt_tx_contexts.is_empty() {
        info!(
            "Spawning XUDT indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            xudt_tx_contexts.len()
        );
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            xudt::index_xudt_transactions(xudt_tx_contexts, network_clone, xudt_sender)
        });
        sub_task_futures.push(Box::pin(async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e).context("XUDT indexer task failed"),
                Err(e) => Err(anyhow::Error::from(e)).context("XUDT indexer task join error"),
            }
        }));
    }
    if !rgbpp_tx_contexts.is_empty() {
        info!(
            "Spawning RGB++ indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            rgbpp_tx_contexts.len(),
        );
        let rgbpp_future = task::spawn(rgbpp::run_rgbpp_indexing_logic(
            rgbpp_tx_contexts,
            rgbpp_sender,
        ));
        sub_task_futures.push(Box::pin(async move {
            match rgbpp_future.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e).context("RGB++ indexer task failed"),
                Err(e) => Err(anyhow::Error::from(e)).context("RGB++ indexer task join error"),
            }
        }));
    }
    if !inscription_tx_contexts.is_empty() {
        info!(
            "Spawning Inscription indexer task for batch {} - {} ({} txs)",
            processing_log_start,
            processing_log_end,
            inscription_tx_contexts.len()
        );
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            inscription::index_inscription_info_batch(
                inscription_tx_contexts,
                network_clone,
                inscription_sender,
            )
        });
        sub_task_futures.push(Box::pin(async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e).context("Inscription indexer task failed"),
                Err(e) => {
                    Err(anyhow::Error::from(e)).context("Inscription indexer task join error")
                }
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

async fn fetch_and_categorize_transactions(
    start_number: u64,
    end_number: u64,
    constants: &Constants,
) -> Result<(CategorizedTxContexts, Vec<TransactionView>)> {
    let count = end_number.saturating_sub(start_number);
    let inclusive_end_number = end_number.saturating_sub(1);

    if count == 0 {
        info!(
            "Fetch/Categorize: Skipping empty range {}..{}",
            start_number, end_number
        );
        return Ok((CategorizedTxContexts::new(), Vec::new()));
    }

    info!(
        "Fetching and categorizing {} blocks: {} - {}",
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
            return Ok((CategorizedTxContexts::new(), Vec::new()));
        }
    }

    let results = block_views
        .into_par_iter()
        .flatten()
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
            let mut block_categorized_data = CategorizedTxContexts::new();
            let mut block_txs_to_cache = Vec::with_capacity(block.transactions.len());

            for tx in block.transactions {
                let categorizes = categorize_transaction(&tx, constants);

                if categorizes.is_spore {
                    block_categorized_data.spore_txs.push(SporeTx {
                        tx: tx.clone(),
                        timestamp,
                        block_number: block_num,
                    });
                }
                if categorizes.is_xudt {
                    block_categorized_data.xudt_txs.push(XudtTxContext {
                        tx: tx.clone(),
                        timestamp,
                        block_number: block_num,
                    });
                }
                if categorizes.is_rgbpp {
                    block_categorized_data.rgbpp_txs.push(RgbppTxContext {
                        tx: tx.clone(),
                        timestamp,
                        block_number: block_num,
                    });
                }
                if categorizes.is_inscription {
                    block_categorized_data
                        .inscription_txs
                        .push(InscriptionTxContext {
                            tx: tx.clone(),
                            timestamp,
                            block_number: block_num,
                        });
                }

                if get_db().is_ok() {
                    block_txs_to_cache.push(tx);
                }
            }
            Some((block_categorized_data, block_txs_to_cache))
        })
        .reduce(
            || (CategorizedTxContexts::new(), Vec::new()),
            |mut a, b| {
                a.0 = a.0.merge(b.0);
                a.1.extend(b.1);
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
