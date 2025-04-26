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
use spore::SporeTx;
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::{self, JoinHandle},
    time,
};
use tracing::{debug, error, info, warn};
use utils::network::NetworkType;

use crate::{categorization, config, database, inscription, rgbpp, spore, xudt};

use categorization::{CategorizedTxs, categorize_transaction};

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
    db_commit_handles: VecDeque<JoinHandle<Result<()>>>,
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

        loop {
            while let Some(handle) = self.db_commit_handles.front() {
                if handle.is_finished() {
                    let finished_handle = self.db_commit_handles.pop_front().unwrap();
                    match finished_handle.await {
                        Ok(Ok(())) => {
                            debug!("Background DB commit task finished successfully.")
                        }
                        Ok(Err(db_err)) => {
                            error!(
                                "Background DB commit task failed: {:?}. Halting indexer.",
                                db_err
                            );
                            return Err(db_err).context("Background DB commit task failed");
                        }
                        Err(join_err) => {
                            error!(
                                "Error joining background DB task: {:?}. Halting indexer.",
                                join_err
                            );
                            return Err(join_err).context("Failed to join background DB task");
                        }
                    }
                } else {
                    break;
                }
            }

            self.batch_size =
                (self.target_height.saturating_sub(self.height)).min(self.max_batch_size);

            if self.batch_size > 0 {
                while self.db_commit_handles.len() >= MAX_CONCURRENT_BATCHES {
                    info!(
                        "Commit pipeline full ({} pending). Waiting for oldest commit task to complete...",
                        self.db_commit_handles.len()
                    );
                    if let Some(oldest_handle) = self.db_commit_handles.pop_front() {
                        match oldest_handle.await {
                            Ok(Ok(())) => {
                                info!("Oldest DB commit task finished successfully while waiting.");
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
                        break;
                    }
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
                    DatabaseProcessor::new(self.db.clone(), inclusive_end_height);
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

                    if let Err(e) = result {
                        error!(
                            "Error processing batch {}..={}: {:?}",
                            batch_start_height, inclusive_end_height, e
                        );
                    }
                });

                let db_commit_waiter_handle = task::spawn(async move {
                    if let Err(join_err) = processing_task_handle.await {
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

                self.db_commit_handles.push_back(db_commit_waiter_handle);
                self.update_height();
            } else {
                if !self.db_commit_handles.is_empty() {
                    info!(
                        "Caught up to target height {}. Waiting for {} pending DB commit(s) to finish...",
                        self.target_height,
                        self.db_commit_handles.len()
                    );
                    while let Some(handle) = self.db_commit_handles.pop_front() {
                        match handle.await {
                            Ok(Ok(())) => info!("Final batch DB commit finished successfully."),
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

    fn update_height(&mut self) {
        self.height += self.batch_size;
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
) -> Result<()> {
    let _sender_guard = SenderGuard(op_sender.clone());

    info!(
        "Starting processing task for batch: {} - {}",
        processing_log_start, processing_log_end
    );

    let (categorized_txs, txs_to_cache) =
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
                    if let Err(e) = fetcher::cache_transaction(db, tx) {
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
        let constants_clone = constants;
        let network_clone = network;
        let handle = task::spawn_blocking(move || {
            spore::SporeIndexer::new(spore_txs, network_clone, constants_clone, spore_sender)
                .index()
        });
        sub_task_futures.push(Box::pin(async move {
            handle.await.map_err(anyhow::Error::from)?
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

        let rgbpp_future = task::spawn(rgbpp::run_rgbpp_indexing_logic(rgbpp_txs, rgbpp_sender));

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
            "DB processor receiver dropped for batch ending {}",
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
) -> Result<(CategorizedTxs, Vec<TransactionView>)> {
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
        if block_views.is_empty() {
            return Ok((CategorizedTxs::new(), Vec::new()));
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
            let mut block_categorized_txs = CategorizedTxs::new();
            let mut block_txs_to_cache = Vec::with_capacity(block.transactions.len());

            for tx in block.transactions {
                let categorizes = categorize_transaction(&tx, constants);

                if categorizes.is_spore {
                    block_categorized_txs.spore_txs.push(SporeTx {
                        tx: tx.clone(),
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

                if get_db().is_ok() {
                    block_txs_to_cache.push(tx);
                }
            }
            Some((block_categorized_txs, block_txs_to_cache))
        })
        .reduce(
            || (CategorizedTxs::new(), Vec::new()),
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
