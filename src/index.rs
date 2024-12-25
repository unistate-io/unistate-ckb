use std::{future::Future, time::Duration};

use anyhow::Result;
use async_scoped::spawner::use_tokio::Tokio;
use ckb_jsonrpc_types::BlockNumber;
use ckb_sdk::NetworkType;
use config::Config;
use constants::Constants;
use database::{DatabaseProcessor, Operations};
use fetcher::HttpFetcher;
use futures::{StreamExt, TryStreamExt};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_orm::DbConn;
use spore::SporeTx;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::info;

use crate::{categorization, config, database, inscription, rgbpp, spore, xudt};

use categorization::{categorize_transaction, CategorizedTxs};

pub struct Indexer {
    height: u64,
    target_height: u64,
    batch_size: u64,
    max_batch_size: u64,
    interval: f32,
    fetch_size: usize,
    client: HttpFetcher,
    db: DbConn,
    constants: Constants,
    network: NetworkType,
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
        let fetch_size = config.unistate.optional_config.fetch_size;
        Self {
            height: initial_height,
            target_height: 0,
            batch_size: 0,
            max_batch_size,
            interval,
            fetch_size,
            client,
            db,
            constants,
            network,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.update_target_height().await?;

        let mut pre_handle = None;
        let mut scope = unsafe { async_scoped::TokioScope::create(Tokio) };
        loop {
            info!(
                "Fetching batch: {} items | Progress: {}/{}",
                self.batch_size, self.height, self.target_height
            );
            let numbers = self.get_block_numbers();
            self.update_height();

            let (database_processor, op_sender, commited) =
                DatabaseProcessor::new(self.db.clone(), self.height);
            let processor_handle = tokio::spawn(database_processor.handle());

            let handle = self.spawn_indexing_task(numbers, op_sender, commited, pre_handle.take());
            scope.spawn(handle);
            pre_handle = Some(processor_handle);

            while let Ok(Some(res)) = scope.try_next().await {
                res?;
            }

            if scope.remaining() >= self.fetch_size {
                if let Some(res) = scope.next().await {
                    res??;
                }
            }

            if self.batch_size == 0 {
                time::sleep(Duration::from_secs(6)).await;
                self.update_target_height().await?;
            }

            time::sleep(Duration::from_secs_f32(self.interval)).await;
        }
    }

    fn get_block_numbers(&self) -> Vec<BlockNumber> {
        (0..self.batch_size)
            .into_par_iter()
            .map(|i| BlockNumber::from(self.height + i))
            .collect()
    }

    fn update_height(&mut self) {
        self.height += self.batch_size;
        self.batch_size = (self.target_height - self.height).min(self.max_batch_size);
    }

    async fn update_target_height(&mut self) -> Result<()> {
        let new_target = self.client.get_tip_block_number().await?.value();
        if new_target != self.target_height {
            self.target_height = new_target;
            self.batch_size = (self.target_height - self.height).min(self.max_batch_size);
        }
        Ok(())
    }

    fn spawn_indexing_task<'i, 'r>(
        &'i self,
        numbers: Vec<BlockNumber>,
        op_sender: mpsc::UnboundedSender<Operations>,
        commited: oneshot::Sender<()>,
        pre_handle: Option<JoinHandle<Result<()>>>,
    ) -> impl Future<Output = Result<()>> + use<'r> {
        let fetcher = self.client.clone();
        let constants = self.constants;
        let network = self.network;

        async move {
            let categorized_txs =
                fetch_and_categorize_transactions(&fetcher, numbers, &constants).await?;

            let (spore_sender, xudt_sender, rgbpp_sender) =
                (op_sender.clone(), op_sender.clone(), op_sender.clone());

            let (mut scope, _) = unsafe {
                async_scoped::TokioScope::scope(|scope| {
                    scope.spawn_blocking(move || {
                        let spore_idxer = spore::SporeIndexer::new(
                            categorized_txs.spore_txs,
                            network,
                            constants,
                            spore_sender,
                        );
                        spore_idxer.index()
                    });
                    scope.spawn_blocking(move || {
                        let xudt_idxer =
                            xudt::XudtIndexer::new(categorized_txs.xudt_txs, network, xudt_sender);
                        xudt_idxer.index()
                    });
                    scope.spawn(async move {
                        let rgbpp_idxer = rgbpp::RgbppIndexer::new(
                            categorized_txs.rgbpp_txs,
                            fetcher,
                            rgbpp_sender,
                        );
                        rgbpp_idxer.index().await
                    });
                    scope.spawn_blocking(move || {
                        let inscription_idxer = inscription::InscriptionInfoIndexer::new(
                            categorized_txs.inscription_txs,
                            network,
                            op_sender,
                        );
                        inscription_idxer.index()
                    });
                })
            };

            while let Some(res) = scope.next().await {
                res??;
            }

            drop(scope);

            if let Some(pre) = pre_handle {
                pre.await??;
            }

            commited
                .send(())
                .map_err(|_| anyhow::anyhow!("commited failed."))?;

            Ok(())
        }
    }
}

async fn fetch_and_categorize_transactions(
    fetcher: &HttpFetcher,
    numbers: Vec<BlockNumber>,
    constants: &Constants,
) -> Result<CategorizedTxs> {
    let categorize_txs = fetcher
        .get_blocks(numbers)
        .await?
        .into_par_iter()
        .fold(CategorizedTxs::new, |txs, block| {
            let timestamp = block.header.inner.timestamp.value();
            txs.merge(
                block
                    .transactions
                    .into_par_iter()
                    .fold(CategorizedTxs::new, |mut txs, tx| {
                        let categorizes = categorize_transaction(&tx, constants);
                        if categorizes.is_spore {
                            txs.spore_txs.push(SporeTx {
                                tx: tx.clone(),
                                timestamp,
                            });
                        }
                        if categorizes.is_xudt {
                            txs.xudt_txs.push(tx.clone());
                        }

                        if categorizes.is_rgbpp {
                            txs.rgbpp_txs.push(tx.clone());
                        }

                        if categorizes.is_inscription {
                            txs.inscription_txs.push(tx.clone());
                        }

                        if let Some(db) = fetcher.db.clone() {
                            let db = db.read();
                            let _ = fetcher::cache_transaction(&*db, tx);
                        }

                        txs
                    })
                    .reduce(CategorizedTxs::new, CategorizedTxs::merge),
            )
        })
        .reduce(CategorizedTxs::new, CategorizedTxs::merge);
    Ok(categorize_txs)
}
