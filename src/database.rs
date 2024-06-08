use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, DbConn, DbErr, EntityTrait as _, TransactionTrait,
};
use tokio::sync::{mpsc, oneshot};

use crate::entity::{
    addresses, block_height, clusters, rgbpp_locks, rgbpp_unlocks, spore_actions, spores,
    token_info, xudt_cell, xudt_status_cell,
};

pub struct DatabaseProcessor {
    pub recv: mpsc::UnboundedReceiver<Operations>,
    pub height: u64,
    pub commited: oneshot::Receiver<()>,
    pub db: DbConn,
}

pub enum Operations {
    UpdateXudt(xudt_status_cell::ActiveModel),
    UpsertTokenInfo(token_info::ActiveModel),
    UpsertXudt(xudt_cell::ActiveModel),
    UpsertAddress(addresses::ActiveModel),
    UpsertCluster(clusters::ActiveModel),
    UpsertSpores(spores::ActiveModel),
    UpsertActions(spore_actions::ActiveModel),
    UpsertLock(rgbpp_locks::ActiveModel),
    UpsertUnlock(rgbpp_unlocks::ActiveModel),
}

macro_rules! define_conflict {
    ($($column:expr),* $(=> [$($update_columns:expr),*])?) => {
        OnConflict::columns([$($column),*])
        .do_nothing()
        $(.update_columns([$($update_columns),*]))?
        .to_owned()
    };
}

const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_MS: u64 = 500;

macro_rules! define_upsert_function {
    ($fn_name:ident, $entity:ident, $filed_count:expr, $conflict:expr $(,$merge:ident)?) => {
        async fn $fn_name(
            buffer: Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            $(let buffer = $merge(buffer);)?
            let futs = buffer
                .into_par_iter()
                .chunks((u16::MAX / $filed_count) as usize)
                .map(|batch| async move {
                    let mut retry_count = 0;
                    loop {
                        match $entity::Entity::insert_many(batch.clone())
                            .on_conflict($conflict)
                            .exec_without_returning(db)
                            .await
                        {
                            Ok(val) => break Ok(val),
                            Err(DbErr::RecordNotInserted) if retry_count < MAX_RETRIES => {
                                // If the insertion fails due to a database connection issue,
                                // we retry the operation up to a maximum number of retries.
                                retry_count += 1;
                                tracing::warn!("Insertion failed, attempting retry {}/{}", retry_count, MAX_RETRIES);
                                tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await; // Sleep for a short duration before retrying.
                            }
                            Err(e) => break Err(e),
                        }
                    }
                })
                .collect::<Vec<_>>();

            let counters = futures::future::try_join_all(futs).await?;
            let counter = counters.into_par_iter().sum::<u64>();

            tracing::info!("upserted {} {}", counter, stringify!($entity));

            Ok(())
        }
    };
}

macro_rules! define_upsert_functions {
    ($($fn_name:ident => ($entity:ident, $batch_size:expr, $conflict:expr $(,$merge:ident)?),)*) => {
        $(
            define_upsert_function!($fn_name, $entity, $batch_size, $conflict $(,$merge)?);
        )*
    };
}

define_upsert_functions! {
    upsert_many_xudt => (
        xudt_cell,
        10,
        define_conflict!(
            xudt_cell::Column::TransactionHash,
            xudt_cell::Column::TransactionIndex
        )
    ),

    upsert_many_info => (
        token_info,
        6,
        define_conflict!(
            token_info::Column::TypeId
        )
    ),

    upsert_many_status => (
        xudt_status_cell,
        4,
        define_conflict!(
            xudt_status_cell::Column::TransactionHash,
            xudt_status_cell::Column::TransactionIndex
        )
    ),

    upsert_many_addresses => (
        addresses,
        4,
        define_conflict!(
            addresses::Column::Id
        )
    ),

    upsert_many_clusters => (
        clusters,
        8,
        define_conflict!(
            clusters::Column::Id => [
                clusters::Column::OwnerAddress,
                clusters::Column::ClusterName,
                clusters::Column::ClusterDescription,
                clusters::Column::MutantId,
                clusters::Column::IsBurned,
                clusters::Column::UpdatedAt
            ]
        ),
        merge_clusters
    ),

    upsert_many_spores => (
        spores,
        8,
        define_conflict!(
            spores::Column::Id => [
                spores::Column::OwnerAddress,
                spores::Column::ContentType,
                spores::Column::Content,
                spores::Column::ClusterId,
                spores::Column::IsBurned,
                spores::Column::UpdatedAt
            ]
        ),
        merge_spores
    ),

    upsert_many_actions => (
        spore_actions,
        15,
        define_conflict!(
            spore_actions::Column::Id
        )
    ),

    upsert_many_locks => (
        rgbpp_locks,
        4,
        define_conflict!(
            rgbpp_locks::Column::LockId
        )
    ),

    upsert_many_unlocks => (
        rgbpp_unlocks,
        7,
        define_conflict!(
            rgbpp_unlocks::Column::UnlockId
        )
    ),
}

macro_rules! process_operations {
    ($commited:expr, $height:expr, $db:expr, $recv:expr, $( $stage:expr => { $( $variant:ident => ($vec:ident, $upsert_fn:ident) ),* } ),*) => {
        {
            use std::time::Instant;
            use futures::StreamExt;

            $(
                $(
                    let mut $vec = Vec::new();
                )*
            )*

            let recv_start = Instant::now();
            let mut sum = 0;
            while let Some(op) = $recv.recv().await {
                sum += 1;
                match op {
                    $(
                        $(
                            Operations::$variant(data) => $vec.push(data),
                        )*
                    )*
                }
            }
            let recv_duration = recv_start.elapsed();
            tracing::debug!("Receiving {sum} Operations took: {:?}", recv_duration);

            $commited.await?;

            let handle_start = Instant::now();

            let txn = $db.begin().await?;

            $(
                let stage_start = Instant::now();

                let (mut scope,_) =  unsafe {
                    async_scoped::TokioScope::scope(|scope| {
                        let txnr = &txn;
                        $(
                            if !$vec.is_empty() {
                                scope.spawn(async move {
                                    $upsert_fn($vec, &txnr).await.map_err(|e| {
                                        tracing::error!("Failed to upsert {}: {:?}", stringify!($variant), e);
                                        e
                                    })
                                });
                            }
                        )*
                    })
                };


                while let Some(result) = scope.next().await {
                    result??;
                }

                drop(scope);

                let stage_duration = stage_start.elapsed();
                tracing::debug!("Stage {} took: {:?}", $stage, stage_duration);
            )*

            // 更新区块高度
            block_height::ActiveModel {
                id: sea_orm::Set(1),
                height: sea_orm::Set($height as i64),
            }
            .update(&txn)
            .await?;

            tracing::debug!("committing {} ...", $height);
            txn.commit().await?;

            let db_duration = Instant::now().duration_since(handle_start);
            tracing::info!("Processed a total of {sum} database operations. Execution time: {:?}", db_duration);
        }
    };
}

impl DatabaseProcessor {
    pub fn new(
        db: DbConn,
        height: u64,
    ) -> (Self, mpsc::UnboundedSender<Operations>, oneshot::Sender<()>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (commit_tx, commit_rx) = oneshot::channel();
        (
            Self {
                recv: rx,
                height,
                db,
                commited: commit_rx,
            },
            tx,
            commit_tx,
        )
    }

    pub async fn handle(self) -> anyhow::Result<()> {
        let Self {
            mut recv,
            db,
            height,
            commited,
        } = self;

        process_operations! {
            commited,
            height,
            db,
            recv,
            0 => {
                UpsertAddress => (address_vec, upsert_many_addresses),
                UpsertTokenInfo => (token_info_vec, upsert_many_info),
                UpsertLock => (lock_vec, upsert_many_locks),
                UpsertUnlock => (unlock_vec, upsert_many_unlocks)
            },
            1 => {
                UpsertXudt => (xudt_vec, upsert_many_xudt),
                UpsertCluster => (cluster_vec, upsert_many_clusters)
            },
            2 => {
                UpsertSpores => (spores_vec, upsert_many_spores),
                UpdateXudt => (status_vec, upsert_many_status)
            },
            3 => {
                UpsertActions => (actions_vec, upsert_many_actions)
            }
        };

        tracing::info!("committed {height}!");

        Ok(())
    }
}

macro_rules! merge_models {
    ($fn_name:ident, $model:ident) => {
        fn $fn_name(items: Vec<$model::ActiveModel>) -> Vec<$model::ActiveModel> {
            use sea_orm::ActiveValue::Set;

            let unique_items = dashmap::DashMap::<Vec<u8>, $model::ActiveModel>::new();

            items.into_par_iter().for_each(|item| {
                if let Set(ref id) = item.id {
                    if !unique_items.contains_key(id)
                        || unique_items
                            .get(id)
                            .unwrap()
                            .updated_at
                            .try_as_ref()
                            .unwrap()
                            < item.updated_at.try_as_ref().unwrap()
                    {
                        unique_items.insert(id.clone(), item);
                    }
                }
            });

            let (_, results) = unique_items.into_par_iter().unzip::<_, _, Vec<_>, Vec<_>>();
            results
        }
    };
}

merge_models!(merge_clusters, clusters);
merge_models!(merge_spores, spores);
