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
    ([$($column:expr),*], update => [$($update_columns:expr),*]) => {
        OnConflict::column([$($column),*])
        .update_columns([$($update_columns),*])
        .to_owned()
    };
    ($column:expr, update => [$($update_columns:expr),*]) => {
        OnConflict::column($column)
        .update_columns([$($update_columns),*])
        .to_owned()
    };
    ([$($column:expr),*]) => {
        OnConflict::columns([$($column),*])
        .do_nothing()
        .to_owned()
    };
    ($column:expr) => {
        OnConflict::column($column)
        .do_nothing()
        .to_owned()
    };
}

macro_rules! define_upsert_function {
    // Define function for inserting many records with a conflict update
    ($fn_name:ident, $entity:ident, $batch_size:expr,[$($column:expr),*], update => [$($update_columns:expr),*]) => {
        async fn $fn_name(
            mut buffer: Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            loop {
                let len = buffer.len();
                if len == 0 {
                    return Ok(());
                }
                match $entity::Entity::insert_many(buffer.drain(..len.min($batch_size)))
                .on_conflict(
                    define_conflict!([$($column),*],update => [$($update_columns),*])
                )
                .exec(db)
                .await
                {
                    Ok(_) | Err(DbErr::RecordNotInserted) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    };
    // Define function for inserting a single record with a conflict update
    ($fn_name:ident, $entity:ident,$batch_size:expr, $column:expr, update => [$($update_columns:expr),*]) => {
        async fn $fn_name(
            mut buffer: Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            loop {
                let len = buffer.len();
                if len == 0 {
                    return Ok(());
                }
                match $entity::Entity::insert_many(buffer.drain(..len.min($batch_size)))
                .on_conflict(
                    define_conflict!($column, update => [$($update_columns),*])
                )
                .exec(db)
                .await
                {
                    Ok(_) | Err(DbErr::RecordNotInserted) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    };
    // Define function for inserting many records without update on conflict
    ($fn_name:ident, $entity:ident, $batch_size:expr,[$($column:expr),*]) => {
        async fn $fn_name(
            mut buffer: Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            loop {
                let len = buffer.len();
                if len == 0 {
                    return Ok(());
                }
                match $entity::Entity::insert_many(buffer.drain(..len.min($batch_size)))
                .on_conflict(
                    define_conflict!([$($column),*])
                )
                .exec(db)
                .await
                {
                    Ok(_) | Err(DbErr::RecordNotInserted) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    };
    // Define function for inserting a single record without update on conflict
    ($fn_name:ident, $entity:ident,$batch_size:expr, $column:expr) => {
        async fn $fn_name(
            mut buffer: Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            loop {
                let len = buffer.len();
                if len == 0 {
                    return Ok(());
                }
                match $entity::Entity::insert_many(buffer.drain(..len.min($batch_size)))
                .on_conflict(
                    define_conflict!($column)
                )
                .exec(db)
                .await
                {
                    Ok(_) | Err(DbErr::RecordNotInserted) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    };
}

define_upsert_function!(
    upsert_many_xudt,
    xudt_cell,
    1000,
    [
        xudt_cell::Column::TransactionHash,
        xudt_cell::Column::TransactionIndex
    ]
);

define_upsert_function!(
    upsert_many_info,
    token_info,
    1000,
    [
        token_info::Column::TransactionHash,
        token_info::Column::TransactionIndex
    ]
);

define_upsert_function!(
    upsert_many_status,
    xudt_status_cell,
    1000,
    [
        xudt_status_cell::Column::TransactionHash,
        xudt_status_cell::Column::TransactionIndex
    ]
);

define_upsert_function!(
    upsert_many_addresses,
    addresses,
    2000,
    addresses::Column::Id
);

define_upsert_function! {
    upsert_many_clusters,
    clusters,
    1000,
    clusters::Column::Id,
    update => [
        clusters::Column::OwnerAddress,
        clusters::Column::ClusterName,
        clusters::Column::ClusterDescription,
        clusters::Column::MutantId,
        clusters::Column::IsBurned,
        clusters::Column::UpdatedAt
    ]
}

define_upsert_function! {
    upsert_many_spores,
    spores,
    1000,
    spores::Column::Id,
    update => [
        spores::Column::OwnerAddress,
        spores::Column::ContentType,
        spores::Column::Content,
        spores::Column::ClusterId,
        spores::Column::IsBurned,
        spores::Column::UpdatedAt
    ]
}

define_upsert_function!(
    upsert_many_actions,
    spore_actions,
    1000,
    spore_actions::Column::Id
);

define_upsert_function!(
    upsert_many_locks,
    rgbpp_locks,
    1000,
    rgbpp_locks::Column::LockId
);

define_upsert_function!(
    upsert_many_unlocks,
    rgbpp_unlocks,
    1000,
    rgbpp_unlocks::Column::UnlockId
);

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
