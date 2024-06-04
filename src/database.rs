use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, DbConn, DbErr, EntityTrait as _, TransactionTrait,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::entity::{
    addresses, block_height, clusters, rgbpp_locks, rgbpp_unlocks, spore_actions, spores,
    token_info, xudt_cell, xudt_status_cell,
};

pub struct DatabaseProcessor {
    pub recv: mpsc::UnboundedReceiver<Operations>,
    pub height: u64,
    pub commited: oneshot::Receiver<Option<JoinHandle<anyhow::Result<()>>>>,
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
    ($recv:expr, $db:expr,$(($variant:ident, $vec:ident, $upsert_fn:ident)),*) => {
        {
            $(
                let mut $vec = Vec::new();
            )*

            while let Some(op) = $recv.recv().await {
                match op {
                    $(
                        Operations::$variant(data) => $vec.push(data),
                    )*
                }
            }

            tracing::debug!("all recv...");

            $(
                if let Err(e) = $upsert_fn($vec, $db).await {
                    tracing::error!("Failed to upsert {}: {:?}", stringify!($variant), e);
                    return Err(e.into());
                }
            )*
        }
    };
}

impl DatabaseProcessor {
    pub fn new(
        db: DbConn,
        height: u64,
    ) -> (
        Self,
        mpsc::UnboundedSender<Operations>,
        oneshot::Sender<Option<JoinHandle<anyhow::Result<()>>>>,
    ) {
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
        let txn = db.begin().await?;

        tracing::debug!("begin handle");

        process_operations! {
            recv,
            &txn,
            (UpsertAddress, address_vec, upsert_many_addresses),
            (UpsertTokenInfo, token_info_vec, upsert_many_info),
            (UpsertXudt, xudt_vec, upsert_many_xudt),
            (UpdateXudt, status_vec, upsert_many_status),
            (UpsertCluster, cluster_vec, upsert_many_clusters),
            (UpsertSpores, spores_vec, upsert_many_spores),
            (UpsertActions, actions_vec, upsert_many_actions),
            (UpsertLock, lock_vec, upsert_many_locks),
            (UpsertUnlock, unlock_vec, upsert_many_unlocks)
        };

        if let Some(pre) = commited.await? {
            pre.await??;
        };

        block_height::ActiveModel {
            id: sea_orm::Set(1),
            height: sea_orm::Set(height as i64),
        }
        .update(&txn)
        .await?;

        tracing::debug!("commiting {height} ...");

        txn.commit().await?;

        tracing::info!("commited {height}!");

        Ok(())
    }
}
