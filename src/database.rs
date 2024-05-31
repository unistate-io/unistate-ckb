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
    pub height: oneshot::Receiver<block_height::ActiveModel>,
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

macro_rules! define_upsert_function {
    // Define function for inserting many records with a conflict update
    ($fn_name:ident, $entity:ident, [$($column:expr),*], update => [$($update_columns:expr),*]) => {
        async fn $fn_name(
            buffer: &mut Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            match $entity::Entity::insert_many(buffer.drain(..))
                .on_conflict(
                    OnConflict::columns([$($column),*])
                    .update_columns([$($update_columns),*])
                    .to_owned(),
                )
                .exec(db)
                .await
            {
                Ok(_) | Err(DbErr::RecordNotInserted) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    };
    // Define function for inserting a single record with a conflict update
    ($fn_name:ident, $entity:ident, $column:expr, update => [$($update_columns:expr),*]) => {
        async fn $fn_name(
            buffer: &mut Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            match $entity::Entity::insert_many(buffer.drain(..))
                .on_conflict(
                    OnConflict::column($column)
                    .update_columns([$($update_columns),*])
                    .to_owned(),
                )
                .exec(db)
                .await
            {
                Ok(_) | Err(DbErr::RecordNotInserted) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    };
    // Define function for inserting many records without update on conflict
    ($fn_name:ident, $entity:ident, [$($column:expr),*]) => {
        async fn $fn_name(
            buffer: &mut Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            match $entity::Entity::insert_many(buffer.drain(..))
                .on_conflict(
                    OnConflict::columns([$($column),*])
                    .do_nothing()
                    .to_owned(),
                )
                .exec(db)
                .await
            {
                Ok(_) | Err(DbErr::RecordNotInserted) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    };
    // Define function for inserting a single record without update on conflict
    ($fn_name:ident, $entity:ident, $column:expr) => {
        async fn $fn_name(
            buffer: &mut Vec<$entity::ActiveModel>,
            db: &sea_orm::DatabaseTransaction,
        ) -> Result<(), anyhow::Error> {
            match $entity::Entity::insert_many(buffer.drain(..))
                .on_conflict(
                    OnConflict::column($column)
                    .do_nothing()
                    .to_owned(),
                )
                .exec(db)
                .await
            {
                Ok(_) | Err(DbErr::RecordNotInserted) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    };

}

define_upsert_function!(
    upsert_many_xudt,
    xudt_cell,
    [
        xudt_cell::Column::TransactionHash,
        xudt_cell::Column::TransactionIndex
    ]
);

define_upsert_function!(
    upsert_many_info,
    token_info,
    [
        token_info::Column::TransactionHash,
        token_info::Column::TransactionIndex
    ]
);

define_upsert_function!(
    upsert_many_status,
    xudt_status_cell,
    [
        xudt_status_cell::Column::TransactionHash,
        xudt_status_cell::Column::TransactionIndex
    ]
);

define_upsert_function!(upsert_many_addresses, addresses, addresses::Column::Id);

define_upsert_function! {
    upsert_many_clusters,
    clusters,
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
    spore_actions::Column::Id
);

define_upsert_function!(upsert_many_locks, rgbpp_locks, rgbpp_locks::Column::LockId);

define_upsert_function!(
    upsert_many_unlocks,
    rgbpp_unlocks,
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

            $(
                if let Err(e) = $upsert_fn(&mut $vec, $db).await {
                    tracing::error!("Failed to upsert {}: {:?}", stringify!($variant), e);
                }
            )*
        }
    };
}

impl DatabaseProcessor {
    pub fn new(
        db: DbConn,
    ) -> (
        Self,
        mpsc::UnboundedSender<Operations>,
        oneshot::Sender<block_height::ActiveModel>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (height_tx, height_rx) = oneshot::channel();
        (
            Self {
                recv: rx,
                height: height_rx,
                db,
            },
            tx,
            height_tx,
        )
    }

    pub async fn handle(self) -> anyhow::Result<()> {
        let Self {
            mut recv,
            db,
            height,
        } = self;
        let txn = db.begin().await?;

        process_operations! {
            recv,
            &txn,
            (UpsertXudt, xudt_vec, upsert_many_xudt),
            (UpsertTokenInfo, token_info_vec, upsert_many_info),
            (UpdateXudt, status_vec, upsert_many_status),
            (UpsertAddress, address_vec, upsert_many_addresses),
            (UpsertCluster, cluster_vec, upsert_many_clusters),
            (UpsertSpores, spores_vec, upsert_many_spores),
            (UpsertActions, actions_vec, upsert_many_actions),
            (UpsertLock, lock_vec, upsert_many_locks),
            (UpsertUnlock, unlock_vec, upsert_many_unlocks)
        };

        let block_height = height.await?;

        block_height.insert(&txn).await?;

        txn.commit().await?;

        Ok(())
    }
}
