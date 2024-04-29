use anyhow::anyhow;
use ckb_jsonrpc_types::CellOutput;
use molecule::{
    bytes::Buf as _,
    prelude::{Entity, Reader as _},
};
use sea_orm::{
    prelude::ActiveModelTrait as _, sea_query, ActiveValue::NotSet, DbConn, EntityTrait, Set,
};

use crate::{
    entity,
    schemas::{
        action::{self, BurnSpore, MintCluster, MintSpore, TransferCluster, TransferSpore},
        rgbpp::{RGBPPLockReader, RGBPPUnlockReader},
        spore_v1::SporeDataReader,
        spore_v2::ClusterDataV2Reader,
    },
};

pub async fn upsert_rgbpp_lock(
    db: &DbConn,
    output: &CellOutput,
    rgbpp_lock: RGBPPLockReader<'_>,
) -> anyhow::Result<entity::rgbpp_locks::Model> {
    println!("upsert_rgbpp_lock");
    let lock_id = output.lock.args.as_bytes();
    let rgbpp_lock = entity::rgbpp_locks::ActiveModel {
        lock_id: Set(lock_id.to_vec()),
        btc_txid: Set(rgbpp_lock.btc_txid().raw_data().to_vec()),
        out_index: Set(rgbpp_lock.out_index().raw_data().get_u32() as i32),
    };

    let res = entity::prelude::RgbppLocks::insert(rgbpp_lock)
        .on_conflict(
            sea_query::OnConflict::column(entity::rgbpp_locks::Column::LockId)
                .do_nothing()
                .to_owned(),
        )
        .do_nothing()
        .exec(db)
        .await?;

    match res {
        sea_orm::TryInsertResult::Empty => Err(anyhow!("Try instert rgbpp lock empty.")),
        sea_orm::TryInsertResult::Conflicted => {
            let model = entity::prelude::RgbppLocks::find_by_id(lock_id)
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
        sea_orm::TryInsertResult::Inserted(res) => {
            let model = entity::prelude::RgbppLocks::find_by_id(res.last_insert_id)
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
    }
}

pub async fn upsert_rgbpp_unlock(
    db: &DbConn,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: RGBPPUnlockReader<'_>,
) -> anyhow::Result<entity::rgbpp_unlocks::Model> {
    println!("upsert_rgbpp_unlock");
    let extra_data = rgbpp_unlock.extra_data().to_entity();

    let rgbpp_unlock = entity::rgbpp_unlocks::ActiveModel {
        unlock_id: Set(rgbpp_lock.lock_id.clone()),
        version: Set(rgbpp_unlock.version().raw_data().get_u16() as i16),
        input_len: Set(extra_data.input_len().as_bytes().get_i16()),
        output_len: Set(extra_data.output_len().as_bytes().get_i16()),
        btc_tx: Set(rgbpp_unlock.btc_tx().raw_data().to_vec()),
        btc_tx_proof: Set(rgbpp_unlock.btc_tx_proof().raw_data().to_vec()),
    };

    let res = entity::prelude::RgbppUnlocks::insert(rgbpp_unlock)
        .on_conflict(
            sea_query::OnConflict::column(entity::rgbpp_unlocks::Column::UnlockId)
                .do_nothing()
                .to_owned(),
        )
        .do_nothing()
        .exec(db)
        .await?;

    match res {
        sea_orm::TryInsertResult::Empty => Err(anyhow!("Try instert rgbpp lock empty.")),
        sea_orm::TryInsertResult::Conflicted => {
            let model = entity::prelude::RgbppUnlocks::find_by_id(rgbpp_lock.lock_id.clone())
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
        sea_orm::TryInsertResult::Inserted(res) => {
            let model = entity::prelude::RgbppUnlocks::find_by_id(res.last_insert_id)
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
    }
}

pub async fn upsert_address(
    db: &DbConn,
    address: action::AddressUnion,
) -> anyhow::Result<entity::addresses::Model> {
    println!("upsert_address");

    let action::AddressUnion::Script(address) = address;
    let address_id = address.as_bytes();
    let address = entity::addresses::ActiveModel {
        address_id: Set(address_id.to_vec()),
        script_code_hash: Set(address.code_hash().raw_data().to_vec()),
        script_hash_type: Set(Into::<u8>::into(address.hash_type()) as i16),
        script_args: Set(Some(address.args().raw_data().to_vec())),
    };

    let res = entity::prelude::Addresses::insert(address)
        .on_conflict(
            sea_query::OnConflict::column(entity::addresses::Column::AddressId)
                .do_nothing()
                .to_owned(),
        )
        .do_nothing()
        .exec(db)
        .await?;

    match res {
        sea_orm::TryInsertResult::Empty => Err(anyhow!("Try instert rgbpp lock empty.")),
        sea_orm::TryInsertResult::Conflicted => {
            let model = entity::prelude::Addresses::find_by_id(address_id.to_vec())
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
        sea_orm::TryInsertResult::Inserted(res) => {
            let model = entity::prelude::Addresses::find_by_id(res.last_insert_id)
                .one(db)
                .await?
                .unwrap();
            Ok(model)
        }
    }
}

pub async fn burn_spore(
    db: &DbConn,
    timestamp: u64,
    burn: BurnSpore,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: Option<&entity::rgbpp_unlocks::Model>,
) -> anyhow::Result<entity::spore_actions::Model> {
    println!("burn_spore");

    let owner = upsert_address(db, burn.from().to_enum()).await?;

    let timestamp = chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .ok_or(anyhow!("invalid timestamp: {}", timestamp))?
        .naive_utc();

    let spore = entity::spores::ActiveModel {
        spore_id: Set(burn.spore_id().raw_data().to_vec()),
        is_burned: Set(true),
        updated_at: Set(timestamp),
        ..Default::default()
    };

    let spore = entity::prelude::Spores::update(spore).exec(db).await?;

    let action = entity::spore_actions::ActiveModel {
        action_id: NotSet,
        // 0代表mint
        action_type: Set(2),
        spore_id: Set(spore.spore_id.clone()),
        from_address_id: Set(Some(owner.address_id.clone())),
        to_address_id: Set(None),
        created_at: Set(timestamp),
        rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id.clone())),
        rgbpp_unlock_id: Set(rgbpp_unlock.map(|unlock| unlock.unlock_id.clone())),
    }
    .insert(db)
    .await?;

    Ok(action)
}

pub async fn mint_spore(
    db: &DbConn,
    timestamp: u64,
    spore_data: SporeDataReader<'_>,
    mint: MintSpore,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: Option<&entity::rgbpp_unlocks::Model>,
) -> anyhow::Result<entity::spore_actions::Model> {
    println!("mint_spore");

    let owner = upsert_address(db, mint.to().to_enum()).await?;

    let timestamp = chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .ok_or(anyhow!("invalid timestamp: {}", timestamp))?
        .naive_utc();

    let spore_id = mint.spore_id().raw_data();

    let spore = entity::spores::ActiveModel {
        spore_id: Set(spore_id.to_vec()),
        content_type: Set(spore_data.content_type().raw_data().to_vec()),
        content: Set(spore_data.content().raw_data().to_vec()),
        cluster_id: Set(spore_data
            .cluster_id()
            .to_opt()
            .map(|b| b.raw_data().to_vec())),
        owner_address_id: Set(owner.address_id.clone()),
        data_hash: Set(mint.data_hash().raw_data().to_vec()),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
        is_burned: Set(false),
    };

    let spore_res = entity::prelude::Spores::insert(spore)
        .on_conflict(
            sea_query::OnConflict::column(entity::spores::Column::SporeId)
                .do_nothing()
                .to_owned(),
        )
        .do_nothing()
        .exec(db)
        .await?;

    let spore = match spore_res {
        sea_orm::TryInsertResult::Conflicted => {
            let model = entity::prelude::Spores::find_by_id(spore_id.to_vec())
                .one(db)
                .await?
                .unwrap();
            model
        }
        sea_orm::TryInsertResult::Inserted(res) => {
            let model = entity::prelude::Spores::find_by_id(res.last_insert_id)
                .one(db)
                .await?
                .unwrap();
            model
        }
        sea_orm::TryInsertResult::Empty => unreachable!(),
    };

    let action = entity::spore_actions::ActiveModel {
        action_id: NotSet,
        // 0代表mint
        action_type: Set(0),
        spore_id: Set(spore.spore_id.clone()),
        from_address_id: Set(Some(owner.address_id.clone())),
        to_address_id: Set(None),
        created_at: Set(timestamp),
        rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id.clone())),
        rgbpp_unlock_id: Set(rgbpp_unlock.map(|unlock| unlock.unlock_id.clone())),
    }
    .insert(db)
    .await?;

    Ok(action)
}

pub async fn transfer_spore(
    db: &DbConn,
    timestamp: u64,
    transfer: TransferSpore,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: Option<&entity::rgbpp_unlocks::Model>,
) -> anyhow::Result<entity::spore_actions::Model> {
    println!("transfer_spore");

    let from = upsert_address(db, transfer.from().to_enum()).await?;
    let to = upsert_address(db, transfer.to().to_enum()).await?;

    let timestamp = chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .ok_or(anyhow!("invalid timestamp: {}", timestamp))?
        .naive_utc();

    let spore = entity::spores::ActiveModel {
        spore_id: Set(transfer.spore_id().raw_data().to_vec()),
        owner_address_id: Set(to.address_id.clone()),
        updated_at: Set(timestamp),
        ..Default::default()
    };

    let spore = entity::prelude::Spores::update(spore).exec(db).await?;

    let action = entity::spore_actions::ActiveModel {
        action_id: NotSet,
        // 0代表mint
        action_type: Set(1),
        spore_id: Set(spore.spore_id.clone()),
        from_address_id: Set(Some(from.address_id.clone())),
        to_address_id: Set(Some(to.address_id.clone())),
        created_at: Set(timestamp),
        rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id.clone())),
        rgbpp_unlock_id: Set(rgbpp_unlock.map(|unlock| unlock.unlock_id.clone())),
    }
    .insert(db)
    .await?;

    Ok(action)
}

pub async fn mint_cluster(
    db: &DbConn,
    timestamp: u64,
    cluster_data: ClusterDataV2Reader<'_>,
    mint: MintCluster,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: Option<&entity::rgbpp_unlocks::Model>,
) -> anyhow::Result<entity::cluster_actions::Model> {
    println!("mint_cluster");

    let owner = upsert_address(db, mint.to().to_enum()).await?;

    let timestamp = chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .ok_or(anyhow!("invalid timestamp: {}", timestamp))?
        .naive_utc();
    let cluster_id = mint.cluster_id().raw_data();
    let cluster = entity::clusters::ActiveModel {
        cluster_id: Set(cluster_id.to_vec()),
        owner_address_id: Set(owner.address_id.clone()),
        data_hash: Set(mint.data_hash().raw_data().to_vec()),
        name: Set(cluster_data.name().raw_data().to_vec()),
        description: Set(cluster_data.description().raw_data().to_vec()),
        mutant_id: Set(cluster_data
            .mutant_id()
            .to_opt()
            .map(|m| m.raw_data().to_vec())),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    };

    let cluster_res = entity::prelude::Clusters::insert(cluster)
        .on_conflict(
            sea_query::OnConflict::column(entity::clusters::Column::ClusterId)
                .do_nothing()
                .to_owned(),
        )
        .do_nothing()
        .exec(db)
        .await?;

    let cluster = match cluster_res {
        sea_orm::TryInsertResult::Conflicted => {
            let model = entity::prelude::Clusters::find_by_id(cluster_id.to_vec())
                .one(db)
                .await?
                .unwrap();
            model
        }
        sea_orm::TryInsertResult::Inserted(res) => {
            let model = entity::prelude::Clusters::find_by_id(res.last_insert_id)
                .one(db)
                .await?
                .unwrap();
            model
        }
        sea_orm::TryInsertResult::Empty => unreachable!(),
    };

    let action = entity::cluster_actions::ActiveModel {
        action_id: NotSet,
        // 0代表mint
        action_type: Set(0),
        cluster_id: Set(cluster.cluster_id.clone()),
        from_address_id: Set(None),
        to_address_id: Set(Some(owner.address_id.clone())),
        created_at: Set(timestamp),
        rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id.clone())),
        rgbpp_unlock_id: Set(rgbpp_unlock.map(|unlock| unlock.unlock_id.clone())),
    }
    .insert(db)
    .await?;

    Ok(action)
}
pub async fn transfer_cluster(
    db: &DbConn,
    timestamp: u64,
    transfer: TransferCluster,
    rgbpp_lock: &entity::rgbpp_locks::Model,
    rgbpp_unlock: Option<&entity::rgbpp_unlocks::Model>,
) -> anyhow::Result<entity::cluster_actions::Model> {
    println!("transfer_cluster");

    let from = upsert_address(db, transfer.from().to_enum()).await?;
    let to = upsert_address(db, transfer.to().to_enum()).await?;

    let timestamp = chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .ok_or(anyhow!("invalid timestamp: {}", timestamp))?
        .naive_utc();

    let cluster = entity::clusters::ActiveModel {
        cluster_id: Set(transfer.cluster_id().raw_data().to_vec()),
        owner_address_id: Set(to.address_id.clone()),
        updated_at: Set(timestamp),
        ..Default::default()
    };

    let cluster = entity::prelude::Clusters::update(cluster).exec(db).await?;

    let action = entity::cluster_actions::ActiveModel {
        action_id: NotSet,
        // 0代表mint
        action_type: Set(1),
        cluster_id: Set(cluster.cluster_id.clone()),
        from_address_id: Set(Some(from.address_id.clone())),
        to_address_id: Set(Some(to.address_id.clone())),
        created_at: Set(timestamp),
        rgbpp_lock_id: Set(Some(rgbpp_lock.lock_id.clone())),
        rgbpp_unlock_id: Set(rgbpp_unlock.map(|unlock| unlock.unlock_id.clone())),
    }
    .insert(db)
    .await?;

    Ok(action)
}
