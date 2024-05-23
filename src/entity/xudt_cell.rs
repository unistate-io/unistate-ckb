//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.15

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "xudt_cell")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "Binary(BlobSize::Blob(None))"
    )]
    pub transaction_hash: Vec<u8>,
    #[sea_orm(primary_key, auto_increment = false)]
    pub transaction_index: i32,
    pub lock_id: String,
    pub type_id: String,
    #[sea_orm(column_type = "Decimal(Some((39, 0)))")]
    pub amount: Decimal,
    pub xudt_args: Option<Vec<String>>,
    pub xudt_data: Option<Vec<String>>,
    #[sea_orm(column_type = "Binary(BlobSize::Blob(None))", nullable)]
    pub xudt_data_lock: Option<Vec<u8>>,
    #[sea_orm(column_type = "Binary(BlobSize::Blob(None))", nullable)]
    pub xudt_owner_lock_script_hash: Option<Vec<u8>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
