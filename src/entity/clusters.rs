//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.15

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "clusters")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "Binary(BlobSize::Blob(None))"
    )]
    pub id: Vec<u8>,
    pub cluster_name: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub cluster_description: Option<String>,
    #[sea_orm(column_type = "Binary(BlobSize::Blob(None))", nullable)]
    pub mutant_id: Option<Vec<u8>>,
    pub owner_address: Option<String>,
    pub is_burned: bool,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
