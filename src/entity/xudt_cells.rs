//! `SeaORM` Entity, @generated by sea-orm-codegen 1.1.10

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, const_field_count::FieldCount)]
#[sea_orm(table_name = "xudt_cells")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "VarBinary(StringLen::None)"
    )]
    pub tx_hash: Vec<u8>,
    #[sea_orm(primary_key, auto_increment = false)]
    pub output_index: i32,
    pub lock_address_id: String,
    pub type_address_id: String,
    #[sea_orm(column_type = "Decimal(Some((39, 0)))")]
    pub amount: BigDecimal,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub xudt_extension_args: Option<Json>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub xudt_extension_data: Option<Json>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
    pub xudt_data_lock_hash: Option<Vec<u8>>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
    pub owner_lock_hash: Option<Vec<u8>>,
    pub block_number: i64,
    pub tx_timestamp: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
