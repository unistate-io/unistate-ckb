//! `SeaORM` Entity, @generated by sea-orm-codegen 1.1.1

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, const_field_count::FieldCount)]
#[sea_orm(table_name = "rgbpp_unlocks")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "VarBinary(StringLen::None)"
    )]
    pub unlock_id: Vec<u8>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub tx: Vec<u8>,
    pub version: i16,
    pub input_len: i16,
    pub output_len: i16,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub btc_tx: Vec<u8>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub btc_tx_proof: Vec<u8>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
