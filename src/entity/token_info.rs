//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.0-rc.5

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "token_info")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub type_id: String,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub transaction_hash: Vec<u8>,
    pub transaction_index: i32,
    pub decimal: i16,
    pub name: String,
    pub symbol: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::addresses::Entity",
        from = "Column::TypeId",
        to = "super::addresses::Column::Id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Addresses,
}

impl Related<super::addresses::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Addresses.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
