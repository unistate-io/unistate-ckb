//! `SeaORM` Entity, @generated by sea-orm-codegen 1.1.1

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, const_field_count::FieldCount)]
#[sea_orm(table_name = "xudt_cell")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "VarBinary(StringLen::None)"
    )]
    pub transaction_hash: Vec<u8>,
    #[sea_orm(primary_key, auto_increment = false)]
    pub transaction_index: i32,
    pub lock_id: String,
    pub type_id: String,
    #[sea_orm(column_type = "Decimal(Some((39, 0)))")]
    pub amount: BigDecimal,
    pub xudt_args: Option<Vec<String>>,
    pub xudt_data: Option<Vec<String>>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
    pub xudt_data_lock: Option<Vec<u8>>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)", nullable)]
    pub xudt_owner_lock_script_hash: Option<Vec<u8>>,
    pub is_consumed: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::addresses::Entity",
        from = "Column::LockId",
        to = "super::addresses::Column::Id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Addresses2,
    #[sea_orm(
        belongs_to = "super::addresses::Entity",
        from = "Column::TypeId",
        to = "super::addresses::Column::Id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Addresses1,
}

impl ActiveModelBehavior for ActiveModel {}
