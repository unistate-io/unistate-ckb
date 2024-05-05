use ckb_jsonrpc_types::TransactionView;
use sea_orm::DatabaseConnection;

pub async fn index_xudt(
    db: &DatabaseConnection,
    tx: &TransactionView,
    timestamp: u64,
    height: u64,
) -> anyhow::Result<()> {
    

    Ok(())
}
