use ckb_jsonrpc_types::TransactionView;

#[derive(Debug, Clone)]
pub struct TxContext {
    pub tx: TransactionView,
    pub block_number: u64,
    pub timestamp: u64,
}

impl TxContext {
    pub fn new(tx: TransactionView, block_number: u64, timestamp: u64) -> Self {
        Self {
            tx,
            block_number,
            timestamp,
        }
    }
}
