#[cfg(feature = "cache")]
mod wrapper;

use std::collections::{HashMap, HashSet};

use ckb_jsonrpc_types::{
    BlockNumber, BlockView, CellInput, CellOutput, JsonBytes, OutPoint, Transaction,
    TransactionWithStatusResponse,
};
use ckb_sdk::rpc::ResponseFormatGetter;
use ckb_types::H256;
use jsonrpsee::{
    core::{
        client::{BatchResponse, ClientT},
        params::BatchRequestBuilder,
    },
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
#[cfg(feature = "cache")]
use redb::{ReadableTable as _, TableDefinition};

use thiserror::Error;
use tracing::debug;

#[cfg(feature = "cache")]
use wrapper::Bincode;

#[cfg(feature = "cache")]
pub use redb::{Database, Error as RedbError};

#[cfg(feature = "cache")]
const TX_TABLE: TableDefinition<Bincode<H256>, Bincode<Transaction>> =
    TableDefinition::new("transactions");

#[cfg(feature = "cache")]
const TX_COUNT_TABLE: TableDefinition<Bincode<H256>, u64> =
    TableDefinition::new("transaction_counts");

/// Custom error type for the Fetcher module.
///
/// This enum represents various error conditions that can occur during
/// interactions with the blockchain via JSON-RPC calls.
#[derive(Error, Debug)]
pub enum Error {
    /// Represents a failure to fetch data from the blockchain.
    ///
    /// Contains the error code and message returned by the blockchain node.
    #[error("Failed to fetch data: code={code}, message={message}")]
    FailedFetch { code: i32, message: String },

    /// Represents a situation where a requested previous output is not found.
    ///
    /// Contains the transaction hash, index, and the length of outputs of the missing output.
    #[error(
        "Previous output not found: tx_hash={tx_hash:#x}, index={index}, outputs_len={outputs_len}"
    )]
    PreviousOutputNotFound {
        tx_hash: H256,
        index: u32,
        outputs_len: usize,
    },

    /// Represents a situation where data for a requested previous output is not found.
    ///
    /// Contains the transaction hash, index, and the length of outputs of the missing output data.
    #[error("Previous output data not found: tx_hash={tx_hash:#x}, index={index}, outputs_data_len={outputs_data_len}")]
    PreviousOutputDataNotFound {
        tx_hash: H256,
        index: u32,
        outputs_data_len: usize,
    },

    /// Represents an error encountered with the JSON-RPC client.
    ///
    /// Wraps the underlying `jsonrpsee::core::client::Error`.
    #[error("Encountered an issue with the JSON RPC client. Details: {0}")]
    JsonRpcClient(#[from] jsonrpsee::core::client::Error),

    /// Represents a serialization/deserialization error.
    ///
    /// Wraps the underlying `serde_json::Error`.
    #[error("There was a problem deserializing data. Details: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[cfg(feature = "cache")]
    /// Represents an error related to database operations.
    ///
    /// Wraps the underlying `redb::Error`.
    #[error("Database error: {0}")]
    Database(#[from] redb::Error),
}

#[derive(Debug, Clone)]
pub struct Fetcher<C> {
    client: C,
    retry_interval: u64,
    max_retries: usize,
}

macro_rules! retry {
    ($call:expr, $max_retries:expr, $retry_interval:expr) => {{
        let mut retries = 0;
        let max_retries = $max_retries;
        let retry_interval = std::time::Duration::from_millis($retry_interval);

        loop {
            match $call {
                Ok(res) => break Ok(res),
                Err(err) => {
                    retries += 1;
                    if retries > max_retries {
                        break Err(err);
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }};
}

#[cfg(feature = "cache")]
mod cache {
    use ckb_jsonrpc_types::TransactionView;

    use super::*;

    pub fn init_redb(db: &redb::Database) -> Result<(), redb::Error> {
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(TX_TABLE)?;
            let _ = write_txn.open_table(TX_COUNT_TABLE)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn clear_transactions_below_count(
        db: &redb::Database,
        threshold: u64,
    ) -> Result<(), redb::Error> {
        let write_txn = db.begin_write()?;
        {
            let mut tx_table = write_txn.open_table(TX_TABLE)?;
            let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

            let to_remove: Vec<H256> = count_table
                .iter()?
                .filter_map(|result| {
                    result.ok().and_then(|(hash, count)| {
                        if count.value() < threshold {
                            Some(hash.value())
                        } else {
                            None
                        }
                    })
                })
                .collect();

            for hash in to_remove {
                tx_table.remove(&hash)?;
                count_table.remove(&hash)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_cached_transactions(
        db: &redb::Database,
        hashes: &[H256],
    ) -> Result<HashMap<H256, Transaction>, Error> {
        #[inline]
        fn retrieve_cached_transactions(
            db: &redb::Database,
            hashes: &[H256],
        ) -> Result<HashMap<H256, Transaction>, redb::Error> {
            let write_txn = db.begin_write()?;
            let mut cached_txs = HashMap::new();

            {
                let tx_table = write_txn.open_table(TX_TABLE)?;
                let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

                for hash in hashes {
                    if let Some(tx) = tx_table.get(hash)? {
                        // Increment count
                        let current_count = count_table
                            .get(hash)?
                            .map(|ag| ag.value())
                            .unwrap_or_default();
                        count_table.insert(hash, &(current_count + 1))?;
                        cached_txs.insert(hash.clone(), tx.value());
                    }
                }
            }

            write_txn.commit()?;

            Ok(cached_txs)
        }

        let res = retrieve_cached_transactions(db, hashes)?;

        Ok(res)
    }

    pub fn cache_transactions(
        db: &redb::Database,
        txs: Vec<TransactionWithStatusResponse>,
    ) -> Result<HashMap<H256, Transaction>, Error> {
        #[inline]
        fn cache_transactions(
            db: &redb::Database,
            txs: Vec<TransactionWithStatusResponse>,
        ) -> Result<HashMap<H256, Transaction>, redb::Error> {
            let write_txn = db.begin_write()?;
            let mut fetched_txs = HashMap::new();

            {
                let mut tx_table = write_txn.open_table(TX_TABLE)?;
                let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;

                for tx_response in txs {
                    if let Some(tx_view) = tx_response.transaction {
                        if let Ok(tx) = tx_view.get_value() {
                            tx_table.insert(&tx.hash, &tx.inner)?;
                            count_table.insert(&tx.hash, &0u64)?; // Initialize count to 0
                            fetched_txs.insert(tx.hash, tx.inner);
                        }
                    }
                }
            }

            write_txn.commit()?;

            Ok(fetched_txs)
        }

        let res = cache_transactions(db, txs)?;

        Ok(res)
    }

    pub fn cache_transaction(db: &redb::Database, tx: TransactionView) -> Result<(), Error> {
        #[inline]
        fn cache_transaction(db: &redb::Database, tx: TransactionView) -> Result<(), redb::Error> {
            let write_txn = db.begin_write()?;

            {
                let mut tx_table = write_txn.open_table(TX_TABLE)?;
                let mut count_table = write_txn.open_table(TX_COUNT_TABLE)?;
                tx_table.insert(&tx.hash, &tx.inner)?;
                count_table.insert(&tx.hash, &0u64)?; // Initialize count to 0
            }

            write_txn.commit()?;

            Ok(())
        }

        cache_transaction(db, tx)?;

        Ok(())
    }

    pub fn get_max_count_transaction(
        db: &redb::Database,
    ) -> Result<Option<(H256, u64)>, redb::Error> {
        let read_txn = db.begin_read()?;
        let count_table = read_txn.open_table(TX_COUNT_TABLE)?;

        let max_entry = count_table
            .iter()?
            .filter_map(|result| result.ok())
            .max_by_key(|(_, count)| count.value());

        Ok(max_entry.map(|(hash, count)| (hash.value(), count.value())))
    }
}

#[cfg(feature = "cache")]
pub use cache::*;

impl<C> Fetcher<C>
where
    C: ClientT,
{
    pub fn new(client: C, retry_interval: u64, max_retries: usize) -> Self {
        Self {
            client,
            retry_interval,
            max_retries,
        }
    }

    pub async fn get_txs_by_hashes(
        &self,
        #[cfg(feature = "cache")] db: &redb::Database,
        hashes: Vec<H256>,
    ) -> Result<HashMap<H256, Transaction>, Error> {
        debug!("Getting transactions by hashes: {:?}", hashes);

        #[cfg(feature = "cache")]
        {
            // Try to get cached transactions first
            let mut result = get_cached_transactions(db, &hashes)?;

            // Collect missing hashes
            let missing_hashes: Vec<_> = hashes
                .into_par_iter()
                .filter(|hash| !result.contains_key(hash))
                .collect();

            // Fetch missing transactions
            if !missing_hashes.is_empty() {
                debug!("Fetching missing transactions: {:?}", missing_hashes);
                let fetched_txs = self.get_txs(missing_hashes).await?;
                let cached_txs = cache_transactions(db, fetched_txs)?;
                result.extend(cached_txs);
            }

            debug!("Got transactions: {:?}", result);
            Ok(result)
        }

        #[cfg(not(feature = "cache"))]
        {
            let txs = self.get_txs(hashes).await?;

            let mut result = HashMap::new();
            for tx_response in txs {
                if let Some(tx_view) = tx_response.transaction {
                    if let Ok(tx) = tx_view.get_value() {
                        result.insert(tx.hash, tx.inner);
                    }
                }
            }

            debug!("Got transactions: {:?}", result);
            Ok(result)
        }
    }

    #[inline]
    async fn call<Params, R>(&self, method: &str, params: Params) -> Result<R, Error>
    where
        Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone,
        R: jsonrpsee::core::DeserializeOwned,
    {
        let res = retry!(
            self.client
                .request::<R, Params>(method, params.clone())
                .await,
            self.max_retries,
            self.retry_interval
        )?;

        Ok(res)
    }

    #[inline]
    async fn batch_request<P, R, HI>(
        &self,
        batchs: Vec<P>,
        handle_item: HI,
    ) -> Result<Vec<R>, Error>
    where
        R: jsonrpsee::core::DeserializeOwned + std::fmt::Debug,
        HI: Fn(&P, &mut BatchRequestBuilder<'_>) -> Result<(), Error>,
    {
        if batchs.is_empty() {
            return Ok(Vec::new());
        }

        let mut batch_request = BatchRequestBuilder::new();

        for item in batchs.iter() {
            handle_item(item, &mut batch_request)?;
        }

        let results: BatchResponse<'_, R> = retry!(
            self.client.batch_request(batch_request.clone()).await,
            self.max_retries,
            self.retry_interval
        )?;

        let results = results
            .into_iter()
            .map(|res| {
                res.map_err(|err| Error::FailedFetch {
                    code: err.code(),
                    message: err.message().into(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(results)
    }

    pub async fn get_outputs_ignore_not_found(
        &self,
        #[cfg(feature = "cache")] db: &redb::Database,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<CellOutput>, Error> {
        debug!("Getting outputs for inputs: {:?}", inputs);
        let hashs = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect::<Vec<_>>();

        debug!("Getting transactions by hashes: {:?}", hashs);
        let txs = self
            .get_txs_by_hashes(
                #[cfg(feature = "cache")]
                db,
                hashs,
            )
            .await?;

        let res = inputs
            .into_par_iter()
            .filter_map(|input| {
                let idx = input.previous_output.index.value();
                let tx = txs.get(&input.previous_output.tx_hash);
                let output = tx.and_then(|tx| tx.outputs.get(idx as usize).cloned());

                debug!("Got output for input: {:?} -> {:?}", input, output);
                output
            })
            .collect::<Vec<_>>();

        debug!("Got outputs: {:?}", res);
        Ok(res)
    }

    pub async fn get_outputs(
        &self,
        #[cfg(feature = "cache")] db: &redb::Database,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<CellOutput>, Error> {
        debug!("Getting outputs for inputs: {:?}", inputs);
        let hashs = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect::<Vec<_>>();

        debug!("Getting transactions by hashes: {:?}", hashs);
        let txs = self
            .get_txs_by_hashes(
                #[cfg(feature = "cache")]
                db,
                hashs,
            )
            .await?;

        let res = inputs
            .into_par_iter()
            .map(|input| {
                let idx = input.previous_output.index.value();
                let tx = txs.get(&input.previous_output.tx_hash);
                let output = tx.and_then(|tx| tx.outputs.get(idx as usize).cloned());

                debug!("Got output for input: {:?} -> {:?}", input, output);
                output.ok_or(Error::PreviousOutputNotFound {
                    tx_hash: input.previous_output.tx_hash.clone(),
                    index: input.previous_output.index.value(),
                    outputs_len: tx.map_or(0, |tx| tx.outputs.len()),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        debug!("Got outputs: {:?}", res);
        Ok(res)
    }

    pub async fn get_outputs_with_data(
        &self,
        #[cfg(feature = "cache")] db: &redb::Database,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!("Getting outputs with data for inputs: {:?}", inputs);
        let hashs = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect::<Vec<_>>();

        debug!("Getting transactions by hashes: {:?}", hashs);
        let txs = self
            .get_txs_by_hashes(
                #[cfg(feature = "cache")]
                db,
                hashs,
            )
            .await?;

        let res = inputs
            .into_par_iter()
            .map(|input| {
                let idx = input.previous_output.index.value();
                let tx = txs.get(&input.previous_output.tx_hash);
                let output = tx
                    .and_then(|tx| tx.outputs.get(idx as usize).cloned())
                    .ok_or_else(|| Error::PreviousOutputNotFound {
                        tx_hash: input.previous_output.tx_hash.clone(),
                        index: idx,
                        outputs_len: tx.map_or(0, |tx| tx.outputs.len()),
                    });
                let output_data = tx
                    .and_then(|tx| tx.outputs_data.get(idx as usize).cloned())
                    .ok_or_else(|| Error::PreviousOutputDataNotFound {
                        tx_hash: input.previous_output.tx_hash.clone(),
                        index: idx,
                        outputs_data_len: tx.map_or(0, |tx| tx.outputs_data.len()),
                    });
                let result = output.and_then(|output| {
                    output_data.map(|data| (input.previous_output.clone(), output, data))
                });
                debug!(
                    "Got output with data for input: {:?} -> {:?}",
                    input, result
                );
                result
            })
            .collect::<Result<Vec<_>, _>>()?;

        debug!("Got outputs with data: {:?}", res);
        Ok(res)
    }

    pub async fn get_outputs_with_data_ignore_not_found(
        &self,
        #[cfg(feature = "cache")] db: &redb::Database,
        inputs: Vec<CellInput>,
    ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
        debug!("Getting outputs with data for inputs: {:?}", inputs);
        let hashs = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect::<Vec<_>>();
        debug!("Getting transactions by hashes: {:?}", hashs);
        let txs = self
            .get_txs_by_hashes(
                #[cfg(feature = "cache")]
                db,
                hashs,
            )
            .await?;
        let res = inputs
            .into_par_iter()
            .filter_map(|input| {
                let idx = input.previous_output.index.value();
                let tx = txs.get(&input.previous_output.tx_hash);
                let output = tx.and_then(|tx| tx.outputs.get(idx as usize).cloned());
                let output_data = tx.and_then(|tx| tx.outputs_data.get(idx as usize).cloned());
                if let (Some(output), Some(data)) = (output, output_data) {
                    Some((input.previous_output.clone(), output, data))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        debug!("Got outputs with data: {:?}", res);
        Ok(res)
    }
}

pub type HttpFetcher = Fetcher<HttpClient>;

impl Fetcher<HttpClient> {
    pub fn http_client(
        url: impl AsRef<str>,
        retry_interval: u64,
        max_retries: usize,
        max_response_size: u32, // 默认是 10485760 即 10mb
        max_request_size: u32,
    ) -> Result<Self, Error> {
        let mut builder = HttpClientBuilder::default();

        builder = builder
            .max_response_size(max_response_size)
            .max_request_size(max_request_size);

        let client = builder.build(url)?;

        Ok(Self::new(client, retry_interval, max_retries))
    }
}

impl<C> Fetcher<C>
where
    C: ClientT,
{
    pub async fn get_tip_block_number(&self) -> Result<BlockNumber, Error> {
        self.call("get_tip_block_number", rpc_params!()).await
    }

    pub async fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> Result<Option<BlockView>, Error> {
        self.call("get_block_by_number", rpc_params!(number)).await
    }

    pub async fn get_blocks(&self, numbers: Vec<BlockNumber>) -> Result<Vec<BlockView>, Error> {
        self.batch_request(numbers, |number, batch_request| {
            batch_request.insert("get_block_by_number", rpc_params!(number))?;
            Ok(())
        })
        .await
    }

    pub async fn get_txs(
        &self,
        hashs: Vec<H256>,
    ) -> Result<Vec<TransactionWithStatusResponse>, Error> {
        self.batch_request(hashs, |hash, batch_request| {
            batch_request.insert("get_transaction", rpc_params!(hash))?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_jsonrpc_types::{BlockNumber, Either};

    fn get_fetcher() -> Fetcher<HttpClient> {
        Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 10485760, 10485760).unwrap()
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_fetcher_methods() {
        #[cfg(feature = "cache")]
        let database_path = "tmp.redb";
        #[cfg(feature = "cache")]
        let database = redb::Database::create(database_path).unwrap();

        // 创建一个Fetcher实例
        let fetcher = get_fetcher();

        // 初始化redb（仅在启用缓存时需要）
        #[cfg(feature = "cache")]
        init_redb(&database).unwrap();

        // 获取区块链的最新区块号
        let tip_block_number = fetcher.get_tip_block_number().await.unwrap();
        // 打印最新区块号
        println!("Tip block number: {:?}", tip_block_number);

        // 定义要测试的区块号范围
        let start_block_number = tip_block_number.value() - 5;
        let end_block_number = tip_block_number.value();
        // 将区块号范围转换为Vec<BlockNumber>
        let block_numbers: Vec<BlockNumber> = (start_block_number..=end_block_number)
            .map(Into::into)
            .collect();

        // 对于范围内的每个区块号，测试get_block_by_number方法
        for &block_number in &block_numbers {
            // 获取指定区块号的区块
            let block = fetcher.get_block_by_number(block_number).await.unwrap();
            // 断言区块存在
            assert!(
                block.is_some(),
                "Block not found for block number: {:?}",
                block_number
            );
        }

        // 测试get_blocks方法，获取指定区块号范围内的所有区块
        let blocks = fetcher.get_blocks(block_numbers.clone()).await.unwrap();
        // 断言获取的区块数量与请求的区块号数量一致
        assert_eq!(
            blocks.len(),
            block_numbers.len(),
            "Number of blocks retrieved does not match the number of block numbers"
        );

        // 从区块中提取所有交易的哈希值
        let mut tx_hashes = HashSet::new();
        for block in &blocks {
            for transaction in &block.transactions {
                tx_hashes.insert(transaction.hash.clone());
            }
        }

        // 将HashSet转换为Vec以便后续测试
        let tx_hashes_vec: Vec<H256> = tx_hashes.into_iter().collect();

        // 测试get_txs方法，获取指定交易哈希的所有交易
        let txs = fetcher.get_txs(tx_hashes_vec.clone()).await.unwrap();
        // 断言获取的交易数量与请求的交易哈希数量一致
        assert_eq!(
            txs.len(),
            tx_hashes_vec.len(),
            "Number of transactions retrieved does not match the number of transaction hashes"
        );

        // 测试get_txs_by_hashes方法，通过交易哈希获取交易
        let txs_by_hashes = fetcher
            .get_txs_by_hashes(
                #[cfg(feature = "cache")]
                &database,
                tx_hashes_vec.clone(),
            )
            .await
            .unwrap();
        // 断言获取的交易数量与请求的交易哈希数量一致
        assert_eq!(txs_by_hashes.len(), tx_hashes_vec.len(), "Number of transactions retrieved by hashes does not match the number of transaction hashes");

        // 从交易中提取所有单元输入（cell inputs）
        let mut inputs = Vec::new();
        for tx in &txs {
            if let Some(ref tx) = tx.transaction {
                if let Either::Left(ref tx) = tx.inner {
                    for input in &tx.inner.inputs {
                        inputs.push(input.clone());
                    }
                }
            }
        }

        // 测试get_outputs方法，获取指定单元输入的输出
        let outputs = fetcher
            .get_outputs_ignore_not_found(
                #[cfg(feature = "cache")]
                &database,
                inputs.clone(),
            )
            .await
            .unwrap();
        // 断言获取到的输出不为空
        assert!(
            !outputs.is_empty(),
            "No outputs found for the given cell inputs"
        );
        // 打印所有输出信息
        println!("Outputs: {:?}", outputs);

        // 测试get_outputs方法，获取指定单元输入的输出
        let outputs = fetcher
            .get_outputs_ignore_not_found(
                #[cfg(feature = "cache")]
                &database,
                inputs.clone(),
            )
            .await
            .unwrap();
        // 断言获取到的输出不为空
        assert!(
            !outputs.is_empty(),
            "No outputs found for the given cell inputs"
        );
        // 打印所有输出信息
        println!("Outputs: {:?}", outputs);

        // 测试get_outputs_with_data方法，获取指定单元输入的输出和数据
        let outputs_with_data = fetcher
            .get_outputs_with_data_ignore_not_found(
                #[cfg(feature = "cache")]
                &database,
                inputs.clone(),
            )
            .await
            .unwrap();
        // 断言获取到的输出和数据不为空
        assert!(
            !outputs_with_data.is_empty(),
            "No outputs with data found for the given cell inputs"
        );
        // 打印所有输出和数据信息
        println!("Outputs with data: {:?}", outputs_with_data);

        // Test getting the maximum count（仅在启用缓存时需要）
        #[cfg(feature = "cache")]
        {
            // Test clearing transactions with a specific count（仅在启用缓存时需要）
            clear_transactions_below_count(&database, 1).unwrap();
            let max_count_transaction = get_max_count_transaction(&database).unwrap();
            println!(
                "Transaction with maximum count: {:?}",
                max_count_transaction
            );
            // Clean up by deleting the database file
            drop(database); // Ensure the database is closed before deletion
            std::fs::remove_file(database_path).expect("Failed to delete the database file");
        }
    }
}
