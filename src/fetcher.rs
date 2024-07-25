use std::collections::{HashMap, HashSet};

use ckb_jsonrpc_types::{
    BlockNumber, BlockView, CellInput, CellOutput, Transaction, TransactionWithStatusResponse,
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
use tracing::debug;

use crate::{error::Error, MB};

#[derive(Debug, Clone)]
pub struct Fetcher<C> {
    client: C,
    retry_interval: u64,
    max_retries: usize,
}

macro_rules! retry {
    ($call:expr, $max_retries:expr, $retry_interval:expr) => {
        {
            let mut retries = 0;
            let max_retries = $max_retries; // adjust this value according to your needs
            let retry_interval = std::time::Duration::from_millis($retry_interval); // adjust this value according to your needs

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
        }
    };
}

impl<C> Fetcher<C>
where
    C: ClientT,
{
    pub const fn new(client: C, retry_interval: u64, max_retries: usize) -> Self {
        Self {
            client,
            retry_interval,
            max_retries,
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
}

impl Fetcher<HttpClient> {
    pub fn from_config(config: &crate::config::UnistateConfig) -> Result<Self, Error> {
        let fetcher = &config.featcher;
        let url = &config.url;
        Self::http_client(
            url,
            fetcher.retry_interval,
            fetcher.max_retries,
            fetcher.max_response_size * MB,
            fetcher.max_request_size * MB,
        )
    }
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

    // pub async fn get_block_by_number(
    //     &self,
    //     number: BlockNumber,
    // ) -> Result<Option<BlockView>, Error> {
    //     self.call("get_block_by_number", rpc_params!(number)).await
    // }

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

    pub async fn get_txs_by_hashes(
        &self,
        hashes: Vec<H256>,
    ) -> Result<HashMap<H256, Transaction>, Error> {
        debug!("Getting transactions by hashes: {:?}", hashes);
        let txs = self
            .get_txs(hashes)
            .await?
            .into_par_iter()
            .filter_map(|tx| {
                tx.transaction
                    .and_then(|t| match t.get_value() {
                        Ok(v) => {
                            debug!("Got transaction: {:?}", v);
                            Some(v)
                        }
                        Err(e) => {
                            debug!("Failed to get transaction value: {e:?}");
                            None
                        }
                    })
                    .map(|tx| (tx.hash, tx.inner))
            })
            .collect::<HashMap<_, _>>();

        debug!("Got transactions: {:?}", txs);
        Ok(txs)
    }

    pub async fn get_outputs(&self, inputs: Vec<CellInput>) -> Result<Vec<CellOutput>, Error> {
        debug!("Getting outputs for inputs: {:?}", inputs);
        let hashs = inputs
            .par_iter()
            .map(|input| input.previous_output.tx_hash.clone())
            .collect::<HashSet<_>>()
            .into_par_iter()
            .collect::<Vec<_>>();

        debug!("Getting transactions by hashes: {:?}", hashs);
        let txs = self.get_txs_by_hashes(hashs).await?;

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
                    });
                debug!("Got output for input: {:?} -> {:?}", input, output);
                output
            })
            .collect::<Result<Vec<_>, _>>()?;

        debug!("Got outputs: {:?}", res);
        Ok(res)
    }

    // pub async fn get_outputs_with_data(
    //     &self,
    //     inputs: Vec<CellInput>,
    // ) -> Result<Vec<(OutPoint, CellOutput, JsonBytes)>, Error> {
    //     debug!("Getting outputs with data for inputs: {:?}", inputs);
    //     let hashs = inputs
    //         .par_iter()
    //         .map(|input| input.previous_output.tx_hash.clone())
    //         .collect::<HashSet<_>>()
    //         .into_par_iter()
    //         .collect::<Vec<_>>();

    //     debug!("Getting transactions by hashes: {:?}", hashs);
    //     let txs = self.get_txs_by_hashes(hashs).await?;

    //     let res = inputs
    //         .into_par_iter()
    //         .map(|input| {
    //             let idx = input.previous_output.index.value();
    //             let tx = txs.get(&input.previous_output.tx_hash);
    //             let output = tx
    //                 .and_then(|tx| tx.outputs.get(idx as usize).cloned())
    //                 .ok_or_else(|| Error::PreviousOutputNotFound {
    //                     tx_hash: input.previous_output.tx_hash.clone(),
    //                     index: idx,
    //                 });
    //             let output_data = tx
    //                 .and_then(|tx| tx.outputs_data.get(idx as usize).cloned())
    //                 .ok_or_else(|| Error::PreviousOutputDataNotFound {
    //                     tx_hash: input.previous_output.tx_hash.clone(),
    //                     index: idx,
    //                 });
    //             let result = output.and_then(|output| {
    //                 output_data.map(|data| (input.previous_output.clone(), output, data))
    //             });
    //             debug!(
    //                 "Got output with data for input: {:?} -> {:?}",
    //                 input, result
    //             );
    //             result
    //         })
    //         .collect::<Result<Vec<_>, _>>()?;

    //     debug!("Got outputs with data: {:?}", res);
    //     Ok(res)
    // }
}

#[tokio::test]
async fn test_get_tip_block_number() {
    let fetcher =
        Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 10485760, 10485760).unwrap();
    let res = fetcher.get_tip_block_number().await;
    println!("{res:?}");
}

// #[tokio::test]
// async fn test_get_block_by_number() {
//     let fetcher =
//         Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 10485760, 10485760).unwrap();
//     let res = fetcher.get_block_by_number(12785755.into()).await;
//     println!("{res:?}");
// }

#[tokio::test]
async fn test_get_blocks() {
    let fetcher =
        Fetcher::http_client("https://ckb-rpc.unistate.io", 500, 5, 10485760, 10485760).unwrap();
    let res = fetcher
        .get_blocks(vec![12785755.into(), 12785756.into(), 12785757.into()])
        .await;
    println!("{res:?}");
}
