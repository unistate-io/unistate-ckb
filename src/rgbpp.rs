use ckb_jsonrpc_types::TransactionView;
use ckb_types::H256;
use jsonrpsee::http_client::HttpClient;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::{
    iter::IntoParallelIterator as _,
    prelude::{IntoParallelRefIterator as _, ParallelIterator as _},
};
use sea_orm::Set;
use tokio::{sync::mpsc, task::JoinSet};
use tracing::debug;

use crate::{
    database::Operations,
    fetcher::Fetcher,
    schemas::{blockchain, rgbpp},
};

pub struct RgbppIndexer {
    txs: Vec<TransactionView>,
    fetcher: Fetcher<HttpClient>,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl RgbppIndexer {
    pub fn new(
        txs: Vec<TransactionView>,
        fetcher: Fetcher<HttpClient>,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Self {
        Self {
            txs,
            fetcher,
            op_sender,
        }
    }

    pub async fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            txs,
            fetcher,
            op_sender,
        } = self;

        let mut tasks = JoinSet::from_iter(
            txs.into_par_iter()
                .map(|tx| index_rgbpp_lock(fetcher.clone(), tx, op_sender.clone()))
                .collect::<Vec<_>>()
                .into_iter(),
        );

        while let Some(task) = tasks.join_next().await {
            task??;
        }

        Ok(())
    }
}

async fn index_rgbpp_lock(
    fetcher: Fetcher<HttpClient>,
    tx: TransactionView,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    debug!("tx: {}", hex::encode(tx.hash.as_bytes()));

    tx.inner
        .witnesses
        .par_iter()
        .filter_map(|witness| blockchain::WitnessArgsReader::from_slice(witness.as_bytes()).ok())
        .filter_map(|witness_args| {
            witness_args
                .to_entity()
                .lock()
                .to_opt()
                .and_then(|lock_witness| {
                    rgbpp::RGBPPUnlockReader::from_slice(lock_witness.raw_data().as_ref())
                        .ok()
                        .map(|unlock| unlock.to_entity())
                })
        })
        .try_for_each(|unlock| upsert_rgbpp_unlock(op_sender.clone(), &unlock, tx.hash.clone()))?;

    let pre_outputs = fetcher.get_outputs(tx.inner.inputs).await?;

    pre_outputs
        .par_iter()
        .filter_map(|output| {
            rgbpp::RGBPPLockReader::from_slice(output.lock.args.as_bytes())
                .ok()
                .map(|reader| reader.to_entity())
        })
        .try_for_each(|lock| upsert_rgbpp_lock(op_sender.clone(), &lock, tx.hash.clone()))?;

    tx.inner
        .outputs
        .par_iter()
        .filter_map(|output| {
            rgbpp::RGBPPLockReader::from_slice(output.lock.args.as_bytes())
                .ok()
                .map(|reader| reader.to_entity())
        })
        .try_for_each(|lock| upsert_rgbpp_lock(op_sender.clone(), &lock, tx.hash.clone()))?;

    Ok(())
}

impl rgbpp::RGBPPLock {
    fn lock_id(&self) -> Vec<u8> {
        blockchain::Bytes::new_unchecked(self.as_bytes())
            .raw_data()
            .to_vec()
    }
}

fn upsert_rgbpp_lock(
    op_sender: mpsc::UnboundedSender<Operations>,
    rgbpp_lock: &rgbpp::RGBPPLock,
    tx: H256,
) -> anyhow::Result<()> {
    use crate::entity::rgbpp_locks;

    let lock_id = rgbpp_lock.lock_id();
    let mut txid = rgbpp_lock.btc_txid().as_bytes().to_vec();
    txid.reverse();
    // Insert rgbpp lock
    let model = rgbpp_locks::ActiveModel {
        lock_id: Set(lock_id),
        out_index: Set(rgbpp_lock.out_index().raw_data().get_u32_le() as i32),
        btc_txid: Set(txid),
        tx: Set(tx.0.to_vec()),
    };

    op_sender.send(Operations::UpsertLock(model))?;

    Ok(())
}

impl rgbpp::RGBPPUnlock {
    fn unlock_id(&self) -> Vec<u8> {
        let hash = ckb_hash::blake2b_256(self.as_bytes());
        hash.to_vec()
    }
}

fn upsert_rgbpp_unlock(
    op_sender: mpsc::UnboundedSender<Operations>,
    rgbpp_unlock: &rgbpp::RGBPPUnlock,
    tx: H256,
) -> anyhow::Result<()> {
    use crate::entity::rgbpp_unlocks;

    let unlock_id = rgbpp_unlock.unlock_id();
    let model = rgbpp_unlocks::ActiveModel {
        unlock_id: Set(unlock_id),
        version: Set(rgbpp_unlock.version().raw_data().get_u16_le() as i16),
        input_len: Set(rgbpp_unlock.extra_data().input_len().as_bytes().get_u8() as i16),
        output_len: Set(rgbpp_unlock.extra_data().output_len().as_bytes().get_u8() as i16),
        btc_tx: Set(rgbpp_unlock.btc_tx().as_bytes().to_vec()),
        btc_tx_proof: Set(rgbpp_unlock.btc_tx_proof().as_bytes().to_vec()),
        tx: Set(tx.0.to_vec()),
    };

    op_sender.send(Operations::UpsertUnlock(model))?;

    Ok(())
}
