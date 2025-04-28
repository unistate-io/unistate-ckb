use ckb_jsonrpc_types::{CellOutput, Script, TransactionView};
use ckb_types::H256;
use fetcher::get_fetcher;
use futures::future::try_join_all;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::prelude::{IntoParallelRefIterator as _, ParallelIterator as _};
use sea_orm::Set;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{
    database::Operations,
    helper::script_hash_type_to_byte,
    schemas::{blockchain, rgbpp},
};

fn to_timestamp_naive(timestamp: u64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .expect("Invalid timestamp")
        .naive_utc()
}

pub struct RgbppTxContext {
    pub tx: TransactionView,
    pub block_number: u64,
    pub timestamp: u64,
}

pub async fn run_rgbpp_indexing_logic(
    tx_contexts: Vec<RgbppTxContext>,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> Result<(), anyhow::Error> {
    let mut tasks = Vec::new();
    for ctx in tx_contexts {
        let op_sender_clone = op_sender.clone();
        tasks.push(tokio::spawn(async move {
            index_rgbpp_lock(ctx.tx, ctx.block_number, ctx.timestamp, op_sender_clone).await
        }));
    }
    try_join_all(tasks)
        .await?
        .into_iter()
        .try_for_each(|res| res)?;
    Ok(())
}

fn process_witnesses(
    tx: &TransactionView,
    block_number: u64,
    timestamp: u64,
    op_sender: &mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let results: Vec<Result<(), anyhow::Error>> = tx
        .inner
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
        .map(|unlock| {
            upsert_rgbpp_unlock(
                op_sender.clone(),
                &unlock,
                tx.hash.clone(),
                block_number,
                timestamp,
            )
        })
        .collect();

    for result in results {
        result?;
    }
    Ok(())
}

fn process_outputs(
    outputs: &[(CellOutput, usize)],
    block_number: u64,
    timestamp: u64,
    op_sender: &mpsc::UnboundedSender<Operations>,
    tx_hash: &H256,
) -> anyhow::Result<()> {
    let results: Vec<Result<(), anyhow::Error>> = outputs
        .par_iter()
        .filter_map(|(output, index)| {
            rgbpp::RGBPPLockReader::from_slice(output.lock.args.as_bytes())
                .ok()
                .map(|reader| (reader.to_entity(), output.lock.clone(), *index))
        })
        .map(|(lock, lock_script, index)| {
            upsert_rgbpp_lock(
                op_sender.clone(),
                &lock,
                tx_hash.clone(),
                lock_script,
                index,
                block_number,
                timestamp,
            )
        })
        .collect();

    for result in results {
        result?;
    }
    Ok(())
}

async fn index_rgbpp_lock(
    tx: TransactionView,
    block_number: u64,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    debug!("Indexing RGBPP for tx: {}", tx.hash);

    process_witnesses(&tx, block_number, timestamp, &op_sender)?;

    let prev_outputs_res = get_fetcher()?.get_outputs(tx.inner.inputs).await;
    match prev_outputs_res {
        Ok(prev_outputs) => {
            let prev_outputs_with_indices: Vec<(CellOutput, usize)> = prev_outputs
                .into_iter()
                .enumerate()
                .map(|(i, output)| (output, i))
                .collect();

            process_outputs(
                &prev_outputs_with_indices,
                block_number,
                timestamp,
                &op_sender,
                &tx.hash,
            )?;
        }
        Err(e) => {
            error!(
                "Failed to fetch previous outputs for tx {}: {:?}. Skipping input processing.",
                tx.hash, e
            );
        }
    }

    let current_outputs_with_indices: Vec<(CellOutput, usize)> = tx
        .inner
        .outputs
        .into_iter()
        .enumerate()
        .map(|(i, output)| (output, i))
        .collect();
    process_outputs(
        &current_outputs_with_indices,
        block_number,
        timestamp,
        &op_sender,
        &tx.hash,
    )?;

    Ok(())
}

fn calc_lock_args_hash(lock_args: &[u8]) -> Vec<u8> {
    let hash = ckb_hash::blake2b_256(lock_args);
    hash.to_vec()
}

fn upsert_rgbpp_lock(
    op_sender: mpsc::UnboundedSender<Operations>,
    rgbpp_lock: &rgbpp::RGBPPLock,
    tx_hash_val: H256,
    lock_script: Script,
    output_index_val: usize,
    block_number: u64,
    timestamp: u64,
) -> anyhow::Result<()> {
    use crate::entity::rgbpp_locks;

    let lock_args_bytes = lock_script.args.as_bytes();
    let lock_args_hash_vec = calc_lock_args_hash(lock_args_bytes);

    let mut txid = rgbpp_lock.btc_txid().as_bytes().to_vec();
    txid.reverse();

    let hash_type_val = script_hash_type_to_byte(lock_script.hash_type);
    let lock_script_hash_type_val = hash_type_val.as_bytes()[0] as i16;

    let model = rgbpp_locks::ActiveModel {
        lock_args_hash: Set(lock_args_hash_vec),
        tx_hash: Set(tx_hash_val.0.to_vec()),
        output_index: Set(output_index_val as i32),
        lock_script_code_hash: Set(lock_script.code_hash.0.to_vec()),
        lock_script_hash_type: Set(lock_script_hash_type_val),
        btc_txid: Set(txid),
        block_number: Set(block_number as i64),
        tx_timestamp: Set(to_timestamp_naive(timestamp)),
    };

    op_sender.send(Operations::UpsertLock(model))?;

    Ok(())
}

fn calc_unlock_witness_hash(unlock_witness: &rgbpp::RGBPPUnlock) -> Vec<u8> {
    let hash = ckb_hash::blake2b_256(unlock_witness.as_bytes());
    hash.to_vec()
}

fn upsert_rgbpp_unlock(
    op_sender: mpsc::UnboundedSender<Operations>,
    rgbpp_unlock: &rgbpp::RGBPPUnlock,
    tx_hash_val: H256,
    block_number: u64,
    timestamp: u64,
) -> anyhow::Result<()> {
    use crate::entity::rgbpp_unlocks;

    let unlock_witness_hash_vec = calc_unlock_witness_hash(rgbpp_unlock);

    let model = rgbpp_unlocks::ActiveModel {
        unlock_witness_hash: Set(unlock_witness_hash_vec),
        tx_hash: Set(tx_hash_val.0.to_vec()),
        version: Set(rgbpp_unlock.version().raw_data().get_u16_le() as i16),
        input_len: Set(rgbpp_unlock.extra_data().input_len().as_bytes().get_u8() as i16),
        output_len: Set(rgbpp_unlock.extra_data().output_len().as_bytes().get_u8() as i16),
        btc_tx: Set(rgbpp_unlock.btc_tx().as_bytes().to_vec()),
        btc_tx_proof: Set(rgbpp_unlock.btc_tx_proof().as_bytes().to_vec()),
        block_number: Set(block_number as i64),
        tx_timestamp: Set(to_timestamp_naive(timestamp)),
    };

    op_sender.send(Operations::UpsertUnlock(model))?;

    Ok(())
}
