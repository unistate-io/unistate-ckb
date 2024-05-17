use std::collections::HashMap;

use ckb_jsonrpc_types::{CellOutput, JsonBytes, TransactionView};
use ckb_sdk::util::blake160;
use ckb_types::H160;
use jsonrpsee::http_client::HttpClient;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::debug;

use crate::{
    constants::mainnet_info::XUDTTYPE_SCRIPT,
    fetcher::Fetcher,
    schemas::{
        blockchain,
        xudt_rce::{self, ScriptVec, XudtData},
    },
};

fn process_witnesses<const IS_INPUT: bool>(
    tx: &TransactionView,
) -> HashMap<H160, xudt_rce::XudtWitnessInput> {
    let witnesses_count = tx.inner.witnesses.len();
    debug!("Processing {} witnesses", witnesses_count);

    tx.inner
        .witnesses
        .par_iter()
        .filter_map(|witness| {
            debug!("Processing witness: {:?}", witness);
            blockchain::WitnessArgsReader::from_slice(witness.as_bytes()).ok()
        })
        .filter_map(|witness_args| {
            debug!("Witness args: {:?}", witness_args);
            let witness = if IS_INPUT {
                witness_args.to_entity().input_type().to_opt()
            } else {
                witness_args.to_entity().output_type().to_opt()
            };
            witness.and_then(|witness| {
                debug!("Witness: {:?}", witness);
                xudt_rce::XudtWitnessInputReader::from_slice(witness.raw_data().as_ref())
                    .ok()
                    .and_then(|xudt_witness_input| {
                        debug!("XudtWitnessInput: {:?}", xudt_witness_input);
                        xudt_witness_input
                            .raw_extension_data()
                            .to_opt()
                            .map(|esc| esc.as_slice())
                            .map(|raw| blake160(raw))
                            .map(|key| (key, xudt_witness_input.to_entity()))
                    })
            })
        })
        .collect()
}

fn parse_xudt((o, od): (CellOutput, JsonBytes), maps: &HashMap<H160, xudt_rce::XudtWitnessInput>) {
    debug!("Parsing CellOutput: {:?}", o);
    debug!("Parsing JsonBytes: {:?}", od);

    let (mut raw_amount, raw_xudt_data) = od.as_bytes().split_at(16);
    debug!("Raw amount: {:?}", raw_amount);
    debug!("Raw XudtData: {:?}", raw_xudt_data);

    let amount = raw_amount.get_u128();
    debug!("Amount: {}", amount);

    let xudt_data = XudtData::from_slice(raw_xudt_data).ok();
    debug!("XudtData: {:?}", xudt_data);

    let (raw_onwer_lock_script_hash, raw_xudt_args) = o
        .type_
        .as_ref()
        .map(|tp| tp.args.as_bytes().split_at(32))
        .unzip();
    debug!(
        "Raw owner lock script hash: {:?}",
        raw_onwer_lock_script_hash
    );
    debug!("Raw Xudt args: {:?}", raw_xudt_args);

    let owner_lock_script_hash =
        raw_onwer_lock_script_hash.map(|raw| std::array::from_fn::<u8, 32, _>(|i| raw[i]));
    debug!("Owner lock script hash: {:?}", owner_lock_script_hash);

    let xudt_args = raw_xudt_args.filter(|raw| !raw.is_empty()).map(|raw| {
        let (mut flags, ext_data) = raw.split_at(4);
        debug!("Flags: {:?}", flags);
        debug!("Extension data: {:?}", ext_data);

        let flags = flags.get_u32();
        debug!("Flags: {}", flags);

        let ext_data = match flags & 0x1FFFFFFF {
            0 => {
                debug!("No extension");
                None
            }
            0x1 => match ScriptVec::from_slice(ext_data) {
                Ok(sv) => {
                    debug!("Script vec: {:?}", sv);
                    Some(sv)
                }
                Err(e) => {
                    debug!("Failed to parse script vec: {:?}", e);
                    unreachable!()
                }
            },
            0x2 => {
                let hash = H160(std::array::from_fn::<u8, 20, _>(|i| ext_data[i]));
                debug!("Hash: {:?}", hash);
                maps.get(&hash)
                    .and_then(|witness| witness.raw_extension_data().to_opt())
            }
            _ => {
                debug!("Unknown flags!");
                unreachable!()
            }
        };
        debug!("Extension data: {:?}", ext_data);
        ext_data
    });
    debug!("Xudt args: {:?}", xudt_args);
}

pub struct XudtTx {
    pub tx: TransactionView,
    pub timestamp: u64,
    pub height: u64,
}

pub struct XudtIndexer {
    db: DatabaseConnection,
    stream: ReceiverStream<XudtTx>,
    fetcher: Fetcher<HttpClient>,
}

impl XudtIndexer {
    pub fn new(db: &DatabaseConnection, fetcher: &Fetcher<HttpClient>) -> (Self, Sender<XudtTx>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                db: db.clone(),
                fetcher: fetcher.clone(),
                stream: ReceiverStream::new(rx),
            },
            tx,
        )
    }

    pub async fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            db,
            mut stream,
            fetcher,
        } = self;

        while let Some(XudtTx {
            tx,
            timestamp,
            height,
        }) = stream.next().await
        {
            index_xudt(&db, tx, &fetcher, timestamp, height).await?;
        }

        Ok(())
    }
}

async fn index_xudt(
    db: &DatabaseConnection,
    tx: TransactionView,
    fetcher: &Fetcher<HttpClient>,
    timestamp: u64,
    height: u64,
) -> anyhow::Result<()> {
    debug!("Indexing transaction: {:?}", tx);

    let xudt_witness_input = process_witnesses::<true>(&tx);
    debug!("XudtWitnessInput: {:?}", xudt_witness_input);

    let xudt_witness_output = process_witnesses::<false>(&tx);
    debug!("XudtWitnessOutput: {:?}", xudt_witness_output);

    tx.inner
        .outputs
        .into_par_iter()
        .zip(tx.inner.outputs_data.into_par_iter())
        .for_each(|p| {
            debug!("Parsing output: {:?}", p);
            debug!("Output type: {:?}", p.0.type_);
            if p.0
                .type_
                .as_ref()
                .map(|t| XUDTTYPE_SCRIPT.code_hash.eq(&t.code_hash))
                .unwrap_or(false)
            {
                debug!("Output is XUDT type");
                parse_xudt(p, &xudt_witness_output);
            } else {
                debug!("Output is not XUDT type");
            }
        });

    let pre_outputs = fetcher.get_outputs_with_data(tx.inner.inputs).await?;
    debug!("Pre-outputs: {:?}", pre_outputs);

    pre_outputs.into_par_iter().for_each(|p| {
        debug!("Parsing pre-output: {:?}", p);
        debug!("Pre-output type: {:?}", p.0.type_);
        if p.0
            .type_
            .as_ref()
            .map(|t| XUDTTYPE_SCRIPT.code_hash.eq(&t.code_hash))
            .unwrap_or(false)
        {
            debug!("Pre-output is XUDT type");
            parse_xudt(p, &xudt_witness_input);
        } else {
            debug!("Pre-output is not XUDT type");
        }
    });

    Ok(())
}
