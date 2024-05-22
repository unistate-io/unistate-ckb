use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use ckb_jsonrpc_types::{CellInput, CellOutput, JsonBytes, OutPoint, TransactionView};
use ckb_sdk::{rpc::ResponseFormatGetter, util::blake160, NetworkType};
use ckb_types::{packed, H160, H256};
use jsonrpsee::http_client::HttpClient;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use sea_orm::{
    prelude::{Decimal, EntityTrait as _},
    ActiveModelTrait, DatabaseConnection, DbConn, Set,
};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::debug;

use crate::{
    constants::mainnet_info::{UNIQUE_TYPE_SCRIPT, XUDTTYPE_SCRIPT},
    entity::{sea_orm_active_enums, token_info, xudt_cell},
    fetcher::Fetcher,
    schemas::{
        action, blockchain,
        xudt_rce::{self, ScriptVec, XudtData},
    },
    unique::{decode_token_info_bytes, TokenInfo},
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

struct Xudt {
    amount: u128,
    xudt_data: Option<XudtData>,
    xudt_args: Option<ScriptVec>,
    owner_lock_script_hash: Option<[u8; 32]>,
    type_script: ckb_jsonrpc_types::Script,
    lock_script: ckb_jsonrpc_types::Script,
}

struct InputOutPoint {
    hash: H256,
    index: usize,
}

async fn upsert_xudt(
    xudt: Xudt,
    db: &DbConn,
    network: ckb_sdk::NetworkType,
    tx_hash: H256,
    index: usize,
    out_point: Option<InputOutPoint>,
) -> anyhow::Result<String> {
    let Xudt {
        amount,
        xudt_data,
        xudt_args,
        owner_lock_script_hash,
        type_script,
        lock_script,
    } = xudt;

    use ckb_types::prelude::Entity as _;

    let type_id = crate::spore::upsert_address(
        db,
        &action::AddressUnion::Script(action::Script::new_unchecked(
            packed::Script::from(type_script).as_bytes(),
        )),
        network,
    )
    .await?;

    let lock_id = crate::spore::upsert_address(
        db,
        &action::AddressUnion::Script(action::Script::new_unchecked(
            packed::Script::from(lock_script).as_bytes(),
        )),
        network,
    )
    .await?;

    let xudt_args = if let Some(args) = xudt_args {
        let mut address = Vec::new();
        for arg in args
            .into_iter()
            .map(|arg| action::AddressUnion::Script(action::Script::new_unchecked(arg.as_bytes())))
        {
            let addr = crate::spore::upsert_address(db, &arg, network).await?;
            address.push(addr);
        }
        Some(address)
    } else {
        None
    };

    let xudt_data_lock = xudt_data
        .as_ref()
        .map(|data| data.lock().raw_data().to_vec());

    let xudt_data = xudt_data.map(|data| {
        data.data()
            .into_iter()
            .map(|d| String::from_utf8_lossy(&d.raw_data()).to_string())
            .collect::<Vec<_>>()
    });

    let xudt_owner_lock_script_hash = owner_lock_script_hash.map(|hash| hash.to_vec());

    let xudt_exists = xudt_cell::Entity::find_by_id((tx_hash.0.to_vec(), index as i32))
        .one(db)
        .await?
        .is_some();

    let xudt_cell = xudt_cell::ActiveModel {
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        lock_id: Set(lock_id),
        type_id: Set(type_id.clone()),
        status: Set(if out_point.is_some() {
            sea_orm_active_enums::CellStatus::Dead
        } else {
            sea_orm_active_enums::CellStatus::Live
        }),
        input_transaction_index: Set(out_point.as_ref().map(|o| o.index as i32)),
        input_transaction_hash: Set(out_point.map(|o| o.hash.0.to_vec())),
        amount: Set(Decimal::from_i128_with_scale(amount as i128, 0)),
        xudt_args: Set(xudt_args),
        xudt_data: Set(xudt_data),
        xudt_data_lock: Set(xudt_data_lock),
        xudt_owner_lock_script_hash: Set(xudt_owner_lock_script_hash),
    };

    if xudt_exists {
        xudt_cell.update(db).await?;
    } else {
        xudt_cell.insert(db).await?;
    }

    Ok(type_id)
}

async fn update_xudt(
    db: &DbConn,
    tx_hash: H256,
    index: usize,
    out_point: InputOutPoint,
) -> anyhow::Result<()> {
    let xudt_cell = xudt_cell::ActiveModel {
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        status: Set(sea_orm_active_enums::CellStatus::Dead),
        input_transaction_index: Set(Some(out_point.index as i32)),
        input_transaction_hash: Set(Some(out_point.hash.0.to_vec())),
        ..Default::default()
    };

    xudt_cell.update(db).await?;

    Ok(())
}

async fn upsert_token_info(
    token_info: TokenInfo,
    db: &DbConn,
    tx_hash: H256,
    index: usize,
    type_id: String,
) -> anyhow::Result<()> {
    let TokenInfo {
        decimal,
        name,
        symbol,
    } = token_info;

    let token_info = token_info::ActiveModel {
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        decimal: Set(decimal as i16),
        name: Set(name),
        symbol: Set(symbol),
        type_id: Set(type_id),
    };

    let token_info_exists = token_info::Entity::find_by_id((tx_hash.0.to_vec(), index as i32))
        .one(db)
        .await?
        .is_some();

    if token_info_exists {
        token_info.update(db).await?;
    } else {
        token_info.insert(db).await?;
    }

    Ok(())
}

fn parse_xudt(
    (o, od): (CellOutput, JsonBytes),
    maps: &HashMap<H160, xudt_rce::XudtWitnessInput>,
) -> Xudt {
    debug!("Parsing CellOutput: {:?}", o);
    debug!("Parsing JsonBytes: {:?}", od);

    let (mut raw_amount, raw_xudt_data) = od.as_bytes().split_at(16);
    debug!("Raw amount: {:?}", raw_amount);
    debug!("Raw XudtData: {:?}", raw_xudt_data);

    let amount = raw_amount.get_u128_le();
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

    let xudt_args = raw_xudt_args
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            let (mut flags, ext_data) = raw.split_at(4);
            debug!("Flags: {:?}", flags);
            debug!("Extension data: {:?}", ext_data);

            let flags = flags.get_u32_le();
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
        })
        .flatten();
    debug!("Xudt args: {:?}", xudt_args);

    Xudt {
        amount,
        xudt_data,
        xudt_args,
        owner_lock_script_hash,
        type_script: unsafe { o.type_.unwrap_unchecked() },
        lock_script: o.lock,
    }
}

pub struct XudtTx {
    pub tx: TransactionView,
}

pub struct XudtIndexer {
    db: DatabaseConnection,
    stream: ReceiverStream<XudtTx>,
    fetcher: Fetcher<HttpClient>,
    network: NetworkType,
}

impl XudtIndexer {
    pub fn new(
        db: &DatabaseConnection,
        fetcher: &Fetcher<HttpClient>,
        network: NetworkType,
    ) -> (Self, Sender<XudtTx>) {
        let (tx, rx) = mpsc::channel(10000);
        (
            Self {
                db: db.clone(),
                fetcher: fetcher.clone(),
                stream: ReceiverStream::new(rx),
                network,
            },
            tx,
        )
    }

    pub async fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            db,
            mut stream,
            fetcher,
            network,
        } = self;

        while let Some(XudtTx { tx }) = stream.next().await {
            index_xudt(&db, tx, &fetcher, network).await?;
        }

        Ok(())
    }
}

enum XudtParseResult {
    XudtInfo(TokenInfo),
    Xudt(Xudt),
}

async fn index_xudt(
    db: &DatabaseConnection,
    tx: TransactionView,
    fetcher: &Fetcher<HttpClient>,
    network: NetworkType,
) -> anyhow::Result<()> {
    debug!("Indexing transaction: {:?}", tx);

    let xudt_witness_input = process_witnesses::<true>(&tx);
    debug!("XudtWitnessInput: {:?}", xudt_witness_input);

    let xudt_witness_output = process_witnesses::<false>(&tx);
    debug!("XudtWitnessOutput: {:?}", xudt_witness_output);
    let mut infos = Vec::new();
    let mut xudt_type_ids = HashSet::new();

    for (xudt, index) in tx
        .inner
        .outputs
        .into_par_iter()
        .zip(tx.inner.outputs_data.into_par_iter())
        .enumerate()
        .filter_map(|(idx, p)| {
            debug!("Parsing output: {:?}", p);
            debug!("Output type: {:?}", p.0.type_);
            if p.0
                .type_
                .as_ref()
                .map(|t| XUDTTYPE_SCRIPT.code_hash.eq(&t.code_hash))
                .unwrap_or(false)
            {
                debug!("Output is XUDT type");
                let xudt = parse_xudt(p, &xudt_witness_input);
                Some((XudtParseResult::Xudt(xudt), idx))
            } else if p
                .0
                .type_
                .as_ref()
                .map(|t| UNIQUE_TYPE_SCRIPT.code_hash.eq(&t.code_hash))
                .unwrap_or(false)
            {
                debug!("Output is UNIQUE type");
                let token_info = decode_token_info_bytes(p.1.as_bytes());
                match token_info {
                    Ok(info) => Some((XudtParseResult::XudtInfo(info), idx)),
                    Err(err) => {
                        debug!("Decode token info {:?} error: {err:?}", p.1.as_bytes());
                        None
                    }
                }
            } else {
                debug!("Output is not XUDT type");
                None
            }
        })
        .collect::<Vec<_>>()
    {
        match xudt {
            XudtParseResult::XudtInfo(info) => {
                infos.push((info, index));
            }
            XudtParseResult::Xudt(xudt) => {
                let xudt_type_id =
                    upsert_xudt(xudt, db, network, tx.hash.clone(), index, None).await?;
                xudt_type_ids.insert(xudt_type_id);
            }
        }
    }

    for (info, index) in infos {
        let type_id = xudt_type_ids
            .iter()
            .next()
            .ok_or(anyhow!("Not found type id."))?;
        upsert_token_info(info, db, tx.hash.clone(), index, type_id.clone()).await?;
    }

    for (input_index, input) in tx.inner.inputs.into_iter().enumerate() {
        let tx_hash = input.previous_output.tx_hash.clone();
        let index = input.previous_output.index.value() as usize;
        let xudt_exists = xudt_cell::Entity::find_by_id((tx_hash.0.to_vec(), index as i32))
            .one(db)
            .await?
            .is_some();

        if xudt_exists {
            update_xudt(
                db,
                tx_hash,
                index,
                InputOutPoint {
                    hash: tx.hash.clone(),
                    index: input_index,
                },
            )
            .await?;
        }
    }

    // let (input_indexs, inputs): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();

    // let pre_outputs = fetcher.get_outputs_with_data(inputs).await?;

    // debug!("Pre-outputs: {:?}", pre_outputs);

    // for (pre_output, xudt, index) in pre_outputs
    //     .into_par_iter()
    //     .zip(input_indexs)
    //     .filter_map(|(p, idx)| {
    //         debug!("Parsing pre-output: {:?}", p);
    //         debug!("Pre-output type: {:?}", p.1.type_);
    //         if p.1
    //             .type_
    //             .as_ref()
    //             .map(|t| XUDTTYPE_SCRIPT.code_hash.eq(&t.code_hash))
    //             .unwrap_or(false)
    //         {
    //             debug!("Pre-output is XUDT type");
    //             let xudt = parse_xudt((p.1, p.2), &xudt_witness_input);
    //             Some((p.0, xudt, idx))
    //         } else {
    //             debug!("Pre-output is not XUDT type");
    //             None
    //         }
    //     })
    //     .collect::<Vec<_>>()
    // {
    //     upsert_xudt(
    //         xudt,
    //         db,
    //         network,
    //         pre_output.tx_hash,
    //         pre_output.index.value() as usize,
    //         Some(InputOutPoint {
    //             hash: tx.hash.clone(),
    //             index,
    //         }),
    //     )
    //     .await?;
    // }

    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn test_index_xudt() -> anyhow::Result<()> {
    use tracing_subscriber::{
        filter::FilterFn,
        prelude::{__tracing_subscriber_Layer as _, __tracing_subscriber_SubscriberExt as _},
        util::SubscriberInitExt as _,
    };

    let filter = FilterFn::new(|metadata| {
        // Only enable spans or events with the target "interesting_things"
        metadata
            .module_path()
            .map(|p| p.starts_with("unistate_ckb"))
            .unwrap_or(false)
    });

    let layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_level(true);

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
        .init();

    let opt = sea_orm::ConnectOptions::new(
        "postgres://unistate_dev:unistate_dev@localhost:5432/unistate_dev",
    );
    let db = sea_orm::Database::connect(opt).await?;
    let client = crate::fetcher::Fetcher::http_client(
        "https://ckb-rpc.unistate.io",
        500,
        5,
        104857600,
        104857600,
    )?;

    let network = NetworkType::Mainnet;

    let mut txs = client
        .get_txs(vec![H256(hex_literal::hex!(
            "71d8c97b824fd245e6b021e84b5a24ba98cf3d4fec3023ee6823492e1be2e592"
        ))])
        .await?;

    let tx = txs.pop().unwrap();

    index_xudt(&db, tx.transaction.unwrap().get_value()?, &client, network).await?;

    Ok(())
}
