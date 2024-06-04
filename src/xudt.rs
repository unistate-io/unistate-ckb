use std::collections::HashMap;

use bigdecimal::num_bigint::BigInt;
use ckb_jsonrpc_types::{CellOutput, JsonBytes, TransactionView};
use ckb_sdk::{util::blake160, NetworkType};
use ckb_types::{packed, H160, H256};
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use sea_orm::{prelude::BigDecimal, Set};
use tokio::sync::mpsc::{self};
use tracing::debug;

use crate::{
    constants::Constants,
    database::Operations,
    entity::{token_info, xudt_cell, xudt_status_cell},
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

fn upsert_xudt(
    xudt: Xudt,
    network: ckb_sdk::NetworkType,
    tx_hash: H256,
    index: usize,
    op_sender: mpsc::UnboundedSender<Operations>,
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
        &action::AddressUnion::Script(action::Script::new_unchecked(
            packed::Script::from(type_script).as_bytes(),
        )),
        network,
        op_sender.clone(),
    )?;

    let lock_id = crate::spore::upsert_address(
        &action::AddressUnion::Script(action::Script::new_unchecked(
            packed::Script::from(lock_script).as_bytes(),
        )),
        network,
        op_sender.clone(),
    )?;

    let xudt_args = if let Some(args) = xudt_args {
        let mut address = Vec::new();
        for arg in args
            .into_iter()
            .map(|arg| action::AddressUnion::Script(action::Script::new_unchecked(arg.as_bytes())))
        {
            let addr = crate::spore::upsert_address(&arg, network, op_sender.clone())?;
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

    let xudt_cell = xudt_cell::ActiveModel {
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        lock_id: Set(lock_id),
        type_id: Set(type_id.clone()),
        amount: Set(BigDecimal::new(BigInt::from(amount), 0)),
        xudt_args: Set(xudt_args),
        xudt_data: Set(xudt_data),
        xudt_data_lock: Set(xudt_data_lock),
        xudt_owner_lock_script_hash: Set(xudt_owner_lock_script_hash),
        is_consumed: Set(false),
    };

    op_sender.send(Operations::UpsertXudt(xudt_cell))?;

    Ok(type_id)
}

fn update_xudt(
    tx_hash: H256,
    index: usize,
    out_point: InputOutPoint,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let xudt_cell = xudt_status_cell::ActiveModel {
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        input_transaction_index: Set(Some(out_point.index as i32)),
        input_transaction_hash: Set(Some(out_point.hash.0.to_vec())),
    };

    op_sender.send(Operations::UpdateXudt(xudt_cell))?;

    Ok(())
}

fn upsert_token_info(
    token_info: TokenInfo,
    tx_hash: H256,
    index: usize,
    type_id: String,
    op_sender: mpsc::UnboundedSender<Operations>,
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

    op_sender.send(Operations::UpsertTokenInfo(token_info))?;

    Ok(())
}

fn parse_xudt(
    (o, od): (CellOutput, JsonBytes),
    maps: &HashMap<H160, xudt_rce::XudtWitnessInput>,
) -> Option<Xudt> {
    debug!("Parsing CellOutput: {:?}", o);
    debug!("Parsing JsonBytes: {:?}", od);

    let (mut raw_amount, raw_xudt_data) = split_at_checked(od.as_bytes(), 16)?;

    debug!("Raw amount: {:?}", raw_amount);
    debug!("Raw XudtData: {:?}", raw_xudt_data);

    let amount = raw_amount.get_u128_le();
    debug!("Amount: {}", amount);

    let xudt_data = XudtData::from_slice(raw_xudt_data).ok();
    debug!("XudtData: {:?}", xudt_data);

    let (raw_onwer_lock_script_hash, raw_xudt_args) = o
        .type_
        .as_ref()
        .and_then(|tp| split_at_checked(tp.args.as_bytes(), 32))
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
            let (mut flags, ext_data) = split_at_checked(raw, 4)?;
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
    let xudt = Xudt {
        amount,
        xudt_data,
        xudt_args,
        owner_lock_script_hash,
        type_script: unsafe { o.type_.unwrap_unchecked() },
        lock_script: o.lock,
    };
    Some(xudt)
}

pub struct XudtTx {
    pub tx: TransactionView,
}

pub struct XudtIndexer {
    txs: Vec<XudtTx>,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl XudtIndexer {
    pub fn new(
        txs: Vec<XudtTx>,
        network: NetworkType,
        op_sender: mpsc::UnboundedSender<Operations>,
    ) -> Self {
        Self {
            txs,
            network,
            op_sender,
        }
    }

    pub fn index(self) -> Result<(), anyhow::Error> {
        let Self {
            txs,
            network,
            op_sender,
        } = self;
        let constants = Constants::from_config(network);

        txs.into_par_iter()
            .try_for_each(|tx| index_xudt(tx.tx, network, constants, op_sender.clone()))?;

        Ok(())
    }
}

enum XudtParseResult {
    XudtInfo(TokenInfo),
    Xudt(Xudt),
}

fn index_xudt(
    tx: TransactionView,
    network: NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
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
        .enumerate()
        .filter_map(|(idx, p)| {
            debug!("Parsing output: {:?}", p);
            debug!("Output type: {:?}", p.0.type_);
            if p.0
                .type_
                .as_ref()
                .map(|t| constants.xudttype_script().code_hash.eq(&t.code_hash))
                .unwrap_or(false)
            {
                debug!("Output is XUDT type");
                let xudt = parse_xudt(p, &xudt_witness_input);
                xudt.map(|xudt| (XudtParseResult::Xudt(xudt), idx))
            } else if p
                .0
                .type_
                .as_ref()
                .map(|t| constants.unique_type_script().code_hash.eq(&t.code_hash))
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
        .try_for_each_init(
            || (None, None),
            |(info_op, type_id_op), (xudt, index)| -> anyhow::Result<()> {
                match xudt {
                    XudtParseResult::XudtInfo(info) => {
                        *info_op = Some(info);
                    }
                    XudtParseResult::Xudt(xudt) => {
                        let xudt_type_id =
                            upsert_xudt(xudt, network, tx.hash.clone(), index, op_sender.clone())?;
                        *type_id_op = Some(xudt_type_id);
                    }
                }
                if info_op.is_some() && type_id_op.is_some() {
                    let info = info_op.take().unwrap();
                    let type_id = type_id_op.take().unwrap();
                    upsert_token_info(info, tx.hash.clone(), index, type_id, op_sender.clone())?;
                }
                Ok(())
            },
        )?;

    tx.inner.inputs.into_par_iter().enumerate().try_for_each(
        |(input_index, input)| -> anyhow::Result<()> {
            let tx_hash = input.previous_output.tx_hash.clone();
            let index = input.previous_output.index.value() as usize;

            update_xudt(
                tx_hash,
                index,
                InputOutPoint {
                    hash: tx.hash.clone(),
                    index: input_index,
                },
                op_sender.clone(),
            )?;
            Ok(())
        },
    )?;

    Ok(())
}

#[inline]
#[must_use]
pub const fn split_at_checked(bytes: &[u8], mid: usize) -> Option<(&[u8], &[u8])> {
    if mid <= bytes.len() {
        // SAFETY: `[ptr; mid]` and `[mid; len]` are inside `self`, which
        // fulfills the requirements of `split_at_unchecked`.
        Some(unsafe { bytes.split_at_unchecked(mid) })
    } else {
        None
    }
}
