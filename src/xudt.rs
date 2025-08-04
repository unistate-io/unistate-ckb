use std::collections::HashMap;

use crate::{
    database::Operations,
    entity::{token_info, transaction_outputs_status, xudt_cells},
    helper::{to_timestamp_naive, upsert_address},
    schemas::{
        action, blockchain,
        xudt_rce::{self, ScriptVec, XudtData},
    },
    unique::{TokenInfo, decode_token_info_bytes},
};
use bigdecimal::num_bigint::BigInt;
use ckb_jsonrpc_types::{CellOutput, JsonBytes, TransactionView};
use ckb_types::{H160, H256};
use constants::Constants;
use molecule::{
    bytes::Buf,
    prelude::{Entity, Reader as _},
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use sea_orm::{Set, prelude::BigDecimal};
use tokio::sync::mpsc::{self};
use tracing::debug;
use utils::blake160;
use utils::network::NetworkType;

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
                            .map(blake160)
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

fn upsert_xudt(
    xudt: Xudt,
    network: utils::network::NetworkType,
    tx_hash: H256,
    index: usize,
    block_number: u64,
    timestamp: u64,
    first_seen_tx_hash: &H256,
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

    let type_address_id = upsert_address(
        &action::AddressUnion::from_json_script(type_script),
        network,
        block_number,
        first_seen_tx_hash,
        timestamp,
        op_sender.clone(),
    )?;

    let lock_address_id = upsert_address(
        &action::AddressUnion::from_json_script(lock_script),
        network,
        block_number,
        first_seen_tx_hash,
        timestamp,
        op_sender.clone(),
    )?;

    let xudt_args_json = if let Some(args) = xudt_args {
        let mut address_ids = Vec::new();
        for arg in args
            .into_iter()
            .map(|arg| action::AddressUnion::Script(action::Script::new_unchecked(arg.as_bytes())))
        {
            let addr_id = upsert_address(
                &arg,
                network,
                block_number,
                first_seen_tx_hash,
                timestamp,
                op_sender.clone(),
            )?;
            address_ids.push(addr_id);
        }
        Some(serde_json::to_value(address_ids)?)
    } else {
        None
    };

    let xudt_data_lock_hash = xudt_data
        .as_ref()
        .map(|data| data.lock().raw_data().to_vec());

    let xudt_data_json = xudt_data.map(|data| {
        let strings = data
            .data()
            .into_iter()
            .map(|d| String::from_utf8_lossy(&d.raw_data()).to_string())
            .collect::<Vec<_>>();
        serde_json::to_value(strings).unwrap_or(serde_json::Value::Null)
    });

    let owner_lock_hash = owner_lock_script_hash.map(|hash| hash.to_vec());

    let xudt_cell_model = xudt_cells::ActiveModel {
        tx_hash: Set(tx_hash.0.to_vec()),
        output_index: Set(index as i32),
        lock_address_id: Set(lock_address_id),
        type_address_id: Set(type_address_id.clone()),
        amount: Set(BigDecimal::new(BigInt::from(amount), 0)),
        xudt_extension_args: Set(xudt_args_json),
        xudt_extension_data: Set(xudt_data_json),
        xudt_data_lock_hash: Set(xudt_data_lock_hash),
        owner_lock_hash: Set(owner_lock_hash),
        block_number: Set(block_number as i64),
        tx_timestamp: Set(to_timestamp_naive(timestamp)),
    };

    op_sender.send(Operations::UpsertXudt(xudt_cell_model))?;

    Ok(type_address_id)
}

fn update_output_status(
    output_tx_hash: H256,
    output_index: usize,
    consuming_tx_hash: H256,
    consuming_input_index: usize,
    consuming_block_number: u64,
    consuming_tx_timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let output_status = transaction_outputs_status::ActiveModel {
        output_tx_hash: Set(output_tx_hash.0.to_vec()),
        output_tx_index: Set(output_index as i32),
        consumed_by_tx_hash: Set(Some(consuming_tx_hash.0.to_vec())),
        consumed_by_input_index: Set(Some(consuming_input_index as i32)),
        consuming_block_number: Set(Some(consuming_block_number as i64)),
        consuming_tx_timestamp: Set(Some(to_timestamp_naive(consuming_tx_timestamp))),
    };

    op_sender.send(Operations::UpdateXudtCell(output_status))?;

    Ok(())
}

fn upsert_token_info(
    token_info_data: TokenInfo,
    tx_hash: H256,
    index: usize,
    type_address_id: String,
    block_number: u64,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let TokenInfo {
        decimal,
        name,
        symbol,
    } = token_info_data;

    let token_info_model = token_info::ActiveModel {
        type_address_id: Set(type_address_id),
        defining_tx_hash: Set(tx_hash.0.to_vec()),
        defining_output_index: Set(index as i32),
        decimal: Set(decimal as i16),
        name: Set(name),
        symbol: Set(symbol),
        inscription_address_id: Set(None),
        udt_hash: Set(None),
        expected_supply: Set(None),
        mint_limit: Set(None),
        mint_status: Set(None),
        block_number: Set(block_number as i64),
        tx_timestamp: Set(to_timestamp_naive(timestamp)),
    };

    debug!("token info: {:?}", token_info_model);

    op_sender.send(Operations::UpsertTokenInfo(token_info_model))?;

    Ok(())
}

fn parse_xudt(
    (o, od): (CellOutput, JsonBytes),
    maps: &HashMap<H160, xudt_rce::XudtWitnessInput>,
) -> Option<Xudt> {
    debug!("Parsing CellOutput: {:?}", o);
    debug!("Parsing JsonBytes: {:?}", od);

    let (mut raw_amount, raw_xudt_data) = od.as_bytes().split_at_checked(16)?;

    debug!("Raw amount: {:?}", raw_amount);
    debug!("Raw XudtData: {:?}", raw_xudt_data);

    let amount = raw_amount.get_u128_le();
    debug!("Amount: {}", amount);

    let xudt_data = XudtData::from_slice(raw_xudt_data).ok();
    debug!("XudtData: {:?}", xudt_data);

    let (raw_owner_lock_script_hash, raw_xudt_args) = o
        .type_
        .as_ref()
        .and_then(|tp| tp.args.as_bytes().split_at_checked(32))
        .unzip();
    debug!(
        "Raw owner lock script hash: {:?}",
        raw_owner_lock_script_hash
    );
    debug!("Raw Xudt args: {:?}", raw_xudt_args);

    let owner_lock_script_hash =
        raw_owner_lock_script_hash.map(|raw| std::array::from_fn::<u8, 32, _>(|i| raw[i]));
    debug!("Owner lock script hash: {:?}", owner_lock_script_hash);

    let xudt_args = raw_xudt_args.filter(|raw| !raw.is_empty()).and_then(|raw| {
        let (mut flags, ext_data) = raw.split_at_checked(4)?;
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
                    tracing::error!("Failed to parse script vec from XUDT args: {:?}", e);
                    None
                }
            },
            0x2 => {
                if ext_data.len() >= 20 {
                    let hash = H160(std::array::from_fn::<u8, 20, _>(|i| ext_data[i]));
                    debug!("Hash: {:?}", hash);
                    maps.get(&hash)
                        .and_then(|witness| witness.raw_extension_data().to_opt())
                } else {
                    tracing::error!("Invalid length for extension data hash (flag 0x2)");
                    None
                }
            }
            _ => {
                tracing::error!("Unknown XUDT flags: {}", flags);
                None
            }
        };
        debug!("Extension data: {:?}", ext_data);
        ext_data
    });
    debug!("Xudt args: {:?}", xudt_args);

    let type_script = unsafe { o.type_.unwrap_unchecked() };

    Some(Xudt {
        amount,
        xudt_data,
        xudt_args,
        owner_lock_script_hash,
        type_script,
        lock_script: o.lock,
    })
}

pub struct XudtTxContext {
    pub tx: TransactionView,
    pub block_number: u64,
    pub timestamp: u64,
}

pub fn index_xudt_transactions(
    tx_contexts: Vec<XudtTxContext>,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> Result<(), anyhow::Error> {
    let constants = Constants::from_config(network);

    tx_contexts.into_par_iter().try_for_each(|ctx| {
        index_xudt(
            ctx.tx,
            network,
            constants,
            ctx.block_number,
            ctx.timestamp,
            op_sender.clone(),
        )
    })?;

    Ok(())
}

enum XudtParseResult {
    XudtInfo(TokenInfo),
    Xudt(Xudt),
}

#[derive(Debug, Clone)]
struct TokenData {
    info: Option<(TokenInfo, usize)>,
    token_id: Option<String>,
}

impl TokenData {
    fn new() -> Self {
        Self {
            info: None,
            token_id: None,
        }
    }
    fn merge(self, other: Self) -> Self {
        Self {
            info: self.info.or(other.info),
            token_id: self.token_id.or(other.token_id),
        }
    }
    fn zip(self) -> Option<((TokenInfo, usize), String)> {
        self.info.zip(self.token_id)
    }
}

fn index_xudt(
    tx: TransactionView,
    network: NetworkType,
    constants: Constants,
    block_number: u64,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    debug!("Indexing transaction: {}", tx.hash);

    let xudt_witness_input = process_witnesses::<true>(&tx);
    debug!("XudtWitnessInput: {:?}", xudt_witness_input);

    let xudt_witness_output = process_witnesses::<false>(&tx);
    debug!("XudtWitnessOutput: {:?}", xudt_witness_output);

    let token_data = tx
        .inner
        .outputs
        .clone()
        .into_par_iter()
        .zip(tx.inner.outputs_data.into_par_iter())
        .enumerate()
        .filter_map(|(idx, p)| {
            debug!("Parsing output index {}: type={:?}", idx, p.0.type_);
            if p.0
                .type_
                .as_ref()
                .map(|t| constants.is_xudt_type(t))
                .unwrap_or(false)
            {
                debug!("Output {} is XUDT type", idx);
                let xudt = parse_xudt(p, &xudt_witness_input);
                xudt.map(|xudt| (XudtParseResult::Xudt(xudt), idx))
            } else if p
                .0
                .type_
                .as_ref()
                .map(|t| constants.is_unique_type(t))
                .unwrap_or(false)
            {
                debug!("Output {} is UNIQUE type", idx);
                let token_info = decode_token_info_bytes(p.1.as_bytes());
                match token_info {
                    Ok(info) => Some((XudtParseResult::XudtInfo(info), idx)),
                    Err(err) => {
                        debug!(
                            "Decode token info for output {} ({:?}) error: {err:?}",
                            idx,
                            p.1.as_bytes()
                        );
                        None
                    }
                }
            } else {
                debug!("Output {} is not XUDT or UNIQUE type", idx);
                None
            }
        })
        .try_fold_with(
            TokenData::new(),
            |mut token_data, (parse_result, index)| -> anyhow::Result<TokenData> {
                match parse_result {
                    XudtParseResult::XudtInfo(info) => {
                        token_data.info = Some((info, index));
                    }
                    XudtParseResult::Xudt(xudt) => {
                        let xudt_type_address_id = upsert_xudt(
                            xudt,
                            network,
                            tx.hash.clone(),
                            index,
                            block_number,
                            timestamp,
                            &tx.hash,
                            op_sender.clone(),
                        )?;
                        token_data.token_id = Some(xudt_type_address_id);
                    }
                }
                debug!("token data after index {}: {:?}", index, token_data);
                Ok(token_data)
            },
        )
        .try_reduce(TokenData::new, |pre, new| Ok(pre.merge(new)))?;

    debug!("Final TokenData for tx {}: {:?}", tx.hash, token_data);

    if let Some(((info, index), token_id)) = token_data.zip() {
        upsert_token_info(
            info,
            tx.hash.clone(),
            index,
            token_id,
            block_number,
            timestamp,
            op_sender.clone(),
        )?;
    }

    tx.inner.inputs.into_par_iter().enumerate().try_for_each(
        |(input_index, input)| -> anyhow::Result<()> {
            let previous_tx_hash = input.previous_output.tx_hash.clone();
            let previous_tx_index = input.previous_output.index.value() as usize;

            update_output_status(
                previous_tx_hash,
                previous_tx_index,
                tx.hash.clone(),
                input_index,
                block_number,
                timestamp,
                op_sender.clone(),
            )?;
            Ok(())
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use ckb_jsonrpc_types::TransactionView;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::xudt::index_xudt;
    use constants::Constants;
    #[test]
    #[tracing_test::traced_test]
    fn debug_index_xudt() {
        let constants = Constants::Mainnet;
        let tx: TransactionView = serde_json::from_str(TEST_JSON).expect("deserialize failed.");
        let is_xudt = tx.inner.cell_deps.iter().any(|cd| constants.is_xudt(cd));
        debug!("is xudt: {is_xudt}");

        let (sender, mut recver) = mpsc::unbounded_channel();
        index_xudt(
            tx,
            utils::network::NetworkType::Mainnet,
            constants,
            0, // block_number
            0, // timestamp
            sender,
        )
        .unwrap();
        while let Some(op) = recver.blocking_recv() {
            debug!("op: {op:?}");
        }
        debug!("done!");
    }

    const TEST_JSON: &'static str = r#"
    {
      "cell_deps": [
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x283e09c4e2e1ee40790e38565555a730a67287680fb0a720cbf46f575e8cc0ca"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xc2a187e18ceb9fc30fdcec5a7f3c547e79a4ddde930bc646cfee278319c00943"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x5292c77c62f108e3a33e54ed3bdcc4457a9d7d88be0c6ef3c1811f473394e2f7"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x44b2bbddee6a13e688b61589cb8af16a1d25e58fa7e0b732cea3f1d32683beb3"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x7dfd96165a48fc0f346ad207491e8309f22c76f73f9b0940253471119706b5cb"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xc3f90640cd458b62b0cf20f20d995c8e72e9931a04185d76aadb27eb89c2ad1a"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xc07844ce21b38e4b071dd0e1ee3b0e27afd8d7532491327f39b786343f558ab7"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xf6a5eef65101899db9709c8de1cc28f23c1bee90d857ebe176f6647ef109e20d"
          }
        },
        {
          "dep_type": "dep_group",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0x71a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c"
          }
        }
      ],
      "hash": "0x02d1d411e55b5232072c8831bd2762915268e5a70ca276e09d303d1b1fa1168b",
      "header_deps": [],
      "inputs": [
        {
          "previous_output": {
            "index": "0x0",
            "tx_hash": "0x7e3d052c407c77e04d2d653ad9aa0abf381fea31d9ff3abfc69a0887cad1cad0"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x2",
            "tx_hash": "0xf75fcce4e9b40d7a6e259b62d731679801f5078e0c141f69c6a2bb6b6b0faa4b"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x3",
            "tx_hash": "0xf75fcce4e9b40d7a6e259b62d731679801f5078e0c141f69c6a2bb6b6b0faa4b"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x4",
            "tx_hash": "0xf75fcce4e9b40d7a6e259b62d731679801f5078e0c141f69c6a2bb6b6b0faa4b"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x5",
            "tx_hash": "0xf75fcce4e9b40d7a6e259b62d731679801f5078e0c141f69c6a2bb6b6b0faa4b"
          },
          "since": "0x0"
        }
      ],
      "outputs": [
        {
          "capacity": "0x35a4e9000",
          "lock": {
            "args": "0x120b23e3588c906c3f723c58ef4d6baee7840a977c00",
            "code_hash": "0x9b819793a64463aed77c615d6cb226eea5487ccfc0783043a587254cda2b6f26",
            "hash_type": "type"
          },
          "type": {
            "args": "0xd591ebdc69626647e056e13345fd830c8b876bb06aa07ba610479eb77153ea9f",
            "code_hash": "0xbfa35a9c38a676682b65ade8f02be164d48632281477e36f8dc2f41f79e56bfc",
            "hash_type": "type"
          }
        },
        {
          "capacity": "0x5a8649300",
          "lock": {
            "args": "0x",
            "code_hash": "0x7ef6e226ca9a3514ac76759f0b1550e70c9aa10aff89111fedf2c9d800d256f7",
            "hash_type": "type"
          },
          "type": {
            "args": "0xf648d08a2f3ddede1cdb2fd2b158534fd5d5257d5696c82e07bdf35306369787",
            "code_hash": "0xc70a8b00526419826023bcf196852eecdc87406cdff7366234f6387265413c98",
            "hash_type": "type"
          }
        },
        {
          "capacity": "0x386480e7065",
          "lock": {
            "args": "0x51fd95a675716e4ca2472d19510d7bdd232b78012b79c5d76f4298106d8badcb",
            "code_hash": "0x393df3359e33f85010cd65a3c4a4268f72d95ec6b049781a916c680b31ea9a88",
            "hash_type": "type"
          },
          "type": null
        },
        {
          "capacity": "0x395e95a00",
          "lock": {
            "args": "0x51fd95a675716e4ca2472d19510d7bdd232b78012b79c5d76f4298106d8badcb",
            "code_hash": "0x393df3359e33f85010cd65a3c4a4268f72d95ec6b049781a916c680b31ea9a88",
            "hash_type": "type"
          },
          "type": {
            "args": "0xd591ebdc69626647e056e13345fd830c8b876bb06aa07ba610479eb77153ea9f",
            "code_hash": "0xbfa35a9c38a676682b65ade8f02be164d48632281477e36f8dc2f41f79e56bfc",
            "hash_type": "type"
          }
        },
        {
          "capacity": "0x1748741f44",
          "lock": {
            "args": "0x42e11d5e294aa6901d30f0d4fb50fb30222b7f54",
            "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
            "hash_type": "type"
          },
          "type": null
        }
      ],
      "outputs_data": [
        "0x670f0600000000000000000000000000",
        "0x1e150000000000000000000000000000000000000000000000000000000000000000000000000000006547f1948403000000000000000000007f3fba3fb8d6e000176f7e1ae22e8cd02841dec6a8341dc69aeef46387a20664f707290900000000000000000000000022c758ac050000000000000000000000d7534600000000000000000000000000",
        "0x",
        "0xf7072909000000000000000000000000",
        "0x51fd95a675716e4ca2472d19510d7bdd232b78012b79c5d76f4298106d8badcb"
      ],
      "version": "0x0",
      "witnesses": [
        "0x",
        "0x",
        "0x",
        "0x",
        "0x5500000010000000550000005500000041000000ab4959c9e48846de608452b86bea419832807cd778b75f18c0d5f381428512ad3c1e02799bb7cb074f24c1108f0ad15f5b85f25d6e53a4378a52ec19aeccf86100"
      ]
    }"#;
}
