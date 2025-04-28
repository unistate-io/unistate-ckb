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
          "dep_type": "dep_group",
          "out_point": {
            "index": "0x1",
            "tx_hash": "0x71a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x2",
            "tx_hash": "0x10d63a996157d32c01078058000052674ca58d15f921bec7f1dcdac2160eb66b"
          }
        },
        {
          "dep_type": "code",
          "out_point": {
            "index": "0x0",
            "tx_hash": "0xf6a5eef65101899db9709c8de1cc28f23c1bee90d857ebe176f6647ef109e20d"
          }
        }
      ],
      "hash": "0x8954d5b572cefdb6f71952ae4f67e97aa447e25681b1c0bf90bb6627cebfe5ce",
      "header_deps": [],
      "inputs": [
        {
          "previous_output": {
            "index": "0x0",
            "tx_hash": "0xd34df609d1d8c53f15224b64bafa19a9c8faae8ff958c7b854fa36a64e79215e"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x0",
            "tx_hash": "0x6a02536a099046d0c8e1509140451649d7b0c85181d45e8aa1712723e7d2dc7d"
          },
          "since": "0x0"
        },
        {
          "previous_output": {
            "index": "0x1",
            "tx_hash": "0x6a02536a099046d0c8e1509140451649d7b0c85181d45e8aa1712723e7d2dc7d"
          },
          "since": "0x0"
        }
      ],
      "outputs": [
        {
          "capacity": "0x4a817c800",
          "lock": {
            "args": "0xe333ae79a46b69931fb98d06a6ccc802364bc728",
            "code_hash": "0x5c5069eb0857efc65e1bca0c07df34c31663b3622fd3876c876320fc9634e2a8",
            "hash_type": "type"
          },
          "type": {
            "args": "0xeda3f8814610a35340f424405c469efb426b3987bd26b9da27f77272f394fec6",
            "code_hash": "0x00000000000000000000000000000000000000000000000000545950455f4944",
            "hash_type": "type"
          }
        },
        {
          "capacity": "0x4a817c800",
          "lock": {
            "args": "0x256d995c234c11788f16c8ac57ed08aae432a1aa629fbf3011811e52f6ae814f",
            "code_hash": "0x2df53b592db3ae3685b7787adcfef0332a611edb83ca3feca435809964c3aff2",
            "hash_type": "data1"
          },
          "type": null
        },
        {
          "capacity": "0x360447100",
          "lock": {
            "args": "0x0001dd93812b8c0bcaf8296530ae02a6c9b8326fe84e",
            "code_hash": "0xd00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e318323",
            "hash_type": "type"
          },
          "type": {
            "args": "0xd591ebdc69626647e056e13345fd830c8b876bb06aa07ba610479eb77153ea9f",
            "code_hash": "0xbfa35a9c38a676682b65ade8f02be164d48632281477e36f8dc2f41f79e56bfc",
            "hash_type": "type"
          }
        }
      ],
      "outputs_data": [
        "0x",
        "0x",
        "0x7c10bf77000000000000000000000000"
      ],
      "version": "0x0",
      "witnesses": [
        "0xd600000010000000d6000000d6000000c200000000000203fb23f3eed8d935ddaed30c95d3f81af61b0064726e6d407958bea4b1fd8452541b5ff619e3ea87ff42cf9a6900b2e59f4dd67e51c9c68939170e4323c5f0d0013ad0130170187ee4a00dd9cada7128d171b1e37581056c37f9d75c3070aacd19d67206cdfbaec8001b19f3fa8d8520fb7ff691f487609dedbc848c6a00f3ac3800b311142f53c9fbc3820485c7f3976c038cd3c117d50c84f400bcf5ca535ae234ee417594cc99058861861ebe997f022a83fa03b6d681728667df13b800",
        "0x",
        "0x"
      ]
    }"#;
}
