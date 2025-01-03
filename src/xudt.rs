use std::collections::HashMap;

use bigdecimal::num_bigint::BigInt;
use ckb_jsonrpc_types::{CellOutput, JsonBytes, TransactionView};
use ckb_sdk::{util::blake160, NetworkType};
use ckb_types::{H160, H256};
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
    database::Operations,
    entity::{token_info, transaction_outputs_status, xudt_cell},
    schemas::{
        action, blockchain,
        xudt_rce::{self, ScriptVec, XudtData},
    },
    unique::{decode_token_info_bytes, TokenInfo},
};
use constants::Constants;

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

    let type_id = crate::spore::upsert_address(
        &action::AddressUnion::from_json_script(type_script),
        network,
        op_sender.clone(),
    )?;

    let lock_id = crate::spore::upsert_address(
        &action::AddressUnion::from_json_script(lock_script),
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

struct InputOutPoint {
    input_transaction_hash: H256,
    input_transaction_index: usize,
}

impl InputOutPoint {
    fn new(current_tx_hash: H256, current_tx_index: usize) -> Self {
        Self {
            input_transaction_hash: current_tx_hash,
            input_transaction_index: current_tx_index,
        }
    }
}

fn update_xudt_cell(
    output_tx_hash: H256,
    output_index: usize,
    input_outpoint: InputOutPoint,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let xudt_cell = transaction_outputs_status::ActiveModel {
        output_transaction_hash: Set(output_tx_hash.0.to_vec()),
        output_transaction_index: Set(output_index as i32),
        consumed_input_transaction_hash: Set(Some(
            input_outpoint.input_transaction_hash.0.to_vec(),
        )),
        consumed_input_transaction_index: Set(Some(input_outpoint.input_transaction_index as i32)),
    };

    op_sender.send(Operations::UpdateXudtCell(xudt_cell))?;

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
        type_id: Set(type_id),
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        decimal: Set(decimal as i16),
        name: Set(name),
        symbol: Set(symbol),
        inscription_id: Set(None),
        udt_hash: Set(None),
        expected_supply: Set(None),
        mint_limit: Set(None),
        mint_status: Set(None),
    };

    debug!("token info: {token_info:?}");

    op_sender.send(Operations::UpsertTokenInfo(token_info))?;

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

    let (raw_onwer_lock_script_hash, raw_xudt_args) = o
        .type_
        .as_ref()
        .and_then(|tp| tp.args.as_bytes().split_at_checked(32))
        .unzip();
    debug!(
        "Raw owner lock script hash: {:?}",
        raw_onwer_lock_script_hash
    );
    debug!("Raw Xudt args: {:?}", raw_xudt_args);

    let owner_lock_script_hash =
        raw_onwer_lock_script_hash.map(|raw| std::array::from_fn::<u8, 32, _>(|i| raw[i]));
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

pub struct XudtIndexer {
    txs: Vec<TransactionView>,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl XudtIndexer {
    pub fn new(
        txs: Vec<TransactionView>,
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
            .try_for_each(|tx| index_xudt(tx, network, constants, op_sender.clone()))?;

        Ok(())
    }
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
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    debug!("Indexing transaction: {:?}", tx);

    let xudt_witness_input = process_witnesses::<true>(&tx);
    debug!("XudtWitnessInput: {:?}", xudt_witness_input);

    let xudt_witness_output = process_witnesses::<false>(&tx);
    debug!("XudtWitnessOutput: {:?}", xudt_witness_output);

    let token_data = tx
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
                .map(|t| constants.is_xudt_type(t))
                .unwrap_or(false)
            {
                debug!("Output is XUDT type");
                let xudt = parse_xudt(p, &xudt_witness_input);
                xudt.map(|xudt| (XudtParseResult::Xudt(xudt), idx))
            } else if p
                .0
                .type_
                .as_ref()
                .map(|t| constants.is_unique_type(t))
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
        .try_fold_with(
            TokenData::new(),
            |mut token_data, (xudt, index)| -> anyhow::Result<TokenData> {
                match xudt {
                    XudtParseResult::XudtInfo(info) => {
                        token_data.info = Some((info, index));
                    }
                    XudtParseResult::Xudt(xudt) => {
                        let xudt_type_id =
                            upsert_xudt(xudt, network, tx.hash.clone(), index, op_sender.clone())?;
                        token_data.token_id = Some(xudt_type_id);
                    }
                }
                debug!("token data: {token_data:?}");

                Ok(token_data)
            },
        )
        .try_reduce(TokenData::new, |pre, new| Ok(pre.merge(new)))?;

    if let Some(((info, index), token_id)) = token_data.zip() {
        upsert_token_info(info, tx.hash.clone(), index, token_id, op_sender.clone())?;
    }

    tx.inner.inputs.into_par_iter().enumerate().try_for_each(
        |(input_index, input)| -> anyhow::Result<()> {
            let previous_tx_hash = input.previous_output.tx_hash.clone();
            let previous_tx_index = input.previous_output.index.value() as usize;

            update_xudt_cell(
                previous_tx_hash,
                previous_tx_index,
                InputOutPoint::new(tx.hash.clone(), input_index),
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
        index_xudt(tx, ckb_sdk::NetworkType::Mainnet, constants, sender).unwrap();
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
        "index": "0x0",
        "tx_hash": "0x621a6f38de3b9f453016780edac3b26bfcbfa3e2ecb47c2da275471a5d3ed165"
      }
    },
    {
      "dep_type": "code",
      "out_point": {
        "index": "0x0",
        "tx_hash": "0x67524c01c0cb5492e499c7c7e406f2f9d823e162d6b0cf432eacde0c9808c2ad"
      }
    }
  ],
  "hash": "0x490cd47d7491b8dcb74f22bd7607b176bf7dbe13d4cc9c2d0f50dc7208082f6d",
  "header_deps": [
    "0xf9325d38528244aacb55fd68d714e205e41e2fb62887805fca44345495c78267"
  ],
  "inputs": [
    {
      "previous_output": {
        "index": "0x1",
        "tx_hash": "0x3cb661e0cced2ef2012b0722cbe225c9c38fe5844b27cdd7a341c8a94b754a11"
      },
      "since": "0x0"
    },
    {
      "previous_output": {
        "index": "0x2",
        "tx_hash": "0x3cb661e0cced2ef2012b0722cbe225c9c38fe5844b27cdd7a341c8a94b754a11"
      },
      "since": "0x0"
    }
  ],
  "outputs": [
    {
      "capacity": "0x3663a5200",
      "lock": {
        "args": "0xd096cb29e2f68a85a46bd6bf6cbee6327959ba64",
        "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
        "hash_type": "type"
      },
      "type": {
        "args": "0xb73b6ab39d79390c6de90a09c96b290c331baf1798ed6f97aed02590929734e800000080",
        "code_hash": "0x50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95",
        "hash_type": "data1"
      }
    },
    {
      "capacity": "0x2e90edd00",
      "lock": {
        "args": "0x0000000000000000000000000000000000000000",
        "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
        "hash_type": "type"
      },
      "type": {
        "args": "0x4c9354a30a86e59779d4b225249c91b8fe7c7c75",
        "code_hash": "0x2c8c11c985da60b0a330c61a85507416d6382c130ba67f0c47ab071e00aec628",
        "hash_type": "data1"
      }
    },
    {
      "capacity": "0x9ec17c2a92",
      "lock": {
        "args": "0xd096cb29e2f68a85a46bd6bf6cbee6327959ba64",
        "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
        "hash_type": "type"
      },
      "type": null
    }
  ],
  "outputs_data": [
    "0x77b56d4e180900000000000000000000",
    "0x080469434b420469434b42",
    "0x"
  ],
  "version": "0x0",
  "witnesses": [
    "0x55000000100000005500000055000000410000008b0f2f5b9779bdeeeb691eb3590a8e50da16d1e4c6030fe823e681b533f5b50d4b4321b29d5a2139b2b297de00f7fc6d0f034a9581e5e2c34b67e4164d5302c501"
  ]
}"#;
}
