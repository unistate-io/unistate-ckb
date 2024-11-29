use bigdecimal::{num_bigint::BigInt, BigDecimal};
use ckb_jsonrpc_types::{Script, TransactionView};
use ckb_sdk::NetworkType;
use ckb_types::prelude::{Builder as _, Entity as _};
use ckb_types::{packed, H256};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator as _};
use sea_orm::Set;
use std::convert::TryInto;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{database::Operations, entity::token_info, schemas::action, spore::upsert_address};

use constants::Constants;

pub struct InscriptionInfoIndexer {
    txs: Vec<TransactionView>,
    network: NetworkType,
    op_sender: mpsc::UnboundedSender<Operations>,
}

impl InscriptionInfoIndexer {
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
            .try_for_each(|tx| index_inscription_info(tx, network, constants, op_sender.clone()))?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct InscriptionInfo {
    decimal: u8,
    name: String,
    symbol: String,
    xudt_hash: [u8; 32],
    max_supply: u128,
    mint_limit: u128,
    mint_status: u8,
}

fn index_inscription_info(
    tx: TransactionView,
    network: NetworkType,
    constants: Constants,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    tx.inner
        .outputs
        .into_par_iter()
        .zip(tx.inner.outputs_data.into_par_iter())
        .enumerate()
        .try_for_each(|(idx, (output, data))| -> anyhow::Result<()> {
            if let Some(tp) = output.type_ {
                let inscription_info = constants.inscription_info_type_script();
                if tp.code_hash.eq(&inscription_info.code_hash)
                    && tp.hash_type.eq(&inscription_info.hash_type)
                {
                    let info = deserialize_inscription_info(data.as_bytes())?;
                    let type_id = upsert_address(
                        &action::AddressUnion::Script(action::Script::new_unchecked(
                            packed::Script::from(tp).as_bytes(),
                        )),
                        network,
                        op_sender.clone(),
                    )?;
                    upsert_inscription_info(UpsertInscriptionInfoParams {
                        inscription_info: info,
                        inscription_info_script: inscription_info,
                        network,
                        constants,
                        tx_hash: tx.hash.clone(),
                        index: idx,
                        type_id,
                        op_sender: op_sender.clone(),
                    })?;
                }
            }
            Ok(())
        })?;

    Ok(())
}

#[derive(Debug)]
struct UpsertInscriptionInfoParams {
    inscription_info: InscriptionInfo,
    inscription_info_script: Script,
    network: NetworkType,
    constants: Constants,
    tx_hash: H256,
    index: usize,
    type_id: String,
    op_sender: mpsc::UnboundedSender<Operations>,
}

fn upsert_inscription_info(params: UpsertInscriptionInfoParams) -> anyhow::Result<()> {
    let UpsertInscriptionInfoParams {
        inscription_info,
        inscription_info_script,
        network,
        constants,
        tx_hash,
        index,
        type_id,
        op_sender,
    } = params;

    let InscriptionInfo {
        decimal,
        name,
        symbol,
        xudt_hash,
        max_supply,
        mint_limit,
        mint_status,
    } = inscription_info;

    let xudt_type_script = action::AddressUnion::Script(action::Script::new_unchecked(
        calc_xudt_type_script(inscription_info_script, constants).as_bytes(),
    ));

    let inscription_id = upsert_address(&xudt_type_script, network, op_sender.clone())?;

    let token_info = token_info::ActiveModel {
        type_id: Set(type_id),
        inscription_id: Set(Some(inscription_id)),
        transaction_hash: Set(tx_hash.0.to_vec()),
        transaction_index: Set(index as i32),
        decimal: Set(decimal as i16),
        name: Set(name),
        symbol: Set(symbol),
        udt_hash: Set(Some(xudt_hash.to_vec())),
        expected_supply: Set(Some(BigDecimal::new(BigInt::from(max_supply), 0))),
        mint_limit: Set(Some(BigDecimal::new(BigInt::from(mint_limit), 0))),
        mint_status: Set(Some(mint_status as i16)),
    };

    debug!("token info: {token_info:?}");

    op_sender.send(Operations::UpsertTokenInfo(token_info))?;

    Ok(())
}

#[derive(Error, Debug)]
pub enum InscriptionError {
    #[error("Invalid UTF-8 in name")]
    InvalidUtf8InName,
    #[error("Invalid xudt_hash length")]
    InvalidXudtHashLength,
    #[error("Invalid max_supply")]
    InvalidMaxSupply,
}

fn deserialize_inscription_info(data: &[u8]) -> Result<InscriptionInfo, InscriptionError> {
    let mut cursor = 0;

    let decimal = data[cursor];
    cursor += 1;

    let (name, new_cursor) = read_string(&data[cursor..])?;
    cursor += new_cursor;

    let (symbol, new_cursor) = read_string(&data[cursor..])?;
    cursor += new_cursor;

    let xudt_hash: [u8; 32] = data[cursor..cursor + 32]
        .try_into()
        .map_err(|_| InscriptionError::InvalidXudtHashLength)?;
    cursor += 32;

    let max_supply = read_u128(&data[cursor..], decimal)?;
    cursor += 16;

    let mint_limit = read_u128(&data[cursor..], decimal)?;
    cursor += 16;

    let mint_status = data[cursor];

    Ok(InscriptionInfo {
        decimal,
        name,
        symbol,
        xudt_hash,
        max_supply,
        mint_limit,
        mint_status,
    })
}

fn read_string(data: &[u8]) -> Result<(String, usize), InscriptionError> {
    let len = data[0] as usize;
    let string = std::str::from_utf8(&data[1..1 + len])
        .map_err(|_| InscriptionError::InvalidUtf8InName)?
        .to_string();
    Ok((string, len + 1))
}

fn read_u128(data: &[u8], decimal: u8) -> Result<u128, InscriptionError> {
    let value = u128::from_le_bytes(
        data[..16]
            .try_into()
            .map_err(|_| InscriptionError::InvalidMaxSupply)?,
    ) / 10u128.pow(decimal as u32);
    Ok(value)
}

fn calc_xudt_type_script(
    inscription_info_script: impl Into<packed::Script>,
    constants: Constants,
) -> packed::Script {
    let inscription_info_script = inscription_info_script.into();
    let owner_script = generate_owner_script(&inscription_info_script, constants);

    packed::Script::from(constants.inscription_xudt_type_script())
        .as_builder()
        .args(generate_args(&owner_script))
        .build()
}

fn generate_owner_script(
    inscription_info_script: &packed::Script,
    constants: Constants,
) -> packed::Script {
    packed::Script::from(constants.inscription_type_script())
        .as_builder()
        .args(generate_args(inscription_info_script))
        .build()
}

fn generate_args(script: &packed::Script) -> packed::Bytes {
    packed::Bytes::new_builder()
        .set(
            script
                .calc_script_hash()
                .raw_data()
                .into_iter()
                .map(|s| packed::Byte::new(s))
                .collect(),
        )
        .build()
}

#[cfg(test)]
mod tests {
    use hex::encode;
    use molecule::prelude::Entity as _;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_calc_xudt_type_script() {
        use serde_json::json;

        let constants = Constants::Testnet;
        let info_script: Script = serde_json::from_value(json!({
            "code_hash": "0x50fdea2d0030a8d0b3d69f883b471cab2a29cae6f01923f19cecac0f27fdaaa6",
            "args": "0x720597d6fde35c93b3a30249d5ad5396eab8dbb70acaffbf2f85c0f9ce9b4180",
            "hash_type": "type"
        }))
        .unwrap();

        println!(
            "info: {:#}",
            serde_json::to_string_pretty(&info_script).unwrap()
        );

        let info_script: packed::Script = info_script.into();

        let raw_owner_script = generate_owner_script(&info_script, constants);
        println!("raw owner: {:#}", raw_owner_script);
        println!(
            "raw owner args: {}",
            hex::encode(raw_owner_script.args().raw_data())
        );
        let owner_script: Script = raw_owner_script.into();

        println!(
            "owner_script: {:#}",
            serde_json::to_string_pretty(&owner_script).unwrap()
        );

        println!("owner args: {}", hex::encode(owner_script.args.as_bytes()));

        let xudt_script = calc_xudt_type_script(info_script.clone(), constants);
        println!(
            "xudt hash: {}",
            hex::encode(xudt_script.calc_script_hash().as_bytes())
        );

        let xudt_script: Script = xudt_script.into();

        let expected_xudt_script: Script = serde_json::from_value(json!(
            {
                "code_hash": "0x25c29dc317811a6f6f3985a7a9ebc4838bd388d19d0feeecf0bcd60f6c0975bb",
                "args": "0x0f8a7460a72566976aedaf1cb946fd28a477ea7baec7529892538e4733874240",
                "hash_type": "type"
            }
        ))
        .unwrap();

        let expected_xudt_hash: packed::Script = expected_xudt_script.clone().into();

        println!(
            "expected xudt hash: {}",
            hex::encode(expected_xudt_hash.calc_script_hash().as_bytes())
        );

        println!(
            "our xudt: {:#}",
            serde_json::to_string_pretty(&xudt_script).unwrap()
        );
        println!(
            "expected xudt: {:#}",
            serde_json::to_string_pretty(&expected_xudt_script).unwrap()
        );

        assert_eq!(expected_xudt_script, xudt_script);
    }

    fn serialize_inscription_info(info: &InscriptionInfo) -> String {
        fn u8_to_hex(value: u8) -> String {
            format!("{:02x}", value)
        }

        fn u128_to_le(value: u128) -> String {
            encode(value.to_le_bytes())
        }

        fn utf8_to_hex(s: &str) -> String {
            encode(s.as_bytes())
        }

        let mut ret = u8_to_hex(info.decimal);

        let name = utf8_to_hex(&info.name);
        ret.push_str(&u8_to_hex((name.len() / 2) as u8));
        ret.push_str(&name);

        let symbol = utf8_to_hex(&info.symbol);
        ret.push_str(&u8_to_hex((symbol.len() / 2) as u8));
        ret.push_str(&symbol);

        ret.push_str(&encode(info.xudt_hash));

        let max_supply = info.max_supply * 10u128.pow(info.decimal as u32);
        ret.push_str(&u128_to_le(max_supply));

        let mint_limit = info.mint_limit * 10u128.pow(info.decimal as u32);
        ret.push_str(&u128_to_le(mint_limit));

        ret.push_str(&u8_to_hex(info.mint_status));

        ret
    }

    #[test]
    fn test_serialize_deserialize() {
        let original_info = InscriptionInfo {
            decimal: 8,
            name: "TestToken".to_string(),
            symbol: "TT".to_string(),
            xudt_hash: [0; 32],
            max_supply: 1000000,
            mint_limit: 1000,
            mint_status: 1,
        };

        let serialized = serialize_inscription_info(&original_info);
        let deserialized =
            deserialize_inscription_info(&hex::decode(&serialized).unwrap()).unwrap();

        assert_eq!(original_info, deserialized);
    }
}
