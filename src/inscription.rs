use bigdecimal::{num_bigint::BigInt, BigDecimal};
use ckb_jsonrpc_types::{Script, TransactionView};
use ckb_sdk::NetworkType;
use ckb_types::prelude::Entity as _;
use ckb_types::{packed, H256};
use molecule::prelude::{Builder as _, Entity as _, Reader as _};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator as _};
use sea_orm::Set;
use std::convert::TryInto;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    constants::Constants, database::Operations, entity::token_info, schemas::action,
    spore::upsert_address,
};

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

impl From<Script> for action::Script {
    fn from(json: Script) -> Self {
        let Script {
            args,
            code_hash,
            hash_type,
        } = json;
        let hash_type: ckb_types::core::ScriptHashType = hash_type.into();
        action::Script::new_builder()
            .args(action::Bytes::new_unchecked(args.into_bytes()))
            .code_hash(
                action::Byte32::from_slice(code_hash.as_bytes())
                    .expect("impossible: fail to pack H256"),
            )
            .hash_type(Into::<u8>::into(hash_type).into())
            .build()
    }
}

fn calc_xudt_type_script(inscription_info_script: Script, constants: Constants) -> action::Script {
    let owner_script = generate_owner_script(inscription_info_script, constants);
    let args = action::Bytes::from_slice(&blake2b_256(owner_script.as_reader().as_slice()))
        .expect("impossible: fail to pack Bytes");
    action::Script::from(constants.xudt_type_script())
        .as_builder()
        .args(args)
        .build()
}

fn generate_owner_script(inscription_info_script: Script, constants: Constants) -> action::Script {
    let packed_script = action::Script::from(inscription_info_script);
    let args = action::Bytes::from_slice(&blake2b_256(packed_script.as_reader().as_slice()))
        .expect("impossible: fail to pack Bytes");
    action::Script::from(constants.inscription_type_script())
        .as_builder()
        .args(args)
        .build()
}

fn blake2b_256(data: &[u8]) -> [u8; 32] {
    ckb_hash::blake2b_256(data)
}

#[cfg(test)]
mod tests {
    use hex::encode;

    use super::*;

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
