use bigdecimal::{num_bigint::BigInt, BigDecimal};
use ckb_jsonrpc_types::TransactionView;
use ckb_sdk::NetworkType;
use ckb_types::prelude::Entity as _;
use ckb_types::{packed, H256};
use hex::encode;
use molecule::prelude::Entity as _;
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
    network: ckb_sdk::NetworkType,
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
                    upsert_inscription_info(
                        info,
                        tx.hash.clone(),
                        idx,
                        type_id,
                        op_sender.clone(),
                    )?;
                }
            }
            Ok(())
        })?;

    Ok(())
}

fn upsert_inscription_info(
    inscription_info: InscriptionInfo,
    tx_hash: H256,
    index: usize,
    type_id: String,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<()> {
    let InscriptionInfo {
        decimal,
        name,
        symbol,
        xudt_hash,
        max_supply,
        mint_limit,
        mint_status,
    } = inscription_info;

    let token_info = token_info::ActiveModel {
        type_id: Set(type_id),
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

#[cfg(test)]
fn serialize_inscription_info(info: &InscriptionInfo) -> String {
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

#[derive(Error, Debug)]
pub enum InscriptionError {
    #[error("Invalid UTF-8 in name")]
    InvalidUtf8InName,
    #[error("Invalid UTF-8 in symbol")]
    InvalidUtf8InSymbol,
    #[error("Invalid xudt_hash length")]
    InvalidXudtHashLength,
    #[error("Invalid max_supply")]
    InvalidMaxSupply,
    #[error("Invalid mint_limit")]
    InvalidMintLimit,
}

fn deserialize_inscription_info(data: &[u8]) -> Result<InscriptionInfo, InscriptionError> {
    let mut cursor = 0;

    let decimal = data[cursor];
    cursor += 1;

    let name_len = data[cursor] as usize;
    cursor += 1;
    let name = std::str::from_utf8(&data[cursor..cursor + name_len])
        .map_err(|_| InscriptionError::InvalidUtf8InName)?
        .to_string();
    cursor += name_len;

    let symbol_len = data[cursor] as usize;
    cursor += 1;
    let symbol = std::str::from_utf8(&data[cursor..cursor + symbol_len])
        .map_err(|_| InscriptionError::InvalidUtf8InSymbol)?
        .to_string();
    cursor += symbol_len;

    let xudt_hash: [u8; 32] = data[cursor..cursor + 32]
        .try_into()
        .map_err(|_| InscriptionError::InvalidXudtHashLength)?;
    cursor += 32;

    let max_supply = u128::from_le_bytes(
        data[cursor..cursor + 16]
            .try_into()
            .map_err(|_| InscriptionError::InvalidMaxSupply)?,
    ) / 10u128.pow(decimal as u32);
    cursor += 16;

    let mint_limit = u128::from_le_bytes(
        data[cursor..cursor + 16]
            .try_into()
            .map_err(|_| InscriptionError::InvalidMintLimit)?,
    ) / 10u128.pow(decimal as u32);

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

fn u8_to_hex(value: u8) -> String {
    format!("{:02x}", value)
}

fn u128_to_le(value: u128) -> String {
    encode(value.to_le_bytes())
}

fn utf8_to_hex(s: &str) -> String {
    encode(s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

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
