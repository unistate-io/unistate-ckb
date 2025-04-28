use ckb_types::{H256, packed};
use molecule::{bytes::Buf as _, prelude::Entity as _};
use sea_orm::ActiveValue::Set;
use tokio::sync::mpsc;
use tracing::error;
use utils::network::NetworkType;

use crate::{database::Operations, entity::addresses, schemas::action};

pub fn to_timestamp_naive(timestamp: u64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_millis(timestamp as i64)
        .expect("Invalid timestamp")
        .naive_utc()
}

impl action::AddressUnion {
    pub fn from_json_script(script: ckb_jsonrpc_types::Script) -> Self {
        use ckb_types::prelude::Entity;
        Self::Script(action::Script::new_unchecked(
            packed::Script::from(script).as_bytes(),
        ))
    }

    pub fn to_string(&self, network: NetworkType) -> Option<String> {
        let script_bytes = self.script().as_bytes();
        match utils::address::script_bytes_to_address(script_bytes.as_ref(), network) {
            Ok(s) => Some(s),
            Err(e) => {
                error!(
                    "Failed to convert script {:?} to address: {:?}",
                    script_bytes, e
                );
                None
            }
        }
    }

    pub fn script(&self) -> &action::Script {
        match self {
            action::AddressUnion::Script(script) => script,
        }
    }
}

pub fn upsert_address(
    address_union: &action::AddressUnion,
    network: NetworkType,
    block_number: u64,
    tx_hash: &H256,
    timestamp: u64,
    op_sender: mpsc::UnboundedSender<Operations>,
) -> anyhow::Result<String> {
    let Some(address_id_str) = address_union.to_string(network) else {
        return Err(anyhow::anyhow!(
            "Failed to convert script to address string. Script bytes: {:?}, Network: {:?}",
            address_union.script().as_bytes(),
            network
        ));
    };

    let script = address_union.script();
    let address_model = addresses::ActiveModel {
        address_id: Set(address_id_str.clone()),
        script_code_hash: Set(script.code_hash().raw_data().to_vec()),
        script_hash_type: Set(script.hash_type().as_bytes().get_u8() as i16),
        script_args: Set(script.args().raw_data().to_vec()),
        first_seen_block_number: Set(block_number as i64),
        first_seen_tx_hash: Set(tx_hash.0.to_vec()),
        first_seen_tx_timestamp: Set(to_timestamp_naive(timestamp)),
    };

    op_sender
        .send(Operations::UpsertAddress(address_model))
        .map_err(|e| anyhow::anyhow!("Failed to send UpsertAddress operation: {}", e))?;

    Ok(address_id_str)
}

pub fn script_hash_type_to_byte(hash_type: ckb_jsonrpc_types::ScriptHashType) -> packed::Byte {
    let hash_type_val: ckb_types::core::ScriptHashType = hash_type.into();
    hash_type_val.into()
}
