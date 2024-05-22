#![allow(unused)]

use ckb_fixed_hash_core::H256;
use ckb_jsonrpc_types::{CellDep, DepType, JsonBytes, OutPoint, Script, ScriptHashType, Uint32};
use ckb_types::bytes::Bytes;
use hex_literal::hex;

// 常量
pub const CKB_UNIT: u64 = 100000000;
pub const MAX_FEE: u64 = 200000000;
pub const MIN_CAPACITY: u64 = 61 * 100000000;
pub const SECP256K1_WITNESS_LOCK_SIZE: u8 = 65;
pub const BTC_JUMP_CONFIRMATION_BLOCKS: u64 = 6;
pub const RGBPP_TX_WITNESS_MAX_SIZE: usize = 5000;
pub const RGBPP_TX_INPUTS_MAX_LENGTH: usize = 10;

pub const RGBPP_WITNESS_PLACEHOLDER: &str = "0xFF";
pub const RGBPP_TX_ID_PLACEHOLDER: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

const fn convert(value: u32) -> Uint32 {
    struct JsonUint32(u32);
    let src = JsonUint32(value);
    unsafe { std::mem::transmute(src) }
}

const fn const_default_bytes() -> JsonBytes {
    struct DefaultJsonBytes(Bytes);
    const EMPTY: &[u8] = &[];

    let src = DefaultJsonBytes(Bytes::from_static(EMPTY));
    unsafe { std::mem::transmute(src) }
}

macro_rules! define_deps {
    ($($name:ident, $dep_type:expr, $dep_tx_hash:expr, $dep_index:expr);*) => {
        $(
            pub const $name: CellDep = CellDep {
                dep_type: $dep_type,
                out_point: OutPoint {
                    tx_hash: H256(hex!($dep_tx_hash)),
                    index: convert($dep_index),
                }
            };
        )*
    };
}

macro_rules! define_script {
    ($($name:ident, $script_code_hash:expr, $script_hash_type:expr, $script_args:expr);*) => {
        $(
            pub const $name: Script = Script {
                code_hash: H256(hex!($script_code_hash)),
                hash_type: $script_hash_type,
                args: $script_args
            };
        )*
    };
}

macro_rules! define_info_mod {
    ($mod_name:ident, $start_height:expr, $secp256k1_lock_dep:expr, $rgbpp_lock_dep:expr, $rgbpp_lock_config_dep:expr, $btc_time_lock_dep:expr, $btc_time_lock_config_dep:expr, $xudttype_dep:expr, $unique_type_dep:expr, $cluster_type_dep:expr, $spore_type_dep:expr, $rgbpp_lock_script:expr, $btc_time_lock_script:expr, $xudttype_script:expr, $unique_type_script:expr, $cluster_type_script:expr, $spore_type_script:expr) => {
        pub mod $mod_name {
            use super::*;

            pub const DEFAULT_START_HEIGHT: u64 = $start_height;

            define_deps!(
                SECP256_K1_LOCK_DEP,
                DepType::DepGroup,
                $secp256k1_lock_dep,
                0x0;
                RGBPP_LOCK_DEP,
                DepType::Code,
                $rgbpp_lock_dep,
                0x0;
                RGBPP_LOCK_CONFIG_DEP,
                DepType::Code,
                $rgbpp_lock_config_dep,
                0x1;
                BTC_TIME_LOCK_DEP,
                DepType::Code,
                $btc_time_lock_dep,
                0x0;
                BTC_TIME_LOCK_CONFIG_DEP,
                DepType::Code,
                $btc_time_lock_config_dep,
                0x1;
                XUDTTYPE_DEP,
                DepType::Code,
                $xudttype_dep,
                0x0;
                UNIQUE_TYPE_DEP,
                DepType::Code,
                $unique_type_dep,
                0x0;
                CLUSTER_TYPE_DEP,
                DepType::Code,
                $cluster_type_dep,
                0x0;
                SPORE_TYPE_DEP,
                DepType::Code,
                $spore_type_dep,
                0x0
            );

            define_script!(
                RGBPP_LOCK_SCRIPT,
                $rgbpp_lock_script,
                ScriptHashType::Type,
                const_default_bytes();
                BTC_TIME_LOCK_SCRIPT,
                $btc_time_lock_script,
                ScriptHashType::Type,
                const_default_bytes();
                XUDTTYPE_SCRIPT,
                $xudttype_script,
                ScriptHashType::Data1,
                const_default_bytes();
                UNIQUE_TYPE_SCRIPT,
                $unique_type_script,
                ScriptHashType::Data1,
                const_default_bytes();
                CLUSTER_TYPE_SCRIPT,
                $cluster_type_script,
                ScriptHashType::Data1,
                const_default_bytes();
                SPORE_TYPE_SCRIPT,
                $spore_type_script,
                ScriptHashType::Data1,
                const_default_bytes()
            );

            pub fn is_rgbpp_lock_script(script: &Script) -> bool {
                script.code_hash.eq(&RGBPP_LOCK_SCRIPT.code_hash)
                    && script.hash_type.eq(&RGBPP_LOCK_SCRIPT.hash_type)
            }

            pub fn is_btc_time_lock_script(script: &Script) -> bool {
                script.code_hash.eq(&BTC_TIME_LOCK_SCRIPT.code_hash)
                    && script.hash_type.eq(&BTC_TIME_LOCK_SCRIPT.hash_type)
            }

            pub fn get_xudttype_script(script: Option<&Script>) -> Option<&Script> {
                script.filter(|script| {
                    script.code_hash.eq(&XUDTTYPE_SCRIPT.code_hash)
                        && script.hash_type.eq(&XUDTTYPE_SCRIPT.hash_type)
                })
            }

            pub fn get_unique_type_script(script: Option<&Script>) -> Option<&Script> {
                script.filter(|script| {
                    script.code_hash.eq(&UNIQUE_TYPE_SCRIPT.code_hash)
                    && script.hash_type.eq(&UNIQUE_TYPE_SCRIPT.hash_type)
                })
            }

            pub fn get_cluster_type_script(script: Option<&Script>) -> Option<&Script> {
                script.filter(|script| {
                    script.code_hash.eq(&CLUSTER_TYPE_SCRIPT.code_hash)
                    && script.hash_type.eq(&CLUSTER_TYPE_SCRIPT.hash_type)
                })
            }

            pub fn get_spore_type_script(script: Option<&Script>) -> Option<&Script> {
                script.filter(|script| {
                    script.code_hash.eq(&SPORE_TYPE_SCRIPT.code_hash)
                    && script.hash_type.eq(&SPORE_TYPE_SCRIPT.hash_type)
                })
            }
        }
    }
}

define_info_mod! {
    testnet_info,
    12720832,
    // Secp256k1LockDep
    "f8de3bb47d055cdf460d93a2a6e1b05f7432f9777c8c474abf4eec1d4aee5d37",
    // RgbppLockDep
    "f1de59e973b85791ec32debbba08dff80c63197e895eb95d67fc1e9f6b413e00",
    // RgbppLockConfigDep
    "f1de59e973b85791ec32debbba08dff80c63197e895eb95d67fc1e9f6b413e00",
    // BtcTimeLockDep
    "de0f87878a97500f549418e5d46d2f7704c565a262aa17036c9c1c13ad638529",
    // BtcTimeLockConfigDep
    "de0f87878a97500f549418e5d46d2f7704c565a262aa17036c9c1c13ad638529",
    // XUDTTypeDep
    "bf6fb538763efec2a70a6a3dcb7242787087e1030c4e7d86585bc63a9d337f5f",
    // UniqueTypeDep
    "ff91b063c78ed06f10a1ed436122bd7d671f9a72ef5f5fa28d05252c17cf4cef",
    // ClusterTypeDep
    "cebb174d6e300e26074aea2f5dbd7f694bb4fe3de52b6dfe205e54f90164510a",
    // SporeTypeDep
    "5e8d2a517d50fd4bb4d01737a7952a1f1d35c8afc77240695bb569cd7d9d5a1f",
    // RgbppLockScript
    "61ca7a4796a4eb19ca4f0d065cb9b10ddcf002f10f7cbb810c706cb6bb5c3248",
    // BtcTimeLockScript
    "00cdf8fab0f8ac638758ebf5ea5e4052b1d71e8a77b9f43139718621f6849326",
    // XUDTTypeScript
    "25c29dc317811a6f6f3985a7a9ebc4838bd388d19d0feeecf0bcd60f6c0975bb",
    // UniqueTypeScript
    "8e341bcfec6393dcd41e635733ff2dca00a6af546949f70c57a706c0f344df8b",
    // ClusterTypeScript
    "0bbe768b519d8ea7b96d58f1182eb7e6ef96c541fbd9526975077ee09f049058",
    // SporeTypeScript
    "685a60219309029d01310311dba953d67029170ca4848a4ff638e57002130a0d"
}

define_info_mod! {
    mainnet_info,
    11922670,
    // Secp256k1LockDep
    "71a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c",
    // RgbppLockDep
    "04c5c3e69f1aa6ee27fb9de3d15a81704e387ab3b453965adbe0b6ca343c6f41",
    // RgbppLockConfigDep
    "04c5c3e69f1aa6ee27fb9de3d15a81704e387ab3b453965adbe0b6ca343c6f41",
    // BtcTimeLockDep
    "6257bf4297ee75fcebe2654d8c5f8d93bc9fc1b3dc62b8cef54ffe166162e996",
    // BtcTimeLockConfigDep
    "6257bf4297ee75fcebe2654d8c5f8d93bc9fc1b3dc62b8cef54ffe166162e996",
    // XUDTTypeDep
    "c07844ce21b38e4b071dd0e1ee3b0e27afd8d7532491327f39b786343f558ab7",
    // UniqueTypeDep
    "67524c01c0cb5492e499c7c7e406f2f9d823e162d6b0cf432eacde0c9808c2ad",
    // ClusterTypeDep
    "e464b7fb9311c5e2820e61c99afc615d6b98bdefbe318c34868c010cbd0dc938",
    // SporeTypeDep
    "96b198fb5ddbd1eed57ed667068f1f1e55d07907b4c0dbd38675a69ea1b69824",
    // RgbppLockScript
    "bc6c568a1a0d0a09f6844dc9d74ddb4343c32143ff25f727c59edf4fb72d6936",
    // BtcTimeLockScript
    "70d64497a075bd651e98ac030455ea200637ee325a12ad08aff03f1a117e5a62",
    // XUDTTypeScript
    "50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95",
    // UniqueTypeScript
    "2c8c11c985da60b0a330c61a85507416d6382c130ba67f0c47ab071e00aec628",
    // ClusterTypeScript
    "7366a61534fa7c7e6225ecc0d828ea3b5366adec2b58206f2ee84995fe030075",
    // SporeTypeScript
    "4a4dce1df3dffff7f8b2cd7dff7303df3b6150c9788cb75dcf6747247132b9f5"
}
