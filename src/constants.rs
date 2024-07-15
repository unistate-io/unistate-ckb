#![allow(unused)]

use ckb_fixed_hash_core::H256;
use ckb_jsonrpc_types::{CellDep, DepType, JsonBytes, OutPoint, Script, ScriptHashType, Uint32};
use ckb_sdk::NetworkType;
use ckb_types::bytes::Bytes;
use hex_literal::hex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

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

#[derive(Debug, Clone, Copy)]
pub enum Version {
    V0,
    V1,
    V2,
}

macro_rules! define_network_item {
    ($item_type:ty, $name:ident, { $network:ident => $expr:expr $(,)? }) => {
        pub const fn $name(self) -> Option<$item_type> {
            match self {
                Self::$network => Some($expr),
                _ => None
            }
        }
    };

    ($item_type:ty, $name:ident, { $(($network:ident, $version:ident) => $expr:expr),+ $(,)? }) => {
        pub const fn $name(self, version: Version) -> Option<$item_type> {
            match (self, version) {
                $((Self::$network, Version::$version) => Some($expr),)*
                _ => None
            }
        }
    };

    ($item_type:ty, $name:ident, { $($network:ident => $expr:expr),+ $(,)? }) => {
        pub const fn $name(self) -> $item_type {
            match self {
                $(Self::$network => $expr),*
            }
        }
    };
}

macro_rules! define_cell_dep {

    ($name:ident, $dep_type:expr, $index:expr, { $($network:ident => $hash:expr),+ $(,)? }) => {
        define_network_item!(CellDep, $name, {
            $($network => define_cell_dep!(@internal $dep_type, $hash, $index),)*
        });
    };

    ($name:ident, $dep_type:expr, $index:expr, { $(($network:ident, $version:ident) => $hash:expr),+ $(,)? }) => {
        define_network_item!(CellDep, $name, {
            $(($network, $version) => define_cell_dep!(@internal $dep_type, $hash, $index),)*
        });
    };


    (@internal $dep_type:expr, $dep_tx_hash:expr, $dep_index:expr) => {
        CellDep {
            dep_type: $dep_type,
            out_point: OutPoint {
                tx_hash: H256(hex!($dep_tx_hash)),
                index: convert($dep_index),
            },
        }
    };
}

macro_rules! define_script {
    ($name:ident, $script_hash_type:expr, { $($network:ident => $hash:expr),+ $(,)? }) => {
        define_network_item!(Script, $name, {
            $($network => define_script!(@internal $script_hash_type, $hash, const_default_bytes()),)*
        });
    };

    ($name:ident, $script_hash_type:expr, { $(($network:ident, $version:ident) => $hash:expr),+ $(,)? }) => {
        define_network_item!(Script, $name, {
            $(($network, $version) => define_script!(@internal $script_hash_type, $hash, const_default_bytes()),)*
        });
    };

    (@internal $script_hash_type:expr, $script_code_hash:expr, $script_args:expr) => {
        Script {
            code_hash: H256(hex!($script_code_hash)),
            hash_type: $script_hash_type,
            args: $script_args,
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Constants {
    Testnet,
    Mainnet,
}

impl Constants {
    pub fn from_config(network: NetworkType) -> Self {
        match network {
            NetworkType::Mainnet => Self::Mainnet,
            NetworkType::Testnet => Self::Testnet,
            _ => unimplemented!(),
        }
    }
}

macro_rules! define_versioned_deps {
    ($self:ident, $($version:ident),*) => {
        [
            $(
                $self.spore_type_dep(Version::$version),
                $self.cluster_type_dep(Version::$version),
                $self.cluster_proxy_dep(Version::$version),
                $self.cluster_agent_dep(Version::$version),
            )*
            $self.lua_dep(),
            $self.mutant_dep(),
        ]
    };
}

impl Constants {
    pub const fn spore_deps(self) -> [Option<CellDep>; 14] {
        define_versioned_deps!(self, V0, V1, V2)
    }

    pub fn is_spore(self, cd: &CellDep) -> bool {
        self.spore_deps()
            .into_par_iter()
            .flatten()
            .any(|dep| dep.out_point.eq(&cd.out_point))
    }

    define_script!(
        mutant_type_script,
        ScriptHashType::Data1,
        {
            Testnet => "5ff1a403458b436ea4b2ceb72f1fa70a6507968493315b646f5302661cb68e57",
        }
    );

    define_script!(
        lua_type_script,
        ScriptHashType::Data1,
        {
            Testnet => "ed08faee8c29b7a7c29bd9d495b4b93cc207bd70ca93f7b356f39c677e7ab0fc",
        }
    );

    define_cell_dep!(
        mutant_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "9b2098e5b6f575b2fd34ffd0212bc1c96e1f9e86fcdb146511849c174dfe0d02",
        }
    );

    define_cell_dep!(
        lua_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "8fb7170a58d631250dabd0f323a833f4ad2cfdd0189f45497e62beb8409e7a0c",
        }
    );

    define_script!(
        joy_id_lock_script,
        ScriptHashType::Type,
        {
            Testnet => "d23761b364210735c19c60561d213fb3beae2fd6172743719eff6920e020baac",
            Mainnet => "d00c84f0ec8fd441c38bc3f87a371f547190f2fcff88e642bc5bf54b9e318323"
        }
    );

    define_cell_dep!(
        joy_id_lock_dep,
        DepType::DepGroup,
        0x0,
        {
            Testnet => "4dcf3f3b09efac8995d6cbee87c5345e812d310094651e0c3d9a730f32dc9263",
            Mainnet => "f05188e5f3a6767fc4687faf45ba5f1a6e25d3ada6129dae8722cb282f262493"
        }
    );

    define_script!(
        cota_type_script,
        ScriptHashType::Type,
        {
            Testnet => "89cd8003a0eaf8e65e0c31525b7d1d5c1becefd2ea75bb4cff87810ae37764d8",
            Mainnet => "1122a4fb54697cf2e6e3a96c9d80fd398a936559b90954c6e88eb7ba0cf652df"
        }
    );

    define_cell_dep!(
        cota_type_dep,
        DepType::DepGroup,
        0x0,
        {
            Testnet => "636a786001f87cb615acfcf408be0f9a1f077001f0bbc75ca54eadfe7e221713",
            Mainnet => "abaa25237554f0d6c586dc010e7e85e6870bcfd9fb8773257ecacfbe1fd738a0"
        }
    );

    define_script!(
        inscription_info_type_script,
        ScriptHashType::Type,
        {
            Testnet => "50fdea2d0030a8d0b3d69f883b471cab2a29cae6f01923f19cecac0f27fdaaa6",
            Mainnet => "5c33fc69bd72e895a63176147c6ab0bb5758d1c7a32e0914f99f9ec1bed90d41"
        }
    );

    define_cell_dep!(
        inscription_info_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "7bf3899cf41879ed0319bf5312c9db5bf5620fff9ebe59556c261c48f0369054",
            Mainnet => "372ba2b2122214855b4a2c34265d3e311798287169bdbd1af50afd49a11a33ab"
        }
    );

    define_script!(
        inscription_type_script,
        ScriptHashType::Type,
        {
            Testnet => "3a241ceceede72a5f55c8fb985652690f09a517d6c9070f0df0d3572fa03fb70",
            Mainnet => "7490970e6af9b9fe63fc19fc523a12b2ec69027e6ae484edffb97334f74e8c97"
        }
    );

    define_cell_dep!(
        inscription_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "9101c1db97bc2013ace8ebd0718723be3d0e3748f2ef22bd7f1dbda0ca75d7d0",
            Mainnet => "d0576d843c36dbad70b1ae92e8600fcdbaf14d6606eb0beadb71a5b3b52dfff7"
        }
    );

    define_script!(
        rebase_type_script,
        ScriptHashType::Type,
        {
            Testnet => "93043b66bb20797caad0deacaadbada5e58f0893d770ecdddb8806aff8877e29",
            Mainnet => "da8fbf9b8497c0a34fad89377026e51128817c60167a2b7673b27c1a3f2a331f"
        }
    );

    define_cell_dep!(
        rebase_type_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "64ba52275a3012605dda1df52e872b3a1d99009d3b3728beffdfa36d4bdd14b7",
            Mainnet => "28edf58d9610cee1536eaf6c151aa31ff91b23cb507ee1dad82a1c56b9250835"
        }
    );

    define_script!(
        xins_type_script,
        ScriptHashType::Type,
        {
            Testnet => "27762139c09452b5d4be6f34720db031a2ab348679a4612b6863a6907a6244ed",
            Mainnet => "27762139c09452b5d4be6f34720db031a2ab348679a4612b6863a6907a6244ed"
        }
    );

    define_cell_dep!(
        xins_type_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "94dccee46e2636a729119abb5a74641d12cc8f64e09378d3abceeac807ea7223",
            Mainnet => "94dccee46e2636a729119abb5a74641d12cc8f64e09378d3abceeac807ea7223"
        }
    );

    define_cell_dep!(
        secp256k1_lock_dep,
        DepType::DepGroup,
        0x0,
        {
            Testnet => "f8de3bb47d055cdf460d93a2a6e1b05f7432f9777c8c474abf4eec1d4aee5d37",
            Mainnet => "71a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c"
        }
    );

    define_cell_dep!(
        rgbpp_lock_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "f1de59e973b85791ec32debbba08dff80c63197e895eb95d67fc1e9f6b413e00",
            Mainnet => "04c5c3e69f1aa6ee27fb9de3d15a81704e387ab3b453965adbe0b6ca343c6f41"
        }
    );

    define_cell_dep!(
        rgbpp_lock_config_dep,
        DepType::Code,
        0x1,
        {
            Testnet => "f1de59e973b85791ec32debbba08dff80c63197e895eb95d67fc1e9f6b413e00",
            Mainnet => "04c5c3e69f1aa6ee27fb9de3d15a81704e387ab3b453965adbe0b6ca343c6f41"
        }
    );

    define_cell_dep!(
        btc_time_lock_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "de0f87878a97500f549418e5d46d2f7704c565a262aa17036c9c1c13ad638529",
            Mainnet => "6257bf4297ee75fcebe2654d8c5f8d93bc9fc1b3dc62b8cef54ffe166162e996"
        }
    );

    define_cell_dep!(
        btc_time_lock_config_dep,
        DepType::Code,
        0x1,
        {
            Testnet => "de0f87878a97500f549418e5d46d2f7704c565a262aa17036c9c1c13ad638529",
            Mainnet => "6257bf4297ee75fcebe2654d8c5f8d93bc9fc1b3dc62b8cef54ffe166162e996"
        }
    );

    define_script!(
        xudt_type_script,
        ScriptHashType::Data1,
        {
            Testnet => "25c29dc317811a6f6f3985a7a9ebc4838bd388d19d0feeecf0bcd60f6c0975bb",
            Mainnet => "50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95"
        }
    );

    define_cell_dep!(
        xudt_type_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "bf6fb538763efec2a70a6a3dcb7242787087e1030c4e7d86585bc63a9d337f5f",
            Mainnet => "c07844ce21b38e4b071dd0e1ee3b0e27afd8d7532491327f39b786343f558ab7"
        }
    );

    define_cell_dep!(
        unique_type_dep,
        DepType::Code,
        0x0,
        {
            Testnet => "ff91b063c78ed06f10a1ed436122bd7d671f9a72ef5f5fa28d05252c17cf4cef",
            Mainnet => "67524c01c0cb5492e499c7c7e406f2f9d823e162d6b0cf432eacde0c9808c2ad"
        }
    );

    define_script!(
        unique_type_script,
        ScriptHashType::Data1,
        {
            Testnet => "8e341bcfec6393dcd41e635733ff2dca00a6af546949f70c57a706c0f344df8b",
            Mainnet => "2c8c11c985da60b0a330c61a85507416d6382c130ba67f0c47ab071e00aec628"
        }
    );

    define_script!(
        cluster_type_script,
        ScriptHashType::Data1,
        {
            (Testnet,V0) => "598d793defef36e2eeba54a9b45130e4ca92822e1d193671f490950c3b856080",
            (Testnet,V1) => "fbceb70b2e683ef3a97865bb88e082e3e5366ee195a9c826e3c07d1026792fcd",
            (Testnet,V2) => "0bbe768b519d8ea7b96d58f1182eb7e6ef96c541fbd9526975077ee09f049058",
            (Mainnet,V1) => "7366a61534fa7c7e6225ecc0d828ea3b5366adec2b58206f2ee84995fe030075"
        }
    );

    define_script!(
        spore_type_script,
        ScriptHashType::Data1,
        {
            (Testnet,V0) => "bbad126377d45f90a8ee120da988a2d7332c78ba8fd679aab478a19d6c133494",
            (Testnet,V1) => "5e063b4c0e7abeaa6a428df3b693521a3050934cf3b0ae97a800d1bc31449398",
            (Testnet,V2) => "685a60219309029d01310311dba953d67029170ca4848a4ff638e57002130a0d",
            (Mainnet,V1) => "4a4dce1df3dffff7f8b2cd7dff7303df3b6150c9788cb75dcf6747247132b9f5"
        }
    );

    define_cell_dep!(
        spore_type_dep,
        DepType::Code,
        0x0,
        {
            (Testnet,V0) => "fd694382e621f175ddf81ce91ce2ecf8bfc027d53d7d31b8438f7d26fc37fd19",
            (Testnet,V1) => "06995b9fc19461a2bf9933e57b69af47a20bf0a5bc6c0ffcb85567a2c733f0a1",
            (Testnet,V2) => "5e8d2a517d50fd4bb4d01737a7952a1f1d35c8afc77240695bb569cd7d9d5a1f",
            (Mainnet,V1) => "96b198fb5ddbd1eed57ed667068f1f1e55d07907b4c0dbd38675a69ea1b69824"
        }
    );

    define_cell_dep!(
        cluster_type_dep,
        DepType::Code,
        0x0,
        {
            (Testnet,V0) => "49551a20dfe39231e7db49431d26c9c08ceec96a29024eef3acc936deeb2ca76",
            (Testnet,V1) => "fbceb70b2e683ef3a97865bb88e082e3e5366ee195a9c826e3c07d1026792fcd",
            (Testnet,V2) => "cebb174d6e300e26074aea2f5dbd7f694bb4fe3de52b6dfe205e54f90164510a",
            (Mainnet,V1) => "e464b7fb9311c5e2820e61c99afc615d6b98bdefbe318c34868c010cbd0dc938"
        }
    );

    define_script!(
        rgbpp_lock_script,
        ScriptHashType::Type,
        {
            Testnet => "61ca7a4796a4eb19ca4f0d065cb9b10ddcf002f10f7cbb810c706cb6bb5c3248",
            Mainnet => "bc6c568a1a0d0a09f6844dc9d74ddb4343c32143ff25f727c59edf4fb72d6936"
        }
    );

    define_script!(
        btc_time_lock_script,
        ScriptHashType::Type,
        {
            Testnet => "00cdf8fab0f8ac638758ebf5ea5e4052b1d71e8a77b9f43139718621f6849326",
            Mainnet => "70d64497a075bd651e98ac030455ea200637ee325a12ad08aff03f1a117e5a62"
        }
    );

    define_script!(
        cluster_agent_type_script,
        ScriptHashType::Data1,
        {
            (Testnet, V1) => "c986099b41d79ca1b2a56ce5874bcda8175440a17298ea5e2bbc3897736b8c21",
            (Testnet, V2) => "923e997654b2697ee3f77052cb884e98f28799a4270fd412c3edb8f3987ca622",
        }
    );

    define_script!(
        cluster_proxy_type_script,
        ScriptHashType::Data1,
        {
            (Testnet, V1) => "be8b9ce3d05a32c4bb26fe71cd5fc1407ce91e3a8b9e8719be2ab072cef1454b",
            (Testnet, V2) => "4349889bda064adab8f49f7dd8810d217917f7df28e9b2a1df0b74442399670a",
        }
    );

    define_cell_dep!(
        cluster_agent_dep,
        DepType::Code,
        0x0,
        {
            (Testnet, V1) => "53fdb9366637434ff685d0aca5e2a68a859b6fcaa4b608a7ecca0713fed0f5b7",
            (Testnet, V2) => "52210232292d10c51b48e72a2cea60d8f0a08c2680a97a8ee7ca0a39379f0036",
        }
    );

    define_cell_dep!(
        cluster_proxy_dep,
        DepType::Code,
        0x0,
        {
            (Testnet, V1) => "0231ea581bbc38965e10a2659da326ae840c038a9d0d6849f458b51d94870104",
            (Testnet, V2) => "c5a41d58155b11ecd87a5a49fdcb6e83bd6684d3b72b2f3686f081945461c156",
        }
    );
}
