// unistate-ckb/crates/utils/src/address.rs (or similar)

use crate::network::{NetworkType, PREFIX_MAINNET, PREFIX_TESTNET};
// Import items from bech32 v0.11.0
use bech32::{self, Bech32m, EncodeError, Hrp, primitives::hrp::Error as HrpError};
use ckb_types::{error::VerificationError, packed, prelude::*};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AddressError {
    #[error("Molecule parsing error: {0}")]
    Molecule(#[from] VerificationError),
    #[error("Invalid CKB Human-Readable Part (HRP): {0}")]
    InvalidHrp(#[from] HrpError),
    #[error("Bech32 encoding error: {0}")]
    Bech32Encode(#[from] EncodeError),
}

/// Generates a CKB2021 format (Bech32m) address string from script bytes
/// using bech32 v0.11.0 API.
///
/// # Arguments
///
/// * `script_bytes` - The raw bytes of a molecule-encoded `packed::Script`.
/// * `network` - The target network (Mainnet or Testnet).
///
/// # Returns
///
/// A CKB address string (e.g., "ckb1...") or an AddressError.
pub fn script_bytes_to_address(
    script_bytes: &[u8],
    network: NetworkType,
) -> Result<String, AddressError> {
    // 1. Parse the script bytes using molecule reader
    let script_reader = packed::ScriptReader::from_slice(script_bytes)?;

    // 2. Extract components
    let code_hash = script_reader.code_hash();
    let hash_type_byte = script_reader.hash_type().as_slice()[0];
    let args = script_reader.args().raw_data();

    // 3. Determine HRP string and parse it into bech32::Hrp
    let hrp_str = match network {
        NetworkType::Mainnet => PREFIX_MAINNET,
        NetworkType::Testnet => PREFIX_TESTNET,
    };
    // Hrp::parse validates the HRP according to bech32 rules (ASCII, length, etc.)
    let hrp = Hrp::parse(hrp_str)?;

    // 4. Construct the CKB2021 payload: 0x00 | code_hash | hash_type | args
    //    This remains the same.
    let mut ckb_payload = Vec::with_capacity(1 + 32 + 1 + args.len());
    ckb_payload.push(0x00); // CKB2021 full address format identifier
    ckb_payload.extend_from_slice(code_hash.as_slice());
    ckb_payload.push(hash_type_byte);
    ckb_payload.extend_from_slice(args.as_ref());

    // 5. Encode using bech32::encode with Bech32m checksum
    //    - Pass the parsed `Hrp`.
    //    - Pass the raw `ckb_payload` bytes (`&[u8]`).
    //    - Specify `Bech32m` as the checksum type parameter.
    //    - No need for `.to_base32()` or `Variant`.
    let address = bech32::encode::<Bech32m>(hrp, &ckb_payload)?; // Returns Result<String, EncodeError>

    Ok(address)
}
