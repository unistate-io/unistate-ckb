use anyhow::{anyhow, bail, Error};

#[derive(Debug, PartialEq, Eq)]
pub struct TokenInfo {
    pub decimal: u8,
    pub name: String,
    pub symbol: String,
}

pub fn decode_token_info(hex_string: &str) -> Result<TokenInfo, Error> {
    // Remove "0x" prefix and decode hex string into a byte array
    let bytes = hex::decode(hex_string.trim_start_matches("0x"))
        .map_err(|_| anyhow!("Invalid hex string"))?;

    let info = decode_token_info_bytes(&bytes)?;

    Ok(info)
}

pub fn decode_token_info_bytes(bytes: &[u8]) -> Result<TokenInfo, Error> {
    // Check if the byte array has at least 3 bytes (decimal, name size, and symbol size)
    if bytes.len() < 3 {
        bail!("Invalid hex string: too short");
    }

    // Extract decimal value from the first byte
    let decimal = bytes[0];

    // Extract name size from the second byte
    let name_size = bytes[1] as usize;

    // Check if the byte array has enough bytes to contain the name
    if bytes.len() < 2 + name_size {
        bail!("Invalid hex string: name too long");
    }

    // Extract name from the subsequent bytes
    let name = String::from_utf8(bytes[2..2 + name_size].to_vec())
        .map_err(|_| anyhow!("Invalid UTF-8 name"))?;

    // Extract symbol size from the byte after the name
    let symbol_size = bytes[2 + name_size] as usize;

    // Check if the byte array has enough bytes to contain the symbol
    if bytes.len() < 3 + name_size + symbol_size {
        bail!("Invalid hex string: symbol too long");
    }

    // Extract symbol from the subsequent bytes
    let symbol = String::from_utf8(bytes[3 + name_size..3 + name_size + symbol_size].to_vec())
        .map_err(|_| anyhow!("Invalid UTF-8 symbol"))?;

    Ok(TokenInfo {
        decimal,
        name,
        symbol,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_token_info(token_info: &TokenInfo) -> String {
        let decimal = format!("{:02x}", token_info.decimal);
        let name = hex::encode(token_info.name.as_bytes());
        let name_size = format!("{:02x}", name.len() / 2);
        let symbol = hex::encode(token_info.symbol.as_bytes());
        let symbol_size = format!("{:02x}", symbol.len() / 2);
        format!(
            "0x{}{}{}{}{}",
            decimal, name_size, name, symbol_size, symbol
        )
    }

    #[test]
    fn test_decode_token_info() {
        let expected = TokenInfo {
            decimal: 1,
            name: "my token".to_string(),
            symbol: "ES".to_string(),
        };

        let hex_string = encode_token_info(&expected);

        println!("hex_string: {hex_string}");

        let result = decode_token_info(&hex_string);
        assert_eq!(result.ok(), Some(expected));

        let res = decode_token_info_bytes(&hex_literal::hex!("0805436c6f776e05436c6f776e")[..]);
        
        println!("{res:?}");
    }

    #[test]
    fn test_decode_token_info_invalid_hex() {
        let hex_string = "0xinvalid";
        let result = decode_token_info(hex_string);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_token_info_too_short() {
        let hex_string = "0x01";
        let result = decode_token_info(hex_string);
        assert!(result.is_err());
    }
}
