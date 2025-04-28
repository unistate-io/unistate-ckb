use ckb_jsonrpc_types::CellDep;
use constants::{Constants, Version};
use fetcher::get_fetcher;
use prettytable::{Table, row};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to fetch data: {0}")]
    FetcherError(#[from] fetcher::Error),

    #[error("Block height not found for CellDep")]
    BlockHeightNotFound,

    #[error("Transaction not found for CellDep")]
    TransactionNotFound,
}

pub async fn find_cell_dep_block_height(cell_dep: &CellDep) -> Result<u64, Error> {
    let tx_hash = cell_dep.out_point.tx_hash.clone();
    let txs = get_fetcher()?.get_txs(vec![tx_hash]).await?;
    let tx = txs.get(0).ok_or(Error::TransactionNotFound)?;
    let block_height = tx
        .as_ref()
        .and_then(|tx| tx.tx_status.block_number)
        .ok_or(Error::BlockHeightNotFound)?
        .value();
    Ok(block_height)
}

// Define a structure to hold dependency information with category and version
struct DepInfo {
    dep: CellDep,
    category: String,
    version: Option<Version>,
}

impl DepInfo {
    fn new(dep: CellDep, category: String, version: Option<Version>) -> Self {
        DepInfo {
            dep,
            category,
            version,
        }
    }
}
// Function to fetch and print dependency heights
pub async fn fetch_and_print_dep_heights(constants: Constants) -> Result<(), Error> {
    let mut dep_list = Vec::new();

    // Add spore deps with versions
    for version in [Version::V2, Version::V1, Version::V0] {
        if let Some(dep) = constants.spore_type_dep(version) {
            dep_list.push(DepInfo::new(dep, "Spore Type".to_string(), Some(version)));
        }
    }

    // Add xudt dep without version
    let xudt_dep = constants.xudt_type_dep();
    dep_list.push(DepInfo::new(xudt_dep, "XUDT Type".to_string(), None));

    // Add ickb_logic_type_dep with versions
    for version in [
        Version::V4,
        Version::V3,
        Version::V2,
        Version::V1,
        Version::V0,
    ] {
        if let Some(dep) = constants.ickb_logic_type_dep(version) {
            dep_list.push(DepInfo::new(
                dep,
                "iCKB Logic Type".to_string(),
                Some(version),
            ));
        }
    }

    // Add rgbpp dep without version
    let rgbpp_dep = constants.rgbpp_lock_dep();
    dep_list.push(DepInfo::new(rgbpp_dep, "RGBPP Lock".to_string(), None));

    // Add inscription deps without version
    let inscription_info_dep = constants.inscription_info_dep();
    dep_list.push(DepInfo::new(
        inscription_info_dep,
        "Inscription Info".to_string(),
        None,
    ));

    let inscription_dep = constants.inscription_dep();
    dep_list.push(DepInfo::new(
        inscription_dep,
        "Inscription".to_string(),
        None,
    ));

    // Sort dep_list by category and version (descending)
    dep_list.sort_by_key(|dep| {
        (
            dep.category.clone(),
            dep.version.map(|v| std::cmp::Reverse(v)), // Reverse for descending order
        )
    });

    let mut table = Table::new();
    table.add_row(row!["Category", "Version", "TX Hash", "Height"]);

    // Fetch and add rows to the table
    for dep_info in dep_list {
        match find_cell_dep_block_height(&dep_info.dep).await {
            Ok(height) => {
                table.add_row(row![
                    dep_info.category,
                    dep_info
                        .version
                        .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    format!("0x{}", dep_info.dep.out_point.tx_hash.to_string()),
                    height.to_string()
                ]);
            }
            Err(Error::BlockHeightNotFound) => {
                table.add_row(row![
                    dep_info.category,
                    dep_info
                        .version
                        .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    format!("0x{}", dep_info.dep.out_point.tx_hash.to_string()),
                    "Not Found"
                ]);
            }
            Err(Error::TransactionNotFound) => {
                table.add_row(row![
                    dep_info.category,
                    dep_info
                        .version
                        .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    format!("0x{}", dep_info.dep.out_point.tx_hash.to_string()),
                    "Transaction Not Found"
                ]);
            }
            Err(e) => return Err(e),
        }
    }

    // Print the table
    table.printstd();

    Ok(())
}
