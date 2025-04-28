use ckb_jsonrpc_types::TransactionView;

macro_rules! define_categories {
    ($($item:ident),*) => {
        pub struct Categories {
            $(pub $item: bool),*
        }

        pub fn categorize_transaction(tx: &TransactionView, constants: &constants::Constants) -> Categories {
            tx.inner.cell_deps.iter().fold(
                Categories {
                    $($item: false),*
                },
                |categories, cd| (
                    Categories {
                        $($item: categories.$item || constants.$item(cd)),*
                    }
                )
            )
        }
    };
}

define_categories! { is_spore, is_xudt, is_rgbpp, is_inscription }
