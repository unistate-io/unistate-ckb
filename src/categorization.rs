use ckb_jsonrpc_types::TransactionView;
use rayon::iter::{IntoParallelIterator as _, ParallelExtend as _};

pub struct CategorizedTxs {
    pub spore_txs: Vec<super::spore::SporeTx>,
    pub xudt_txs: Vec<TransactionView>,
    pub rgbpp_txs: Vec<TransactionView>,
    pub inscription_txs: Vec<TransactionView>,
}

impl CategorizedTxs {
    pub fn new() -> Self {
        Self {
            spore_txs: Vec::new(),
            xudt_txs: Vec::new(),
            rgbpp_txs: Vec::new(),
            inscription_txs: Vec::new(),
        }
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.rgbpp_txs.par_extend(other.rgbpp_txs.into_par_iter());
        self.spore_txs.par_extend(other.spore_txs.into_par_iter());
        self.xudt_txs.par_extend(other.xudt_txs.into_par_iter());
        self.inscription_txs
            .par_extend(other.inscription_txs.into_par_iter());
        self
    }
}

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
