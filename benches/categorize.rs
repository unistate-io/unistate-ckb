use criterion::{criterion_group, criterion_main, Criterion};
use rayon::prelude::*;

#[derive(Clone)]
struct CellDep {
    out_point: usize,
}

#[derive(Clone)]
struct InnerTx {
    cell_deps: Vec<CellDep>,
}

#[derive(Clone)]
struct Transaction {
    inner: InnerTx,
}

#[derive(Clone)]
struct Block {
    transactions: Vec<Transaction>,
}

struct CategorizedTxs {
    spore_txs: Vec<Transaction>,
    xudt_txs: Vec<Transaction>,
    rgbpp_txs: Vec<Transaction>,
}

impl CategorizedTxs {
    fn new() -> Self {
        Self {
            spore_txs: Vec::new(),
            xudt_txs: Vec::new(),
            rgbpp_txs: Vec::new(),
        }
    }

    fn merge(mut self, other: CategorizedTxs) -> Self {
        self.spore_txs.extend(other.spore_txs);
        self.xudt_txs.extend(other.xudt_txs);
        self.rgbpp_txs.extend(other.rgbpp_txs);
        self
    }
}
// Original implementation using map
fn original_categorize(blocks: Vec<Block>) -> CategorizedTxs {
    blocks
        .into_par_iter()
        .fold(CategorizedTxs::new, |acc, block| {
            let new = block
                .transactions
                .into_par_iter()
                .fold(CategorizedTxs::new, |mut txs, tx| {
                    let rgbpp = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 1);
                    let spore = tx
                        .inner
                        .cell_deps
                        .par_iter()
                        .any(|cd| cd.out_point == 2 || cd.out_point == 3);
                    let xudt = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 4);

                    if spore {
                        txs.spore_txs.push(tx.clone());
                    }
                    if xudt {
                        txs.xudt_txs.push(tx.clone());
                    }
                    if rgbpp {
                        txs.rgbpp_txs.push(tx);
                    }
                    txs
                })
                .reduce(CategorizedTxs::new, |pre, next| pre.merge(next));
            acc.merge(new)
        })
        .reduce(CategorizedTxs::new, |acc, txs| acc.merge(txs))
}
// Optimized implementation using filter_map
fn optimized_categorize(blocks: Vec<Block>) -> CategorizedTxs {
    blocks
        .into_par_iter()
        .flat_map(|block| {
            block.transactions.into_par_iter().filter_map(move |tx| {
                let rgbpp = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 1);
                let spore = tx
                    .inner
                    .cell_deps
                    .par_iter()
                    .any(|cd| cd.out_point == 2 || cd.out_point == 3);
                let xudt = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 4);

                if rgbpp || spore || xudt {
                    let mut categorized = CategorizedTxs::new();

                    if spore {
                        categorized.spore_txs.push(tx.clone());
                    }

                    if xudt {
                        categorized.xudt_txs.push(tx.clone());
                    }

                    if rgbpp {
                        categorized.rgbpp_txs.push(tx);
                    }

                    Some(categorized)
                } else {
                    None
                }
            })
        })
        .reduce(CategorizedTxs::new, |acc, txs| acc.merge(txs))
}

fn optimized_categorize2(blocks: Vec<Block>) -> CategorizedTxs {
    blocks
        .into_par_iter()
        .flat_map(|block| {
            block.transactions.into_par_iter().map(move |tx| {
                let rgbpp = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 1);
                let spore = tx
                    .inner
                    .cell_deps
                    .par_iter()
                    .any(|cd| cd.out_point == 2 || cd.out_point == 3);
                let xudt = tx.inner.cell_deps.par_iter().any(|cd| cd.out_point == 4);

                let mut categorized = CategorizedTxs::new();

                if spore {
                    categorized.spore_txs.push(tx.clone());
                }
                if xudt {
                    categorized.xudt_txs.push(tx.clone());
                }
                if rgbpp {
                    categorized.rgbpp_txs.push(tx);
                }
                categorized
            })
        })
        .reduce(CategorizedTxs::new, |acc, txs| acc.merge(txs))
}

fn generate_test_data(
    num_blocks: usize,
    num_txs_per_block: usize,
    num_deps_per_tx: usize,
) -> Vec<Block> {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    (0..num_blocks)
        .map(|_| Block {
            transactions: (0..num_txs_per_block)
                .map(|_| Transaction {
                    inner: InnerTx {
                        cell_deps: (0..num_deps_per_tx)
                            .map(|_| CellDep {
                                out_point: rng.gen_range(1..=4),
                            })
                            .collect(),
                    },
                })
                .collect(),
        })
        .collect()
}

fn benchmark(c: &mut Criterion) {
    let num_blocks = 300;
    let num_txs_per_block = 100;
    let num_deps_per_tx = 5;

    let test_data = generate_test_data(num_blocks, num_txs_per_block, num_deps_per_tx);

    c.bench_function("original_categorize", |b| {
        b.iter(|| original_categorize(test_data.clone()))
    });

    c.bench_function("optimized_categorize", |b| {
        b.iter(|| optimized_categorize(test_data.clone()))
    });

    c.bench_function("optimized_categorize2", |b| {
        b.iter(|| optimized_categorize2(test_data.clone()))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
