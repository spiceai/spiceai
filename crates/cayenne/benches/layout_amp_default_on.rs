// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Default-on adaptive layout (F4) read-amplification anchor.
//!
//! Measures how many file-bytes a selective range filter must touch under:
//! 1. **append / random layout** — the production default when `sort_columns` is empty
//! 2. **sorted-by-filter-col layout** — what auto-cluster from observed filters produces
//!
//! Plus a micro-bench of the filter-column histogram used on every scan.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_expr::{col, lit};

// Scale-invariant amp math that anchors the F4 bet. Histogram unit tests live
// in provider/predicate_stats.rs.

const N_ROWS: usize = 100_000;
const N_FILES: usize = 64;
const SELECTIVITY: f64 = 0.05;
const SEED: u64 = 0x00F4_ADA9_2026_0721;

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }
}

/// Bytes touched under zone-map prune (full file when max >= threshold).
fn bytes_touched(order: &[usize], values: &[f64], threshold: f64, files: usize) -> usize {
    let n = order.len();
    let mut touched = 0usize;
    for file in 0..files {
        // Balanced partition covering EVERY row: the `n % files` remainder is
        // spread one-per-file across the leading slices, so no rows are dropped
        // (a fixed `n / files` slice width would skip the last `n % files` rows
        // and skew the amp math).
        let start = file * n / files;
        let end = (file + 1) * n / files;
        let slice = &order[start..end];
        let mut max = f64::NEG_INFINITY;
        for &row in slice {
            max = max.max(values[row]);
        }
        if max >= threshold {
            touched = touched.saturating_add(slice.len());
        }
    }
    touched
}

fn rows_needed(values: &[f64], threshold: f64) -> usize {
    values.iter().filter(|&&v| v >= threshold).count()
}

fn print_amp_report() {
    let mut rng = Rng::new(SEED);
    let values: Vec<f64> = (0..N_ROWS).map(|_| rng.unit()).collect();
    let mut sorted_vals = values.clone();
    sorted_vals.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let threshold = sorted_vals[((1.0 - SELECTIVITY) * N_ROWS as f64) as usize];

    let mut random_order: Vec<usize> = (0..N_ROWS).collect();
    // Fisher–Yates
    for i in (1..N_ROWS).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        random_order.swap(i, j);
    }
    let sorted_order: Vec<usize> = {
        let mut idx: Vec<usize> = (0..N_ROWS).collect();
        idx.sort_by(|&a, &b| values[a].partial_cmp(&values[b]).expect("finite"));
        idx
    };

    let needed = rows_needed(&values, threshold);
    let random_bytes = bytes_touched(&random_order, &values, threshold, N_FILES);
    let sorted_bytes = bytes_touched(&sorted_order, &values, threshold, N_FILES);
    let random_amp = random_bytes as f64 / needed.max(1) as f64;
    let sorted_amp = sorted_bytes as f64 / needed.max(1) as f64;

    println!(
        "\n=== F4 layout read-amp ({N_ROWS} rows, {N_FILES} files, ~{:.0}% selective) ===",
        SELECTIVITY * 100.0
    );
    println!(
        "  random_append (default empty sort_columns): touched={random_bytes} needed≈{needed} amp={random_amp:.1}x"
    );
    println!(
        "  sorted_by_filter_col (auto-cluster target):  touched={sorted_bytes} needed≈{needed} amp={sorted_amp:.1}x"
    );
    println!(
        "  ratio: {:.1}x fewer bytes after adaptive layout\n",
        random_amp / sorted_amp.max(1e-9)
    );

    assert!(
        random_amp > sorted_amp * 5.0,
        "sorted layout must cut amp by ≥5× (random={random_amp}, sorted={sorted_amp})"
    );
}

fn bench_amp(criterion: &mut Criterion) {
    let mut rng = Rng::new(SEED);
    let values: Vec<f64> = (0..N_ROWS).map(|_| rng.unit()).collect();
    let mut sorted_vals = values.clone();
    sorted_vals.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let threshold = sorted_vals[((1.0 - SELECTIVITY) * N_ROWS as f64) as usize];

    let mut random_order: Vec<usize> = (0..N_ROWS).collect();
    for i in (1..N_ROWS).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        random_order.swap(i, j);
    }
    let mut sorted_order: Vec<usize> = (0..N_ROWS).collect();
    sorted_order.sort_by(|&a, &b| values[a].partial_cmp(&values[b]).expect("finite"));

    let mut group = criterion.benchmark_group("layout_amp_zone_map");
    group.throughput(Throughput::Elements(N_ROWS as u64));
    group.bench_function("random_append_files_touched", |bencher| {
        bencher.iter(|| {
            black_box(bytes_touched(
                black_box(&random_order),
                black_box(&values),
                black_box(threshold),
                black_box(N_FILES),
            ))
        });
    });
    group.bench_function("sorted_filter_col_files_touched", |bencher| {
        bencher.iter(|| {
            black_box(bytes_touched(
                black_box(&sorted_order),
                black_box(&values),
                black_box(threshold),
                black_box(N_FILES),
            ))
        });
    });
    group.finish();
}

fn bench_observation_record(criterion: &mut Criterion) {
    // Standalone histogram logic is unit-tested; this benches Expr tree walk
    // cost of a representative filter set (what scan() pays per query).
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_expr::Expr;

    fn collect_names(expr: &Expr, out: &mut Vec<String>) {
        let _ = expr.apply(|node| {
            if let Expr::Column(column) = node {
                let name = column.name.as_str();
                if !out.iter().any(|e| e == name) {
                    out.push(name.to_string());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }

    let filters = vec![
        col("region").eq(lit("west")),
        col("amount").gt(lit(10_i64)),
        col("region")
            .eq(lit("east"))
            .and(col("amount").lt(lit(100_i64))),
    ];

    let mut group = criterion.benchmark_group("filter_column_extract");
    group.throughput(Throughput::Elements(filters.len() as u64));
    group.bench_function("extract_3_filters", |bencher| {
        bencher.iter(|| {
            let mut names = Vec::new();
            for filter in &filters {
                collect_names(filter, &mut names);
            }
            black_box(names)
        });
    });
    group.finish();
}

fn bench_layout_amp(criterion: &mut Criterion) {
    print_amp_report();
    bench_amp(criterion);
    bench_observation_record(criterion);
}

criterion_group!(benches, bench_layout_amp);
criterion_main!(benches);
