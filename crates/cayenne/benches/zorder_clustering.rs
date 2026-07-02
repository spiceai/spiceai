/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Cold-tier Z-order clustering benchmark.
//!
//! Two things are reported:
//!  1. **Clustering effectiveness** — the fraction of cold "files" a point query
//!     must scan when the data is laid out by a single-column sort vs. a
//!     multi-column Z-order, on a 2-D grid. This is the read-optimization the
//!     cold tier exists to deliver: Z-order tightens the per-file zone maps on
//!     *every* clustering dimension at once, so a selective predicate on any of
//!     them prunes most files (single-column sort only helps the leading column).
//!  2. **Kernel throughput** — how fast `zorder_keys` produces the interleaved
//!     keys, so the clustering cost stays a rounding error against the cold write.

#![allow(clippy::expect_used, clippy::cast_precision_loss)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array};
use cayenne::__bench_zorder::zorder_keys;
use criterion::{Criterion, Throughput, black_box};

/// Grid side length: `GRID * GRID` points.
const GRID: i64 = 64;
/// Number of cold "files" the sorted rows are split into.
const FILES: usize = 64;
const ROWS_PER_FILE: usize = (GRID * GRID) as usize / FILES;

fn build_grid() -> (Vec<i64>, Vec<i64>) {
    let mut d0 = Vec::with_capacity((GRID * GRID) as usize);
    let mut d1 = Vec::with_capacity((GRID * GRID) as usize);
    for a in 0..GRID {
        for b in 0..GRID {
            d0.push(a);
            d1.push(b);
        }
    }
    (d0, d1)
}

fn argsort_binary(keys: &BinaryArray) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..keys.len()).collect();
    idx.sort_by(|&a, &b| keys.value(a).cmp(keys.value(b)));
    idx
}

/// Average fraction of files a point query on dimension `which` (0 or 1) must
/// scan under `order`, given per-file zone-map (min/max) pruning, averaged over
/// every possible query value.
fn avg_files_scanned(order: &[usize], d0: &[i64], d1: &[i64], which: usize) -> f64 {
    let dim = if which == 0 { d0 } else { d1 };
    let mut total = 0usize;
    for v in 0..GRID {
        let mut touched = 0usize;
        for f in 0..FILES {
            let rows = &order[f * ROWS_PER_FILE..(f + 1) * ROWS_PER_FILE];
            let mut mn = i64::MAX;
            let mut mx = i64::MIN;
            for &r in rows {
                mn = mn.min(dim[r]);
                mx = mx.max(dim[r]);
            }
            if v >= mn && v <= mx {
                touched += 1;
            }
        }
        total += touched;
    }
    total as f64 / (GRID as f64 * FILES as f64)
}

fn print_clustering_effectiveness() {
    let (d0, d1) = build_grid();

    // Single-column layout: sort by d0 only (what `cayenne_sort_columns` does).
    let mut single: Vec<usize> = (0..d0.len()).collect();
    single.sort_by_key(|&i| d0[i]);

    // Z-order layout over (d0, d1).
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0.clone())),
        Arc::new(Int64Array::from(d1.clone())),
    ];
    let keys = zorder_keys(&cols).expect("zorder keys");
    let zorder = argsort_binary(&keys);

    println!(
        "\n=== Cold-tier clustering effectiveness ({GRID}x{GRID} grid, {FILES} files, point-query file pruning) ==="
    );
    for which in 0..2 {
        let s = avg_files_scanned(&single, &d0, &d1, which);
        let z = avg_files_scanned(&zorder, &d0, &d1, which);
        let speedup = if z > 0.0 { s / z } else { f64::INFINITY };
        println!(
            "  query on dim{which}: single-column-sort scans {:>5.1}% of files | Z-order scans {:>5.1}% of files | {:>4.1}x fewer files",
            s * 100.0,
            z * 100.0,
            speedup
        );
    }
    println!(
        "  (single-column sort cannot prune the non-leading dimension at all; Z-order prunes both.)\n"
    );
}

fn bench_kernel(c: &mut Criterion) {
    let n = 100_000i64;
    let d0: Vec<i64> = (0..n).collect();
    let d1: Vec<i64> = (0..n).map(|i| (i * 7) % 1000).collect();
    let d2: Vec<i64> = (0..n).map(|i| i % 50).collect();
    let two: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0.clone())),
        Arc::new(Int64Array::from(d1.clone())),
    ];
    let three: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0)),
        Arc::new(Int64Array::from(d1)),
        Arc::new(Int64Array::from(d2)),
    ];

    let mut group = c.benchmark_group("zorder_keys");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("2cols_100k", |b| {
        b.iter(|| black_box(zorder_keys(black_box(&two)).expect("keys")));
    });
    group.bench_function("3cols_100k", |b| {
        b.iter(|| black_box(zorder_keys(black_box(&three)).expect("keys")));
    });
    group.finish();
}

fn main() {
    print_clustering_effectiveness();
    let mut criterion = Criterion::default().configure_from_args();
    bench_kernel(&mut criterion);
    criterion.final_summary();
}
