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

//! Cold-tier clustering benchmark.
//!
//! Three things are reported:
//!  1. **Clustering effectiveness** — the fraction of cold "files" a point query
//!     must scan when the data is laid out by a single-column sort vs. the
//!     multi-dimensional curve, on a 2-D grid. This is the read-optimization the
//!     cold tier exists to deliver: the curve tightens the per-file zone maps on
//!     *every* clustering dimension at once, so a selective predicate on any of
//!     them prunes most files (single-column sort only helps the leading column).
//!  2. **Unequal column ranges** — the same measurement where one column is a
//!     microsecond timestamp and the other a small tenant id. Without normalized
//!     coordinates a bit-interleave spends its high-order rounds entirely on the
//!     wide column, so this is the case that separates a real multi-dimensional
//!     layout from one that has collapsed onto a single column.
//!  3. **Kernel throughput** — how fast `cluster_keys` produces the keys, so the
//!     clustering cost stays a rounding error against the cold write.

#![allow(clippy::expect_used, clippy::cast_precision_loss)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array};
use cayenne::__bench_clustering::cluster_keys;
use criterion::{Criterion, Throughput};
use std::hint::black_box;

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

/// `[min, max]` of `values` in the kernel's key space, as the engine takes them
/// from the maintained statistics aggregate.
fn bounds_of(values: &[i64]) -> Option<(u128, u128)> {
    let key = |v: i64| u128::from(v.cast_unsigned() ^ (1u64 << 63));
    Some((
        key(values.iter().copied().min()?),
        key(values.iter().copied().max()?),
    ))
}

fn argsort_binary(keys: &BinaryArray) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..keys.len()).collect();
    idx.sort_by(|&a, &b| keys.value(a).cmp(keys.value(b)));
    idx
}

/// Row order produced by clustering `(d0, d1)` along the curve.
fn clustered_order(d0: &[i64], d1: &[i64]) -> Vec<usize> {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0.to_vec())),
        Arc::new(Int64Array::from(d1.to_vec())),
    ];
    let keys = cluster_keys(&cols, &[bounds_of(d0), bounds_of(d1)]).expect("cluster keys");
    argsort_binary(&keys)
}

/// Average fraction of files a point query on `dim` must scan under `order`,
/// given per-file zone-map (min/max) pruning, averaged over every `probes` value.
fn avg_files_scanned(order: &[usize], dim: &[i64], probes: &[i64]) -> f64 {
    let files = order.len() / ROWS_PER_FILE;
    let mut total = 0usize;
    for &v in probes {
        for f in 0..files {
            let rows = &order[f * ROWS_PER_FILE..(f + 1) * ROWS_PER_FILE];
            let mut mn = i64::MAX;
            let mut mx = i64::MIN;
            for &r in rows {
                mn = mn.min(dim[r]);
                mx = mx.max(dim[r]);
            }
            if v >= mn && v <= mx {
                total += 1;
            }
        }
    }
    total as f64 / (probes.len() as f64 * files as f64)
}

fn print_clustering_effectiveness() {
    let (d0, d1) = build_grid();
    let probes: Vec<i64> = (0..GRID).collect();

    // Single-column layout: sort by d0 only (what `cayenne_sort_columns` does).
    let mut single: Vec<usize> = (0..d0.len()).collect();
    single.sort_by_key(|&i| d0[i]);

    let curve = clustered_order(&d0, &d1);

    println!(
        "\n=== Cold-tier clustering effectiveness ({GRID}x{GRID} grid, {FILES} files, point-query file pruning) ==="
    );
    for (which, dim) in [&d0, &d1].into_iter().enumerate() {
        let s = avg_files_scanned(&single, dim, &probes);
        let c = avg_files_scanned(&curve, dim, &probes);
        let speedup = if c > 0.0 { s / c } else { f64::INFINITY };
        println!(
            "  query on dim{which}: single-column-sort scans {:>5.1}% of files | curve scans {:>5.1}% of files | {:>4.1}x fewer files",
            s * 100.0,
            c * 100.0,
            speedup
        );
    }
    println!(
        "  (single-column sort cannot prune the non-leading dimension at all; the curve prunes both.)"
    );
}

/// The shape that separates a real multi-dimensional layout from one that has
/// collapsed: a microsecond timestamp beside a small tenant id. Interleaving raw
/// value bits puts every varying bit of the timestamp above the first varying
/// bit of the tenant id, so the key degenerates to timestamp order and a tenant
/// predicate has to open every file.
fn print_unequal_range_effectiveness() {
    const ROWS: i64 = 4096;
    const TENANTS: i64 = 16;
    let base_ts = 1_700_000_000_000_000i64;
    let ts: Vec<i64> = (0..ROWS).map(|i| base_ts + i * 1_000).collect();
    let tenants: Vec<i64> = (0..ROWS).map(|i| (i * 7) % TENANTS).collect();

    let curve = clustered_order(&ts, &tenants);
    let mut by_ts: Vec<usize> = (0..ts.len()).collect();
    by_ts.sort_by_key(|&i| ts[i]);

    let tenant_probes: Vec<i64> = (0..TENANTS).collect();
    let ts_probes: Vec<i64> = (0..16).map(|i| ts[i * ts.len() / 16]).collect();

    println!(
        "\n=== Unequal column ranges (us timestamp x {TENANTS} tenants, {ROWS} rows, {} files) ===",
        ts.len() / ROWS_PER_FILE
    );
    println!(
        "  query on tenant_id: timestamp-sorted scans {:>5.1}% of files | curve scans {:>5.1}% of files",
        avg_files_scanned(&by_ts, &tenants, &tenant_probes) * 100.0,
        avg_files_scanned(&curve, &tenants, &tenant_probes) * 100.0,
    );
    println!(
        "  query on ts       : timestamp-sorted scans {:>5.1}% of files | curve scans {:>5.1}% of files",
        avg_files_scanned(&by_ts, &ts, &ts_probes) * 100.0,
        avg_files_scanned(&curve, &ts, &ts_probes) * 100.0,
    );
    println!(
        "  (a curve that has collapsed onto the timestamp matches the timestamp-sorted row.)\n"
    );
}

fn bench_kernel(c: &mut Criterion) {
    let n = 100_000i64;
    let d0: Vec<i64> = (0..n).collect();
    let d1: Vec<i64> = (0..n).map(|i| (i * 7) % 1000).collect();
    let d2: Vec<i64> = (0..n).map(|i| i % 50).collect();
    let two_bounds = vec![bounds_of(&d0), bounds_of(&d1)];
    let three_bounds = vec![bounds_of(&d0), bounds_of(&d1), bounds_of(&d2)];
    let two: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0.clone())),
        Arc::new(Int64Array::from(d1.clone())),
    ];
    let three: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(d0)),
        Arc::new(Int64Array::from(d1)),
        Arc::new(Int64Array::from(d2)),
    ];

    let mut group = c.benchmark_group("cluster_keys");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("2cols_100k", |b| {
        b.iter(|| black_box(cluster_keys(black_box(&two), black_box(&two_bounds)).expect("keys")));
    });
    group.bench_function("3cols_100k", |b| {
        b.iter(|| {
            black_box(cluster_keys(black_box(&three), black_box(&three_bounds)).expect("keys"))
        });
    });
    group.finish();
}

fn main() {
    print_clustering_effectiveness();
    print_unequal_range_effectiveness();
    let mut criterion = Criterion::default().configure_from_args();
    bench_kernel(&mut criterion);
    criterion.final_summary();
}
