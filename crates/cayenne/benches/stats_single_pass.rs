/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Stats-computation microbenchmark for the CDC write path (Path 2 apply +
//! Path 1/3 write_to_snapshot), grounded on the CH-benCHmark `order_line`
//! schema (7×Int32, 1×Timestamp(µs), 1×Float64, 1×Utf8; PK =
//! `ol_w_id,ol_d_id,ol_o_id,ol_number`).
//!
//! Questions this settles:
//! 1. **Apply-path cost** — how expensive is the real
//!    `statistics_from_record_batches` (min/max/null over EVERY column, run
//!    under the per-shard publish lock on every mem-tier append) at realistic
//!    coalesced burst sizes? This is the explorer's "prime suspect" for the
//!    80ms/burst regression; measure whether it holds for a 10-col table.
//! 2. **B1 (column restriction)** — payoff of computing pruning stats for only
//!    the PK columns (4 of 10) vs all 10.
//! 3. **B2 (single pass)** — does a fused single-pass min+max beat arrow's two
//!    SIMD passes (min then max)? And on the write path, does fusing the NDV
//!    hash into that pass beat the separate min+max+NDV three-pass shape?
//! 4. **Write-path NDV weight** — cost of the accumulator with vs without the
//!    HyperLogLog fold (the eager-vs-lazy-NDV delta).
//!
//! Candidate lanes are checked bit-identical to the real baseline
//! (`verify_equivalence`) before timing.
//!
//! `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
//!   cargo bench --bench stats_single_pass -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::too_many_lines)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use cayenne::__bench_stats::{
    accumulate_write_stats, compute_column_stats, statistics_from_record_batches,
};
use cayenne::hll::HyperLogLog;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;

/// Coalesced-burst row counts to sweep. 16K is the CDC coalesced envelope; the
/// larger sizes model heavily-coalesced order_line bursts.
const ROWS: &[usize] = &[16_384, 131_072, 1_048_576];

const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

/// The CH-benCHmark `order_line` Arrow schema after CDC ingest.
fn order_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ol_o_id", DataType::Int32, false),
        Field::new("ol_d_id", DataType::Int32, false),
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_supply_w_id", DataType::Int32, false),
        Field::new(
            "ol_delivery_d",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Float64, false),
        Field::new("ol_dist_info", DataType::Utf8, false),
    ]))
}

/// PK-only projection of `order_line` (the pruning-relevant columns for B1).
const PK_COLS: &[usize] = &[2, 1, 0, 3]; // ol_w_id, ol_d_id, ol_o_id, ol_number

fn gen_order_line_batch(rows: usize) -> RecordBatch {
    let mut o_id = Vec::with_capacity(rows);
    let mut d_id = Vec::with_capacity(rows);
    let mut w_id = Vec::with_capacity(rows);
    let mut number = Vec::with_capacity(rows);
    let mut i_id = Vec::with_capacity(rows);
    let mut supply = Vec::with_capacity(rows);
    let mut delivery: Vec<Option<i64>> = Vec::with_capacity(rows);
    let mut quantity = Vec::with_capacity(rows);
    let mut amount = Vec::with_capacity(rows);
    let mut dist = Vec::with_capacity(rows);

    for r in 0..rows {
        let h = (r as u64).wrapping_mul(SPREAD);
        o_id.push((h % 3_000) as i32);
        d_id.push((h >> 6 & 0x9) as i32);
        w_id.push((h >> 10 & 0xff) as i32);
        number.push((h >> 18 & 0xf) as i32);
        i_id.push((h >> 22 & 0xffff) as i32);
        supply.push((h >> 38 & 0xff) as i32);
        // ~5% NULL delivery (unshipped), else a monotone-ish microsecond ts.
        if h % 20 == 0 {
            delivery.push(None);
        } else {
            delivery.push(Some(1_600_000_000_000_000_i64 + (r as i64) * 1_000));
        }
        quantity.push((h >> 46 & 0xf) as i32 + 1);
        amount.push(((h >> 4 & 0xffff) as f64) / 100.0);
        dist.push(format!("{:024x}", h & 0xffff_ffff_ffff));
    }

    RecordBatch::try_new(
        order_line_schema(),
        vec![
            Arc::new(Int32Array::from(o_id)),
            Arc::new(Int32Array::from(d_id)),
            Arc::new(Int32Array::from(w_id)),
            Arc::new(Int32Array::from(number)),
            Arc::new(Int32Array::from(i_id)),
            Arc::new(Int32Array::from(supply)),
            Arc::new(TimestampMicrosecondArray::from(delivery)),
            Arc::new(Int32Array::from(quantity)),
            Arc::new(Float64Array::from(amount)),
            Arc::new(StringArray::from(dist.iter().map(String::as_str).collect::<Vec<_>>())),
        ],
    )
    .expect("build order_line batch")
}

fn project(batch: &RecordBatch, cols: &[usize]) -> RecordBatch {
    batch.project(cols).expect("project")
}

// ---------------------------------------------------------------------------
// Candidate B2: fused single-pass min+max for one Int32 column, returning the
// same (min, max) ScalarValues the real `compute_column_stats` produces.
// ---------------------------------------------------------------------------

fn fused_i32_min_max(col: &Int32Array) -> (Option<i32>, Option<i32>) {
    let mut min = None::<i32>;
    let mut max = None::<i32>;
    if col.null_count() == 0 {
        for &v in col.values() {
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }
    } else {
        for v in col.iter().flatten() {
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }
    }
    (min, max)
}

/// Candidate: fused single-pass min+max+NDV over an Int32 column (the write
/// path shape — folds the HLL hash in the same pass instead of a separate one).
fn fused_i32_min_max_ndv(col: &Int32Array, hll: &mut HyperLogLog) -> (Option<i32>, Option<i32>) {
    let mut min = None::<i32>;
    let mut max = None::<i32>;
    for v in col.iter().flatten() {
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
        hll.add_i128(i128::from(v));
    }
    (min, max)
}

/// Baseline kernel shape: arrow's two SIMD passes for min/max, then a separate
/// NDV fold pass — exactly the accumulator's per-Int32-column work.
fn baseline_i32_min_max_ndv(col: &Int32Array, hll: &mut HyperLogLog) -> (Option<i32>, Option<i32>) {
    let min = aggregate::min::<Int32Type>(col);
    let max = aggregate::max::<Int32Type>(col);
    for v in col.iter().flatten() {
        hll.add_i128(i128::from(v));
    }
    (min, max)
}

fn verify_equivalence(batch: &RecordBatch) {
    // Fused i32 min/max must match arrow's SIMD min/max on the Int32 columns.
    for &c in &[0usize, 1, 2, 3, 4, 5, 7] {
        let col = batch
            .column(c)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32 col");
        let (fmin, fmax) = fused_i32_min_max(col);
        assert_eq!(fmin, aggregate::min::<Int32Type>(col), "fused min col {c}");
        assert_eq!(fmax, aggregate::max::<Int32Type>(col), "fused max col {c}");

        let mut h1 = HyperLogLog::new();
        let mut h2 = HyperLogLog::new();
        let a = fused_i32_min_max_ndv(col, &mut h1);
        let b = baseline_i32_min_max_ndv(col, &mut h2);
        assert_eq!(a, b, "fused+ndv min/max col {c}");
        assert_eq!(h1.estimate(), h2.estimate(), "fused+ndv NDV col {c}");
    }

    // NDV: the oneshot-i128 hash must fold to a bit-identical HyperLogLog as the
    // current streaming add_i128 (same bytes, same seed) — so persisted sketches
    // stay merge-compatible. The native-i64 lane hashes 8 bytes not 16, so it is
    // a DIFFERENT (version-bumped) sketch and is only compared for speed.
    for &c in &[0usize, 1, 2, 3, 4, 5, 7] {
        let col = batch
            .column(c)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32 col");
        let mut h_stream = HyperLogLog::new();
        let mut h_oneshot = HyperLogLog::new();
        for v in col.iter().flatten() {
            h_stream.add_i128(i128::from(v));
            h_oneshot.add_hash(hash_index::hash_key_bytes_oneshot(&i128::from(v).to_le_bytes()));
        }
        assert_eq!(
            h_stream.estimate(),
            h_oneshot.estimate(),
            "oneshot-i128 NDV matches streaming for col {c}"
        );
    }

    // The real per-append stats must agree with our understanding: min/max
    // present and exact for the PK columns.
    let schema = batch.schema();
    let stats = statistics_from_record_batches(&schema, std::slice::from_ref(batch));
    assert_eq!(
        stats.column_statistics.len(),
        batch.num_columns(),
        "one ColumnStatistics per column"
    );
    for &c in PK_COLS {
        let col = batch
            .column(c)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("pk i32");
        let want = aggregate::min::<Int32Type>(col).map(|v| ScalarValue::Int32(Some(v)));
        assert_eq!(
            stats.column_statistics[c].min_value.get_value().cloned(),
            want,
            "real stats min matches arrow for PK col {c}"
        );
    }
}

fn bench_stats(c: &mut Criterion) {
    // Equivalence gate at a mid size before any timing.
    verify_equivalence(&gen_order_line_batch(65_536));

    // ---- Apply path (Path 2): the real per-append stats cost ----
    let mut apply = c.benchmark_group("apply_path_stats");
    for &rows in ROWS {
        let batch = gen_order_line_batch(rows);
        let schema = batch.schema();
        let pk_batch = project(&batch, PK_COLS);
        let pk_schema = pk_batch.schema();
        apply.throughput(Throughput::Elements(rows as u64));

        // Real baseline: statistics_from_record_batches over all 10 columns.
        apply.bench_with_input(BenchmarkId::new("real_all_cols", rows), &rows, |b, _| {
            b.iter(|| {
                black_box(statistics_from_record_batches(
                    &schema,
                    std::slice::from_ref(&batch),
                ))
            });
        });

        // B1: only the 4 PK (pruning-relevant) columns.
        apply.bench_with_input(BenchmarkId::new("real_pk_only", rows), &rows, |b, _| {
            b.iter(|| {
                black_box(statistics_from_record_batches(
                    &pk_schema,
                    std::slice::from_ref(&pk_batch),
                ))
            });
        });

        // B2: fused single-pass min+max over the 7 Int32 columns only (isolates
        // the fused-vs-2×SIMD question on the type that dominates the schema).
        apply.bench_with_input(BenchmarkId::new("fused_i32_1pass", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("i32");
                    black_box(fused_i32_min_max(col));
                }
            });
        });
        apply.bench_with_input(BenchmarkId::new("simd_i32_2pass", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("i32");
                    black_box((aggregate::min::<Int32Type>(col), aggregate::max::<Int32Type>(col)));
                }
            });
        });
    }
    apply.finish();

    // ---- Write path (Path 1/3): accumulator with vs without NDV ----
    let mut write = c.benchmark_group("write_path_stats");
    for &rows in ROWS {
        let batch = gen_order_line_batch(rows);
        let schema = batch.schema();
        write.throughput(Throughput::Elements(rows as u64));

        write.bench_with_input(BenchmarkId::new("accumulate_ndv", rows), &rows, |b, _| {
            b.iter(|| black_box(accumulate_write_stats(&schema, std::slice::from_ref(&batch), true)));
        });
        write.bench_with_input(BenchmarkId::new("accumulate_no_ndv", rows), &rows, |b, _| {
            b.iter(|| {
                black_box(accumulate_write_stats(&schema, std::slice::from_ref(&batch), false))
            });
        });

        // Kernel-level: fused min+max+ndv (1 pass) vs 2×SIMD + separate ndv pass,
        // over the 7 Int32 columns.
        write.bench_with_input(BenchmarkId::new("kernel_fused_mmn_1pass", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("i32");
                    let mut hll = HyperLogLog::new();
                    black_box(fused_i32_min_max_ndv(col, &mut hll));
                    black_box(hll.estimate());
                }
            });
        });
        write.bench_with_input(BenchmarkId::new("kernel_baseline_mmn_3pass", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("i32");
                    let mut hll = HyperLogLog::new();
                    black_box(baseline_i32_min_max_ndv(col, &mut hll));
                    black_box(hll.estimate());
                }
            });
        });

        // NDV hash-fold cost in isolation (7 int cols): the current streaming
        // add_i128 vs a bit-identical oneshot over the same 16 i128 bytes vs a
        // version-bumped native-8-byte oneshot.
        write.bench_with_input(BenchmarkId::new("ndv_streaming_i128", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch.column(col_idx).as_any().downcast_ref::<Int32Array>().expect("i32");
                    let mut hll = HyperLogLog::new();
                    for v in col.iter().flatten() {
                        hll.add_i128(i128::from(v));
                    }
                    black_box(hll.estimate());
                }
            });
        });
        write.bench_with_input(BenchmarkId::new("ndv_oneshot_i128", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch.column(col_idx).as_any().downcast_ref::<Int32Array>().expect("i32");
                    let mut hll = HyperLogLog::new();
                    for v in col.iter().flatten() {
                        hll.add_hash(hash_index::hash_key_bytes_oneshot(&i128::from(v).to_le_bytes()));
                    }
                    black_box(hll.estimate());
                }
            });
        });
        write.bench_with_input(BenchmarkId::new("ndv_oneshot_i64native", rows), &rows, |b, _| {
            b.iter(|| {
                for &col_idx in &[0usize, 1, 2, 3, 4, 5, 7] {
                    let col = batch.column(col_idx).as_any().downcast_ref::<Int32Array>().expect("i32");
                    let mut hll = HyperLogLog::new();
                    for v in col.iter().flatten() {
                        hll.add_hash(hash_index::hash_key_bytes_oneshot(&i64::from(v).to_le_bytes()));
                    }
                    black_box(hll.estimate());
                }
            });
        });
    }
    write.finish();

    // Reference the compute_column_stats primitive so it is linked (and to keep
    // the re-export honest if the signature drifts).
    let one = gen_order_line_batch(1);
    black_box(compute_column_stats(one.column(0).as_ref()));
}

criterion_group!(benches, bench_stats);
criterion_main!(benches);
