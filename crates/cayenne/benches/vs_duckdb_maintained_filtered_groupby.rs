// Copyright 2026 The Spice.ai OSS Authors
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

//! PREDICATE-AWARE maintained aggregate — Cayenne vs DuckDB on the CH-benCH q1/q6
//! shape: a single-table grouped aggregate over a large CDC-fed table WITH a
//! `WHERE` filter.
//!
//! ## What this measures and why (the frontier bet)
//!
//! Cayenne already maintains grouped `SUM`/`COUNT`/`AVG` from the CDC delta and
//! serves a matching query in O(groups) — but ONLY when the plan is a clean
//! UNFILTERED aggregate directly over the scan. Every real CH-benCH analytical
//! query carries a `WHERE`, so the flagship served NONE of them: a `FilterExec`
//! between the aggregate and the scan defeated the rewrite, and the query fell
//! back to an O(rows) re-scan. This lever makes the maintained view
//! predicate-aware: it maintains the aggregate over only the rows the filter
//! selects, and serves a query carrying the identical predicate from that state.
//!
//! - `duckdb_rescan`  — `SELECT grp, SUM(value), COUNT(*) FROM t WHERE delivery > T
//!   GROUP BY grp` against in-memory DuckDB. O(rows): DuckDB has no CDC delta and
//!   no cross-query state, so it re-scans + re-filters + re-aggregates every call.
//! - `cayenne_serve`  — the same answer from the real `MaintainedAggregateRegistry`
//!   via the production serve path (`batch_for_spec`: fresh/epoch gate + filtered
//!   view match + O(groups) materialize). Flat in N.
//! - `cayenne_maintain_delta` — applying a small CDC delta (re-upsert of existing
//!   PKs) to the filtered view. O(delta): the per-ingest cost the serve amortizes.
//!
//! The headline is `cayenne_serve` vs `duckdb_rescan`: O(groups) vs O(rows), so
//! the ratio widens with table size. The result is asserted EQUAL to DuckDB
//! before any timing — a fast wrong answer is worthless.
//!
//! ## Moat
//! Durable (F1 × F5): serving a FILTERED grouped aggregate cheaply needs the CDC
//! delta stream + cross-query maintained state + the recurring observed predicate
//! — none of which a general-purpose, cold-start batch engine (DuckDB/chDB) has.
//! Exact + deletion-safe via the per-PK contribution index, so it is not the
//! approximate slice. chDB (ClickHouse-embedded) shares DuckDB's re-scan shape
//! here (no automatic incremental view over a CDC delta), so the same O(rows) vs
//! O(groups) separation holds against it.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateRegistry,
    MaintainedAggregateSpec,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::logical_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{binary, col, lit};
use duckdb::Connection;

// NOTE: the chDB comparison lives in the sibling `vs_chdb_maintained_filtered_groupby`
// bench, NOT here. DuckDB (bundled) and chDB (bundled full ClickHouse) cannot both
// be driven in one process — linking and running both aborts the process at startup
// (global static-init / allocator conflict). Each engine therefore gets its own
// bench binary so it runs alone, exactly as the existing vs_duckdb_* / vs_chdb_*
// benches are split.

const ROW_COUNTS: &[usize] = &[100_000, 1_000_000];
/// `ol_number` in CH-benCH q1 has ~15 distinct values; mirror that low cardinality.
const GROUP_COUNT: i64 = 16;
/// `delivery > THRESHOLD` selects ~90% of rows — q1's `ol_delivery_d > <early date>`
/// matches essentially every delivered order line, so the filter is highly
/// selective of NOTHING (a near-pass-through), the hard case for a maintained
/// view (it can't just drop most of the data).
const DELIVERY_MODULUS: i64 = 10_000;
const DELIVERY_THRESHOLD: i64 = 1_000;
/// Rows applied per maintenance delta (a CDC micro-batch).
const DELTA_ROWS: usize = 100;
/// Batch size for loading the base table into the registry (bounded allocation).
const LOAD_BATCH_ROWS: usize = 65_536;

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new("delivery", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Int64, false),
        Field::new("value_sum", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, false),
    ]))
}

fn delivery_filter() -> Arc<dyn PhysicalExpr> {
    let schema = table_schema();
    binary(
        col("delivery", &schema).expect("col delivery"),
        Operator::Gt,
        lit(DELIVERY_THRESHOLD),
        &schema,
    )
    .expect("delivery > threshold predicate")
}

fn filtered_spec() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["grp".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("value".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
        ],
        filter: Some(delivery_filter()),
    }
}

// Deterministic per-row generators, identical on the Cayenne and DuckDB sides so
// the two engines aggregate the exact same data (the correctness gate depends on
// this). Kept trivial so DuckDB can reproduce them in SQL over `range(n)`.
fn grp_of(i: i64) -> i64 {
    i % GROUP_COUNT
}
fn delivery_of(i: i64) -> i64 {
    i % DELIVERY_MODULUS
}
fn value_of(i: i64) -> i64 {
    (i % 2_001) - 1_000
}

fn row_batch(start: i64, count: usize) -> RecordBatch {
    let pk: Vec<i64> = (0..count as i64).map(|j| start + j).collect();
    let grp: Vec<i64> = pk.iter().map(|&i| grp_of(i)).collect();
    let delivery: Vec<i64> = pk.iter().map(|&i| delivery_of(i)).collect();
    let value: Vec<i64> = pk.iter().map(|&i| value_of(i)).collect();
    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(pk)),
            Arc::new(Int64Array::from(grp)),
            Arc::new(Int64Array::from(delivery)),
            Arc::new(Int64Array::from(value)),
        ],
    )
    .expect("row batch")
}

/// Load `rows` into a fresh filtered registry (with PK index) at epoch 1, in
/// bounded batches. Returns the populated registry.
fn load_registry(rows: usize) -> MaintainedAggregateRegistry {
    let schema = table_schema();
    let registry = MaintainedAggregateRegistry::try_new_with_pk(
        std::slice::from_ref(&filtered_spec()),
        &schema,
        &[0],
        usize::MAX,
    )
    .expect("filtered registry");

    let mut batches = Vec::with_capacity(rows.div_ceil(LOAD_BATCH_ROWS));
    let mut start = 0_i64;
    while (start as usize) < rows {
        let count = LOAD_BATCH_ROWS.min(rows - start as usize);
        batches.push(row_batch(start, count));
        start += count as i64;
    }
    registry
        .apply_insert_batches(1, &batches)
        .expect("load registry");
    registry
}

/// Serve the maintained filtered aggregate into a sorted `grp -> (sum, count)`.
fn cayenne_result(registry: &MaintainedAggregateRegistry, epoch: u64) -> BTreeMap<i64, (i64, i64)> {
    let batch = registry
        .batch_for_spec(&filtered_spec(), epoch, output_schema())
        .expect("serve must not error")
        .expect("filtered view must serve at the scan epoch");
    decode_grouped(&batch)
}

fn decode_grouped(batch: &RecordBatch) -> BTreeMap<i64, (i64, i64)> {
    let grp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("grp Int64");
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum Int64");
    let count = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count Int64");
    let mut out = BTreeMap::new();
    for row in 0..batch.num_rows() {
        out.insert(grp.value(row), (sum.value(row), count.value(row)));
    }
    out
}

/// Build an in-memory DuckDB table of `rows` rows via `range()` using the SAME
/// per-row formulas as the Cayenne side.
fn load_duckdb(rows: usize) -> Connection {
    let conn = Connection::open_in_memory().expect("duckdb open_in_memory");
    conn.execute_batch(&format!(
        "CREATE TABLE t AS
           SELECT i AS pk,
                  (i % {GROUP_COUNT}) AS grp,
                  (i % {DELIVERY_MODULUS}) AS delivery,
                  ((i % 2001) - 1000) AS value
           FROM range({rows}) tbl(i);"
    ))
    .expect("duckdb create table");
    conn
}

const DUCKDB_QUERY: &str = "SELECT grp, SUM(value)::BIGINT, COUNT(*)::BIGINT \
     FROM t WHERE delivery > 1000 GROUP BY grp";

fn duckdb_result(conn: &Connection) -> BTreeMap<i64, (i64, i64)> {
    let mut stmt = conn.prepare(DUCKDB_QUERY).expect("duckdb prepare");
    let mut rows = stmt.query([]).expect("duckdb query");
    let mut out = BTreeMap::new();
    // Bounded: at most GROUP_COUNT groups are possible.
    while let Some(row) = rows.next().expect("duckdb row") {
        let grp: i64 = row.get(0).expect("grp");
        let sum: i64 = row.get(1).expect("sum");
        let count: i64 = row.get(2).expect("count");
        out.insert(grp, (sum, count));
    }
    assert!(
        out.len() <= GROUP_COUNT as usize,
        "duckdb returned more groups than possible"
    );
    out
}

fn bench_filtered_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_duckdb_maintained_filtered_groupby");
    group.sample_size(10);

    for &rows in ROW_COUNTS {
        let registry = load_registry(rows);
        let duckdb = load_duckdb(rows);

        // Correctness gate: the maintained filtered serve must EQUAL DuckDB's
        // re-scan-and-filter before either lane is timed.
        let cayenne = cayenne_result(&registry, 1);
        let duck = duckdb_result(&duckdb);
        assert_eq!(
            cayenne, duck,
            "maintained filtered serve diverged from DuckDB at rows={rows}"
        );
        assert!(
            !cayenne.is_empty() && cayenne.len() <= GROUP_COUNT as usize,
            "expected 1..=GROUP_COUNT groups, got {}",
            cayenne.len()
        );

        group.bench_with_input(BenchmarkId::new("duckdb_rescan", rows), &rows, |b, _| {
            b.iter(|| black_box(duckdb_result(&duckdb)));
        });

        group.bench_with_input(BenchmarkId::new("cayenne_serve", rows), &rows, |b, _| {
            b.iter(|| black_box(cayenne_result(&registry, 1)));
        });

        group.bench_with_input(
            BenchmarkId::new("cayenne_maintain_delta", rows),
            &rows,
            |b, _| {
                // Seed once outside the timer (the RwLock-bearing registry can't
                // be cloned per-iter); time only the repeated O(delta) re-upserts
                // of existing PKs, advancing the epoch each apply.
                b.iter_custom(|iterations| {
                    let registry = load_registry(rows);
                    let delta = row_batch(0, DELTA_ROWS);
                    let mut total = std::time::Duration::ZERO;
                    for iteration in 0..iterations {
                        let epoch = 2 + iteration;
                        let start = std::time::Instant::now();
                        registry
                            .apply_insert_batches(epoch, std::slice::from_ref(&delta))
                            .expect("maintain delta");
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_filtered_groupby);
criterion_main!(benches);
