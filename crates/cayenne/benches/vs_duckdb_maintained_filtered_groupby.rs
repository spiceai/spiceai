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
//! The Cayenne-side fixture is shared with the chDB sibling via
//! `maintained_filtered_helpers/common.rs`. The chDB comparison is a SEPARATE bench
//! binary because DuckDB and chDB (both bundled) abort if driven in one process.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

#[path = "maintained_filtered_helpers/common.rs"]
mod common;

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use duckdb::Connection;

use common::{DELIVERY_MODULUS, GROUP_COUNT, ROW_COUNTS, cayenne_result, load_registry, row_batch};

/// Rows applied per maintenance delta (a CDC micro-batch).
const DELTA_ROWS: usize = 100;

const DUCKDB_QUERY: &str = "SELECT grp, SUM(value)::BIGINT, COUNT(*)::BIGINT \
     FROM t WHERE delivery > 1000 GROUP BY grp";

/// Build an in-memory DuckDB table of `rows` rows via `range()` using the SAME
/// per-row formulas as the Cayenne side (see `common::row_batch`).
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
