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

//! A selective join whose key IS the probe table's primary key.
//!
//! ## The shape
//!
//! ```sql
//! SELECT ... FROM parent p INNER JOIN child c ON p.id = c.parent_id
//! WHERE c.sel = <one value>          -- matches exactly one child row
//! ```
//!
//! Both tables are large, the filter is on the CHILD and returns one row, the
//! join key on the PARENT side is its declared primary key, and the answer is
//! one row. This is the serving shape behind a high-QPS lookup API: a wide
//! record fetched by a secondary attribute, via its own key.
//!
//! ## What is already good, and must stay good
//!
//! Sideways information passing works end to end: the one-row build side's key
//! reaches the probe scan as a dynamic filter, the min/max conjuncts of that
//! filter push into Vortex, and the probe emits ONE row rather than materialising
//! the wide parent. Only the `InList` membership conjunct is declined — Vortex
//! evaluates it with an O(N×M) `list_contains` kernel
//! (`crates/vortex/src/persistent/opener.rs`) — and declining it costs nothing
//! here because the bounds already collapse to a point range.
//!
//! `selective_join_probe_emits_one_row` in
//! `crates/cayenne/tests/selective_join_pk_probe_test.rs` pins that property.
//! This bench measures what it COSTS.
//!
//! ## What this bench exists to expose
//!
//! The probe still pays file-group fan-out that the equivalent literal lookup
//! does not, because the two reach `scan()` differently:
//!
//!   - `WHERE id = K` arrives as a literal filter, so `is_pk_selective_scan`
//!     (`crates/cayenne/src/provider/table.rs`) recognises point equality on the
//!     PK and suppresses byte-range fan-out.
//!   - A join key is NOT a literal at `scan()` time — it arrives later, as a
//!     dynamic filter. `pk_column_equals_literal` requires `is_literal_like`, so
//!     the suppression never engages and the probe opens every file group.
//!
//! Two lanes measure the same single row through both paths, so the ratio
//! between them is the cost of that gap alone:
//!
//!   - `literal_pk`  — `WHERE p.id = K`, the suppressed-fan-out path.
//!   - `join_pk`     — the join above, resolving to the same `K`.
//!
//! Sizing is deliberate: DataFusion only byte-range splits when each resulting
//! group would still clear `repartition_file_min_size` (10 MB), so the `wide`
//! lane (~131k rows x 2.7 KB ≈ 350 MB) splits at 16 partitions while `narrow`
//! does not. A smaller table shows no fan-out at all and would measure nothing —
//! which is exactly why the companion test can only assert parity.
//!
//! `target_partitions` is swept because fan-out is what is under test; at 1 the
//! two lanes should converge, and the gap at 16 is the cost being paid today.
//!
//! `probe_width` is swept because the production table this models carries two
//! JSON blobs (~2.7 KB/row against a ~197 B/row child, a ~14× width ratio).
//! Width is what makes an unnecessary file open expensive rather than merely
//! wasteful, so a narrow-only bench would understate the gap.
//!
//! A second axis, `file_count`, exists because PK **hash**-sharding on the write
//! path gives every file a `min/max` spanning the whole key domain
//! (`resolved_shard_key_columns`), so listing-time min/max pruning can never
//! drop a file no matter how selective the predicate. Growing the file count at
//! a fixed row count therefore grows the work for a one-row answer — which is
//! the signature of that gap, and what a range-sharded write path would flatten.
//!
//! `cargo bench --bench selective_join_pk_probe -p cayenne --features duckdb-bench`
//! (the shared fixture helper lives under `vs_duckdb_helpers`, hence the feature).

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::CayenneTableProvider;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use common::{CayenneFixture, Metastore, cayenne_insert, setup_cayenne_custom};

/// Parent rows. Large enough that a full scan is obviously wrong for a one-row
/// answer, small enough that the bench stays interactive.
const PARENT_ROWS: usize = 131_072;
/// Child rows, roughly one per parent — the production ratio is ~1.01:1.
const CHILD_ROWS: usize = 131_072;

/// Payload widths for the parent. `narrow` isolates the file-open cost;
/// `wide` reproduces the JSON-blob width that makes a redundant open expensive.
const PROBE_WIDTHS: &[(&str, usize)] = &[("narrow", 0), ("wide", 2_700)];

const TARGET_PARTITIONS: &[usize] = &[1, 4, 16];

fn parent_schema(payload: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ];
    if payload {
        // Stands in for `ConfigurationJSON` + `ModuleTypeConfigurationJSON`.
        fields.push(Field::new("payload", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn child_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("child_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, false),
        // The selective attribute the API looks a record up by.
        Field::new("sel", DataType::Utf8, false),
    ]))
}

#[expect(
    dead_code,
    reason = "kept alongside child_batch for symmetry when adding lanes"
)]
fn parent_batch(schema: Arc<Schema>, rows: usize, payload_bytes: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();
    let mut cols: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(Int64Array::from(values)),
    ];
    if payload_bytes > 0 {
        // Per-row unique so it cannot dictionary-compress to nothing — the
        // production blobs are distinct per row.
        let payloads: Vec<String> = ids
            .iter()
            .map(|id| format!("{id:0width$}", width = payload_bytes))
            .collect();
        cols.push(Arc::new(StringArray::from(payloads)));
    }
    RecordBatch::try_new(schema, cols).expect("parent batch")
}

fn child_batch(schema: Arc<Schema>, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let parent_ids: Vec<i64> = ids.clone();
    // Unique per row, so `sel = 'sel_K'` matches exactly one child — the
    // unique-ish `(DeveloperAccountSid, RemoteEntityId)` pair in production.
    let sels: Vec<String> = ids.iter().map(|id| format!("sel_{id}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(parent_ids)),
            Arc::new(StringArray::from(sels)),
        ],
    )
    .expect("child batch")
}

/// Load the pair. `file_count` writes the parent in that many separate inserts,
/// each producing its own Vortex file, so the sweep can show what an extra file
/// costs a one-row answer when min/max pruning cannot drop it.
async fn load_pair(payload_bytes: usize, file_count: usize) -> (CayenneFixture, CayenneFixture) {
    let p_schema = parent_schema(payload_bytes > 0);
    let parent = setup_cayenne_custom(
        "sjp_parent",
        Metastore::Sqlite,
        vec!["id".to_string()],
        None,
        Arc::clone(&p_schema),
        cayenne::metadata::VortexConfig::default(),
        Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
    )
    .await;

    let per = PARENT_ROWS / file_count;
    for f in 0..file_count {
        let start = (f * per) as i64;
        let ids: Vec<i64> = (0..per as i64).map(|i| start + i).collect();
        let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();
        let mut cols: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(values)),
        ];
        if payload_bytes > 0 {
            let payloads: Vec<String> = ids
                .iter()
                .map(|id| format!("{id:0width$}", width = payload_bytes))
                .collect();
            cols.push(Arc::new(StringArray::from(payloads)));
        }
        let batch = RecordBatch::try_new(Arc::clone(&p_schema), cols).expect("parent chunk");
        let _ = cayenne_insert(&parent.table, batch).await;
    }

    let child = setup_cayenne_custom(
        "sjp_child",
        Metastore::Sqlite,
        vec!["child_id".to_string()],
        None,
        child_schema(),
        cayenne::metadata::VortexConfig::default(),
        Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
    )
    .await;
    let _ = cayenne_insert(&child.table, child_batch(child_schema(), CHILD_ROWS)).await;

    (parent, child)
}

async fn query(
    parent: &Arc<CayenneTableProvider>,
    child: &Arc<CayenneTableProvider>,
    sql: &str,
    target_partitions: usize,
) -> Vec<RecordBatch> {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("p", Arc::clone(parent) as Arc<dyn TableProvider>)
        .expect("register parent");
    ctx.register_table("c", Arc::clone(child) as Arc<dyn TableProvider>)
        .expect("register child");
    ctx.sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

/// `literal_pk` vs `join_pk` across `target_partitions` and probe width.
///
/// Both lanes resolve the SAME single row, so their ratio is the cost of
/// reaching the probe through a dynamic filter instead of a literal.
fn bench_literal_vs_join(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    // A key in the middle, so no lane wins by landing in the first file.
    let key = (PARENT_ROWS / 2) as i64;

    for (width_label, payload) in PROBE_WIDTHS {
        let (parent, child) = rt.block_on(load_pair(*payload, 4));
        let mut group = c.benchmark_group(format!("selective_join_pk_probe/{width_label}"));

        for &tp in TARGET_PARTITIONS {
            group.bench_with_input(BenchmarkId::new("literal_pk", tp), &tp, |b, &tp| {
                let sql = format!("SELECT p.id, p.value FROM p WHERE p.id = {key}");
                b.to_async(&rt).iter(|| async {
                    black_box(query(&parent.table, &child.table, &sql, tp).await)
                });
            });
            group.bench_with_input(BenchmarkId::new("join_pk", tp), &tp, |b, &tp| {
                let sql = format!(
                    "SELECT p.id, p.value FROM p INNER JOIN c ON p.id = c.parent_id \
                     WHERE c.sel = 'sel_{key}'"
                );
                b.to_async(&rt).iter(|| async {
                    black_box(query(&parent.table, &child.table, &sql, tp).await)
                });
            });
        }
        group.finish();
    }
}

/// Cost of the same one-row answer as the parent is split across more files.
///
/// Flat is the goal. A rising curve is PK hash-sharding denying listing-time
/// min/max pruning: every file's key range spans the domain, so none can be
/// dropped and each new file is another open on the way to one row.
fn bench_file_count_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let key = (PARENT_ROWS / 2) as i64;
    let mut group = c.benchmark_group("selective_join_pk_probe/file_count");

    for &files in &[1_usize, 4, 16] {
        let (parent, child) = rt.block_on(load_pair(0, files));
        group.bench_with_input(BenchmarkId::new("join_pk", files), &files, |b, _| {
            let sql = format!(
                "SELECT p.id, p.value FROM p INNER JOIN c ON p.id = c.parent_id \
                 WHERE c.sel = 'sel_{key}'"
            );
            b.to_async(&rt)
                .iter(|| async { black_box(query(&parent.table, &child.table, &sql, 16).await) });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_literal_vs_join, bench_file_count_scaling);
criterion_main!(benches);
