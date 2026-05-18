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

//! Regression bench: `MERGE INTO` key-probe scan reads every column of every
//! row instead of only the key columns.
//!
//! ## What this bench measures
//!
//! `CayenneTableProvider::delete_matched_rows_by_key_probe` is the fast path
//! taken by `MERGE INTO` on `PositionBased` tables
//! (`crates/cayenne/src/provider/table.rs:7832-7874`). It fans out to
//! `CayenneDeletionSink::delete_by_key_hash_probe` →
//! `scan_file_for_key_matches`
//! (`crates/cayenne/src/provider/delete/sink/position_based.rs:458-579`).
//!
//! `scan_file_for_key_matches` opens each Vortex file and, in the current
//! code, runs `vxf.scan()` with **no projection**
//! (`position_based.rs:494`). The docstring at `:449-453` explains the intent:
//!
//! > The scan reads **all columns** (no projection) because Vortex's
//! > `with_projection` API takes a single `Expression` and may not support
//! > mixed `data+row_idx` projections.
//!
//! This is **outdated** — `vortex::expr::select(&[col_name, ...], root())`
//! exists (see `vortex-array/src/expr/exprs.rs:440` and the test at
//! `vortex-file/src/tests.rs:290`) and only requires data columns. The probe
//! does not need `row_idx`; it tracks `row_position` manually (`:507`,
//! `:574`).
//!
//! Result: per-file scan cost grows with **total** column count, not with
//! the number of key columns. A 1-key MERGE against a table with N data
//! columns pays N× the data-read cost it should.
//!
//! ## Variants
//!
//! Two table shapes, identical row count, identical 1024-key probe set:
//!
//! - `wide/<rows>` — schema with `id` (key) + 31 payload columns. The
//!   payload columns are pure overhead: the probe never inspects them.
//! - `narrow/<rows>` — schema with `id` (key) + 2 payload columns. Matches
//!   the canonical bench schema.
//!
//! With the existing code, `wide/<rows>` should run ~3-10× slower per
//! delete than `narrow/<rows>` despite doing the same logical work. Once
//! `scan_file_for_key_matches` projects only `key_columns`, the wide /
//! narrow gap should collapse to within ~1.2× (allocator + dispatch
//! overhead only).
//!
//! ## Why this is a regression bench and not Cayenne-vs-DuckDB
//!
//! DuckDB has no analogue to `delete_by_key_hash_probe`: it rewrites the
//! affected page directly. A cross-engine ratio here would conflate
//! "different storage model" with "Cayenne pays for columns it doesn't
//! read." Keeping the bench Cayenne-only makes the regression unambiguous —
//! the only knob being measured is the projection.
//!
//! ## How to read
//!
//! ```ignore
//! cargo bench --bench wide_table_key_probe_scan -p cayenne
//! ```
//!
//! Compare the `wide/<rows>` lane against `narrow/<rows>` at the same row
//! count. The ratio is the headroom available from pushing a column
//! projection into the Vortex scan in `scan_file_for_key_matches`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ROW_COUNTS: &[usize] = &[16_384, 65_536];
const MATCHED_KEYS: usize = 1_024;
const WIDE_PAYLOAD_COLUMNS: usize = 31;
const NARROW_PAYLOAD_COLUMNS: usize = 2;

fn build_schema(payload_columns: usize) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(payload_columns + 1);
    fields.push(Field::new("id", DataType::Int64, false));
    for col in 0..payload_columns {
        fields.push(Field::new(format!("v{col}"), DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}

fn build_batch(schema: &Arc<Schema>, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let mut columns: Vec<arrow::array::ArrayRef> =
        Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(Int64Array::from(ids)));
    // Payload columns: a different deterministic int per (row, col). Anything
    // big enough that read-all-columns has a non-trivial cost.
    for col in 0..schema.fields().len() - 1 {
        let offset = (col as i64 + 1) * 1_000;
        let values: Vec<i64> = (0..rows as i64).map(|i| i + offset).collect();
        columns.push(Arc::new(Int64Array::from(values)));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).expect("record batch")
}

struct Fixture {
    _temp_dir: TempDir,
    table: Arc<CayenneTableProvider>,
}

async fn build_fixture(payload_columns: usize, rows: usize) -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy()))
            .expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let schema = build_schema(payload_columns);
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            CreateTableOptions {
                table_name: format!("wide_probe_{payload_columns}"),
                schema: Arc::clone(&schema),
                // Empty primary_key → PositionBased deletion strategy
                // (deletion_strategy.rs:263). PositionBased is required for
                // `delete_matched_rows_by_key_probe` to dispatch to the
                // bug-affected fast path.
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("create_table"),
    );

    // Single-batch load. The bug is per-column-per-row data read; one file is
    // sufficient and keeps the bench's wall-clock bounded.
    let batch = build_batch(&schema, rows);
    let ctx = SessionContext::new();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
        .expect("memory exec");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let _ = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("insert collect");

    Fixture {
        _temp_dir: temp_dir,
        table,
    }
}

/// Build the matched-keys set used by every iteration. The keys are spread
/// uniformly across the row id space so the probe touches the whole file
/// rather than terminating early.
fn build_matched_keys(rows: usize) -> HashSet<Vec<ScalarValue>> {
    let stride = rows / MATCHED_KEYS;
    let mut keys: HashSet<Vec<ScalarValue>> = HashSet::with_capacity(MATCHED_KEYS);
    for i in 0..MATCHED_KEYS {
        let id = (i * stride) as i64;
        keys.insert(vec![ScalarValue::Int64(Some(id))]);
    }
    keys
}

fn bench_key_probe_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("wide_table_key_probe_scan");
    group.sample_size(10);

    let key_columns = vec!["id".to_string()];

    for &rows in ROW_COUNTS {
        let matched_keys = build_matched_keys(rows);

        // --- narrow: id + 2 payload cols ---
        group.bench_with_input(BenchmarkId::new("narrow", rows), &rows, |b, &_| {
            b.iter_batched(
                || rt.block_on(build_fixture(NARROW_PAYLOAD_COLUMNS, rows)),
                |fixture| {
                    rt.block_on(async {
                        let deleted = fixture
                            .table
                            .delete_matched_rows_by_key_probe(
                                matched_keys.clone(),
                                &key_columns,
                            )
                            .await
                            .expect("narrow delete");
                        black_box(deleted);
                    });
                },
                BatchSize::SmallInput,
            );
        });

        // --- wide: id + 31 payload cols (matches a TPC-H-ish "lineitem"
        // shape and is wide enough to make the per-column-per-row data read
        // dominate the wall clock). ---
        group.bench_with_input(BenchmarkId::new("wide", rows), &rows, |b, &_| {
            b.iter_batched(
                || rt.block_on(build_fixture(WIDE_PAYLOAD_COLUMNS, rows)),
                |fixture| {
                    rt.block_on(async {
                        let deleted = fixture
                            .table
                            .delete_matched_rows_by_key_probe(
                                matched_keys.clone(),
                                &key_columns,
                            )
                            .await
                            .expect("wide delete");
                        black_box(deleted);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_key_probe_scan);
criterion_main!(benches);
