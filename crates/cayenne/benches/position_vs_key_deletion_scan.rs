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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

//! Read-tax bench: scanning a PK upsert table that has pending deletions, under
//! `deletion_mode: key` vs `deletion_mode: position`.
//!
//! Key mode applies deletes ABOVE the Vortex scan ({Int64Pk,KeyBased}
//! DeletionFilterExec) — a `RowConverter`/probe tax over every scanned row.
//! Position mode pushes per-file position bitmaps INTO the Vortex scan
//! (`Selection::ExcludeRoaring`), skipping deleted pages with no per-row CPU.
//!
//! Expectation: at `deletion_fraction = 0` the two are ~equal; at a non-zero
//! fraction `key` slows down (per-row tax over the full scan) while `position`
//! stays flat. This models the `cluster C` read-tax as a Q6-shaped
//! `SELECT SUM(value)`. Self-contained (no DuckDB), so it runs with a plain
//! `cargo bench --bench position_vs_key_deletion_scan`.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const SCAN_ROWS: usize = 262_144;
const DELETION_FRACTIONS: &[f64] = &[0.0, 0.25];

struct Fixture {
    table: Arc<CayenneTableProvider>,
    _dir: TempDir,
}

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(schema: &Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows as i64).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("name_{id}")).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 2).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn append_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert collect");
}

async fn query_sum(table: &Arc<CayenneTableProvider>) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql("SELECT SUM(value) FROM t")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

async fn setup(mode: DeletionMode, rows: usize, frac: f64) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let data_path = dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let schema = bench_schema();
    let ctx = SessionContext::new();
    let vortex_config = VortexConfig {
        deletion_mode: mode,
        // Disable inlining so rows materialize as Vortex files (position deletes
        // apply to files; inlined rows use the inline-rewrite path).
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        ..VortexConfig::default()
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "scan_del".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create table"),
    );

    append_batch(&table, make_batch(&schema, 0, rows)).await;

    let n_del = (rows as f64 * frac) as usize;
    if n_del > 0 {
        // Capture positions, then upsert the first `n_del` ids with new values
        // (delete-old + insert-new). Under position mode this records position
        // deletes; under key mode it records key deletes.
        table.run_position_capture().await.expect("capture");
        append_batch(&table, make_batch(&schema, 0, n_del)).await;
        table.run_position_capture().await.expect("capture");
    }

    Fixture { table, _dir: dir }
}

fn bench_deletion_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("position_vs_key_deletion_scan");
    group.sample_size(10);

    let modes: [(&str, DeletionMode); 2] = [
        ("key", DeletionMode::Key),
        ("position", DeletionMode::Position),
    ];

    for &frac in DELETION_FRACTIONS {
        for (label, mode) in modes {
            let fixture = Arc::new(runtime.block_on(setup(mode, SCAN_ROWS, frac)));
            group.bench_with_input(
                BenchmarkId::new(label, format!("del{frac:.2}")),
                &frac,
                |b, _frac| {
                    b.iter(|| {
                        let batches = runtime.block_on(query_sum(&fixture.table));
                        black_box(batches);
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_deletion_scan);
criterion_main!(benches);
