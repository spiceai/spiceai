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

//! Regression bench: per-append cost when `sort_columns` is configured.
//!
//! Demonstrates that a single append on a table with `cayenne_sort_columns`
//! set scales **linearly with the preloaded table size**, because
//! `AppendMutationWriter::sort_if_configured` (`provider/mutation_writer.rs:638`)
//! calls `CayenneTableProvider::sort_and_rewrite_data`
//! (`provider/table.rs:4272`) on every successful write. That helper reads
//! every row in the current snapshot, runs them through DataFusion's
//! `SortExec`, and rewrites them into a new snapshot via
//! `commit_compaction` — i.e. `O(N log N)` per burst, where `N` is the full
//! table row count, *not* the burst row count.
//!
//! For comparison the bench also measures the same append on an otherwise
//! identical table without `sort_columns`. That lane stays roughly constant
//! in the preload size because the append path is `O(K)` where `K` is the
//! incoming row count (no full-table rewrite).
//!
//! ## Why this matters
//!
//! Sustained CDC ingestion on a sort-column table currently hits this cost
//! on every coalesced burst. On a 50 M-row table at SF100, a single burst
//! drives hundreds of MB of read + write before the LSN can be acked. The
//! benchmark is intentionally a regression test: it asserts (via Criterion's
//! report) that the sorted-append cost grows with preload size while the
//! unsorted lane does not. A future fix that moves the sort to compaction
//! (level-0 unsorted writes → background level-N sorted compaction) should
//! make the sorted curve match the unsorted one.
//!
//! ## How to read the report
//!
//! Criterion will produce one group `sorted_append_overhead/{lane}/{preload}`.
//! Look for:
//! - `unsorted/<preload>` time roughly constant across preload sizes — the
//!   `O(K)` baseline.
//! - `sorted/<preload>` time growing roughly linearly with preload size —
//!   the `O(N log N)` regression.
//!
//! The append payload size is held constant at [`APPEND_ROWS`] across all
//! cases so the only varying input is the preloaded table size.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const APPEND_ROWS: usize = 1_024;
const PRELOAD_SIZES: &[usize] = &[8_192, 65_536, 524_288];

struct BenchTable {
    _temp_dir: TempDir,
    table: Arc<CayenneTableProvider>,
    schema: Arc<Schema>,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(schema: Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let ids = (start..start + rows as i64).collect::<Vec<_>>();
    let names = ids.iter().map(|id| format!("name_{id}")).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch")
}

async fn append_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    let ctx = SessionContext::new();
    let input_schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], input_schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert collect");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |rows| rows.value(0))
}

async fn setup_table(table_name: &str, sorted: bool) -> BenchTable {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("bench.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let mut vortex_config = VortexConfig::default();
    if sorted {
        vortex_config.sort_columns = vec!["id".to_string()];
    }

    let ctx = SessionContext::new();
    let schema = schema();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("table"),
    );

    BenchTable {
        _temp_dir: temp_dir,
        table,
        schema,
    }
}

/// Preload `rows` rows into the table, chunked so no single insert
/// dominates the preload time and so that the sorted-table preload itself
/// is representative of steady-state apply (each chunk triggers
/// `sort_and_rewrite_data` exactly the way a CDC burst would).
async fn preload(bench: &BenchTable, rows: usize) {
    const CHUNK: usize = 4_096;
    let mut written: usize = 0;
    while written < rows {
        let this_chunk = CHUNK.min(rows - written);
        let batch = make_batch(Arc::clone(&bench.schema), written as i64, this_chunk);
        let n = append_batch(&bench.table, batch).await;
        assert_eq!(n as usize, this_chunk);
        written += this_chunk;
    }
}

fn bench_sorted_append_overhead(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("sorted_append_overhead");
    // Sorted preloads at the largest preload size dominate setup time; keep
    // the sample count low to bound total bench wall time.
    group.sample_size(10);

    for &preload_rows in PRELOAD_SIZES {
        group.throughput(Throughput::Elements(APPEND_ROWS as u64));

        for sorted in [false, true] {
            let lane = if sorted { "sorted" } else { "unsorted" };

            group.bench_with_input(
                BenchmarkId::new(lane, preload_rows),
                &preload_rows,
                |b, &preload_rows| {
                    b.iter_batched(
                        || {
                            rt.block_on(async {
                                let bench = setup_table("sorted_append_bench", sorted).await;
                                preload(&bench, preload_rows).await;
                                bench
                            })
                        },
                        |bench| {
                            rt.block_on(async {
                                let batch = make_batch(
                                    Arc::clone(&bench.schema),
                                    preload_rows as i64,
                                    APPEND_ROWS,
                                );
                                let written = append_batch(&bench.table, batch).await;
                                black_box((bench, written));
                            });
                        },
                        // Preload reuses a fresh temp dir per iteration; the
                        // sorted-preload cost is bounded by APPEND_ROWS but
                        // the dataset is reset between iterations, so use
                        // `PerIteration` rather than the cheaper
                        // `LargeInput`.
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_sorted_append_overhead);
criterion_main!(benches);
