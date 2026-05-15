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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

struct BenchTable {
    _temp_dir: TempDir,
    table: Arc<CayenneTableProvider>,
    schema: Arc<Schema>,
}

async fn setup_table(table_name: &str) -> BenchTable {
    setup_table_with_options(table_name, vec![], None).await
}

async fn setup_pk_upsert_table(table_name: &str) -> BenchTable {
    setup_table_with_options(
        table_name,
        vec!["id".to_string()],
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    )
    .await
}

async fn setup_table_with_options(
    table_name: &str,
    primary_key: Vec<String>,
    on_conflict: Option<OnConflict>,
) -> BenchTable {
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

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key,
                on_conflict,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
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

async fn setup_table_with_inline_segments(table_name: &str, segments: usize) -> BenchTable {
    let bench_table = setup_table(table_name).await;
    for segment in 0..segments {
        let batch = make_batch(Arc::clone(&bench_table.schema), segment as i64, 1);
        let written = append_batch(&bench_table.table, batch).await;
        assert_eq!(written, 1);
    }
    bench_table
}

async fn setup_pk_upsert_table_with_seed(table_name: &str) -> BenchTable {
    let bench_table = setup_pk_upsert_table(table_name).await;
    let batch = make_batch(Arc::clone(&bench_table.schema), 0, 1);
    let written = append_batch(&bench_table.table, batch).await;
    assert_eq!(written, 1);
    bench_table
}

fn make_batch(schema: Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let ids = (start..start + rows as i64).collect::<Vec<_>>();
    let names = ids
        .iter()
        .map(|id| format!("name_{id}"))
        .collect::<Vec<_>>();
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
    let schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
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

fn bench_append_roundtrip(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    let mut inline = c.benchmark_group("mutation_writer_inline_append_roundtrip");
    inline.sample_size(10);
    for rows in [1_usize, 128, 1_024] {
        inline.throughput(Throughput::Elements(rows as u64));
        inline.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_batched(
                || rt.block_on(setup_table("bench_inline_append")),
                |bench_table| {
                    rt.block_on(async move {
                        let batch = make_batch(Arc::clone(&bench_table.schema), 0, rows);
                        let written = append_batch(&bench_table.table, batch).await;
                        black_box((bench_table, written));
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    inline.finish();

    let mut fallback = c.benchmark_group("mutation_writer_vortex_fallback_roundtrip");
    fallback.sample_size(10);
    for rows in [1_025_usize, 4_096] {
        fallback.throughput(Throughput::Elements(rows as u64));
        fallback.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_batched(
                || rt.block_on(setup_table("bench_vortex_fallback_append")),
                |bench_table| {
                    rt.block_on(async move {
                        let batch = make_batch(Arc::clone(&bench_table.schema), 0, rows);
                        let written = append_batch(&bench_table.table, batch).await;
                        black_box((bench_table, written));
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    fallback.finish();
}

fn bench_inline_mutation_paths(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    let mut upsert = c.benchmark_group("mutation_writer_inline_pk_upsert_roundtrip");
    upsert.sample_size(10);
    upsert.throughput(Throughput::Elements(1));
    upsert.bench_function("single_row_inline_rewrite", |b| {
        b.iter_batched(
            || rt.block_on(setup_pk_upsert_table_with_seed("bench_inline_pk_upsert")),
            |bench_table| {
                rt.block_on(async move {
                    let batch = make_batch(Arc::clone(&bench_table.schema), 0, 1);
                    let written = append_batch(&bench_table.table, batch).await;
                    black_box((bench_table, written));
                });
            },
            BatchSize::SmallInput,
        );
    });
    upsert.finish();

    let mut pressure = c.benchmark_group("mutation_writer_inline_memtable_pressure_append");
    pressure.sample_size(10);
    pressure.throughput(Throughput::Elements(1));
    for (case, preexisting_segments) in [
        ("below_segment_threshold", 63_usize),
        ("segment_pressure_checkpoint", 64_usize),
    ] {
        pressure.bench_with_input(
            BenchmarkId::from_parameter(case),
            &preexisting_segments,
            |b, &preexisting_segments| {
                b.iter_batched(
                    || {
                        rt.block_on(setup_table_with_inline_segments(
                            "bench_inline_memtable_pressure",
                            preexisting_segments,
                        ))
                    },
                    |bench_table| {
                        rt.block_on(async move {
                            let batch = make_batch(
                                Arc::clone(&bench_table.schema),
                                preexisting_segments as i64,
                                1,
                            );
                            let written = append_batch(&bench_table.table, batch).await;
                            black_box((bench_table, written));
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    pressure.finish();
}

/// Benchmarks the directory durability primitives added for ACID correctness
/// on local FS (parent-directory `sync_all` after `create_dir_all` for
/// snapshot directories, _partitioned_wal/, and deletions/ subdirs).
///
/// These one-time-per-snapshot or per-table costs are the direct result of
/// the durability hardening. The benchmark quantifies the "tax" for Q21
/// workloads that trigger frequent compactions or cross-partition operations.
fn bench_directory_durability_primitives(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("directory_durability_sync_all");
    // These are one-time operations; a smaller sample size is sufficient
    // to get stable numbers without making the bench too slow.
    group.sample_size(30);

    group.bench_function("create_dir_all_plus_parent_sync", |b| {
        b.iter_batched(
            || {
                let temp = tempfile::tempdir().expect("tempdir for bench");
                let parent = temp.path().to_path_buf();
                let child = parent.join("new_snapshot_or_wal_or_deletions_dir");
                (temp, parent, child)
            },
            |(_keep_alive, parent, child)| {
                rt.block_on(async {
                    // Replicate the exact hardened pattern used in
                    // ensure_snapshot_dir_exists, ensure_partitioned_wal_dir_and_sync_parent,
                    // and the deletions/ subdir creation in DeletionVectorWriter.
                    if !child.exists() {
                        tokio::fs::create_dir_all(&child).await.expect("create_dir_all");
                        let p = parent.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            std::fs::File::open(&p).and_then(|f| f.sync_all())
                        })
                        .await;
                    }
                    black_box(child);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_append_roundtrip,
    bench_inline_mutation_paths,
    bench_directory_durability_primitives
);
criterion_main!(benches);
