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

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;
use tokio::runtime::Runtime;

struct BenchTable {
    _temp_dir: TempDir,
    table: Arc<CayenneTableProvider>,
    schema: Arc<Schema>,
}

async fn setup_table(table_name: &str) -> BenchTable {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("data dir");
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
                primary_key: vec![],
                on_conflict: None,
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

criterion_group!(benches, bench_append_roundtrip);
criterion_main!(benches);
