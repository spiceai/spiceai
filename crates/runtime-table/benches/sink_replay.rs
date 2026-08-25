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

//! Benchmarks `AccelerationSink::insert_into` -- the non-CDC (full/append
//! refresh) write boundary every accelerated dataset that isn't `changes`
//! mode goes through (`crates/runtime-table/src/accelerated/sink/mod.rs`).
//! Unlike the CDC path (the `cdc_cayenne_inline` bench in `runtime`), no
//! bench exercised this seam before this file, even though it also carries
//! the always-on `SchemaCastScanExec` wrapping (`sink/table.rs`) every
//! full/append write pays.
//!
//! Two accelerators: a bare `MemTable` (the floor -- no accelerator-side
//! write machinery at all) and a real Cayenne table (tempdir + SQLite
//! metastore), so the delta between them isolates Cayenne's own write-path
//! overhead from the sink/schema-cast plumbing every accelerator shares.

#![cfg(not(windows))]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::stream as fstream;
use runtime_table::accelerated::AccelerationSink;
use tempfile::TempDir;
use tokio::runtime::Runtime as TokioRuntime;

const ROWS: usize = 8192;
const BATCH_SIZE: usize = 1024;

fn data_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batches(schema: &SchemaRef, rows: usize, batch_size: usize) -> Vec<RecordBatch> {
    (0..rows)
        .collect::<Vec<usize>>()
        .chunks(batch_size)
        .map(|chunk| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
                chunk.iter().map(|&i| i as i64),
            ));
            let names: ArrayRef = Arc::new(StringArray::from_iter_values(
                chunk.iter().map(|&i| format!("row-{i}")),
            ));
            RecordBatch::try_new(Arc::clone(schema), vec![ids, names]).expect("record batch")
        })
        .collect()
}

fn batches_to_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Pin<Box<dyn RecordBatchStream + Send>> {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        fstream::iter(batches.into_iter().map(Ok)),
    ))
}

async fn make_mem_table(schema: SchemaRef) -> Arc<dyn TableProvider> {
    Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("empty MemTable"))
}

struct CayenneFixture {
    _temp: TempDir,
    table: Arc<dyn TableProvider>,
}

async fn make_cayenne_table(schema: SchemaRef) -> CayenneFixture {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp.path().join("test.db");
    let conn = format!("sqlite://{}", db_path.to_string_lossy());

    let catalog = Arc::new(CayenneCatalog::new(conn).expect("CayenneCatalog::new"));
    catalog.init().await.expect("catalog init");

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "bench_sink".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create_table");

    CayenneFixture {
        _temp: temp,
        table: Arc::new(table),
    }
}

fn bench_sink_append(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let schema = data_schema();

    let mut group = c.benchmark_group("accel_sink_append");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("mem_table", |b| {
        b.iter_batched(
            || rt.block_on(make_mem_table(Arc::clone(&schema))),
            |provider| {
                rt.block_on(async {
                    let sink = AccelerationSink::new(provider);
                    let stream = batches_to_stream(
                        Arc::clone(&schema),
                        make_batches(&schema, ROWS, BATCH_SIZE),
                    );
                    sink.insert_into(stream, InsertOp::Append)
                        .await
                        .expect("insert_into");
                    black_box(sink);
                });
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("cayenne", |b| {
        b.iter_batched(
            || rt.block_on(make_cayenne_table(Arc::clone(&schema))),
            |fixture| {
                rt.block_on(async {
                    let sink = AccelerationSink::new(fixture.table);
                    let stream = batches_to_stream(
                        Arc::clone(&schema),
                        make_batches(&schema, ROWS, BATCH_SIZE),
                    );
                    sink.insert_into(stream, InsertOp::Append)
                        .await
                        .expect("insert_into");
                    black_box((sink, fixture._temp));
                });
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_sink_append);
criterion_main!(benches);
