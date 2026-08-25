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

//! End-to-end throughput benchmark for the CDC apply pipeline writing into a
//! Cayenne accelerator via the data-inlining write path.
//!
//! Two benchmark groups:
//!
//! 1. **`cdc_cayenne_inline`** — each iteration builds a synthetic
//!    `ChangesStream` of N create-op envelopes, runs them through
//!    `RefreshTask::start_changes_stream` (which creates the bounded
//!    prefetch channel internally), and times the full apply+commit loop
//!    end to end. Throughput is reported in envelopes/sec via
//!    `criterion::Throughput::Elements`.
//! 2. **`cdc_fixture_replay`** — the same apply pipeline, but replaying a
//!    *recorded* changeset instead of synthesizing one: an Arrow IPC file of
//!    `changes_schema`-shaped batches (columns `op`/`primary_keys`/`data`),
//!    e.g. produced once from a captured MySQL binlog (see
//!    `data_components::mysql_replication` for the decode side) or any other
//!    CDC source. This is what makes the group above's uniform create-only
//!    workload comparable against a real op-mix/table-shape capture. The
//!    fixture is intentionally not committed (large, source-specific);
//!    point `CDC_FIXTURE_FILE` at one — without it this group prints
//!    capture instructions and exits successfully, so it is safe under
//!    `cargo bench` in CI.

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError,
    changes_schema,
};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use futures::StreamExt;
use futures::stream as fstream;
use runtime::accelerated::refresh::Refresh;
use runtime::accelerated::refresh_task::{RefreshTask, RefreshTaskBuilder};
use runtime::federated::FederatedTable;
use runtime::status::RuntimeStatus;
use tempfile::TempDir;
use tokio::runtime::{Handle, Runtime as TokioRuntime};
use tokio::sync::RwLock;

struct NoopCommitter;

#[async_trait]
impl CommitChange for NoopCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_create_envelope(id: i64) -> ChangeEnvelope {
    let data = data_schema();
    let wrapper = changes_schema(&data);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec!["c"]));

    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_array: ArrayRef = Arc::new(
        ListArray::try_new(
            pk_field,
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(StringArray::from(vec!["id"])),
            None,
        )
        .expect("ListArray"),
    );

    let id_array: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![Some(format!("row-{id}"))]));
    let data_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int64, false)), id_array),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            name_array,
        ),
    ]));

    let record = RecordBatch::try_new(Arc::new(wrapper), vec![op_array, pk_array, data_array])
        .expect("wrapper RecordBatch");
    let batch = ChangeBatch::try_new(record).expect("ChangeBatch");

    ChangeEnvelope::new(Box::new(NoopCommitter), batch, false)
}

fn make_n_envelopes(n: usize) -> ChangesStream {
    let envelopes: Vec<Result<ChangeEnvelope, StreamError>> = (0..n)
        .map(|i| {
            Ok(make_create_envelope(
                i64::try_from(i).expect("usize fits in i64"),
            ))
        })
        .collect();
    fstream::iter(envelopes).boxed()
}

struct CayenneFixture {
    _temp: TempDir,
    table: Arc<CayenneTableProvider>,
    table_name: String,
}

async fn make_cayenne_fixture_for_schema(table_name: &str, schema: Arc<Schema>) -> CayenneFixture {
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
            table_name: table_name.to_string(),
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
        table_name: table_name.to_string(),
    }
}

async fn make_cayenne_fixture(table_name: &str) -> CayenneFixture {
    make_cayenne_fixture_for_schema(table_name, data_schema()).await
}

/// Reads a changeset fixture: an Arrow IPC file of `changes_schema`-shaped
/// batches, one batch per recorded change.
fn read_fixture_batches(path: &Path) -> Vec<RecordBatch> {
    let file =
        std::fs::File::open(path).unwrap_or_else(|e| panic!("failed to open {path:?}: {e}"));
    let reader = FileReader::try_new(file, None).expect("valid Arrow IPC file");
    reader
        .collect::<Result<Vec<RecordBatch>, _>>()
        .expect("valid Arrow IPC batches")
}

/// Recovers the dataset's own schema (pre-CDC-wrapping) from a single
/// wrapper batch's `data` struct column, so the Cayenne table can be created
/// with a schema matching the fixture, whatever that schema is.
fn data_schema_from_wrapper(record: &RecordBatch) -> Arc<Schema> {
    let data_field = record
        .schema()
        .field_with_name("data")
        .expect("wrapper batch has a 'data' column")
        .clone();
    match data_field.data_type() {
        DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
        other => panic!("expected 'data' column to be a Struct, found {other:?}"),
    }
}

fn make_fixture_stream(batches: Vec<RecordBatch>) -> ChangesStream {
    let envelopes: Vec<Result<ChangeEnvelope, StreamError>> = batches
        .into_iter()
        .map(|record| {
            let batch = ChangeBatch::try_new(record).expect("ChangeBatch");
            Ok(ChangeEnvelope::new(Box::new(NoopCommitter), batch, false))
        })
        .collect();
    fstream::iter(envelopes).boxed()
}

fn print_fixture_instructions(reason: &str) {
    println!(
        "cdc_fixture_replay: skipped ({reason}).\n\
         \n\
         This bench replays a recorded changeset instead of a synthetic\n\
         stream. To produce one, capture a real source's ChangesStream and\n\
         serialize the `ChangeBatch::record` of each envelope to an Arrow IPC\n\
         file (`arrow::ipc::writer::FileWriter`, one `write()` call per\n\
         batch, then `finish()`) — every batch must share the\n\
         `data_components::cdc::changes_schema` wrapper shape\n\
         (op/primary_keys/data columns).\n\
         \n\
         Then run:\n\
            CDC_FIXTURE_FILE=/path/to/changeset.arrows \\\n\
              cargo bench -p runtime --bench cdc_cayenne_inline -- fixture_replay"
    );
}

fn make_refresh_task(fixture: &CayenneFixture) -> RefreshTask {
    let accelerator: Arc<dyn TableProvider> = Arc::clone(&fixture.table) as _;
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
    RefreshTaskBuilder::new(
        RuntimeStatus::new(),
        TableReference::bare(fixture.table_name.clone()),
        federated,
        None,
        accelerator,
        Handle::current(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .build()
}

fn bench_cdc_into_cayenne_inline(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("cdc_cayenne_inline");
    // Sample size kept low: each iteration creates a fresh SQLite metastore +
    // data dir, which is heavy compared to typical micro-benchmarks. We care
    // about relative envelopes/sec across N, not absolute wall time.
    group.sample_size(10);

    for &n in &[16usize, 64, 256, 1024] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("envelopes", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    // Per-iteration setup runs outside the timed region so the
                    // measurement reflects only the apply pipeline.
                    let table_name = format!("bench_table_{n}_{}", uuid::Uuid::now_v7());
                    rt.block_on(make_cayenne_fixture(&table_name))
                },
                |fixture| {
                    rt.block_on(async {
                        let task = make_refresh_task(&fixture);
                        let stream = make_n_envelopes(n);
                        let refresh = Arc::new(RwLock::new(Refresh::default()));
                        task.start_changes_stream(
                            refresh,
                            stream,
                            None,
                            None,
                            Arc::new(AtomicBool::new(false)),
                        )
                        .await
                        .expect("start_changes_stream");
                        black_box(fixture);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_cdc_fixture_replay(c: &mut Criterion) {
    let Some(path) = std::env::var_os("CDC_FIXTURE_FILE") else {
        print_fixture_instructions("CDC_FIXTURE_FILE is not set");
        return;
    };
    let batches = read_fixture_batches(Path::new(&path));
    if batches.is_empty() {
        print_fixture_instructions("fixture file contained zero batches");
        return;
    }
    let schema = data_schema_from_wrapper(&batches[0]);
    let n = batches.len();

    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("cdc_fixture_replay");
    group.sample_size(10);
    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("fixture_replay", |b| {
        b.iter_batched(
            || {
                let table_name = format!("bench_fixture_{}", uuid::Uuid::now_v7());
                let fixture =
                    rt.block_on(make_cayenne_fixture_for_schema(&table_name, Arc::clone(&schema)));
                (fixture, batches.clone())
            },
            |(fixture, batches)| {
                rt.block_on(async {
                    let task = make_refresh_task(&fixture);
                    let stream = make_fixture_stream(batches);
                    let refresh = Arc::new(RwLock::new(Refresh::default()));
                    task.start_changes_stream(
                        refresh,
                        stream,
                        None,
                        None,
                        Arc::new(AtomicBool::new(false)),
                    )
                    .await
                    .expect("start_changes_stream");
                    black_box(fixture);
                });
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_cdc_into_cayenne_inline,
    bench_cdc_fixture_replay
);
criterion_main!(benches);
