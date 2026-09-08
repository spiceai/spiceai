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

//! Microbenchmark for the CDC composite-PK delete-burst path (issue #11673),
//! run against a **Cayenne** accelerator — the engine SF-1000 HTAP uses.
//!
//! A changes-mode Delete sub-batch of `N` composite-key rows is applied by
//! `process_delete_batch`. When the in-memory absorb path is unavailable (here:
//! no slot advancer is armed, exactly as for composite-PK tables whose keys
//! cannot yet be absorbed), the burst falls through to a durable `delete_from`
//! with a balanced OR-of-ANDs predicate `(pk1=.. AND pk2=..) OR ..`. Before the
//! fix this was one monolithic plan over all `N` keys (~50k comparisons at
//! `N=16384`, ~89 s in production). The fix caps keys at `cdc_delete_subbatch_max`
//! (default 2,048) per `delete_from`, so the burst runs as `⌈N/cap⌉` bounded,
//! interruptible plans.
//!
//! Each case builds a fresh Cayenne table, populates it with `N` inline rows via
//! one create burst (untimed setup), then times applying a single delete burst
//! removing all `N` keys, comparing:
//!   - `monolithic` — `cap = N` (one durable plan, the pre-fix behavior), vs
//!   - `chunked`    — `cap = 2048` (the default; `⌈N/2048⌉` plans).

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
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

#[async_trait::async_trait]
impl CommitChange for NoopCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

/// Composite-key data schema: `(pk1, pk2)` plus a `val` payload.
fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pk1", DataType::Int64, false),
        Field::new("pk2", DataType::Utf8, false),
        Field::new("val", DataType::Utf8, true),
    ]))
}

/// One envelope carrying `n` rows for op `op` (`"c"` create / `"d"` delete).
/// `pk_names` is the per-row primary-key list: empty for creates (appends),
/// `["pk1","pk2"]` for deletes (drives the composite OR-tree predicate).
fn make_batch_envelope(op: &str, n: usize, pk_names: &[&'static str]) -> ChangeEnvelope {
    let data = data_schema();
    let wrapper = changes_schema(&data);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec![op; n]));

    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
    let mut names: Vec<&str> = Vec::with_capacity(n * pk_names.len());
    offsets.push(0);
    for _ in 0..n {
        for name in pk_names {
            names.push(name);
        }
        offsets.push(i32::try_from(names.len()).expect("offset fits in i32"));
    }
    let pk_array: ArrayRef = Arc::new(
        ListArray::try_new(
            pk_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(names)),
            None,
        )
        .expect("pk ListArray"),
    );

    let pk1: ArrayRef = Arc::new(Int64Array::from(
        (0..n)
            .map(|i| i64::try_from(i).expect("index fits in i64"))
            .collect::<Vec<_>>(),
    ));
    let pk2: ArrayRef = Arc::new(StringArray::from(
        (0..n).map(|i| format!("k-{i}")).collect::<Vec<_>>(),
    ));
    let val: ArrayRef = Arc::new(StringArray::from(
        (0..n).map(|i| Some(format!("v-{i}"))).collect::<Vec<_>>(),
    ));
    let data_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("pk1", DataType::Int64, false)), pk1),
        (Arc::new(Field::new("pk2", DataType::Utf8, false)), pk2),
        (Arc::new(Field::new("val", DataType::Utf8, true)), val),
    ]));

    let record = RecordBatch::try_new(Arc::new(wrapper), vec![op_array, pk_array, data_array])
        .expect("wrapper RecordBatch");
    let batch = ChangeBatch::try_new(record).expect("ChangeBatch");
    ChangeEnvelope::new(Box::new(NoopCommitter), batch, false)
}

fn single_envelope_stream(envelope: ChangeEnvelope) -> ChangesStream {
    fstream::iter(vec![Ok::<_, StreamError>(envelope)]).boxed()
}

struct CayenneFixture {
    _temp: TempDir,
    table: Arc<CayenneTableProvider>,
}

async fn make_cayenne_fixture(table_name: &str) -> CayenneFixture {
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
            schema: data_schema(),
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

/// A `RefreshTask` over `table`, optionally pinning `cdc_delete_subbatch_max`.
fn make_task(table: &Arc<CayenneTableProvider>, cap: Option<usize>) -> RefreshTask {
    let accelerator: Arc<dyn TableProvider> = Arc::clone(table) as _;
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
    let builder = RefreshTaskBuilder::new(
        RuntimeStatus::new(),
        TableReference::bare("delete_burst_bench"),
        federated,
        None,
        accelerator,
        Handle::current(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    let builder = match cap {
        Some(cap) => builder.with_cdc_param_overrides(Some(Arc::new(HashMap::from([(
            "cdc_delete_subbatch_max".to_string(),
            cap.to_string(),
        )])))),
        None => builder,
    };
    builder.build()
}

async fn apply_stream(task: &RefreshTask, stream: ChangesStream) {
    task.start_changes_stream(
        Arc::new(RwLock::new(Refresh::default())),
        stream,
        None,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("changes stream should apply");
}

fn bench_delete_burst(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("cdc_delete_burst_cayenne");
    // Each iteration rebuilds a fresh Cayenne table (SQLite metastore + data
    // dir) and repopulates N rows in untimed setup — heavy — so keep samples
    // low. We care about relative monolithic-vs-chunked burst duration.
    group.sample_size(10);

    for &n in &[2048usize, 8192, 16384] {
        for (mode, cap) in [("monolithic", n), ("chunked", 2048usize)] {
            group.throughput(Throughput::Elements(n as u64));
            group.bench_with_input(BenchmarkId::new(mode, n), &(n, cap), |b, &(n, cap)| {
                b.iter_batched(
                    || {
                        // Untimed: fresh table populated with N inline rows +
                        // the delete task/stream to time.
                        rt.block_on(async {
                            let name = format!("bench_{mode}_{n}_{}", uuid::Uuid::now_v7());
                            let fixture = make_cayenne_fixture(&name).await;
                            let populate = make_task(&fixture.table, None);
                            apply_stream(
                                &populate,
                                single_envelope_stream(make_batch_envelope("c", n, &[])),
                            )
                            .await;
                            let delete_task = make_task(&fixture.table, Some(cap));
                            let delete_stream = single_envelope_stream(make_batch_envelope(
                                "d",
                                n,
                                &["pk1", "pk2"],
                            ));
                            (fixture, delete_task, delete_stream)
                        })
                    },
                    |(fixture, delete_task, delete_stream)| {
                        rt.block_on(async {
                            apply_stream(&delete_task, delete_stream).await;
                            black_box(fixture);
                        });
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_delete_burst);
criterion_main!(benches);
