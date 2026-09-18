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

//! What serving a results-cache hit costs before any rows move: decoding an encoded entry,
//! computing a logical-plan key (including for a parameterized statement), and
//! draining a Raw multi-batch serve stream.

#![allow(clippy::expect_used)] // Benchmarks can panic

use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cache::encoding::{Encoder, ZstdEncoder};
use cache::get_hash_builder;
use cache::key::CacheKey;
use cache::result::CacheStatus;
use cache::result::query::{
    CachedQueryResult, CachedStream, QueryResult, QueryResultSource, SendableCachedRawStream,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, col, placeholder, table_scan};
use futures::Stream;
use spicepod::component::caching::HashingAlgorithm;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Barrier;
use std::task::{Context, Poll};
use std::time::Instant;

fn batch(rows: usize, text_columns: usize) -> RecordBatch {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ];
    let row_count = i64::try_from(rows).expect("row count fits in i64");
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..row_count)),
        Arc::new(Float64Array::from_iter_values((0..row_count).map(|i| {
            f64::from(u32::try_from(i).expect("fits in u32")) * 0.5
        }))),
    ];
    for column in 0..text_columns {
        fields.push(Field::new(format!("t{column}"), DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("row {i} column {column}")),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a valid batch")
}

/// A batch holding one repeated value per column: tiny once compressed, however many rows it
/// holds.
fn constant_batch(rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_value(42, rows)),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "active", rows,
        ))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).expect("a valid batch")
}

/// Decoding an entry stored under `encoding: zstd`: zstd decompression and an Arrow IPC read.
/// Each case is named by its shape, the size of the IPC stream it decodes to, and the size of
/// the encoded payload, since the two diverge for a result that compresses well.
fn bench_zstd_decode(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a tokio runtime");
    let encoder = ZstdEncoder::default();
    let mut group = c.benchmark_group("zstd_decode");
    let cases = [
        (1, 1),
        (7, 1),
        (10, 20),
        (100, 1),
        (100, 20),
        (1_000, 1),
        (10_000, 1),
        (1, 50),
        (1, 200),
        (10, 200),
    ]
    .into_iter()
    .map(|(rows, text_columns)| {
        (
            format!("rows={rows}/text_columns={text_columns}"),
            vec![batch(rows, text_columns)],
        )
    })
    .chain(
        [1_000, 10_000, 100_000]
            .into_iter()
            .map(|rows| (format!("rows={rows}/constant"), vec![constant_batch(rows)])),
    )
    // Many small or empty batches: each is its own IPC message to decode, whatever it holds.
    .chain(
        [(16, 1), (64, 1), (256, 1), (256, 0)]
            .into_iter()
            .map(|(batches, rows)| {
                (
                    format!("batches={batches}/rows={rows}/text_columns=1"),
                    vec![batch(rows, 1); batches],
                )
            }),
    );
    for (shape, batches) in cases {
        let encoded = runtime
            .block_on(encoder.encode(&batches))
            .expect("the batches encode");
        let id = format!(
            "{shape}/ipc_bytes={}/encoded_bytes={}",
            encoded.decoded_len,
            encoded.bytes.len()
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(id),
            &encoded.bytes,
            |b, bytes| {
                b.to_async(&runtime).iter(|| async {
                    black_box(
                        encoder
                            .decode(black_box(bytes))
                            .await
                            .expect("the payload decodes"),
                    )
                });
            },
        );
    }
    group.finish();
}

fn wide_schema(columns: usize) -> Schema {
    Schema::new(
        (0..columns)
            .map(|i| {
                let data_type = if i % 2 == 0 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                };
                Field::new(format!("c{i}"), data_type, true)
            })
            .collect::<Vec<_>>(),
    )
}

/// `SELECT c0..c{columns} FROM wide WHERE c0 = $1`, before any value is bound.
fn parameterized_plan(schema: &Schema, columns: usize) -> LogicalPlan {
    table_scan(Some("wide"), schema, None)
        .expect("a scan")
        .filter(col("c0").eq(placeholder("$1")))
        .expect("a filter")
        .project((0..columns).map(|i| col(format!("c{i}"))))
        .expect("a projection")
        .build()
        .expect("the plan")
}

/// A logical-plan results-cache key, computed two ways: through the hasher the cache builds
/// (`KeyHasher`, which matches on the configured algorithm per write), and through a boxed
/// hasher, which is the virtual call per plan node, expression and field that the enum removes.
fn bench_plan_key(c: &mut Criterion) {
    let schema = wide_schema(200);
    let mut group = c.benchmark_group("plan_key");
    for columns in [1, 20, 200] {
        let plan = parameterized_plan(&schema, columns);
        for (name, algorithm) in [
            ("xxh3", HashingAlgorithm::XXH3),
            ("ahash", HashingAlgorithm::Ahash),
            ("siphash", HashingAlgorithm::Siphash),
            ("blake3", HashingAlgorithm::Blake3),
        ] {
            let builder = get_hash_builder(algorithm).expect("a supported algorithm");
            group.bench_with_input(
                BenchmarkId::new(format!("{name}/enum_dispatch"), columns),
                &plan,
                |b, plan| {
                    b.iter(|| {
                        black_box(
                            CacheKey::LogicalPlan(black_box(plan))
                                .as_raw_key_in_namespace(builder.build_hasher(), 1, b"principal")
                                .as_u64(),
                        )
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{name}/boxed_per_write"), columns),
                &plan,
                |b, plan| {
                    b.iter(|| {
                        let boxed: Box<dyn Hasher> = Box::new(builder.build_hasher());
                        black_box(
                            CacheKey::LogicalPlan(black_box(plan))
                                .as_raw_key_in_namespace(boxed, 1, b"principal")
                                .as_u64(),
                        )
                    });
                },
            );
        }
    }
    group.finish();
}

/// What the results-cache probe does for a parameterized statement under the plan key type
/// (bind the values into the cached template plan, then key the bound plan), against keying
/// the statement's text and its values.
fn bench_parameterized_key(c: &mut Criterion) {
    const SQL: &str = "SELECT c0, c1, c2 FROM wide WHERE c0 = $1";
    let schema = wide_schema(200);
    let builder = get_hash_builder(HashingAlgorithm::XXH3).expect("xxh3");
    let parameters = ParamValues::List(vec![ScalarValue::Int64(Some(42)).into()]);
    let mut group = c.benchmark_group("parameterized_key");
    for columns in [1, 20, 200] {
        let template = parameterized_plan(&schema, columns);
        group.bench_with_input(
            BenchmarkId::new("bind_then_plan_key", columns),
            &template,
            |b, template| {
                b.iter(|| {
                    let bound = black_box(template)
                        .clone()
                        .with_param_values(parameters.clone())
                        .expect("the value binds");
                    black_box(
                        CacheKey::LogicalPlan(&bound)
                            .as_raw_key_in_namespace(builder.build_hasher(), 1, b"principal")
                            .as_u64(),
                    )
                });
            },
        );
    }
    group.bench_function("sql_and_values_key", |b| {
        b.iter(|| {
            black_box(
                CacheKey::Query(black_box(SQL), Some(black_box(&parameters)))
                    .as_raw_key_in_namespace(builder.build_hasher(), 1, b"principal")
                    .as_u64(),
            )
        });
    });
    group.finish();
}

fn drain_raw_stream<S>(mut stream: S) -> usize
where
    S: Stream<Item = Result<Arc<RecordBatch>, DataFusionError>> + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut rows = 0;
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => {
                rows += batch.num_rows();
                black_box(&batch);
            }
            Poll::Ready(None) => break,
            Poll::Ready(Some(Err(e))) => panic!("stream error: {e}"),
            Poll::Pending => panic!("serve stream must be immediately ready"),
        }
    }
    rows
}

fn drain_raw_and_touch<S>(mut stream: S) -> i64
where
    S: Stream<Item = Result<Arc<RecordBatch>, DataFusionError>> + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut sum = 0_i64;
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Some(col) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                {
                    for value in col.values() {
                        sum = sum.wrapping_add(*value);
                    }
                }
                black_box(&batch);
            }
            Poll::Ready(None) => break,
            Poll::Ready(Some(Err(e))) => panic!("stream error: {e}"),
            Poll::Pending => panic!("serve stream must be immediately ready"),
        }
    }
    sum
}

/// Production SQL Raw hit: `QueryResult::from_cached_raw` + `into_source`.
/// Prefetch runs at construction; each poll is one `Arc` clone.
fn sql_raw_hit_stream(
    stored: &cache::result::query::CachedBatches,
    schema: &SchemaRef,
) -> SendableCachedRawStream {
    let result = QueryResult::from_cached_raw(
        Arc::clone(stored),
        Arc::clone(schema),
        CacheStatus::CacheHit,
    );
    match result.into_source() {
        QueryResultSource::CachedRaw { data, .. } => data,
        QueryResultSource::Stream { .. } => {
            panic!("from_cached_raw must yield QueryResultSource::CachedRaw")
        }
    }
}

fn drain_stream<S>(mut stream: S) -> usize
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut rows = 0;
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => {
                rows += batch.num_rows();
                black_box(&batch);
            }
            Poll::Ready(None) => break,
            Poll::Ready(Some(Err(e))) => panic!("stream error: {e}"),
            Poll::Pending => panic!("serve stream must be immediately ready"),
        }
    }
    rows
}

/// Touch column values so a consumer-side scan is in the wall time.
fn drain_and_touch<S>(mut stream: S) -> i64
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut sum = 0_i64;
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Some(col) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                {
                    for value in col.values() {
                        sum = sum.wrapping_add(*value);
                    }
                }
                black_box(&batch);
            }
            Poll::Ready(None) => break,
            Poll::Ready(Some(Err(e))) => panic!("stream error: {e}"),
            Poll::Pending => panic!("serve stream must be immediately ready"),
        }
    }
    sum
}

/// Pre-change SQL serve: `Arc<Vec<RecordBatch>>`, no prefetch,
/// `RecordBatch::clone` on each poll. Bench-local so construct+drain and
/// construct+drain+touch isolate the new SQL path
/// (`QueryResult::from_cached_raw` → `CachedRawStream`, prefetch + one
/// `Arc` clone per poll).
struct LegacyCachedStream {
    data: Arc<Vec<RecordBatch>>,
    schema: SchemaRef,
    index: usize,
}

impl LegacyCachedStream {
    fn new(data: Arc<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
        Self {
            data,
            schema,
            index: 0,
        }
    }
}

impl Stream for LegacyCachedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            let index = self.index;
            let batch = self.data.get(index).cloned().map(Ok);
            self.index += 1;
            batch
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for LegacyCachedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Time `n_threads` hits that start together. Workers live for the whole
/// bench; each sample sends one hit per worker, they barrier among
/// themselves, then the clock waits for every drain. Thread create/join
/// stays outside the timed region.
fn run_persistent_concurrent_bench<F>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    id: BenchmarkId,
    n_threads: usize,
    work: F,
) where
    F: Fn() + Send + Sync,
{
    std::thread::scope(|scope| {
        let go = Arc::new(Barrier::new(n_threads));
        let mut work_txs = Vec::with_capacity(n_threads);
        let mut done_rxs = Vec::with_capacity(n_threads);

        for _ in 0..n_threads {
            let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<bool>(1);
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel::<()>(1);
            work_txs.push(work_tx);
            done_rxs.push(done_rx);
            let go = Arc::clone(&go);
            let work = &work;
            scope.spawn(move || {
                loop {
                    match work_rx.recv() {
                        Ok(true) => {
                            go.wait();
                            work();
                            done_tx.send(()).expect("main is waiting for this hit");
                        }
                        Ok(false) | Err(_) => break,
                    }
                }
            });
        }

        group.bench_function(id, |b| {
            b.iter_custom(|iters| {
                let t0 = Instant::now();
                for _ in 0..iters {
                    for tx in &work_txs {
                        tx.send(true).expect("worker is alive");
                    }
                    for rx in &done_rxs {
                        rx.recv().expect("worker finished the hit");
                    }
                }
                t0.elapsed()
            });
        });

        for tx in work_txs {
            let _ = tx.send(false);
        }
    });
}

/// Distinct Raw entries whose array buffers exceed a large last-level
/// cache (this host's L3 is 320 MiB). Rotating through the set keeps
/// the scan off a warm line.
fn numeric_working_set(
    target_bytes: usize,
) -> (
    SchemaRef,
    Vec<cache::result::query::CachedBatches>,
    Vec<Arc<Vec<RecordBatch>>>,
) {
    const BATCHES: usize = 8;
    const ROWS: usize = 262_144;
    let mut raws = Vec::new();
    let mut legacies = Vec::new();
    let mut schema = None;
    let mut filled = 0;
    while filled < target_bytes {
        let payload: Vec<RecordBatch> = (0..BATCHES).map(|_| batch(ROWS, 0)).collect();
        filled += payload
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let this_schema = payload[0].schema();
        if schema.is_none() {
            schema = Some(Arc::clone(&this_schema));
        }
        let now = Instant::now();
        let cached =
            CachedQueryResult::new_raw(payload, this_schema, Arc::new(HashSet::new()), now, now);
        let stored = cached.raw_batches().expect("raw entry");
        let shared = Arc::new(
            stored
                .iter()
                .map(|batch| RecordBatch::clone(batch))
                .collect::<Vec<_>>(),
        );
        raws.push(stored);
        legacies.push(shared);
    }
    (schema.expect("working set"), raws, legacies)
}

/// Raw multi-batch hit: construct the serve stream and drain it.
///
/// `legacy_stream` / `legacy_stream_touch` are the old SQL serve path
/// (`LegacyCachedStream`: `Arc<Vec<_>>`, no prefetch, `RecordBatch::clone`
/// on poll). `cached_raw_stream` / `cached_raw_stream_touch` are the new
/// SQL serve path (`QueryResult::from_cached_raw` → `into_source`: prefetch
/// + one `Arc` clone per poll). `legacy_column_clone` and `arc_batch_clone`
/// isolate the per-batch clone cost without stream construction — they are
/// not the serve-path comparison. `*_touch_working_set` rotates through
/// entries larger than LLC so prefetch is not measured on a warm line.
/// `concurrent_hits` / `concurrent_legacy_hits` use persistent workers.
fn bench_raw_stream_serve(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw_stream_serve");
    let cases = [
        (1, 100, 1),
        (8, 100, 1),
        (16, 100, 1),
        (64, 32, 1),
        (8, 64, 20),
        (1, 10, 200),
    ];
    for (batches, rows, text_columns) in cases {
        let payload: Vec<RecordBatch> = (0..batches).map(|_| batch(rows, text_columns)).collect();
        let schema = payload[0].schema();
        let now = Instant::now();
        let cached = CachedQueryResult::new_raw(
            payload,
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            now,
            now,
        );
        let stored = cached.raw_batches().expect("raw entry");
        let shared_vec = Arc::new(
            stored
                .iter()
                .map(|batch| RecordBatch::clone(batch))
                .collect::<Vec<_>>(),
        );
        let id = format!("batches={batches}/rows={rows}/text_columns={text_columns}");

        group.bench_with_input(
            BenchmarkId::new("legacy_column_clone", &id),
            &stored,
            |b, stored| {
                b.iter(|| {
                    let mut rows = 0;
                    for batch in stored.iter() {
                        let cloned = RecordBatch::clone(batch);
                        rows += cloned.num_rows();
                        black_box(cloned);
                    }
                    black_box(rows)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("arc_batch_clone", &id),
            &stored,
            |b, stored| {
                b.iter(|| {
                    let mut n = 0;
                    for batch in stored.iter() {
                        let handle = Arc::clone(batch);
                        n += handle.num_rows();
                        black_box(handle);
                    }
                    black_box(n)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("legacy_stream", &id),
            &shared_vec,
            |b, shared_vec| {
                b.iter(|| {
                    let stream =
                        LegacyCachedStream::new(Arc::clone(shared_vec), Arc::clone(&schema));
                    black_box(drain_stream(stream))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("legacy_stream_touch", &id),
            &shared_vec,
            |b, shared_vec| {
                b.iter(|| {
                    let stream =
                        LegacyCachedStream::new(Arc::clone(shared_vec), Arc::clone(&schema));
                    black_box(drain_and_touch(stream))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cached_raw_stream", &id),
            &stored,
            |b, stored| {
                b.iter(|| {
                    let stream = sql_raw_hit_stream(stored, &schema);
                    black_box(drain_raw_stream(stream))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cached_raw_stream_touch", &id),
            &stored,
            |b, stored| {
                b.iter(|| {
                    let stream = sql_raw_hit_stream(stored, &schema);
                    black_box(drain_raw_and_touch(stream))
                });
            },
        );
    }

    // Concurrent Raw hits of one stored entry: the path that used to bounce
    // column `ArrayRef` atomics across cores on every poll.
    let wide: Vec<RecordBatch> = (0..8).map(|_| batch(64, 20)).collect();
    let schema = wide[0].schema();
    let now = Instant::now();
    let cached = CachedQueryResult::new_raw(
        wide,
        Arc::clone(&schema),
        Arc::new(HashSet::new()),
        now,
        now,
    );
    let stored = cached.raw_batches().expect("raw entry");
    let wide_shared = Arc::new(
        stored
            .iter()
            .map(|batch| RecordBatch::clone(batch))
            .collect::<Vec<_>>(),
    );
    let concurrent_id = "threads=4/batches=8/rows=64/text_columns=20";
    run_persistent_concurrent_bench(
        &mut group,
        BenchmarkId::new("concurrent_hits", concurrent_id),
        4,
        || {
            let stream = sql_raw_hit_stream(&stored, &schema);
            black_box(drain_raw_stream(stream));
        },
    );
    run_persistent_concurrent_bench(
        &mut group,
        BenchmarkId::new("concurrent_legacy_hits", concurrent_id),
        4,
        || {
            let stream = LegacyCachedStream::new(Arc::clone(&wide_shared), Arc::clone(&schema));
            black_box(drain_stream(stream));
        },
    );

    // Search-cache path: `CachedStream::new` indexes `Arc<Vec<_>>`.
    let shared_vec = wide_shared;
    group.bench_function(
        BenchmarkId::new(
            "cached_stream_from_shared_vec",
            "batches=8/rows=64/text_columns=20",
        ),
        |b| {
            b.iter(|| {
                let stream = CachedStream::new(Arc::clone(&shared_vec), Arc::clone(&schema));
                black_box(drain_stream(stream))
            });
        },
    );

    // Matched old/new touch on a working set larger than a 320 MiB LLC.
    const WORKING_SET_BYTES: usize = 384 * 1024 * 1024;
    let (ws_schema, ws_raw, ws_legacy) = numeric_working_set(WORKING_SET_BYTES);
    let ws_id = format!(
        "batches=8/rows=262144/text_columns=0/entries={}/target=384MiB",
        ws_raw.len()
    );
    let mut raw_idx = 0;
    group.bench_function(
        BenchmarkId::new("cached_raw_stream_touch_working_set", &ws_id),
        |b| {
            b.iter(|| {
                let stored = &ws_raw[raw_idx];
                raw_idx = (raw_idx + 1) % ws_raw.len();
                let stream = sql_raw_hit_stream(stored, &ws_schema);
                black_box(drain_raw_and_touch(stream))
            });
        },
    );
    let mut legacy_idx = 0;
    group.bench_function(
        BenchmarkId::new("legacy_stream_touch_working_set", &ws_id),
        |b| {
            b.iter(|| {
                let shared = &ws_legacy[legacy_idx];
                legacy_idx = (legacy_idx + 1) % ws_legacy.len();
                let stream = LegacyCachedStream::new(Arc::clone(shared), Arc::clone(&ws_schema));
                black_box(drain_and_touch(stream))
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_zstd_decode,
    bench_plan_key,
    bench_parameterized_key,
    bench_raw_stream_serve
);
criterion_main!(benches);
