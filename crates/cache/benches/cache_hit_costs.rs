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
//! and computing a logical-plan key, including for a parameterized statement.

#![allow(clippy::expect_used)] // Benchmarks can panic

use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use cache::QueryResultsCacheProvider;
use cache::encoding::{Encoder, ZstdEncoder};
use cache::get_hash_builder;
use cache::key::{CacheKey, RawCacheKey};
use cache::result::query::CachedQueryResult;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::{LogicalPlan, col, placeholder, table_scan};
use spicepod::component::caching::HashingAlgorithm;
use spicepod::component::caching::SQLResultsCacheConfig;
use std::collections::HashSet;
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

fn zstd_results_cache() -> QueryResultsCacheProvider {
    QueryResultsCacheProvider::try_new(
        &SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            max_size: Some("64MiB".to_string()),
            encoding: spicepod::component::caching::Encoding::Zstd,
            ..SQLResultsCacheConfig::default()
        },
        Box::new([]),
    )
    .expect("provider")
}

fn encoded_entry(
    bytes: Bytes,
    decoded_len: usize,
    schema: &arrow::datatypes::SchemaRef,
) -> CachedQueryResult {
    let now = Instant::now();
    CachedQueryResult::new(
        bytes,
        decoded_len,
        Arc::clone(schema),
        Arc::new(HashSet::new()),
        now,
        now,
        cache::encoding::get_encoder(spicepod::component::caching::Encoding::Zstd),
    )
}

/// First, second, and third fetch of the same zstd-encoded cache key.
///
/// `hit1` stores a fresh encoded entry and pays zstd+IPC; the store stays
/// Encoded. `hit2` runs against that one-hit encoded entry, pays zstd+IPC
/// again, and promotes to Raw. `hit3` is `Arc::clone` of the raw batches.
fn bench_encoded_hit_promotion(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a tokio runtime");
    let encoder = ZstdEncoder::default();
    let mut group = c.benchmark_group("encoded_hit");

    let cases = [
        ("rows=100/text_columns=1", vec![batch(100, 1)]),
        ("rows=1_000/text_columns=1", vec![batch(1_000, 1)]),
        ("rows=10_000/constant", vec![constant_batch(10_000)]),
    ];

    for (shape, batches) in cases {
        let payload = runtime
            .block_on(encoder.encode(&batches))
            .expect("the batches encode");
        let schema = batches[0].schema();
        let bytes = Bytes::from(payload.bytes);
        let decoded_len = payload.decoded_len;
        let id = format!(
            "{shape}/ipc_bytes={decoded_len}/encoded_bytes={}",
            bytes.len()
        );

        group.bench_function(BenchmarkId::new("hit1_decode", &id), {
            let bytes = bytes.clone();
            let schema = Arc::clone(&schema);
            move |b| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("a tokio runtime");
                b.iter_batched(
                    || {
                        let provider = zstd_results_cache();
                        let key = RawCacheKey::new(1);
                        runtime
                            .block_on(provider.put_raw_key(
                                &key,
                                encoded_entry(bytes.clone(), decoded_len, &schema),
                            ))
                            .expect("put");
                        (provider, key)
                    },
                    |(provider, key)| {
                        runtime.block_on(async move {
                            let entry =
                                provider.get_raw_key(&key).await.expect("get").expect("hit");
                            debug_assert!(entry.is_encoded(), "hit1 starts encoded");
                            black_box(provider.records(&key, &entry).await.expect("hit1 decode"))
                        })
                    },
                    BatchSize::SmallInput,
                );
            }
        });

        group.bench_function(BenchmarkId::new("hit2_decode_and_promote", &id), {
            let bytes = bytes.clone();
            let schema = Arc::clone(&schema);
            move |b| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("a tokio runtime");
                b.iter_batched(
                    || {
                        let provider = zstd_results_cache();
                        let key = RawCacheKey::new(1);
                        runtime.block_on(async {
                            provider
                                .put_raw_key(
                                    &key,
                                    encoded_entry(bytes.clone(), decoded_len, &schema),
                                )
                                .await
                                .expect("put");
                            let entry = provider
                                .get_raw_key(&key)
                                .await
                                .expect("get")
                                .expect("hit1");
                            provider.records(&key, &entry).await.expect("hit1 decode");
                        });
                        (provider, key)
                    },
                    |(provider, key)| {
                        runtime.block_on(async move {
                            let entry = provider
                                .get_raw_key(&key)
                                .await
                                .expect("get")
                                .expect("hit2");
                            debug_assert!(
                                entry.is_encoded(),
                                "hit2 must still be encoded before promote"
                            );
                            black_box(
                                provider
                                    .records(&key, &entry)
                                    .await
                                    .expect("hit2 decode+promote"),
                            )
                        })
                    },
                    BatchSize::SmallInput,
                );
            }
        });

        group.bench_function(BenchmarkId::new("hit3_raw", &id), {
            let bytes = bytes.clone();
            let schema = Arc::clone(&schema);
            move |b| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("a tokio runtime");
                b.iter_batched(
                    || {
                        let provider = zstd_results_cache();
                        let key = RawCacheKey::new(1);
                        runtime.block_on(async {
                            provider
                                .put_raw_key(
                                    &key,
                                    encoded_entry(bytes.clone(), decoded_len, &schema),
                                )
                                .await
                                .expect("put");
                            for _ in 0..2 {
                                let entry = provider
                                    .get_raw_key(&key)
                                    .await
                                    .expect("get")
                                    .expect("warmup hit");
                                provider.records(&key, &entry).await.expect("warmup decode");
                            }
                        });
                        (provider, key)
                    },
                    |(provider, key)| {
                        runtime.block_on(async move {
                            let entry = provider
                                .get_raw_key(&key)
                                .await
                                .expect("get")
                                .expect("hit3");
                            debug_assert!(!entry.is_encoded(), "hit3 must be the raw path");
                            black_box(provider.records(&key, &entry).await.expect("raw"))
                        })
                    },
                    BatchSize::SmallInput,
                );
            }
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_zstd_decode,
    bench_plan_key,
    bench_parameterized_key,
    bench_encoded_hit_promotion
);
criterion_main!(benches);
