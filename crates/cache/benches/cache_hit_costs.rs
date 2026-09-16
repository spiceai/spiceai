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

use std::hash::{BuildHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cache::encoding::{Encoder, ZstdEncoder};
use cache::get_hash_builder;
use cache::key::CacheKey;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::{LogicalPlan, col, placeholder, table_scan};
use spicepod::component::caching::HashingAlgorithm;

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

/// A hasher dispatching on an enum instead of through `Box<dyn Hasher>`: what the plan key
/// would hash through if the configured algorithm were matched on per write.
enum EnumHasher {
    XxHash3(twox_hash::XxHash3_64),
    Ahash(ahash::AHasher),
    Siphash(std::hash::DefaultHasher),
}

impl Hasher for EnumHasher {
    fn finish(&self) -> u64 {
        match self {
            Self::XxHash3(hasher) => hasher.finish(),
            Self::Ahash(hasher) => hasher.finish(),
            Self::Siphash(hasher) => hasher.finish(),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        match self {
            Self::XxHash3(hasher) => hasher.write(bytes),
            Self::Ahash(hasher) => hasher.write(bytes),
            Self::Siphash(hasher) => hasher.write(bytes),
        }
    }

    fn write_u64(&mut self, i: u64) {
        match self {
            Self::XxHash3(hasher) => hasher.write_u64(i),
            Self::Ahash(hasher) => hasher.write_u64(i),
            Self::Siphash(hasher) => hasher.write_u64(i),
        }
    }

    fn write_usize(&mut self, i: usize) {
        match self {
            Self::XxHash3(hasher) => hasher.write_usize(i),
            Self::Ahash(hasher) => hasher.write_usize(i),
            Self::Siphash(hasher) => hasher.write_usize(i),
        }
    }

    fn write_u8(&mut self, i: u8) {
        match self {
            Self::XxHash3(hasher) => hasher.write_u8(i),
            Self::Ahash(hasher) => hasher.write_u8(i),
            Self::Siphash(hasher) => hasher.write_u8(i),
        }
    }
}

fn enum_hasher(algorithm: HashingAlgorithm) -> Option<EnumHasher> {
    match algorithm {
        HashingAlgorithm::XXH3 => Some(EnumHasher::XxHash3(twox_hash::XxHash3_64::default())),
        HashingAlgorithm::Ahash => Some(EnumHasher::Ahash(
            ahash::RandomState::with_seeds(1, 2, 3, 4).build_hasher(),
        )),
        HashingAlgorithm::Siphash => Some(EnumHasher::Siphash(std::hash::DefaultHasher::new())),
        _ => None,
    }
}

/// A logical-plan results-cache key, computed three ways: the plan's bytes collected and
/// handed to the boxed hasher in one write (what the key does), each write going through the
/// boxed hasher, and each write dispatched on an enum.
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
                BenchmarkId::new(format!("{name}/collected_bytes"), columns),
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
                        let mut hasher = builder.build_hasher();
                        hasher.write_u8(1);
                        hasher.write(b"principal");
                        black_box(plan).hash(&mut hasher);
                        black_box(hasher.finish())
                    });
                },
            );
            if enum_hasher(algorithm).is_some() {
                group.bench_with_input(
                    BenchmarkId::new(format!("{name}/enum_per_write"), columns),
                    &plan,
                    |b, plan| {
                        b.iter(|| {
                            let mut hasher = enum_hasher(algorithm).expect("an enum hasher");
                            hasher.write_u8(1);
                            hasher.write(b"principal");
                            black_box(plan).hash(&mut hasher);
                            black_box(hasher.finish())
                        });
                    },
                );
            }
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

criterion_group!(
    benches,
    bench_zstd_decode,
    bench_plan_key,
    bench_parameterized_key
);
criterion_main!(benches);
