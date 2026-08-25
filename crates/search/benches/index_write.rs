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

//! Benchmarks `SearchIndex::write` on `MemoryVectorIndex`
//! (`crates/search/src/index/memory/mod.rs`) -- the write-time cost of
//! embedding a batch and upserting it into the in-memory vector store.
//! Search has been an active area of recent work but had zero in-process
//! benchmarks before this file; all existing coverage is end-to-end via
//! `testoperator run search`.
//!
//! Uses a deterministic, model-free `Embed` stub (byte-hash embedding, the
//! same shape `crates/search/src/index/compound/tests.rs`'s `ByteEmbed`
//! uses -- that one is test-cfg-private and unreachable from a `benches/`
//! target, so this bench carries its own copy) so the bench prices
//! `MemoryVectorIndex`'s own write path -- schema lookup, embedding-column
//! update, primary-key extraction, store upsert -- rather than a real
//! model's inference latency.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Builder, Int64Array, ListBuilder, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::scalar::ScalarValue;
use llms::embeddings::{Embed, EmbeddingInput};
use search::index::SearchIndex;
use search::index::memory::{MemoryDistanceMetric, MemoryVectorIndex};
use search::metadata::MetadataColumns;
use tokio::runtime::Runtime as TokioRuntime;

const DIM: i32 = 384;

fn byte_vector(text: &str) -> Vec<f32> {
    let dim = usize::try_from(DIM).expect("DIM is positive");
    let mut vector = vec![0.0_f32; dim];
    for (i, b) in text.bytes().enumerate() {
        vector[i % dim] += f32::from(b) / 255.0;
    }
    vector
}

#[derive(Debug)]
struct ByteEmbed;

#[async_trait]
impl Embed for ByteEmbed {
    async fn embed(&self, input: EmbeddingInput) -> llms::embeddings::Result<Vec<Vec<f32>>> {
        match input {
            EmbeddingInput::String(s) => Ok(vec![byte_vector(&s)]),
            EmbeddingInput::StringArray(v) => Ok(v.iter().map(|s| byte_vector(s)).collect()),
            _ => Ok(vec![]),
        }
    }

    fn size(&self) -> i32 {
        DIM
    }
}

/// A DataFusion UDF matching `ByteEmbed` -- required at index construction
/// for query-time scoring, but never invoked by `write()`, so its body only
/// needs to be well-typed, not exercised.
fn embed_udf() -> Arc<datafusion::logical_expr::ScalarUDF> {
    Arc::new(create_udf(
        "embed",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
        Volatility::Volatile,
        Arc::new(|args: &[ColumnarValue]| {
            let ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) = &args[0] else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "bench embed UDF expects a literal text argument".to_string(),
                ));
            };
            let mut builder = ListBuilder::new(Float32Builder::new());
            builder.values().append_slice(&byte_vector(text));
            builder.append(true);
            Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                builder.finish(),
            ))))
        }),
    ))
}

fn make_index() -> MemoryVectorIndex {
    MemoryVectorIndex::try_new(
        "content".to_string(),
        vec![Field::new("id", DataType::Int64, false)],
        MetadataColumns::none(),
        Arc::new(ByteEmbed),
        embed_udf(),
        "bench_model".to_string(),
        MemoryDistanceMetric::Cosine,
    )
    .expect("valid memory index")
}

fn make_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let content: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|i| format!("document body number {i} with some representative text")),
    ));
    RecordBatch::try_new(schema, vec![ids, content]).expect("record batch")
}

fn bench_index_write(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("search_index_write");
    group.sample_size(20);

    for &rows in &[100usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::new("memory_vector_index", rows),
            &rows,
            |b, &rows| {
                b.iter_batched(
                    || (make_index(), make_batch(rows)),
                    |(index, batch)| {
                        let result = rt.block_on(index.write(batch)).expect("write succeeds");
                        black_box(result);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_index_write);
criterion_main!(benches);
