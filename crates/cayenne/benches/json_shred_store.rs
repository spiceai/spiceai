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

//! What storing a JSON column's frequent keys costs a write.
//!
//! Over 1M generated OpenTelemetry-like log attribute documents (Utf8, 8,192-row
//! batches, as a write receives them), reports:
//!  1. the time to choose the stored keys from a write's head;
//!  2. the time to extract them, per row and per batch (P50 and P99 over every
//!     batch of ten passes), single-threaded;
//!  3. the stored columns' size against the JSON column's, in memory and as
//!     Vortex files written with the default strategy.
//!
//! Run with `cargo bench -p cayenne --bench json_shred_store`.

#![allow(clippy::expect_used, clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::json_shred::StoredJsonKeys;
use criterion::{Criterion, Throughput};

const ROWS: usize = 1 << 20;
const BATCH_ROWS: usize = 8192;
const PASSES: usize = 10;

/// A small seeded generator, so every run measures the same documents.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const STATUSES: [u64; 6] = [200, 201, 204, 404, 500, 503];
const AGENTS: [&str; 3] = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "curl/8.5.0",
    "okhttp/4.12.0",
];

/// One attributes document of an HTTP service's request log: the keys every
/// request carries, and keys only some carry.
fn attributes(rng: &mut Rng) -> String {
    let mut doc = format!(
        r#"{{"http.request.method":"{}","url.path":"/api/v1/items/{}","http.response.status_code":{},"duration_ms":{},"client.address":"10.0.{}.{}","server.address":"api.example.com","user_agent.original":"{}","trace.sampled":{}"#,
        METHODS[usize::try_from(rng.below(4)).expect("fits")],
        rng.below(200),
        STATUSES[usize::try_from(rng.below(6)).expect("fits")],
        rng.below(5000),
        rng.below(256),
        rng.below(256),
        AGENTS[usize::try_from(rng.below(3)).expect("fits")],
        rng.below(2) == 0,
    );
    if rng.below(10) < 3 {
        doc.push_str(r#","error.type":"timeout""#);
    }
    if rng.below(10) < 2 {
        doc.push_str(&format!(
            r#","db.statement":"SELECT * FROM items WHERE id = {} AND tenant = 'tenant-{}'""#,
            rng.below(1_000_000),
            rng.below(50)
        ));
    }
    doc.push('}');
    doc
}

fn batches() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "attributes",
        DataType::Utf8,
        true,
    )]));
    let mut rng = Rng(20_261_126);
    let batches = (0..ROWS / BATCH_ROWS)
        .map(|_| {
            let mut builder = StringBuilder::with_capacity(BATCH_ROWS, BATCH_ROWS * 320);
            for _ in 0..BATCH_ROWS {
                builder.append_value(attributes(&mut rng));
            }
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(builder.finish()) as ArrayRef],
            )
            .expect("batch")
        })
        .collect();
    (schema, batches)
}

/// The size of `batches` written as one Vortex file with the default strategy.
fn vortex_bytes(batches: &[RecordBatch]) -> usize {
    use vortex::VortexSessionDefault;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::arrow::ArrowSessionExt;
    use vortex::buffer::ByteBufferMut;
    use vortex::file::WriteOptionsSessionExt;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let session = vortex_session::VortexSession::default();
        let schema = batches[0].schema();
        let arrow = session.arrow();
        let dtype = arrow.from_arrow_schema(&schema).expect("dtype");
        let chunks: Vec<_> = batches
            .iter()
            .map(|batch| arrow.from_arrow_record_batch(batch.clone(), &schema))
            .collect();
        drop(arrow);
        let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks));
        let mut file = ByteBufferMut::empty();
        session
            .write_options()
            .write(&mut file, stream)
            .await
            .expect("write");
        file.len()
    })
}

fn percentile(sorted: &[Duration], percent: f64) -> Duration {
    let rank = ((sorted.len() as f64) * percent / 100.0).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn report(schema: &SchemaRef, batches: &[RecordBatch]) -> StoredJsonKeys {
    let candidates = StoredJsonKeys::candidates(schema, schema);
    let started = Instant::now();
    let stored = StoredJsonKeys::choose(schema, &candidates, &batches[..1]).expect("keys");
    let choose = started.elapsed();
    let file_schema = stored.file_schema(schema);
    println!(
        "chose {} keys from {BATCH_ROWS} rows in {choose:?}: {:?}",
        stored.keys(schema).len(),
        stored.keys(schema)
    );

    let mut samples = Vec::with_capacity(PASSES * batches.len());
    let mut written = Vec::with_capacity(batches.len());
    for pass in 0..PASSES {
        for batch in batches {
            let started = Instant::now();
            let out = stored.append(batch, &file_schema).expect("append");
            samples.push(started.elapsed());
            if pass == 0 {
                written.push(out);
            }
        }
    }
    let total: Duration = samples.iter().sum();
    samples.sort_unstable();
    println!(
        "extract: {:.1} ns/row over {} rows x {PASSES} passes; per {BATCH_ROWS}-row batch P50 {:?}, P99 {:?} ({} samples)",
        total.as_nanos() as f64 / (ROWS * PASSES) as f64,
        ROWS,
        percentile(&samples, 50.0),
        percentile(&samples, 99.0),
        samples.len()
    );

    let json_bytes: usize = written
        .iter()
        .map(|batch| batch.column(0).get_array_memory_size())
        .sum();
    let stored_bytes: usize = written
        .iter()
        .map(|batch| {
            batch.columns()[1..]
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>()
        })
        .sum();
    let json_only: Vec<RecordBatch> = written
        .iter()
        .map(|batch| batch.project(&[0]).expect("project"))
        .collect();
    let stored_only: Vec<RecordBatch> = written
        .iter()
        .map(|batch| {
            batch
                .project(&(1..batch.num_columns()).collect::<Vec<_>>())
                .expect("project")
        })
        .collect();
    let json_file = vortex_bytes(&json_only);
    let stored_file = vortex_bytes(&stored_only);
    println!(
        "size: in memory JSON {json_bytes} B, stored {stored_bytes} B ({:.1}%); as Vortex files JSON {json_file} B, stored {stored_file} B ({:.1}%)",
        stored_bytes as f64 * 100.0 / json_bytes as f64,
        stored_file as f64 * 100.0 / json_file as f64,
    );
    stored
}

fn main() {
    let (schema, batches) = batches();
    let stored = report(&schema, &batches);
    let file_schema = stored.file_schema(&schema);

    let mut criterion = Criterion::default().configure_from_args();
    let mut group = criterion.benchmark_group("json_shred_store");
    group.throughput(Throughput::Elements(BATCH_ROWS as u64));
    group.bench_function("append_8192_rows", |b| {
        let mut next = 0;
        b.iter(|| {
            let batch = &batches[next % batches.len()];
            next += 1;
            black_box(stored.append(batch, &file_schema).expect("append"))
        });
    });
    group.finish();
    criterion.final_summary();
}
