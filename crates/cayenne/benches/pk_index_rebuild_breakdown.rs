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

//! Where the PK existence-index rebuild's time goes.
//!
//! ## What this measures and why
//!
//! `load_existing_pk_index` scans every live key to rebuild the PK existence index
//! the CDC upsert path validates against, and it runs ON the apply thread. Measured
//! at SF1000 on `order_line` it cost **335 s for ~337M keys — ~1 µs/key**, or about
//! 36 MB/s of primary-key data on a 64-core box. The per-key work should be an order
//! of magnitude cheaper than that:
//!
//! | stage | expected |
//! |---|---|
//! | Vortex decode of 4 int PK columns | ~10-20 ns/row |
//! | `RowConverter` encode of the composite key | ~50-100 ns |
//! | XXH3 over the encoded bytes | ~10 ns |
//! | split-block bloom insert (one cache line) | ~5 ns |
//!
//! So ~100-150 ns/key is the budget and ~1 µs is what we pay. This bench attributes
//! the gap, because the three candidate fixes are mutually exclusive in effort:
//!
//!   * **serialization** — the rebuild does `execute_stream(scan_plan, …)`, ONE
//!     stream, drained serially. If this is the gap, partitioning the scan across
//!     the CPU budget is the whole fix and needs no format change (`consume/serial`
//!     vs `consume/partitioned`).
//!   * **the deletion filter** — the rebuild applies the full deletion indexes over
//!     every scanned row, and composite-PK key deletion encodes and probes per batch
//!     (`stage/plus_deletion_probe`).
//!   * **encode + hash** — only if these dominate is it worth either fusing the
//!     encoder into the hash or storing a `pk_hash` column in the data files
//!     (`stage/plus_encode`, `stage/plus_digest`).
//!
//! `alt/hash_column_only` bounds the stored-`pk_hash` idea WITHOUT building the
//! format: it scans a single `Int64` column and inserts straight into the filter, so
//! it is the floor a stored hash could reach. Compare it against `stage/plus_insert`
//! — that ratio is the ceiling on what the column could buy, and if it is small the
//! 8 bytes/row of incompressible storage is not worth paying.
//!
//! The PK is composite (four `Int64`s, mirroring TPC-C `order_line`'s
//! `ol_w_id, ol_d_id, ol_o_id, ol_number`) because composite is what makes both the
//! row encode and the key-based deletion probe expensive; a single-`Int64` PK takes
//! neither path.
//!
//! Caveat on fidelity: `PkBloom`, `pk_digest` and `process_stream_into_keyset` are
//! `pub(crate)`, so this bench reconstructs the pipeline from the public scan plus a
//! local split-block filter and XXH3 of the same encoded bytes. Absolute numbers are
//! therefore indicative; the STAGE RATIOS, which is what decides the fix, are not
//! affected by the reimplementation.

#![allow(clippy::expect_used, reason = "benches fail loudly")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::row_converter::{RowConverter, SortField};
use hash_index::hash_key_128;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;

/// Minimal Cayenne fixture. Deliberately NOT the `vs_duckdb_helpers` one: that
/// module links the `duckdb` crate, which this bench has no use for.
mod fixture {
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::dml::InsertOp;

    pub struct Fixture {
        pub _dir: tempfile::TempDir,
        pub table: Arc<CayenneTableProvider>,
    }

    pub async fn create(schema: Arc<Schema>, primary_key: Vec<String>) -> Fixture {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        tokio::fs::create_dir_all(&data).await.expect("data dir");
        let db = dir.path().join("catalog.db");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", db.to_string_lossy())).expect("catalog"),
        );
        catalog.init().await.expect("catalog init");
        let table = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                CreateTableOptions {
                    table_name: "pk_rebuild".to_string(),
                    schema,
                    primary_key,
                    on_conflict: None,
                    base_path: data.to_string_lossy().to_string(),
                    partition_column: None,
                    vortex_config: VortexConfig::default(),
                },
                Arc::new(RuntimeEnv::default()),
            )
            .await
            .expect("create_table"),
        );
        Fixture { _dir: dir, table }
    }

    pub async fn insert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
        let ctx = SessionContext::new();
        let schema = Arc::clone(batch.schema_ref());
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("memory exec");
        let plan = table
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .expect("insert plan");
        datafusion_physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("insert");
    }
}

/// Rows in the fixture table. Large enough that per-row costs dominate fixture
/// overhead, small enough to build in a local bench.
const ROWS: usize = 200_000;
/// Rows per insert batch.
const BATCH: usize = 50_000;
/// Fraction of keys placed in the deletion map for `plus_deletion_probe`.
const DELETED_EVERY: usize = 20;

fn pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("w_id", DataType::Int64, false),
        Field::new("d_id", DataType::Int64, false),
        Field::new("o_id", DataType::Int64, false),
        Field::new("number", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

fn pk_batch(schema: &Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let mk = |f: &dyn Fn(i64) -> i64| {
        Arc::new(Int64Array::from(
            (0..rows as i64).map(|i| f(start + i)).collect::<Vec<_>>(),
        )) as Arc<dyn Array>
    };
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            mk(&|id| id % 1_000),
            mk(&|id| id % 10),
            mk(&|id| id / 10),
            mk(&|id| id % 15),
            mk(&|id| id * 7),
        ],
    )
    .expect("pk batch")
}

/// A minimal split-block filter: one 256-bit block per key, eight lanes, one bit per
/// lane — the shape `PkBloom`'s v3 layout uses, so an insert touches one cache line.
struct Filter {
    blocks: Vec<[u32; 8]>,
    mask: u64,
}

impl Filter {
    fn with_expected_keys(keys: usize) -> Self {
        let want_blocks = (keys * 10 / 256).next_power_of_two().max(1);
        Self {
            blocks: vec![[0u32; 8]; want_blocks],
            mask: want_blocks as u64 - 1,
        }
    }

    #[inline]
    fn insert_hash(&mut self, hash: u64) {
        let block = &mut self.blocks[(hash & self.mask) as usize];
        let mut h = hash.rotate_left(17) | 1;
        for lane in block.iter_mut() {
            *lane |= 1u32 << (h & 31);
            h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13);
        }
    }
}

/// The SHIPPED digest — `pk_digest` is `hash_key_128` over the encoded row bytes,
/// and `hash_index` is a normal dependency, so the bench measures the real function
/// rather than a stand-in whose cost is its own artifact.
#[inline]
fn digest(bytes: &[u8]) -> u64 {
    hash_key_128(bytes) as u64
}

/// What each stage of the pipeline does with a scanned batch.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    ScanOnly,
    Encode,
    Digest,
    Insert,
    DeletionProbe,
    HashColumnOnly,
}

fn scan_plan(
    rt: &tokio::runtime::Runtime,
    fixture: &fixture::Fixture,
    projection: Vec<usize>,
) -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new();
    rt.block_on(async {
        fixture
            .table
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .expect("scan plan")
    })
}

#[expect(clippy::too_many_lines, reason = "one linear pipeline per stage")]
fn drain(
    rt: &tokio::runtime::Runtime,
    plan: &Arc<dyn ExecutionPlan>,
    stage: Stage,
    deleted: &HashMap<u64, i64>,
    parallel: bool,
) -> usize {
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let converter = || {
        RowConverter::new(
            (0..4)
                .map(|_| SortField::new(DataType::Int64))
                .collect::<Vec<_>>(),
        )
        .expect("row converter")
    };
    rt.block_on(async {
        // Serial = ONE coalesced stream, which is exactly what the rebuild does
        // (`execute_stream(scan_plan, …)`). Executing a single partition of a
        // multi-partition plan is not the same thing and is not even legal: a
        // `RepartitionExec` panics with "partition not used yet" if its other
        // outputs are never consumed.
        let streams = if parallel {
            let n = plan.output_partitioning().partition_count();
            (0..n)
                .map(|part| plan.execute(part, Arc::clone(&task_ctx)).expect("execute"))
                .collect::<Vec<_>>()
        } else {
            vec![
                datafusion_physical_plan::execute_stream(Arc::clone(plan), Arc::clone(&task_ctx))
                    .expect("execute_stream"),
            ]
        };
        let n = streams.len();
        let mut handles = Vec::with_capacity(n);
        for stream in streams {
            let deleted = deleted.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = stream;
                let mut conv = (stage != Stage::ScanOnly && stage != Stage::HashColumnOnly)
                    .then(converter);
                let mut filter = Filter::with_expected_keys(ROWS / n.max(1));
                let mut seen = 0usize;
                while let Some(batch) = stream.next().await {
                    let batch = batch.expect("batch");
                    seen += batch.num_rows();
                    match stage {
                        Stage::ScanOnly => {}
                        Stage::HashColumnOnly => {
                            let col = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("int64 column");
                            for i in 0..col.len() {
                                filter.insert_hash(col.value(i) as u64);
                            }
                        }
                        _ => {
                            let cols: Vec<_> =
                                (0..4).map(|i| Arc::clone(batch.column(i))).collect();
                            let rows = conv
                                .as_mut()
                                .expect("converter")
                                .convert_columns(&cols)
                                .expect("convert");
                            if stage == Stage::Encode {
                                continue;
                            }
                            for row in rows.iter() {
                                let h = digest(row.as_ref());
                                match stage {
                                    Stage::Digest => {}
                                    Stage::Insert => filter.insert_hash(h),
                                    Stage::DeletionProbe => {
                                        if deleted.get(&h).is_none() {
                                            filter.insert_hash(h);
                                        }
                                    }
                                    Stage::ScanOnly | Stage::Encode | Stage::HashColumnOnly => {}
                                }
                            }
                        }
                    }
                }
                std::hint::black_box(&filter);
                seen
            }));
        }
        let mut total = 0;
        for h in handles {
            total += h.await.expect("join");
        }
        total
    })
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let schema = pk_schema();
    let fixture = rt.block_on(async {
        let f = fixture::create(
            Arc::clone(&schema),
            ["w_id", "d_id", "o_id", "number"]
                .iter()
                .map(|c| (*c).to_string())
                .collect(),
        )
        .await;
        eprintln!("[bench] fixture created");
        let mut start = 0i64;
        while (start as usize) < ROWS {
            fixture::insert(&f.table, pk_batch(&schema, start, BATCH)).await;
            start += BATCH as i64;
            eprintln!("[bench] inserted {start}/{ROWS}");
        }
        f
    });

    // Never trust the declared row count: if the fixture landed fewer rows than
    // `ROWS` (an insert path that dedups, a projection pushdown that elides work)
    // every per-row figure below is wrong by that factor. Measure it, and fail
    // loudly on a mismatch rather than reporting a throughput that is off by 10x.
    eprintln!("[bench] planning serial verification drain");
    let observed = drain(
        &rt,
        &scan_plan(&rt, &fixture, vec![0, 1, 2, 3]),
        Stage::ScanOnly,
        &HashMap::new(),
        false,
    );
    eprintln!("[bench] serial drain saw {observed}; planning partitioned drain");
    let observed_parallel = drain(
        &rt,
        &scan_plan(&rt, &fixture, vec![0, 1, 2, 3]),
        Stage::ScanOnly,
        &HashMap::new(),
        true,
    );
    eprintln!("fixture rows: serial drain saw {observed}, partitioned drain saw {observed_parallel}");
    assert_eq!(
        observed, ROWS,
        "the fixture must hold exactly the rows the throughput is declared against"
    );
    assert_eq!(
        observed_parallel, ROWS,
        "the partitioned drain must see every row the serial drain does, or consume/partitioned is measuring less work"
    );

    let deleted: HashMap<u64, i64> = (0..ROWS as u64)
        .step_by(DELETED_EVERY)
        .map(|k| (k, k as i64))
        .collect();

    let mut group = c.benchmark_group("pk_index_rebuild_breakdown");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(10);

    // An `ExecutionPlan` is single-use here: re-executing a `RepartitionExec`
    // instance panics ("partition not used yet"), so each iteration gets a fresh
    // plan. `iter_batched` keeps that setup out of the measurement.
    let pk_cols = vec![0, 1, 2, 3];
    for (name, stage, projection, parallel) in [
        ("stage/scan_only", Stage::ScanOnly, &pk_cols, false),
        ("stage/plus_encode", Stage::Encode, &pk_cols, false),
        ("stage/plus_digest", Stage::Digest, &pk_cols, false),
        ("stage/plus_insert", Stage::Insert, &pk_cols, false),
        ("stage/plus_deletion_probe", Stage::DeletionProbe, &pk_cols, false),
        ("alt/hash_column_only", Stage::HashColumnOnly, &vec![2], false),
        // B2: is the gap the SERIAL CONSUMER downstream of the coalesce? The scan
        // itself is already partitioned; `serial` drains one coalesced stream, the
        // way `execute_stream` does today, while `partitioned` does the per-key work
        // inside each partition.
        ("consume/serial", Stage::Insert, &pk_cols, false),
        ("consume/partitioned", Stage::Insert, &pk_cols, true),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || scan_plan(&rt, &fixture, projection.clone()),
                |plan| drain(&rt, &plan, stage, &deleted, parallel),
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
