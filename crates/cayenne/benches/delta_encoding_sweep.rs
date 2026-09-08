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

//! Matrix sweep over `cayenne_delta_encoding` levels × `compression_strategy`
//! families: write latency (criterion) and on-disk bytes (printed table) for
//! one staged delta write per configuration.
//!
//! Lanes: {btrblocks, zstd} × {auto, 1..=9} on a 5,000-row staged write
//! (above the inline admission cap, so the encoding level applies). The
//! pre-pass prints a `(family, level) -> bytes` table — the engagement /
//! compression-ratio half of the matrix that wall-clock alone can't show.
//!
//! Bench discipline (Tiger Style): setup outside the timed closure; every
//! loop bounded; every `expect` carries a message; the timed write's row
//! count is asserted; data generation is deterministic (no RNG).

#![allow(clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CompressionStrategy, CreateTableOptions, DeltaEncoding, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use std::hint::black_box;

/// Above the inline admission cap (default 1024) so the write takes the
/// staged file path the encoding level applies to.
const ROW_COUNT: usize = 5_000;

/// Mixed-compressibility strings: a repetitive prefix (dict/FSST-friendly)
/// plus a long deterministic pseudo-entropy tail (where zstd can win).
const DISTINCT_PREFIXES: usize = 32;

const FAMILIES: &[(&str, CompressionStrategy)] = &[
    ("btrblocks", CompressionStrategy::Btrblocks),
    ("zstd", CompressionStrategy::Zstd),
];

fn encodings() -> Vec<(String, DeltaEncoding)> {
    let mut lanes = vec![("auto".to_string(), DeltaEncoding::Auto)];
    for level in 1..=9_u8 {
        lanes.push((format!("level{level}"), DeltaEncoding::Level(level)));
    }
    lanes
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Deterministic pseudo-entropy suffix (xorshift on the row index) so string
/// columns carry both repetitive and high-entropy content.
fn entropy_suffix(row: usize) -> String {
    let mut state = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(48);
    for _ in 0..6 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out
}

fn workload_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let names: Vec<String> = (0..rows)
        .map(|i| {
            format!(
                "delta_sweep_prefix_{:02}_{}",
                i % DISTINCT_PREFIXES,
                entropy_suffix(i)
            )
        })
        .collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("build workload batch")
}

struct SweepFixture {
    _temp_dir: tempfile::TempDir,
    table: Arc<CayenneTableProvider>,
    data_path: std::path::PathBuf,
    table_id: String,
}

async fn setup_lane(
    lane_name: &str,
    family: CompressionStrategy,
    encoding: DeltaEncoding,
) -> SweepFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("create data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let vortex_config = VortexConfig {
        compression_strategy: family,
        delta_encoding: encoding,
        ..VortexConfig::default()
    };
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: lane_name.to_string(),
                schema: test_schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create table"),
    );
    let table_id = catalog
        .get_table(lane_name)
        .await
        .expect("get table")
        .table_id;
    SweepFixture {
        _temp_dir: temp_dir,
        table,
        data_path,
        table_id,
    }
}

/// Write one delta via the real `insert_into` path — the inline gate buffers
/// the (over-cap) batch, computes an exact size estimate, and falls back to
/// the staged file write, so `auto` resolves against the true delta size
/// exactly as production inserts do.
async fn staged_write(fixture: &SweepFixture, batch: RecordBatch) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
    let insert_plan = fixture
        .table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(insert_plan, ctx.task_ctx()).await.expect("insert");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|counts| counts.value(0))
        })
        .unwrap_or(0)
}

fn vortex_bytes_under(dir: &Path) -> u64 {
    let mut total = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += vortex_bytes_under(&path);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

fn bench_delta_encoding_sweep(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let batch = workload_batch(ROW_COUNT);

    // --- Pre-pass: one write per lane, report on-disk bytes (engagement +
    // ratio table; wall-clock alone can't show this half of the matrix). ---
    eprintln!("\n=== delta_encoding_sweep on-disk bytes ({ROW_COUNT} rows) ===");
    eprintln!("{:<12} {:<8} {:>12}", "family", "level", "vortex_bytes");
    for (family_name, family) in FAMILIES {
        for (encoding_name, encoding) in encodings() {
            let lane = format!("sweep_{family_name}_{encoding_name}");
            let bytes = runtime.block_on(async {
                let fixture = setup_lane(&lane, family.clone(), encoding).await;
                let rows = staged_write(&fixture, batch.clone()).await;
                assert_eq!(rows as usize, ROW_COUNT, "staged write row count");
                vortex_bytes_under(&fixture.data_path.join(&fixture.table_id))
            });
            assert!(bytes > 0, "lane {lane} produced no vortex bytes");
            eprintln!("{family_name:<12} {encoding_name:<8} {bytes:>12}");
        }
    }
    eprintln!();

    // --- Timed matrix: staged write latency per (family, encoding). ---
    let mut group = c.benchmark_group("delta_encoding_sweep");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ROW_COUNT as u64));
    for (family_name, family) in FAMILIES {
        for (encoding_name, encoding) in encodings() {
            let lane = format!("{family_name}/{encoding_name}");
            let batch_for_lane = batch.clone();
            group.bench_with_input(
                BenchmarkId::new(*family_name, encoding_name.clone()),
                &encoding,
                |bencher, &encoding| {
                    // Sync bencher with `runtime.block_on` in BOTH closures
                    // (the `vs_duckdb_upsert_scaling` pattern): criterion's
                    // async executor would otherwise drive the setup closure
                    // from inside the runtime, and the nested `block_on`
                    // panics ("Cannot start a runtime from within a runtime").
                    bencher.iter_batched(
                        || {
                            let lane_table = format!(
                                "bench_{}_{}",
                                family_name,
                                encoding_name.replace('/', "_")
                            );
                            runtime.block_on(setup_lane(&lane_table, family.clone(), encoding))
                        },
                        |fixture| {
                            let batch = batch_for_lane.clone();
                            runtime.block_on(async {
                                let rows = staged_write(&fixture, batch).await;
                                assert_eq!(
                                    rows as usize, ROW_COUNT,
                                    "timed staged write row count"
                                );
                                black_box(rows);
                            });
                        },
                        criterion::BatchSize::PerIteration,
                    );
                },
            );
            let _ = lane;
        }
    }
    group.finish();
}

criterion_group!(benches, bench_delta_encoding_sweep);
criterion_main!(benches);
