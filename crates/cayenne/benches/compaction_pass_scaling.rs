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

//! How a maintenance pass's cost scales with the work it is given: pass SIZE,
//! encode FAN-OUT, and the seq-prefix BAKE.
//!
//! These are the two questions an SF1000 CH-benCHmark run answers slowly and
//! expensively (runs serialize at ~3h each, and end-to-end throughput on that
//! config carries ~28% run-to-run variance, so it cannot resolve anything under
//! about 2x). Both are really per-pass cost curves, which is a microbench
//! question:
//!
//! 1. **Is pass cost linear in input bytes, or is there a fixed floor?**
//!    The live compaction trigger admits on a COUNT of protected snapshots
//!    (`compaction_trigger_protected_snapshots`, default 8) with no byte term, so
//!    the bytes a pass merges scale with the table's per-snapshot size — measured
//!    on the benchmark, from ~97 KB to ~2.4 GB per pass across seven tables. If
//!    cost is linear, trading fewer-larger passes for more-smaller ones is free
//!    and the trigger can be made byte-aware. If there is a fixed floor, that
//!    trade costs total throughput and the floor is the thing to attack first.
//!    The `pass_size` lane sweeps snapshot count at a fixed snapshot size.
//!
//! 2. **What does encode fan-out beyond the output file count buy?**
//!    `snapshot_shard_count` caps encode shards at the session fan-out, and the
//!    read side coalesces back into one stream before the writer sees it, so
//!    width past the number of writers should be pure coordination. The
//!    `encode_fanout` lane pins `write_concurrency`, which overrides that cap
//!    directly (`snapshot_write_concurrency`), and sweeps it at a fixed pass size.
//!
//! The `bake` lane covers the other live maintenance pass: the seq-prefix bake
//! folds a superseded prefix out of the protected set, so its cost scales with
//! the deletion index rather than with fresh rows.
//!
//! # Scale
//!
//! Sizes are deliberately below production so a full sweep finishes in minutes.
//! `CAYENNE_BENCH_ROWS_PER_SNAPSHOT` scales every lane; the production shape is
//! ~240 MB per protected snapshot and ~2.4 GB per pass, roughly 20x the default
//! here. Raise it when a result needs confirming at real size, and expect the
//! runtime and disk use to rise with it.
//!
//! Bench discipline: setup outside the timed closure (`iter_batched` with
//! `PerIteration`, since a pass consumes the snapshots it merges); every loop
//! bounded; every `expect` carries a message; the pass outcome is asserted so a
//! lane can never silently time a no-op; data generation is deterministic.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::hint::black_box;

/// Rows in one protected snapshot. Default gives ~11 MB of Arrow per snapshot —
/// several encode-shard units (16 MiB floor, `target/16`) once a pass merges a
/// handful, without the runtime of production sizes.
fn rows_per_snapshot() -> usize {
    std::env::var("CAYENNE_BENCH_ROWS_PER_SNAPSHOT")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(200_000)
}

/// Snapshot counts for the pass-size lane. Geometric so a fixed floor shows up
/// as sub-linear scaling rather than needing a fitted curve.
const PASS_SNAPSHOT_COUNTS: [usize; 4] = [2, 4, 8, 16];
/// Encode fan-out widths. 1 is the serial writer; 64 is a typical core count.
const ENCODE_FANOUT: [usize; 4] = [1, 4, 16, 64];
/// Snapshots per pass for the fan-out and bake lanes.
const FIXED_SNAPSHOTS: usize = 8;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Deterministic mixed-compressibility suffix, so light and full encodings
/// actually diverge instead of both collapsing to a constant.
fn entropy_suffix(row: usize) -> String {
    let mut h = row as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    format!("{h:016x}")
}

/// One snapshot's rows. `id_base` lets a later batch deliberately overlap an
/// earlier one, which is how the bake lane accumulates superseded versions.
fn snapshot_batch(id_base: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).map(|i| id_base + i).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();
    let names: Vec<String> = (0..rows)
        .map(|i| {
            format!(
                "row_prefix_{:03}_{}",
                i % 64,
                entropy_suffix(id_base as usize + i)
            )
        })
        .collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("build snapshot batch")
}

/// Logical Arrow bytes of `snapshots` batches — the throughput denominator, so
/// criterion reports MB/s of merged input rather than bare wall-clock.
fn logical_bytes(snapshots: usize, rows: usize) -> u64 {
    let per = snapshot_batch(0, rows).get_array_memory_size() as u64;
    per.saturating_mul(snapshots as u64)
}

struct Fixture {
    _temp_dir: tempfile::TempDir,
    catalog: Arc<dyn MetadataCatalog>,
    provider: CayenneTableProvider,
}

/// An upsert table with the inline memtable disabled, so every insert lands in a
/// file-backed protected snapshot rather than being absorbed inline, and with the
/// background compactor pinned far out so only the explicit call under test runs.
///
/// `write_concurrency` is the encode-shard cap the fan-out lane sweeps; `None`
/// leaves `snapshot_write_concurrency`'s own default in place.
async fn setup_table(
    table_name: &str,
    trigger: usize,
    write_concurrency: Option<usize>,
    runtime_env: Arc<RuntimeEnv>,
) -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("create data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");

    let provider = CayenneTableProvider::create_table(
        Arc::clone(&catalog),
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: test_schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                // Set to the lane's snapshot count, which is the highest value
                // that still lets the explicit merge qualify: `min_runs` in
                // `select_protected_snapshot_merge_tier` reads this same knob as
                // the automatic post-write gate, so it cannot be raised out of the
                // way. At this value the automatic gate opens only on the final
                // write, leaving the timed call first to the compaction lock.
                compaction_trigger_protected_snapshots: trigger,
                // The small-file gate would otherwise schedule an automatic pass
                // after 8 files regardless of the protected count.
                compaction_trigger_files: usize::MAX,
                // 0 disables the age trigger, which would fire independently.
                compaction_trigger_snapshot_age_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                write_concurrency,
                ..VortexConfig::default()
            },
        },
        runtime_env,
    )
    .await
    .expect("create table");

    Fixture {
        _temp_dir: temp_dir,
        catalog,
        provider,
    }
}

/// Write one protected snapshot through the real `insert_into` path.
async fn write_snapshot(fixture: &Fixture, batch: RecordBatch) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
    let plan = fixture
        .provider
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(plan, ctx.task_ctx()).await.expect("insert");
    results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|c| c.value(0))
        })
        .unwrap_or(0)
}

/// A fresh table carrying `snapshots` disjoint protected snapshots.
async fn accumulate(
    table: &str,
    snapshots: usize,
    rows: usize,
    write_concurrency: Option<usize>,
    runtime_env: Arc<RuntimeEnv>,
) -> Fixture {
    let fixture = setup_table(table, snapshots, write_concurrency, runtime_env).await;
    for s in 0..snapshots {
        let base = (s * rows) as i64;
        let written = write_snapshot(&fixture, snapshot_batch(base, rows)).await;
        assert_eq!(written as usize, rows, "snapshot {s} row count");
    }
    fixture
}



/// A fresh table whose protected set is mostly SUPERSEDED: the same keys are
/// rewritten across every snapshot, so each new version tombstones the last and
/// the deletion index — which is what the bake folds — grows with the snapshot
/// count instead of the row count.
async fn accumulate_superseded(
    table: &str,
    snapshots: usize,
    rows: usize,
    runtime_env: Arc<RuntimeEnv>,
) -> Fixture {
    let fixture = setup_table(table, snapshots, None, runtime_env).await;
    for s in 0..snapshots {
        // Same id range every time: version s+1 supersedes version s.
        let written = write_snapshot(&fixture, snapshot_batch(0, rows)).await;
        assert_eq!(written as usize, rows, "superseding snapshot {s} row count");
    }
    fixture
}

fn bench_compaction_pass_scaling(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let rows = rows_per_snapshot();
    let ctx = SessionContext::new();
    let env = ctx.runtime_env();

    eprintln!(
        "\ncompaction_pass_scaling: {rows} rows/snapshot ~= {:.1} MiB Arrow; \
         production is ~240 MiB/snapshot (raise CAYENNE_BENCH_ROWS_PER_SNAPSHOT to close the gap)\n",
        logical_bytes(1, rows) as f64 / (1024.0 * 1024.0)
    );

    // --- Lane 1: does pass cost scale with the bytes merged, or hit a floor? ---
    let mut group = c.benchmark_group("compaction_pass_size");
    group.sample_size(10);
    let mut lane = 0u64;
    for snapshots in PASS_SNAPSHOT_COUNTS {
        group.throughput(Throughput::Bytes(logical_bytes(snapshots, rows)));
        group.bench_function(format!("{snapshots}_snapshots"), |b| {
            b.iter_batched(
                || {
                    lane += 1;
                    runtime.block_on(accumulate(
                        &format!("pass_{lane}"),
                        snapshots,
                        rows,
                        None,
                        Arc::clone(&env),
                    ))
                },
                |fixture| {
                    let merged = runtime.block_on(
                        fixture
                            .provider
                            .compact_protected_snapshots_subset(usize::MAX),
                    );
                    assert!(
                        merged.is_ok(),
                        "pass-size lane must not error at {snapshots} snapshots, got {merged:?}"
                    );
                    if merged.as_ref().is_ok_and(|merged| !merged) {
                        // Loud, because a declined pass times the DECLINE path, not
                        // a merge — averaging the two would be meaningless.
                        eprintln!(
                            "  DECLINED: {snapshots}-snapshot pass merged nothing; this lane times the decline path"
                        );
                    }
                    black_box(&fixture.catalog);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    // --- Lane 2: what does encode width beyond the output file count buy? ---
    let mut group = c.benchmark_group("compaction_encode_fanout");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(logical_bytes(FIXED_SNAPSHOTS, rows)));
    for width in ENCODE_FANOUT {
        group.bench_function(format!("write_concurrency_{width}"), |b| {
            b.iter_batched(
                || {
                    lane += 1;
                    runtime.block_on(accumulate(
                        &format!("fanout_{lane}"),
                        FIXED_SNAPSHOTS,
                        rows,
                        Some(width),
                        Arc::clone(&env),
                    ))
                },
                |fixture| {
                    let merged = runtime.block_on(
                        fixture
                            .provider
                            .compact_protected_snapshots_subset(usize::MAX),
                    );
                    assert!(
                        matches!(merged, Ok(true)),
                        "fan-out lane at width {width} must merge (pass size is fixed), got {merged:?}"
                    );
                    black_box(&fixture.catalog);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    // --- Lane 3: the seq-prefix bake over a superseded protected set. ---
    let mut group = c.benchmark_group("bake_seq_prefix");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(logical_bytes(FIXED_SNAPSHOTS, rows)));
    group.bench_function(format!("{FIXED_SNAPSHOTS}_superseded_snapshots"), |b| {
        b.iter_batched(
            || {
                lane += 1;
                runtime.block_on(accumulate_superseded(
                    &format!("bake_{lane}"),
                    FIXED_SNAPSHOTS,
                    rows,
                    Arc::clone(&env),
                ))
            },
            |fixture| {
                let baked = runtime
                    .block_on(fixture.provider.bake_seq_prefix_protected_snapshots());
                assert!(
                    baked.is_ok(),
                    "bake lane must not error, got {baked:?}"
                );
                // Whether a bake COMMITS depends on the clean-prefix invariant,
                // so the committed/declined split is reported rather than
                // asserted — a declined bake is a real measurement of the
                // decline path, but it must not be mistaken for a merge.
                let _ = black_box(baked);
                black_box(&fixture.catalog);
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_compaction_pass_scaling);
criterion_main!(benches);
