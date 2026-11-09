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

//! Primary-key lookup latency on an upsert table with protected snapshots.
//!
//! A scan reads the current snapshot and every protected snapshot (an upsert's
//! rows stay in one until compaction folds them in), so it has to know each
//! protected snapshot's files. This bench measures what a `WHERE id = ?` lookup
//! pays for that, on a file-backed table whose initial load is followed by
//! `PROTECTED_BENCH_SNAPSHOTS` upserts (compaction off, so every one stays
//! protected).
//!
//! Two tables over the same data, one per scan-view reuse mode:
//! * `recapture` — [`ScanViewReuse::WithinLag`] of zero: every scan captures
//!   its own view, so it pays the capture on every lookup.
//! * `reuse` — [`ScanViewReuse::UntilInvalidated`], the default: scans share
//!   the view captured after the last write.
//!
//! For each, `plan` times `TableProvider::scan` alone and `collect` a full
//! executed lookup, which checks the row it returns. Reports P50, P99, P99.9 and
//! max per series, with the sample count.
//!
//! `cargo bench -p cayenne --bench protected_snapshot_scan_capture`
//! (`PROTECTED_BENCH_SNAPSHOTS`, `PROTECTED_BENCH_SAMPLES` override the
//! protected-snapshot count and the plan sample count).

#![expect(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    ScanViewReuse,
};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{col, lit};
use datafusion_physical_plan::collect;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const BASE_ROWS: i64 = 100_000;
const UPSERT_ROWS: i64 = 1_000;
const DEFAULT_SNAPSHOTS: usize = 16;
const DEFAULT_PLAN_SAMPLES: usize = 5_000;
const WARMUP: usize = 200;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Rows `start..start + rows` with `value = id * 10 + generation`.
fn batch(start: i64, rows: i64, generation: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10 + generation).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn insert(table: &dyn TableProvider, ctx: &SessionContext, batch: RecordBatch) {
    let plan = table
        .insert_into(
            &ctx.state(),
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
                .expect("memory exec"),
            InsertOp::Append,
        )
        .await
        .expect("insert plan");
    collect(plan, ctx.task_ctx()).await.expect("insert");
}

async fn create_table(
    catalog: &Arc<CayenneCatalog>,
    runtime_env: &Arc<RuntimeEnv>,
    data_path: &std::path::Path,
    name: &str,
    reuse: ScanViewReuse,
) -> Arc<CayenneTableProvider> {
    let table = CayenneTableProviderBuilder::new(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        Arc::clone(runtime_env),
    )
    .with_scan_view_reuse(reuse)
    .create(CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            compaction_background_interval_ms: 0,
            compaction_trigger_files: usize::MAX,
            compaction_trigger_protected_snapshots: usize::MAX,
            compaction_trigger_snapshot_age_ms: u64::MAX,
            ..VortexConfig::default()
        },
    })
    .await
    .expect("create table");
    Arc::new(table)
}

/// The id a sample looks up, spread over keys the upserts rewrote and keys
/// only the initial load holds.
fn lookup_id(sample: usize, snapshots: usize) -> (i64, i64) {
    let sample = i64::try_from(sample).expect("sample index");
    let snapshots = i64::try_from(snapshots).expect("snapshot count");
    let id = (sample * 7_919) % BASE_ROWS;
    // Upsert `g` (1-based) rewrote ids `(g - 1) * UPSERT_ROWS .. g * UPSERT_ROWS`.
    let generation = if id < snapshots * UPSERT_ROWS {
        id / UPSERT_ROWS + 1
    } else {
        0
    };
    (id, id * 10 + generation)
}

async fn plan_lookup(table: &dyn TableProvider, ctx: &SessionContext, id: i64) -> Duration {
    let start = Instant::now();
    let plan = table
        .scan(&ctx.state(), Some(&vec![1]), &[col("id").eq(lit(id))], None)
        .await
        .expect("scan");
    let elapsed = start.elapsed();
    black_box(plan);
    elapsed
}

async fn collect_lookup(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    id: i64,
    expected: i64,
) -> Duration {
    let start = Instant::now();
    let batches = ctx
        .read_table(Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("read table")
        .filter(col("id").eq(lit(id)))
        .expect("filter")
        .select_columns(&["value"])
        .expect("project")
        .collect()
        .await
        .expect("lookup");
    let elapsed = start.elapsed();
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 value")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![expected], "lookup of id {id}");
    elapsed
}

/// Nearest-rank percentile of `sorted`, in microseconds.
fn percentile_us(sorted: &[Duration], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "a rank within a bench sample count"
    )]
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1].as_micros()
}

fn report(name: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    println!(
        "{name:<18} n={:<6} p50={:>9}us p99={:>9}us p99.9={:>9}us max={:>9}us",
        samples.len(),
        percentile_us(samples, 50.0),
        percentile_us(samples, 99.0),
        percentile_us(samples, 99.9),
        samples.last().map_or(0, Duration::as_micros),
    );
}

/// The number of directories under `root` that directly hold a data file.
fn snapshot_dirs_with_files(root: &std::path::Path) -> usize {
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut holds_file = false;
        for path in entries.flatten().map(|entry| entry.path()) {
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "vortex") {
                holds_file = true;
            }
        }
        count += usize::from(holds_file);
    }
    count
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map_or(default, |value| value.parse().expect(name))
}

async fn run(snapshots: usize, plan_samples: usize) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            temp_dir.path().join("catalog.db").display()
        ))
        .expect("catalog"),
    );
    catalog.init().await.expect("catalog init");
    // The default runtime env caches directory listings, as spiced's does.
    let runtime_env = Arc::new(RuntimeEnvBuilder::new().build().expect("runtime env"));
    let ctx = SessionContext::new_with_config_rt(
        datafusion::prelude::SessionConfig::new(),
        Arc::clone(&runtime_env),
    );
    let collect_samples = plan_samples / 5;

    for (name, reuse) in [
        ("recapture", ScanViewReuse::WithinLag(Duration::ZERO)),
        ("reuse", ScanViewReuse::UntilInvalidated),
    ] {
        let table = create_table(&catalog, &runtime_env, &data_path, name, reuse).await;
        insert(table.as_ref(), &ctx, batch(0, BASE_ROWS, 0)).await;
        for generation in 1..=snapshots {
            let generation = i64::try_from(generation).expect("generation");
            insert(
                table.as_ref(),
                &ctx,
                batch((generation - 1) * UPSERT_ROWS, UPSERT_ROWS, generation),
            )
            .await;
        }
        let dirs = snapshot_dirs_with_files(&data_path.join(name));
        assert!(
            dirs > snapshots,
            "premise: every upsert left a protected snapshot ({dirs} snapshot dirs hold files)"
        );
        for sample in 0..WARMUP {
            let (id, expected) = lookup_id(sample, snapshots);
            collect_lookup(&table, &ctx, id, expected).await;
        }
        let mut plans = Vec::with_capacity(plan_samples);
        for sample in 0..plan_samples {
            let (id, _) = lookup_id(sample, snapshots);
            plans.push(plan_lookup(table.as_ref(), &ctx, id).await);
        }
        let mut collects = Vec::with_capacity(collect_samples);
        for sample in 0..collect_samples {
            let (id, expected) = lookup_id(sample, snapshots);
            collects.push(collect_lookup(&table, &ctx, id, expected).await);
        }
        report(&format!("{name}/plan"), &mut plans);
        report(&format!("{name}/collect"), &mut collects);
    }
}

fn main() {
    let snapshots = env_usize("PROTECTED_BENCH_SNAPSHOTS", DEFAULT_SNAPSHOTS);
    let plan_samples = env_usize("PROTECTED_BENCH_SAMPLES", DEFAULT_PLAN_SAMPLES);
    println!(
        "protected_snapshot_scan_capture: base_rows={BASE_ROWS} protected_snapshots={snapshots} upsert_rows={UPSERT_ROWS}"
    );
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(run(snapshots, plan_samples));
}
