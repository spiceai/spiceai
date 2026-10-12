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

//! Scan planning latency right after a full refresh.
//!
//! A full refresh publishes a new snapshot with an `INSERT OVERWRITE`. Every scan
//! of the table then plans against that snapshot, and a primary-key lookup has
//! to list the snapshot's files with their statistics before it can prune to
//! the one file that holds its key. This bench measures what the scans that
//! reach a new snapshot together pay for that.
//!
//! Each round overwrites a file-backed table (many small files, so the listing
//! has real work to do), then immediately plans `CONCURRENT_SCANS` concurrent
//! `WHERE id = ?` scans and records each one's latency (`post_refresh_scan`).
//! Then it plans the same number of sequential scans against the settled
//! snapshot (`steady_scan`), and records how long the overwrite itself took to
//! complete (`refresh`). Every round checks that a lookup returns the round's
//! rows, so a fast answer from the wrong snapshot fails the run.
//!
//! Reports P50, P99, P99.9 and max per series.
//!
//! `cargo bench -p cayenne --bench listing_after_overwrite`
//! (`LISTING_BENCH_ROUNDS` overrides the round count).

#![expect(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{col, lit};
use datafusion_physical_plan::collect;

const TABLE_NAME: &str = "listing_after_overwrite";
/// Rows per overwrite. With `PAYLOAD_LEN` and a 1 MiB file target this writes
/// on the order of fifty files per snapshot.
const ROWS: i64 = 200_000;
const PAYLOAD_LEN: usize = 256;
const TARGET_FILE_SIZE_MB: usize = 1;
const CONCURRENT_SCANS: usize = 64;
const DEFAULT_ROUNDS: usize = 30;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Rows `0..ROWS` with `value = id * 1000 + round`, so a lookup tells which
/// round's snapshot answered it.
fn round_batch(round: i64) -> RecordBatch {
    let ids: Vec<i64> = (0..ROWS).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 1000 + round).collect();
    let mut payload = Vec::with_capacity(ids.len() * PAYLOAD_LEN);
    for id in &ids {
        let mut x = id.unsigned_abs().wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ round.unsigned_abs();
        for _ in 0..PAYLOAD_LEN {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            payload.push(x.to_le_bytes()[7]);
        }
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
            Arc::new(BinaryArray::from_iter_values(
                payload.chunks_exact(PAYLOAD_LEN),
            )),
        ],
    )
    .expect("batch")
}

async fn overwrite(table: &CayenneTableProvider, ctx: &SessionContext, round: i64) {
    let batch = round_batch(round);
    let plan = table
        .insert_into(
            &ctx.state(),
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
                .expect("memory exec"),
            InsertOp::Overwrite,
        )
        .await
        .expect("overwrite plan");
    collect(plan, ctx.task_ctx()).await.expect("overwrite");
}

async fn plan_lookup(table: &CayenneTableProvider, ctx: &SessionContext, id: i64) -> Duration {
    let start = Instant::now();
    let plan = table
        .scan(&ctx.state(), Some(&vec![1]), &[col("id").eq(lit(id))], None)
        .await
        .expect("scan");
    let elapsed = start.elapsed();
    black_box(plan);
    elapsed
}

async fn lookup_value(table: Arc<CayenneTableProvider>, ctx: &SessionContext, id: i64) -> Vec<i64> {
    let batches = ctx
        .read_table(table as Arc<dyn TableProvider>)
        .expect("read table")
        .filter(col("id").eq(lit(id)))
        .expect("filter")
        .select_columns(&["value"])
        .expect("project")
        .collect()
        .await
        .expect("lookup");
    batches
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
        .collect()
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

/// Files in the directory named `snapshot_id` anywhere under `root`.
fn snapshot_file_count(root: &std::path::Path, snapshot_id: &str) -> usize {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if entry.file_name().to_string_lossy() == snapshot_id {
                    return std::fs::read_dir(entry.path()).map_or(0, Iterator::count);
                }
                stack.push(entry.path());
            }
        }
    }
    0
}

async fn run(rounds: usize) {
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
    let runtime_env = Arc::new(RuntimeEnvBuilder::new().build().expect("runtime env"));
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            Arc::clone(&runtime_env),
        )
        .create(CreateTableOptions {
            table_name: TABLE_NAME.to_string(),
            schema: schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                target_vortex_file_size_mb: TARGET_FILE_SIZE_MB,
                compaction_background_interval_ms: 0,
                compaction_trigger_files: usize::MAX,
                compaction_trigger_protected_snapshots: usize::MAX,
                compaction_trigger_snapshot_age_ms: u64::MAX,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create table"),
    );
    table.init_scan_view_cache();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&runtime_env));

    // Settle one snapshot first so the first timed round is not the table's
    // first-ever listing.
    overwrite(&table, &ctx, 0).await;
    let _ = lookup_value(Arc::clone(&table), &ctx, 1).await;

    let mut refresh = Vec::with_capacity(rounds);
    let mut post_refresh = Vec::with_capacity(rounds * CONCURRENT_SCANS);
    let mut steady = Vec::with_capacity(rounds * CONCURRENT_SCANS);
    for round in 1..=rounds {
        let round = i64::try_from(round).expect("round fits i64");
        let start = Instant::now();
        overwrite(&table, &ctx, round).await;
        refresh.push(start.elapsed());

        let scans: Vec<_> = (0..CONCURRENT_SCANS)
            .map(|scan| {
                let table = Arc::clone(&table);
                let ctx = ctx.clone();
                let id = i64::try_from(scan).expect("scan fits i64") * 997 % ROWS;
                tokio::spawn(async move { plan_lookup(&table, &ctx, id).await })
            })
            .collect();
        for scan in scans {
            post_refresh.push(scan.await.expect("scan task"));
        }
        for scan in 0..CONCURRENT_SCANS {
            let id = i64::try_from(scan).expect("scan fits i64") * 1_013 % ROWS;
            steady.push(plan_lookup(&table, &ctx, id).await);
        }

        let id = 4_242;
        assert_eq!(
            lookup_value(Arc::clone(&table), &ctx, id).await,
            vec![id * 1000 + round],
            "round {round}: a lookup must read the round's snapshot"
        );
    }

    let files = snapshot_file_count(&data_path, &table.current_snapshot_id());
    println!(
        "listing_after_overwrite: rounds={rounds} rows={ROWS} files_per_snapshot={files} concurrent_scans={CONCURRENT_SCANS}"
    );
    report("refresh", &mut refresh);
    report("post_refresh_scan", &mut post_refresh);
    report("steady_scan", &mut steady);
}

fn main() {
    let rounds = std::env::var("LISTING_BENCH_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(run(rounds));
}
