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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_wrap)]

//! Steady-state timing harness for `deletion_mode` on a primary-key upsert
//! table. Seeds a table through `insert_into`, then drives upsert rounds through
//! the same path while the provider runs its own post-write maintenance and
//! compaction, timing writes and scans after every round. `#[ignore]`d: run it
//! explicitly with `--ignored --nocapture`, configured by `HARNESS_*` variables.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneContext, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// `SplitMix64`: a seeded generator so every arm sees the same keys and values.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("generation", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn rows_batch(schema: &Arc<Schema>, ids: &[i64], generation: i64) -> RecordBatch {
    let categories: Vec<String> = ids.iter().map(|id| format!("cat_{:02}", id % 37)).collect();
    let qty: Vec<i64> = ids
        .iter()
        .map(|id| (id.wrapping_mul(31).wrapping_add(generation * 7)).rem_euclid(1000))
        .collect();
    let amount: Vec<f64> = ids
        .iter()
        .map(|id| ((id % 10_000) as f64) * 0.25 + generation as f64)
        .collect();
    let generations = vec![generation; ids.len()];
    let payload: Vec<String> = ids
        .iter()
        .map(|id| format!("payload-{id:012}-g{generation:04}-xxxxxxxxxxxxxxxx"))
        .collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(qty)),
            Arc::new(Float64Array::from(amount)),
            Arc::new(Int64Array::from(generations)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("harness batch is valid")
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn summarize(label: &str, mode: &str, samples: &mut [f64]) {
    samples.sort_by(f64::total_cmp);
    eprintln!(
        "HARNESS mode={mode} metric={label} n={} p50_ms={:.3} p90_ms={:.3} p99_ms={:.3} p999_ms={:.3} max_ms={:.3}",
        samples.len(),
        percentile(samples, 50.0),
        percentile(samples, 90.0),
        percentile(samples, 99.0),
        percentile(samples, 99.9),
        samples.last().copied().unwrap_or(f64::NAN),
    );
}

/// Install an SDK meter provider whose reader the harness collects from, so a run
/// can report Cayenne's compaction outcomes. Call it before any table is built:
/// Cayenne binds its instruments to the global provider on first use.
fn install_compaction_metrics() -> telemetry::metrics_reader::MetricsReader {
    let reader = telemetry::metrics_reader::MetricsReader::new_cumulative();
    opentelemetry::global::set_meter_provider(
        opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build(),
    );
    reader
}

/// Every `cayenne_compaction_outcome_total` count, summed over tables, by kind and
/// outcome.
fn report_compaction_outcomes(mode: &str, reader: &telemetry::metrics_reader::MetricsReader) {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader as _;
    let mut resource_metrics = ResourceMetrics::default();
    if let Err(error) = reader.collect(&mut resource_metrics) {
        eprintln!("HARNESS mode={mode} metric=c_outcome error={error}");
        return;
    }
    let mut totals = std::collections::BTreeMap::<(String, String), u64>::new();
    for metric in resource_metrics
        .scope_metrics()
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
    {
        if metric.name() != "cayenne_compaction_outcome_total" {
            continue;
        }
        if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
            for point in sum.data_points() {
                let label = |key: &str| {
                    point
                        .attributes()
                        .find(|attribute| attribute.key.as_str() == key)
                        .map(|attribute| attribute.value.as_str().to_string())
                        .unwrap_or_default()
                };
                *totals.entry((label("kind"), label("outcome"))).or_default() += point.value();
            }
        }
    }
    for ((kind, outcome), count) in totals {
        eprintln!(
            "HARNESS mode={mode} metric=c_outcome kind={kind} outcome={outcome} count={count}"
        );
    }
}

/// Each table's P99, reported as the smallest, the median and the largest across
/// tables, so one table falling behind shows even when the pooled P99 does not.
fn summarize_per_table(label: &str, mode: &str, per_table: &mut [Vec<f64>]) {
    let mut p99s: Vec<f64> = per_table
        .iter_mut()
        .filter(|samples| !samples.is_empty())
        .map(|samples| {
            samples.sort_by(f64::total_cmp);
            percentile(samples, 99.0)
        })
        .collect();
    p99s.sort_by(f64::total_cmp);
    eprintln!(
        "HARNESS mode={mode} metric={label}_per_table tables={} p99_min_ms={:.3} p99_median_ms={:.3} p99_max_ms={:.3}",
        p99s.len(),
        p99s.first().copied().unwrap_or(f64::NAN),
        percentile(&p99s, 50.0),
        p99s.last().copied().unwrap_or(f64::NAN),
    );
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("harness query planned")
        .collect()
        .await
        .expect("harness query collected")
}

fn checksum(batches: &[RecordBatch]) -> String {
    let formatted = arrow::util::pretty::pretty_format_batches(batches).expect("format batches");
    formatted.to_string().replace('\n', " ")
}

/// Position-delete files and rows the catalog holds for the table (key-delete
/// files carry no source data file).
async fn position_delete_counts(
    catalog: &Arc<impl MetadataCatalog + ?Sized>,
    table: &CayenneTableProvider,
) -> (usize, i64, usize, i64) {
    let files = catalog
        .get_table_delete_files(table.table_id())
        .await
        .expect("delete files listed");
    let (mut pos_files, mut pos_rows, mut key_files, mut key_rows) = (0, 0, 0, 0);
    for file in &files {
        if file.source_data_file_path.is_some() {
            pos_files += 1;
            pos_rows += file.delete_count;
        } else {
            key_files += 1;
            key_rows += file.delete_count;
        }
    }
    (pos_files, pos_rows, key_files, key_rows)
}

/// Bytes of every file under `dir`, recursively. Reported for the fixture's data
/// directory as `dir_bytes`: the table's snapshots and any not yet swept. That is
/// not the size `deletion_mode: auto` sizes a table by, the files of its live
/// snapshots (`CayenneTableProvider::warm_listed_bytes`), which this harness cannot
/// report: an A/B compiles it against trunk's crate too, where that method is
/// absent.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// The deletion mode `HARNESS_MODE` names (`key` when unset). `auto-keeps-key` is
/// `auto` as it opens on a runtime with fewer than 8 CPU cores, on key deletes from
/// the start (see [`auto_keeps_key_deletes_from_env`]). Any other value fails the
/// run, so a typo cannot label one mode's numbers with another's name.
fn mode_from_env() -> DeletionMode {
    match env_string("HARNESS_MODE", "key").as_str() {
        "key" => DeletionMode::Key,
        "position" => DeletionMode::Position,
        "auto" | "auto-keeps-key" => DeletionMode::Auto,
        other => panic!(
            "HARNESS_MODE must be `auto`, `auto-keeps-key`, `key` or `position`, not {other:?}"
        ),
    }
}

/// Whether `HARNESS_MODE` names `auto` on key deletes from the start.
fn auto_keeps_key_deletes_from_env() -> bool {
    env_string("HARNESS_MODE", "key") == "auto-keeps-key"
}

/// Create a harness table. One that keeps key deletes from the start opens with a
/// context that says so, as the accelerator opens it: the stored configuration
/// never carries `auto_keeps_key_deletes`.
async fn create_harness_table(
    catalog: Arc<dyn MetadataCatalog>,
    options: CreateTableOptions,
    runtime_env: Arc<RuntimeEnv>,
) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
    if options.vortex_config.auto_keeps_key_deletes {
        let context = CayenneContext::new(
            &options.vortex_config,
            Arc::clone(&runtime_env),
            &options.table_name,
        );
        return Ok(CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .create(options)
            .await?);
    }
    Ok(CayenneTableProvider::create_table(catalog, options, runtime_env).await?)
}

/// How many rounds a fixed-rate reader started against how many its interval
/// scheduled within the run. A reader that keeps up starts that many, or one more
/// when a round falls due at the deadline itself. One whose rounds take longer than
/// the interval falls behind and starts fewer, putting less read load on its run
/// than on one that kept up.
fn report_read_rounds(mode: &str, started: usize, duration_s: u64, read_interval_ms: u64) {
    if read_interval_ms == 0 {
        return;
    }
    let scheduled = duration_s * 1000 / read_interval_ms;
    eprintln!("HARNESS mode={mode} metric=c_read_rounds started={started} scheduled={scheduled}");
}

/// Sets up Cayenne's process-wide compaction the way `spiced` does before any
/// table opens, so maintenance competes for the CPU here as it does there: the
/// global encode budget, and a dedicated compaction runtime whose worker threads
/// run at nice 10, as `ManagedTokioRuntime::with_low_priority` builds it. Every
/// compaction pass, the whole-table rewrite included, runs on that runtime. Left
/// on the test's runtime instead, a rewrite competes with writes and queries at
/// their own priority, which on a host with few cores costs writers more than it
/// does under `spiced` on Linux, where the lower priority is per thread. On
/// macOS `setpriority` lowers the whole process, writers included.
///
/// `HARNESS_COMPACTION_RUNTIME=shared` keeps compaction on the test's runtime.
fn install_compaction_runtime_from_env() {
    match env_string("HARNESS_COMPACTION_RUNTIME", "dedicated").as_str() {
        "shared" => return,
        "dedicated" => {}
        other => {
            panic!("HARNESS_COMPACTION_RUNTIME must be `dedicated` or `shared`, not {other:?}")
        }
    }
    let budget = cpu_budget::cpu_budget();
    cayenne::set_global_encode_concurrency(budget.cayenne_encode_permits());
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(budget.dedicated_runtime_worker_threads())
        .thread_name("compaction-worker")
        .enable_all();
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    builder.on_thread_start(|| {
        // SAFETY: `setpriority` takes no pointers; `PRIO_PROCESS` with id 0 names
        // the calling thread on Linux and the calling process on macOS.
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, 10);
        }
    });
    let runtime = builder.build().expect("compaction runtime builds");
    cayenne::set_compaction_runtime_handle(runtime.handle().clone());
    // Lives for the rest of the process, as it does in `spiced`. Dropped here,
    // inside the test's runtime, it would panic.
    std::mem::forget(runtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing harness; run explicitly with HARNESS_* variables"]
async fn deletion_mode_steady_state_harness() -> Result<(), Box<dyn std::error::Error>> {
    let mode = mode_from_env();
    let mode_label = env_string("HARNESS_MODE", "key");
    let rows = env_usize("HARNESS_ROWS", 1_000_000);
    let rounds = env_usize("HARNESS_ROUNDS", 30);
    let round_keys = env_usize("HARNESS_ROUND_KEYS", 10_000);
    let scans = env_usize("HARNESS_SCANS", 10);
    let seed = env_usize("HARNESS_SEED", 42) as u64;
    let pattern = env_string("HARNESS_PATTERN", "slice");
    let trigger = env_usize("HARNESS_TRIGGER", 50_000);
    let protected_trigger = env_usize("HARNESS_PROTECTED_TRIGGER", 0);
    let seed_batch = env_usize("HARNESS_SEED_BATCH", 100_000);
    let seed_op = env_string("HARNESS_SEED_OP", "append");
    let eager_capture = env_usize("HARNESS_EAGER_CAPTURE", 0) == 1;
    // `upsert`: each round upserts `round_keys` keys. `refresh`: each round
    // overwrites the whole table, repeating `dup_permille` of its keys in a
    // trailing batch (a source that delivers some keys twice).
    let workload = env_string("HARNESS_WORKLOAD", "upsert");
    let dup_permille = env_usize("HARNESS_DUP_PERMILLE", 0);

    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let schema = schema();
    let mut vortex_config = VortexConfig {
        deletion_mode: mode,
        auto_keeps_key_deletes: auto_keeps_key_deletes_from_env(),
        bake_deletion_index_trigger: trigger,
        ..VortexConfig::default()
    };
    if protected_trigger > 0 {
        vortex_config.compaction_trigger_protected_snapshots = protected_trigger;
    }
    let options = CreateTableOptions {
        table_name: "t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(create_harness_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    eprintln!(
        "HARNESS mode={mode_label} config rows={rows} rounds={rounds} round_keys={round_keys} scans={scans} seed={seed} pattern={pattern} trigger={trigger} protected_trigger={protected_trigger} seed_op={seed_op} eager_capture={eager_capture} workload={workload} dup_permille={dup_permille}"
    );

    // Seed: either appends (each publishes a protected snapshot on an upsert
    // table) or one overwrite (publishes the current snapshot).
    let seed_start = Instant::now();
    let mut seed_batches = Vec::new();
    let mut next = 0usize;
    while next < rows {
        let end = (next + seed_batch).min(rows);
        let ids: Vec<i64> = (next as i64..end as i64).collect();
        seed_batches.push(rows_batch(&schema, &ids, 0));
        next = end;
    }
    if seed_op == "overwrite" {
        common::write_batches(&table, seed_batches, InsertOp::Overwrite).await?;
    } else {
        for batch in seed_batches {
            common::insert_batch(&table, batch).await?;
        }
    }
    table.drain_in_flight_maintenance().await?;
    eprintln!(
        "HARNESS mode={mode_label} metric=seed_ms value={:.1} dir_bytes={}",
        seed_start.elapsed().as_secs_f64() * 1e3,
        dir_bytes(&fixture.data_path)
    );

    // Scans over a clean table (empty deletion index): the floor the deletion
    // filter adds to.
    let mut clean_scan_ms = Vec::new();
    for _ in 0..scans.max(5) {
        let start = Instant::now();
        run_query(&ctx, "SELECT SUM(amount), SUM(qty), COUNT(payload) FROM t").await;
        clean_scan_ms.push(start.elapsed().as_secs_f64() * 1e3);
    }
    summarize("scan_agg_clean", &mode_label, &mut clean_scan_ms);

    let mut rng = SplitMix(seed);
    let mut write_ms = Vec::with_capacity(rounds);
    let mut maint_ms = Vec::with_capacity(rounds);
    let mut scan_ms = Vec::new();
    let mut group_ms = Vec::new();
    let mut filter_ms = Vec::new();
    let mut point_ms = Vec::new();
    let mut count_ms = Vec::new();
    let mut index_trajectory = Vec::with_capacity(rounds);

    for round in 0..rounds {
        let generation = (round + 1) as i64;
        let ids: Vec<i64> = match pattern.as_str() {
            "random" => (0..round_keys)
                .map(|_| rng.below(rows as u64) as i64)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            "hot" => (0..round_keys)
                .map(|_| {
                    // 80% of the writes land on 20% of the keys.
                    if rng.below(10) < 8 {
                        rng.below((rows / 5).max(1) as u64) as i64
                    } else {
                        rng.below(rows as u64) as i64
                    }
                })
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            _ => {
                // A fresh slice per round, walking the key space.
                let start = (round * round_keys) % rows.max(1);
                (start..start + round_keys)
                    .map(|id| (id % rows) as i64)
                    .collect()
            }
        };

        let write_start = Instant::now();
        if workload == "refresh" {
            let mut batches = Vec::new();
            let mut next = 0usize;
            while next < rows {
                let end = (next + seed_batch).min(rows);
                let ids: Vec<i64> = (next as i64..end as i64).collect();
                batches.push(rows_batch(&schema, &ids, generation));
                next = end;
            }
            let dups = rows * dup_permille / 1000;
            if let Some(stride) = rows.checked_div(dups) {
                // Spread the repeated keys over the whole key space.
                let stride = stride.max(1);
                let ids: Vec<i64> = (0..dups).map(|i| ((i * stride) % rows) as i64).collect();
                batches.push(rows_batch(&schema, &ids, generation + 1_000));
            }
            common::write_batches(&table, batches, InsertOp::Overwrite).await?;
        } else {
            common::insert_batch(&table, rows_batch(&schema, &ids, generation)).await?;
        }
        write_ms.push(write_start.elapsed().as_secs_f64() * 1e3);

        let maint_start = Instant::now();
        tokio::time::timeout(Duration::from_mins(10), table.drain_in_flight_maintenance())
            .await
            .map_err(|_| format!("maintenance after round {round} did not drain"))??;
        if eager_capture {
            table.run_position_capture().await?;
        }
        maint_ms.push(maint_start.elapsed().as_secs_f64() * 1e3);

        index_trajectory.push(table.deletion_index_len());

        for _ in 0..scans {
            let start = Instant::now();
            run_query(&ctx, "SELECT SUM(amount), SUM(qty), COUNT(payload) FROM t").await;
            scan_ms.push(start.elapsed().as_secs_f64() * 1e3);

            let start = Instant::now();
            run_query(
                &ctx,
                "SELECT category, SUM(qty) FROM t GROUP BY category ORDER BY category",
            )
            .await;
            group_ms.push(start.elapsed().as_secs_f64() * 1e3);

            let start = Instant::now();
            run_query(&ctx, "SELECT COUNT(payload) FROM t WHERE qty < 50").await;
            filter_ms.push(start.elapsed().as_secs_f64() * 1e3);

            let key = rng.below(rows as u64);
            let start = Instant::now();
            run_query(&ctx, &format!("SELECT * FROM t WHERE id = {key}")).await;
            point_ms.push(start.elapsed().as_secs_f64() * 1e3);

            let start = Instant::now();
            run_query(&ctx, "SELECT COUNT(*) FROM t").await;
            count_ms.push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    summarize("write", &mode_label, &mut write_ms);
    summarize("maintenance_drain", &mode_label, &mut maint_ms);
    summarize("scan_agg", &mode_label, &mut scan_ms);
    summarize("scan_group", &mode_label, &mut group_ms);
    summarize("scan_filter", &mode_label, &mut filter_ms);
    summarize("point_lookup", &mode_label, &mut point_ms);
    summarize("count_star", &mode_label, &mut count_ms);
    eprintln!(
        "HARNESS mode={mode_label} metric=deletion_index_trajectory values={index_trajectory:?}"
    );
    eprintln!(
        "HARNESS mode={mode_label} metric=final accounted_bytes={} last_small_file_compact_path={:?}",
        table.accounted_memory_bytes(),
        table.last_small_file_compact_path(),
    );
    eprintln!(
        "HARNESS mode={mode_label} metric=final_dir_bytes value={}",
        dir_bytes(&fixture.data_path)
    );
    let (pos_files, pos_rows, key_files, key_rows) =
        position_delete_counts(&fixture.catalog, &table).await;
    eprintln!(
        "HARNESS mode={mode_label} metric=delete_files position_files={pos_files} position_rows={pos_rows} key_files={key_files} key_rows={key_rows}"
    );
    let answer = run_query(
        &ctx,
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS keys, SUM(generation) AS gens, SUM(qty) AS qty FROM t",
    )
    .await;
    eprintln!(
        "HARNESS mode={mode_label} metric=answer value={}",
        checksum(&answer)
    );
    Ok(())
}

/// Concurrent steady-state harness: a fixed-rate upsert writer and a closed-loop
/// reader run together for `HARNESS_DURATION_S` seconds while the provider runs
/// its own post-write maintenance and background compactor (the accelerator's
/// small-write profile: protected-snapshot trigger 4, 60 s age, 10 s interval).
/// Write latency is measured from each write's SCHEDULED start, so a write that
/// waits behind a maintenance pass holding the write lock is charged for the wait.
///
/// A closed-loop reader issues more queries the faster they run, and on a host
/// with few cores those queries take CPU from the writer. `HARNESS_READ_INTERVAL_MS`
/// starts each round of the four queries at a fixed rate instead, so two builds
/// whose reads differ in speed are compared at the same read load.
///
/// Compaction runs as under `spiced`; see [`install_compaction_runtime_from_env`].
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing harness; run explicitly with HARNESS_* variables"]
async fn deletion_mode_concurrent_harness() -> Result<(), Box<dyn std::error::Error>> {
    let mode = mode_from_env();
    install_compaction_runtime_from_env();
    let compaction_runtime = env_string("HARNESS_COMPACTION_RUNTIME", "dedicated");
    let mode_label = env_string("HARNESS_MODE", "key");
    let rows = env_usize("HARNESS_ROWS", 1_000_000);
    let round_keys = env_usize("HARNESS_ROUND_KEYS", 2_000);
    let write_interval_ms = env_usize("HARNESS_WRITE_INTERVAL_MS", 250) as u64;
    let duration_s = env_usize("HARNESS_DURATION_S", 120) as u64;
    let seed = env_usize("HARNESS_SEED", 42) as u64;
    let pattern = env_string("HARNESS_PATTERN", "slice");
    let trigger = env_usize("HARNESS_TRIGGER", 50_000);
    let seed_batch = env_usize("HARNESS_SEED_BATCH", 100_000);
    // `append` seeds through upserts, into protected snapshots; `overwrite` seeds
    // through one replace, into the current snapshot, as a full refresh does.
    let seed_op = env_string("HARNESS_SEED_OP", "append");
    // 0: the reader runs closed-loop. Otherwise a round of reads starts every
    // this many milliseconds.
    let read_interval_ms = env_usize("HARNESS_READ_INTERVAL_MS", 0) as u64;

    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let schema = schema();
    let vortex_config = VortexConfig {
        deletion_mode: mode,
        auto_keeps_key_deletes: auto_keeps_key_deletes_from_env(),
        bake_deletion_index_trigger: trigger,
        compaction_trigger_files: 4,
        compaction_trigger_protected_snapshots: 4,
        compaction_trigger_snapshot_age_ms: 60_000,
        compaction_background_interval_ms: 10_000,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: "t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(create_harness_table(catalog, options, ctx.runtime_env()).await?);
    table.spawn_background_compaction(Arc::new(tokio::sync::Semaphore::new(2)));
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    eprintln!(
        "HARNESS mode={mode_label} config concurrent rows={rows} round_keys={round_keys} write_interval_ms={write_interval_ms} duration_s={duration_s} seed={seed} pattern={pattern} trigger={trigger} read_interval_ms={read_interval_ms} compaction_runtime={compaction_runtime}"
    );

    let mut next = 0usize;
    let mut seed_batches = Vec::new();
    while next < rows {
        let end = (next + seed_batch).min(rows);
        let ids: Vec<i64> = (next as i64..end as i64).collect();
        if seed_op == "overwrite" {
            seed_batches.push(rows_batch(&schema, &ids, 0));
        } else {
            common::insert_batch(&table, rows_batch(&schema, &ids, 0)).await?;
        }
        next = end;
    }
    if seed_op == "overwrite" {
        common::write_batches(&table, seed_batches, InsertOp::Overwrite).await?;
    }
    table.drain_in_flight_maintenance().await?;
    eprintln!(
        "HARNESS mode={mode_label} metric=seed dir_bytes={} seed_op={seed_op}",
        dir_bytes(&fixture.data_path)
    );

    let deadline = Instant::now() + Duration::from_secs(duration_s);

    let writer = {
        let table = Arc::clone(&table);
        let schema = Arc::clone(&schema);
        let pattern = pattern.clone();
        tokio::spawn(async move {
            let mut rng = SplitMix(seed);
            let mut latencies = Vec::new();
            let interval = Duration::from_millis(write_interval_ms);
            let mut scheduled = Instant::now();
            let mut round = 0usize;
            while scheduled < deadline {
                let now = Instant::now();
                if now < scheduled {
                    tokio::time::sleep(scheduled - now).await;
                }
                let generation = (round + 1) as i64;
                let ids: Vec<i64> = if pattern == "random" {
                    (0..round_keys)
                        .map(|_| rng.below(rows as u64) as i64)
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect()
                } else {
                    let start = (round * round_keys) % rows.max(1);
                    (start..start + round_keys)
                        .map(|id| (id % rows) as i64)
                        .collect()
                };
                common::insert_batch(&table, rows_batch(&schema, &ids, generation))
                    .await
                    .expect("upsert");
                latencies.push(scheduled.elapsed().as_secs_f64() * 1e3);
                round += 1;
                scheduled += interval;
            }
            latencies
        })
    };

    let explain = env_usize("HARNESS_EXPLAIN", 0) == 1;
    let midpoint = Instant::now() + Duration::from_secs(duration_s / 2);
    let reader = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut rng = SplitMix(seed ^ 0xA5A5);
            let mut agg = Vec::new();
            let mut filter = Vec::new();
            let mut point = Vec::new();
            let mut count = Vec::new();
            let mut explained = !explain;
            let mut next_round = Instant::now();
            while Instant::now() < deadline {
                if read_interval_ms > 0 {
                    let now = Instant::now();
                    if now < next_round {
                        tokio::time::sleep(next_round - now).await;
                    }
                    next_round += Duration::from_millis(read_interval_ms);
                }
                if !explained && Instant::now() >= midpoint {
                    explained = true;
                    let plan = run_query(
                        &ctx,
                        "EXPLAIN ANALYZE SELECT SUM(amount), SUM(qty), COUNT(payload) FROM t",
                    )
                    .await;
                    eprintln!(
                        "HARNESS_EXPLAIN mid\n{}",
                        arrow::util::pretty::pretty_format_batches(&plan).expect("format plan")
                    );
                }
                let start = Instant::now();
                run_query(&ctx, "SELECT SUM(amount), SUM(qty), COUNT(payload) FROM t").await;
                agg.push(start.elapsed().as_secs_f64() * 1e3);
                let start = Instant::now();
                run_query(&ctx, "SELECT COUNT(payload) FROM t WHERE qty < 50").await;
                filter.push(start.elapsed().as_secs_f64() * 1e3);
                let key = rng.below(rows as u64);
                let start = Instant::now();
                run_query(&ctx, &format!("SELECT * FROM t WHERE id = {key}")).await;
                point.push(start.elapsed().as_secs_f64() * 1e3);
                let start = Instant::now();
                run_query(&ctx, "SELECT COUNT(*) FROM t").await;
                count.push(start.elapsed().as_secs_f64() * 1e3);
            }
            (agg, filter, point, count)
        })
    };

    let sampler = {
        let table = Arc::clone(&table);
        tokio::spawn(async move {
            let mut index = Vec::new();
            let mut memory = Vec::new();
            while Instant::now() < deadline {
                index.push(table.deletion_index_len());
                memory.push(table.accounted_memory_bytes());
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            (index, memory)
        })
    };

    let mut write_ms = writer.await?;
    let (mut agg, mut filter, mut point, mut count) = reader.await?;
    let (index, memory) = sampler.await?;
    report_read_rounds(&mode_label, agg.len(), duration_s, read_interval_ms);
    summarize("c_write", &mode_label, &mut write_ms);
    summarize("c_scan_agg", &mode_label, &mut agg);
    summarize("c_scan_filter", &mode_label, &mut filter);
    summarize("c_point_lookup", &mode_label, &mut point);
    summarize("c_count_star", &mode_label, &mut count);
    let index_max = index.iter().copied().max().unwrap_or(0);
    let memory_max = memory.iter().copied().max().unwrap_or(0);
    eprintln!(
        "HARNESS mode={mode_label} metric=c_deletion_index max={index_max} last={} samples={}",
        index.last().copied().unwrap_or(0),
        index.len()
    );
    eprintln!(
        "HARNESS mode={mode_label} metric=c_accounted_bytes max={memory_max} last={}",
        memory.last().copied().unwrap_or(0)
    );
    table.drain_in_flight_maintenance().await?;
    eprintln!(
        "HARNESS mode={mode_label} metric=c_final dir_bytes={} last_small_file_compact_path={:?}",
        dir_bytes(&fixture.data_path),
        table.last_small_file_compact_path()
    );
    let (pos_files, pos_rows, key_files, key_rows) =
        position_delete_counts(&fixture.catalog, &table).await;
    eprintln!(
        "HARNESS mode={mode_label} metric=c_delete_files position_files={pos_files} position_rows={pos_rows} key_files={key_files} key_rows={key_rows}"
    );
    let answer = run_query(
        &ctx,
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS keys, SUM(generation) AS gens, SUM(qty) AS qty FROM t",
    )
    .await;
    eprintln!(
        "HARNESS mode={mode_label} metric=c_answer value={}",
        checksum(&answer)
    );
    Ok(())
}

/// Many primary-key upsert tables at once, each under its own open-loop writer,
/// as a Spicepod with many small tables runs them. `HARNESS_TABLES` tables of
/// `HARNESS_ROWS` rows each; every writer upserts a slice of `HARNESS_ROUND_KEYS`
/// keys every `HARNESS_WRITE_INTERVAL_MS`, the writers staggered across the
/// interval. One reader starts a round of the four queries every
/// `HARNESS_READ_INTERVAL_MS` (default 60), on the next table in turn. Write and
/// read latencies are pooled across tables, and each table's P99 is reported as
/// well (the smallest, median and largest across tables). The final answer sums
/// every table's. Compaction runs as under `spiced`, on its runtime (see
/// [`install_compaction_runtime_from_env`]) and under its process-wide budget of
/// one compaction per core.
///
/// `HARNESS_SEED_OP=overwrite` seeds each table through one replace, into its
/// current snapshot, as a full refresh does (default `append`). `HARNESS_LARGE_ROWS`
/// adds one more table, `t_large`, of that many rows, seeded through one replace
/// and written like the others but never read: a large table rewriting itself whole
/// alongside the small ones. Every table's deletion index is sampled every 100 ms;
/// how far past its trigger it grows says how long its reclaims were held back.
/// The run ends with every `cayenne_compaction_outcome_total` count.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing harness; run explicitly with HARNESS_* variables"]
async fn deletion_mode_many_tables_harness() -> Result<(), Box<dyn std::error::Error>> {
    let mode = mode_from_env();
    install_compaction_runtime_from_env();
    let outcome_metrics = install_compaction_metrics();
    let compaction_runtime = env_string("HARNESS_COMPACTION_RUNTIME", "dedicated");
    let mode_label = env_string("HARNESS_MODE", "key");
    let tables = env_usize("HARNESS_TABLES", 20).max(1);
    let rows = env_usize("HARNESS_ROWS", 200_000);
    let round_keys = env_usize("HARNESS_ROUND_KEYS", 2_000);
    let write_interval_ms = env_usize("HARNESS_WRITE_INTERVAL_MS", 250) as u64;
    let duration_s = env_usize("HARNESS_DURATION_S", 120) as u64;
    let seed = env_usize("HARNESS_SEED", 42) as u64;
    let trigger = env_usize("HARNESS_TRIGGER", 50_000);
    let seed_batch = env_usize("HARNESS_SEED_BATCH", 100_000);
    let read_interval_ms = env_usize("HARNESS_READ_INTERVAL_MS", 60) as u64;
    let seed_op = env_string("HARNESS_SEED_OP", "append");
    let large_rows = env_usize("HARNESS_LARGE_ROWS", 0);

    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let schema = schema();
    let ctx = SessionContext::new();
    // Each table: its name, its rows, and whether it is seeded through one replace.
    let mut specs: Vec<(String, usize, bool)> = (0..tables)
        .map(|index| (format!("t{index}"), rows, seed_op == "overwrite"))
        .collect();
    if large_rows > 0 {
        specs.push(("t_large".to_string(), large_rows, true));
    }
    let mut providers = Vec::with_capacity(specs.len());
    // The process-wide compaction budget `spiced` hands every Cayenne table: one
    // permit per core.
    let compactions = cayenne::compaction_budget();
    for (name, table_rows, overwrite) in &specs {
        let options = CreateTableOptions {
            table_name: name.clone(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: mode,
                auto_keeps_key_deletes: auto_keeps_key_deletes_from_env(),
                bake_deletion_index_trigger: trigger,
                compaction_trigger_files: 4,
                compaction_trigger_protected_snapshots: 4,
                compaction_trigger_snapshot_age_ms: 60_000,
                compaction_background_interval_ms: 10_000,
                ..VortexConfig::default()
            },
        };
        let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
        let table = Arc::new(create_harness_table(catalog, options, ctx.runtime_env()).await?);
        table.spawn_background_compaction(Arc::clone(&compactions));
        ctx.register_table(name.as_str(), Arc::clone(&table) as Arc<dyn TableProvider>)?;
        let mut next = 0usize;
        let mut seed_batches = Vec::new();
        while next < *table_rows {
            let end = (next + seed_batch).min(*table_rows);
            let ids: Vec<i64> = (next as i64..end as i64).collect();
            if *overwrite {
                seed_batches.push(rows_batch(&schema, &ids, 0));
            } else {
                common::insert_batch(&table, rows_batch(&schema, &ids, 0)).await?;
            }
            next = end;
        }
        if !seed_batches.is_empty() {
            common::write_batches(&table, seed_batches, InsertOp::Overwrite).await?;
        }
        providers.push(table);
    }
    for table in &providers {
        table.drain_in_flight_maintenance().await?;
    }
    eprintln!(
        "HARNESS mode={mode_label} config many_tables tables={tables} rows={rows} round_keys={round_keys} write_interval_ms={write_interval_ms} duration_s={duration_s} seed={seed} trigger={trigger} read_interval_ms={read_interval_ms} compaction_runtime={compaction_runtime} seed_op={seed_op} large_rows={large_rows}"
    );

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_s);
    let interval = Duration::from_millis(write_interval_ms);
    let writers: Vec<_> = providers
        .iter()
        .zip(&specs)
        .enumerate()
        .map(|(index, (table, (_, table_rows, _)))| {
            let table = Arc::clone(table);
            let schema = Arc::clone(&schema);
            let table_rows = (*table_rows).max(1);
            let stagger = index as f64 / specs.len() as f64;
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut scheduled = start + interval.mul_f64(stagger);
                let mut round = 0usize;
                while scheduled < deadline {
                    let now = Instant::now();
                    if now < scheduled {
                        tokio::time::sleep(scheduled - now).await;
                    }
                    let first = (round * round_keys) % table_rows;
                    let ids: Vec<i64> = (first..first + round_keys)
                        .map(|id| (id % table_rows) as i64)
                        .collect();
                    common::insert_batch(&table, rows_batch(&schema, &ids, (round + 1) as i64))
                        .await
                        .expect("upsert");
                    latencies.push(scheduled.elapsed().as_secs_f64() * 1e3);
                    round += 1;
                    scheduled += interval;
                }
                latencies
            })
        })
        .collect();

    let reader = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut rng = SplitMix(seed ^ 0xA5A5);
            let (mut agg, mut filter, mut point, mut count) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            // Per table: aggregate, filter, point lookup, `COUNT(*)`.
            let mut by_table = vec![[Vec::new(), Vec::new(), Vec::new(), Vec::new()]; tables];
            let mut next_round = Instant::now();
            let mut round = 0usize;
            while Instant::now() < deadline {
                let now = Instant::now();
                if now < next_round {
                    tokio::time::sleep(next_round - now).await;
                }
                next_round += Duration::from_millis(read_interval_ms.max(1));
                let table = round % tables;
                let name = format!("t{table}");
                round += 1;
                let begin = Instant::now();
                run_query(
                    &ctx,
                    &format!("SELECT SUM(amount), SUM(qty), COUNT(payload) FROM {name}"),
                )
                .await;
                let ms = begin.elapsed().as_secs_f64() * 1e3;
                agg.push(ms);
                by_table[table][0].push(ms);
                let begin = Instant::now();
                run_query(
                    &ctx,
                    &format!("SELECT COUNT(payload) FROM {name} WHERE qty < 50"),
                )
                .await;
                let ms = begin.elapsed().as_secs_f64() * 1e3;
                filter.push(ms);
                by_table[table][1].push(ms);
                let key = rng.below(rows as u64);
                let begin = Instant::now();
                run_query(&ctx, &format!("SELECT * FROM {name} WHERE id = {key}")).await;
                let ms = begin.elapsed().as_secs_f64() * 1e3;
                point.push(ms);
                by_table[table][2].push(ms);
                let begin = Instant::now();
                run_query(&ctx, &format!("SELECT COUNT(*) FROM {name}")).await;
                let ms = begin.elapsed().as_secs_f64() * 1e3;
                count.push(ms);
                by_table[table][3].push(ms);
            }
            (agg, filter, point, count, by_table)
        })
    };
    // Each table's largest deletion index, sampled every 100 ms.
    let sampler = {
        let providers: Vec<_> = providers.iter().map(Arc::clone).collect();
        tokio::spawn(async move {
            let mut peaks = vec![0usize; providers.len()];
            while Instant::now() < deadline {
                for (peak, table) in peaks.iter_mut().zip(&providers) {
                    *peak = (*peak).max(table.deletion_index_len());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            peaks
        })
    };

    // The small tables' writes are pooled; the large table's are reported apart.
    let mut write_ms = Vec::new();
    let mut write_by_table = Vec::with_capacity(tables);
    let mut large_write_ms = Vec::new();
    for (index, writer) in writers.into_iter().enumerate() {
        let table_ms = writer.await?;
        if index < tables {
            write_ms.extend_from_slice(&table_ms);
            write_by_table.push(table_ms);
        } else {
            large_write_ms = table_ms;
        }
    }
    let peaks = sampler.await?;
    let mut small_peaks = peaks.iter().take(tables).copied().collect::<Vec<_>>();
    small_peaks.sort_unstable();
    eprintln!(
        "HARNESS mode={mode_label} metric=c_index_peak_per_table tables={tables} trigger={trigger} min={} median={} max={}",
        small_peaks.first().copied().unwrap_or(0),
        small_peaks.get(small_peaks.len() / 2).copied().unwrap_or(0),
        small_peaks.last().copied().unwrap_or(0),
    );
    if large_rows > 0 {
        summarize("c_write_large", &mode_label, &mut large_write_ms);
        eprintln!(
            "HARNESS mode={mode_label} metric=c_index_peak_large rows={large_rows} trigger={trigger} peak={}",
            peaks.get(tables).copied().unwrap_or(0)
        );
    }
    let (mut agg, mut filter, mut point, mut count, mut by_table) = reader.await?;
    report_read_rounds(&mode_label, agg.len(), duration_s, read_interval_ms.max(1));
    summarize("c_write", &mode_label, &mut write_ms);
    summarize("c_scan_agg", &mode_label, &mut agg);
    summarize("c_scan_filter", &mode_label, &mut filter);
    summarize("c_point_lookup", &mode_label, &mut point);
    summarize("c_count_star", &mode_label, &mut count);
    summarize_per_table("c_write", &mode_label, &mut write_by_table);
    for (query, label) in [
        "c_scan_agg",
        "c_scan_filter",
        "c_point_lookup",
        "c_count_star",
    ]
    .into_iter()
    .enumerate()
    {
        let mut per_table: Vec<Vec<f64>> = by_table
            .iter_mut()
            .map(|queries| std::mem::take(&mut queries[query]))
            .collect();
        summarize_per_table(label, &mode_label, &mut per_table);
    }
    let mut rewritten_whole = 0usize;
    let mut large_rewritten_whole = false;
    for (index, table) in providers.iter().enumerate() {
        table.drain_in_flight_maintenance().await?;
        let whole = matches!(
            table.last_small_file_compact_path(),
            cayenne::LastSmallFileCompactPath::Full
        );
        if index < tables {
            rewritten_whole += usize::from(whole);
        } else {
            large_rewritten_whole = whole;
        }
    }
    eprintln!(
        "HARNESS mode={mode_label} metric=c_final dir_bytes={} tables_rewritten_whole={rewritten_whole}/{tables} large_rewritten_whole={large_rewritten_whole} last_small_file_compact_path={}",
        dir_bytes(&fixture.data_path),
        if rewritten_whole > 0 { "Full" } else { "None" }
    );
    report_compaction_outcomes(&mode_label, &outcome_metrics);
    let union = specs
        .iter()
        .map(|(name, _, _)| format!("SELECT id, generation, qty FROM {name}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let answer = run_query(
        &ctx,
        &format!("SELECT COUNT(*) AS n, SUM(generation) AS gens, SUM(qty) AS qty FROM ({union})"),
    )
    .await;
    eprintln!(
        "HARNESS mode={mode_label} metric=c_answer value={}",
        checksum(&answer)
    );
    Ok(())
}
