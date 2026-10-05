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

#![allow(clippy::expect_used)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

//! Secondary index lookups on a table that keeps being appended to.
//!
//! An indexed table and an identical unindexed one take the same appends, each
//! written as its own Vortex file, and after every append the same point
//! lookups: keys the append just wrote, and keys from the initial load. The
//! appends' latency is the cost of indexing on write; the lookups' latency is
//! what the index is worth while the table changes. Percentiles are over every
//! individual operation, so the P99 reflects the lookups that met a stale or
//! missing index.
//!
//! Both tables have a primary key on `id` and key-based deletes. Without a key
//! a table uses position deletes, and every small-file compaction rewrites the
//! whole table while holding the write lock, so appends stall for the length
//! of that rewrite on either arm.
//!
//! Knobs: `FRESHNESS_BENCH_ROWS` (initial load, default 1,000,000),
//! `FRESHNESS_BENCH_APPENDS` (default 200), `FRESHNESS_BENCH_APPEND_ROWS`
//! (default 5,000, above the inline cap so each append writes a file).
//!
//! `FRESHNESS_BENCH_TRACE=<file>` writes Cayenne's debug events there, stamped
//! with the time since start, and every append slower than
//! `FRESHNESS_BENCH_SLOW_MS` (default 100) is printed on the same clock, so a
//! slow append can be matched to the write phases and background work that
//! overlapped it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneCatalog, CayenneContext, CayenneTableProvider, CayenneTableProviderBuilder,
    MetadataCatalog,
};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn rows(start: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + count as i64).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("payload-{id:016x}-{:016x}", id.wrapping_mul(0x9E37_79B9)))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn write(table: &Arc<CayenneTableProvider>, batch: RecordBatch, op: InsertOp) -> Duration {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("exec");
    let started = Instant::now();
    let plan = table
        .insert_into(&ctx.state(), exec, op)
        .await
        .expect("insert plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("insert");
    started.elapsed()
}

async fn lookup(table: &Arc<CayenneTableProvider>, id: i64) -> Duration {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let sql = format!(
        "SELECT id FROM t WHERE tenant = {} AND service = 'SV{id:032x}'",
        id % 997
    );
    let started = Instant::now();
    let batches = ctx
        .sql(&sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("lookup");
    let elapsed = started.elapsed();
    let found: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(found, 1, "lookup of {id} returned {found} rows");
    elapsed
}

async fn table(
    catalog: &Arc<CayenneCatalog>,
    env: &Arc<RuntimeEnv>,
    base: &std::path::Path,
    name: &str,
    indexes: Vec<Vec<String>>,
) -> Arc<CayenneTableProvider> {
    // Key-based deletes: in the default position mode a primary-key table's
    // small-file compaction deadlocks on the table's write lock
    // (spiceai/spiceai#14420).
    let vortex_config = VortexConfig {
        deletion_mode: cayenne::metadata::DeletionMode::Key,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(env), name);
    Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
            Arc::clone(env),
        )
        .with_context(context)
        .with_secondary_indexes(indexes)
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            // A primary key, so deletes are key-based and compaction can
            // rewrite just the small files instead of the whole table under
            // the write lock (what a keyless, position-delete table does).
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: base.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config,
        })
        .await
        .expect("create table"),
    )
}

fn percentiles(label: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let at = |q: f64| {
        let index = ((samples.len() as f64 * q).ceil() as usize).clamp(1, samples.len()) - 1;
        samples[index].as_secs_f64() * 1e3
    };
    println!(
        "{label:<32} n={:<6} p50={:>9.3}ms p99={:>9.3}ms p99.9={:>9.3}ms max={:>9.3}ms",
        samples.len(),
        at(0.50),
        at(0.99),
        at(0.999),
        at(1.0)
    );
}

/// Installs a global subscriber writing Cayenne's debug events to `path`,
/// stamped with the time since it was installed.
fn trace_to(path: &str) {
    use tracing_subscriber::fmt::time::Uptime;
    let file = std::fs::File::create(path).expect("trace file");
    tracing_subscriber::fmt()
        .with_writer(std::sync::Mutex::new(file))
        .with_ansi(false)
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            std::env::var("FRESHNESS_BENCH_TRACE_FILTER")
                .unwrap_or_else(|_| "cayenne=debug".to_string()),
        ))
        .init();
}

fn main() {
    if let Ok(path) = std::env::var("FRESHNESS_BENCH_TRACE") {
        trace_to(&path);
    }
    // The same origin as the trace's uptime clock, to within its install.
    let origin = Instant::now();
    let slow = Duration::from_millis(env_usize("FRESHNESS_BENCH_SLOW_MS", 100) as u64);
    let initial = env_usize("FRESHNESS_BENCH_ROWS", 1_000_000);
    let appends = env_usize("FRESHNESS_BENCH_APPENDS", 200);
    let append_rows = env_usize("FRESHNESS_BENCH_APPEND_ROWS", 5_000);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        tokio::fs::create_dir_all(&data).await.expect("data dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!(
                "sqlite://{}",
                dir.path().join("catalog.db").to_string_lossy()
            ))
            .expect("catalog"),
        );
        catalog.init().await.expect("catalog init");
        let env = Arc::new(RuntimeEnv::default());
        let key = vec![vec!["tenant".to_string(), "service".to_string()]];
        let indexed = table(&catalog, &env, &data, "indexed", key).await;
        let plain = table(&catalog, &env, &data, "plain", Vec::new()).await;
        for arm in [&indexed, &plain] {
            write(arm, rows(0, initial), InsertOp::Overwrite).await;
        }
        // Start once the initial load is indexed, however the index gets
        // built: lookups request a background build of any file it does not
        // cover yet.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut i = 0_i64;
        loop {
            lookup(&indexed, (i * 7919) % initial as i64).await;
            i += 1;
            let verification = indexed
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify the index");
            if verification.uncovered_files == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the initial load was not indexed within 60s: {verification:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let (mut write_indexed, mut write_plain) = (Vec::new(), Vec::new());
        let (mut fresh_indexed, mut fresh_plain) = (Vec::new(), Vec::new());
        let (mut old_indexed, mut old_plain) = (Vec::new(), Vec::new());
        let mut next = initial as i64;
        for round in 0..appends {
            // Alternate which arm goes first, so neither always meets a cold
            // cache or a busier machine.
            let arms: [(&Arc<CayenneTableProvider>, bool); 2] = if round % 2 == 0 {
                [(&indexed, true), (&plain, false)]
            } else {
                [(&plain, false), (&indexed, true)]
            };
            for (arm, is_indexed) in arms {
                let started = origin.elapsed();
                let took = write(arm, rows(next, append_rows), InsertOp::Append).await;
                if took >= slow {
                    println!(
                        "slow append: table={} round={round} start={:.3}s end={:.3}s took={:.1}ms",
                        if is_indexed { "indexed" } else { "plain" },
                        started.as_secs_f64(),
                        (started + took).as_secs_f64(),
                        took.as_secs_f64() * 1e3
                    );
                }
                let fresh: Vec<Duration> = {
                    let mut samples = Vec::new();
                    for offset in [0, 1, append_rows as i64 / 2, append_rows as i64 - 1] {
                        samples.push(lookup(arm, next + offset).await);
                    }
                    samples
                };
                let old: Vec<Duration> = {
                    let mut samples = Vec::new();
                    for i in 0..4_i64 {
                        let id = (round as i64 * 4 + i) * 104_729 % initial as i64;
                        samples.push(lookup(arm, id).await);
                    }
                    samples
                };
                if is_indexed {
                    write_indexed.push(took);
                    fresh_indexed.extend(fresh);
                    old_indexed.extend(old);
                } else {
                    write_plain.push(took);
                    fresh_plain.extend(fresh);
                    old_plain.extend(old);
                }
            }
            next += append_rows as i64;
        }

        println!(
            "initial={initial} appends={appends} append_rows={append_rows} build={}",
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }
        );
        percentiles("append indexed", &mut write_indexed);
        percentiles("append unindexed", &mut write_plain);
        percentiles("lookup just-appended indexed", &mut fresh_indexed);
        percentiles("lookup just-appended unindexed", &mut fresh_plain);
        percentiles("lookup initial-load indexed", &mut old_indexed);
        percentiles("lookup initial-load unindexed", &mut old_plain);
        println!(
            "indexed counters: {:?}",
            indexed.lookup_index_counters().expect("indexed")
        );
    });
}
