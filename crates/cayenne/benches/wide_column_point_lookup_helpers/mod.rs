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
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::print_stdout)]

//! Concurrent primary-key lookups on a Cayenne table with a wide (~3 KB) string
//! column, through the shared Vortex segment cache (spiceai/spiceai#15323).
//!
//! The table mirrors the issue's HTTP caching table: a composite key
//! `(request_query, request_path)` with a lookup index on it, and a `content`
//! column of ~3 KB JSON. `CLIENTS` tasks each run `SELECT content ... WHERE key`
//! one after another, first over the oldest keys in insertion order, then over
//! keys sampled from the whole table. Reported per lookup: process CPU, bytes
//! allocated (every `pread` buffer and every segment-cache copy is an
//! allocation), and latency percentiles.
//!
//! The segment cache is process-wide and installed once, so each cache size is
//! its own run: `WCPL_CACHE_MB` (0 disables it).
//!
//! Knobs: `WCPL_ROWS` (default 200,000), `WCPL_CLIENTS` (16), `WCPL_THREADS`
//! (Tokio workers, 8), `WCPL_LOOKUPS` (per client per phase, 1,000),
//! `WCPL_CACHE_MB` (256), `WCPL_HOT_KEYS` (insertion-order key count, 80,000).
//!
//! `WCPL_EXTRA=1` adds the write and scan costs of the wide column: the load's CPU, full-column
//! queries (`WCPL_SCAN_REPS`, 11) before and after `WCPL_APPENDS` (40) appends of 2,048 rows,
//! and the CPU of those appends with the compaction they trigger.
//!
//! `cargo bench -p cayenne --bench wide_column_point_lookup` (macOS/Linux system allocator),
//! `--bench wide_column_point_lookup_jemalloc`, or `--bench wide_column_point_lookup_snmalloc`
//! (the allocator `spiced` ships with).

use std::alloc::{GlobalAlloc, Layout};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
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

/// Wraps the allocator under test and counts what is allocated through it.
pub struct Counting<A>(pub A);

static ALLOCATED: AtomicU64 = AtomicU64::new(0);
/// Bytes allocated by log2 size class, from 16 KiB (2^14) up.
static BIG: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];
static BIG_N: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];
/// When set, backtraces of allocations at least this large are collected.
static BT_MIN: AtomicU64 = AtomicU64::new(u64::MAX);
static BT_LEFT: AtomicU64 = AtomicU64::new(0);
static BACKTRACES: std::sync::Mutex<Vec<(usize, String)>> = std::sync::Mutex::new(Vec::new());
thread_local! { static IN_BT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn track(size: usize) {
    ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
    if size >= 1 << 14 {
        let class = (usize::BITS - 1 - size.leading_zeros()) as usize - 14;
        let class = class.min(17);
        BIG[class].fetch_add(size as u64, Ordering::Relaxed);
        BIG_N[class].fetch_add(1, Ordering::Relaxed);
        if size as u64 >= BT_MIN.load(Ordering::Relaxed)
            && BT_LEFT.load(Ordering::Relaxed) > 0
            && !IN_BT.with(std::cell::Cell::get)
        {
            IN_BT.with(|g| g.set(true));
            // Take a credit only while one is left: two threads past the check above could
            // otherwise both decrement the last one and wrap the count.
            if BT_LEFT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                let bt = std::backtrace::Backtrace::force_capture().to_string();
                if let Ok(mut v) = BACKTRACES.lock() {
                    v.push((size, bt));
                }
            }
            IN_BT.with(|g| g.set(false));
        }
    }
}

// SAFETY: delegates every call to the wrapped allocator and only adds bookkeeping.
unsafe impl<A: GlobalAlloc> GlobalAlloc for Counting<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track(layout.size());
        // SAFETY: forwarded unchanged.
        unsafe { self.0.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { self.0.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        track(layout.size());
        // SAFETY: forwarded unchanged.
        unsafe { self.0.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        track(new_size);
        // SAFETY: forwarded unchanged.
        unsafe { self.0.realloc(ptr, layout, new_size) }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `WCPL_KEY=int`: a single Int64 primary key and no secondary index, so lookups take the
/// key-block point read instead of the lookup index.
fn int_key() -> bool {
    std::env::var("WCPL_KEY").is_ok_and(|v| v == "int")
}

fn schema() -> Arc<Schema> {
    if int_key() {
        return Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
        ]));
    }
    Arc::new(Schema::new(vec![
        Field::new("request_query", DataType::Utf8, false),
        Field::new("request_path", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]))
}

/// Deterministic xorshift so every run loads identical data.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn key(i: usize) -> (String, String) {
    (
        format!("page={}&q={i:x}", i % 97),
        format!("/api/v1/items/{i}"),
    )
}

/// A ~3 KB JSON document: repeated keys, varying values, like a cached HTTP
/// response body.
fn json_row(rng: &mut Rng, i: usize) -> String {
    let mut s = String::with_capacity(3200);
    s.push_str(&format!("{{\"id\":{i},\"items\":["));
    let mut first = true;
    while s.len() < 3000 {
        if !first {
            s.push(',');
        }
        first = false;
        let a = rng.next();
        s.push_str(&format!(
            "{{\"sku\":\"SKU-{:08x}\",\"price\":{}.{:02},\"qty\":{},\"name\":\"item {:x}\",\"tags\":[\"t{}\",\"t{}\"]}}",
            a as u32,
            (a >> 32) % 1000,
            (a >> 40) % 100,
            (a >> 48) % 50,
            a >> 20,
            (a >> 8) % 17,
            (a >> 12) % 23
        ));
    }
    s.push_str("]}");
    s
}

fn batches(range: std::ops::Range<usize>) -> Vec<RecordBatch> {
    batches_of(range, 8192)
}

fn batches_of(range: std::ops::Range<usize>, size: usize) -> Vec<RecordBatch> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ range.start as u64);
    let rows = range.end;
    range
        .clone()
        .step_by(size)
        .map(|start| {
            let end = (start + size).min(rows);
            let (queries, paths): (Vec<String>, Vec<String>) = (start..end).map(key).unzip();
            let content: Vec<String> = (start..end).map(|i| json_row(&mut rng, i)).collect();
            if int_key() {
                return RecordBatch::try_new(
                    schema(),
                    vec![
                        Arc::new(arrow::array::Int64Array::from_iter_values(
                            (start..end).map(|i| i as i64),
                        )),
                        Arc::new(StringArray::from(content)),
                    ],
                )
                .expect("batch");
            }
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(queries)),
                    Arc::new(StringArray::from(paths)),
                    Arc::new(StringArray::from(content)),
                ],
            )
            .expect("batch")
        })
        .collect()
}

fn lookup_sql(i: usize) -> String {
    if int_key() {
        return format!("SELECT content FROM t WHERE id = {i}");
    }
    let (query, path) = key(i);
    format!("SELECT content FROM t WHERE request_path = '{path}' AND request_query = '{query}'")
}

/// Process user and system CPU seconds, and minor (no I/O) page faults.
fn usage() -> (f64, f64, i64) {
    // SAFETY: getrusage fills the zeroed struct.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to a rusage.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut ru) };
    // `tv_usec` is `i32` on macOS and `i64` on Linux.
    #[allow(clippy::cast_lossless)]
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (tv(ru.ru_utime), tv(ru.ru_stime), ru.ru_minflt)
}

fn cpu_seconds() -> f64 {
    let (user, sys, _) = usage();
    user + sys
}

async fn run_phase(label: &str, ctx: &Arc<SessionContext>, keys_per_client: Vec<Vec<usize>>) {
    let total: usize = keys_per_client.iter().map(Vec::len).sum();
    let cpu0 = cpu_seconds();
    let (_, sys0, faults0) = usage();
    let big0: Vec<(u64, u64)> = (0..18)
        .map(|i| {
            (
                BIG[i].load(Ordering::Relaxed),
                BIG_N[i].load(Ordering::Relaxed),
            )
        })
        .collect();
    let alloc0 = ALLOCATED.load(Ordering::Relaxed);
    let started = Instant::now();
    let handles: Vec<_> = keys_per_client
        .into_iter()
        .map(|keys| {
            let ctx = Arc::clone(ctx);
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(keys.len());
                for i in keys {
                    let sql = lookup_sql(i);
                    let t = Instant::now();
                    let batches = ctx
                        .sql(&sql)
                        .await
                        .expect("plan")
                        .collect()
                        .await
                        .expect("lookup");
                    latencies.push(t.elapsed());
                    let found: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(found, 1, "lookup of key {i} returned {found} rows");
                }
                latencies
            })
        })
        .collect();
    let mut latencies: Vec<Duration> = Vec::with_capacity(total);
    for handle in handles {
        latencies.extend(handle.await.expect("client"));
    }
    let wall = started.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;
    let (_, sys1, faults1) = usage();
    let allocated = ALLOCATED.load(Ordering::Relaxed) - alloc0;
    latencies.sort_unstable();
    let at = |q: f64| {
        let index = ((latencies.len() as f64 * q).ceil() as usize).clamp(1, latencies.len()) - 1;
        latencies[index].as_secs_f64() * 1e3
    };
    println!(
        "{label:<18} n={total:<6} lookups/s={:>8.0} cpu_ms/lk={:>7.3} sys_ms/lk={:>7.3} minflt/lk={:>8.1} alloc_KiB/lk={:>9.1} p50={:>7.2}ms p99={:>7.2}ms p99.9={:>7.2}ms",
        total as f64 / wall,
        cpu * 1e3 / total as f64,
        (sys1 - sys0) * 1e3 / total as f64,
        (faults1 - faults0) as f64 / total as f64,
        allocated as f64 / 1024.0 / total as f64,
        at(0.50),
        at(0.99),
        at(0.999),
    );
    let mut hist = String::new();
    for (i, (b0, n0)) in big0.iter().enumerate() {
        let b = BIG[i].load(Ordering::Relaxed) - b0;
        let n = BIG_N[i].load(Ordering::Relaxed) - n0;
        if n > 0 {
            hist.push_str(&format!(
                " [{}KiB+: {:.2}/lk {:.0}KiB/lk]",
                (1u64 << (i + 14)) >> 10,
                n as f64 / total as f64,
                b as f64 / 1024.0 / total as f64
            ));
        }
    }
    println!("  allocations >=16KiB by size class:{hist}");
}

/// Full-column queries over `content`, each repeated `WCPL_SCAN_REPS` (11) times from one
/// client: every 8,192-row block of `content` is read and decoded.
async fn run_scans(ctx: &Arc<SessionContext>) {
    let reps = env_usize("WCPL_SCAN_REPS", 11);
    for sql in [
        "SELECT sum(length(content)) FROM t",
        "SELECT count(*) FROM t WHERE content LIKE '%SKU-00ab%'",
    ] {
        let (mut cpu, mut wall, mut out) = (Vec::new(), Vec::new(), String::new());
        for _ in 0..reps {
            let (c0, w0) = (cpu_seconds(), Instant::now());
            let batches = ctx
                .sql(sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect("scan");
            cpu.push((cpu_seconds() - c0) * 1e3);
            wall.push(w0.elapsed().as_secs_f64() * 1e3);
            out = arrow::util::pretty::pretty_format_batches(&batches)
                .expect("format")
                .to_string()
                .lines()
                .nth(3)
                .unwrap_or_default()
                .to_string();
        }
        cpu.sort_by(f64::total_cmp);
        wall.sort_by(f64::total_cmp);
        let n = cpu.len();
        println!(
            "scan n={n} cpu_ms med={:.1} min={:.1} max={:.1} wall_ms med={:.1} min={:.1} max={:.1} result={} sql={sql}",
            cpu[n / 2],
            cpu[0],
            cpu[n - 1],
            wall[n / 2],
            wall[0],
            wall[n - 1],
            out.trim()
        );
    }
}

/// Appends `WCPL_APPENDS` (40) batches of 2,048 new rows, checkpoints anything inlined, then
/// compacts: the delta (light-level) and maintenance (full-tier) write paths of a wide table.
async fn run_appends(table: &Arc<CayenneTableProvider>, rows: usize, data: &std::path::Path) {
    let appends = env_usize("WCPL_APPENDS", 40);
    let state = SessionContext::new();
    let (files0, bytes0) = (count_files(data), walk_size(data));
    let (cpu0, wall0) = (cpu_seconds(), Instant::now());
    for a in 0..appends {
        let start = rows + a * 2_048;
        let exec = MemorySourceConfig::try_new_exec(
            &[batches_of(start..start + 2_048, 2_048)],
            schema(),
            None,
        )
        .expect("exec");
        let plan = table
            .insert_into(&state.state(), exec, InsertOp::Append)
            .await
            .expect("append plan");
        datafusion_physical_plan::collect(plan, state.task_ctx())
            .await
            .expect("append");
    }
    let inlined = table.checkpoint_inlined_data().await.expect("checkpoint");
    let (files1, bytes1) = (count_files(data), walk_size(data));
    println!(
        "appends n={appends}x2048 rows cpu_s={:.2} wall_s={:.2} checkpointed_inlined={inlined} files {files0}->{files1} bytes {:.1}->{:.1}MiB",
        cpu_seconds() - cpu0,
        wall0.elapsed().as_secs_f64(),
        bytes0 as f64 / f64::from(1 << 20),
        bytes1 as f64 / f64::from(1 << 20)
    );
    // Writes schedule background compaction passes; drain them, then run explicit passes
    // until one declines, so the CPU below covers every compaction of the appended files.
    let (cpu0, wall0) = (cpu_seconds(), Instant::now());
    let mut passes = 0;
    loop {
        assert!(
            cayenne::drain_compaction_tasks(Duration::from_secs(120)).await,
            "compaction did not drain"
        );
        if passes >= 8
            || !table
                .compact_current_snapshot_small_files()
                .await
                .expect("compact")
        {
            break;
        }
        passes += 1;
    }
    println!(
        "compaction passes={passes} cpu_s={:.2} wall_s={:.2} files {files1}->{} bytes {:.1}MiB",
        cpu_seconds() - cpu0,
        wall0.elapsed().as_secs_f64(),
        count_files(data),
        walk_size(data) as f64 / f64::from(1 << 20)
    );
}

fn count_files(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += count_files(&path);
        } else if path.extension().is_some_and(|e| e == "vortex") {
            total += 1;
        }
    }
    total
}

/// Runs the bench; `allocator` names the global allocator the calling target installed.
pub fn run(allocator: &str) {
    let rows = env_usize("WCPL_ROWS", 200_000);
    let clients = env_usize("WCPL_CLIENTS", 16);
    let threads = env_usize("WCPL_THREADS", 8);
    let lookups = env_usize("WCPL_LOOKUPS", 1_000);
    let cache_mb = env_usize("WCPL_CACHE_MB", 256);
    let hot_keys = env_usize("WCPL_HOT_KEYS", 80_000).min(rows);

    vortex_datafusion::install_process_segment_cache((cache_mb as u64) << 20);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
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
        let vortex_config = VortexConfig {
            deletion_mode: cayenne::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        };
        let context = CayenneContext::new(&vortex_config, Arc::clone(&env), "lookups");
        let table: Arc<CayenneTableProvider> = Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                Arc::clone(&env),
            )
            .with_context(context)
            .with_secondary_indexes(if int_key() {
                Vec::new()
            } else {
                vec![vec!["request_query".to_string(), "request_path".to_string()]]
            })
            .create(CreateTableOptions {
                table_name: "lookups".to_string(),
                schema: schema(),
                primary_key: if int_key() {
                    vec!["id".to_string()]
                } else {
                    vec!["request_query".to_string(), "request_path".to_string()]
                },
                on_conflict: None,
                base_path: data.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            })
            .await
            .expect("create table"),
        );

        let load = SessionContext::new();
        let exec = MemorySourceConfig::try_new_exec(&[batches(0..rows)], schema(), None)
            .expect("exec");
        let (load_cpu0, load_wall0) = (cpu_seconds(), Instant::now());
        let plan = table
            .insert_into(&load.state(), exec, InsertOp::Overwrite)
            .await
            .expect("insert plan");
        datafusion_physical_plan::collect(plan, load.task_ctx())
            .await
            .expect("insert");
        println!(
            "load (overwrite, full tier): cpu_s={:.2} wall_s={:.2}",
            cpu_seconds() - load_cpu0,
            load_wall0.elapsed().as_secs_f64()
        );

        let ctx = Arc::new(SessionContext::new());
        ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");

        // Wait until every file is indexed, so each lookup is narrowed to its row.
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut i = 0;
        while !int_key() {
            ctx.sql(&lookup_sql(i % rows))
                .await
                .expect("plan")
                .collect()
                .await
                .expect("warm lookup");
            i += 7919;
            let verification = table
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify the index");
            if verification.uncovered_files == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "index never covered the table");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let bytes_on_disk: u64 = walk_size(&data);
        println!(
            "allocator={allocator} rows={rows} clients={clients} tokio_workers={threads} lookups/client/phase={lookups} segment_cache_mb={cache_mb} vortex_bytes_on_disk={:.1}MiB",
            bytes_on_disk as f64 / f64::from(1 << 20)
        );
        let explain = ctx
            .sql(&format!("EXPLAIN ANALYZE {}", lookup_sql(rows / 2)))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("explain");
        let text = arrow::util::pretty::pretty_format_batches(&explain)
            .expect("format")
            .to_string();
        for line in text
            .lines()
            .filter(|l| l.contains("CayenneAccelerationExec") || l.contains("DataSourceExec"))
        {
            println!("  {}", line.trim().trim_matches('|').trim());
        }

        let mut rng = Rng(42);
        let in_order: Vec<Vec<usize>> = (0..clients)
            .map(|c| (0..lookups).map(|n| (c + n * clients) % hot_keys).collect())
            .collect();
        let shuffled: Vec<Vec<usize>> = (0..clients)
            .map(|_| (0..lookups).map(|_| (rng.next() as usize) % hot_keys).collect())
            .collect();
        let whole_table: Vec<Vec<usize>> = (0..clients)
            .map(|_| (0..lookups).map(|_| (rng.next() as usize) % rows).collect())
            .collect();
        run_phase("hot_in_order", &ctx, in_order).await;
        run_phase("hot_random", &ctx, shuffled).await;
        run_phase("whole_table_rand", &ctx, whole_table).await;
        if std::env::var("WCPL_EXTRA").is_ok_and(|v| v == "1") {
            run_scans(&ctx).await;
            run_appends(&table, rows, &data).await;
            run_scans(&ctx).await;
        }
        if let Some(min) = std::env::var("WCPL_BT_MIN").ok().and_then(|v| v.parse().ok()) {
            BT_MIN.store(min, Ordering::Relaxed);
            BT_LEFT.store(6, Ordering::Relaxed);
            for i in [11, 22_222, 33_333] {
                ctx.sql(&lookup_sql(i)).await.expect("plan").collect().await.expect("lookup");
            }
            BT_LEFT.store(0, Ordering::Relaxed);
            for (size, bt) in BACKTRACES.lock().expect("bt").iter() {
                println!("=== allocation of {size} bytes");
                for line in bt.lines().filter(|l| (l.contains("vortex") || l.contains("cayenne") || l.contains("datafusion")) && !l.contains("tokio") && !l.contains("{{closure}}")).take(16) {
                    println!("{line}");
                }
            }
        }
    });
}

fn walk_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += walk_size(&path);
        } else if path.extension().is_some_and(|e| e == "vortex") {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}
