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

//! Latency percentiles for the catalog writes that rewrite metastore child
//! rows, through the real [`CayenneCatalog`] on an on-disk `SQLite` metastore,
//! at 1, 2, 4, … 128 concurrent clients. Each client is a task that times every
//! call it makes, one after another; the clients run concurrently on a runtime
//! with as many worker threads as spiced's main runtime has on this machine
//! (`CpuBudget::main_runtime_worker_threads`), so the client count is the only
//! thing that changes.
//!
//! Every client works on its own table, and all tables share one metastore —
//! as every Cayenne table under one metadata directory does — so the clients
//! contend for its single write lock:
//!
//! - `replace_snapshot_files n=<n>` — one full-manifest rewrite of an
//!   `n`-file snapshot: DELETE the snapshot's rows, INSERT every file, inside
//!   one transaction. Compaction, overwrite and the protected-snapshot bake
//!   run it, and it holds the write lock for its whole duration.
//! - `upsert_table_statistics` — the per-commit table-stats upsert over an
//!   existing row.
//! - `upsert_snapshot_file_statistics` — one per-file stats upsert over an
//!   existing row, with a changed statistics blob every call so each upsert
//!   rewrites the row (`SQLite` skips the page write for identical bytes).
//! - `upsert_table_statistics behind a rewrite` — the per-commit upsert on
//!   every client's table while one more table rewrites a 1,000-file manifest
//!   in a loop: what a commit on one table waits for while another table
//!   compacts. Only the upserts are reported.
//!
//! Criterion reports central estimates; the write-lock question is about the
//! tail, so this harness records every call and prints p50/p99/p99.9/max per
//! lane and client count. A call that fails — a writer that waits out the
//! metastore's busy timeout gets `database is locked` — is counted in `errors`
//! with its time to failure rather than aborting the run, so the percentiles
//! cover successful calls and `errors` says how many never succeeded. A
//! background task runs the metastore's WAL checkpoint every second while
//! clients run (PASSIVE, escalating to TRUNCATE past the metastore's WAL-size
//! threshold), standing in for the per-table maintenance ticks that drain the
//! WAL in production (`wal_autocheckpoint = 0`).
//!
//! Run: `cargo bench -p cayenne --bench metastore_manifest_rewrite [-- <lane filter>]`.
//! `METASTORE_BENCH_CLIENTS=1,8,64` restricts the client counts;
//! `METASTORE_BENCH_RAW_DIR=<dir>` writes every sample, one latency in µs per
//! line, to `<dir>/<lane>_c<clients>.csv`.

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::{
    CreateTableOptions, SnapshotFile, SnapshotFileStatistics, TableStatistics, VortexConfig,
};
use cayenne::{CayenneCatalog, MetadataCatalog};

const CLIENTS: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128];
const FILE_STATS_PER_TABLE: usize = 100;
const REWRITE_BEHIND_FILES: usize = 1_000;

#[derive(Clone, Copy)]
enum Lane {
    Replace { files: usize },
    TableStats,
    FileStats,
    TableStatsBehindRewrite,
}

struct LaneSpec {
    name: &'static str,
    lane: Lane,
    /// Total calls to aim for across all clients at one client count.
    target_calls: usize,
    /// Floor on calls per client, so a high client count still gives each
    /// client several samples.
    min_calls_per_client: usize,
}

const LANES: &[LaneSpec] = &[
    LaneSpec {
        name: "replace_snapshot_files n=100",
        lane: Lane::Replace { files: 100 },
        target_calls: 400,
        min_calls_per_client: 4,
    },
    LaneSpec {
        name: "replace_snapshot_files n=1000",
        lane: Lane::Replace { files: 1_000 },
        target_calls: 200,
        min_calls_per_client: 4,
    },
    LaneSpec {
        name: "upsert_table_statistics",
        lane: Lane::TableStats,
        target_calls: 4_000,
        min_calls_per_client: 20,
    },
    LaneSpec {
        name: "upsert_snapshot_file_statistics",
        lane: Lane::FileStats,
        target_calls: 4_000,
        min_calls_per_client: 20,
    },
    LaneSpec {
        name: "upsert_table_statistics behind a rewrite",
        lane: Lane::TableStatsBehindRewrite,
        target_calls: 200,
        min_calls_per_client: 4,
    },
];

struct Table {
    id: String,
    snapshot_id: String,
    files: Arc<Vec<SnapshotFile>>,
    file_stats: Arc<Vec<SnapshotFileStatistics>>,
}

struct Fixture {
    catalog: Arc<dyn MetadataCatalog>,
    tables: Vec<Table>,
    _dir: tempfile::TempDir,
}

/// One catalog holding `tables` tables, each seeded (untimed) with a
/// `files`-file manifest and its file-stats rows.
async fn fixture(tables: usize, files: usize, file_stats: usize) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.display())).expect("create catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut out = Vec::with_capacity(tables);
    for t in 0..tables {
        let table_name = format!("t{t:03}");
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: table_name.clone(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: dir.path().join("data").to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("create table");
        let snapshot_id = catalog
            .get_table(&table_name)
            .await
            .expect("get table")
            .current_snapshot_id;
        let manifest = manifest(&table_id, &snapshot_id, files);
        if !manifest.is_empty() {
            catalog
                .replace_snapshot_files(&table_id, &snapshot_id, &manifest)
                .await
                .expect("seed manifest");
        }
        let stats: Vec<SnapshotFileStatistics> = (0..file_stats)
            .map(|i| SnapshotFileStatistics {
                table_id: table_id.clone(),
                snapshot_id: snapshot_id.clone(),
                file_path: format!("file-{i:05}.vortex"),
                file_size_bytes: 1 << 20,
                num_rows: 8_192,
                statistics_blob: blob(1_536, i as u8),
            })
            .collect();
        for s in &stats {
            catalog
                .upsert_snapshot_file_statistics(s)
                .await
                .expect("seed file stats");
        }
        catalog
            .upsert_table_statistics(&table_stats(&table_id, 0))
            .await
            .expect("seed table stats");
        out.push(Table {
            id: table_id,
            snapshot_id,
            files: Arc::new(manifest),
            file_stats: Arc::new(stats),
        });
    }
    Fixture {
        catalog,
        tables: out,
        _dir: dir,
    }
}

fn manifest(table_id: &str, snapshot_id: &str, n: usize) -> Vec<SnapshotFile> {
    (0..n)
        .map(|i| SnapshotFile {
            table_id: table_id.to_string(),
            snapshot_id: snapshot_id.to_string(),
            file_path: format!("{}.vortex", uuid::Uuid::now_v7()),
            row_count: 8_192,
            file_size_bytes: 1 << 20,
            min_sequence: 0,
            max_sequence: i as i64,
            digest: Some(format!("xxh3-128:{i:032x}")),
        })
        .collect()
}

fn blob(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn table_stats(table_id: &str, num_rows: i64) -> TableStatistics {
    TableStatistics {
        table_id: table_id.to_string(),
        statistics_blob: blob(4_096, num_rows as u8),
        num_rows,
        num_rows_exact: true,
        ndv_sketches: Some(blob(16_384, num_rows as u8)),
    }
}

/// One call's payload, built before its timer starts.
enum Op {
    Replace,
    TableStats(TableStatistics),
    FileStats(SnapshotFileStatistics),
}

fn prepare(lane: Lane, table: &Table, i: usize) -> Op {
    match lane {
        Lane::Replace { .. } => Op::Replace,
        Lane::TableStats | Lane::TableStatsBehindRewrite => {
            Op::TableStats(table_stats(&table.id, i as i64 + 1))
        }
        Lane::FileStats => {
            // A changed blob every call, so each upsert rewrites the row.
            let mut stats = table.file_stats[i % table.file_stats.len()].clone();
            stats.statistics_blob = blob(stats.statistics_blob.len(), (i + 1) as u8 ^ 0x5a);
            Op::FileStats(stats)
        }
    }
}

async fn run(catalog: &Arc<dyn MetadataCatalog>, table: &Table, op: &Op) -> Result<(), String> {
    let result = match op {
        Op::Replace => {
            catalog
                .replace_snapshot_files(&table.id, &table.snapshot_id, &table.files)
                .await
        }
        Op::TableStats(stats) => catalog.upsert_table_statistics(stats).await,
        Op::FileStats(stats) => catalog.upsert_snapshot_file_statistics(stats).await,
    };
    result.map_err(|e| e.to_string())
}

struct LaneRun {
    samples: Vec<Duration>,
    /// Time to failure and message of every call that failed.
    errors: Vec<(Duration, String)>,
    wall: Duration,
}

/// Run `spec` with `clients` concurrent clients on a runtime sized like spiced's.
fn run_lane(spec: &LaneSpec, clients: usize) -> LaneRun {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpu_budget::cpu_budget().main_runtime_worker_threads())
        .enable_all()
        .build()
        .expect("tokio runtime");
    let lane = spec.lane;
    let calls_per_client = spec
        .target_calls
        .div_ceil(clients)
        .max(spec.min_calls_per_client);
    runtime.block_on(async move {
        let (files, file_stats, rewriters) = match lane {
            Lane::Replace { files } => (files, 0, 0),
            Lane::TableStats => (0, 0, 0),
            Lane::FileStats => (0, FILE_STATS_PER_TABLE, 0),
            Lane::TableStatsBehindRewrite => (REWRITE_BEHIND_FILES, 0, 1),
        };
        let fixture = fixture(clients + rewriters, files, file_stats).await;
        fixture.catalog.checkpoint_wal().await.expect("checkpoint");
        let catalog = Arc::clone(&fixture.catalog);
        let mut tables = fixture.tables.into_iter();
        let rewrite_table = (rewriters > 0).then(|| tables.next().expect("rewrite table"));
        let client_tables: Vec<Table> = tables.collect();

        let stop = Arc::new(AtomicBool::new(false));
        let checkpointer = {
            let catalog = Arc::clone(&catalog);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    catalog
                        .checkpoint_wal()
                        .await
                        .expect("background checkpoint");
                }
            })
        };
        let rewriter = rewrite_table.map(|table| {
            let catalog = Arc::clone(&catalog);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                // The rewriter's own outcome is not what this lane measures; a
                // rewrite that fails is simply the next one's turn.
                while !stop.load(Ordering::Relaxed) {
                    let _ = run(&catalog, &table, &Op::Replace).await;
                }
            })
        });

        let barrier = Arc::new(tokio::sync::Barrier::new(client_tables.len()));
        let started = Instant::now();
        let tasks: Vec<_> = client_tables
            .into_iter()
            .map(|table| {
                let catalog = Arc::clone(&catalog);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    let mut samples = Vec::with_capacity(calls_per_client);
                    let mut errors = Vec::new();
                    for i in 0..calls_per_client {
                        let op = prepare(lane, &table, i);
                        let t0 = Instant::now();
                        match run(&catalog, &table, &op).await {
                            Ok(()) => samples.push(t0.elapsed()),
                            Err(e) => errors.push((t0.elapsed(), e)),
                        }
                    }
                    (samples, errors)
                })
            })
            .collect();
        let mut samples = Vec::with_capacity(clients * calls_per_client);
        let mut errors = Vec::new();
        for task in tasks {
            let (task_samples, task_errors) = task.await.expect("client task");
            samples.extend(task_samples);
            errors.extend(task_errors);
        }
        let wall = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        if let Some(rewriter) = rewriter {
            rewriter.await.expect("rewriter task");
        }
        checkpointer.await.expect("checkpointer task");
        LaneRun {
            samples,
            errors,
            wall,
        }
    })
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[idx]
}

fn report(name: &str, clients: usize, mut run: LaneRun, raw_dir: Option<&std::path::Path>) {
    run.samples.sort();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    if let Some(dir) = raw_dir {
        let slug: String = name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let mut body = String::with_capacity(run.samples.len() * 10);
        for d in &run.samples {
            let _ = writeln!(body, "{:.1}", us(*d));
        }
        std::fs::write(dir.join(format!("{slug}_c{clients}.csv")), body).expect("write samples");
    }
    let n = run.samples.len();
    let errors = run.errors.len();
    let error_wait = run.errors.iter().map(|(d, _)| *d).max().unwrap_or_default();
    let first_error = run.errors.first().map_or("", |(_, e)| e.as_str());
    let (p50, p99, p999, max) = if run.samples.is_empty() {
        (0.0, 0.0, 0.0, 0.0)
    } else {
        (
            us(pct(&run.samples, 0.50)),
            us(pct(&run.samples, 0.99)),
            us(pct(&run.samples, 0.999)),
            us(*run.samples.last().expect("samples")),
        )
    };
    println!(
        "RESULT lane=\"{name}\" clients={clients} calls={n} errors={errors} p50_us={p50:.1} p99_us={p99:.1} p999_us={p999:.1} max_us={max:.1} calls_per_s={:.0} max_error_wait_us={:.1} first_error=\"{first_error}\"",
        n as f64 / run.wall.as_secs_f64(),
        us(error_wait),
    );
}

fn main() {
    // `cargo bench` passes `--bench`; a filter argument selects lanes by substring.
    let filter = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .unwrap_or_default();
    let clients: Vec<usize> = std::env::var("METASTORE_BENCH_CLIENTS").map_or_else(
        |_| CLIENTS.to_vec(),
        |list| {
            list.split(',')
                .map(|c| c.trim().parse().expect("client count"))
                .collect()
        },
    );
    let raw_dir = std::env::var_os("METASTORE_BENCH_RAW_DIR").map(std::path::PathBuf::from);
    if let Some(dir) = &raw_dir {
        std::fs::create_dir_all(dir).expect("create raw sample directory");
    }
    for spec in LANES.iter().filter(|spec| spec.name.contains(&filter)) {
        for &c in &clients {
            report(spec.name, c, run_lane(spec, c), raw_dir.as_deref());
        }
    }
}
