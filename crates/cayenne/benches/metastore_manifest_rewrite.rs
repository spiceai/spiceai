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
//! rows, timed one call at a time through the real [`CayenneCatalog`] on an
//! on-disk `SQLite` metastore, at 1, 2, 4, … 128 concurrent client threads.
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
//!   existing row.
//! - `upsert_table_statistics behind a rewrite` — the per-commit upsert on
//!   every client's table while one more table rewrites a 1,000-file manifest
//!   in a loop: what a commit on one table waits for while another table
//!   compacts. Only the upserts are reported.
//!
//! Criterion reports central estimates; the write-lock question is about the
//! tail, so this harness records every call and prints p50/p99/p99.9/max per
//! lane and thread count. A background task runs a PASSIVE WAL checkpoint every
//! second while clients run, standing in for the per-table maintenance ticks
//! that drain the WAL in production (`wal_autocheckpoint = 0`).
//!
//! Run: `cargo bench -p cayenne --bench metastore_manifest_rewrite [-- <lane filter>]`.
//! `METASTORE_BENCH_THREADS=1,8,64` restricts the thread counts;
//! `METASTORE_BENCH_RAW_DIR=<dir>` writes every sample, one latency in µs per
//! line, to `<dir>/<lane>_t<threads>.csv`.

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::{
    CreateTableOptions, SnapshotFile, SnapshotFileStatistics, TableStatistics, VortexConfig,
};
use cayenne::{CayenneCatalog, MetadataCatalog};

const THREADS: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128];
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
    /// Total calls to aim for across all clients at one thread count.
    target_calls: usize,
    /// Floor on calls per client, so a high thread count still gives each
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
        target_calls: 400,
        min_calls_per_client: 8,
    },
];

struct Table {
    table_id: String,
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
            table_id,
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

/// One timed call of `lane` by the client that owns `table`.
async fn call(catalog: &Arc<dyn MetadataCatalog>, lane: Lane, table: &Table, i: usize) {
    match lane {
        Lane::Replace { .. } => catalog
            .replace_snapshot_files(&table.table_id, &table.snapshot_id, &table.files)
            .await
            .expect("replace manifest"),
        Lane::TableStats | Lane::TableStatsBehindRewrite => catalog
            .upsert_table_statistics(&table_stats(&table.table_id, i as i64 + 1))
            .await
            .expect("upsert table stats"),
        Lane::FileStats => catalog
            .upsert_snapshot_file_statistics(&table.file_stats[i % table.file_stats.len()])
            .await
            .expect("upsert file stats"),
    }
}

struct LaneRun {
    samples: Vec<Duration>,
    wall: Duration,
}

/// Run `spec` with `threads` clients on a runtime of `threads` worker threads.
fn run_lane(spec: &LaneSpec, threads: usize) -> LaneRun {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let lane = spec.lane;
    let calls_per_client = spec
        .target_calls
        .div_ceil(threads)
        .max(spec.min_calls_per_client);
    runtime.block_on(async move {
        let (files, file_stats, rewriters) = match lane {
            Lane::Replace { files } => (files, 0, 0),
            Lane::TableStats => (0, 0, 0),
            Lane::FileStats => (0, FILE_STATS_PER_TABLE, 0),
            Lane::TableStatsBehindRewrite => (REWRITE_BEHIND_FILES, 0, 1),
        };
        let fixture = fixture(threads + rewriters, files, file_stats).await;
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
                while !stop.load(Ordering::Relaxed) {
                    call(
                        &catalog,
                        Lane::Replace {
                            files: REWRITE_BEHIND_FILES,
                        },
                        &table,
                        0,
                    )
                    .await;
                }
            })
        });

        let barrier = Arc::new(tokio::sync::Barrier::new(client_tables.len()));
        let started = Instant::now();
        let clients: Vec<_> = client_tables
            .into_iter()
            .map(|table| {
                let catalog = Arc::clone(&catalog);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    let mut samples = Vec::with_capacity(calls_per_client);
                    for i in 0..calls_per_client {
                        let t0 = Instant::now();
                        call(&catalog, lane, &table, i).await;
                        samples.push(t0.elapsed());
                    }
                    samples
                })
            })
            .collect();
        let mut samples = Vec::with_capacity(threads * calls_per_client);
        for client in clients {
            samples.extend(client.await.expect("client task"));
        }
        let wall = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        if let Some(rewriter) = rewriter {
            rewriter.await.expect("rewriter task");
        }
        checkpointer.await.expect("checkpointer task");
        LaneRun { samples, wall }
    })
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[idx]
}

fn report(name: &str, threads: usize, mut run: LaneRun, raw_dir: Option<&std::path::Path>) {
    run.samples.sort();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    if let Some(dir) = raw_dir {
        let slug: String = name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let body: String = run
            .samples
            .iter()
            .map(|d| format!("{:.1}\n", us(*d)))
            .collect();
        std::fs::write(dir.join(format!("{slug}_t{threads}.csv")), body).expect("write samples");
    }
    let n = run.samples.len();
    println!(
        "RESULT lane=\"{name}\" threads={threads} calls={n} p50_us={:.1} p99_us={:.1} p999_us={:.1} max_us={:.1} calls_per_s={:.0}",
        us(pct(&run.samples, 0.50)),
        us(pct(&run.samples, 0.99)),
        us(pct(&run.samples, 0.999)),
        us(*run.samples.last().expect("samples")),
        n as f64 / run.wall.as_secs_f64(),
    );
}

fn main() {
    // `cargo bench` passes `--bench`; a filter argument selects lanes by substring.
    let filter = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .unwrap_or_default();
    let threads: Vec<usize> = std::env::var("METASTORE_BENCH_THREADS").map_or_else(
        |_| THREADS.to_vec(),
        |list| {
            list.split(',')
                .map(|t| t.trim().parse().expect("thread count"))
                .collect()
        },
    );
    let raw_dir = std::env::var_os("METASTORE_BENCH_RAW_DIR").map(std::path::PathBuf::from);
    if let Some(dir) = &raw_dir {
        std::fs::create_dir_all(dir).expect("create raw sample directory");
    }
    for spec in LANES.iter().filter(|spec| spec.name.contains(&filter)) {
        for &t in &threads {
            report(spec.name, t, run_lane(spec, t), raw_dir.as_deref());
        }
    }
}
