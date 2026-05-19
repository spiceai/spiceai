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

//! Regression bench: cross-table CDC throughput ceiling from the
//! single-connection metastore mutex.
//!
//! Older versions of `SqliteMetastore` and `TursoMetastore` held one
//! `tokio::sync::Mutex<Connection>` for the whole catalog:
//!
//! ```ignore
//! pub struct SqliteMetastore {
//!     connection_string: String,
//!     conn: OnceCell<Arc<Mutex<tokio_rusqlite::Connection>>>,
//! }
//! ```
//!
//! Every metastore call from every Cayenne table sharing one catalog
//! acquired this same mutex. The mutex was held across each `.await` of the
//! underlying `tokio_rusqlite` call, so concurrent CDC commits from
//! different tables serialized on it. Under a workload with N
//! independently-replicating tables (CH-benCH SF100 ran 14), the
//! metastore-bound term of every commit became `N · RTT` instead of
//! `RTT` — a 14× ceiling on aggregate throughput at the SF100 shape.
//!
//! The production path now uses `SqliteConnectionPool`
//! ([`crate::metastore::sqlite::SqliteMetastore`], `metastore/sqlite.rs:45-67`):
//! K = `min(available_parallelism, 8)` (minimum 2) independent
//! `tokio_rusqlite::Connection` instances behind a round-robin
//! `try_lock_owned` acquisition pattern. SQLite-WAL allows concurrent
//! readers + one writer, so the K connections do not serialize at the
//! SQLite level — only the in-process Rust mutex did, and that's gone.
//! Turso's MVCC supports `BEGIN CONCURRENT` so it gains even more.
//!
//! ## What this bench measures
//!
//! Pure mutex contention pattern — no real SQLite, no on-disk work.
//! Simulated per-call metastore work is `tokio::time::sleep(rtt)` (one
//! RTT models the full `execute_transaction_batch` round trip).
//!
//! Two lanes per `(N_tables, RTT)` pair:
//!
//! - `single_mutex_baseline/N=...` — all N workers contend on one
//!   `tokio::sync::Mutex<()>`. Total wall time ≈ `N · commits · RTT`
//!   because the mutex serializes every commit. Mirrors the older
//!   single-connection shape.
//! - `per_table_pool/N=...` — current behavior: each worker has its own
//!   `tokio::sync::Mutex<()>` (modeling a per-table connection in a pool
//!   of size K = N). Total wall time ≈ `commits · RTT` because the N
//!   workers run in true parallel.
//!
//! ## How to read
//!
//! `cargo bench --bench metastore_connection_contention -p cayenne`.
//! The throughput report makes the historical ceiling visible:
//!
//! - `single_mutex_baseline/N=14/rtt_10ms` throughput is ~100 commits/s
//!   total regardless of N — what the older single-mutex code capped at.
//! - `per_table_pool/N=14/rtt_10ms` is ~1400 commits/s — one RTT batch
//!   in parallel, the current floor.
//!
//! The bench exercises two RTTs (`rtt_1ms` for local SQLite with WAL+normal-sync,
//! `rtt_10ms` for a network metastore like Turso) so the ceiling is legible in
//! both deployment shapes.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

/// Per-call simulated metastore round trip. After the iteration-3 fix
/// (`execute_transaction_batch`), one commit ≈ one round trip. Two
/// realistic shapes:
/// - 1 ms: local SQLite, WAL mode, NORMAL sync (Cayenne's default
///   tokio-rusqlite config — see `metastore/sqlite.rs:97-108`).
/// - 10 ms: same-zone network metastore (Turso, managed Postgres).
const RTTS: &[(&str, Duration)] = &[
    ("rtt_1ms", Duration::from_millis(1)),
    ("rtt_10ms", Duration::from_millis(10)),
];

/// CDC table counts. 14 matches CH-benCH (12 TPC-C tables + 1 probe +
/// 1 marker, per the May 15 2026 SF100 retest). 4 is a typical small
/// pipeline. 32 stresses the ceiling at higher cardinality.
const TABLE_COUNTS: &[usize] = &[4, 14, 32];

/// Commits per worker per iteration. Picked so the simulated total
/// work lands in the low-millisecond range at `rtt_1ms` and the
/// high-millisecond range at `rtt_10ms` — Criterion can collect 10+
/// samples in 2 s.
const COMMITS_PER_WORKER: usize = 8;

/// One simulated CDC commit: acquire the connection mutex, do the
/// metastore round trip, release. Models the metastore-bound term of
/// `CayenneCatalog::commit_compaction` /
/// `clear_inlined_data_and_deletes` / `commit_inlined_mutation` —
/// after the iteration-3 fix, all of these are single-batch
/// `execute_transaction_batch` calls.
async fn one_commit(mutex: &Mutex<()>, rtt: Duration) {
    let _guard = mutex.lock().await;
    tokio::time::sleep(rtt).await;
}

/// Lane A: all workers contend on one `Mutex<()>` — mirrors today's
/// `SqliteMetastore.conn`.
async fn run_single_mutex(n_tables: usize, rtt: Duration) {
    let mutex = Arc::new(Mutex::new(()));
    let mut handles = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let mutex = Arc::clone(&mutex);
        handles.push(tokio::spawn(async move {
            for _ in 0..COMMITS_PER_WORKER {
                one_commit(&mutex, rtt).await;
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    black_box(mutex);
}

/// Lane B: each worker has its own `Mutex<()>` — models a connection
/// pool sized at N (one connection per table).
async fn run_per_table_pool(n_tables: usize, rtt: Duration) {
    let mutexes: Vec<Arc<Mutex<()>>> = (0..n_tables).map(|_| Arc::new(Mutex::new(()))).collect();
    let mut handles = Vec::with_capacity(n_tables);
    for mutex in &mutexes {
        let mutex = Arc::clone(mutex);
        handles.push(tokio::spawn(async move {
            for _ in 0..COMMITS_PER_WORKER {
                one_commit(&mutex, rtt).await;
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    black_box(mutexes);
}

fn bench_metastore_connection_contention(c: &mut Criterion) {
    // Multi-thread runtime — the contention story requires multiple
    // worker threads. A current-thread runtime would serialize every
    // task and hide the gap.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("metastore_connection_contention");
    for &(rtt_label, rtt) in RTTS {
        for &n in TABLE_COUNTS {
            let commits_total = u64::try_from(n * COMMITS_PER_WORKER).unwrap_or(u64::MAX);
            group.throughput(Throughput::Elements(commits_total));

            let id = format!("N={n}/{rtt_label}");
            group.bench_with_input(
                BenchmarkId::new("single_mutex_baseline", &id),
                &n,
                |b, &n| {
                    b.to_async(&rt).iter(|| async move {
                        run_single_mutex(n, rtt).await;
                    });
                },
            );

            group.bench_with_input(BenchmarkId::new("per_table_pool", &id), &n, |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    run_per_table_pool(n, rtt).await;
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_metastore_connection_contention);
criterion_main!(benches);

#[allow(dead_code)]
fn _runtime_local_for_clippy() -> Runtime {
    Runtime::new().expect("runtime")
}
