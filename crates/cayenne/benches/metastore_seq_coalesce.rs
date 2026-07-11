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

//! E2 microbench: does folding the per-publish snapshot-sequence write INTO the
//! delete-file transaction (instead of a separate autocommit) save meaningful
//! metastore writer-lock cost?
//!
//! The CDC sync-publish path (`mutation_writer::write_new_snapshot`) currently
//! takes the SQLite WAL writer lock TWICE per publish: once for the delete-file
//! transaction (`commit_on_conflict_deletions(..., None)`), then again for a
//! separate autocommit `set_snapshot_sequence` (`INSERT OR REPLACE INTO
//! cayenne_snapshot_sequence`). `commit_on_conflict_deletions` already writes
//! that exact row inside its transaction when passed `Some(sequence)` (the
//! checkpoint path uses it that way), so the second acquisition is avoidable.
//!
//! Lanes (real `SqliteMetastore`, real WAL/`synchronous=NORMAL`/
//! `wal_autocheckpoint=0` pragmas; two FK-free aux tables so we measure the
//! commit machinery, not FK bookkeeping):
//!   * `two_txn`  — delete-file txn (K rows) + separate seq autocommit (today).
//!   * `one_txn`  — delete-file txn (K rows) with the seq row folded in (E2).
//!   * `seq_only` — just the seq autocommit (the pure-insert publish that has NO
//!                  delete txn to fold into — E2 cannot help this case).
//!
//! The `two_txn` − `one_txn` gap is the writer-lock cost E2 removes per
//! deletion-carrying publish.
//!
//! `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
//!   cargo bench --bench metastore_seq_coalesce -p cayenne`.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

use cayenne::metastore::sqlite::SqliteMetastore;
use cayenne::metastore::{ExecuteParams, MetastoreBackend, MetastoreValue};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const AUX_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS bench_delete_file (
    delete_file_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    path TEXT NOT NULL,
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS bench_snapshot_sequence (
    table_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    PRIMARY KEY (table_id, snapshot_id)
);
";

/// Delete-vector files written per publish (a burst supersedes some keys →
/// a few delete-vector files grouped by delete-sequence). Bracket 1 and 4.
const DELETE_FILES: &[usize] = &[1, 4];

const TABLE_ID: &str = "bench_table";

static SEQ: AtomicU64 = AtomicU64::new(1);

fn setup(rt: &Runtime) -> (SqliteMetastore, TempDir) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("meta.db");
    let metastore = SqliteMetastore::new(&format!("sqlite://{}", db_path.display()));
    rt.block_on(async {
        // init_schema installs the real pragmas (WAL, synchronous=NORMAL,
        // wal_autocheckpoint=0, busy_timeout, mmap, cache) — the commit
        // machinery E2 is measured against.
        metastore.init_schema().await.expect("init_schema");
        metastore.execute_batch(AUX_SCHEMA).await.expect("aux schema");
    });
    (metastore, temp_dir)
}

fn insert_delete_file_params(seq: u64, k: usize) -> ExecuteParams<'static> {
    #[expect(clippy::cast_possible_wrap)]
    let seq_i = seq as i64;
    ExecuteParams {
        sql: "INSERT INTO bench_delete_file (delete_file_id, table_id, path, delete_count, file_size_bytes, sequence_number) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params: vec![
            MetastoreValue::Text(format!("df-{seq}-{k}")),
            MetastoreValue::Text(TABLE_ID.to_string()),
            MetastoreValue::Text(format!("s3://bucket/table/snap-{seq}/delete-{k}.vortex")),
            MetastoreValue::Integer(128),
            MetastoreValue::Integer(4096),
            MetastoreValue::Integer(seq_i),
        ],
    }
}

fn insert_seq_params(seq: u64) -> ExecuteParams<'static> {
    #[expect(clippy::cast_possible_wrap)]
    let seq_i = seq as i64;
    ExecuteParams {
        sql: "INSERT OR REPLACE INTO bench_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?1, ?2, ?3)",
        params: vec![
            MetastoreValue::Text(TABLE_ID.to_string()),
            MetastoreValue::Text(format!("snap-{seq}")),
            MetastoreValue::Integer(seq_i),
        ],
    }
}

/// One publish's metastore writes, either as two writer-lock acquisitions
/// (delete-file txn + separate seq autocommit) or one (seq folded into the txn).
async fn publish(ms: &SqliteMetastore, seq: u64, k: usize, coalesced: bool) {
    let tx = ms.begin_transaction().await.expect("begin");
    for i in 0..k {
        tx.execute(insert_delete_file_params(seq, i)).await.expect("df");
    }
    if coalesced {
        tx.execute(insert_seq_params(seq)).await.expect("seq in txn");
        tx.commit().await.expect("commit");
    } else {
        tx.commit().await.expect("commit");
        ms.execute(insert_seq_params(seq)).await.expect("seq autocommit");
    }
}

/// Contended throughput: `writers` concurrent publishers each doing
/// `per_writer` publishes; report wall time + publishes/sec for two_txn vs
/// one_txn. This is where halving writer-lock acquisitions should pay off
/// (WAL single-writer serialization) — the uncontended criterion lanes cannot
/// show it. Uses `std::time::Instant` via tokio (bench binary, not a workflow
/// script, so `Instant::now` is available here).
fn concurrent_compare(rt: &Runtime) {
    use std::sync::Arc;
    use std::time::Instant;
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 400;
    let total = WRITERS * PER_WRITER;
    println!("\n=== metastore_seq_coalesce CONTENDED ({WRITERS} writers x {PER_WRITER} publishes, K=4 delete files) ===");
    for (label, coalesced) in [("two_txn", false), ("one_txn", true)] {
        let (ms, _t) = setup(rt);
        let ms = Arc::new(ms);
        let elapsed = rt.block_on(async {
            let start = Instant::now();
            let mut handles = Vec::with_capacity(WRITERS as usize);
            for w in 0..WRITERS {
                let ms = Arc::clone(&ms);
                handles.push(tokio::spawn(async move {
                    let base = SEQ.fetch_add(PER_WRITER, Ordering::Relaxed) + w * 1_000_000;
                    for j in 0..PER_WRITER {
                        publish(&ms, base + j, 4, coalesced).await;
                    }
                }));
            }
            for h in handles {
                h.await.expect("join");
            }
            start.elapsed()
        });
        #[expect(clippy::cast_precision_loss)]
        let pps = total as f64 / elapsed.as_secs_f64();
        println!("  {label:<8} wall={elapsed:?}  {pps:.0} publishes/sec  ({:.1} us/publish)", elapsed.as_micros() as f64 / total as f64);
    }
}

fn bench_seq_coalesce(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    concurrent_compare(&rt);
    let mut group = c.benchmark_group("metastore_seq_coalesce");

    for &k in DELETE_FILES {
        // two_txn: delete-file txn + separate seq autocommit (today's sync path).
        let (ms, _t) = setup(&rt);
        group.bench_with_input(BenchmarkId::new("two_txn", k), &k, |b, &k| {
            b.iter(|| {
                rt.block_on(async {
                    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
                    let tx = ms.begin_transaction().await.expect("begin");
                    for i in 0..k {
                        tx.execute(insert_delete_file_params(seq, i)).await.expect("df");
                    }
                    tx.commit().await.expect("commit");
                    ms.execute(insert_seq_params(seq)).await.expect("seq autocommit");
                    black_box(seq);
                });
            });
        });

        // one_txn: seq row folded into the delete-file txn (E2).
        let (ms, _t) = setup(&rt);
        group.bench_with_input(BenchmarkId::new("one_txn", k), &k, |b, &k| {
            b.iter(|| {
                rt.block_on(async {
                    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
                    let tx = ms.begin_transaction().await.expect("begin");
                    for i in 0..k {
                        tx.execute(insert_delete_file_params(seq, i)).await.expect("df");
                    }
                    tx.execute(insert_seq_params(seq)).await.expect("seq in txn");
                    tx.commit().await.expect("commit");
                    black_box(seq);
                });
            });
        });
    }

    // seq_only: the pure-insert publish (no delete txn) — E2 cannot help it;
    // shown for context (its cost is the floor a single publish pays).
    let (ms, _t) = setup(&rt);
    group.bench_function("seq_only", |b| {
        b.iter(|| {
            rt.block_on(async {
                let seq = SEQ.fetch_add(1, Ordering::Relaxed);
                ms.execute(insert_seq_params(seq)).await.expect("seq autocommit");
                black_box(seq);
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_seq_coalesce);
criterion_main!(benches);
