// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Cycle-11 decomposition bench: where do the hundreds of milliseconds go in
//! the folded Stage-A metastore transaction
//! ([`cayenne_catalog::CayenneCatalog::commit_on_conflict_deletions_with_tombstone`])
//! on the heavy upsert tables (stock / order_line) at SF-100 @ 10K txn/s?
//!
//! The production function holds the per-table SQLite writer for the whole
//! `BEGIN IMMEDIATE … COMMIT` and was measured at 700-940 ms per batch. Raw WAL
//! frames for a few MB should take single-digit ms (NORMAL+WAL COMMIT does not
//! fsync; `wal_autocheckpoint = 0` so no checkpoint runs inside COMMIT). This
//! bench replicates the EXACT statement set and order against a real
//! [`SqliteMetastore`] (same pragmas the runtime applies via
//! `set_sqlite_metastore_config`) and decomposes the cost.
//!
//! ## Statement set replicated (verbatim order from the production fn)
//!
//! 1. `begin_transaction()` — `BEGIN IMMEDIATE` (one `conn.call` round-trip).
//! 2. delete-file rows, chunked by `MAX_PARAMS/9` (multi-VALUES `INSERT … ON
//!    CONFLICT … DO UPDATE`).
//! 3. insert-record rows, chunked by `MAX_PARAMS/4 = 8000` (multi-VALUES
//!    `INSERT OR REPLACE INTO cayenne_insert_record`). One UUID minted per row.
//! 4. snapshot-sequence (single `INSERT OR REPLACE`).
//! 5. inline tombstone (single `INSERT INTO cayenne_inlined_delete`, ~1-3 MB
//!    `delete_ipc` BLOB).
//! 6. deferred `published = 1` flips, chunked `IN (…)` (small set).
//! 7. `commit()` — `COMMIT` (one `conn.call` round-trip).
//!
//! Each `tx.execute()` is ONE `tokio_rusqlite` `conn.call` = MPSC send → bg
//! thread wake → prepare_cached + step → oneshot back → await.
//!
//! ## Lanes
//!
//! - `folded_per_statement/keys=N` — the production path: per-statement
//!   `tx.execute()` round-trips. THIS is the measured baseline.
//! - `one_closure_batch/keys=N` — the fix candidate: identical statements
//!   executed in ONE `conn.call` via `execute_transaction_batch` (blobs inlined
//!   as `X'…'` hex literals). Isolates the per-statement channel round-trip
//!   cost from the SQLite engine cost.
//! - `decompose/*` — each segment timed alone at the worst-case key count:
//!   begin, delete-files, insert-records, snapshot, tombstone, commit.
//! - `insert_records_only/keys=N` — just the insert-record chunks inside a txn
//!   (the suspected volume driver), to see the scaling shape.
//! - `prepare_cache/{aligned,tail}` — fixed 8000-row chunks (prepare_cached
//!   HIT) vs an odd tail-chunk size (prepare_cached MISS → re-parse of a giant
//!   multi-VALUES statement).
//! - `index_growth/{empty,preloaded}` — INSERT OR REPLACE into an empty
//!   `cayenne_insert_record` vs one pre-loaded with 200K rows (the
//!   between-checkpoint accumulation: the `UNIQUE(table_id, pk_bytes)` index is
//!   only cleared by `clear_insert_records` at checkpoint).
//!
//! `cargo bench --bench folded_txn_decomposition -p cayenne`.

#![expect(clippy::expect_used)]
#![expect(clippy::too_many_lines)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Duration;

use cayenne::metastore::sqlite::{SqliteMetastore, SqliteMetastoreConfig, set_sqlite_metastore_config};
use cayenne::metastore::{ExecuteParams, MetastoreBackend, MetastoreValue};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Heavy-table per-batch composite-key counts at SF-100 @ 10K txn/s. The task
/// states stock / order_line bursts carry ~20K-55K PK keys; the half/double
/// points bracket the scaling shape (linear ⇒ engine-bound; flat ⇒
/// round-trip-bound).
const KEY_COUNTS: &[usize] = &[10_000, 20_000, 40_000, 55_000];

/// Worst-case burst used by the single-segment decomposition lanes.
const DECOMPOSE_KEYS: usize = 55_000;

/// Delete files per staged on-conflict commit. Heavy-table PK upserts produce a
/// small number of deletion-vector files (position + key DVs); 2 is
/// representative. The delete-file count is NOT the volume driver — it is
/// included so the replicated txn is faithful.
const DELETE_FILES: usize = 2;

/// Inline tombstone `delete_ipc` payload size — the task's "packed-i64/LZ4
/// ~1-3 MB" compacted tombstone. 2 MiB sits in the middle of that range.
const TOMBSTONE_IPC_BYTES: usize = 2 * 1024 * 1024;

/// Composite-PK row-key width. `stock` PK is `(s_w_id i32, s_i_id i32)`;
/// `order_line` is wider. The encoded row-key for a composite PK is the
/// concatenated big-endian column bytes — 16 bytes is a faithful stock-scale
/// width (an Int64-only PK would be 8). Drives both the `pk_bytes` BLOB volume
/// and the unique-index key size.
const PK_KEY_BYTES: usize = 16;

/// `cayenne_catalog` chunking constants (mirrored verbatim).
const MAX_PARAMS: usize = 32_000;
const INSERT_RECORD_PARAMS_PER_ROW: usize = 4;
const MAX_INSERT_RECORD_ROWS_PER_CHUNK: usize = MAX_PARAMS / INSERT_RECORD_PARAMS_PER_ROW; // 8000
const DELETE_FILE_PARAMS_PER_ROW: usize = 9;
const MAX_DELETE_FILE_ROWS_PER_CHUNK: usize = MAX_PARAMS / DELETE_FILE_PARAMS_PER_ROW;

const TABLE_ID: &str = "bench-table-id";

/// Apply the SAME pragmas the bench pod / runtime uses
/// (`cayenne_metastore_cache_mb = 1024`, `cayenne_metastore_mmap_mb = 4096`),
/// so cache/mmap behaviour matches production rather than the in-code default
/// (`cache_size_mb = 256`). Process-wide; set once before opening connections.
fn apply_runtime_pragmas() {
    set_sqlite_metastore_config(SqliteMetastoreConfig {
        cache_size_mb: 1024,
        mmap_size_bytes: 4096 * 1024 * 1024,
        ..SqliteMetastoreConfig::default()
    });
}

async fn fresh_metastore() -> (SqliteMetastore, TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path().join("catalog.db");
    let connection_string = format!("sqlite://{}", db_path.display());
    let metastore = SqliteMetastore::new(&connection_string);
    metastore.init_schema().await.expect("init_schema");
    // Seed the parent row so the FK (table_id → cayenne_table) is satisfiable.
    metastore
        .execute(ExecuteParams {
            sql: "INSERT OR IGNORE INTO cayenne_table \
                  (table_id, table_name, path, path_is_relative, schema_json) \
                  VALUES (?1, ?2, ?3, 0, '{}')",
            params: vec![
                MetastoreValue::Text(TABLE_ID.to_string()),
                MetastoreValue::Text("bench_table".to_string()),
                MetastoreValue::Text("/tmp/bench".to_string()),
            ],
        })
        .await
        .expect("seed cayenne_table");
    (metastore, temp_dir)
}

/// Distinct composite-PK row-key bytes (`PK_KEY_BYTES` wide). The first 8 bytes
/// encode `i` big-endian; the rest are a fixed warehouse-id-like suffix so the
/// keys are realistic-width but deterministic.
fn make_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut key = Vec::with_capacity(PK_KEY_BYTES);
            key.extend_from_slice(&(i as u64).to_be_bytes());
            while key.len() < PK_KEY_BYTES {
                key.push(0xAB);
            }
            key
        })
        .collect()
}

/// A 2 MiB pseudo-payload standing in for the packed-i64/LZ4 tombstone IPC blob.
fn make_tombstone_ipc() -> Vec<u8> {
    (0..TOMBSTONE_IPC_BYTES).map(|i| (i % 251) as u8).collect()
}

// --- SQL builders: byte-for-byte equivalents of the cayenne_catalog helpers. ---

fn build_insert_records_chunk_sql(
    pk_bytes_list: &[Vec<u8>],
    sequence_number: i64,
) -> (String, Vec<MetastoreValue>) {
    const PREFIX: &str = "INSERT OR REPLACE INTO cayenne_insert_record \
         (insert_record_id, table_id, pk_bytes, sequence_number) VALUES ";
    let mut sql = String::with_capacity(PREFIX.len() + pk_bytes_list.len() * 32);
    sql.push_str(PREFIX);
    let mut params = Vec::with_capacity(pk_bytes_list.len() * 4);
    for (i, pk_bytes) in pk_bytes_list.iter().enumerate() {
        let base = i * 4 + 1;
        if i > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "(?{}, ?{}, ?{}, ?{})", base, base + 1, base + 2, base + 3);
        params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
        params.push(MetastoreValue::Text(TABLE_ID.to_string()));
        params.push(MetastoreValue::Blob(pk_bytes.clone()));
        params.push(MetastoreValue::Integer(sequence_number));
    }
    (sql, params)
}

fn build_insert_delete_files_chunk_sql(count: usize, seq: i64) -> (String, Vec<MetastoreValue>) {
    const PARAMS_PER_ROW: usize = 9;
    const PREFIX: &str = "INSERT INTO cayenne_delete_file (\
             delete_file_id, table_id, path, path_is_relative, \
             format, delete_count, file_size_bytes, source_data_file_path, sequence_number\
         ) VALUES ";
    const SUFFIX: &str = " ON CONFLICT(table_id, path) DO UPDATE SET path = cayenne_delete_file.path";
    let mut sql = String::with_capacity(PREFIX.len() + SUFFIX.len() + count * 64);
    sql.push_str(PREFIX);
    let mut params = Vec::with_capacity(count * PARAMS_PER_ROW);
    for i in 0..count {
        let base = i * PARAMS_PER_ROW + 1;
        if i > 0 {
            sql.push_str(", ");
        }
        let _ = write!(
            sql,
            "(?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
            base,
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
            base + 6,
            base + 7,
            base + 8,
        );
        params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
        params.push(MetastoreValue::Text(TABLE_ID.to_string()));
        params.push(MetastoreValue::Text(format!("deletes/dv-{seq}-{i}.vortex")));
        params.push(MetastoreValue::Bool(true));
        params.push(MetastoreValue::Text("vortex".to_string()));
        params.push(MetastoreValue::Integer(1000));
        params.push(MetastoreValue::Integer(4096));
        params.push(MetastoreValue::Null);
        params.push(MetastoreValue::Integer(seq));
    }
    sql.push_str(SUFFIX);
    (sql, params)
}

/// LANE A — production path. Replicates `commit_on_conflict_deletions_with_tombstone`
/// statement-for-statement using per-statement `tx.execute()` round-trips
/// (single attempt, no conflict so no retry branch is exercised).
async fn run_folded_per_statement(
    metastore: &SqliteMetastore,
    keys: &[Vec<u8>],
    tombstone_ipc: &[u8],
    delete_files: usize,
    insert_seq: i64,
    snapshot_seq: i64,
) {
    let tx = metastore.begin_transaction().await.expect("begin");

    // delete-file chunks
    for chunk_start in (0..delete_files).step_by(MAX_DELETE_FILE_ROWS_PER_CHUNK) {
        let n = (delete_files - chunk_start).min(MAX_DELETE_FILE_ROWS_PER_CHUNK);
        let (sql, params) = build_insert_delete_files_chunk_sql(n, insert_seq - 1);
        tx.execute(ExecuteParams { sql: &sql, params })
            .await
            .expect("delete files");
    }

    // insert-record chunks (8000 rows each)
    for chunk in keys.chunks(MAX_INSERT_RECORD_ROWS_PER_CHUNK) {
        let (sql, params) = build_insert_records_chunk_sql(chunk, insert_seq);
        tx.execute(ExecuteParams { sql: &sql, params })
            .await
            .expect("insert records");
    }

    // snapshot sequence
    tx.execute(ExecuteParams {
        sql: "INSERT OR REPLACE INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?1, ?2, ?3)",
        params: vec![
            MetastoreValue::Text(TABLE_ID.to_string()),
            MetastoreValue::Text(format!("snap-{snapshot_seq}")),
            MetastoreValue::Integer(snapshot_seq),
        ],
    })
    .await
    .expect("snapshot seq");

    // inline tombstone (LARGE blob)
    tx.execute(ExecuteParams {
        sql: "INSERT INTO cayenne_inlined_delete \
              (inlined_id, table_id, delete_ipc, delete_count, sequence_number, published) \
              VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params: vec![
            MetastoreValue::Text(uuid::Uuid::now_v7().to_string()),
            MetastoreValue::Text(TABLE_ID.to_string()),
            MetastoreValue::Blob(tombstone_ipc.to_vec()),
            MetastoreValue::Integer(keys.len() as i64),
            MetastoreValue::Integer(insert_seq - 1),
            MetastoreValue::Integer(0),
        ],
    })
    .await
    .expect("tombstone");

    tx.commit().await.expect("commit");
}

/// LANE B — fix candidate. Identical statements, ONE `conn.call` via
/// `execute_transaction_batch`. Bindable params (BLOBs / text) are inlined as
/// SQL literals (`X'…'` for blobs) because `execute_transaction_batch` takes a
/// flat SQL string. This isolates the per-statement channel round-trip cost.
async fn run_one_closure_batch(
    metastore: &SqliteMetastore,
    keys: &[Vec<u8>],
    tombstone_ipc: &[u8],
    delete_files: usize,
    insert_seq: i64,
    snapshot_seq: i64,
) {
    // Pre-size: each insert-record row is ~ "(x'<32hex>', 'bench-table-id',
    // x'<32hex>', <seq>)," ≈ 90 bytes.
    let mut sql = String::with_capacity(keys.len() * 96 + tombstone_ipc.len() * 2 + 4096);

    // delete-file rows
    for i in 0..delete_files {
        let id = uuid::Uuid::now_v7();
        let _ = write!(
            sql,
            "INSERT INTO cayenne_delete_file (delete_file_id, table_id, path, path_is_relative, format, delete_count, file_size_bytes, source_data_file_path, sequence_number) \
             VALUES ('{id}', '{TABLE_ID}', 'deletes/dv-{}-{i}.vortex', 1, 'vortex', 1000, 4096, NULL, {}) \
             ON CONFLICT(table_id, path) DO UPDATE SET path = cayenne_delete_file.path; ",
            insert_seq - 1,
            insert_seq - 1,
        );
    }

    // insert-record rows — ONE multi-VALUES statement (no 8000-row chunking
    // needed: literals carry no bind-param budget), matching the fix's intent.
    sql.push_str(
        "INSERT OR REPLACE INTO cayenne_insert_record (insert_record_id, table_id, pk_bytes, sequence_number) VALUES ",
    );
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let id = uuid::Uuid::now_v7();
        let _ = write!(sql, "('{id}', '{TABLE_ID}', x'");
        for b in key {
            let _ = write!(sql, "{b:02x}");
        }
        let _ = write!(sql, "', {insert_seq})");
    }
    sql.push_str("; ");

    // snapshot sequence
    let _ = write!(
        sql,
        "INSERT OR REPLACE INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES ('{TABLE_ID}', 'snap-{snapshot_seq}', {snapshot_seq}); "
    );

    // inline tombstone (large blob as hex literal)
    let tomb_id = uuid::Uuid::now_v7();
    let _ = write!(
        sql,
        "INSERT INTO cayenne_inlined_delete (inlined_id, table_id, delete_ipc, delete_count, sequence_number, published) VALUES ('{tomb_id}', '{TABLE_ID}', x'"
    );
    for b in tombstone_ipc {
        let _ = write!(sql, "{b:02x}");
    }
    let _ = write!(sql, "', {}, {}, 0)", keys.len(), insert_seq - 1);

    metastore
        .execute_transaction_batch(&sql)
        .await
        .expect("execute_transaction_batch");
}

/// Run ONE folded txn and clear the per-table rows afterwards so each iteration
/// starts from the same (empty insert-record) state. The clear runs OUTSIDE the
/// timed closure via a separate call in the bench body.
async fn clear_table(metastore: &SqliteMetastore) {
    metastore
        .execute(ExecuteParams {
            sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1",
            params: vec![MetastoreValue::Text(TABLE_ID.to_string())],
        })
        .await
        .expect("clear insert_record");
    metastore
        .execute(ExecuteParams {
            sql: "DELETE FROM cayenne_inlined_delete WHERE table_id = ?1",
            params: vec![MetastoreValue::Text(TABLE_ID.to_string())],
        })
        .await
        .expect("clear inlined_delete");
    metastore
        .execute(ExecuteParams {
            sql: "DELETE FROM cayenne_delete_file WHERE table_id = ?1",
            params: vec![MetastoreValue::Text(TABLE_ID.to_string())],
        })
        .await
        .expect("clear delete_file");
}

fn bench_folded_vs_closure(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");
    let tombstone_ipc = make_tombstone_ipc();

    let mut group = c.benchmark_group("folded_txn");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));

    for &keys_n in KEY_COUNTS {
        let keys = make_keys(keys_n);
        group.throughput(Throughput::Elements(keys_n as u64));

        // LANE A: production per-statement path.
        group.bench_with_input(
            BenchmarkId::new("folded_per_statement", keys_n),
            &keys_n,
            |b, _| {
                let (metastore, _td) = rt.block_on(fresh_metastore());
                let mut seq = 100_i64;
                b.to_async(&rt).iter(|| {
                    seq += 10;
                    let keys = &keys;
                    let tombstone_ipc = &tombstone_ipc;
                    let metastore = &metastore;
                    async move {
                        run_folded_per_statement(
                            metastore, keys, tombstone_ipc, DELETE_FILES, seq + 2, seq + 3,
                        )
                        .await;
                        clear_table(metastore).await;
                        black_box(());
                    }
                });
            },
        );

        // LANE B: one-closure batch (fix candidate).
        group.bench_with_input(
            BenchmarkId::new("one_closure_batch", keys_n),
            &keys_n,
            |b, _| {
                let (metastore, _td) = rt.block_on(fresh_metastore());
                let mut seq = 100_i64;
                b.to_async(&rt).iter(|| {
                    seq += 10;
                    let keys = &keys;
                    let tombstone_ipc = &tombstone_ipc;
                    let metastore = &metastore;
                    async move {
                        run_one_closure_batch(
                            metastore, keys, tombstone_ipc, DELETE_FILES, seq + 2, seq + 3,
                        )
                        .await;
                        clear_table(metastore).await;
                        black_box(());
                    }
                });
            },
        );
    }
    group.finish();
}

/// Single-segment decomposition at the worst-case key count. Each lane times
/// ONE segment of the folded txn in isolation, run inside its own
/// `BEGIN…COMMIT` so the writer-acquisition + commit cost is attributed to the
/// segment it wraps (begin/commit lanes measure exactly those).
fn bench_decompose(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");
    let keys = make_keys(DECOMPOSE_KEYS);
    let tombstone_ipc = make_tombstone_ipc();

    let mut group = c.benchmark_group("decompose");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(DECOMPOSE_KEYS as u64));

    // begin + commit only (empty txn): the writer-acquisition + COMMIT floor.
    group.bench_function("begin_commit_only", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        b.to_async(&rt).iter(|| {
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                tx.commit().await.expect("commit");
                black_box(());
            }
        });
    });

    // insert-records only (3+4 round-trips + 55K-row engine work), inside a txn.
    group.bench_function("insert_records_only", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let mut seq = 100_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let keys = &keys;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                for chunk in keys.chunks(MAX_INSERT_RECORD_ROWS_PER_CHUNK) {
                    let (sql, params) = build_insert_records_chunk_sql(chunk, seq);
                    tx.execute(ExecuteParams { sql: &sql, params })
                        .await
                        .expect("insert records");
                }
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    // tombstone only (one 2 MiB-blob INSERT), inside a txn.
    group.bench_function("tombstone_only", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let mut seq = 100_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let tombstone_ipc = &tombstone_ipc;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                tx.execute(ExecuteParams {
                    sql: "INSERT INTO cayenne_inlined_delete (inlined_id, table_id, delete_ipc, delete_count, sequence_number, published) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params: vec![
                        MetastoreValue::Text(uuid::Uuid::now_v7().to_string()),
                        MetastoreValue::Text(TABLE_ID.to_string()),
                        MetastoreValue::Blob(tombstone_ipc.to_vec()),
                        MetastoreValue::Integer(55_000),
                        MetastoreValue::Integer(seq),
                        MetastoreValue::Integer(0),
                    ],
                })
                .await
                .expect("tombstone");
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    // delete-files only, inside a txn.
    group.bench_function("delete_files_only", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let mut seq = 100_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                let (sql, params) = build_insert_delete_files_chunk_sql(DELETE_FILES, seq);
                tx.execute(ExecuteParams { sql: &sql, params })
                    .await
                    .expect("delete files");
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    group.finish();
}

/// `prepare_cached` effect: the production path chunks insert-records by exactly
/// 8000 rows, so every FULL chunk re-uses a cached prepared statement after the
/// first burst, but the TAIL chunk's row count varies per burst → cache miss →
/// re-parse of a multi-VALUES statement with up to ~32K placeholders. This lane
/// contrasts an aligned (all-8000) burst with one whose only chunk is an odd
/// size that changes every iteration (forcing a re-parse each time).
fn bench_prepare_cache(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("prepare_cache");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    // Aligned: exactly one full 8000-row chunk every iteration (same SQL text ⇒
    // prepare_cached HIT after iteration 1).
    let aligned_keys = make_keys(MAX_INSERT_RECORD_ROWS_PER_CHUNK);
    group.bench_function("aligned_8000", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let mut seq = 100_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let keys = &aligned_keys;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                let (sql, params) = build_insert_records_chunk_sql(keys, seq);
                tx.execute(ExecuteParams { sql: &sql, params })
                    .await
                    .expect("insert records");
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    // Tail-miss: a chunk whose row count CHANGES every iteration (7999, 7998, …)
    // so the SQL text differs each time ⇒ prepare_cached MISS ⇒ re-parse.
    group.bench_function("tail_varying", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let base_keys = make_keys(MAX_INSERT_RECORD_ROWS_PER_CHUNK);
        let mut seq = 100_i64;
        let mut shrink = 0_usize;
        b.to_async(&rt).iter(|| {
            seq += 10;
            shrink = (shrink + 1) % 500; // vary the row count each iteration
            let n = MAX_INSERT_RECORD_ROWS_PER_CHUNK - 1 - shrink;
            let keys = &base_keys[..n];
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                let (sql, params) = build_insert_records_chunk_sql(keys, seq);
                tx.execute(ExecuteParams { sql: &sql, params })
                    .await
                    .expect("insert records");
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    group.finish();
}

/// Index-growth effect: `cayenne_insert_record` accumulates rows between
/// checkpoints (only `clear_insert_records` empties it). Each INSERT OR REPLACE
/// probes the `UNIQUE(table_id, pk_bytes)` index, so a large resident index
/// raises per-row cost. This lane inserts a fixed 20K-row burst into (a) an
/// empty table and (b) a table pre-loaded with 200K committed rows that are NOT
/// cleared between iterations (distinct key ranges so no REPLACE collisions).
fn bench_index_growth(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");
    const BURST: usize = 20_000;
    const PRELOAD: usize = 200_000;

    let mut group = c.benchmark_group("index_growth");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(BURST as u64));

    // Empty: clear between iterations so the index stays tiny.
    group.bench_function("empty_table", |b| {
        let (metastore, _td) = rt.block_on(fresh_metastore());
        let keys = make_keys(BURST);
        let mut seq = 100_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let keys = &keys;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                for chunk in keys.chunks(MAX_INSERT_RECORD_ROWS_PER_CHUNK) {
                    let (sql, params) = build_insert_records_chunk_sql(chunk, seq);
                    tx.execute(ExecuteParams { sql: &sql, params })
                        .await
                        .expect("insert records");
                }
                tx.commit().await.expect("commit");
                clear_table(metastore).await;
                black_box(());
            }
        });
    });

    // Preloaded: 200K resident rows; the timed 20K burst uses a disjoint key
    // range and is REMOVED (by exact id) after each iteration to keep the
    // resident size at PRELOAD. Approximated here by clearing only the burst's
    // range via sequence_number (burst rows carry the iteration seq).
    group.bench_function("preloaded_200k", |b| {
        let (metastore, _td) = rt.block_on(async {
            let (m, td) = fresh_metastore().await;
            // Preload 200K committed rows in the low key range.
            let preload_keys = make_keys(PRELOAD);
            let tx = m.begin_transaction().await.expect("begin");
            for chunk in preload_keys.chunks(MAX_INSERT_RECORD_ROWS_PER_CHUNK) {
                let (sql, params) = build_insert_records_chunk_sql(chunk, 50);
                tx.execute(ExecuteParams { sql: &sql, params })
                    .await
                    .expect("preload");
            }
            tx.commit().await.expect("commit preload");
            (m, td)
        });
        // Burst keys in a disjoint high range so they never REPLACE a preloaded
        // row (pure inserts into the large index).
        let burst_keys: Vec<Vec<u8>> = (PRELOAD..PRELOAD + BURST)
            .map(|i| {
                let mut key = Vec::with_capacity(PK_KEY_BYTES);
                key.extend_from_slice(&(i as u64).to_be_bytes());
                while key.len() < PK_KEY_BYTES {
                    key.push(0xAB);
                }
                key
            })
            .collect();
        let mut seq = 1000_i64;
        b.to_async(&rt).iter(|| {
            seq += 10;
            let keys = &burst_keys;
            let metastore = &metastore;
            async move {
                let tx = metastore.begin_transaction().await.expect("begin");
                for chunk in keys.chunks(MAX_INSERT_RECORD_ROWS_PER_CHUNK) {
                    let (sql, params) = build_insert_records_chunk_sql(chunk, seq);
                    tx.execute(ExecuteParams { sql: &sql, params })
                        .await
                        .expect("insert records");
                }
                tx.commit().await.expect("commit");
                // Remove only this burst's rows (the high range) to hold the
                // resident index at PRELOAD for the next iteration.
                metastore
                    .execute(ExecuteParams {
                        sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?1 AND sequence_number = ?2",
                        params: vec![
                            MetastoreValue::Text(TABLE_ID.to_string()),
                            MetastoreValue::Integer(seq),
                        ],
                    })
                    .await
                    .expect("clear burst");
                black_box(());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_folded_vs_closure,
    bench_decompose,
    bench_prepare_cache,
    bench_index_growth
);
criterion_main!(benches);
