// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Cycle-11 follow-up: the folded Stage-A txn's cost is ~98% the
//! `cayenne_insert_record` INSERT OR REPLACE work (see
//! `folded_txn_decomposition`). This bench pinpoints WHICH part of the
//! insert-record write is expensive by contrasting schema variants on the same
//! 20K-row burst, run inside one `BEGIN…COMMIT` against a real `SqliteMetastore`
//! with the runtime pragmas.
//!
//! The production schema is:
//! ```sql
//! CREATE TABLE cayenne_insert_record (
//!     insert_record_id TEXT PRIMARY KEY,   -- a random UUIDv7, NEVER queried
//!     table_id TEXT NOT NULL,
//!     pk_bytes BLOB NOT NULL,
//!     sequence_number BIGINT NOT NULL,
//!     UNIQUE(table_id, pk_bytes)           -- the ONLY access path used
//! );
//! ```
//! Reads are always `SELECT pk_bytes, sequence_number … WHERE table_id = ?`;
//! deletes are by `table_id` (checkpoint) or `(table_id, pk_bytes)`. The UUID PK
//! is dead weight: every row maintains BOTH a rowid→UUID PK btree AND the
//! `(table_id, pk_bytes)` unique index, and mints a UUID + 36-byte text alloc.
//!
//! Variants (each a fresh DB, 20K rows, one txn, repeated):
//! - `prod_or_replace_uuid_pk` — production: UUID TEXT PK + UNIQUE(table_id,
//!   pk_bytes), `INSERT OR REPLACE`. Baseline.
//! - `plain_insert_uuid_pk` — same schema, plain `INSERT` (no OR REPLACE).
//!   Isolates the OR-REPLACE conflict-probe cost.
//! - `without_rowid_composite_pk` — `PRIMARY KEY (table_id, pk_bytes) WITHOUT
//!   ROWID`, no UUID column, `INSERT OR REPLACE`. The proposed fix: one btree,
//!   no UUID.
//! - `without_rowid_plain_insert` — same WITHOUT ROWID schema, plain `INSERT`.
//!   Floor for the fix.
//!
//! `cargo bench --bench insert_record_schema_variants -p cayenne`.

#![expect(clippy::expect_used)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Duration;

use cayenne::metastore::sqlite::{
    SqliteMetastore, SqliteMetastoreConfig, set_sqlite_metastore_config,
};
use cayenne::metastore::{ExecuteParams, MetastoreBackend, MetastoreValue};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const BURST: usize = 20_000;
const PK_KEY_BYTES: usize = 16;
const MAX_PARAMS: usize = 32_000;
const TABLE_ID: &str = "bench-table-id";

fn apply_runtime_pragmas() {
    set_sqlite_metastore_config(SqliteMetastoreConfig {
        cache_size_mb: 1024,
        mmap_size_bytes: 4096 * 1024 * 1024,
        ..SqliteMetastoreConfig::default()
    });
}

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

/// `with_uuid`: schema has the UUID TEXT PK + 4 params/row; otherwise the
/// WITHOUT ROWID composite-PK schema with 3 params/row.
/// `or_replace`: emit `INSERT OR REPLACE` vs plain `INSERT`.
fn build_chunk_sql(
    keys: &[Vec<u8>],
    seq: i64,
    with_uuid: bool,
    or_replace: bool,
) -> (String, Vec<MetastoreValue>) {
    let verb = if or_replace { "INSERT OR REPLACE" } else { "INSERT" };
    let (prefix, params_per_row) = if with_uuid {
        (
            format!(
                "{verb} INTO ir_uuid (insert_record_id, table_id, pk_bytes, sequence_number) VALUES "
            ),
            4usize,
        )
    } else {
        (
            format!("{verb} INTO ir_wr (table_id, pk_bytes, sequence_number) VALUES "),
            3usize,
        )
    };
    let mut sql = String::with_capacity(prefix.len() + keys.len() * 32);
    sql.push_str(&prefix);
    let mut params = Vec::with_capacity(keys.len() * params_per_row);
    for (i, key) in keys.iter().enumerate() {
        let base = i * params_per_row + 1;
        if i > 0 {
            sql.push_str(", ");
        }
        if with_uuid {
            let _ = write!(sql, "(?{}, ?{}, ?{}, ?{})", base, base + 1, base + 2, base + 3);
            params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
            params.push(MetastoreValue::Text(TABLE_ID.to_string()));
            params.push(MetastoreValue::Blob(key.clone()));
            params.push(MetastoreValue::Integer(seq));
        } else {
            let _ = write!(sql, "(?{}, ?{}, ?{})", base, base + 1, base + 2);
            params.push(MetastoreValue::Text(TABLE_ID.to_string()));
            params.push(MetastoreValue::Blob(key.clone()));
            params.push(MetastoreValue::Integer(seq));
        }
    }
    (sql, params)
}

async fn fresh(with_uuid: bool) -> (SqliteMetastore, TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path().join("catalog.db");
    let metastore = SqliteMetastore::new(format!("sqlite://{}", db_path.display()));
    // init_schema applies the runtime pragmas to the connection; then we add the
    // two experimental tables.
    metastore.init_schema().await.expect("init_schema");
    let ddl = if with_uuid {
        "CREATE TABLE ir_uuid (\
            insert_record_id TEXT PRIMARY KEY, \
            table_id TEXT NOT NULL, \
            pk_bytes BLOB NOT NULL, \
            sequence_number BIGINT NOT NULL, \
            UNIQUE(table_id, pk_bytes))"
    } else {
        "CREATE TABLE ir_wr (\
            table_id TEXT NOT NULL, \
            pk_bytes BLOB NOT NULL, \
            sequence_number BIGINT NOT NULL, \
            PRIMARY KEY (table_id, pk_bytes)) WITHOUT ROWID"
    };
    metastore.execute_batch(ddl).await.expect("create variant table");
    (metastore, temp_dir)
}

async fn run_burst(
    metastore: &SqliteMetastore,
    keys: &[Vec<u8>],
    seq: i64,
    with_uuid: bool,
    or_replace: bool,
) {
    let params_per_row = if with_uuid { 4 } else { 3 };
    let rows_per_chunk = MAX_PARAMS / params_per_row;
    let tx = metastore.begin_transaction().await.expect("begin");
    for chunk in keys.chunks(rows_per_chunk) {
        let (sql, params) = build_chunk_sql(chunk, seq, with_uuid, or_replace);
        tx.execute(ExecuteParams { sql: &sql, params })
            .await
            .expect("insert");
    }
    tx.commit().await.expect("commit");
    let table = if with_uuid { "ir_uuid" } else { "ir_wr" };
    metastore
        .execute(ExecuteParams {
            sql: &format!("DELETE FROM {table}"),
            params: vec![],
        })
        .await
        .expect("clear");
}

fn bench_variants(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");
    let keys = make_keys(BURST);

    let mut group = c.benchmark_group("insert_record_schema");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(BURST as u64));

    let cases: &[(&str, bool, bool)] = &[
        ("prod_or_replace_uuid_pk", true, true),
        ("plain_insert_uuid_pk", true, false),
        ("without_rowid_composite_pk", false, true),
        ("without_rowid_plain_insert", false, false),
    ];

    for &(name, with_uuid, or_replace) in cases {
        group.bench_function(name, |b| {
            let (metastore, _td) = rt.block_on(fresh(with_uuid));
            let mut seq = 100_i64;
            b.to_async(&rt).iter(|| {
                seq += 10;
                let keys = &keys;
                let metastore = &metastore;
                async move {
                    run_burst(metastore, keys, seq, with_uuid, or_replace).await;
                    black_box(());
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_variants);
criterion_main!(benches);
