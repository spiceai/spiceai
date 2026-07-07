// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Insert-record write-cost / WAL-volume bench. The folded Stage-A txn's cost
//! is ~98% the `cayenne_insert_record` INSERT OR REPLACE work (see
//! `folded_txn_decomposition`). This bench contrasts the historical and current
//! `cayenne_insert_record` schema shapes on the same burst, run inside one
//! `BEGIN…COMMIT` against a real `SqliteMetastore` with the runtime pragmas, and
//! reports the WAL-frame reduction of the current shape.
//!
//! The current (cycle-12) production schema is:
//! ```sql
//! CREATE TABLE cayenne_insert_record (
//!     table_id BLOB NOT NULL,              -- 16 raw UUID bytes (was 36-char text)
//!     pk_bytes BLOB NOT NULL,
//!     sequence_number BIGINT NOT NULL,
//!     PRIMARY KEY (table_id, pk_bytes)     -- the ONLY access path used
//! ) WITHOUT ROWID;                         -- one B-tree, no UUID PK, no FK
//! ```
//! Reads are always `SELECT pk_bytes, sequence_number … WHERE table_id = ?`;
//! deletes are by `table_id` (checkpoint) or `(table_id, pk_bytes)`. cycle-11
//! dropped the dead `insert_record_id` UUID PK + redundant unique index;
//! cycle-12 re-encodes the leading `table_id` from 36-char text to the 16 raw
//! UUID bytes, cutting the WAL frames a hot upsert burst writes by ~a third
//! (the column is identical on every row of a burst and leads the clustered
//! key, so the shrink both narrows each cell and packs more rows per leaf).
//!
//! Variants (each a fresh DB, 20K rows, one txn, repeated):
//! - `prod_or_replace_uuid_pk` — pre-cycle-11: UUID TEXT PK + UNIQUE(table_id,
//!   pk_bytes), `INSERT OR REPLACE`. Original baseline.
//! - `plain_insert_uuid_pk` — same schema, plain `INSERT` (no OR REPLACE).
//!   Isolates the OR-REPLACE conflict-probe cost.
//! - `without_rowid_composite_pk` — cycle-11: `PRIMARY KEY (table_id, pk_bytes)
//!   WITHOUT ROWID`, no UUID column, TEXT `table_id`, `INSERT OR REPLACE`.
//! - `without_rowid_plain_insert` — same cycle-11 schema, plain `INSERT`.
//! - `blob_table_id_composite_pk` — cycle-12: the cycle-11 WITHOUT ROWID shape
//!   but `table_id` stored as the 16 raw UUID bytes (`BLOB`) instead of 36-char
//!   text. The current production shape; the WAL-volume lever.
//!
//! In addition to the criterion wall-time variants, [`bench_variants`] prints a
//! one-shot **WAL-frame-count** comparison (cycle-11 TEXT `table_id` vs
//! cycle-12 BLOB `table_id`) for a fixed 55K-key burst — the actual WAL-volume
//! metric the lever targets (frames = `(-wal size − 32) / 4120`) — and asserts
//! the BLOB shape writes strictly fewer frames.
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
/// The hot-table per-burst key count the WAL-volume lever is anchored on (the
/// cycle-11 `insert_records_only` = 55.7 ms / 55K-keys measurement point).
const WAL_BURST: usize = 55_000;
const PK_KEY_BYTES: usize = 16;
const MAX_PARAMS: usize = 32_000;
/// A realistic UUIDv7 `table_id` so the cycle-12 BLOB shape stores the 16 raw
/// bytes (`uuid::parse_str` succeeds) rather than the non-UUID fallback.
const TABLE_ID: &str = "0197e8a0-1234-7890-abcd-ef0123456789";

/// `cayenne_insert_record` row layout under test.
#[derive(Clone, Copy)]
enum Shape {
    /// Pre-cycle-11: random-UUID `insert_record_id` TEXT PK + `UNIQUE(table_id,
    /// pk_bytes)`; `table_id` TEXT. 4 bound params/row.
    UuidPkText,
    /// cycle-11: `PRIMARY KEY (table_id, pk_bytes) WITHOUT ROWID`; `table_id`
    /// TEXT. 3 bound params/row.
    WithoutRowidText,
    /// cycle-12: cycle-11 shape but `table_id` is the 16 raw UUID bytes (BLOB).
    /// 3 bound params/row. The production shape and WAL-volume lever.
    WithoutRowidBlob,
}

impl Shape {
    fn table(self) -> &'static str {
        match self {
            Shape::UuidPkText => "ir_uuid",
            Shape::WithoutRowidText => "ir_wr",
            Shape::WithoutRowidBlob => "ir_blob",
        }
    }

    fn params_per_row(self) -> usize {
        match self {
            Shape::UuidPkText => 4,
            Shape::WithoutRowidText | Shape::WithoutRowidBlob => 3,
        }
    }

    fn ddl(self) -> &'static str {
        match self {
            Shape::UuidPkText => {
                "CREATE TABLE ir_uuid (\
                    insert_record_id TEXT PRIMARY KEY, \
                    table_id TEXT NOT NULL, \
                    pk_bytes BLOB NOT NULL, \
                    sequence_number BIGINT NOT NULL, \
                    UNIQUE(table_id, pk_bytes))"
            }
            Shape::WithoutRowidText => {
                "CREATE TABLE ir_wr (\
                    table_id TEXT NOT NULL, \
                    pk_bytes BLOB NOT NULL, \
                    sequence_number BIGINT NOT NULL, \
                    PRIMARY KEY (table_id, pk_bytes)) WITHOUT ROWID"
            }
            Shape::WithoutRowidBlob => {
                "CREATE TABLE ir_blob (\
                    table_id BLOB NOT NULL, \
                    pk_bytes BLOB NOT NULL, \
                    sequence_number BIGINT NOT NULL, \
                    PRIMARY KEY (table_id, pk_bytes)) WITHOUT ROWID"
            }
        }
    }

    /// The bound `table_id` value: 16 raw UUID bytes for the BLOB shape, the
    /// 36-char text otherwise.
    fn table_id_value(self) -> MetastoreValue {
        match self {
            Shape::UuidPkText | Shape::WithoutRowidText => {
                MetastoreValue::Text(TABLE_ID.to_string())
            }
            Shape::WithoutRowidBlob => {
                MetastoreValue::Blob(cayenne::metastore::table_id_to_key_bytes(TABLE_ID))
            }
        }
    }
}

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

/// Build one chunk's `INSERT [OR REPLACE]` for the given shape.
fn build_chunk_sql(
    keys: &[Vec<u8>],
    seq: i64,
    shape: Shape,
    or_replace: bool,
) -> (String, Vec<MetastoreValue>) {
    let verb = if or_replace {
        "INSERT OR REPLACE"
    } else {
        "INSERT"
    };
    let params_per_row = shape.params_per_row();
    let prefix = match shape {
        Shape::UuidPkText => format!(
            "{verb} INTO {} (insert_record_id, table_id, pk_bytes, sequence_number) VALUES ",
            shape.table()
        ),
        Shape::WithoutRowidText | Shape::WithoutRowidBlob => {
            format!(
                "{verb} INTO {} (table_id, pk_bytes, sequence_number) VALUES ",
                shape.table()
            )
        }
    };
    let mut sql = String::with_capacity(prefix.len() + keys.len() * 32);
    sql.push_str(&prefix);
    let mut params = Vec::with_capacity(keys.len() * params_per_row);
    for (i, key) in keys.iter().enumerate() {
        let base = i * params_per_row + 1;
        if i > 0 {
            sql.push_str(", ");
        }
        if matches!(shape, Shape::UuidPkText) {
            let _ = write!(
                sql,
                "(?{}, ?{}, ?{}, ?{})",
                base,
                base + 1,
                base + 2,
                base + 3
            );
            params.push(MetastoreValue::Text(uuid::Uuid::now_v7().to_string()));
            params.push(shape.table_id_value());
            params.push(MetastoreValue::Blob(key.clone()));
            params.push(MetastoreValue::Integer(seq));
        } else {
            let _ = write!(sql, "(?{}, ?{}, ?{})", base, base + 1, base + 2);
            params.push(shape.table_id_value());
            params.push(MetastoreValue::Blob(key.clone()));
            params.push(MetastoreValue::Integer(seq));
        }
    }
    (sql, params)
}

async fn fresh(shape: Shape) -> (SqliteMetastore, TempDir) {
    let (m, _path, td) = fresh_with_path(shape).await;
    (m, td)
}

/// Like [`fresh`] but also returns the on-disk DB path so a caller can stat the
/// `-wal` sidecar directly (the backend's WAL-size accessor is private).
async fn fresh_with_path(shape: Shape) -> (SqliteMetastore, std::path::PathBuf, TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path().join("catalog.db");
    let metastore = SqliteMetastore::new(format!("sqlite://{}", db_path.display()));
    // init_schema applies the runtime pragmas to the connection; then we add the
    // experimental table for this shape.
    metastore.init_schema().await.expect("init_schema");
    metastore
        .execute_batch(shape.ddl())
        .await
        .expect("create variant table");
    (metastore, db_path, temp_dir)
}

fn wal_bytes(db_path: &std::path::Path) -> u64 {
    // The WAL sidecar is `<db_path>-wal` (see SqliteMetastore::read_wal_bytes).
    let wal = format!("{}-wal", db_path.display());
    std::fs::metadata(&wal).map_or(0, |m| m.len())
}

async fn insert_burst(
    metastore: &SqliteMetastore,
    keys: &[Vec<u8>],
    seq: i64,
    shape: Shape,
    or_replace: bool,
) {
    let rows_per_chunk = MAX_PARAMS / shape.params_per_row();
    let tx = metastore.begin_transaction().await.expect("begin");
    for chunk in keys.chunks(rows_per_chunk) {
        let (sql, params) = build_chunk_sql(chunk, seq, shape, or_replace);
        tx.execute(ExecuteParams { sql: &sql, params })
            .await
            .expect("insert");
    }
    tx.commit().await.expect("commit");
}

async fn run_burst(
    metastore: &SqliteMetastore,
    keys: &[Vec<u8>],
    seq: i64,
    shape: Shape,
    or_replace: bool,
) {
    insert_burst(metastore, keys, seq, shape, or_replace).await;
    metastore
        .execute(ExecuteParams {
            sql: &format!("DELETE FROM {}", shape.table()),
            params: vec![],
        })
        .await
        .expect("clear");
}

/// Measure the WAL frames written by exactly one `WAL_BURST`-key `INSERT OR
/// REPLACE` burst for a shape. The WAL is checkpoint-truncated to zero *before*
/// the burst so the `-wal` size reflects only this COMMIT's dirty pages; with
/// `synchronous=NORMAL` + `wal_autocheckpoint=0`, frames = `(size − 32) / 4120`.
async fn wal_frames_for_burst(shape: Shape, keys: &[Vec<u8>]) -> u64 {
    let (metastore, db_path, _td) = fresh_with_path(shape).await;
    // Drain the DDL/init frames out of the WAL so we start from ~0.
    metastore.checkpoint_wal().await.expect("pre-checkpoint");
    let before = wal_bytes(&db_path);
    insert_burst(&metastore, keys, 1_234_567, shape, true).await;
    let after = wal_bytes(&db_path);
    let delta = after.saturating_sub(before);
    // Each frame is a 4096-byte page + 24-byte header.
    delta / (4096 + 24)
}

/// Print (and assert) the WAL-frame reduction of the cycle-12 BLOB `table_id`
/// vs the cycle-11 TEXT `table_id` for a fixed `WAL_BURST` burst. This is the
/// WAL-volume metric the lever targets; it runs once at bench start.
fn report_wal_frame_reduction(rt: &Runtime, keys: &[Vec<u8>]) {
    let text_frames = rt.block_on(wal_frames_for_burst(Shape::WithoutRowidText, keys));
    let blob_frames = rt.block_on(wal_frames_for_burst(Shape::WithoutRowidBlob, keys));
    let pct = if text_frames > 0 {
        100.0 * (text_frames as f64 - blob_frames as f64) / text_frames as f64
    } else {
        0.0
    };
    eprintln!(
        "[wal-volume] {WAL_BURST}-key burst: TEXT table_id = {text_frames} frames, \
         BLOB table_id = {blob_frames} frames, reduction = {pct:.1}%"
    );
    assert!(
        blob_frames < text_frames,
        "BLOB table_id must write strictly fewer WAL frames than TEXT \
         (BLOB={blob_frames}, TEXT={text_frames})"
    );
}

fn bench_variants(c: &mut Criterion) {
    apply_runtime_pragmas();
    let rt = Runtime::new().expect("runtime");

    // One-shot WAL-volume report on the 55K-key anchor point.
    let wal_keys = make_keys(WAL_BURST);
    report_wal_frame_reduction(&rt, &wal_keys);

    let keys = make_keys(BURST);

    let mut group = c.benchmark_group("insert_record_schema");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(BURST as u64));

    let cases: &[(&str, Shape, bool)] = &[
        ("prod_or_replace_uuid_pk", Shape::UuidPkText, true),
        ("plain_insert_uuid_pk", Shape::UuidPkText, false),
        ("without_rowid_composite_pk", Shape::WithoutRowidText, true),
        ("without_rowid_plain_insert", Shape::WithoutRowidText, false),
        ("blob_table_id_composite_pk", Shape::WithoutRowidBlob, true),
    ];

    for &(name, shape, or_replace) in cases {
        group.bench_function(name, |b| {
            let (metastore, _td) = rt.block_on(fresh(shape));
            let mut seq = 100_i64;
            b.to_async(&rt).iter(|| {
                seq += 10;
                let keys = &keys;
                let metastore = &metastore;
                async move {
                    run_burst(metastore, keys, seq, shape, or_replace).await;
                    black_box(());
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_variants);
criterion_main!(benches);
