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

//! Writes the Turso 0.7.2 on-disk files the 0.8.1 compat test opens.
//!
//! This crate is **not** a workspace member: cargo would unify `turso` to
//! 0.8.1 and the files would no longer be 0.7.2. Run it from this directory:
//!
//! ```text
//! cargo run --release
//! ```
//!
//! Output lands in `crates/cayenne/tests/fixtures/turso_0_7_2/`, with a
//! `.fixture` suffix so the repo `.gitignore` (`*.db`, `*.db-log`) does not
//! drop the checked-in copies.

use std::path::{Path, PathBuf};

use turso::{Builder, Connection, Database};

const ACCELERATOR_ROW_COUNT: i64 = 32;
const CAYENNE_TABLE_ID: &str = "07200000-0000-7000-8000-000000000001";
const CAYENNE_TABLE_NAME: &str = "compat_from_072";
const CAYENNE_SNAPSHOT_ID: &str = "07200000-0000-7000-8000-000000000002";
const CAYENNE_SEQUENCE: i64 = 42;
const CHECKPOINT_DATASET: &str = "compat_from_072";
const CHECKPOINT_REFRESH_SQL: &str = "SELECT id, name FROM source WHERE id <= 32";

#[tokio::main]
async fn main() {
    let out_dir = fixture_dir();
    std::fs::create_dir_all(&out_dir).expect("create fixture directory");

    write_accelerator(&out_dir, false)
        .await
        .expect("accelerator (MVCC log)");
    write_accelerator(&out_dir, true)
        .await
        .expect("accelerator (checkpointed)");
    write_checkpoint(&out_dir)
        .await
        .expect("dataset checkpoint");
    write_cayenne_metastore(&out_dir)
        .await
        .expect("cayenne metastore");

    write_manifest(&out_dir).expect("write manifest");
    println!("wrote Turso 0.7.2 fixtures to {}", out_dir.display());
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tools/")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn fixture_dir() -> PathBuf {
    workspace_root().join("crates/cayenne/tests/fixtures/turso_0_7_2")
}

struct Session {
    db: Database,
    conn: Connection,
}

async fn open_db(path: &Path, checkpoint_every_commit: bool) -> Session {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create db parent");
    }
    let db = Builder::new_local(path.to_str().expect("utf-8 path"))
        .build()
        .await
        .expect("open turso 0.7.2 database");
    let conn = db.connect().expect("connect");
    // `PRAGMA journal_mode` returns a row; `execute` rejects that as Misuse.
    // The PRAGMA runs when it is stepped, so a failure surfaces from `next()`.
    let mut journal = conn
        .query("PRAGMA journal_mode = 'mvcc'", ())
        .await
        .expect("enable MVCC journal mode");
    journal.next().await.expect("enable MVCC journal mode");
    drop(journal);
    if checkpoint_every_commit {
        let mut threshold = conn
            .query("PRAGMA mvcc_checkpoint_threshold = 0", ())
            .await
            .expect("checkpoint after every commit");
        threshold
            .next()
            .await
            .expect("checkpoint after every commit");
        drop(threshold);
    }
    Session { db, conn }
}

async fn write_accelerator(out_dir: &Path, checkpoint_every_commit: bool) -> Result<(), String> {
    let stem = if checkpoint_every_commit {
        "accelerator-checkpointed"
    } else {
        "accelerator-mvcc"
    };
    let work = tempfile_work_dir(stem);
    let db_path = work.join("data.db");
    let session = open_db(&db_path, checkpoint_every_commit).await;
    session
        .conn
        .execute(
            "CREATE TABLE accelerated (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            note TEXT
        )",
            (),
        )
        .await
        .map_err(|e| format!("create accelerated: {e}"))?;

    for id in 1..=ACCELERATOR_ROW_COUNT {
        let name = format!("row-{id}");
        let note = if id == 2 {
            Some("utf8-✓".to_string())
        } else {
            None
        };
        session
            .conn
            .execute(
                "INSERT INTO accelerated (id, name, value, note) VALUES (?1, ?2, ?3, ?4)",
                (id, name, id * 10, note),
            )
            .await
            .map_err(|e| format!("insert accelerated row {id}: {e}"))?;
    }
    drop(session.conn);
    drop(session.db);
    copy_turso_sidecars(&work, "data.db", out_dir, stem)
}

async fn write_checkpoint(out_dir: &Path) -> Result<(), String> {
    let work = tempfile_work_dir("checkpoint");
    let db_path = work.join("data.db");
    let session = open_db(&db_path, false).await;
    session
        .conn
        .execute(
            "CREATE TABLE spice_sys_dataset_checkpoint (
            dataset_name TEXT PRIMARY KEY,
            schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            refresh_sql TEXT
        )",
            (),
        )
        .await
        .map_err(|e| format!("create checkpoint table: {e}"))?;
    session
        .conn
        .execute(
            "INSERT INTO spice_sys_dataset_checkpoint
            (dataset_name, schema_json, refresh_sql)
         VALUES (?1, ?2, ?3)",
            (
                CHECKPOINT_DATASET.to_string(),
                "{\"fields\":[{\"name\":\"id\",\"data_type\":\"Int64\"}]}".to_string(),
                CHECKPOINT_REFRESH_SQL.to_string(),
            ),
        )
        .await
        .map_err(|e| format!("insert checkpoint row: {e}"))?;
    drop(session.conn);
    drop(session.db);
    copy_turso_sidecars(&work, "data.db", out_dir, "checkpoint")
}

async fn write_cayenne_metastore(out_dir: &Path) -> Result<(), String> {
    let work = tempfile_work_dir("cayenne");
    let db_path = work.join("data.db");
    let session = open_db(&db_path, false).await;

    // Current Cayenne Turso DDL (schema is Spice's; the engine writing the
    // pages is 0.7.2). CREATE IF NOT EXISTS so the 0.8.1 open can migrate.
    session
        .conn
        .execute_batch(
            r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            on_conflict_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT,
            current_sequence_number BIGINT NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_key TEXT,
            data_ipc BLOB NOT NULL,
            record_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS cayenne_delete_file (
            delete_file_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            format TEXT NOT NULL,
            delete_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            source_data_file_path TEXT,
            sequence_number BIGINT NOT NULL DEFAULT 0,
            reinsert_sequence BIGINT,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS cayenne_partition (
            partition_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_columns_json TEXT NOT NULL,
            partition_values_json TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, partition_key)
        );
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            PRIMARY KEY (table_id, pk_bytes)
        );
        CREATE TABLE IF NOT EXISTS cayenne_pending_write_back (
            table_id BLOB NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            first_marked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            PRIMARY KEY (table_id, pk_bytes)
        );
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id)
        );
        CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
            table_id TEXT NOT NULL PRIMARY KEY,
            statistics_blob BLOB NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            ndv_sketches BLOB,
            num_rows_exact INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_file_statistics (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            statistics_blob BLOB NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id, file_path)
        );
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_file (
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            min_sequence BIGINT NOT NULL DEFAULT 0,
            max_sequence BIGINT NOT NULL DEFAULT 0,
            digest TEXT,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id, file_path)
        );
        CREATE TABLE IF NOT EXISTS cayenne_cold_tier_file (
            table_id TEXT NOT NULL,
            file_url TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            min_sequence BIGINT NOT NULL DEFAULT 0,
            max_sequence BIGINT NOT NULL DEFAULT 0,
            statistics_blob BLOB NOT NULL,
            pk_bloom_blob BLOB,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, file_url)
        );
        CREATE TABLE IF NOT EXISTS cayenne_pk_index (
            table_id TEXT NOT NULL PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            index_blob BLOB NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            delete_ipc BLOB NOT NULL,
            delete_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            published INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        );
        ",
        )
        .await
        .map_err(|e| format!("cayenne schema: {e}"))?;

    let mut version = session
        .conn
        .query("PRAGMA user_version = 1", ())
        .await
        .map_err(|e| format!("stamp user_version: {e}"))?;
    version
        .next()
        .await
        .map_err(|e| format!("stamp user_version: {e}"))?;
    drop(version);

    session
        .conn
        .execute(
            "INSERT INTO cayenne_table (
            table_id, table_name, path, path_is_relative, schema_json,
            current_snapshot_id, current_sequence_number
         ) VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6)",
            (
                CAYENNE_TABLE_ID.to_string(),
                CAYENNE_TABLE_NAME.to_string(),
                "/tmp/compat-from-072".to_string(),
                "schema-written-by-turso-0.7.2".to_string(),
                CAYENNE_SNAPSHOT_ID.to_string(),
                CAYENNE_SEQUENCE,
            ),
        )
        .await
        .map_err(|e| format!("insert cayenne_table: {e}"))?;

    session
        .conn
        .execute(
            "INSERT INTO cayenne_inlined_data (
            inlined_id, table_id, data_ipc, record_count, sequence_number
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                "07200000-0000-7000-8000-000000000003".to_string(),
                CAYENNE_TABLE_ID.to_string(),
                vec![0x07, 0x72, 0x00, 0x81],
                1_i64,
                7_i64,
            ),
        )
        .await
        .map_err(|e| format!("insert cayenne_inlined_data: {e}"))?;

    drop(session.conn);
    drop(session.db);
    copy_turso_sidecars(&work, "data.db", out_dir, "cayenne-metastore")
}

fn tempfile_work_dir(stem: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("turso-072-fixture-{stem}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create work dir");
    dir
}

/// Copy every Turso sidecar next to `db_name` into `out_dir` as
/// `{stem}{suffix}.fixture` (e.g. `accelerator-mvcc.db.fixture`,
/// `accelerator-mvcc.db-log.fixture`).
fn copy_turso_sidecars(
    work: &Path,
    db_name: &str,
    out_dir: &Path,
    stem: &str,
) -> Result<(), String> {
    let db_path = work.join(db_name);
    let file_stem = db_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("db path {} has no stem", db_path.display()))?;
    let entries =
        std::fs::read_dir(work).map_err(|e| format!("read work dir {}: {e}", work.display()))?;
    let mut copied = 0_usize;
    for entry in entries {
        // A failed entry read could be a sidecar the fixture needs, so it
        // fails the run instead of leaving an incomplete fixture behind.
        let entry =
            entry.map_err(|e| format!("read an entry of work dir {}: {e}", work.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(rest) = name.strip_prefix(file_stem) else {
            continue;
        };
        if !(rest.is_empty() || rest.starts_with('.') || rest.starts_with('-')) {
            continue;
        }
        let dest_name = format!("{stem}{rest}.fixture");
        let dest = out_dir.join(&dest_name);
        let len = entry
            .metadata()
            .map_err(|e| format!("stat {}: {e}", entry.path().display()))?
            .len();
        if len == 0 {
            // Empty WAL/log sidecars are not needed to reopen; skip them so the
            // 0.8.1 open does not see a zero-length MVCC log next to a
            // checkpointed B-tree.
            continue;
        }
        std::fs::copy(entry.path(), &dest)
            .map_err(|e| format!("copy {} -> {}: {e}", entry.path().display(), dest.display()))?;
        println!("  {dest_name} ({len} bytes)");
        copied += 1;
    }
    if copied == 0 {
        return Err(format!(
            "turso 0.7.2 wrote no files next to {}",
            db_path.display()
        ));
    }
    Ok(())
}

fn write_manifest(out_dir: &Path) -> Result<(), String> {
    let sum_value = (1..=ACCELERATOR_ROW_COUNT).map(|id| id * 10).sum::<i64>();
    let names = (1..=ACCELERATOR_ROW_COUNT)
        .map(|id| format!("row-{id}"))
        .collect::<Vec<_>>()
        .join(",");
    let text = format!(
        "turso_version=0.7.2\n\
         accelerator_row_count={ACCELERATOR_ROW_COUNT}\n\
         accelerator_sum_value={sum_value}\n\
         accelerator_names={names}\n\
         accelerator_note_row=2\n\
         accelerator_note=utf8-✓\n\
         checkpoint_dataset={CHECKPOINT_DATASET}\n\
         checkpoint_refresh_sql={CHECKPOINT_REFRESH_SQL}\n\
         cayenne_table_id={CAYENNE_TABLE_ID}\n\
         cayenne_table_name={CAYENNE_TABLE_NAME}\n\
         cayenne_snapshot_id={CAYENNE_SNAPSHOT_ID}\n\
         cayenne_sequence={CAYENNE_SEQUENCE}\n\
         cayenne_inlined_id=07200000-0000-7000-8000-000000000003\n\
         cayenne_inlined_bytes=07,72,00,81\n\
         cayenne_inlined_record_count=1\n"
    );
    std::fs::write(out_dir.join("MANIFEST.txt"), text)
        .map_err(|e| format!("write MANIFEST.txt: {e}"))
}
