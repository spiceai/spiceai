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

//! Turso 0.7.2 → 0.8.1 on-disk compatibility.
//!
//! The fixtures in `tests/fixtures/turso_0_7_2/` were created by
//! `tools/turso-072-fixture-gen` against crates.io `turso = 0.7.2`. This test
//! opens them with the workspace's 0.8.1 engine — the same `Builder::new_local`
//! the accelerator, dataset checkpoint, and Cayenne Turso metastore use — and
//! checks the rows. That is the end-to-end proof a format-constant comparison
//! is not.
//!
//! Regenerating the fixtures: `cargo run --release` in
//! `tools/turso-072-fixture-gen` (that crate is excluded from the workspace so
//! cargo does not unify `turso` to 0.8.1).

#![cfg(feature = "turso")]
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cayenne::{CayenneCatalog, MetadataCatalog};
use tempfile::TempDir;
use turso::{Builder, Connection, Database, Value};

const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/turso_0_7_2");
const MANIFEST: &str = include_str!("fixtures/turso_0_7_2/MANIFEST.txt");

struct Opened {
    _db: Database,
    conn: Connection,
}

#[tokio::test]
async fn turso_0_8_1_reads_rows_written_by_0_7_2() {
    let expected = parse_manifest();
    assert_eq!(
        expected["turso_version"], "0.7.2",
        "fixtures must be generated with Turso 0.7.2"
    );

    let tmp = TempDir::new().expect("temp dir for a writable copy of the fixtures");

    assert_accelerator(
        &materialize(&tmp, "accelerator-mvcc"),
        &expected,
        "accelerator-mvcc",
    )
    .await;
    assert_accelerator(
        &materialize(&tmp, "accelerator-checkpointed"),
        &expected,
        "accelerator-checkpointed",
    )
    .await;
    assert_checkpoint(&materialize(&tmp, "checkpoint"), &expected).await;
    assert_cayenne_metastore(&materialize(&tmp, "cayenne-metastore"), &expected).await;
}

/// Copy `{stem}*.fixture` into `tmp` as `{stem}*` (strip the suffix the
/// `.gitignore` of `*.db` / `*.db-log` forces on the checked-in copies).
fn materialize(tmp: &TempDir, stem: &str) -> PathBuf {
    let dest_dir = tmp.path().join(stem);
    std::fs::create_dir_all(&dest_dir).expect("create per-fixture dir");
    let mut copied = 0_usize;
    for entry in std::fs::read_dir(FIXTURE_DIR).expect("read fixture dir") {
        let entry = entry.expect("fixture dirent");
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(rest) = name.strip_prefix(stem) else {
            continue;
        };
        let Some(rest) = rest.strip_suffix(".fixture") else {
            continue;
        };
        let dest = dest_dir.join(format!("{stem}{rest}"));
        std::fs::copy(entry.path(), &dest).expect("copy fixture sidecar");
        copied += 1;
    }
    assert!(
        copied > 0,
        "no 0.7.2 fixture files for '{stem}' in {FIXTURE_DIR}"
    );
    dest_dir.join(format!("{stem}.db"))
}

async fn open(path: &Path) -> Opened {
    let db = Builder::new_local(path.to_str().expect("utf-8 path"))
        .build()
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Turso 0.8.1 failed to open 0.7.2 file {}: {error}",
                path.display()
            )
        });
    let conn = db.connect().unwrap_or_else(|error| {
        panic!(
            "Turso 0.8.1 failed to connect to 0.7.2 file {}: {error}",
            path.display()
        )
    });
    Opened { _db: db, conn }
}

async fn assert_accelerator(path: &Path, expected: &HashMap<String, String>, label: &str) {
    let opened = open(path).await;
    let count = query_i64(&opened.conn, "SELECT COUNT(*) FROM accelerated").await;
    let expected_count: i64 = expected["accelerator_row_count"]
        .parse()
        .expect("accelerator_row_count");
    assert_eq!(count, expected_count, "{label}: row count written by 0.7.2");

    let sum = query_i64(&opened.conn, "SELECT SUM(value) FROM accelerated").await;
    let expected_sum: i64 = expected["accelerator_sum_value"]
        .parse()
        .expect("accelerator_sum_value");
    assert_eq!(sum, expected_sum, "{label}: SUM(value) written by 0.7.2");

    let names = query_text_column(&opened.conn, "SELECT name FROM accelerated ORDER BY id").await;
    let expected_names: Vec<&str> = expected["accelerator_names"].split(',').collect();
    assert_eq!(
        names, expected_names,
        "{label}: name column written by 0.7.2"
    );

    let note = query_optional_text(&opened.conn, "SELECT note FROM accelerated WHERE id = 2").await;
    assert_eq!(
        note.as_deref(),
        Some(expected["accelerator_note"].as_str()),
        "{label}: utf-8 note on row 2"
    );

    opened
        .conn
        .execute(
            "INSERT INTO accelerated (id, name, value, note) VALUES (99, 'written-by-0.8.1', 99, NULL)",
            (),
        )
        .await
        .expect("0.8.1 can write after opening a 0.7.2 accelerator file");
    let written =
        query_text_column(&opened.conn, "SELECT name FROM accelerated WHERE id = 99").await;
    assert_eq!(
        written,
        ["written-by-0.8.1"],
        "{label}: row written by 0.8.1 is visible"
    );
}

async fn assert_checkpoint(path: &Path, expected: &HashMap<String, String>) {
    let opened = open(path).await;
    let dataset = query_text_column(
        &opened.conn,
        "SELECT dataset_name FROM spice_sys_dataset_checkpoint",
    )
    .await;
    assert_eq!(
        dataset,
        [expected["checkpoint_dataset"].as_str()],
        "checkpoint dataset_name written by 0.7.2"
    );
    let refresh = query_text_column(
        &opened.conn,
        "SELECT refresh_sql FROM spice_sys_dataset_checkpoint",
    )
    .await;
    assert_eq!(
        refresh,
        [expected["checkpoint_refresh_sql"].as_str()],
        "checkpoint refresh_sql written by 0.7.2"
    );
}

async fn assert_cayenne_metastore(path: &Path, expected: &HashMap<String, String>) {
    let opened = open(path).await;
    let table_name = query_text_column(
        &opened.conn,
        "SELECT table_name FROM cayenne_table WHERE table_id = '07200000-0000-7000-8000-000000000001'",
    )
    .await;
    assert_eq!(
        table_name,
        [expected["cayenne_table_name"].as_str()],
        "cayenne_table.table_name written by 0.7.2"
    );
    let snapshot = query_text_column(
        &opened.conn,
        "SELECT current_snapshot_id FROM cayenne_table WHERE table_id = '07200000-0000-7000-8000-000000000001'",
    )
    .await;
    assert_eq!(
        snapshot,
        [expected["cayenne_snapshot_id"].as_str()],
        "cayenne_table.current_snapshot_id written by 0.7.2"
    );
    let sequence = query_i64(
        &opened.conn,
        "SELECT current_sequence_number FROM cayenne_table WHERE table_id = '07200000-0000-7000-8000-000000000001'",
    )
    .await;
    let expected_sequence: i64 = expected["cayenne_sequence"]
        .parse()
        .expect("cayenne_sequence");
    assert_eq!(
        sequence, expected_sequence,
        "cayenne_table.current_sequence_number written by 0.7.2"
    );

    let mut blob_rows = opened
        .conn
        .query(
            "SELECT data_ipc, record_count FROM cayenne_inlined_data \
             WHERE inlined_id = '07200000-0000-7000-8000-000000000003'",
            (),
        )
        .await
        .expect("query cayenne_inlined_data");
    let blob_row = blob_rows
        .next()
        .await
        .expect("step cayenne_inlined_data")
        .expect("cayenne_inlined_data row written by 0.7.2");
    match blob_row.get_value(0).expect("data_ipc") {
        Value::Blob(bytes) => {
            let expected_bytes: Vec<u8> = expected["cayenne_inlined_bytes"]
                .split(',')
                .map(|part| u8::from_str_radix(part, 16).expect("hex byte"))
                .collect();
            assert_eq!(bytes, expected_bytes, "inlined blob written by 0.7.2");
        }
        other => panic!("expected BLOB data_ipc, got {other:?}"),
    }
    drop(opened);

    // The catalog open path — `init` reads `user_version`, runs migrations,
    // and validates `EXPECTED_TABLES` — must accept a metastore 0.7.2 wrote.
    let catalog = CayenneCatalog::new(format!("libsql://{}", path.display()))
        .expect("construct catalog over the 0.7.2 metastore");
    catalog
        .init()
        .await
        .expect("CayenneCatalog::init opens a Turso 0.7.2 metastore with 0.8.1");

    let reopened = open(path).await;
    let table_name = query_text_column(
        &reopened.conn,
        "SELECT table_name FROM cayenne_table WHERE table_id = '07200000-0000-7000-8000-000000000001'",
    )
    .await;
    assert_eq!(
        table_name,
        [expected["cayenne_table_name"].as_str()],
        "0.7.2 cayenne_table row survives CayenneCatalog::init on 0.8.1"
    );
}

async fn query_i64(conn: &Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.expect("query");
    let row = rows
        .next()
        .await
        .expect("step")
        .unwrap_or_else(|| panic!("expected a row from {sql}"));
    match row.get_value(0).expect("column 0") {
        Value::Integer(n) => n,
        other => panic!("expected INTEGER from {sql}, got {other:?}"),
    }
}

async fn query_text_column(conn: &Connection, sql: &str) -> Vec<String> {
    let mut rows = conn.query(sql, ()).await.expect("query");
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.expect("step") {
        match row.get_value(0).expect("column 0") {
            Value::Text(text) => values.push(text),
            other => panic!("expected TEXT from {sql}, got {other:?}"),
        }
    }
    values
}

async fn query_optional_text(conn: &Connection, sql: &str) -> Option<String> {
    let mut rows = conn.query(sql, ()).await.expect("query");
    let row = rows
        .next()
        .await
        .expect("step")
        .unwrap_or_else(|| panic!("expected a row from {sql}"));
    match row.get_value(0).expect("column 0") {
        Value::Text(text) => Some(text),
        Value::Null => None,
        other => panic!("expected TEXT or NULL from {sql}, got {other:?}"),
    }
}

fn parse_manifest() -> HashMap<String, String> {
    MANIFEST
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (key, value) = line
                .split_once('=')
                .unwrap_or_else(|| panic!("MANIFEST line must be key=value, got {line}"));
            (key.to_string(), value.to_string())
        })
        .collect()
}
