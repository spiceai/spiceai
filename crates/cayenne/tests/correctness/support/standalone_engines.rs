// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! **Out-of-Spice** reference SQL engines for result-correctness.
//!
//! These engines are **not** Spice accelerators. They are the embedded engine
//! crates (DuckDB, SQLite / `rusqlite`, chDB) used as external oracles:
//!
//! | Engine | Crate | Spice accelerator counterpart |
//! |--------|-------|-------------------------------|
//! | DuckDB | `duckdb` | `runtime::dataaccelerator::duckdb` |
//! | SQLite | `rusqlite` | `runtime::dataaccelerator::sqlite` |
//! | chDB   | `chdb-rust` | (none — ClickHouse-compatible reference only) |
//!
//! Correctness matrix:
//! 1. Standalone engines agree with each other on portable SQL (this module +
//!    `result_correctness_standalone_engines_test`).
//! 2. Spice accelerators (Cayenne, DuckDB accel, SQLite accel) match a
//!    standalone oracle on the same data/SQL (see cayenne `result_correctness_vs_*`
//!    and runtime `acceleration/result_correctness`).

#[cfg(feature = "result-correctness-duckdb")]
use std::path::Path;

#[cfg(feature = "result-correctness-duckdb")]
use arrow::array::RecordBatch;

// --- SQLite (always available via cayenne → rusqlite) ---

#[expect(unused_imports)] // re-exported for integration test crates
pub use super::sqlite_engine::{
    load_sqlite_from_batches, load_sqlite_from_parquet, sqlite_query_batches,
};

/// Human-readable labels for coverage reports / engine pairs.
pub const STANDALONE_DUCKDB: &str = "standalone-duckdb";
pub const STANDALONE_SQLITE: &str = "standalone-sqlite";
pub const STANDALONE_CHDB: &str = "standalone-chdb";
pub const SPICE_CAYENNE: &str = "spice-cayenne";
pub const SPICE_DUCKDB_ACCEL: &str = "spice-duckdb-accel";
pub const SPICE_SQLITE_ACCEL: &str = "spice-sqlite-accel";

// --- DuckDB (feature `result-correctness-duckdb`) ---

/// Load named parquet tables into an in-process DuckDB database (not Spice).
#[cfg(feature = "result-correctness-duckdb")]
pub fn load_duckdb_from_parquet(
    parquet_dir: &Path,
    tables: &[&str],
) -> (tempfile::TempDir, duckdb::Connection) {
    let temp = tempfile::tempdir().expect("duckdb temp");
    let db_path = temp.path().join("parity.duckdb");
    let conn = duckdb::Connection::open(&db_path).expect("duckdb open");
    for table in tables {
        let path = parquet_dir.join(format!("{table}.parquet"));
        conn.execute_batch(&format!(
            "CREATE TABLE {table} AS SELECT * FROM read_parquet('{}');",
            path.display()
        ))
        .unwrap_or_else(|e| panic!("standalone duckdb load {table}: {e}"));
    }
    (temp, conn)
}

/// Execute SQL on standalone DuckDB; return Arrow batches.
#[cfg(feature = "result-correctness-duckdb")]
pub fn duckdb_query_batches(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("duckdb prepare: {e}"))?;
    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .map_err(|e| format!("duckdb query_arrow: {e}"))?
        .collect();
    Ok(batches)
}

/// Load RecordBatches into standalone DuckDB via a temp parquet staging dir.
#[cfg(feature = "result-correctness-duckdb")]
pub fn load_duckdb_from_batches(
    tables: &[(&str, RecordBatch)],
) -> (tempfile::TempDir, duckdb::Connection) {
    let stage = tempfile::tempdir().expect("duckdb stage");
    for (name, batch) in tables {
        let path = stage.path().join(format!("{name}.parquet"));
        super::write_parquet(batch, &path);
    }
    let names: Vec<&str> = tables.iter().map(|(n, _)| *n).collect();
    load_duckdb_from_parquet(stage.path(), &names)
}

// DuckDB helpers above are feature-gated; SQLite re-exports are always available.
