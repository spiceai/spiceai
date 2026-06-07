// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Shared helpers for the Cayenne-vs-chDB micro-benchmarks.
//!
//! chDB is ClickHouse's in-process OLAP engine (the `chdb-rust` crate, which
//! statically links `libchdb`). It is the canonical embedded-OLAP reference
//! for SCAN / AGGREGATE workloads — the same role DuckDB plays in the
//! `vs_duckdb_*` series — so a chDB lane lets Cayenne be read against both
//! engines on identical data and identical queries (a three-way table).
//!
//! Design constraints that shaped this module:
//!
//! * **Same dataset as `vs_duckdb_*`.** Each `vs_chdb_*` bench includes the
//!   DuckDB harness's `common.rs` for the dataset generators
//!   (`schema` / `make_batch*` / `write_parquet`) and feeds chDB the SAME
//!   on-disk parquet file. All three engines therefore run byte-identical
//!   input — the comparison is apples-to-apples by construction.
//! * **Parquet ingest, not a Rust-side row loader.** chDB reads the parquet
//!   directly via ClickHouse's `file(path, 'Parquet')` table function, then
//!   `INSERT … SELECT`s into an in-session `MergeTree` table. This is the
//!   ClickHouse-native bulk-load path and the analog of DuckDB's
//!   `read_parquet` insert — neither engine pays a per-row Rust marshalling
//!   tax, so the load cost is the engine's, not the harness's.
//! * **One process-global engine.** `libchdb` keeps a single ClickHouse
//!   server instance per process; a second live [`ChdbFixture`] would collide
//!   with the first. Benches therefore build ONE fixture, load it once
//!   outside the timed region, and reuse it across `b.iter` — which also
//!   satisfies Tiger-Style "setup outside the hot loop". Drop the fixture
//!   before constructing the next one (the criterion benches do this between
//!   row-count iterations).
//!
//! Included via `#[path = "vs_chdb_helpers/chdb_common.rs"] mod chdb_common;`
//! from each bench file. Placing the helper inside a subdirectory keeps
//! Cargo's bench auto-discovery from picking it up as a standalone target,
//! mirroring how `vs_duckdb_helpers/common.rs` is wired.

#![allow(dead_code)]
#![allow(clippy::expect_used)]

use std::path::Path;

use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;
use tempfile::TempDir;

/// Stable lane label used in `BenchmarkId`s, matching the `cayenne` / `duckdb`
/// labels emitted by the `vs_duckdb_*` benches so a three-way report lines up.
pub const CHDB_LANE: &str = "chdb";

/// A chDB session backed by a temp data directory, with the comparison data
/// already loaded into a `MergeTree` table named by the caller.
///
/// Holds the [`TempDir`] so the on-disk ClickHouse state is cleaned up when the
/// fixture drops. `auto_cleanup(true)` additionally drops chDB's own session
/// state on `build`/teardown.
pub struct ChdbFixture {
    pub _temp_dir: TempDir,
    pub session: chdb_rust::session::Session,
    pub table_name: String,
}

impl ChdbFixture {
    /// Run a SQL query that returns rows, discarding the row content and
    /// returning the number of rows chDB reported reading. Discarding the
    /// payload keeps the bench focused on engine work, not Rust-side decode —
    /// the same discipline as `duckdb_query_count`. `OutputFormat::CSV` is the
    /// cheapest text encoding chDB offers; the byte payload is never parsed.
    ///
    /// # Panics
    /// Panics (via `expect`) if chDB fails to execute the query — a query that
    /// errors out means the bench is measuring nothing and must fail loudly.
    pub fn query_rows_read(&self, sql: &str) -> u64 {
        let result = self
            .session
            .execute(sql, Some(&[Arg::OutputFormat(OutputFormat::CSV)]))
            .expect("chdb query execute");
        result.rows_read()
    }

    /// Execute a query for its engine-side work and discard the rendered
    /// result entirely, returning only the number of rows chDB emitted into the
    /// output block (parsed from the result's line count). Used by multi-row
    /// shapes (GROUP BY) where the analog DuckDB lane times `duckdb_query_count`
    /// — the timed work is the aggregation, and the returned count lets the
    /// caller assert the right number of groups came back.
    ///
    /// # Panics
    /// Panics if the query fails to execute.
    pub fn query_emit_count(&self, sql: &str) -> usize {
        let result = self
            .session
            .execute(sql, Some(&[Arg::OutputFormat(OutputFormat::CSV)]))
            .expect("chdb query execute");
        // `CSV` (no header) emits exactly one line per output row; an empty
        // result is the empty string → zero lines.
        let text = result.data_utf8_lossy();
        if text.is_empty() {
            0
        } else {
            text.lines().filter(|line| !line.is_empty()).count()
        }
    }

    /// Append `rows` synthetic rows (ids starting at `start_id`) and then force
    /// a MergeTree merge with `OPTIMIZE TABLE … FINAL` — ClickHouse's analog of
    /// a Cayenne compaction pass / a DuckDB `CHECKPOINT`: it re-reads and
    /// re-writes data parts. Used by the scan-under-compaction bench to put the
    /// engine under maintenance pressure between timed scans.
    ///
    /// Returns the `start_id` for the next burst so the caller can advance the
    /// cursor without tracking row counts itself.
    ///
    /// # Panics
    /// Panics if the insert or the `OPTIMIZE` fails — either means the
    /// maintenance pressure the bench claims to apply never happened.
    pub fn append_burst_and_optimize(&self, start_id: i64, rows: usize) -> i64 {
        assert!(rows > 0, "burst must insert at least one row");
        // Generate the burst rows entirely inside ClickHouse via `numbers()` so
        // there is no Rust-side row marshalling on this maintenance path —
        // mirrors how the inserted columns are derived in `common::make_batch`
        // (name = concat('name_', id), value = id * 100).
        let end = start_id + rows as i64;
        self.session
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     SELECT number AS id, concat('name_', toString(number)) AS name, \
                     number * 100 AS value \
                     FROM numbers({start_id}, {rows})",
                    table = self.table_name
                ),
                None,
            )
            .expect("chdb maintenance insert burst");
        self.session
            .execute(
                &format!("OPTIMIZE TABLE {} FINAL", self.table_name),
                Some(&[Arg::OutputFormat(OutputFormat::CSV)]),
            )
            .expect("chdb OPTIMIZE FINAL");
        end
    }

    /// Run a scalar aggregate query (e.g. `SELECT COUNT(*) …`) and return the
    /// single integer it produced, parsed from chDB's `CSV` output. The analog
    /// of `duckdb_query_scalar` — both engines compute the aggregate fully and
    /// hand back one number, so the timed work is the aggregate, not decoding.
    ///
    /// # Panics
    /// Panics if the query fails, returns no row, or the single cell is not a
    /// base-10 integer — each is a sign the bench is not exercising the path it
    /// claims, so a loud failure is correct.
    pub fn query_scalar(&self, sql: &str) -> i64 {
        let result = self
            .session
            .execute(sql, Some(&[Arg::OutputFormat(OutputFormat::CSV)]))
            .expect("chdb scalar execute");
        let text = result.data_utf8().expect("chdb scalar utf8");
        let first_line = text
            .lines()
            .next()
            .expect("chdb scalar produced no output line");
        // A single-column scalar in CSV is just the bare value; strip the
        // surrounding quotes ClickHouse adds to string-typed columns (numeric
        // aggregates are emitted unquoted, but trim defensively).
        first_line
            .trim()
            .trim_matches('"')
            .parse::<i64>()
            .expect("chdb scalar not an integer")
    }
}

/// Build a chDB fixture and bulk-load `parquet_path` into a fresh `MergeTree`
/// table (`id Int64, name String, value Int64`) ordered by `id`.
///
/// The schema mirrors [`common::schema`] exactly: `id`/`value` map to
/// ClickHouse `Int64`, `name` to `String`. `ORDER BY id` matches Cayenne's
/// single-`id` primary key so the on-disk physical ordering is comparable
/// across engines.
///
/// # Panics
/// Panics if the session cannot be built or either DDL/insert statement fails
/// — a fixture that didn't load is unusable and must abort the bench.
pub fn setup_chdb_from_parquet(table_name: &str, parquet_path: &Path) -> ChdbFixture {
    setup_chdb_with_schema(
        table_name,
        parquet_path,
        "id Int64, name String, value Int64",
        "id",
    )
}

/// Build a chDB fixture for the join bench's dim table
/// (`id Int64, region String`), mirroring [`common::dim_schema`].
///
/// # Panics
/// See [`setup_chdb_from_parquet`].
pub fn setup_chdb_dim_from_parquet(table_name: &str, parquet_path: &Path) -> ChdbFixture {
    setup_chdb_with_schema(table_name, parquet_path, "id Int64, region String", "id")
}

/// Fully-parameterized chDB loader: caller supplies the ClickHouse column list
/// and the `ORDER BY` key. Both convenience wrappers route through this.
///
/// # Panics
/// Panics if the session build, `CREATE TABLE`, or `INSERT … SELECT file(...)`
/// fails.
pub fn setup_chdb_with_schema(
    table_name: &str,
    parquet_path: &Path,
    columns: &str,
    order_by: &str,
) -> ChdbFixture {
    let temp_dir = tempfile::tempdir().expect("chdb temp dir");
    let session = SessionBuilder::new()
        .with_data_path(temp_dir.path())
        .with_auto_cleanup(true)
        .build()
        .expect("chdb session build");

    // MergeTree is ClickHouse's columnar storage engine — the apples-to-apples
    // counterpart to a DuckDB table / a Cayenne Vortex table for a scan bench.
    session
        .execute(
            &format!("CREATE TABLE {table_name} ({columns}) ENGINE = MergeTree() ORDER BY {order_by}"),
            None,
        )
        .expect("chdb create table");

    // ClickHouse infers the parquet schema from the file; the column names line
    // up with the table, so positional `SELECT *` lands each column correctly.
    // `file()` reads the SAME parquet the DuckDB lane ingests via read_parquet.
    let parquet_display = parquet_path.to_string_lossy();
    session
        .execute(
            &format!(
                "INSERT INTO {table_name} SELECT * FROM file('{parquet_display}', 'Parquet')"
            ),
            None,
        )
        .expect("chdb insert from parquet");

    ChdbFixture {
        _temp_dir: temp_dir,
        session,
        table_name: table_name.to_string(),
    }
}
