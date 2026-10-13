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

//! chDB — embedded ClickHouse — as a standalone oracle for the suites.
//!
//! Results come back as Parquet, which keeps their types: a count stays an
//! integer, a date a date, a decimal a decimal. Rendering them as CSV and
//! inferring types back would turn `'01'` into `1` and lose the distinction the
//! compare path draws between exact and approximate columns.

use std::path::Path;

use arrow::array::RecordBatch;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::{Session, SessionBuilder};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::dialect::Oracle;
use super::inventory::InventoryEntry;
use super::oracle_lane::OracleEngine;

/// Session settings under which ClickHouse answers the standard's way where its
/// defaults part from it — and from DataFusion. Each is a semantic the suites'
/// queries depend on, not a performance knob.
const STANDARD_SQL_SETTINGS: &[&str] = &[
    // An outer join's unmatched side is NULL rather than the column type's
    // default. Without it TPC-H Q13 counts each order-less customer's `0` key as
    // an order, and the `c_count = 0` group disappears.
    "SET join_use_nulls = 1",
    // `SUM`, `AVG`, `MIN` and `MAX` over no rows are NULL rather than 0.
    "SET aggregate_functions_null_for_empty = 1",
    // A bare `UNION`, `INTERSECT` or `EXCEPT` removes duplicates. ClickHouse's
    // defaults are `ALL` for the latter two and an error for the first.
    "SET union_default_mode = 'DISTINCT'",
    "SET intersect_default_mode = 'DISTINCT'",
    "SET except_default_mode = 'DISTINCT'",
    // `IN` and `NOT IN` treat NULL with three-valued logic.
    "SET transform_null_in = 1",
    // The subtotal rows `ROLLUP` adds carry NULL keys rather than defaults.
    "SET group_by_use_nulls = 1",
    // A name that is both a column and a select-list alias means the column.
    "SET prefer_column_name_to_alias = 1",
    // `CAST` keeps a NULL a NULL instead of failing on it.
    "SET cast_keep_nullable = 1",
    // Timestamps are read and written as the UTC instants Cayenne stores.
    "SET session_timezone = 'UTC'",
];

/// An in-process ClickHouse database holding a suite's tables.
///
/// chDB keeps process-wide state, so a test binary must not run two of these
/// at once; the chDB lanes hold a lock for each one's lifetime.
pub struct ChdbOracle {
    _temp: tempfile::TempDir,
    session: Session,
}

impl ChdbOracle {
    #[must_use]
    pub fn new() -> Self {
        let temp = tempfile::tempdir().expect("chdb data dir");
        let session = SessionBuilder::new()
            .with_data_path(temp.path())
            .with_auto_cleanup(true)
            .build()
            .expect("open a chDB session");
        for setting in STANDARD_SQL_SETTINGS {
            session
                .execute(setting, None)
                .unwrap_or_else(|e| panic!("chDB `{setting}`: {e}"));
        }
        Self {
            _temp: temp,
            session,
        }
    }

    /// Load `{table}.parquet` from `parquet_dir` for each table — the files
    /// Cayenne loads, so both sides compare the same rows.
    pub fn load_parquet_dir(&self, parquet_dir: &Path, tables: &[&str]) {
        for table in tables {
            let path = parquet_dir.join(format!("{table}.parquet"));
            let path = path.to_string_lossy().replace('\'', "\\'");
            self.session
                .execute(
                    &format!(
                        "CREATE TABLE {table} ENGINE = Memory AS SELECT * FROM file('{path}', Parquet)"
                    ),
                    None,
                )
                .unwrap_or_else(|e| panic!("chDB load {table} from {path}: {e}"));
        }
    }
}

impl Default for ChdbOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleEngine for ChdbOracle {
    fn pair(&self) -> &'static str {
        "cayenne-chdb"
    }

    fn dialect(&self) -> Oracle {
        Oracle::ClickHouse
    }

    fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let result = self
            .session
            .execute(sql, Some(&[Arg::OutputFormat(OutputFormat::Parquet)]))
            .map_err(|e| format!("chdb execute: {e}"))?;
        let data = bytes::Bytes::copy_from_slice(result.data_ref());
        if data.is_empty() {
            return Ok(Vec::new());
        }
        ParquetRecordBatchReaderBuilder::try_new(data)
            .and_then(ParquetRecordBatchReaderBuilder::build)
            .map_err(|e| format!("chdb result parquet: {e}"))?
            .map(|batch| batch.map_err(|e| format!("chdb result batch: {e}")))
            .collect()
    }

    fn exclusion(&self, entry: &InventoryEntry) -> Option<&'static str> {
        entry.chdb_exclusion
    }
}
