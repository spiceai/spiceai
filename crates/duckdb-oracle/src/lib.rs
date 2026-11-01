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

//! In-process test oracle backed by an installed `DuckDB` ADBC driver.

use adbc_core::{
    Connection as _, Database as _, Driver as _, LOAD_FLAG_DEFAULT, Statement as _,
    options::{AdbcVersion, OptionDatabase},
};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use arrow::{
    array::{Array, StringArray},
    record_batch::RecordBatch,
};
use snafu::{ResultExt, Snafu};
use std::{path::Path, sync::Mutex};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unable to load DuckDB oracle: {source}. Install libduckdb 1.5.0 or later and set ADBC_DRIVER_PATH to the directory containing duckdb.toml ([Driver] shared = absolute library path, entrypoint = duckdb_adbc_init)."
    ))]
    Load { source: adbc_core::error::Error },
    #[snafu(display("DuckDB oracle operation failed: {source}"))]
    Adbc { source: adbc_core::error::Error },
    #[snafu(display("Unable to read DuckDB oracle Arrow results: {source}"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display(
        "DuckDB oracle requires libduckdb 1.5.0 or later; found {found}. Install a supported libduckdb and set ADBC_DRIVER_PATH to its duckdb.toml manifest directory."
    ))]
    Version { found: String },
    #[snafu(display("DuckDB oracle connection lock was poisoned"))]
    Poisoned,
    #[snafu(display("DuckDB database path is not valid UTF-8: {path:?}"))]
    Path { path: std::path::PathBuf },
}

/// A synchronous connection; queries return the driver's Arrow batches unchanged.
pub struct Connection {
    connection: Mutex<ManagedConnection>,
}

impl Connection {
    /// Open a private in-memory database.
    /// # Errors
    /// Fails if the installed driver is missing, unsupported, or cannot open.
    pub fn open_in_memory() -> Result<Self, Error> {
        Self::open(":memory:")
    }

    /// Open a database file, creating it if necessary.
    /// # Errors
    /// Fails on a non-UTF-8 path, driver load, version, or database error.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let path = path.to_str().ok_or_else(|| Error::Path {
            path: path.to_owned(),
        })?;
        let mut driver = ManagedDriver::load_from_name(
            "duckdb",
            Some(b"duckdb_adbc_init"),
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .context(LoadSnafu)?;
        let database = driver
            .new_database_with_opts([(OptionDatabase::Other("path".into()), path.into())])
            .context(AdbcSnafu)?;
        let connection = database.new_connection().context(AdbcSnafu)?;
        let result = Self {
            connection: Mutex::new(connection),
        };
        let batches = result.query("SELECT version()")?;
        let found = batches
            .first()
            .filter(|b| b.num_rows() == 1 && b.num_columns() == 1)
            .and_then(|b| b.column(0).as_any().downcast_ref::<StringArray>())
            .filter(|a| !a.is_null(0))
            .map_or("unknown", |a| a.value(0));
        let parts: Vec<_> = found
            .trim_start_matches('v')
            .split('-')
            .next()
            .unwrap_or("")
            .split('.')
            .map(str::parse::<u64>)
            .collect();
        if !matches!(parts.as_slice(), [Ok(major), Ok(minor), Ok(revision)] if (*major, *minor, *revision) >= (1, 5, 0))
        {
            return Err(Error::Version {
                found: found.into(),
            });
        }
        Ok(result)
    }

    /// Execute SQL, including multi-statement setup scripts.
    /// # Errors
    /// Returns driver errors, including SQL errors.
    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let mut connection = self.connection.lock().map_err(|_| Error::Poisoned)?;
        let mut statement = connection.new_statement().context(AdbcSnafu)?;
        statement.set_sql_query(sql).context(AdbcSnafu)?;
        statement.execute_update().context(AdbcSnafu)?;
        Ok(())
    }

    /// Query SQL and collect Arrow batches through the C Data Interface.
    /// # Errors
    /// Returns SQL, driver, or Arrow stream errors without dropping failed batches.
    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, Error> {
        let mut connection = self.connection.lock().map_err(|_| Error::Poisoned)?;
        let mut statement = connection.new_statement().context(AdbcSnafu)?;
        statement.set_sql_query(sql).context(AdbcSnafu)?;
        statement
            .execute()
            .context(AdbcSnafu)?
            .collect::<Result<Vec<_>, _>>()
            .context(ArrowSnafu)
    }
}
