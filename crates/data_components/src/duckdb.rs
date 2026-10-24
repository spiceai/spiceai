/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{Read, ReadWrite};
use async_trait::async_trait;
use datafusion::{common::TableReference, datasource::TableProvider};
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions, create_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Serializes extension installs between the connections of this process.
static EXTENSION_INSTALL: Mutex<()> = Mutex::new(());

/// Installs and loads a `DuckDB` extension, one caller at a time on this host.
///
/// `INSTALL` downloads the extension to a temporary file, deletes the installed
/// copy, and moves the new one into place. Two connections installing the same
/// extension into an empty extension directory therefore delete each other's
/// files: one `INSTALL` fails with `Could not remove file`, or a `LOAD` finds the
/// file it was about to open already gone. A lock held across both statements,
/// in this process and through an advisory lock on a file in the extension
/// directory, which every process installing into it shares, keeps one caller's
/// install and load from overlapping another's.
///
/// # Errors
///
/// Returns a `duckdb::Error` if `INSTALL` or `LOAD` fails.
pub fn install_and_load_extension(
    conn: &duckdb::Connection,
    extension: &'static str,
) -> Result<(), duckdb::Error> {
    let _guard = EXTENSION_INSTALL.lock();
    let _directory_lock = extension_directory(conn).and_then(|dir| lock_extension_directory(&dir));
    conn.execute(&format!("INSTALL {extension}"), [])?;
    conn.execute(&format!("LOAD {extension}"), [])?;
    Ok(())
}

/// The directory `INSTALL` writes into, resolved as `DuckDB` resolves it: the
/// `extension_directory` setting, else `.duckdb/extensions` under the
/// `home_directory` setting or the user's home directory.
fn extension_directory(conn: &duckdb::Connection) -> Option<PathBuf> {
    let setting = |name: &str| -> Option<String> {
        conn.query_row("SELECT current_setting(?)", [name], |row| row.get(0))
            .ok()
            .filter(|value: &String| !value.is_empty())
    };
    let home = || {
        setting("home_directory")
            .map(PathBuf::from)
            .or_else(util::home_dir::home_dir)
    };
    match setting("extension_directory") {
        // `DuckDB` expands a leading `~` by concatenation, whatever separator follows it.
        Some(dir) => match dir.strip_prefix('~') {
            Some(rest) => home().map(|home| {
                let mut expanded = home.into_os_string();
                expanded.push(rest);
                PathBuf::from(expanded)
            }),
            None => Some(PathBuf::from(dir)),
        },
        None => home().map(|home| home.join(".duckdb").join("extensions")),
    }
}

/// Takes the install lock for `dir`, released when the returned file is dropped.
///
/// Returns `None` when the lock file cannot be created or locked, for example in
/// a read-only extension directory; installs are then serialized only within this
/// process.
fn lock_extension_directory(dir: &Path) -> Option<File> {
    let path = dir.join("spice-install.lock");
    create_dir_all(dir)
        .and_then(|()| {
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
        })
        .or_else(|_| File::open(&path))
        .and_then(|file| file.lock().map(|()| file))
        .inspect_err(|e| {
            tracing::debug!(
                "Could not lock {} to serialize DuckDB extension installs across processes: {e}",
                path.display()
            );
        })
        .ok()
}

#[async_trait]
impl Read for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.table_provider(table_reference).await
    }
}

#[async_trait]
impl ReadWrite for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.read_write_table_provider(table_reference).await
    }
}

/// The statement [`with_utc_session_timezone`] applies.
pub const SET_UTC_SESSION_TIMEZONE: &str = "SET TimeZone = 'UTC'";

/// Pin a Spice-opened `DuckDB` connection to `UTC`.
///
/// `DuckDB` labels a `TIMESTAMPTZ` column it exports to Arrow with the connection's own
/// `TimeZone`, so an unpinned session makes a dataset's *schema* depend on the host: the same
/// table reads back as `Timestamp(us, "Asia/Tokyo")` on one machine and `Timestamp(us, "UTC")`
/// on another. `DataFusion` coerces a naive timestamp literal into the column's zone, and
/// Arrow reads that literal as wall-clock in it, so `WHERE ts > TIMESTAMP '2024-01-15 15:00:00'`
/// selects a different set of rows on each host ([#13899](https://github.com/spiceai/spiceai/issues/13899)).
///
/// `TimeZone` is a session setting, so this is applied per connection — the same scope the
/// accelerator uses for it (`accelerator-duckdb`'s `settings::TimeZone`,
/// `DuckDBSettingScope::Local`). Existing setup queries are kept, and `connect_sync` runs them
/// in order, so a caller that set its own zone is overridden rather than dropped.
#[must_use]
pub fn with_utc_session_timezone(pool: DuckDbConnectionPool) -> DuckDbConnectionPool {
    let mut queries = pool.connection_setup_queries().to_vec();
    queries.push(Arc::from(SET_UTC_SESSION_TIMEZONE));
    pool.with_connection_setup_queries(queries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;

    /// Read `TimeZone` back off a connection the pool hands out — the setup queries run on
    /// `connect_sync`, so a live connection is the only place their effect is observable.
    fn session_timezone(pool: DuckDbConnectionPool) -> String {
        let mut conn = Arc::new(pool).connect_sync().expect("connect to DuckDB");
        conn.as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("DuckDB hands out a DuckDB connection")
            .get_underlying_conn_mut()
            .query_row("SELECT current_setting('TimeZone')", [], |row| {
                row.get::<_, String>(0)
            })
            .expect("read the session TimeZone")
    }

    fn seeded_with(query: &str) -> DuckDbConnectionPool {
        DuckDbConnectionPool::new_memory()
            .expect("in-memory DuckDB pool")
            .with_connection_setup_queries(vec![Arc::from(query)])
    }

    /// The pool is seeded with a non-UTC zone on purpose: a CI host already running UTC would
    /// satisfy the second assertion with or without the pin, so the seed is what makes this
    /// measure something. Without [`with_utc_session_timezone`] the connection stays on
    /// `Asia/Tokyo`, and a `TIMESTAMPTZ` column then reaches Arrow labelled with it (#13899).
    #[test]
    fn the_pin_overrides_a_zone_the_session_already_carries() {
        assert_eq!(
            session_timezone(seeded_with("SET TimeZone = 'Asia/Tokyo'")),
            "Asia/Tokyo",
            "the seed has to take effect, or the pinned case below proves nothing"
        );

        assert_eq!(
            session_timezone(with_utc_session_timezone(seeded_with(
                "SET TimeZone = 'Asia/Tokyo'"
            ))),
            "UTC"
        );
    }

    /// `with_connection_setup_queries` replaces the whole list, so the pin has to append to
    /// what a caller already set rather than dropping it — `search`'s `DuckDB` index relies on a
    /// `LOAD vss` setup query, and losing one would be silent.
    #[test]
    fn the_pin_keeps_the_setup_queries_already_on_the_pool() {
        let pinned = with_utc_session_timezone(seeded_with("SET memory_limit = '123MB'"));
        let queries: Vec<&str> = pinned
            .connection_setup_queries()
            .iter()
            .map(AsRef::as_ref)
            .collect();

        assert_eq!(
            queries,
            vec!["SET memory_limit = '123MB'", SET_UTC_SESSION_TIMEZONE]
        );
    }
}
