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

//! Measures whether an in-flight ADBC query can be interrupted at all.
//!
//! A Flight client that goes away drops the `DataFusion` stream, which drops the
//! ADBC result stream — but the query itself runs inside a blocking
//! `Statement::execute` FFI call that owns the pooled connection until it
//! returns. Stopping the remote query, and getting that connection back, is
//! therefore possible only if some ADBC call interrupts an `execute` that is
//! already running. These tests drive a real driver directly, with no runtime in
//! the way, so what they report is a fact about the ADBC binding and the driver
//! rather than about Spice.
//!
//! Two constraints they exist to keep visible:
//!
//! * `adbc_driver_manager::ManagedStatement` takes the same
//!   `Mutex<FFI_AdbcStatement>` in `cancel` and in `execute`, and `execute` holds
//!   it across the whole FFI call — so `cancel` is serialized behind the call it
//!   is meant to interrupt, even though the ADBC C API specifies
//!   `AdbcStatementCancel` as callable during `AdbcStatementExecuteQuery`.
//! * `Connection::cancel` takes a different lock and so can be called
//!   concurrently, but a driver is free to scope it to the connection rather than
//!   to the statement, in which case it does not end the query.
//!
//! Either test turning into a fast, effective cancel is the signal that
//! cancellation can be propagated to the ADBC layer
//! ([#13781](https://github.com/spiceai/spiceai/issues/13781)).
//!
//! Opt-in. It needs a driver and a query that runs long enough to interrupt:
//!
//! ```text
//! ADBC_CANCEL_DRIVER_PATH=/path/to/libadbc_driver_bigquery.dylib \
//! ADBC_CANCEL_URI='bigquery:///<project>?DatasetId=<dataset>' \
//! ADBC_CANCEL_DRIVER_OPTIONS='adbc.bigquery.sql.auth_type=...;adbc.bigquery.sql.auth_credentials=...' \
//! ADBC_CANCEL_SQL='SELECT * FROM `slow_view`' \
//!   cargo test -p connector-adbc --test adbc_cancellation_boundary -- --nocapture
//! ```
//!
//! Any ADBC driver works, given SQL slow enough to interrupt — a recursive CTE
//! counting to a few hundred takes a minute or so on most engines and reads no
//! data. Without `ADBC_CANCEL_DRIVER_PATH` the test reports that it did not run
//! rather than pretending to have proved something.

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection as _, Database as _, Driver as _, LOAD_FLAG_DEFAULT, Statement as _};
use adbc_driver_manager::ManagedDriver;
use std::time::{Duration, Instant};

struct Config {
    driver_path: String,
    uri: String,
    driver_options: Vec<(String, String)>,
    sql: String,
    wait_before_cancel: Duration,
}

/// Reads the configuration, or reports why the test cannot run.
///
/// `Ok(None)` means the driver path is unset, which is the documented way to
/// leave the test out of a run. `Err` means it was asked to run and could not.
fn config() -> Result<Option<Config>, String> {
    let Ok(driver_path) = std::env::var("ADBC_CANCEL_DRIVER_PATH") else {
        return Ok(None);
    };
    let uri =
        std::env::var("ADBC_CANCEL_URI").map_err(|_| "ADBC_CANCEL_URI must be set".to_string())?;
    let sql =
        std::env::var("ADBC_CANCEL_SQL").map_err(|_| "ADBC_CANCEL_SQL must be set".to_string())?;

    let mut driver_options = Vec::new();
    for pair in std::env::var("ADBC_CANCEL_DRIVER_OPTIONS")
        .unwrap_or_default()
        .split(';')
        .filter(|pair| !pair.trim().is_empty())
    {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| "ADBC_CANCEL_DRIVER_OPTIONS entries must be key=value".to_string())?;
        driver_options.push((key.trim().to_string(), value.trim().to_string()));
    }

    let wait_before_cancel = Duration::from_secs(
        std::env::var("ADBC_CANCEL_WAIT_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8),
    );
    Ok(Some(Config {
        driver_path,
        uri,
        driver_options,
        sql,
        wait_before_cancel,
    }))
}

/// Appends a unique trailing comment to the query.
///
/// A database that caches results by query text — BigQuery does — would
/// otherwise answer the second test in a run from the first one's cached result,
/// and a query that returns instantly cannot be interrupted.
fn uncached(sql: &str, tag: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("{sql}\n-- adbc-cancellation-boundary {tag} {nonce}")
}

fn connect(
    config: &Config,
) -> Result<adbc_driver_manager::ManagedConnection, adbc_core::error::Error> {
    let mut driver = ManagedDriver::load_from_name(
        &config.driver_path,
        None,
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )?;

    let mut options: Vec<(OptionDatabase, adbc_core::options::OptionValue)> =
        vec![(OptionDatabase::Uri, config.uri.as_str().into())];
    for (key, value) in &config.driver_options {
        let key = if key.starts_with("adbc.") {
            key.clone()
        } else {
            format!("adbc.{key}")
        };
        options.push((OptionDatabase::Other(key), value.as_str().into()));
    }

    driver.new_database_with_opts(options)?.new_connection()
}

/// `Statement::cancel` is the only ADBC call that claims to interrupt a running
/// query. This measures how long it takes to return while `Statement::execute`
/// is in flight on another thread.
#[test]
fn statement_cancel_during_execute() {
    let Some(config) = config().expect("the test configuration should be usable") else {
        eprintln!("SKIPPED: set ADBC_CANCEL_DRIVER_PATH to run this test");
        return;
    };

    let mut connection = connect(&config).expect("the ADBC connection should be established");
    let mut statement = connection
        .new_statement()
        .expect("the ADBC statement should be allocated");
    statement
        .set_sql_query(uncached(&config.sql, "statement"))
        .expect("the query should be set");

    let mut canceller = statement.clone();
    let started = Instant::now();
    let executor = std::thread::spawn(move || {
        let result = statement.execute();
        (
            started.elapsed(),
            result.map(|_| ()).map_err(|error| error.to_string()),
        )
    });

    std::thread::sleep(config.wait_before_cancel);
    let cancel_started = Instant::now();
    let cancel_result = canceller.cancel();
    let cancel_took = cancel_started.elapsed();

    let (execute_took, execute_outcome) = executor
        .join()
        .expect("the executing thread should not panic");

    println!("statement.cancel returned after {cancel_took:?}: {cancel_result:?}");
    println!("statement.execute returned after {execute_took:?}: {execute_outcome:?}");
    println!(
        "cancel_blocked_until_execute_finished={}",
        cancel_took.as_secs_f64() > 1.0
    );
}

/// `Connection::cancel` takes a different lock than `Statement::execute`, so it
/// can at least be *called* while a query runs. This measures whether calling it
/// actually ends the query.
#[test]
fn connection_cancel_during_execute() {
    let Some(config) = config().expect("the test configuration should be usable") else {
        eprintln!("SKIPPED: set ADBC_CANCEL_DRIVER_PATH to run this test");
        return;
    };

    let mut connection = connect(&config).expect("the ADBC connection should be established");
    let mut statement = connection
        .new_statement()
        .expect("the ADBC statement should be allocated");
    statement
        .set_sql_query(uncached(&config.sql, "connection"))
        .expect("the query should be set");

    let mut canceller = connection.clone();
    let started = Instant::now();
    let executor = std::thread::spawn(move || {
        let result = statement.execute();
        (
            started.elapsed(),
            result.err().map(|error| error.to_string()),
        )
    });

    std::thread::sleep(config.wait_before_cancel);
    let cancel_started = Instant::now();
    let cancel_result = canceller.cancel();
    let cancel_took = cancel_started.elapsed();

    let (execute_took, execute_error) = executor
        .join()
        .expect("the executing thread should not panic");

    println!("connection.cancel returned after {cancel_took:?}: {cancel_result:?}");
    println!("statement.execute returned after {execute_took:?}, error={execute_error:?}");
}
