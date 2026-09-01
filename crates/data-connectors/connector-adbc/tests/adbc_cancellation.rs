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

//! A query nobody is reading any more must stop, and give its pooled connection back.
//!
//! This is the repo-side guard for two fork patches that a re-cut can drop
//! silently, because losing either compiles and simply stops cancelling
//! ([#13781](https://github.com/spiceai/spiceai/issues/13781)):
//!
//! * `spiceai/arrow-adbc` — `AdbcStatementCancel` must not be serialized behind
//!   the statement call it exists to interrupt.
//! * `spiceai/datafusion-table-providers` — dropping the record-batch stream
//!   must cancel the statement, so the blocking thread leaves
//!   `Statement::execute` and releases its pooled connection.
//!
//! An ADBC driver is built here rather than loaded: its `StatementExecuteQuery`
//! blocks until `StatementCancel` releases it, the way a real driver blocks on a
//! remote query. Nothing external is needed, so this runs everywhere.

use std::ffi::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use adbc_core::Driver as _;
use adbc_core::options::AdbcVersion;
use adbc_driver_manager::ManagedDriver;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::FFI_ArrowSchema;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::AdbcConnectionPoolBuilder;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;

type AdbcStatusCode = u8;
const ADBC_STATUS_OK: AdbcStatusCode = 0;
const ADBC_STATUS_INTERNAL: AdbcStatusCode = 9;
const ADBC_STATUS_CANCELLED: AdbcStatusCode = 11;

/// How long the fake driver stays inside `StatementExecuteQuery` before giving
/// up. It is the failure timeout: if the cancel never arrives, the test has to
/// end somehow.
const EXECUTE_GIVE_UP: Duration = Duration::from_secs(60);

static CANCELLED: Mutex<bool> = Mutex::new(false);
static CANCEL_SIGNAL: Condvar = Condvar::new();
static EXECUTING: AtomicBool = AtomicBool::new(false);
static CANCELS: AtomicUsize = AtomicUsize::new(0);

fn reset_driver_state() {
    *CANCELLED
        .lock()
        .expect("the cancel state should be lockable") = false;
    EXECUTING.store(false, Ordering::SeqCst);
    CANCELS.store(0, Ordering::SeqCst);
}

fn wait_until_executing(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !EXECUTING.load(Ordering::SeqCst) {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

unsafe extern "C" fn noop_database(
    _database: *mut adbc_ffi::FFI_AdbcDatabase,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_new(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_init(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _database: *mut adbc_ffi::FFI_AdbcDatabase,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_new(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { (*statement).private_data = 1 as *mut c_void };
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_release(
    statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    unsafe { (*statement).private_data = null_mut() };
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_set_sql_query(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    _query: *const c_char,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

/// Answers the schema without running anything, the way a driver with a dry-run
/// does — otherwise the schema fetch would block instead of the query.
unsafe extern "C" fn statement_execute_schema(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    out: *mut FFI_ArrowSchema,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, false)]);
    match FFI_ArrowSchema::try_from(&schema) {
        Ok(exported) => {
            unsafe { std::ptr::write(out, exported) };
            ADBC_STATUS_OK
        }
        Err(_) => ADBC_STATUS_INTERNAL,
    }
}

/// Blocks like a driver waiting on a remote query, and returns only when
/// cancelled.
unsafe extern "C" fn statement_execute_query(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    _stream: *mut arrow::ffi_stream::FFI_ArrowArrayStream,
    _rows_affected: *mut i64,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    EXECUTING.store(true, Ordering::SeqCst);
    let mut cancelled = CANCELLED
        .lock()
        .expect("the cancel state should be lockable");
    let deadline = Instant::now() + EXECUTE_GIVE_UP;
    while !*cancelled {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            EXECUTING.store(false, Ordering::SeqCst);
            return ADBC_STATUS_INTERNAL;
        }
        let (guard, _) = CANCEL_SIGNAL
            .wait_timeout(cancelled, remaining)
            .expect("the cancel state should be lockable");
        cancelled = guard;
    }
    EXECUTING.store(false, Ordering::SeqCst);
    ADBC_STATUS_CANCELLED
}

unsafe extern "C" fn statement_cancel(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    CANCELS.fetch_add(1, Ordering::SeqCst);
    let mut cancelled = CANCELLED
        .lock()
        .expect("the cancel state should be lockable");
    *cancelled = true;
    CANCEL_SIGNAL.notify_all();
    ADBC_STATUS_OK
}

unsafe extern "C" fn driver_release(
    _driver: *mut adbc_ffi::FFI_AdbcDriver,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn driver_init(
    _version: c_int,
    raw_driver: *mut c_void,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    let driver = raw_driver.cast::<adbc_ffi::FFI_AdbcDriver>();
    unsafe {
        (*driver).release = Some(driver_release);
        (*driver).DatabaseNew = Some(noop_database);
        (*driver).DatabaseInit = Some(noop_database);
        (*driver).DatabaseRelease = Some(noop_database);
        (*driver).ConnectionNew = Some(connection_new);
        (*driver).ConnectionInit = Some(connection_init);
        (*driver).ConnectionRelease = Some(connection_release);
        (*driver).StatementNew = Some(statement_new);
        (*driver).StatementRelease = Some(statement_release);
        (*driver).StatementSetSqlQuery = Some(statement_set_sql_query);
        (*driver).StatementExecuteQuery = Some(statement_execute_query);
        (*driver).StatementExecuteSchema = Some(statement_execute_schema);
        (*driver).StatementCancel = Some(statement_cancel);
    }
    ADBC_STATUS_OK
}

/// The connection a cancelled query was using must be available again, and the
/// driver must have been told to cancel.
///
/// The pool holds one connection, so the second `connect()` can only succeed
/// once the first query has actually released it — which cannot happen while the
/// blocking thread is still inside `StatementExecuteQuery`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_stream_cancels_the_query_and_frees_the_pool_connection() {
    reset_driver_state();

    let init: adbc_ffi::FFI_AdbcDriverInitFunc = driver_init;
    let mut driver =
        ManagedDriver::load_static(&init, AdbcVersion::V110).expect("the driver should load");
    let database = driver
        .new_database()
        .expect("the database should be created");
    let pool = AdbcConnectionPoolBuilder::new(database)
        .with_max_size(Some(1))
        .build()
        .expect("the pool should build");

    let connection = pool
        .connect()
        .await
        .expect("a connection should be available");
    let stream = query_arrow(connection, "SELECT 1".to_string(), None)
        .await
        .expect("the query should start");

    assert!(
        wait_until_executing(Duration::from_secs(20)),
        "the driver never started executing, so nothing was cancelled"
    );

    drop(stream);

    let waited = Instant::now();
    let second = tokio::time::timeout(Duration::from_secs(30), pool.connect())
        .await
        .expect("the pool connection did not come back after the query was abandoned")
        .expect("a connection should be available");
    drop(second);

    assert_eq!(
        CANCELS.load(Ordering::SeqCst),
        1,
        "the abandoned query was not cancelled"
    );
    assert!(
        waited.elapsed() < Duration::from_secs(30),
        "the pool connection took {:?} to come back",
        waited.elapsed()
    );
}
