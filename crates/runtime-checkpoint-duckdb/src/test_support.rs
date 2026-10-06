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

//! A file-backed pool for the store tests.
//!
//! The stores under test only need a `DuckDB` connection, so the tests open one
//! directly rather than standing up a `Runtime` and a `Dataset` to reach the same pool
//! through the accelerator. That keeps them fast and keeps this crate free of a
//! `runtime` dev-dependency.

use std::sync::Arc;

use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use tempfile::TempDir;

/// A pool over a fresh database file, plus the `TempDir` that owns it — hold the
/// directory for the lifetime of the test or the file is removed underneath the pool.
pub(crate) fn temp_pool(name: &str) -> (Arc<DuckDbConnectionPool>, TempDir) {
    let temp_dir = TempDir::new().expect("to create temp dir");
    let db_path = temp_dir.path().join(format!("{name}.db"));
    let pool =
        DuckDbConnectionPool::new_file(&db_path.to_string_lossy(), &duckdb::AccessMode::ReadWrite)
            .expect("to open the test DuckDB pool");
    (Arc::new(pool), temp_dir)
}
