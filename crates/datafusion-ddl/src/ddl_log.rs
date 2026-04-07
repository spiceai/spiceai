/*
Copyright 2026, Spice AI, Inc.

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

//! Append-only, in-memory DDL log for the scheduler.
//!
//! Records every DDL statement (CREATE TABLE, DROP TABLE, CREATE SCHEMA)
//! successfully executed on the scheduler so that late-joining executors can
//! replay the full log via `ClusterService::GetAppDefinition` and reach the
//! same catalog state as existing nodes.
//!
//! ## TOCTOU safety
//!
//! The log is append-only and protected by a `std::sync::RwLock`.
//! `GetAppDefinition` snapshots the log atomically under a read lock, so no
//! statement committed before the RPC returns is missed.  Any DDL committed
//! *after* the snapshot but *before* the executor completes
//! `AllocateInitialPartitions` is forwarded through the normal
//! `forward_ddl_to_executors` path once the executor's Flight service
//! is registered.

use std::sync::{Arc, RwLock};

/// Append-only, ordered log of DDL SQL statements executed by the scheduler.
#[derive(Debug, Default, Clone)]
pub struct DdlLog {
    statements: Arc<RwLock<Vec<String>>>,
}

impl DdlLog {
    /// Creates a new, empty DDL log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a DDL statement to the log.
    ///
    /// Call this **after** the statement has been successfully applied locally
    /// so the log never contains statements that failed.
    pub fn append(&self, sql: &str) {
        match self.statements.write() {
            Ok(mut stmts) => {
                stmts.push(sql.to_string());
            }
            Err(e) => {
                // Poisoned lock — log a warning but don't panic.  The log may
                // be incomplete; late-joining executors will still get whatever
                // was recorded before the poison.
                eprintln!("DdlLog: failed to acquire write lock: {e}");
            }
        }
    }

    /// Returns a point-in-time snapshot of all statements in the log.
    ///
    /// Cheap: clones the `String`s under a brief read lock.
    #[must_use]
    pub fn snapshot(&self) -> Vec<String> {
        self.statements
            .read()
            .map(|s| s.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddl_log_empty() {
        let log = DdlLog::new();
        assert!(log.snapshot().is_empty());
    }

    #[test]
    fn test_ddl_log_append_and_snapshot() {
        let log = DdlLog::new();
        log.append("CREATE SCHEMA IF NOT EXISTS \"spice\".\"s\"");
        log.append(
            "CREATE TABLE IF NOT EXISTS \"spice\".\"s\".\"t\" \
             (\"id\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
        );

        let snap = log.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(snap[0].contains("CREATE SCHEMA"));
        assert!(snap[1].contains("CREATE TABLE"));
    }

    #[test]
    fn test_ddl_log_snapshot_preserves_order() {
        let log = DdlLog::new();
        for i in 0..5u32 {
            log.append(&format!(
                "CREATE TABLE IF NOT EXISTS \"spice\".\"s\".\"t{i}\" (\"id\" INT)"
            ));
        }
        let snap = log.snapshot();
        assert_eq!(snap.len(), 5);
        for (i, stmt) in snap.iter().enumerate() {
            assert!(stmt.contains(&format!("t{i}")), "expected t{i} at position {i}");
        }
    }
}
