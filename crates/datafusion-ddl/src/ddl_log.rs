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

//! Append-only DDL statement log.
//!
//! Any query-planning or execution phase that applies DDL (e.g. `CREATE TABLE`,
//! `CREATE SCHEMA`, `DROP TABLE`) can record the statement in a [`DdlLog`] so
//! that late-joining nodes can replay the same mutations.
//!
//! Two implementations are provided:
//!
//! - [`InMemoryDdlLog`] — backed by a `tokio::sync::RwLock<Vec<String>>`,
//!   suitable for single-process / test use.
//!
//! Higher-level crates (e.g. `runtime-cluster`) can supply an OCC-backed
//! implementation that persists the log to a shared object store for
//! multi-scheduler clusters.

use std::fmt;

use async_trait::async_trait;
use snafu::Snafu;
use tokio::sync::RwLock;

/// Errors returned by [`DdlLog`] operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to append DDL statement to log: {source}"))]
    AppendFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
        statement: String,
    },

    #[snafu(display("Failed to read DDL log snapshot: {source}"))]
    SnapshotFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to read DDL log statements since version {since_version}: {source}"))]
    ReadSinceFailed {
        since_version: u64,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Append-only log of DDL SQL statements.
///
/// Implementations must be safe to share across tasks (`Send + Sync`).
/// The *version* is defined as the count of statements in the log; callers
/// use it to request incremental catch-up via [`statements_since`](DdlLog::statements_since).
#[async_trait]
pub trait DdlLog: Send + Sync + fmt::Debug {
    /// Append a DDL SQL statement to the log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AppendFailed`] if the underlying persistence fails.
    async fn append(&self, sql: String) -> Result<()>;

    /// Returns all statements and the current version (statement count).
    ///
    /// # Errors
    ///
    /// Returns [`Error::SnapshotFailed`] if the underlying read fails.
    async fn snapshot(&self) -> Result<(Vec<String>, u64)>;

    /// Returns statements appended after `since_version`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadSinceFailed`] if the underlying read fails.
    async fn statements_since(&self, since_version: u64) -> Result<Vec<String>>;
}

// ── In-memory implementation ──────────────────────────────────────────────────

/// A [`DdlLog`] backed by an in-memory `Vec<String>`.
///
/// Useful for single-process runtimes and tests. Not shared across
/// processes — use the OCC-backed implementation in `runtime-cluster`
/// for multi-scheduler clusters.
#[derive(Debug, Default)]
pub struct InMemoryDdlLog {
    statements: RwLock<Vec<String>>,
}

impl InMemoryDdlLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DdlLog for InMemoryDdlLog {
    async fn append(&self, sql: String) -> Result<()> {
        self.statements.write().await.push(sql);
        Ok(())
    }

    async fn snapshot(&self) -> Result<(Vec<String>, u64)> {
        let guard = self.statements.read().await;
        let version = u64::try_from(guard.len()).unwrap_or(u64::MAX);
        Ok((guard.clone(), version))
    }

    async fn statements_since(&self, since_version: u64) -> Result<Vec<String>> {
        let guard = self.statements.read().await;
        let idx = usize::try_from(since_version).unwrap_or(usize::MAX);
        if idx >= guard.len() {
            Ok(Vec::new())
        } else {
            Ok(guard[idx..].to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_log() {
        let log = InMemoryDdlLog::new();
        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert!(stmts.is_empty());
        assert_eq!(version, 0);
        assert!(log.statements_since(0).await.expect("since").is_empty());
    }

    #[tokio::test]
    async fn append_and_snapshot() {
        let log = InMemoryDdlLog::new();
        log.append("CREATE SCHEMA IF NOT EXISTS \"cat\".\"s1\"".to_string())
            .await
            .expect("append");
        log.append(
            "CREATE TABLE IF NOT EXISTS \"cat\".\"s1\".\"t1\" (id BIGINT NOT NULL)".to_string(),
        )
        .await
        .expect("append");

        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert_eq!(version, 2);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE SCHEMA"));
        assert!(stmts[1].contains("CREATE TABLE"));
    }

    #[tokio::test]
    async fn statements_since() {
        let log = InMemoryDdlLog::new();
        log.append("stmt0".to_string()).await.expect("append");
        log.append("stmt1".to_string()).await.expect("append");
        log.append("stmt2".to_string()).await.expect("append");

        assert_eq!(
            log.statements_since(0).await.expect("since"),
            vec!["stmt0", "stmt1", "stmt2"]
        );
        assert_eq!(
            log.statements_since(1).await.expect("since"),
            vec!["stmt1", "stmt2"]
        );
        assert_eq!(log.statements_since(2).await.expect("since"), vec!["stmt2"]);
        assert!(log.statements_since(3).await.expect("since").is_empty());
        assert!(log.statements_since(100).await.expect("since").is_empty());
    }
}
