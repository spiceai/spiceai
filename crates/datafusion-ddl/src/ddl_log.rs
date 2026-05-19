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
//! - [`InMemoryDdlLog`] — backed by a `tokio::sync::RwLock<Vec<DdlStatement>>`,
//!   suitable for single-process / test use.
//!
//! Higher-level crates (e.g. `runtime-cluster`) can supply an OCC-backed
//! implementation that persists the log to a shared object store for
//! multi-scheduler clusters.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::sync::RwLock;

use crate::handler::{CreateSchemaParams, CreateTableParams, DropTableParams};

/// A structured DDL statement stored in the cluster DDL log.
///
/// Each variant corresponds to a DDL operation that can be replayed on
/// late-joining nodes. `CreateTable` additionally stores the original SQL
/// string so it can be replayed directly without reconstructing it from params.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DdlStatement {
    CreateTable {
        params: Box<CreateTableParams>,
        sql: String,
    },
    DropTable(DropTableParams),
    CreateSchema(CreateSchemaParams),
}

impl DdlStatement {
    /// Convert the statement to SQL for replay on joining nodes.
    ///
    /// `CreateTable` returns the original SQL verbatim (including any
    /// `WITH (...)` / `PARTITION BY` clauses).  Other variants reconstruct
    /// minimal idempotent SQL (`IF EXISTS` / `IF NOT EXISTS`).
    #[must_use]
    pub fn to_sql(&self) -> String {
        match self {
            DdlStatement::CreateTable { sql, .. } => sql.clone(),
            DdlStatement::DropTable(p) => {
                let if_exists = if p.if_exists { " IF EXISTS" } else { "" };
                format!(
                    "DROP TABLE{if_exists} \"{}\".\"{}\".\"{}\"",
                    p.catalog_name, p.schema_name, p.table_name
                )
            }
            DdlStatement::CreateSchema(p) => {
                let if_not_exists = if p.if_not_exists {
                    " IF NOT EXISTS"
                } else {
                    ""
                };
                format!(
                    "CREATE SCHEMA{if_not_exists} \"{}\".\"{}\"",
                    p.catalog_name, p.schema_name
                )
            }
        }
    }
}

/// Errors returned by [`DdlLog`] operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to append DDL statement to log: {source}"))]
    AppendFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
        stmt: DdlStatement,
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

/// Append-only log of DDL statements.
///
/// Implementations must be safe to share across tasks (`Send + Sync`).
/// The *version* is defined as the count of statements in the log; callers
/// use it to request incremental catch-up via [`statements_since`](DdlLog::statements_since).
#[async_trait]
pub trait DdlLog: Send + Sync + fmt::Debug {
    /// Append a DDL statement to the log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AppendFailed`] if the underlying persistence fails.
    async fn append(&self, stmt: DdlStatement) -> Result<()>;

    /// Returns all statements and the current version (statement count).
    ///
    /// # Errors
    ///
    /// Returns [`Error::SnapshotFailed`] if the underlying read fails.
    async fn snapshot(&self) -> Result<(Vec<DdlStatement>, u64)>;

    /// Returns statements appended after `since_version`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadSinceFailed`] if the underlying read fails.
    async fn statements_since(&self, since_version: u64) -> Result<Vec<DdlStatement>>;

    async fn drop_table(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> Result<()> {
        self.append(DdlStatement::DropTable(DropTableParams {
            catalog_name,
            schema_name,
            table_name,
            if_exists: true,
        }))
        .await
    }
    async fn create_schema(&self, catalog_name: String, schema_name: String) -> Result<()> {
        self.append(DdlStatement::CreateSchema(CreateSchemaParams {
            catalog_name,
            schema_name,
            if_not_exists: true,
        }))
        .await
    }
}

// ── In-memory implementation ──────────────────────────────────────────────────

/// A [`DdlLog`] backed by an in-memory `Vec<DdlStatement>`.
///
/// Useful for single-process runtimes and tests. Not shared across
/// processes — use the OCC-backed implementation in `runtime-cluster`
/// for multi-scheduler clusters.
#[derive(Debug, Default)]
pub struct InMemoryDdlLog {
    statements: RwLock<Vec<DdlStatement>>,
}

impl InMemoryDdlLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DdlLog for InMemoryDdlLog {
    async fn append(&self, stmt: DdlStatement) -> Result<()> {
        self.statements.write().await.push(stmt);
        Ok(())
    }

    async fn snapshot(&self) -> Result<(Vec<DdlStatement>, u64)> {
        let guard = self.statements.read().await;
        let version = u64::try_from(guard.len()).unwrap_or(u64::MAX);
        Ok((guard.clone(), version))
    }

    async fn statements_since(&self, since_version: u64) -> Result<Vec<DdlStatement>> {
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
    use crate::handler::CreateSchemaParams;

    fn make_create_schema(catalog: &str, schema: &str) -> DdlStatement {
        DdlStatement::CreateSchema(CreateSchemaParams {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            if_not_exists: true,
        })
    }

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
        log.append(make_create_schema("cat", "s1"))
            .await
            .expect("append");
        log.append(DdlStatement::DropTable(DropTableParams {
            catalog_name: "cat".to_string(),
            schema_name: "s1".to_string(),
            table_name: "t1".to_string(),
            if_exists: true,
        }))
        .await
        .expect("append");

        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert_eq!(version, 2);
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], DdlStatement::CreateSchema(_)));
        assert!(matches!(stmts[1], DdlStatement::DropTable(_)));
    }

    #[tokio::test]
    async fn statements_since() {
        let log = InMemoryDdlLog::new();
        for name in ["s0", "s1", "s2"] {
            log.append(make_create_schema("cat", name))
                .await
                .expect("append");
        }

        assert_eq!(log.statements_since(0).await.expect("since").len(), 3);
        assert_eq!(log.statements_since(1).await.expect("since").len(), 2);
        assert_eq!(log.statements_since(2).await.expect("since").len(), 1);
        assert!(log.statements_since(3).await.expect("since").is_empty());
        assert!(log.statements_since(100).await.expect("since").is_empty());
    }
}
