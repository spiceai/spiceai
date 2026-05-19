/*
Copyright 2026 The Spice.ai OSS Authors

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

//! OCC-backed [`DdlLog`] implementation.
//!
//! Persists DDL statements in the `ddl_log` field of the shared
//! `cluster.json` document via [`ClusterStateStore`]. Safe for
//! multi-scheduler clusters — concurrent appends are serialised by
//! the OCC retry loop.
//!
//! Deduplication: [`append`](OccDdlLog::append) checks whether the statement
//! is already present before pushing. If it is, the OCC write is skipped.

use async_trait::async_trait;
use datafusion_ddl::DdlStatement;
use datafusion_ddl::ddl_log::{DdlLog, Error as DdlLogError, Result as DdlLogResult};
use std::sync::Arc;

use crate::cluster_state::{ClusterStateStore, MutationOutcome};

/// Returns `true` when appending `stmt` would be a no-op given the current log.
///
/// The rules are object-aware:
/// - For `CreateTable` / `DropTable`: find the *last* entry that references the
///   same `(catalog, schema, table)` triple.  If it equals `stmt`, the statement
///   is redundant.  A `CreateTable` that follows a `DropTable` (or vice-versa)
///   is therefore *not* considered redundant, which correctly handles
///   create → drop → create sequences.
/// - For `CreateSchema`: no `DropSchema` variant exists, so any existing
///   identical `CreateSchema` for the same schema is redundant.
fn is_redundant(log: &[DdlStatement], stmt: &DdlStatement) -> bool {
    match stmt {
        DdlStatement::CreateTable { params, .. } => log
            .iter()
            .rev()
            .find(|s| {
                same_table(
                    s,
                    &params.catalog_name,
                    &params.schema_name,
                    &params.table_name,
                )
            })
            .map_or(false, |last| last == stmt),
        DdlStatement::DropTable(p) => log
            .iter()
            .rev()
            .find(|s| same_table(s, &p.catalog_name, &p.schema_name, &p.table_name))
            .map_or(false, |last| last == stmt),
        DdlStatement::CreateSchema(_) => log.contains(stmt),
    }
}

/// Returns `true` if `stmt` references the given `(catalog, schema, table)`.
fn same_table(stmt: &DdlStatement, catalog: &str, schema: &str, table: &str) -> bool {
    match stmt {
        DdlStatement::CreateTable { params, .. } => {
            params.catalog_name == catalog
                && params.schema_name == schema
                && params.table_name == table
        }
        DdlStatement::DropTable(p) => {
            p.catalog_name == catalog && p.schema_name == schema && p.table_name == table
        }
        DdlStatement::CreateSchema(_) => false,
    }
}

/// A [`DdlLog`] that stores statements in the OCC-protected
/// `cluster.json` document shared by all schedulers.
#[derive(Debug)]
pub struct OccDdlLog {
    cluster_state: Arc<ClusterStateStore>,
}

impl OccDdlLog {
    #[must_use]
    pub fn new(cluster_state: Arc<ClusterStateStore>) -> Self {
        Self { cluster_state }
    }
}

#[async_trait]
impl DdlLog for OccDdlLog {
    async fn append(&self, stmt: DdlStatement) -> DdlLogResult<()> {
        let value = stmt.clone();
        self.cluster_state
            .mutate(move |state| {
                if is_redundant(&state.ddl_log, &value) {
                    return MutationOutcome::NoChange;
                }
                state.ddl_log.push(value.clone());
                MutationOutcome::Apply
            })
            .await
            .map_err(|e| DdlLogError::AppendFailed {
                source: Box::new(e),
                stmt,
            })?;
        Ok(())
    }

    async fn snapshot(&self) -> DdlLogResult<(Vec<DdlStatement>, u64)> {
        let state = self
            .cluster_state
            .read()
            .await
            .map_err(|e| DdlLogError::SnapshotFailed {
                source: Box::new(e),
            })?;
        let version = u64::try_from(state.ddl_log.len()).unwrap_or(u64::MAX);
        Ok((state.ddl_log.clone(), version))
    }

    async fn statements_since(&self, since_version: u64) -> DdlLogResult<Vec<DdlStatement>> {
        let state = self
            .cluster_state
            .read()
            .await
            .map_err(|e| DdlLogError::ReadSinceFailed {
                since_version,
                source: Box::new(e),
            })?;
        let idx = usize::try_from(since_version).unwrap_or(usize::MAX);
        if idx >= state.ddl_log.len() {
            Ok(Vec::new())
        } else {
            Ok(state.ddl_log[idx..].to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use datafusion_ddl::handler::CreateSchemaParams;

    use super::*;

    async fn make_occ_log() -> OccDdlLog {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        OccDdlLog::new(cs)
    }

    fn schema_stmt(catalog: &str, schema: &str) -> DdlStatement {
        DdlStatement::CreateSchema(CreateSchemaParams {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            if_not_exists: true,
        })
    }

    #[tokio::test]
    async fn empty_log() {
        let log = make_occ_log().await;
        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert!(stmts.is_empty());
        assert_eq!(version, 0);
        assert!(log.statements_since(0).await.expect("since").is_empty());
    }

    #[tokio::test]
    async fn append_and_snapshot() {
        let log = make_occ_log().await;
        log.append(schema_stmt("cat", "s1")).await.expect("append");
        log.append(DdlStatement::DropTable(
            datafusion_ddl::handler::DropTableParams {
                catalog_name: "cat".to_string(),
                schema_name: "s1".to_string(),
                table_name: "t1".to_string(),
                if_exists: true,
            },
        ))
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
        let log = make_occ_log().await;
        for name in ["s0", "s1", "s2"] {
            log.append(schema_stmt("cat", name)).await.expect("append");
        }

        assert_eq!(log.statements_since(0).await.expect("since").len(), 3);
        assert_eq!(log.statements_since(1).await.expect("since").len(), 2);
        assert_eq!(log.statements_since(2).await.expect("since").len(), 1);
        assert!(log.statements_since(3).await.expect("since").is_empty());
        assert!(log.statements_since(100).await.expect("since").is_empty());
    }

    #[tokio::test]
    async fn deduplication_prevents_double_append() {
        let log = make_occ_log().await;
        log.append(schema_stmt("cat", "s1"))
            .await
            .expect("first append");
        log.append(schema_stmt("cat", "s1"))
            .await
            .expect("second append (dedup)");

        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert_eq!(version, 1, "duplicate should not increase version");
        assert_eq!(stmts.len(), 1);
    }

    fn drop_stmt(catalog: &str, schema: &str, table: &str) -> DdlStatement {
        DdlStatement::DropTable(datafusion_ddl::handler::DropTableParams {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            if_exists: true,
        })
    }

    fn create_table_stmt(catalog: &str, schema: &str, table: &str) -> DdlStatement {
        use arrow::datatypes::Schema;
        use std::sync::Arc;
        DdlStatement::CreateTable {
            params: Box::new(datafusion_ddl::handler::CreateTableParams {
                catalog_name: catalog.to_string(),
                schema_name: schema.to_string(),
                table_name: table.to_string(),
                arrow_schema: Arc::new(Schema::empty()),
                primary_key: vec![],
                extension: datafusion_ddl::CreateTableStatementExtension::default(),
                if_not_exists: true,
                or_replace: false,
                like_source_table: None,
            }),
            sql: format!("CREATE TABLE IF NOT EXISTS \"{catalog}\".\"{schema}\".\"{table}\" ()"),
        }
    }

    #[tokio::test]
    async fn create_drop_create_not_deduplicated() {
        let log = make_occ_log().await;
        let ct = create_table_stmt("cat", "s1", "t1");
        let dt = drop_stmt("cat", "s1", "t1");

        log.append(ct.clone()).await.expect("create");
        log.append(dt.clone()).await.expect("drop");
        // Second create must NOT be deduplicated — the last op was a drop.
        log.append(ct.clone()).await.expect("re-create");

        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert_eq!(version, 3, "create/drop/create should each be recorded");
        assert_eq!(stmts.len(), 3);
    }

    #[tokio::test]
    async fn duplicate_create_table_is_deduplicated() {
        let log = make_occ_log().await;
        let ct = create_table_stmt("cat", "s1", "t1");
        log.append(ct.clone()).await.expect("first create");
        log.append(ct.clone()).await.expect("second create (dedup)");

        let (stmts, version) = log.snapshot().await.expect("snapshot");
        assert_eq!(version, 1, "identical create should not be duplicated");
        assert_eq!(stmts.len(), 1);
    }

    #[tokio::test]
    async fn two_writers_both_visible() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");

        let log_a = OccDdlLog::new(Arc::clone(&cs));
        let log_b = OccDdlLog::new(cs);

        log_a
            .append(schema_stmt("cat", "a"))
            .await
            .expect("a append");
        log_b
            .append(schema_stmt("cat", "b"))
            .await
            .expect("b append");

        let (stmts, version) = log_a.snapshot().await.expect("snapshot");
        assert_eq!(version, 2);
        assert_eq!(stmts.len(), 2);
        assert!(stmts.contains(&schema_stmt("cat", "a")));
        assert!(stmts.contains(&schema_stmt("cat", "b")));
    }
}
