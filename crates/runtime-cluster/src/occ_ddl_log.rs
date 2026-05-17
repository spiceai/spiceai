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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_ddl::ddl_log::{self, DdlLog};

use crate::cluster_state::{ClusterStateStore, MutationOutcome};

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
    async fn append(&self, sql: String) -> ddl_log::Result<()> {
        self.cluster_state
            .mutate(move |state| {
                state.ddl_log.push(sql.clone());
                MutationOutcome::Apply
            })
            .await
            .map_err(|e| ddl_log::Error::AppendFailed {
                source: Box::new(e),
                statement: sql,
            })?;
        Ok(())
    }

    async fn snapshot(&self) -> ddl_log::Result<(Vec<String>, u64)> {
        let state =
            self.cluster_state
                .read()
                .await
                .map_err(|e| ddl_log::Error::SnapshotFailed {
                    source: Box::new(e),
                })?;
        let version = u64::try_from(state.ddl_log.len()).unwrap_or(u64::MAX);
        Ok((state.ddl_log.clone(), version))
    }

    async fn statements_since(&self, since_version: u64) -> ddl_log::Result<Vec<String>> {
        let state =
            self.cluster_state
                .read()
                .await
                .map_err(|e| ddl_log::Error::ReadSinceFailed {
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

    use super::*;

    async fn make_occ_log() -> OccDdlLog {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        OccDdlLog::new(cs)
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
        let log = make_occ_log().await;
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

    #[tokio::test]
    async fn two_writers_both_visible() {
        // Simulates two schedulers appending to the same store.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");

        let log_a = OccDdlLog::new(Arc::clone(&cs));
        let log_b = OccDdlLog::new(cs);

        log_a
            .append("CREATE SCHEMA a".to_string())
            .await
            .expect("a append");
        log_b
            .append("CREATE SCHEMA b".to_string())
            .await
            .expect("b append");

        // Both writers see both statements.
        let (stmts, version) = log_a.snapshot().await.expect("snapshot");
        assert_eq!(version, 2);
        assert_eq!(stmts.len(), 2);
        assert!(stmts.contains(&"CREATE SCHEMA a".to_string()));
        assert!(stmts.contains(&"CREATE SCHEMA b".to_string()));
    }
}
