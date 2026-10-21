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

//! The executors each scheduler has connected, published next to the cluster
//! document as `executors/{scheduler_id}.json`. An executor is live while some
//! scheduler that published recently has it connected, so one scheduler that
//! cannot reach an executor does not take the executor's partitions away.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::cluster_state::ClusterStateStore;

/// How long a view outlives the scheduler that wrote it before a reader
/// deletes it.
const VIEW_RETENTION: Duration = Duration::from_hours(1);

/// One scheduler's published view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ConnectedExecutors {
    scheduler_id: String,
    /// When the scheduler wrote the view, by its own clock (milliseconds since
    /// the Unix epoch).
    written_at_ms: u64,
    executors: Vec<String>,
}

/// Reads and writes the schedulers' views of their connected executors.
#[derive(Debug, Clone)]
pub struct ExecutorLiveness {
    store: Arc<dyn ObjectStore>,
    /// The directory the views live in, e.g. `prefix/executors`.
    dir: Path,
}

impl ExecutorLiveness {
    /// The views stored beside `cluster`'s document.
    #[must_use]
    pub fn new(cluster: &ClusterStateStore) -> Self {
        let mut parts: Vec<_> = cluster.path().parts().collect();
        parts.pop();
        parts.push("executors".into());
        Self {
            store: Arc::clone(cluster.object_store()),
            dir: Path::from_iter(parts),
        }
    }

    /// Publishes `executors` as the executors `scheduler_id` has connected at
    /// `now_ms`.
    ///
    /// # Errors
    ///
    /// Returns an error if the object-store write fails.
    pub async fn publish(
        &self,
        scheduler_id: &str,
        mut executors: Vec<String>,
        now_ms: u64,
    ) -> object_store::Result<()> {
        executors.sort_unstable();
        let view = ConnectedExecutors {
            scheduler_id: scheduler_id.to_string(),
            written_at_ms: now_ms,
            executors,
        };
        let payload = serde_json::to_vec(&view).map_err(|source| object_store::Error::Generic {
            store: "executor_liveness",
            source: Box::new(source),
        })?;
        self.store
            .put(
                &self.dir.clone().join(format!("{scheduler_id}.json")),
                PutPayload::from(payload),
            )
            .await
            .map(|_| ())
    }

    /// Every executor in a view written within `freshness` of `now_ms`. A view
    /// that cannot be parsed is skipped; one written more than
    /// [`VIEW_RETENTION`] away from `now_ms` is deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the views cannot be listed or read.
    pub async fn live_executors(
        &self,
        now_ms: u64,
        freshness: Duration,
    ) -> object_store::Result<HashSet<String>> {
        let freshness_ms = u64::try_from(freshness.as_millis()).unwrap_or(u64::MAX);
        let retention_ms = u64::try_from(VIEW_RETENTION.as_millis()).unwrap_or(u64::MAX);
        let objects: Vec<_> = self.store.list(Some(&self.dir)).try_collect().await?;
        let mut live = HashSet::new();
        for object in objects {
            let bytes = match self.store.get(&object.location).await {
                Ok(result) => result.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(err) => return Err(err),
            };
            let Ok(view) = serde_json::from_slice::<ConnectedExecutors>(&bytes) else {
                tracing::debug!(
                    "Skipping an unreadable executor view at {}",
                    object.location
                );
                continue;
            };
            let age_ms = now_ms.abs_diff(view.written_at_ms);
            if age_ms <= freshness_ms {
                live.extend(view.executors);
            } else if age_ms > retention_ms
                && let Err(err) = self.store.delete(&object.location).await
            {
                tracing::debug!(
                    "Failed to delete the old executor view at {}: {err}",
                    object.location
                );
            }
        }
        Ok(live)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn only_recent_views_count() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = ClusterStateStore::new(Arc::clone(&store), "prefix");
        let liveness = ExecutorLiveness::new(&cluster);
        liveness
            .publish("s1", vec!["e1".to_string(), "e2".to_string()], 1_000)
            .await
            .expect("publish");
        liveness
            .publish("s2", vec!["e3".to_string()], 100_000)
            .await
            .expect("publish");

        let live = liveness
            .live_executors(100_000, Duration::from_secs(10))
            .await
            .expect("read");
        assert_eq!(live, HashSet::from(["e3".to_string()]));

        let live = liveness
            .live_executors(100_000, Duration::from_secs(100))
            .await
            .expect("read");
        assert_eq!(
            live,
            HashSet::from(["e1".to_string(), "e2".to_string(), "e3".to_string()])
        );

        // A view written ahead of the reader's clock counts only within the
        // same window.
        let live = liveness
            .live_executors(50_000, Duration::from_secs(10))
            .await
            .expect("read");
        assert!(live.is_empty(), "unexpected live executors: {live:?}");
    }

    /// A view far older than any live scheduler's is deleted.
    #[tokio::test]
    async fn views_past_retention_are_deleted() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = ClusterStateStore::new(Arc::clone(&store), "prefix");
        let liveness = ExecutorLiveness::new(&cluster);
        liveness
            .publish("old", vec!["e1".to_string()], 1_000)
            .await
            .expect("publish");
        liveness
            .publish("new", vec!["e2".to_string()], 1_000 + 3_600_000)
            .await
            .expect("publish");

        let now_ms = 2_000 + 3_600_000;
        let live = liveness
            .live_executors(now_ms, Duration::from_secs(10))
            .await
            .expect("read");
        assert_eq!(live, HashSet::from(["e2".to_string()]));
        let names: Vec<String> = store
            .list(Some(&liveness.dir))
            .map_ok(|object| object.location.filename().unwrap_or_default().to_string())
            .try_collect()
            .await
            .expect("list");
        assert_eq!(names, vec!["new.json".to_string()]);
    }
}
