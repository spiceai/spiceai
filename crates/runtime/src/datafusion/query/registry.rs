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

//! Registry of active synchronous queries for a [`DataFusion`](crate::datafusion::DataFusion)
//! instance, keyed by query id.
//!
//! The registry provides a single place for administrative cancel operations
//! (HTTP `/v1/sql/{id}/cancel`, custom Flight action `CancelQuery` with a
//! JSON body) to look up a running query's [`CancellationToken`] and signal
//! cancellation.
//!
//! Each entry is installed when a query begins execution and removed when the
//! query completes via a RAII [`ActiveQueryGuard`]. Entries carry the
//! cancellation token plus lightweight metadata to support listing.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use runtime_request_context::Protocol;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Lightweight snapshot of an active query, suitable for listing via admin APIs.
#[derive(Clone, Debug)]
pub struct ActiveQueryInfo {
    pub query_id: Uuid,
    pub sql_preview: Arc<str>,
    pub protocol: Protocol,
    pub started_at_ms: u64,
}

struct ActiveQueryEntry {
    info: ActiveQueryInfo,
    token: CancellationToken,
}

/// Registry of active synchronous queries for administrative cancellation.
///
/// Uses [`DashMap`] for lock-free concurrent access since the registry is
/// touched on every query start/finish and is looked up by concurrent admin
/// cancel requests.
pub struct QueryCancelRegistry {
    entries: DashMap<Uuid, ActiveQueryEntry>,
}

impl QueryCancelRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Registers a new active query. The returned guard removes the entry when
    /// dropped, regardless of whether the query completed successfully.
    #[must_use]
    pub fn register(
        self: &Arc<Self>,
        query_id: Uuid,
        sql_preview: Arc<str>,
        protocol: Protocol,
        token: CancellationToken,
    ) -> ActiveQueryGuard {
        let started_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.entries.insert(
            query_id,
            ActiveQueryEntry {
                info: ActiveQueryInfo {
                    query_id,
                    sql_preview,
                    protocol,
                    started_at_ms,
                },
                token,
            },
        );
        ActiveQueryGuard {
            registry: Arc::clone(self),
            query_id,
        }
    }

    /// Cancels the active query with the given id, if present. Returns `true`
    /// if an entry existed and was signalled.
    #[must_use]
    pub fn cancel(&self, query_id: Uuid) -> bool {
        if let Some(entry) = self.entries.get(&query_id) {
            entry.token.cancel();
            true
        } else {
            false
        }
    }

    /// Cancels every active query in this registry. Returns the number of
    /// entries that were signalled.
    #[must_use]
    pub fn cancel_all(&self) -> usize {
        let mut cancelled = 0;
        for entry in &self.entries {
            entry.value().token.cancel();
            cancelled += 1;
        }
        cancelled
    }

    /// Returns a snapshot of all active queries.
    #[must_use]
    pub fn list(&self) -> Vec<ActiveQueryInfo> {
        self.entries
            .iter()
            .map(|e| e.value().info.clone())
            .collect()
    }

    /// Returns the number of active queries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&self, query_id: Uuid) {
        self.entries.remove(&query_id);
    }
}

impl Default for QueryCancelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for QueryCancelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCancelRegistry")
            .field("active_queries", &self.entries.len())
            .finish()
    }
}

/// RAII guard that removes the associated query from the registry on drop.
pub struct ActiveQueryGuard {
    registry: Arc<QueryCancelRegistry>,
    query_id: Uuid,
}

impl ActiveQueryGuard {
    #[must_use]
    pub fn query_id(&self) -> Uuid {
        self.query_id
    }
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        self.registry.remove(self.query_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_cancel_roundtrip() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let token = CancellationToken::new();
        let query_id = Uuid::new_v4();

        let _guard = registry.register(
            query_id,
            Arc::from("SELECT 1"),
            Protocol::Http,
            token.clone(),
        );

        assert_eq!(registry.len(), 1);
        assert!(registry.cancel(query_id));
        assert!(token.is_cancelled());
    }

    #[test]
    fn guard_removes_entry_on_drop() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let query_id = Uuid::new_v4();
        {
            let _guard = registry.register(
                query_id,
                Arc::from("SELECT 1"),
                Protocol::Http,
                CancellationToken::new(),
            );
            assert_eq!(registry.len(), 1);
        }
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn cancel_missing_returns_false() {
        let registry = Arc::new(QueryCancelRegistry::new());
        assert!(!registry.cancel(Uuid::new_v4()));
    }

    #[test]
    fn cancel_all_signals_every_entry() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let first = CancellationToken::new();
        let second = CancellationToken::new();

        let _first_guard = registry.register(
            Uuid::new_v4(),
            Arc::from("SELECT 1"),
            Protocol::Http,
            first.clone(),
        );
        let _second_guard = registry.register(
            Uuid::new_v4(),
            Arc::from("SELECT 2"),
            Protocol::Flight,
            second.clone(),
        );

        assert_eq!(registry.cancel_all(), 2);
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
    }
}
