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

//! Registry of active synchronous queries, keyed by query id.
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

const SQL_PREVIEW_MAX_CHARS: usize = 100;
const SQL_PREVIEW_ELLIPSIS: &str = "...";

/// Lightweight snapshot of an active query, suitable for listing via admin APIs.
#[derive(Clone, Debug)]
pub struct ActiveQueryInfo {
    pub query_id: Uuid,
    pub sql_preview: Arc<str>,
    pub protocol: Protocol,
    pub started_at_ms: u64,
    /// The scope that submitted the query: the storage id of the submitting
    /// request's cache namespace. Listing and cancellation are restricted to
    /// this scope, so one principal cannot read another's in-flight SQL or
    /// stop its work.
    pub owner: Arc<str>,
}

struct ActiveQueryEntry {
    info: ActiveQueryInfo,
    token: CancellationToken,
}

/// Registry of active synchronous queries for administrative cancellation.
///
/// Uses [`DashMap`] for sharded-lock concurrent access since the registry is
/// touched on every query start/finish and looked up by concurrent admin cancel
/// requests.
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
        sql: &str,
        protocol: Protocol,
        owner: Arc<str>,
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
                    sql_preview: Self::truncate_sql_preview(sql),
                    protocol,
                    started_at_ms,
                    owner,
                },
                token,
            },
        );
        ActiveQueryGuard {
            registry: Arc::clone(self),
            query_id,
        }
    }

    /// Cancels the active query with the given id when `caller` submitted it.
    /// Returns `true` if a matching entry existed and was signalled.
    ///
    /// A query submitted by another scope reports the same `false` as an id
    /// that does not exist, so a caller cannot probe for other principals'
    /// query ids.
    #[must_use]
    pub fn cancel_owned(&self, query_id: Uuid, caller: &str) -> bool {
        if let Some(entry) = self.entries.get(&query_id)
            && entry.info.owner.as_ref() == caller
        {
            entry.token.cancel();
            return true;
        }
        false
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

    /// Returns a snapshot of the active queries `caller` submitted.
    #[must_use]
    pub fn list_for(&self, caller: &str) -> Vec<ActiveQueryInfo> {
        self.list_matching(|info| info.owner.as_ref() == caller)
    }

    /// Returns a snapshot of every active query regardless of who submitted
    /// it. For internal callers only — request-facing surfaces use
    /// [`Self::list_for`].
    #[must_use]
    pub fn list_all(&self) -> Vec<ActiveQueryInfo> {
        self.list_matching(|_| true)
    }

    fn list_matching(&self, predicate: impl Fn(&ActiveQueryInfo) -> bool) -> Vec<ActiveQueryInfo> {
        let mut entries: Vec<ActiveQueryInfo> = self
            .entries
            .iter()
            .map(|e| e.value().info.clone())
            .filter(|info| predicate(info))
            .collect();
        entries.sort_by(|left, right| {
            left.started_at_ms
                .cmp(&right.started_at_ms)
                .then_with(|| left.query_id.cmp(&right.query_id))
        });
        entries
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

    fn truncate_sql_preview(sql: &str) -> Arc<str> {
        if sql.char_indices().nth(SQL_PREVIEW_MAX_CHARS).is_none() {
            return Arc::from(sql);
        }

        let truncate_chars = SQL_PREVIEW_MAX_CHARS.saturating_sub(SQL_PREVIEW_ELLIPSIS.len());
        let truncate_at = sql
            .char_indices()
            .nth(truncate_chars)
            .map_or(sql.len(), |(index, _)| index);
        let mut preview = String::with_capacity(truncate_at + SQL_PREVIEW_ELLIPSIS.len());
        preview.push_str(&sql[..truncate_at]);
        preview.push_str(SQL_PREVIEW_ELLIPSIS);
        Arc::from(preview)
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

    const OWNER: &str = "apikey:0123456789abcdef";
    const OTHER: &str = "apikey:fedcba9876543210";

    fn owner() -> Arc<str> {
        Arc::from(OWNER)
    }

    #[test]
    fn register_and_cancel_roundtrip() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let token = CancellationToken::new();
        let query_id = Uuid::new_v4();

        let _guard =
            registry.register(query_id, "SELECT 1", Protocol::Http, owner(), token.clone());

        assert_eq!(registry.len(), 1);
        assert!(registry.cancel_owned(query_id, OWNER));
        assert!(token.is_cancelled());
    }

    #[test]
    fn guard_removes_entry_on_drop() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let query_id = Uuid::new_v4();
        {
            let _guard = registry.register(
                query_id,
                "SELECT 1",
                Protocol::Http,
                owner(),
                CancellationToken::new(),
            );
            assert_eq!(registry.len(), 1);
        }
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn cancel_missing_returns_false() {
        let registry = Arc::new(QueryCancelRegistry::new());
        assert!(!registry.cancel_owned(Uuid::new_v4(), OWNER));
    }

    #[test]
    fn cancel_all_signals_every_entry() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let first = CancellationToken::new();
        let second = CancellationToken::new();

        let _first_guard = registry.register(
            Uuid::new_v4(),
            "SELECT 1",
            Protocol::Http,
            owner(),
            first.clone(),
        );
        let _second_guard = registry.register(
            Uuid::new_v4(),
            "SELECT 2",
            Protocol::Flight,
            owner(),
            second.clone(),
        );

        assert_eq!(registry.cancel_all(), 2);
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
    }

    #[test]
    fn list_returns_stable_order() {
        let registry = Arc::new(QueryCancelRegistry::new());

        let _first_guard = registry.register(
            Uuid::from_u128(2),
            "SELECT 2",
            Protocol::Http,
            owner(),
            CancellationToken::new(),
        );
        let _second_guard = registry.register(
            Uuid::from_u128(1),
            "SELECT 1",
            Protocol::Http,
            owner(),
            CancellationToken::new(),
        );

        let listed = registry.list_all();
        assert!(listed.windows(2).all(|window| {
            let left = &window[0];
            let right = &window[1];
            left.started_at_ms < right.started_at_ms
                || (left.started_at_ms == right.started_at_ms && left.query_id <= right.query_id)
        }));
    }

    #[test]
    fn register_stores_truncated_sql_preview() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let long_sql = format!("SELECT '{}'", "x".repeat(160));

        let _guard = registry.register(
            Uuid::new_v4(),
            &long_sql,
            Protocol::Http,
            owner(),
            CancellationToken::new(),
        );

        let preview = registry
            .list_all()
            .pop()
            .expect("registered query should be listed")
            .sql_preview
            .to_string();
        assert_eq!(preview.chars().count(), SQL_PREVIEW_MAX_CHARS);
        assert!(preview.ends_with(SQL_PREVIEW_ELLIPSIS));
        assert!(long_sql.starts_with(preview.trim_end_matches(SQL_PREVIEW_ELLIPSIS)));
    }

    #[test]
    fn cancel_owned_refuses_a_query_another_scope_submitted() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let token = CancellationToken::new();
        let query_id = Uuid::new_v4();
        let _guard =
            registry.register(query_id, "SELECT 1", Protocol::Http, owner(), token.clone());

        assert!(
            !registry.cancel_owned(query_id, OTHER),
            "another scope must not cancel the query"
        );
        assert!(
            !token.is_cancelled(),
            "a refused cancellation must leave the query running"
        );
        assert!(
            registry.cancel_owned(query_id, OWNER),
            "the submitting scope may cancel its own query"
        );
        assert!(token.is_cancelled());
    }

    #[test]
    fn list_for_returns_only_the_callers_queries() {
        let registry = Arc::new(QueryCancelRegistry::new());
        let mine = Uuid::from_u128(1);
        let theirs = Uuid::from_u128(2);
        let _mine_guard = registry.register(
            mine,
            "SELECT mine",
            Protocol::Http,
            owner(),
            CancellationToken::new(),
        );
        let _theirs_guard = registry.register(
            theirs,
            "SELECT theirs",
            Protocol::Http,
            Arc::from(OTHER),
            CancellationToken::new(),
        );

        let listed = registry.list_for(OWNER);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].query_id, mine);
        assert!(
            !listed.iter().any(|q| q.sql_preview.contains("theirs")),
            "another scope's SQL must not be disclosed"
        );
        assert_eq!(registry.list_all().len(), 2);
    }
}
