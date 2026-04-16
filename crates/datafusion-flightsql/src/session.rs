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

//! Lightweight session tracking for Flight SQL.
//!
//! Provides session management to support SQL PREPARE/EXECUTE/DEALLOCATE
//! across multiple Flight SQL requests. Each session maintains its own
//! `SessionContext` which stores prepared statements in its `SessionState`.
//!
//! ## Session ID resolution (priority order)
//!
//! 1. `x-session-id` header
//! 2. `Authorization: Bearer <id>` header
//!
//! Sessions expire after 1 hour of inactivity (configurable via
//! `SESSION_TTL_SECS`) and a maximum of 10 000 sessions are kept.

use datafusion::prelude::SessionContext;
use http::HeaderMap;
use moka::sync::Cache;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataMap;
use util::session_state::builder_from_existing;
use uuid::Uuid;

const SESSION_TTL_SECS: u64 = 3600;
const MAX_SESSIONS: u64 = 10_000;

/// Manages Flight SQL sessions, mapping session IDs to `SessionContext` instances.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Cache<String, Arc<SessionContext>>,
    /// API key (or other principal token) associated with each session.
    session_principals: Cache<String, String>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.entry_count())
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new() -> Self {
        let ttl = Duration::from_secs(SESSION_TTL_SECS);
        Self {
            sessions: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(ttl)
                .build(),
            session_principals: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(ttl)
                .build(),
        }
    }

    /// Create a new session derived from `base_ctx` and return `(session_id, ctx)`.
    ///
    /// If `api_key` is provided it is stored for later validation via
    /// [`validate_session`].
    #[must_use]
    pub fn create_session(
        &self,
        base_ctx: &SessionContext,
        api_key: Option<&str>,
    ) -> (String, Arc<SessionContext>) {
        let session_id = Uuid::now_v7().hyphenated().to_string();

        let new_state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.clone())
            .build();
        let session_ctx = Arc::new(SessionContext::new_with_state(new_state));

        self.sessions
            .insert(session_id.clone(), Arc::clone(&session_ctx));
        if let Some(key) = api_key {
            self.session_principals
                .insert(session_id.clone(), key.to_string());
        }
        self.sessions.run_pending_tasks();
        self.session_principals.run_pending_tasks();

        (session_id, session_ctx)
    }

    /// Return the API key associated with a valid session, or `None`.
    #[must_use]
    pub fn validate_session(&self, session_id: &str) -> Option<String> {
        if self.sessions.get(session_id).is_some() {
            self.session_principals.get(session_id)
        } else {
            None
        }
    }

    /// Look up a session by ID; returns `None` if expired or unknown.
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.sessions.get(session_id)
    }

    /// Look up the session context from gRPC request metadata, returning `None`
    /// when no recognisable session ID header is present.
    #[must_use]
    pub fn get_session_from_metadata(&self, metadata: &MetadataMap) -> Option<Arc<SessionContext>> {
        let session_id = extract_session_id(metadata)?;
        self.get_session(&session_id)
    }

    /// Look up or create a session from gRPC request metadata.
    ///
    /// Returns `None` when no authorization/session header is present.
    #[must_use]
    pub fn get_or_create_session(
        &self,
        metadata: &MetadataMap,
        base_ctx: &SessionContext,
    ) -> Option<Arc<SessionContext>> {
        let session_id = extract_session_id(metadata)?;
        if let Some(session) = self.get_session(&session_id) {
            return Some(session);
        }
        let new_state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.clone())
            .build();
        let session_ctx = Arc::new(SessionContext::new_with_state(new_state));
        self.sessions.insert(session_id, Arc::clone(&session_ctx));
        self.sessions.run_pending_tasks();
        Some(session_ctx)
    }

    /// Look up or create a session from HTTP headers.
    ///
    /// Returns `None` when no authorization/session header is present.
    #[must_use]
    pub fn get_or_create_session_from_http(
        &self,
        headers: &HeaderMap,
        base_ctx: &SessionContext,
    ) -> Option<Arc<SessionContext>> {
        let session_id = extract_session_id_from_headers(headers)?;
        if let Some(session) = self.get_session(&session_id) {
            return Some(session);
        }
        let new_state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.clone())
            .build();
        let session_ctx = Arc::new(SessionContext::new_with_state(new_state));
        self.sessions.insert(session_id, Arc::clone(&session_ctx));
        self.sessions.run_pending_tasks();
        Some(session_ctx)
    }

    /// Remove a session; returns `true` if it existed.
    #[must_use]
    pub fn remove_session(&self, session_id: &str) -> bool {
        let removed = self.sessions.remove(session_id).is_some();
        self.sessions.run_pending_tasks();
        removed
    }

    /// Number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        usize::try_from(self.sessions.entry_count()).unwrap_or(0)
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

fn extract_session_id(metadata: &MetadataMap) -> Option<String> {
    if let Some(v) = metadata.get("x-session-id")
        && let Ok(s) = v.to_str()
    {
        return Some(s.to_string());
    }
    if let Some(v) = metadata.get("authorization")
        && let Ok(s) = v.to_str()
        && let Some(t) = s
            .strip_prefix("Bearer ")
            .or_else(|| s.strip_prefix("bearer "))
    {
        return Some(t.to_string());
    }
    None
}

fn extract_session_id_from_headers(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get("x-session-id")
        && let Ok(s) = v.to_str()
    {
        return Some(s.to_string());
    }
    if let Some(v) = headers.get("authorization")
        && let Ok(s) = v.to_str()
        && let Some(t) = s
            .strip_prefix("Bearer ")
            .or_else(|| s.strip_prefix("bearer "))
    {
        return Some(t.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let (id, ctx1) = store.create_session(&base_ctx, Some("key"));
        assert!(store.get_session(&id).is_some());
        let ctx2 = store.get_session(&id).expect("session exists");
        assert!(Arc::ptr_eq(&ctx1, &ctx2));
        assert_eq!(store.validate_session(&id), Some("key".to_string()));
    }

    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let (id, _) = store.create_session(&base_ctx, Some("k"));
        assert_eq!(store.session_count(), 1);
        assert!(store.remove_session(&id));
        assert_eq!(store.session_count(), 0);
    }

    #[test]
    fn test_extract_session_id_from_metadata() {
        let mut meta = MetadataMap::new();
        assert!(extract_session_id(&meta).is_none());

        meta.insert(
            "authorization",
            "Bearer my-session".parse().expect("valid header value"),
        );
        assert_eq!(extract_session_id(&meta), Some("my-session".to_string()));
    }
}
