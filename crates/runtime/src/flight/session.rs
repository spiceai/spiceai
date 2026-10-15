/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Lightweight session tracking for Flight SQL
//!
//! This module provides session management to support SQL PREPARE/EXECUTE/DEALLOCATE
//! statements across multiple Flight SQL requests. Each session maintains its own
//! `SessionContext` which stores prepared statements in its `SessionState`.
//!
//! ## Session Lifecycle
//!
//! Sessions are automatically expired after 1 hour of inactivity (configurable via
//! `SESSION_TTL_SECS`). The store also enforces a maximum of 10,000 concurrent sessions
//! (configurable via `MAX_SESSIONS`) with LRU eviction when the limit is reached.
//!
//! ## Session ID Resolution
//!
//! The session ID is extracted from request metadata in the following priority order:
//!
//! 1. **`x-session-id` header** (preferred): Standard session tracking header returned
//!    from the handshake response. Clients that support custom headers should use this.
//!
//! 2. **Authorization Bearer token** (fallback): For compatibility with Flight SQL clients
//!    that don't support custom headers (like the arrow-flight `FlightSqlServiceClient`),
//!    the Bearer token from handshake is used as the session identifier.
//!
//! This two-tier approach ensures session persistence works with both:
//! - Modern clients that can send custom headers
//! - Standard Flight SQL clients that only support authorization headers
//!
//! ## Example Flow
//!
//! ```text
//! 1. Client calls handshake() -> receives session ID in both x-session-id header and payload
//! 2. Client sends PREPARE statement with Authorization: Bearer <session_id>
//!    -> Server creates prepared statement in session context
//! 3. Client sends EXECUTE statement with same Authorization header
//!    -> Server finds session, retrieves prepared statement, executes query
//! ```

use datafusion::prelude::SessionContext;
use http::HeaderMap;
use moka::sync::Cache;
use runtime_auth::AuthPrincipal;
use spicepod::component::runtime::ApiKey;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use util::session_state::builder_from_existing;
use uuid::Uuid;

/// Default session time-to-live in seconds (1 hour)
const SESSION_TTL_SECS: u64 = 3600;

/// Maximum number of concurrent sessions (with LRU eviction)
const MAX_SESSIONS: u64 = 10_000;

/// Manages Flight SQL sessions, mapping session IDs to `DataFusion` `SessionContext` instances.
///
/// This enables stateful operations like SQL PREPARE/EXECUTE across multiple Flight SQL requests.
/// Sessions are automatically expired after `SESSION_TTL_SECS` of inactivity and the store
/// enforces a maximum of `MAX_SESSIONS` concurrent sessions with LRU eviction.
///
/// Each session is associated with an optional API key that was used during the handshake.
/// This allows validating session IDs in the auth layer.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Cache<String, FlightSession>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.entry_count())
            .finish_non_exhaustive()
    }
}

/// A Flight SQL session and the principal it belongs to.
///
/// The owner and the Handshake API key are stored in the same cache entry as
/// the context, so a session cannot lose either while it still exists.
#[derive(Clone)]
pub struct FlightSession {
    pub context: Arc<SessionContext>,
    /// Stable id of the principal that created the session, or `None` when it
    /// was created without an authenticated principal.
    pub owner_stable_id: Option<String>,
    /// The API key a Handshake authenticated with, which makes the session id
    /// usable as a Bearer token.
    api_key: Option<String>,
}

impl FlightSession {
    fn new(base_ctx: &SessionContext, session_id: &str, owner_stable_id: Option<String>) -> Self {
        // Each session needs its own `session_id` so that prepared statements
        // stored in `SessionState.prepared_plans` are isolated. Cloning the
        // existing state keeps catalog/function registrations and the custom
        // analyzer/optimizer rules.
        let state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.to_string())
            .build();
        Self {
            context: Arc::new(SessionContext::new_with_state(state)),
            owner_stable_id,
            api_key: None,
        }
    }
}

impl SessionStore {
    /// Creates a new empty session store with default TTL and max capacity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(Duration::from_secs(SESSION_TTL_SECS))
                .build(),
        }
    }

    /// Creates a new session with a unique ID and returns both the ID and the context.
    ///
    /// The session context is created from the provided base context's state.
    /// If an `api_key` is provided, it's associated with this session for auth validation.
    /// `owner_stable_id` is the principal the session belongs to.
    ///
    /// # Important: Session State Snapshot
    ///
    /// The session context is a **point-in-time snapshot** of the base context's state.
    /// Any changes to datasets, catalogs, or tables registered after session creation
    /// will **not** be visible within existing sessions. Clients will only see the
    /// schema and tables that existed at the moment of session creation.
    ///
    /// This design is intentional for consistency within a session's lifetime, but
    /// long-running sessions may see stale catalog information if the runtime's
    /// registered datasets change.
    #[must_use]
    pub fn create_session(
        &self,
        base_ctx: &SessionContext,
        api_key: Option<&str>,
        owner_stable_id: Option<String>,
    ) -> (String, Arc<SessionContext>) {
        // Use a CSPRNG-backed UUIDv4 rather than the time-ordered UUIDv7: this id
        // is returned to the client and accepted as a bearer session token, so it
        // must be unpredictable and must not leak its creation time.
        let session_id = Uuid::new_v4().hyphenated().to_string();

        let session = FlightSession {
            api_key: api_key.map(str::to_string),
            ..FlightSession::new(base_ctx, &session_id, owner_stable_id)
        };
        let session_ctx = Arc::clone(&session.context);
        self.sessions.insert(session_id.clone(), session);
        self.sessions.run_pending_tasks();
        (session_id, session_ctx)
    }

    /// Validates a session ID and returns the associated API key if valid.
    ///
    /// Returns `Some(api_key)` if the session exists and was created with an API key.
    /// Returns `None` if the session doesn't exist or wasn't created with an API key.
    #[must_use]
    pub fn validate_session(&self, session_id: &str) -> Option<String> {
        self.sessions.get(session_id)?.api_key
    }

    /// Gets an existing session context by ID.
    ///
    /// Returns `None` if the session doesn't exist or has expired.
    /// Accessing a session refreshes its TTL.
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.sessions.get(session_id).map(|session| session.context)
    }

    /// The stable id of the principal an API key authenticates as.
    ///
    /// `ApiKey::stable_id` hashes only the key bytes (group-independent), so
    /// reconstructing the key yields the same id the authenticated principal
    /// exposes.
    #[must_use]
    pub fn api_key_stable_id(api_key: &str) -> Option<String> {
        ApiKey::parse_str(api_key).stable_id().map(Cow::into_owned)
    }

    /// Gets or creates the session named by HTTP headers.
    ///
    /// The session id is the `x-session-id` header, or else the Bearer token.
    /// An existing session is returned with the owner it was created with; a
    /// missing one is created and owned by `owner_stable_id`.
    ///
    /// Returns `None` if neither header is present.
    pub fn get_or_create_session_from_http(
        &self,
        headers: &http::HeaderMap,
        base_ctx: &SessionContext,
        owner_stable_id: Option<&str>,
    ) -> Option<FlightSession> {
        let session_id = extract_session_id_from_headers(headers)?;

        // The session id can be a Bearer credential, so it is never logged.
        tracing::debug!(
            existing_sessions = self.session_count(),
            "Flight SQL session lookup"
        );

        let entry = self.sessions.entry_by_ref(&session_id).or_insert_with(|| {
            FlightSession::new(base_ctx, &session_id, owner_stable_id.map(str::to_string))
        });
        if entry.is_fresh() {
            tracing::debug!("Created Flight SQL session from request");
            self.sessions.run_pending_tasks();
        }
        Some(entry.into_value())
    }

    /// Removes a session from the store.
    ///
    /// Returns `true` if the session existed and was removed.
    #[must_use]
    pub fn remove_session(&self, session_id: &str) -> bool {
        let removed = self.sessions.remove(session_id).is_some();
        self.sessions.run_pending_tasks();
        removed
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        // MAX_SESSIONS is 10,000 which fits in usize on all platforms,
        // but we use try_from for explicit safety and better code quality
        if let Ok(count) = usize::try_from(self.sessions.entry_count()) {
            count
        } else {
            tracing::warn!(
                "Flight SQL session count {} exceeded usize::MAX; returning 0 sessions",
                self.sessions.entry_count()
            );
            0
        }
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracts the session ID from HTTP headers.
///
/// Tries to extract session ID in the following priority order:
/// 1. "x-session-id" header (standard session tracking header)
/// 2. Authorization header Bearer token (fallback for compatibility)
///
/// Returns None if neither header is present.
fn extract_session_id_from_headers(headers: &HeaderMap) -> Option<String> {
    // First try x-session-id header (preferred)
    if let Some(session_header) = headers.get("x-session-id")
        && let Ok(session_id) = session_header.to_str()
    {
        return Some(session_id.to_string());
    }

    // Fall back to authorization header for backward compatibility
    if let Some(auth_header) = headers.get("authorization")
        && let Ok(auth_str) = auth_header.to_str()
    {
        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            return Some(token.to_string());
        }
        if let Some(token) = auth_str.strip_prefix("bearer ") {
            return Some(token.to_string());
        }
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

        let (session_id, ctx1) = store.create_session(&base_ctx, Some("test-api-key"), None);
        assert!(store.get_session(&session_id).is_some());

        let ctx2 = store
            .get_session(&session_id)
            .expect("Session should exist");
        assert!(Arc::ptr_eq(&ctx1, &ctx2));

        // Validate session returns the API key
        assert_eq!(
            store.validate_session(&session_id),
            Some("test-api-key".to_string())
        );
    }

    #[test]
    fn test_create_session_without_api_key() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx, None, None);
        assert!(store.get_session(&session_id).is_some());

        // Validate session returns None when no API key was provided
        assert_eq!(store.validate_session(&session_id), None);
    }

    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx, Some("key"), None);
        assert_eq!(store.session_count(), 1);

        assert!(store.remove_session(&session_id));
        assert_eq!(store.session_count(), 0);
        assert!(store.get_session(&session_id).is_none());
    }

    #[test]
    fn test_multiple_sessions() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (id1, _) = store.create_session(&base_ctx, Some("key1"), None);
        let (id2, _) = store.create_session(&base_ctx, Some("key2"), None);

        assert_ne!(id1, id2);
        assert_eq!(store.session_count(), 2);

        assert!(store.get_session(&id1).is_some());
        assert!(store.get_session(&id2).is_some());

        // Validate each session returns its own API key
        assert_eq!(store.validate_session(&id1), Some("key1".to_string()));
        assert_eq!(store.validate_session(&id2), Some("key2".to_string()));
    }

    #[test]
    fn test_extract_session_id_from_headers() {
        let mut headers = HeaderMap::new();

        // No authorization header
        assert!(extract_session_id_from_headers(&headers).is_none());

        // With Bearer prefix
        headers.insert(
            "authorization",
            "Bearer test-session-id".parse().expect("Valid header"),
        );
        assert_eq!(
            extract_session_id_from_headers(&headers),
            Some("test-session-id".to_string())
        );

        // Without Bearer prefix - should return None since we require "Bearer " prefix
        headers.insert(
            "authorization",
            "test-session-id-2".parse().expect("Valid header"),
        );
        assert!(
            extract_session_id_from_headers(&headers).is_none(),
            "Authorization header without 'Bearer ' prefix should return None"
        );

        // `x-session-id` takes precedence over the Bearer token.
        headers.insert(
            "authorization",
            "Bearer test-session-id".parse().expect("Valid header"),
        );
        headers.insert("x-session-id", "chosen".parse().expect("Valid header"));
        assert_eq!(
            extract_session_id_from_headers(&headers),
            Some("chosen".to_string())
        );
    }

    #[test]
    fn test_get_or_create_session_from_http() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer new-session".parse().expect("Valid header"),
        );

        // First call creates a new session
        let first = store
            .get_or_create_session_from_http(&headers, &base_ctx, Some("apikey:creator"))
            .expect("Should create session");
        assert_eq!(store.session_count(), 1);

        // Second call returns the same session
        let second = store
            .get_or_create_session_from_http(&headers, &base_ctx, Some("apikey:creator"))
            .expect("Should return existing session");
        assert!(Arc::ptr_eq(&first.context, &second.context));
        assert_eq!(store.session_count(), 1);
    }

    #[test]
    fn test_session_id_is_csprng_uuid_v4() {
        // Regression: session ids are returned to clients and accepted as bearer
        // session tokens, so they must be CSPRNG-random (UUIDv4), never the
        // time-ordered/partly-predictable UUIDv7.
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let (session_id, _) = store.create_session(&base_ctx, Some("k"), None);

        let uuid = Uuid::parse_str(&session_id).expect("session id should be a valid UUID");
        assert_eq!(
            uuid.get_version(),
            Some(uuid::Version::Random),
            "session id must be a UUIDv4 (random), got {session_id}"
        );
    }

    #[test]
    fn a_session_keeps_the_owner_it_was_created_with() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let header = |session_id: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-session-id",
                session_id.parse().expect("valid header value"),
            );
            headers
        };

        // A Handshake session is owned by the principal of the key that created it.
        let owner = SessionStore::api_key_stable_id("owner-key");
        assert!(owner.is_some());
        let (session_id, _) = store.create_session(&base_ctx, Some("owner-key"), owner.clone());
        let named_by_another = store
            .get_or_create_session_from_http(&header(&session_id), &base_ctx, Some("apikey:other"))
            .expect("the session exists");
        assert_eq!(named_by_another.owner_stable_id, owner);

        // A session a principal creates by naming a new id is owned by that principal,
        // and naming it again as another principal does not change the owner.
        let created = store
            .get_or_create_session_from_http(&header("shared-label"), &base_ctx, Some("apikey:a"))
            .expect("creates the session");
        assert_eq!(created.owner_stable_id.as_deref(), Some("apikey:a"));
        let named_by_b = store
            .get_or_create_session_from_http(&header("shared-label"), &base_ctx, Some("apikey:b"))
            .expect("the session exists");
        assert!(Arc::ptr_eq(&created.context, &named_by_b.context));
        assert_eq!(named_by_b.owner_stable_id.as_deref(), Some("apikey:a"));

        // Without an authenticated principal, a session has no owner.
        let anonymous = store
            .get_or_create_session_from_http(&header("anonymous-label"), &base_ctx, None)
            .expect("creates the session");
        assert_eq!(anonymous.owner_stable_id, None);
    }
}
