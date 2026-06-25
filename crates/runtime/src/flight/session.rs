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
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataMap;
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
    sessions: Cache<String, Arc<SessionContext>>,
    /// Maps session IDs to the API key that created them (for auth validation)
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
    /// Creates a new empty session store with default TTL and max capacity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(Duration::from_secs(SESSION_TTL_SECS))
                .build(),
            session_principals: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(Duration::from_secs(SESSION_TTL_SECS))
                .build(),
        }
    }

    /// Creates a new session with a unique ID and returns both the ID and the context.
    ///
    /// The session context is created from the provided base context's state.
    /// If an `api_key` is provided, it's associated with this session for auth validation.
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
    ) -> (String, Arc<SessionContext>) {
        // Use a CSPRNG-backed UUIDv4 rather than the time-ordered UUIDv7: this id
        // is returned to the client and accepted as a bearer session token, so it
        // must be unpredictable and must not leak its creation time.
        let session_id = Uuid::new_v4().hyphenated().to_string();

        // Create a new SessionState with a unique session_id using SessionStateBuilder.
        // This is critical for session isolation - each session needs its own session_id
        // so that prepared statements stored in SessionState.prepared_plans are isolated.
        //
        // Note: We clone the existing state into a new builder so catalog/function
        // registrations and custom analyzer/optimizer rules are preserved, while
        // `session_id` remains unset until we assign a unique one below.
        let new_state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.clone())
            .build();
        let session_ctx = Arc::new(SessionContext::new_with_state(new_state));
        self.sessions
            .insert(session_id.clone(), Arc::clone(&session_ctx));

        // Track the API key associated with this session for auth validation
        if let Some(key) = api_key {
            self.session_principals
                .insert(session_id.clone(), key.to_string());
        }

        self.sessions.run_pending_tasks();
        self.session_principals.run_pending_tasks();
        (session_id, session_ctx)
    }

    /// Validates a session ID and returns the associated API key if valid.
    ///
    /// Returns `Some(api_key)` if the session exists and was created with an API key.
    /// Returns `None` if the session doesn't exist or wasn't created with an API key.
    #[must_use]
    pub fn validate_session(&self, session_id: &str) -> Option<String> {
        // Only return the principal if the session also exists
        if self.sessions.get(session_id).is_some() {
            self.session_principals.get(session_id)
        } else {
            None
        }
    }

    /// Gets an existing session context by ID.
    ///
    /// Returns `None` if the session doesn't exist or has expired.
    /// Accessing a session refreshes its TTL.
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.sessions.get(session_id)
    }

    /// Returns the stable principal id of the session's owner if the request
    /// names an existing, owned session.
    ///
    /// The session id is read from the same `x-session-id` / Authorization
    /// headers used to select the session. The returned id matches the
    /// `stable_id()` of the `ApiKey` principal that created the session at
    /// handshake, so callers can compare it against the authenticated principal
    /// to confirm the session belongs to them. Returns `None` for unknown or
    /// unowned sessions (the latter preserving behavior for unauthenticated
    /// setups).
    #[must_use]
    pub fn owner_stable_id_from_http(&self, headers: &HeaderMap) -> Option<String> {
        let session_id = extract_session_id_from_headers(headers)?;
        let owner_api_key = self.validate_session(&session_id)?;
        // `ApiKey::stable_id` hashes only the key bytes (group-independent), so
        // reconstructing the key here yields the same id the owning principal
        // exposes. Kept consistent with `runtime_auth`'s `AuthPrincipal` impl.
        ApiKey::parse_str(&owner_api_key)
            .stable_id()
            .map(std::borrow::Cow::into_owned)
    }

    /// Gets or creates a session from the request metadata.
    ///
    /// Extracts the session ID from the "authorization" Bearer token in the metadata.
    /// If a session exists for that ID, returns it. Otherwise, creates a new session
    /// using the provided base context.
    ///
    /// Returns `None` if no authorization header is present.
    #[must_use]
    pub fn get_or_create_session(
        &self,
        metadata: &MetadataMap,
        base_ctx: &SessionContext,
    ) -> Option<Arc<SessionContext>> {
        let session_id = extract_session_id(metadata)?;

        // Try to get existing session, or create a new one
        if let Some(session) = self.get_session(&session_id) {
            Some(session)
        } else {
            // Create new session with the provided ID (from auth token)
            // Use SessionStateBuilder to ensure the session has its own unique session_id
            // for proper prepared statement isolation
            let new_state = builder_from_existing(&base_ctx.state())
                .with_session_id(session_id.clone())
                .build();
            let session_ctx = Arc::new(SessionContext::new_with_state(new_state));
            self.sessions.insert(session_id, Arc::clone(&session_ctx));
            self.sessions.run_pending_tasks();
            Some(session_ctx)
        }
    }

    /// Gets or creates a session from HTTP headers.
    ///
    /// Extracts the session ID from the "authorization" Bearer token in the headers.
    /// If a session exists for that ID, returns it. Otherwise, creates a new session
    /// using the provided base context.
    ///
    /// Returns `None` if no authorization header is present.
    pub fn get_or_create_session_from_http(
        &self,
        headers: &http::HeaderMap,
        base_ctx: &SessionContext,
    ) -> Option<Arc<SessionContext>> {
        let session_id = extract_session_id_from_headers(headers)?;

        tracing::debug!(
            "Flight SQL session lookup: ID={}, existing_sessions={}",
            session_id,
            self.session_count()
        );

        // Try to get existing session, or create a new one
        if let Some(session) = self.get_session(&session_id) {
            tracing::debug!("Using existing Flight SQL session: {}", session_id);
            Some(session)
        } else {
            tracing::debug!(
                "Creating NEW Flight SQL session from request: {}",
                session_id
            );
            // Create new session with the provided ID (from auth token)
            // Use SessionStateBuilder to ensure the session has its own unique session_id
            // for proper prepared statement isolation
            let new_state = builder_from_existing(&base_ctx.state())
                .with_session_id(session_id.clone())
                .build();
            let session_ctx = Arc::new(SessionContext::new_with_state(new_state));
            self.sessions.insert(session_id, Arc::clone(&session_ctx));
            self.sessions.run_pending_tasks();
            Some(session_ctx)
        }
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

/// Extracts the session ID from metadata headers.
///
/// Tries to extract session ID in the following priority order:
/// 1. "x-session-id" header (standard session tracking header)
/// 2. Authorization header Bearer token (fallback for compatibility)
///
/// Returns None if neither header is present.
fn extract_session_id(metadata: &MetadataMap) -> Option<String> {
    // Try x-session-id header (standard approach)
    if let Some(session_header) = metadata.get("x-session-id")
        && let Ok(session_id) = session_header.to_str()
    {
        return Some(session_id.to_string());
    }

    // Fallback to Authorization Bearer token for compatibility
    // This allows clients that use handshake() to have session persistence
    // even if they don't explicitly set x-session-id headers
    if let Some(auth_header) = metadata.get("authorization")
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

        let (session_id, ctx1) = store.create_session(&base_ctx, Some("test-api-key"));
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

        let (session_id, _) = store.create_session(&base_ctx, None);
        assert!(store.get_session(&session_id).is_some());

        // Validate session returns None when no API key was provided
        assert_eq!(store.validate_session(&session_id), None);
    }

    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx, Some("key"));
        assert_eq!(store.session_count(), 1);

        assert!(store.remove_session(&session_id));
        assert_eq!(store.session_count(), 0);
        assert!(store.get_session(&session_id).is_none());
    }

    #[test]
    fn test_multiple_sessions() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (id1, _) = store.create_session(&base_ctx, Some("key1"));
        let (id2, _) = store.create_session(&base_ctx, Some("key2"));

        assert_ne!(id1, id2);
        assert_eq!(store.session_count(), 2);

        assert!(store.get_session(&id1).is_some());
        assert!(store.get_session(&id2).is_some());

        // Validate each session returns its own API key
        assert_eq!(store.validate_session(&id1), Some("key1".to_string()));
        assert_eq!(store.validate_session(&id2), Some("key2".to_string()));
    }

    #[test]
    fn test_extract_session_id() {
        let mut metadata = MetadataMap::new();

        // No authorization header
        assert!(extract_session_id(&metadata).is_none());

        // With Bearer prefix
        metadata.insert(
            "authorization",
            "Bearer test-session-id".parse().expect("Valid header"),
        );
        assert_eq!(
            extract_session_id(&metadata),
            Some("test-session-id".to_string())
        );

        // Without Bearer prefix - should return None since we require "Bearer " prefix
        metadata.insert(
            "authorization",
            "test-session-id-2".parse().expect("Valid header"),
        );
        assert!(
            extract_session_id(&metadata).is_none(),
            "Authorization header without 'Bearer ' prefix should return None"
        );
    }

    #[test]
    fn test_get_or_create_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let mut metadata = MetadataMap::new();

        metadata.insert(
            "authorization",
            "Bearer new-session".parse().expect("Valid header"),
        );

        // First call creates a new session
        let ctx1 = store
            .get_or_create_session(&metadata, &base_ctx)
            .expect("Should create session");
        assert_eq!(store.session_count(), 1);

        // Second call returns the same session
        let ctx2 = store
            .get_or_create_session(&metadata, &base_ctx)
            .expect("Should return existing session");
        assert!(Arc::ptr_eq(&ctx1, &ctx2));
        assert_eq!(store.session_count(), 1);
    }

    #[test]
    fn test_session_id_is_csprng_uuid_v4() {
        // Regression: session ids are returned to clients and accepted as bearer
        // session tokens, so they must be CSPRNG-random (UUIDv4), never the
        // time-ordered/partly-predictable UUIDv7.
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let (session_id, _) = store.create_session(&base_ctx, Some("k"));

        let uuid = Uuid::parse_str(&session_id).expect("session id should be a valid UUID");
        assert_eq!(
            uuid.get_version(),
            Some(uuid::Version::Random),
            "session id must be a UUIDv4 (random), got {session_id}"
        );
    }

    #[test]
    fn test_owner_stable_id_from_http_binds_session_to_owner() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        // An owned session resolves to its owner's stable id.
        let (session_id, _) = store.create_session(&base_ctx, Some("owner-key"));
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-session-id",
            session_id.parse().expect("valid header value"),
        );
        let expected = ApiKey::parse_str("owner-key")
            .stable_id()
            .map(std::borrow::Cow::into_owned);
        assert!(expected.is_some());
        assert_eq!(store.owner_stable_id_from_http(&headers), expected);

        // A session created without an api key is unowned (None).
        let (anon_id, _) = store.create_session(&base_ctx, None);
        let mut anon_headers = http::HeaderMap::new();
        anon_headers.insert("x-session-id", anon_id.parse().expect("valid header value"));
        assert_eq!(store.owner_stable_id_from_http(&anon_headers), None);

        // An unknown session id yields no owner.
        let mut unknown = http::HeaderMap::new();
        unknown.insert(
            "x-session-id",
            "00000000-0000-4000-8000-000000000000"
                .parse()
                .expect("valid header value"),
        );
        assert_eq!(store.owner_stable_id_from_http(&unknown), None);
    }
}
