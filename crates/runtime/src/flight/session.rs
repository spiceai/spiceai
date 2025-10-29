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

use dashmap::DashMap;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use uuid::Uuid;

/// Manages Flight SQL sessions, mapping session IDs to DataFusion `SessionContext` instances.
///
/// This enables stateful operations like SQL PREPARE/EXECUTE across multiple Flight SQL requests.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Arc<DashMap<String, Arc<SessionContext>>>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.len())
            .finish()
    }
}

impl SessionStore {
    /// Creates a new empty session store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    /// Creates a new session with a unique ID and returns both the ID and the context.
    ///
    /// The session context is created from the provided base context's state.
    pub fn create_session(&self, base_ctx: &SessionContext) -> (String, Arc<SessionContext>) {
        let session_id = Uuid::new_v4().hyphenated().to_string();
        let session_ctx = Arc::new(SessionContext::new_with_state(base_ctx.state()));
        self.sessions
            .insert(session_id.clone(), Arc::clone(&session_ctx));
        (session_id, session_ctx)
    }

    /// Gets an existing session context by ID.
    ///
    /// Returns `None` if the session doesn't exist.
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.sessions
            .get(session_id)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Gets or creates a session from the request metadata.
    ///
    /// Extracts the session ID from the "authorization" Bearer token in the metadata.
    /// If a session exists for that ID, returns it. Otherwise, creates a new session
    /// using the provided base context.
    ///
    /// Returns `None` if no authorization header is present.
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
            let session_ctx = Arc::new(SessionContext::new_with_state(base_ctx.state()));
            self.sessions.insert(session_id, Arc::clone(&session_ctx));
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
        let session_id = extract_session_id_from_http(headers)?;

        tracing::debug!(
            "Flight SQL session: ID={}, existing_sessions={}",
            session_id,
            self.session_count()
        );

        // Try to get existing session, or create a new one
        if let Some(session) = self.get_session(&session_id) {
            tracing::debug!("Using existing Flight SQL session: {}", session_id);
            Some(session)
        } else {
            tracing::debug!("Creating new Flight SQL session: {}", session_id);
            // Create new session with the provided ID (from auth token)
            let session_ctx = Arc::new(SessionContext::new_with_state(base_ctx.state()));
            self.sessions.insert(session_id, Arc::clone(&session_ctx));
            Some(session_ctx)
        }
    }

    /// Removes a session from the store.
    ///
    /// Returns `true` if the session existed and was removed.
    pub fn remove_session(&self, session_id: &str) -> bool {
        self.sessions.remove(session_id).is_some()
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
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
    if let Some(session_header) = metadata.get("x-session-id") {
        if let Ok(session_id) = session_header.to_str() {
            return Some(session_id.to_string());
        }
    }

    // Fallback to Authorization Bearer token for compatibility
    // This allows clients that use handshake() to have session persistence
    // even if they don't explicitly set x-session-id headers
    if let Some(auth_header) = metadata.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
            if let Some(token) = auth_str.strip_prefix("bearer ") {
                return Some(token.to_string());
            }
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
fn extract_session_id_from_http(headers: &http::HeaderMap) -> Option<String> {
    // Try x-session-id header (standard approach)
    if let Some(session_header) = headers.get("x-session-id") {
        if let Ok(session_id) = session_header.to_str() {
            return Some(session_id.to_string());
        }
    }

    // Fallback to Authorization Bearer token for compatibility
    // This allows clients that use handshake() to have session persistence
    // even if they don't explicitly set x-session-id headers
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
            if let Some(token) = auth_str.strip_prefix("bearer ") {
                return Some(token.to_string());
            }
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

        let (session_id, ctx1) = store.create_session(&base_ctx);
        assert!(store.get_session(&session_id).is_some());

        let ctx2 = store
            .get_session(&session_id)
            .expect("Session should exist");
        assert!(Arc::ptr_eq(&ctx1, &ctx2));
    }

    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx);
        assert_eq!(store.session_count(), 1);

        assert!(store.remove_session(&session_id));
        assert_eq!(store.session_count(), 0);
        assert!(store.get_session(&session_id).is_none());
    }

    #[test]
    fn test_multiple_sessions() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (id1, _) = store.create_session(&base_ctx);
        let (id2, _) = store.create_session(&base_ctx);

        assert_ne!(id1, id2);
        assert_eq!(store.session_count(), 2);

        assert!(store.get_session(&id1).is_some());
        assert!(store.get_session(&id2).is_some());
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

        // Without Bearer prefix
        metadata.insert(
            "authorization",
            "test-session-id-2".parse().expect("Valid header"),
        );
        assert_eq!(
            extract_session_id(&metadata),
            Some("test-session-id-2".to_string())
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
}
