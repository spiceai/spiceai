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

//! Session-aware authentication wrapper for Flight SQL.
//!
//! This module provides `SessionAwareAuth`, a wrapper that combines the original
//! authentication (e.g., API key auth) with session-based validation.
//!
//! ## Why is this needed?
//!
//! When a client performs a handshake, we return a session ID as the Bearer token.
//! This session ID is used by the `FlightSqlServiceClient` for all subsequent requests.
//! However, the original auth validator (e.g., `ApiKeyAuth`) only knows about API keys,
//! not session IDs.
//!
//! This wrapper first checks if the Bearer token is a valid session ID (by looking up
//! the session store). If found, it returns the API key associated with that session.
//! If not found, it falls back to the original auth validator.

use std::sync::Arc;

use runtime_auth::{AuthVerdict, FlightBasicAuth, error::Error};

use super::SessionStore;

/// Authentication wrapper that validates both session IDs and API keys.
///
/// This allows the Bearer token to be either:
/// 1. A session ID (created during handshake) - validated via session store
/// 2. An API key - validated via the inner auth validator
pub struct SessionAwareAuth {
    inner: Arc<dyn FlightBasicAuth + Send + Sync>,
    session_store: SessionStore,
}

impl SessionAwareAuth {
    /// Creates a new session-aware auth wrapper.
    ///
    /// The `inner` auth validator is used for initial handshake validation and
    /// as a fallback for Bearer tokens that aren't session IDs.
    #[must_use]
    pub fn new(inner: Arc<dyn FlightBasicAuth + Send + Sync>, session_store: SessionStore) -> Self {
        Self {
            inner,
            session_store,
        }
    }
}

impl FlightBasicAuth for SessionAwareAuth {
    /// Validates username/password during handshake.
    ///
    /// Delegates to the inner auth validator.
    fn validate(&self, username: &str, password: &str) -> Result<String, Error> {
        self.inner.validate(username, password)
    }

    /// Validates a Bearer token.
    ///
    /// First checks if the token is a valid session ID. If so, looks up the
    /// associated API key and validates that. Otherwise, falls back to the
    /// inner auth validator.
    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        // First, check if this is a session ID
        if let Some(api_key) = self.session_store.validate_session(bearer_token) {
            // Session is valid - look up the API key associated with this session
            // and use it to create the auth verdict
            return self.inner.is_valid(&api_key);
        }

        // Fall back to the inner auth validator (for direct API key usage)
        self.inner.is_valid(bearer_token)
    }
}

/// Wraps an optional auth with session awareness.
///
/// If `inner` is `None`, returns `None` (no auth required).
/// Otherwise, wraps the auth in `SessionAwareAuth`.
#[must_use]
pub fn with_session_awareness(
    inner: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    session_store: SessionStore,
) -> Option<Arc<dyn FlightBasicAuth + Send + Sync>> {
    inner.map(|auth| {
        Arc::new(SessionAwareAuth::new(auth, session_store))
            as Arc<dyn FlightBasicAuth + Send + Sync>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use runtime_auth::api_key::ApiKeyAuth;
    use spicepod::component::runtime::ApiKey;

    #[test]
    fn test_session_aware_auth() {
        // Set up API key auth with a known key
        let api_key_auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("test-key:rw")]));
        let session_store = SessionStore::new();

        // Create a session with the known API key
        let base_ctx = SessionContext::new();
        let (session_id, _) = session_store.create_session(&base_ctx, Some("test-key"));

        // Wrap in session-aware auth
        let session_auth = SessionAwareAuth::new(api_key_auth, session_store);

        // Validating the session ID should succeed
        let result = session_auth.is_valid(&session_id);
        assert!(matches!(result, Ok(AuthVerdict::Allow(_))));
    }

    #[test]
    fn test_fallback_to_api_key() {
        let api_key_auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("direct-key:rw")]));
        let session_store = SessionStore::new();

        let session_auth = SessionAwareAuth::new(api_key_auth, session_store);

        // Direct API key should still work
        let result = session_auth.is_valid("direct-key");
        assert!(matches!(result, Ok(AuthVerdict::Allow(_))));
    }

    #[test]
    fn test_invalid_token() {
        let api_key_auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key:rw")]));
        let session_store = SessionStore::new();

        let session_auth = SessionAwareAuth::new(api_key_auth, session_store);

        // Invalid token should be denied
        let result = session_auth.is_valid("invalid-token");
        assert!(matches!(result, Ok(AuthVerdict::Deny)));
    }
}
