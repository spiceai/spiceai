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
//! A client that handshakes is handed a session id and, per the Flight SQL
//! convention, presents it as its bearer token from then on. The underlying
//! validator knows about API keys, not session ids, so this wrapper resolves an
//! id back to the credential the session was issued against before delegating.
//! Anything that is not a live session id falls through to the validator
//! unchanged, so direct API-key use is unaffected.

use std::sync::Arc;

use runtime_auth::{AuthVerdict, FlightBasicAuth, error::Error};

use crate::sessions::SessionStore;

/// Authentication wrapper that accepts either an API key or the id of a live
/// session issued to one.
pub struct SessionAwareAuth {
    inner: Arc<dyn FlightBasicAuth + Send + Sync>,
    session_store: SessionStore,
}

impl SessionAwareAuth {
    #[must_use]
    pub fn new(inner: Arc<dyn FlightBasicAuth + Send + Sync>, session_store: SessionStore) -> Self {
        Self {
            inner,
            session_store,
        }
    }
}

impl FlightBasicAuth for SessionAwareAuth {
    /// Validates username/password during handshake, which is always a
    /// credential rather than a session id.
    fn validate(&self, username: &str, password: &str) -> Result<String, Error> {
        self.inner.validate(username, password)
    }

    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        if let Some(api_key) = self.session_store.bearer_credential(bearer_token) {
            return self.inner.is_valid(&api_key);
        }
        self.inner.is_valid(bearer_token)
    }
}

/// Wraps an optional auth so session ids are accepted alongside credentials.
/// `None` in, `None` out: a runtime with no auth configured has nothing to wrap.
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

    fn auth_with(keys: &[&str]) -> Arc<ApiKeyAuth> {
        Arc::new(ApiKeyAuth::new(
            keys.iter().map(|k| ApiKey::parse_str(k)).collect(),
        ))
    }

    #[test]
    fn a_session_id_authenticates_as_the_key_it_was_issued_against() {
        let store = SessionStore::new();
        let session = store.issue(&SessionContext::new(), None, Some("test-key".to_string()));

        let session_auth = SessionAwareAuth::new(auth_with(&["test-key:rw"]), store);

        assert!(matches!(
            session_auth.is_valid(session.id()),
            Ok(AuthVerdict::Allow(_))
        ));
    }

    #[test]
    fn a_direct_api_key_still_authenticates() {
        let session_auth =
            SessionAwareAuth::new(auth_with(&["direct-key:rw"]), SessionStore::new());

        assert!(matches!(
            session_auth.is_valid("direct-key"),
            Ok(AuthVerdict::Allow(_))
        ));
    }

    #[test]
    fn an_unknown_token_is_denied() {
        let session_auth = SessionAwareAuth::new(auth_with(&["valid-key:rw"]), SessionStore::new());

        assert!(matches!(
            session_auth.is_valid("invalid-token"),
            Ok(AuthVerdict::Deny)
        ));
    }

    /// A session issued without a credential — created by an unauthenticated
    /// caller — is not itself a credential, so its id must not authenticate.
    #[test]
    fn a_session_issued_without_a_credential_is_not_a_credential() {
        let store = SessionStore::new();
        let session = store.issue(&SessionContext::new(), None, None);

        let session_auth = SessionAwareAuth::new(auth_with(&["valid-key:rw"]), store);

        assert!(matches!(
            session_auth.is_valid(session.id()),
            Ok(AuthVerdict::Deny)
        ));
    }

    /// A session outlives its key only until the key stops validating: the id
    /// resolves to the credential, and the credential is checked every time, so
    /// rotating the key away revokes every session issued against it.
    #[test]
    fn revoking_the_key_revokes_the_sessions_issued_against_it() {
        let store = SessionStore::new();
        let session = store.issue(
            &SessionContext::new(),
            None,
            Some("retired-key".to_string()),
        );

        let session_auth = SessionAwareAuth::new(auth_with(&["current-key:rw"]), store);

        assert!(matches!(
            session_auth.is_valid(session.id()),
            Ok(AuthVerdict::Deny)
        ));
    }
}
