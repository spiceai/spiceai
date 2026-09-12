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

//! Session-aware authentication for the HTTP API.
//!
//! The HTTP counterpart of [`crate::flight::session_auth`]: a session id issued
//! by either endpoint is a valid bearer token on either endpoint, so a client
//! that holds one can move between `/v1/sql` and Flight SQL without carrying a
//! second credential.
//!
//! The id is resolved back to the credential it was issued against and the
//! ordinary verifier decides, so a session grants exactly what its key grants —
//! and rotating the key away revokes every session issued against it.

use std::sync::Arc;

use http::HeaderValue;
use runtime_auth::{AuthVerdict, HttpAuth, error::Error};

use crate::sessions::{SessionStore, bearer_token};

const API_KEY_HEADER: &str = "X-API-Key";

pub struct SessionAwareHttpAuth {
    inner: Arc<dyn HttpAuth + Send + Sync>,
    sessions: SessionStore,
}

impl SessionAwareHttpAuth {
    #[must_use]
    pub fn new(inner: Arc<dyn HttpAuth + Send + Sync>, sessions: SessionStore) -> Self {
        Self { inner, sessions }
    }
}

impl HttpAuth for SessionAwareHttpAuth {
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error> {
        // `X-API-Key` is a credential in its own right and the inner verifier
        // prefers it, so a request carrying one is left exactly as it is.
        if request.headers.contains_key(API_KEY_HEADER) {
            return self.inner.http_verify(request);
        }

        let Some(api_key) = request
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token)
            .and_then(|token| self.sessions.bearer_credential(token))
        else {
            return self.inner.http_verify(request);
        };

        let Ok(substituted) = HeaderValue::from_str(&format!("Bearer {api_key}")) else {
            // The stored credential cannot be expressed as a header, so it
            // cannot be verified. Fall through with the request untouched
            // rather than inventing a verdict.
            return self.inner.http_verify(request);
        };

        // Cloning the head is confined to this branch — a request whose bearer
        // token really is a live session id — so ordinary API-key traffic pays
        // nothing for it.
        let mut parts = request.clone();
        parts
            .headers
            .insert(http::header::AUTHORIZATION, substituted);
        self.inner.http_verify(&parts)
    }
}

/// Wraps an optional HTTP auth so session ids are accepted alongside
/// credentials. `None` in, `None` out: a runtime with no auth configured has
/// nothing to wrap.
#[must_use]
pub fn with_session_awareness(
    inner: Option<Arc<dyn HttpAuth + Send + Sync>>,
    sessions: SessionStore,
) -> Option<Arc<dyn HttpAuth + Send + Sync>> {
    inner.map(|auth| {
        Arc::new(SessionAwareHttpAuth::new(auth, sessions)) as Arc<dyn HttpAuth + Send + Sync>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_auth::api_key::ApiKeyAuth;
    use spicepod::component::runtime::ApiKey;

    fn auth_with(keys: &[&str]) -> Arc<dyn HttpAuth + Send + Sync> {
        Arc::new(ApiKeyAuth::new(
            keys.iter().map(|k| ApiKey::parse_str(k)).collect(),
        )) as Arc<dyn HttpAuth + Send + Sync>
    }

    fn parts_with(headers: &[(&str, &str)]) -> http::request::Parts {
        let mut builder = http::Request::builder().uri("/v1/sql");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder
            .body(())
            .expect("the test request head is well-formed")
            .into_parts()
            .0
    }

    #[test]
    fn a_session_id_authenticates_as_the_key_it_was_issued_against() {
        let sessions = SessionStore::new();
        let session = sessions.issue(Some("k".to_string()));
        let auth = SessionAwareHttpAuth::new(auth_with(&["k:rw"]), sessions);

        let verdict = auth
            .http_verify(&parts_with(&[(
                "Authorization",
                &format!("Bearer {}", session.id()),
            )]))
            .expect("verification does not error");
        assert!(matches!(verdict, AuthVerdict::Allow(_)));
    }

    #[test]
    fn a_direct_api_key_still_authenticates() {
        let auth = SessionAwareHttpAuth::new(auth_with(&["k:rw"]), SessionStore::new());

        for headers in [
            vec![("Authorization", "Bearer k")],
            vec![("X-API-Key", "k")],
        ] {
            let verdict = auth
                .http_verify(&parts_with(&headers))
                .expect("verification does not error");
            assert!(matches!(verdict, AuthVerdict::Allow(_)), "{headers:?}");
        }
    }

    #[test]
    fn an_unknown_bearer_token_is_denied() {
        let auth = SessionAwareHttpAuth::new(auth_with(&["k:rw"]), SessionStore::new());

        let verdict = auth
            .http_verify(&parts_with(&[("Authorization", "Bearer nonsense")]))
            .expect("verification does not error");
        assert!(matches!(verdict, AuthVerdict::Deny));
    }

    /// The session stands in for a credential rather than replacing the check,
    /// so a key that no longer validates takes its sessions with it.
    #[test]
    fn revoking_the_key_revokes_the_sessions_issued_against_it() {
        let sessions = SessionStore::new();
        let session = sessions.issue(Some("retired".to_string()));
        let auth = SessionAwareHttpAuth::new(auth_with(&["current:rw"]), sessions);

        let verdict = auth
            .http_verify(&parts_with(&[(
                "Authorization",
                &format!("Bearer {}", session.id()),
            )]))
            .expect("verification does not error");
        assert!(matches!(verdict, AuthVerdict::Deny));
    }
}
