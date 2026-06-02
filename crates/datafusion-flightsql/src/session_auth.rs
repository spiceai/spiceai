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

//! Session-aware authentication adapter for Flight SQL.
//!
//! Wraps any [`FlightAuth`] implementation so that a Bearer token can be
//! either a raw credential **or** a session ID returned from a prior
//! handshake.  The session store is checked first; on a hit the stored
//! credential is forwarded to the inner validator.

use crate::SessionStore;

/// Trait for Flight SQL authentication.
pub trait FlightAuth: Send + Sync {
    /// Validate a `(username, password)` pair during the handshake.
    ///
    /// Return the token/principal that should be stored with the session.
    ///
    /// # Errors
    ///
    /// Returns an error string if the credentials are invalid.
    fn validate(&self, username: &str, password: &str) -> Result<String, String>;

    /// Validate a Bearer token on every non-handshake request.
    ///
    /// Return `true` to allow the request, `false` to deny it.
    fn is_valid(&self, bearer_token: &str) -> bool;
}

/// Wraps an inner [`FlightAuth`] with session-ID awareness.
///
/// The `is_valid` check first looks up `bearer_token` as a session ID in the
/// store.  If found, the associated credential is re-validated against the
/// inner authenticator.  Unknown tokens fall through to the inner
/// authenticator directly, preserving backward compatibility with clients
/// that send credentials without a prior handshake.
pub struct SessionAwareAuth<A> {
    inner: A,
    session_store: SessionStore,
}

impl<A: FlightAuth> SessionAwareAuth<A> {
    #[must_use]
    pub fn new(inner: A, session_store: SessionStore) -> Self {
        Self {
            inner,
            session_store,
        }
    }
}

impl<A: FlightAuth> FlightAuth for SessionAwareAuth<A> {
    fn validate(&self, username: &str, password: &str) -> Result<String, String> {
        self.inner.validate(username, password)
    }

    fn is_valid(&self, bearer_token: &str) -> bool {
        if let Some(credential) = self.session_store.validate_session(bearer_token) {
            return self.inner.is_valid(&credential);
        }
        self.inner.is_valid(bearer_token)
    }
}
