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

use std::borrow::Cow;
use std::sync::Arc;

use crate::error::Error;
use app::spicepod::component::runtime::ApiKey;
use axum::http;
use sha2::{Digest, Sha256};

pub type AuthPrincipalRef = Arc<dyn AuthPrincipal + Sync + Send>;

pub trait AuthPrincipal {
    fn username(&self) -> &str; // The username as presented during auth
    fn groups(&self) -> &[&str]; // Group memberships

    /// A stable, opaque identifier for this principal, suitable for use as
    /// a cache namespace key. Returning `None` means the principal is
    /// effectively anonymous and should share the public cache scope.
    ///
    /// The id MUST be:
    /// - **stable** across credential rotation for the same identity (so
    ///   rotating an OIDC token does not invalidate the principal's cache);
    /// - **opaque** — not the bearer token or API key value itself; and
    /// - prefixed with the auth scheme (e.g. `apikey:`, `u2m:`) so two
    ///   schemes that happen to mint the same id never collide.
    ///
    /// The default implementation returns `None`, which is the correct
    /// behavior for anonymous principals. Real principals must override.
    fn stable_id(&self) -> Option<Cow<'_, str>> {
        None
    }
}

/// Whether `principal` is permitted to perform writes.
///
/// A principal has write access when its group memberships include `write` or
/// `read_write`; see `AuthPrincipal::groups`. Callers that need the read-only
/// posture of the *current request* should use
/// `runtime_request_context::current_principal_requires_read_only`, which
/// resolves the principal from the request context first.
#[must_use]
pub fn principal_has_write_access(principal: &AuthPrincipalRef) -> bool {
    principal
        .groups()
        .iter()
        .any(|group| *group == "write" || *group == "read_write")
}

pub trait AuthRequestContext {
    /// Sets the current authentication principal for the request context.
    ///
    /// # Errors
    ///
    /// This function returns an error if the principal cannot be set.
    fn set_auth_principal(
        &self,
        auth_principal: AuthPrincipalRef,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Retrieves the principal information associated with the request.
    ///
    /// Returns `None` if authentication is not set up or has not yet been completed.
    fn auth_principal(&self) -> Option<&AuthPrincipalRef>;
}

pub enum AuthVerdict {
    Allow(AuthPrincipalRef),
    Deny,
}

impl AuthPrincipal for ApiKey {
    fn username(&self) -> &'static str {
        "api_key_auth"
    }

    fn groups(&self) -> &[&str] {
        match self {
            ApiKey::ReadOnly { .. } => &["read"],
            ApiKey::ReadWrite { .. } => &["read_write"],
        }
    }

    /// Stable opaque id derived from the SHA-256 of the API key material,
    /// truncated to 16 bytes (32 hex chars). The key value itself is
    /// never exposed; rotating the key produces a new id (which is the
    /// desired behavior — rotated keys are a different principal).
    ///
    /// Format: `apikey:<32-hex-chars>`.
    fn stable_id(&self) -> Option<Cow<'_, str>> {
        let key_bytes = match self {
            ApiKey::ReadOnly { key } | ApiKey::ReadWrite { key } => key.as_bytes(),
        };
        let digest = Sha256::digest(key_bytes);
        let mut id = String::with_capacity("apikey:".len() + 32);
        id.push_str("apikey:");
        for byte in &digest[..16] {
            id.push(HEX[usize::from(byte >> 4)] as char);
            id.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        Some(Cow::Owned(id))
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

pub trait HttpAuth {
    /// Receive the entire HTTP request object and return a verdict on whether to allow/deny it
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error>;
}

/// A trait for validating Flight basic auth requests.
///
/// This is inspired by the arrow-go Flight basic auth implementation.
/// <https://github.com/apache/arrow-go/blob/55f8b2075e2bee544b2bb4120966297a5a7d4e43/arrow/flight/server_auth.go#L146>
pub trait FlightBasicAuth {
    /// Receive the username/password for Flight basic auth and return the token that should be used on each subsequent request.
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the username/password.
    fn validate(&self, username: &str, password: &str) -> Result<String, Error>;

    /// Receive a bearer token and return a verdict on whether it is valid.
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the bearer token.
    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error>;
}

pub trait GrpcAuth {
    // Receive the entire gRPC request object and return a verdict
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn grpc_verify(&self, req: &tonic::Request<()>) -> Result<AuthVerdict, Error>;
}

#[cfg(test)]
mod tests {
    use super::{AuthPrincipal, AuthPrincipalRef, principal_has_write_access};
    use app::spicepod::component::runtime::ApiKey;
    use std::sync::Arc;

    struct TestPrincipal(Vec<&'static str>);

    impl AuthPrincipal for TestPrincipal {
        fn username(&self) -> &'static str {
            "test"
        }

        fn groups(&self) -> &[&str] {
            &self.0
        }
    }

    fn principal(groups: Vec<&'static str>) -> AuthPrincipalRef {
        Arc::new(TestPrincipal(groups))
    }

    #[test]
    fn write_access_requires_a_write_group() {
        assert!(principal_has_write_access(&principal(vec!["write"])));
        assert!(principal_has_write_access(&principal(vec!["read_write"])));
        // Present alongside others, in any position.
        assert!(principal_has_write_access(&principal(vec![
            "read",
            "read_write"
        ])));

        assert!(!principal_has_write_access(&principal(vec!["read"])));
        assert!(!principal_has_write_access(&principal(vec![])));
        // Not a prefix or substring match: only the exact group names grant write.
        assert!(!principal_has_write_access(&principal(vec![
            "writer",
            "readwrite",
            "write_read"
        ])));
    }

    #[test]
    fn api_key_variants_map_to_write_access() {
        let read_only: AuthPrincipalRef = Arc::new(ApiKey::ReadOnly {
            key: "k".to_string(),
        });
        let read_write: AuthPrincipalRef = Arc::new(ApiKey::ReadWrite {
            key: "k".to_string(),
        });

        assert!(!principal_has_write_access(&read_only));
        assert!(principal_has_write_access(&read_write));
    }
}
