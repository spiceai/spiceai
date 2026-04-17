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

pub mod jwks;

use std::sync::Arc;

use axum::http;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

use crate::error::Error;
use crate::identity::IdentityContext;
use crate::{AuthPrincipal, AuthVerdict, FlightBasicAuth, GrpcAuth, HttpAuth};
use jwks::{DiscoveryResult, JwksCache};

/// Algorithms considered safe for OIDC JWT validation.
/// We never trust the JWT header's `alg` directly — only these are accepted.
const ALLOWED_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::EdDSA,
];

/// An OIDC-authenticated principal extracted from a JWT.
pub struct OidcPrincipal {
    subject: String,
    /// Owned group strings. Not read directly but kept alive for `groups_ptrs`.
    #[expect(dead_code)]
    groups_owned: Vec<String>,
    /// Cached `&str` references into `groups_owned` for the `AuthPrincipal` trait.
    /// SAFETY: These pointers are valid for the lifetime of `self` because
    /// `groups_owned` is never mutated after construction.
    groups_ptrs: Vec<*const str>,
    /// Rich identity context carrying `user_id`, `org_id`, roles, and all JWT claims.
    identity: IdentityContext,
}

// SAFETY: The raw pointers in `groups_ptrs` point into `groups_owned` which is owned
// and immutable, making the struct safe to send/share across threads.
unsafe impl Send for OidcPrincipal {}
unsafe impl Sync for OidcPrincipal {}

impl OidcPrincipal {
    fn new(subject: String, groups: Vec<String>, identity: IdentityContext) -> Self {
        let groups_ptrs: Vec<*const str> = groups
            .iter()
            .map(|s| std::ptr::from_ref::<str>(s.as_str()))
            .collect();
        Self {
            subject,
            groups_owned: groups,
            groups_ptrs,
            identity,
        }
    }
}

impl AuthPrincipal for OidcPrincipal {
    fn username(&self) -> &str {
        &self.subject
    }

    fn groups(&self) -> &[&str] {
        // SAFETY: `groups_ptrs` contains pointers into `groups_owned` which is
        // never modified after construction, so the pointers remain valid.
        unsafe {
            &*(std::ptr::from_ref::<[*const str]>(self.groups_ptrs.as_slice()) as *const [&str])
        }
    }

    fn identity_context(&self) -> Option<&IdentityContext> {
        Some(&self.identity)
    }
}

/// JWT claims we extract during validation.
#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    /// All remaining claims, collected for `session_property()` lookups.
    /// Standard claims decoded into named fields (e.g. `sub`) are not captured
    /// here by serde, so they must be inserted explicitly if needed.
    #[serde(flatten)]
    extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Configurable claim mappings for extracting identity fields from JWT tokens.
#[derive(Debug, Clone)]
pub struct ClaimMappings {
    /// JWT claim to use as the primary user identifier. Defaults to `"sub"`.
    pub user_id: String,
    /// JWT claim to extract the organization/tenant identifier from.
    pub org_id: Option<String>,
    /// JWT claim names to extract role memberships from. Roles from all matching
    /// claims are merged.
    pub roles: Vec<String>,
}

impl Default for ClaimMappings {
    fn default() -> Self {
        Self {
            user_id: "sub".to_string(),
            org_id: None,
            roles: Vec::new(),
        }
    }
}

/// OIDC authentication provider that validates JWT Bearer tokens using JWKS.
pub struct OidcAuth {
    jwks_cache: Arc<JwksCache>,
    groups_claims: Vec<String>,
    claim_mappings: ClaimMappings,
    /// Pre-built validation config (audience, issuer, algorithms). Avoids
    /// rebuilding `HashSet`s and `Vec`s on every request.
    validation: Validation,
    refresh_handle: tokio::task::JoinHandle<()>,
}

impl Drop for OidcAuth {
    fn drop(&mut self) {
        self.refresh_handle.abort();
    }
}

impl OidcAuth {
    /// Create a new OIDC auth provider. Performs OIDC discovery and fetches initial JWKS keys.
    ///
    /// # Errors
    ///
    /// Returns an error if OIDC discovery or the initial JWKS key fetch fails.
    pub async fn new(
        issuer_url: String,
        audience: Vec<String>,
        groups_claims: Vec<String>,
        claim_mappings: ClaimMappings,
    ) -> Result<Self, Error> {
        let (
            jwks_cache,
            DiscoveryResult {
                issuer: canonical_issuer,
            },
        ) = JwksCache::new(&issuer_url).await?;

        // Per OIDC Discovery spec Section 4.3, the `issuer` from the discovery document
        // MUST be identical to the `iss` claim in tokens. Use the canonical issuer from
        // the discovery doc — not the user-configured `issuer_url` — for validation.
        // Warn if they differ (beyond a trailing slash) so the user can correct their
        // config, but proceed with the canonical value.
        if canonical_issuer != issuer_url.trim_end_matches('/') && canonical_issuer != issuer_url {
            tracing::warn!(
                configured = %issuer_url,
                canonical = %canonical_issuer,
                "Configured issuer_url does not exactly match the issuer from the OIDC discovery document. \
                JWT iss validation will use the canonical issuer from the discovery document."
            );
        }

        let jwks_cache = Arc::new(jwks_cache);
        let refresh_handle = Arc::clone(&jwks_cache).start_refresh_task();

        // Pre-build validation config carrying audience and issuer so we don't
        // re-parse them on every request. The algorithms field is intentionally
        // left as a placeholder — it is overridden per-request in `validate_token`
        // with the single algorithm from the JWT header.
        let mut validation = Validation::new(Algorithm::RS256);
        validation.algorithms = ALLOWED_ALGORITHMS.to_vec();
        let aud_refs: Vec<&str> = audience.iter().map(String::as_str).collect();
        validation.set_audience(&aud_refs);
        validation.set_issuer(&[&canonical_issuer]);

        Ok(Self {
            jwks_cache,
            groups_claims,
            claim_mappings,
            validation,
            refresh_handle,
        })
    }

    /// Validate a JWT token string against the cached JWKS keys.
    ///
    /// Returns `Deny` for any token that fails validation (bad signature,
    /// expired, wrong audience, malformed, unknown key, etc.).
    fn validate_token(&self, token: &str) -> AuthVerdict {
        let header = match decode_header(token) {
            Ok(h) => h,
            Err(e) => {
                tracing::trace!("Malformed JWT header: {e}");
                return AuthVerdict::Deny;
            }
        };

        if !ALLOWED_ALGORITHMS.contains(&header.alg) {
            tracing::trace!(alg = ?header.alg, "JWT uses disallowed algorithm");
            return AuthVerdict::Deny;
        }

        let keys = self.jwks_cache.get_keys();

        let matching_keys: Vec<_> = if let Some(ref kid) = header.kid {
            keys.iter()
                .filter(|k| k.common.key_id.as_deref() == Some(kid.as_str()))
                .collect()
        } else {
            keys.iter().collect()
        };

        if matching_keys.is_empty() {
            tracing::trace!("No matching JWKS key found for JWT");
            return AuthVerdict::Deny;
        }

        // Build a per-request validation using only the token's own algorithm.
        // jsonwebtoken requires all algorithms in `Validation::algorithms` to share
        // the same key family as the `DecodingKey` — using a multi-family list
        // (RSA + EC + OKP) causes `InvalidAlgorithm` even when the token's `alg` is valid.
        let mut validation = self.validation.clone();
        validation.algorithms = vec![header.alg];

        for jwk in matching_keys {
            let Ok(decoding_key) = DecodingKey::from_jwk(jwk) else {
                continue;
            };

            match decode::<Claims>(token, &decoding_key, &validation) {
                Ok(token_data) => {
                    let groups = self.extract_groups(&token_data.claims);
                    let identity = self.build_identity_context(&token_data.claims);
                    let principal = OidcPrincipal::new(token_data.claims.sub, groups, identity);
                    return AuthVerdict::Allow(Arc::new(principal));
                }
                Err(e) => {
                    tracing::trace!("JWT validation failed with key: {e}");
                }
            }
        }

        AuthVerdict::Deny
    }

    /// Extract groups from all configured group claim names, merging the results.
    fn extract_groups(&self, claims: &Claims) -> Vec<String> {
        let mut groups = Vec::new();
        for claim_name in &self.groups_claims {
            if let Some(arr) = claims.extra.get(claim_name).and_then(|v| v.as_array()) {
                groups.extend(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ToString::to_string)),
                );
            }
        }
        groups
    }

    /// Build an [`IdentityContext`] from the JWT claims using the configured claim mappings.
    fn build_identity_context(&self, claims: &Claims) -> IdentityContext {
        // User ID: use the mapped claim, falling back to `sub`.
        let user_id = if self.claim_mappings.user_id == "sub" {
            claims.sub.clone()
        } else {
            claims
                .extra
                .get(&self.claim_mappings.user_id)
                .and_then(|v| v.as_str())
                .map_or_else(|| claims.sub.clone(), ToString::to_string)
        };

        // Org ID: extract from the configured claim.
        let org_id = self
            .claim_mappings
            .org_id
            .as_ref()
            .and_then(|claim_name| claims.extra.get(claim_name))
            .and_then(|v| v.as_str())
            .map(ToString::to_string);

        // Roles: extract from all configured role claim names, merging string arrays.
        let mut roles = Vec::new();
        for claim_name in &self.claim_mappings.roles {
            if let Some(value) = claims.extra.get(claim_name) {
                if let Some(arr) = value.as_array() {
                    roles.extend(
                        arr.iter()
                            .filter_map(|v| v.as_str().map(ToString::to_string)),
                    );
                } else if let Some(s) = value.as_str() {
                    // Support single-value role claims.
                    roles.push(s.to_string());
                }
            }
        }

        // `sub` is consumed by the named field above and absent from `extra`,
        // so insert it explicitly for session_property('sub') lookups.
        let mut all_claims = claims.extra.clone();
        all_claims
            .entry("sub".to_string())
            .or_insert_with(|| serde_json::Value::String(claims.sub.clone()));

        IdentityContext::new(user_id)
            .with_org_id_opt(org_id)
            .with_roles(roles)
            .with_claims(all_claims)
    }
}

fn extract_bearer_token(headers: &http::HeaderMap) -> Option<&str> {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

impl HttpAuth for OidcAuth {
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error> {
        let Some(token) = extract_bearer_token(&request.headers) else {
            return Ok(AuthVerdict::Deny);
        };
        Ok(self.validate_token(token))
    }
}

impl FlightBasicAuth for OidcAuth {
    fn validate(&self, _username: &str, password: &str) -> Result<String, Error> {
        if password.is_empty() {
            return Err(Error::InvalidCredentials);
        }
        match self.validate_token(password) {
            AuthVerdict::Allow(_) => Ok(password.to_string()),
            AuthVerdict::Deny => Err(Error::InvalidCredentials),
        }
    }

    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        if bearer_token.is_empty() {
            return Ok(AuthVerdict::Deny);
        }
        Ok(self.validate_token(bearer_token))
    }
}

impl GrpcAuth for OidcAuth {
    fn grpc_verify(&self, req: &tonic::Request<()>) -> Result<AuthVerdict, Error> {
        let metadata = req.metadata();
        let Some(auth_header) = metadata.get("authorization") else {
            return Ok(AuthVerdict::Deny);
        };
        let Ok(auth_str) = auth_header.to_str() else {
            return Ok(AuthVerdict::Deny);
        };
        let Some(token) = auth_str.strip_prefix("Bearer ") else {
            return Ok(AuthVerdict::Deny);
        };
        Ok(self.validate_token(token))
    }
}
