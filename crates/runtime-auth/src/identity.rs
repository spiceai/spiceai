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

use std::collections::HashMap;

/// Rich identity information extracted from authentication.
///
/// Populated from API key metadata, OIDC token claims, or external identity providers.
/// Exposed to SQL queries via `current_user_id()`, `current_org_id()`, `current_role()`,
/// and `session_property(key)` UDFs.
#[derive(Debug, Clone)]
pub struct IdentityContext {
    /// Primary user identifier (e.g., `sub` claim from OIDC, or API key identifier).
    pub user_id: String,
    /// Organization/tenant identifier (e.g., from a custom OIDC claim).
    pub org_id: Option<String>,
    /// Role memberships extracted from the auth source.
    pub roles: Vec<String>,
    /// All validated claims as key-value pairs, accessible via `session_property()`.
    pub claims: HashMap<String, serde_json::Value>,
}

impl IdentityContext {
    #[must_use]
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            org_id: None,
            roles: Vec::new(),
            claims: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_org_id(mut self, org_id: impl Into<String>) -> Self {
        self.org_id = Some(org_id.into());
        self
    }

    #[must_use]
    pub fn with_org_id_opt(mut self, org_id: Option<String>) -> Self {
        self.org_id = org_id;
        self
    }

    #[must_use]
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    #[must_use]
    pub fn with_claims(mut self, claims: HashMap<String, serde_json::Value>) -> Self {
        self.claims = claims;
        self
    }
}

/// Convert a JSON claim value to a string representation suitable for SQL.
///
/// Strings are returned as-is, numbers/booleans are stringified,
/// and other types (arrays, objects) use JSON serialization.
#[must_use]
pub fn claim_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_context_builder() {
        let ctx = IdentityContext::new("user-123")
            .with_org_id("org-456")
            .with_roles(vec!["admin".into(), "viewer".into()]);

        assert_eq!(ctx.user_id, "user-123");
        assert_eq!(ctx.org_id.as_deref(), Some("org-456"));
        assert_eq!(ctx.roles, vec!["admin", "viewer"]);
        assert!(ctx.claims.is_empty());
    }

    #[test]
    fn test_identity_context_with_claims() {
        let mut claims = HashMap::new();
        claims.insert(
            "email".to_string(),
            serde_json::Value::String("user@example.com".to_string()),
        );

        let ctx = IdentityContext::new("user-123").with_claims(claims);
        assert_eq!(
            ctx.claims.get("email").and_then(|v| v.as_str()),
            Some("user@example.com")
        );
    }
}
