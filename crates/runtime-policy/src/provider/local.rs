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

//! Local policy provider — reads Cedar policies from inline YAML definitions
//! and local `.cedar` file references.

use std::fmt::Write;
use std::path::PathBuf;

use cedar_policy::PolicySet;

use crate::engine::parse_policies;
use crate::error::Error;

use super::PolicyProvider;

/// A policy definition from spicepod.yaml configuration.
#[derive(Debug, Clone)]
pub struct PolicyDefinition {
    /// A human-readable name for this policy.
    pub name: String,
    /// Inline Cedar policy text.
    pub cedar: Option<String>,
    /// Path to a `.cedar` file (resolved relative to spicepod.yaml).
    pub path: Option<PathBuf>,
}

/// Reads policies from inline Cedar text and/or local `.cedar` files.
pub struct LocalPolicyProvider {
    policies: Vec<PolicyDefinition>,
    default_allow: bool,
}

impl LocalPolicyProvider {
    /// Create a new local provider.
    ///
    /// If `default_allow` is true, a built-in `permit(principal, action, resource)`
    /// policy is prepended so that unmatched requests are allowed by default.
    #[must_use]
    pub fn new(policies: Vec<PolicyDefinition>, default_allow: bool) -> Self {
        Self {
            policies,
            default_allow,
        }
    }
}

#[async_trait::async_trait]
impl PolicyProvider for LocalPolicyProvider {
    async fn fetch_policies(&self) -> Result<PolicySet, Error> {
        let mut combined = String::new();

        if self.default_allow {
            combined.push_str("// Built-in default-allow policy\n");
            combined.push_str(crate::engine::DEFAULT_ALLOW_POLICY);
            combined.push('\n');
        }

        for def in &self.policies {
            if let Some(cedar_text) = &def.cedar {
                let _ = writeln!(combined, "// Policy: {}", def.name);
                combined.push_str(cedar_text);
                combined.push('\n');
            }

            if let Some(path) = &def.path {
                let content =
                    tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| Error::PolicyFileRead {
                            path: path.display().to_string(),
                            source: e,
                        })?;
                let _ = writeln!(combined, "// Policy from file: {}", path.display());
                combined.push_str(&content);
                combined.push('\n');
            }
        }

        parse_policies(&combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_inline_policy() {
        let provider = LocalPolicyProvider::new(
            vec![PolicyDefinition {
                name: "test".to_string(),
                cedar: Some(
                    r#"permit(principal, action == Spice::Action::"query", resource);"#.to_string(),
                ),
                path: None,
            }],
            false,
        );

        let policies = provider.fetch_policies().await.expect("should parse");
        assert_eq!(policies.policies().count(), 1);
    }

    #[tokio::test]
    async fn test_default_allow_prepends_permit() {
        let provider = LocalPolicyProvider::new(vec![], true);
        let policies = provider.fetch_policies().await.expect("should parse");
        assert_eq!(policies.policies().count(), 1);
    }

    #[tokio::test]
    async fn test_missing_file_returns_error() {
        let provider = LocalPolicyProvider::new(
            vec![PolicyDefinition {
                name: "missing".to_string(),
                cedar: None,
                path: Some(PathBuf::from("/nonexistent/policy.cedar")),
            }],
            false,
        );

        let result = provider.fetch_policies().await;
        result.expect_err("expected missing file error");
    }
}
