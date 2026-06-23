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

//! Local policy provider — manages named Cedar policy sets.

use std::collections::HashMap;

use cedar_policy::PolicySet;

use crate::engine::parse_policies;
use crate::error::Error;

use super::PolicyProvider;

pub struct InMemoryPolicyProvider {
    policies: HashMap<String, PolicySet>,
}

impl InMemoryPolicyProvider {
    /// Create a provider from raw Cedar policy strings.
    ///
    /// # Errors
    ///
    /// Returns an error if the Cedar policy text cannot be parsed.
    pub fn try_new(name: impl Into<String>, policies: Vec<String>) -> Result<Self, Error> {
        let mut named_policies = HashMap::new();
        named_policies.insert(name.into(), Self::parse_policy_strings(policies)?);

        Ok(Self {
            policies: named_policies,
        })
    }

    fn parse_policy_strings(policies: Vec<String>) -> Result<PolicySet, Error> {
        let mut combined = String::new();

        for policy in policies {
            combined.push_str(&policy);
            if !combined.ends_with('\n') {
                combined.push('\n');
            }
        }

        parse_policies(&combined)
    }

    /// Replace or insert a named group of policies.
    ///
    /// # Errors
    ///
    /// Returns an error if the Cedar policy text cannot be parsed.
    pub fn update_policy(
        &mut self,
        name: impl Into<String>,
        policies: Vec<String>,
    ) -> Result<(), Error> {
        self.policies
            .insert(name.into(), Self::parse_policy_strings(policies)?);
        Ok(())
    }

    pub(crate) fn merged_policy_set(&self) -> Result<PolicySet, Error> {
        let mut combined = PolicySet::new();
        let mut policy_names = self.policies.keys().cloned().collect::<Vec<_>>();
        policy_names.sort_unstable();

        for policy_name in policy_names {
            let Some(policy_set) = self.policies.get(&policy_name) else {
                continue;
            };

            combined
                .merge(policy_set, true)
                .map_err(|e| Error::PolicySetMerge {
                    name: policy_name,
                    reason: e.to_string(),
                })?;
        }

        Ok(combined)
    }
}

#[async_trait::async_trait]
impl PolicyProvider for InMemoryPolicyProvider {
    async fn fetch_policies(&self) -> Result<PolicySet, Error> {
        self.merged_policy_set()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_policy() {
        let provider = InMemoryPolicyProvider::try_new(
            "initial",
            vec![r#"permit(principal, action == Spice::Action::"query", resource);"#.to_string()],
        )
        .expect("provider should build");

        let policies = provider.merged_policy_set().expect("policies should parse");
        assert_eq!(policies.policies().count(), 1);
    }

    #[test]
    fn test_update_policy_replaces_named_policy_set() {
        let mut provider = InMemoryPolicyProvider::try_new(
            "initial",
            vec![r#"permit(principal, action == Spice::Action::"read", resource);"#.to_string()],
        )
        .expect("provider should build");

        provider
            .update_policy(
                "test",
                vec![
                    r#"permit(principal, action == Spice::Action::"query", resource);"#.to_string(),
                    r#"forbid(principal, action == Spice::Action::"read", resource);"#.to_string(),
                ],
            )
            .expect("updated policies should parse");

        let updated = provider
            .merged_policy_set()
            .expect("updated policies should merge");
        assert_eq!(updated.policies().count(), 3);
    }

    #[test]
    fn test_update_policy_replaces_existing_name() {
        let mut provider =
            InMemoryPolicyProvider::try_new("initial", vec![]).expect("provider should build");

        provider
            .update_policy(
                "test",
                vec![
                    r#"permit(principal, action == Spice::Action::"read", resource);"#.to_string(),
                ],
            )
            .expect("initial update should parse");
        provider
            .update_policy(
                "test",
                vec![
                    r#"forbid(principal, action == Spice::Action::"read", resource);"#.to_string(),
                ],
            )
            .expect("replacement update should parse");

        let updated = provider
            .merged_policy_set()
            .expect("updated policies should merge");
        assert_eq!(updated.policies().count(), 1);
    }
}
