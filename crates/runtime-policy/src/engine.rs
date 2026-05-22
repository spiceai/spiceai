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

//! Core policy engine wrapping Cedar's [`Authorizer`].
//!
//! The [`PolicyEngine`] holds the Cedar authorizer, policy set, and schema
//! behind an `Arc<RwLock<>>` for concurrent evaluation and hot-reload.

use std::sync::Arc;

use cedar_policy::{Authorizer, Decision, PolicySet, Schema};
use tokio::sync::RwLock;

use crate::compile::{AccessPlan, compile_access_plan, validate_policy_annotations};
use crate::entities::{SpiceResource, build_entities};
use crate::error::Error;
use crate::request::build_request;
use crate::schema::build_schema;
use runtime_auth::AuthPrincipalRef;

/// The result of an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzDecision {
    /// The request is allowed.
    Allow,
    /// The request is denied, with the set of policy IDs that caused the denial.
    Deny { reasons: Vec<String> },
}

impl AuthzDecision {
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, AuthzDecision::Allow)
    }
}

struct PolicyEngineInner {
    authorizer: Authorizer,
    policy_set: PolicySet,
    schema: Schema,
}

/// Cedar-based authorization policy engine.
///
/// Thread-safe: multiple requests evaluate concurrently under a read lock.
/// Policy reloads acquire a write lock briefly to swap the policy set.
#[derive(Clone)]
pub struct PolicyEngine {
    inner: Arc<RwLock<PolicyEngineInner>>,
}

impl PolicyEngine {
    /// Create a new policy engine with the given initial policies.
    ///
    /// # Errors
    ///
    /// Returns an error if the Cedar schema fails to build.
    pub fn new(initial_policies: PolicySet) -> Result<Self, Error> {
        validate_policy_annotations(&initial_policies)?;
        let schema = build_schema()?;
        let authorizer = Authorizer::new();

        Ok(Self {
            inner: Arc::new(RwLock::new(PolicyEngineInner {
                authorizer,
                policy_set: initial_policies,
                schema,
            })),
        })
    }

    /// Evaluate whether the given principal is authorized to perform the action
    /// on the resource.
    ///
    /// This acquires a read lock — concurrent evaluations are fully parallel.
    pub async fn is_authorized(
        &self,
        principal: &AuthPrincipalRef,
        action: &str,
        resource: &SpiceResource,
    ) -> AuthzDecision {
        let inner = self.inner.read().await;

        let entities = match build_entities(principal, resource) {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Failed to build Cedar entities: {e}");
                return AuthzDecision::Deny {
                    reasons: vec![format!("entity build error: {e}")],
                };
            }
        };

        let request = match build_request(principal, action, resource, &inner.schema) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to build Cedar request: {e}");
                return AuthzDecision::Deny {
                    reasons: vec![format!("request build error: {e}")],
                };
            }
        };

        let response = inner
            .authorizer
            .is_authorized(&request, &inner.policy_set, &entities);

        match response.decision() {
            Decision::Allow => AuthzDecision::Allow,
            Decision::Deny => {
                let reasons: Vec<String> = response
                    .diagnostics()
                    .reason()
                    .map(std::string::ToString::to_string)
                    .collect();
                AuthzDecision::Deny { reasons }
            }
        }
    }

    /// Evaluate fine-grained read access for a dataset.
    ///
    /// The returned [`AccessPlan`] contains SQL row filters and column masks
    /// from the permit policies that allowed the `read` action.
    ///
    /// # Errors
    ///
    /// Returns an error if Cedar entities or the request cannot be built, if
    /// Cedar reports policy evaluation diagnostics, or if matching policy
    /// annotations cannot be compiled into an access plan.
    pub async fn evaluate_read_access(
        &self,
        principal: &AuthPrincipalRef,
        resource: &SpiceResource,
    ) -> Result<AccessPlan, Error> {
        let inner = self.inner.read().await;

        let entities = build_entities(principal, resource)?;
        let request = build_request(
            principal,
            crate::request::SpiceAction::READ,
            resource,
            &inner.schema,
        )?;

        let response = inner
            .authorizer
            .is_authorized(&request, &inner.policy_set, &entities);

        let errors = response
            .diagnostics()
            .errors()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            return Err(Error::PolicyEvaluation {
                reason: errors.join(", "),
            });
        }

        let policy_ids = response
            .diagnostics()
            .reason()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>();

        match response.decision() {
            Decision::Allow => {
                let mut plan = compile_access_plan(
                    &inner.policy_set,
                    response.diagnostics().reason(),
                    resource,
                )?;
                plan.policy_ids = policy_ids;
                Ok(plan)
            }
            Decision::Deny => Ok(AccessPlan {
                allowed: false,
                policy_ids,
                ..AccessPlan::default()
            }),
        }
    }

    /// Replace the current policy set with new policies.
    ///
    /// This acquires a write lock briefly. In-flight authorization evaluations
    /// complete with the old policy set; subsequent evaluations use the new one.
    ///
    /// # Errors
    ///
    /// Returns an error if the replacement policy set contains invalid
    /// fine-grained policy annotations.
    pub async fn reload(&self, new_policies: PolicySet) -> Result<(), Error> {
        validate_policy_annotations(&new_policies)?;
        let mut inner = self.inner.write().await;
        tracing::info!(
            policy_count = new_policies.policies().count(),
            "Reloading Cedar policy set"
        );
        inner.policy_set = new_policies;
        Ok(())
    }

    /// Returns the number of policies currently loaded.
    pub async fn policy_count(&self) -> usize {
        self.inner.read().await.policy_set.policies().count()
    }
}

/// The default-allow policy: permits all authenticated requests unless
/// overridden by a `forbid` policy.
pub const DEFAULT_ALLOW_POLICY: &str = r"permit(principal, action, resource);";

/// Parse a Cedar policy text into a [`PolicySet`].
///
/// # Errors
///
/// Returns an error if the Cedar policy text is syntactically invalid.
pub fn parse_policies(cedar_text: &str) -> Result<PolicySet, Error> {
    cedar_text
        .parse::<PolicySet>()
        .map_err(|e| Error::PolicyParse {
            reason: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime_auth::{AuthPrincipal, identity::IdentityContext};

    use super::*;
    use crate::entities::SpiceResource;

    struct TestPrincipal {
        identity: IdentityContext,
    }

    impl AuthPrincipal for TestPrincipal {
        fn username(&self) -> &str {
            &self.identity.user_id
        }
        fn groups(&self) -> &[&str] {
            &[]
        }
        fn identity_context(&self) -> Option<&IdentityContext> {
            Some(&self.identity)
        }
    }

    fn make_principal(user_id: &str, roles: Vec<String>) -> AuthPrincipalRef {
        Arc::new(TestPrincipal {
            identity: IdentityContext::new(user_id).with_roles(roles),
        })
    }

    #[tokio::test]
    async fn test_default_allow_permits_everything() {
        let policies = parse_policies(DEFAULT_ALLOW_POLICY).expect("valid policy");
        let engine = PolicyEngine::new(policies).expect("engine should build");

        let principal = make_principal("alice", vec!["analyst".into()]);
        let resource = SpiceResource::Dataset {
            name: "sales".to_string(),
            catalog: None,
            schema: None,
        };

        let decision = engine.is_authorized(&principal, "query", &resource).await;
        assert!(decision.is_allowed());
    }

    #[tokio::test]
    async fn test_forbid_overrides_permit() {
        let policy_text = r#"
            permit(principal, action, resource);
            forbid(
                principal,
                action == Spice::Action::"query",
                resource == Spice::Dataset::"pii_table"
            );
        "#;
        let policies = parse_policies(policy_text).expect("valid policy");
        let engine = PolicyEngine::new(policies).expect("engine should build");

        let principal = make_principal("alice", vec!["analyst".into()]);

        // pii_table should be denied
        let decision = engine
            .is_authorized(
                &principal,
                "query",
                &SpiceResource::Dataset {
                    name: "pii_table".to_string(),
                    catalog: None,
                    schema: None,
                },
            )
            .await;
        assert!(!decision.is_allowed());

        // other tables should be allowed
        let decision = engine
            .is_authorized(
                &principal,
                "query",
                &SpiceResource::Dataset {
                    name: "sales".to_string(),
                    catalog: None,
                    schema: None,
                },
            )
            .await;
        assert!(decision.is_allowed());
    }

    #[tokio::test]
    async fn test_role_based_policy() {
        let policy_text = r#"
            permit(
                principal in Spice::Role::"writer",
                action == Spice::Action::"insert",
                resource
            );
        "#;
        let policies = parse_policies(policy_text).expect("valid policy");
        let engine = PolicyEngine::new(policies).expect("engine should build");

        let resource = SpiceResource::Dataset {
            name: "logs".to_string(),
            catalog: None,
            schema: None,
        };

        // User with "writer" role should be allowed to insert
        let writer = make_principal("bob", vec!["writer".into()]);
        let decision = engine.is_authorized(&writer, "insert", &resource).await;
        assert!(decision.is_allowed());

        // User without "writer" role should be denied
        let reader = make_principal("charlie", vec!["reader".into()]);
        let decision = engine.is_authorized(&reader, "insert", &resource).await;
        assert!(!decision.is_allowed());
    }

    #[tokio::test]
    async fn test_reload_policies() {
        let initial = parse_policies(DEFAULT_ALLOW_POLICY).expect("valid policy");
        let engine = PolicyEngine::new(initial).expect("engine should build");

        let principal = make_principal("alice", vec![]);
        let resource = SpiceResource::Dataset {
            name: "secrets".to_string(),
            catalog: None,
            schema: None,
        };

        // Initially allowed
        assert!(
            engine
                .is_authorized(&principal, "query", &resource)
                .await
                .is_allowed()
        );

        // Reload with a deny-all policy
        let new_policies =
            parse_policies(r"forbid(principal, action, resource);").expect("valid policy");
        engine
            .reload(new_policies)
            .await
            .expect("reload should succeed");

        // Now denied
        assert!(
            !engine
                .is_authorized(&principal, "query", &resource)
                .await
                .is_allowed()
        );
    }
}
