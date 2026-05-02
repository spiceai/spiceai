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

//! Cedar policy enforcement for SQL queries.
//!
//! Walks the logical plan to identify the tables and operations being performed,
//! then checks each (principal, action, resource) triple against the policy engine.

use std::sync::Arc;

use datafusion::{
    common::tree_node::TreeNodeRecursion, error::DataFusionError, logical_expr::LogicalPlan,
};
use runtime_auth::AuthRequestContext;
use runtime_policy::{PolicyEngine, entities::SpiceResource, request::SpiceAction};
use runtime_request_context::{AsyncMarker, RequestContext};

/// Authorize a SQL query's logical plan against the Cedar policy engine.
///
/// Walks the plan to find all referenced tables and operations, then checks
/// each against the policy engine using the current request's principal.
///
/// Returns `Ok(())` if all operations are authorized, or an error describing
/// which resource/action was denied.
pub async fn authorize_query_plan(
    plan: &LogicalPlan,
    policy_engine: &Arc<PolicyEngine>,
) -> Result<(), DataFusionError> {
    let request_context = RequestContext::current(AsyncMarker::new().await);
    let Some(principal) = request_context.auth_principal() else {
        // No principal = unauthenticated request. The auth layer already
        // decided whether to allow or deny unauthenticated requests.
        // If we got here, the auth layer allowed it (e.g. no auth configured).
        return Ok(());
    };

    // Collect all (action, table) pairs from the plan.
    let mut checks: Vec<(&str, String, Option<String>, Option<String>)> = Vec::new();

    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::Dml(dml) => {
                let action = if let datafusion::logical_expr::WriteOp::Insert(_) = &dml.op {
                    SpiceAction::INSERT
                } else if matches!(&dml.op, datafusion::logical_expr::WriteOp::Delete) {
                    SpiceAction::DELETE
                } else {
                    SpiceAction::UPDATE
                };
                checks.push((
                    action,
                    dml.table_name.table().to_string(),
                    dml.table_name.catalog().map(ToString::to_string),
                    dml.table_name.schema().map(ToString::to_string),
                ));
            }
            LogicalPlan::TableScan(scan) => {
                checks.push((
                    SpiceAction::QUERY,
                    scan.table_name.table().to_string(),
                    scan.table_name.catalog().map(ToString::to_string),
                    scan.table_name.schema().map(ToString::to_string),
                ));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    // Evaluate each check against the policy engine.
    for (action, table_name, catalog, schema) in checks {
        let resource = SpiceResource::Dataset {
            name: table_name.clone(),
            catalog: catalog.clone(),
            schema: schema.clone(),
        };

        let decision = policy_engine
            .is_authorized(principal, action, &resource)
            .await;
        if !decision.is_allowed() {
            let reasons = match &decision {
                runtime_policy::AuthzDecision::Deny { reasons } if !reasons.is_empty() => {
                    format!(" (policies: {})", reasons.join(", "))
                }
                _ => String::new(),
            };
            return Err(DataFusionError::Plan(format!(
                "Authorization denied: action '{action}' on dataset '{table_name}' is not permitted for this user{reasons}",
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
    use runtime_auth::{AuthPrincipal, AuthRequestContext, identity::IdentityContext};
    use runtime_policy::PolicyEngine;
    use runtime_policy::engine::parse_policies;
    use runtime_request_context::{Protocol, RequestContext};

    use super::*;

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

    fn make_engine(cedar: &str) -> Arc<PolicyEngine> {
        let ps = parse_policies(cedar).expect("valid cedar policy");
        Arc::new(PolicyEngine::new(ps).expect("engine should build"))
    }

    fn make_scan_plan(table_name: &str) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table_source = Arc::new(LogicalTableSource::new(schema));
        LogicalPlanBuilder::scan(TableReference::bare(table_name), table_source, None)
            .expect("scan plan")
            .build()
            .expect("build plan")
    }

    async fn run_authz(
        engine: &Arc<PolicyEngine>,
        plan: &LogicalPlan,
        user_id: &str,
        roles: Vec<String>,
    ) -> Result<(), datafusion::error::DataFusionError> {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());
        let principal: runtime_auth::AuthPrincipalRef = Arc::new(TestPrincipal {
            identity: IdentityContext::new(user_id).with_roles(roles),
        });
        ctx.set_auth_principal(principal)
            .expect("set auth principal");
        ctx.scope(authorize_query_plan(plan, engine)).await
    }

    #[tokio::test]
    async fn test_default_allow_permits_query() {
        let engine = make_engine(r"permit(principal, action, resource);");
        let plan = make_scan_plan("sales");

        let result = run_authz(&engine, &plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");
    }

    #[tokio::test]
    async fn test_forbid_overrides_permit() {
        let engine = make_engine(
            r#"
            permit(principal, action, resource);
            forbid(
                principal,
                action == Spice::Action::"query",
                resource == Spice::Dataset::"pii_table"
            );
            "#,
        );

        let pii_plan = make_scan_plan("pii_table");
        let result = run_authz(&engine, &pii_plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect_err("expected Err");
        let err_msg = result.expect_err("should be denied").to_string();
        assert!(
            err_msg.contains("Authorization denied"),
            "expected denial message, got: {err_msg}"
        );

        // Other tables should still be allowed
        let ok_plan = make_scan_plan("sales");
        let result = run_authz(&engine, &ok_plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");
    }

    #[tokio::test]
    async fn test_role_based_access() {
        let engine = make_engine(
            r#"
            permit(
                principal in Spice::Role::"analyst",
                action == Spice::Action::"query",
                resource
            );
            "#,
        );
        let plan = make_scan_plan("reports");

        // analyst role: allowed
        let result = run_authz(&engine, &plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");

        // guest role: denied
        let result = run_authz(&engine, &plan, "bob", vec!["guest".into()]).await;
        result.as_ref().expect_err("expected Err");
    }

    #[tokio::test]
    async fn test_no_principal_skips_authz() {
        let engine = make_engine(r"forbid(principal, action, resource);");
        let plan = make_scan_plan("anything");

        // No principal set — should pass (auth layer decides unauthenticated access)
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());
        let result = ctx.scope(authorize_query_plan(&plan, &engine)).await;
        result.as_ref().expect("expected Ok");
    }
}
