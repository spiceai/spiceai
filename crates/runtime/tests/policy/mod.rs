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

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use app::AppBuilder;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use runtime::Runtime;
use runtime_auth::{AuthPrincipal, AuthRequestContext, identity::IdentityContext};
use runtime_request_context::{Protocol, RequestContext};
use spicepod::{
    component::{
        dataset::Dataset,
        runtime::{
            Authorization, AuthorizationDefault, AuthorizationProvider, PolicyDefinition,
            Runtime as SpicepodRuntime,
        },
    },
    param::{ParamValue, Params},
};
use tempfile::TempDir;
use tokio::{fs, time::timeout};

use crate::{configure_test_datafusion, init_tracing, utils};

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

fn request_context(user_id: &str, roles: Vec<&str>) -> Arc<RequestContext> {
    let context = Arc::new(RequestContext::builder(Protocol::Http).build());
    let principal: runtime_auth::AuthPrincipalRef = Arc::new(TestPrincipal {
        identity: IdentityContext::new(user_id)
            .with_roles(roles.into_iter().map(ToString::to_string).collect()),
    });
    context
        .set_auth_principal(principal)
        .expect("auth principal should be set once");
    context
}

async fn make_policy_runtime(cedar: impl Into<String>) -> Result<(Arc<Runtime>, TempDir)> {
    configure_test_datafusion();

    let temp_dir = TempDir::new().context("create policy integration temp dir")?;
    let csv_path = temp_dir.path().join("patients.csv");
    fs::write(
        &csv_path,
        "id,physician_id,ssn\n1,alice,111-11-1111\n2,bob,222-22-2222\n3,alice,333-33-3333\n",
    )
    .await
    .context("write patients CSV")?;

    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), "patients");
    dataset.params = Some(Params {
        data: HashMap::from([
            (
                "file_format".to_string(),
                ParamValue::String("csv".to_string()),
            ),
            (
                "csv_has_header".to_string(),
                ParamValue::String("true".to_string()),
            ),
        ]),
    });

    let app = AppBuilder::new("policy_integration")
        .with_runtime(SpicepodRuntime {
            authorization: Some(Authorization {
                enabled: true,
                default: AuthorizationDefault::Deny,
                provider: AuthorizationProvider::Local,
                policies: vec![PolicyDefinition {
                    name: "test-policy".to_string(),
                    cedar: Some(cedar.into()),
                    path: None,
                }],
                operator: None,
                cloud: None,
            }),
            ..Default::default()
        })
        .with_dataset(dataset)
        .build();

    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
    timeout(
        Duration::from_secs(60),
        Arc::clone(&runtime).load_components(),
    )
    .await
    .context("timed out loading runtime components")?;
    utils::runtime_ready_check(runtime.as_ref()).await;

    Ok((runtime, temp_dir))
}

async fn query_batches(
    runtime: &Arc<Runtime>,
    context: Arc<RequestContext>,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    context.scope(utils::run_query(runtime, sql)).await
}

fn assert_pretty_eq(expected: &[&str], batches: &[RecordBatch]) {
    let pretty = pretty_format_batches(batches).expect("format batches");
    assert_eq!(expected.join("\n"), pretty.to_string());
}

#[tokio::test]
async fn file_dataset_applies_row_filter_and_column_mask() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (runtime, _temp_dir) = make_policy_runtime(
        r#"
        @row_filter("physician_id = current_user_id()")
        @mask_ssn("'***'")
        permit(
            principal in Spice::Role::"physician",
            action == Spice::Action::"read",
            resource == Spice::Dataset::"patients"
        );
        "#,
    )
    .await?;

    let batches = query_batches(
        &runtime,
        request_context("alice", vec!["physician"]),
        "SELECT id, ssn FROM patients ORDER BY id",
    )
    .await?;

    assert_pretty_eq(
        &[
            "+----+-----+",
            "| id | ssn |",
            "+----+-----+",
            "| 1  | *** |",
            "| 3  | *** |",
            "+----+-----+",
        ],
        &batches,
    );

    Ok(())
}

#[tokio::test]
async fn read_forbid_denies_query_even_when_query_is_permitted() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (runtime, _temp_dir) = make_policy_runtime(
        r#"
        permit(
            principal in Spice::Role::"physician",
            action == Spice::Action::"query",
            resource == Spice::Dataset::"patients"
        );

        forbid(
            principal in Spice::Role::"physician",
            action == Spice::Action::"read",
            resource == Spice::Dataset::"patients"
        );
        "#,
    )
    .await?;

    let error = query_batches(
        &runtime,
        request_context("alice", vec!["physician"]),
        "SELECT id FROM patients",
    )
    .await
    .expect_err("read forbid must deny data access");
    let message = error.to_string();
    assert!(
        message.contains("Authorization denied: action 'read'"),
        "expected read denial, got: {message}"
    );

    Ok(())
}

#[tokio::test]
async fn row_filter_can_use_current_user_has_role_udf() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (runtime, _temp_dir) = make_policy_runtime(
        r#"
        @row_filter("current_user_has_role('auditor')")
        permit(
            principal,
            action == Spice::Action::"read",
            resource == Spice::Dataset::"patients"
        );
        "#,
    )
    .await?;

    let auditor_batches = query_batches(
        &runtime,
        request_context("alice", vec!["auditor"]),
        "SELECT id FROM patients ORDER BY id",
    )
    .await?;
    assert_pretty_eq(
        &[
            "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "+----+",
        ],
        &auditor_batches,
    );

    let non_auditor_batches = query_batches(
        &runtime,
        request_context("bob", vec!["physician"]),
        "SELECT id FROM patients ORDER BY id",
    )
    .await?;
    let row_count: usize = non_auditor_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(0, row_count, "non-auditors must not see any rows");

    Ok(())
}
