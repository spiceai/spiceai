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

//! Integration tests for Databricks SQL Warehouse permissions and warehouse-type
//! interactions, mirroring the nine cases exercised by
//! `data/test-app-databricks/spicepod.yaml`.
//!
//! **Required environment variables** (all must be non-empty):
//!   - `TEST_DATABRICKS_HOST`
//!   - `TEST_DATABRICKS_PRO_WAREHOUSE_ID`        — a Pro/Serverless warehouse
//!   - `TEST_DATABRICKS_CLASSIC_WAREHOUSE_ID`    — a Classic warehouse
//!   - `TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_ID`
//!   - `TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_SECRET`
//!   - `TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_ID`
//!   - `TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_SECRET`
//!   - `TEST_DATABRICKS_TOKEN_CLASSIC`           — PAT with access to the Classic warehouse
//!
//! The test catalog/schema layout expected in the Databricks workspace:
//!   - `test_scp_permissions.test_scp.table_case1`          — UC managed table
//!   - `test_scp_permissions.test_scp.table_case2`          — UC managed table
//!   - `neon_pg_foreign_viktor.public.test_schema_repro`    — Lakehouse Federation foreign table
//!   - `neon_pg_foreign_viktor.public.test_schema_repro_2`  — Lakehouse Federation foreign table

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use datafusion::sql::TableReference;
use runtime::{Runtime, status::ComponentStatus};
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, test_request_context},
};

const LOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[expect(clippy::expect_used)]
fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

#[derive(Clone, Copy)]
enum Sp {
    WithInfoSchema,
    WithoutInfoSchema,
}

#[derive(Clone, Copy)]
enum Warehouse {
    Pro,
    Classic,
}

#[derive(Clone, Copy)]
enum Auth {
    ServicePrincipal(Sp),
    /// PAT token — used for the Classic-warehouse foreign-table case.
    TokenClassic,
}

fn make_params(auth: Auth, warehouse: Warehouse) -> Params {
    let endpoint = env("TEST_DATABRICKS_HOST");
    let sql_warehouse_id = match warehouse {
        Warehouse::Pro => env("TEST_DATABRICKS_PRO_WAREHOUSE_ID"),
        Warehouse::Classic => env("TEST_DATABRICKS_CLASSIC_WAREHOUSE_ID"),
    };

    let mut params = vec![
        ("mode".to_string(), "sql_warehouse".to_string()),
        ("databricks_endpoint".to_string(), endpoint),
        ("databricks_sql_warehouse_id".to_string(), sql_warehouse_id),
        ("client_timeout".to_string(), "120s".to_string()),
    ];

    match auth {
        Auth::ServicePrincipal(Sp::WithInfoSchema) => {
            params.push((
                "databricks_client_id".to_string(),
                env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_ID"),
            ));
            params.push((
                "databricks_client_secret".to_string(),
                env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_SECRET"),
            ));
        }
        Auth::ServicePrincipal(Sp::WithoutInfoSchema) => {
            params.push((
                "databricks_client_id".to_string(),
                env("TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_ID"),
            ));
            params.push((
                "databricks_client_secret".to_string(),
                env("TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_SECRET"),
            ));
        }
        Auth::TokenClassic => {
            params.push((
                "databricks_token".to_string(),
                env("TEST_DATABRICKS_TOKEN_CLASSIC"),
            ));
        }
    }

    Params::from_string_map(params.into_iter().collect())
}

fn make_dataset(path: &str, name: &str, auth: Auth, warehouse: Warehouse) -> Dataset {
    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(make_params(auth, warehouse));
    dataset
}

async fn load_with_timeout(rt: &Runtime) -> Result<(), anyhow::Error> {
    let cloned_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(LOAD_TIMEOUT) => {
            Err(anyhow::anyhow!("Timed out waiting for datasets to load"))
        }
        () = cloned_rt.load_components() => Ok(()),
    }
}

fn dataset_status(rt: &Runtime, name: &str) -> Option<ComponentStatus> {
    rt.status()
        .get_dataset_statuses()
        .get(&TableReference::parse_str(name))
        .cloned()
}

fn assert_ready(rt: &Runtime, name: &str) {
    let status = dataset_status(rt, name);
    assert!(
        matches!(status, Some(ComponentStatus::Ready)),
        "Dataset '{name}' should be Ready but was {status:?}"
    );
}

fn assert_error(rt: &Runtime, name: &str, expected_substring: Option<&str>) {
    let status = dataset_status(rt, name);
    match &status {
        Some(ComponentStatus::Error(msg)) => {
            if let Some(expected) = expected_substring {
                let rendered = msg.as_deref().unwrap_or("");
                assert!(
                    rendered.contains(expected),
                    "Error for '{name}' should contain {expected:?} but was: {rendered:?}"
                );
            }
        }
        other => panic!("Dataset '{name}' should be in Error state but was {other:?}"),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

async fn run_single_dataset_test<F>(
    test_name: &str,
    dataset: Dataset,
    check: F,
) -> Result<(), anyhow::Error>
where
    F: FnOnce(&Runtime),
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async move {
            let app = AppBuilder::new(test_name).with_dataset(dataset).build();
            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            load_with_timeout(&rt).await?;
            check(&rt);
            Ok(())
        })
        .await
}

// ── UC-Native tests ────────────────────────────────────────────────────────

/// Case 1: UC-native table, SP with both info_schema and table access → Ready.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn uc_native_full_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_uc_native_full_access",
        make_dataset(
            "test_scp_permissions.test_scp.table_case1",
            "uc_native_full_access",
            Auth::ServicePrincipal(Sp::WithInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_ready(rt, "uc_native_full_access"),
    )
    .await
}

/// Case 2: UC-native table, SP has info_schema access but NOT table access → Error.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn uc_native_infoschema_access_without_table_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_uc_native_infoschema_only",
        make_dataset(
            "test_scp_permissions.test_scp.table_case2",
            "uc_native_infoschema_only",
            Auth::ServicePrincipal(Sp::WithInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_error(rt, "uc_native_infoschema_only", None),
    )
    .await
}

/// Case 3: UC-native table, SP has table access but NOT info_schema access → Ready (fallback to DESCRIBE TABLE).
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn uc_native_table_access_without_infoschema_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_uc_native_table_only",
        make_dataset(
            "test_scp_permissions.test_scp.table_case1",
            "uc_native_table_only",
            Auth::ServicePrincipal(Sp::WithoutInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_ready(rt, "uc_native_table_only"),
    )
    .await
}

/// Case 4: UC-native table, SP has no access → Error.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn uc_native_no_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_uc_native_no_access",
        make_dataset(
            "test_scp_permissions.test_scp.table_case2",
            "uc_native_no_access",
            Auth::ServicePrincipal(Sp::WithoutInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_error(rt, "uc_native_no_access", None),
    )
    .await
}

// ── Foreign (Lakehouse Federation) tests ───────────────────────────────────

/// Case 5: Foreign table, SP with both info_schema and table access → Ready.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn foreign_full_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_foreign_full_access",
        make_dataset(
            "neon_pg_foreign_viktor.public.test_schema_repro",
            "foreign_full_access",
            Auth::ServicePrincipal(Sp::WithInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_ready(rt, "foreign_full_access"),
    )
    .await
}

/// Case 6: Foreign table, SP has info_schema access but NOT table access → Error.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn foreign_infoschema_access_without_table_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_foreign_infoschema_only",
        make_dataset(
            "neon_pg_foreign_viktor.public.test_schema_repro_2",
            "foreign_infoschema_only",
            Auth::ServicePrincipal(Sp::WithInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_error(rt, "foreign_infoschema_only", None),
    )
    .await
}

/// Case 7: Foreign table, SP has table access but NOT info_schema access → Ready (fallback).
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn foreign_table_access_without_infoschema_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_foreign_table_only",
        make_dataset(
            "neon_pg_foreign_viktor.public.test_schema_repro",
            "foreign_table_only",
            Auth::ServicePrincipal(Sp::WithoutInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_ready(rt, "foreign_table_only"),
    )
    .await
}

/// Case 8: Foreign table, SP has no access → Error.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn foreign_no_access() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_foreign_no_access",
        make_dataset(
            "neon_pg_foreign_viktor.public.test_schema_repro_2",
            "foreign_no_access",
            Auth::ServicePrincipal(Sp::WithoutInfoSchema),
            Warehouse::Pro,
        ),
        |rt| assert_error(rt, "foreign_no_access", None),
    )
    .await
}

// ── Classic warehouse + foreign table ──────────────────────────────────────

/// Case 9: Foreign table on a Classic SQL warehouse → Error with actionable
/// message mentioning "Lakehouse Federation foreign table".
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn foreign_on_classic_warehouse_surfaces_clear_error() -> Result<(), anyhow::Error> {
    run_single_dataset_test(
        "databricks_permissions_foreign_on_classic",
        make_dataset(
            "neon_pg_foreign_viktor.public.test_schema_repro",
            "foreign_on_classic",
            Auth::TokenClassic,
            Warehouse::Classic,
        ),
        |rt| {
            assert_error(
                rt,
                "foreign_on_classic",
                Some("Lakehouse Federation foreign table"),
            );
        },
    )
    .await
}
