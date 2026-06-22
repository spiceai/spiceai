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

//! Integration tests for Databricks SQL Warehouse schema discovery across
//! different permission levels and warehouse types.
//!
//! All Pro-warehouse cases (1–8) load in a **single Runtime** so the
//! warehouse only needs to wake once. The Classic-warehouse case (9) runs
//! separately because it uses a different warehouse + auth.
//!
//! **Required environment variables:**
//!   - `TEST_DATABRICKS_PERMISSIONS_HOST`
//!   - `TEST_DATABRICKS_PRO_WAREHOUSE_ID`
//!   - `TEST_DATABRICKS_CLASSIC_WAREHOUSE_ID`
//!   - `TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_ID`
//!   - `TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_SECRET`
//!   - `TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_ID`
//!   - `TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_SECRET`

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

/// 15 minutes — enough for a cold warehouse to wake up + run queries.
const LOAD_TIMEOUT: Duration = Duration::from_mins(15);

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

fn make_sp_dataset(path: &str, name: &str, with_infoschema: bool) -> Dataset {
    let endpoint = env("TEST_DATABRICKS_PERMISSIONS_HOST");
    let warehouse_id = env("TEST_DATABRICKS_PRO_WAREHOUSE_ID");
    let (client_id, client_secret) = if with_infoschema {
        (
            env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_ID"),
            env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_SECRET"),
        )
    } else {
        (
            env("TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_ID"),
            env("TEST_DATABRICKS_SP_WITHOUT_INFOSCHEMA_CLIENT_SECRET"),
        )
    };

    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![
            ("mode".to_string(), "sql_warehouse".to_string()),
            ("databricks_endpoint".to_string(), endpoint),
            ("databricks_sql_warehouse_id".to_string(), warehouse_id),
            ("databricks_client_id".to_string(), client_id),
            ("databricks_client_secret".to_string(), client_secret),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

fn make_classic_dataset(path: &str, name: &str) -> Dataset {
    let endpoint = env("TEST_DATABRICKS_PERMISSIONS_HOST");
    let warehouse_id = env("TEST_DATABRICKS_CLASSIC_WAREHOUSE_ID");
    let client_id = env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_ID");
    let client_secret = env("TEST_DATABRICKS_SP_WITH_INFOSCHEMA_CLIENT_SECRET");

    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(Params::from_string_map(
        vec![
            ("mode".to_string(), "sql_warehouse".to_string()),
            ("databricks_endpoint".to_string(), endpoint),
            ("databricks_sql_warehouse_id".to_string(), warehouse_id),
            ("databricks_client_id".to_string(), client_id),
            ("databricks_client_secret".to_string(), client_secret),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
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

// ── Pro warehouse: all 8 permission cases in one Runtime ───────────────────

/// Cases 1–8: UC-native and foreign tables with varying service principal
/// permissions, all using the Pro SQL warehouse. Loads all datasets in a
/// single Runtime so the warehouse only wakes once.
///
/// | Case | Table type | SP              | Expected |
/// |------|-----------|-----------------|----------|
/// | 1    | UC-native | with_infoschema | Ready    |
/// | 2    | UC-native | with_infoschema (no table access) | Error |
/// | 3    | UC-native | without_infoschema | Ready (DESCRIBE fallback) |
/// | 4    | UC-native | without_infoschema (no table access) | Error |
/// | 5    | Foreign   | with_infoschema | Ready    |
/// | 6    | Foreign   | with_infoschema (no table access) | Error |
/// | 7    | Foreign   | without_infoschema | Ready (DESCRIBE fallback) |
/// | 8    | Foreign   | without_infoschema (no table access) | Error |
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_permissions_pro_warehouse() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_permissions_pro")
                // Case 1: UC-native, full access
                .with_dataset(make_sp_dataset(
                    "test_scp_permissions.test_scp.table_case1",
                    "uc_native_full_access",
                    true,
                ))
                // Case 2: UC-native, infoschema but no table access
                .with_dataset(make_sp_dataset(
                    "test_scp_permissions.test_scp.table_case2",
                    "uc_native_infoschema_only",
                    true,
                ))
                // Case 3: UC-native, table but no infoschema access
                .with_dataset(make_sp_dataset(
                    "test_scp_permissions.test_scp.table_case1",
                    "uc_native_table_only",
                    false,
                ))
                // Case 4: UC-native, no access
                .with_dataset(make_sp_dataset(
                    "test_scp_permissions.test_scp.table_case2",
                    "uc_native_no_access",
                    false,
                ))
                // Case 5: Foreign, full access
                .with_dataset(make_sp_dataset(
                    "spiceai_sandbox_via_serverless.tpch.region",
                    "foreign_full_access",
                    true,
                ))
                // Case 6: Foreign, infoschema but no table access
                .with_dataset(make_sp_dataset(
                    "spiceai_sandbox_via_serverless.tpch.nation",
                    "foreign_infoschema_only",
                    true,
                ))
                // Case 7: Foreign, table but no infoschema access
                .with_dataset(make_sp_dataset(
                    "spiceai_sandbox_via_serverless.tpch.region",
                    "foreign_table_only",
                    false,
                ))
                // Case 8: Foreign, no access
                .with_dataset(make_sp_dataset(
                    "spiceai_sandbox_via_serverless.tpch.nation",
                    "foreign_no_access",
                    false,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            load_with_timeout(&rt).await?;

            // Cases that should succeed
            assert_ready(&rt, "uc_native_full_access"); // Case 1
            assert_ready(&rt, "uc_native_table_only"); // Case 3
            assert_ready(&rt, "foreign_full_access"); // Case 5
            assert_ready(&rt, "foreign_table_only"); // Case 7

            // Cases that should fail
            assert_error(&rt, "uc_native_infoschema_only", None); // Case 2
            assert_error(&rt, "uc_native_no_access", None); // Case 4
            assert_error(&rt, "foreign_infoschema_only", None); // Case 6
            assert_error(&rt, "foreign_no_access", None); // Case 8

            Ok(())
        })
        .await
}

// ── Classic warehouse: foreign table error detection ───────────────────────

/// Case 9: Foreign table on a Classic SQL warehouse → Error with actionable
/// message mentioning "Lakehouse Federation foreign table".
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn databricks_permissions_classic_foreign_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("databricks_permissions_classic_foreign")
                .with_dataset(make_classic_dataset(
                    "spiceai_sandbox_via_serverless.tpch.region",
                    "foreign_on_classic",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            load_with_timeout(&rt).await?;

            assert_error(
                &rt,
                "foreign_on_classic",
                Some("Lakehouse Federation foreign table"),
            );

            Ok(())
        })
        .await
}
