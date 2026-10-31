/*
Copyright 2025 The Spice.ai OSS Authors

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
use crate::acceleration::refresh::common::{
    execute_rt_sql, get_acceleration_config_append, get_acceleration_config_full,
    get_dataset_no_time_column, initialize_postgres, test_append_iso8601_for_engine,
    test_append_timestamp_for_engine, test_append_unix_seconds_for_engine,
};
use crate::postgres::common;
use crate::{
    configure_test_datafusion, configure_test_datafusion_request_context, init_tracing,
    utils::{register_test_connectors, test_request_context, wait_until_true},
};
use datafusion::common::TableReference;
use runtime::status::ComponentStatus;
use spicepod::acceleration::Mode;
use spicepod::param::Params;
use spicepod::partitioning::PartitionedBy;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Append refresh, `mode: file`, against the labeled `PostgreSQL` source.
///
/// Compares schema and the row multiset for the projection, one aggregation, and one
/// self-join. `created_at` is UTC epoch milliseconds. The statements do not order rows
/// and do not divide integers. A probe row inserted after the last refresh must stay
/// out of the acceleration.
#[tokio::test]
async fn test_acceleration_refresh_cayenne_append() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    cayenne_refresh_matches_source(super::refresh_modes::RefreshCase::Append).await
}

/// Full refresh, `mode: file`, against the same source and the same three statements.
#[tokio::test]
async fn test_acceleration_refresh_cayenne_full() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    cayenne_refresh_matches_source(super::refresh_modes::RefreshCase::Full).await
}

async fn cayenne_refresh_matches_source(
    mode: super::refresh_modes::RefreshCase,
) -> Result<(), anyhow::Error> {
    // The directory has to outlive the runtime: Cayenne's file mode reads it after refresh.
    let (_temp_dir, params) = cayenne_params();
    super::refresh_modes::refresh_matches_postgres_source("cayenne", mode, move |_port| {
        let mut acceleration = match mode {
            super::refresh_modes::RefreshCase::Append => {
                get_acceleration_config_append("cayenne", Some(params))
            }
            super::refresh_modes::RefreshCase::Full => {
                get_acceleration_config_full("cayenne", Some(params))
            }
        };
        acceleration.mode = Mode::File;
        acceleration
    })
    .await
}

/// Cayenne append mode, `mode: file`, with both `primary_key` and `time_column`: the time
/// column selects the rows each refresh fetches, and the primary key deduplicates them on
/// insert (`on_conflict: upsert`). The rows after each refresh must match `PostgreSQL`; see
/// [`super::refresh_modes::upsert_history_matches_postgres_source`] for the history.
#[tokio::test]
async fn test_cayenne_append_mode_with_pk_and_time_column() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (_temp_dir, params) = cayenne_params();
    super::refresh_modes::upsert_history_matches_postgres_source("cayenne", move |_port| {
        let mut acceleration = get_acceleration_config_append("cayenne", Some(params));
        acceleration.mode = Mode::File;
        acceleration
    })
    .await
}

/// Regression test for #12779.
///
/// A partitioned Cayenne dataset with `on_conflict: upsert` and
/// `refresh_mode: append` drives the cross-partition append coordinator
/// (`CayennePartitionedAppendSink`). That coordinator moved the on-conflict
/// payload out of each receipt before it prepared the deferred manifest, and
/// the manifest step then found no reserved append sequence and failed every
/// append refresh with "Deferred append manifest has no reserved append
/// sequence".
///
/// The `partition_by` clause is the only difference from
/// `test_cayenne_append_mode_with_pk_and_time_column`: it routes writes through
/// the runtime cross-partition coordinator instead of the single-provider
/// insert path, so it exercises the coordinator's manifest-preparation ordering.
#[tokio::test]
async fn test_cayenne_partitioned_append_on_conflict_upsert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (_temp_dir, params) = cayenne_params();
    // Before the fix the initial load, itself a partitioned append, fails and the dataset
    // never becomes ready.
    super::refresh_modes::upsert_history_matches_postgres_source(
        "cayenne-partitioned",
        move |_port| {
            let mut acceleration = get_acceleration_config_append("cayenne", Some(params));
            acceleration.mode = Mode::File;
            acceleration.partition_by = vec![PartitionedBy {
                name: "id_bucket".to_string(),
                expression: "bucket(4, id)".to_string(),
            }];
            acceleration
        },
    )
    .await
}

#[tokio::test]
async fn test_cayenne_append_mode_requires_constraint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let running_container = common::start_postgres_docker_container().await?;
            let port = usize::from(running_container.host_port(5432)?);

            let _db_conn = initialize_postgres(port).await?;

            // Create unique temp directory for this test
            let temp_dir = tempfile::tempdir()?;
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            std::fs::create_dir_all(&metadata_dir)?;

            let mut params = HashMap::new();
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
            );

            let mut acceleration_config =
                get_acceleration_config_append("cayenne", Some(Params::from_string_map(params)));
            acceleration_config.mode = Mode::File;

            // Remove primary_key (combined with no time_column from the dataset)
            // so that append mode has neither constraint - this should cause
            // dataset initialization to fail.
            acceleration_config.primary_key = None;

            // Create the dataset with invalid configuration (no time_column)
            let mut dataset = get_dataset_no_time_column(port);
            dataset.acceleration = Some(acceleration_config);

            register_test_connectors().await;

            let app = app::AppBuilder::new("test_acceleration_refresh")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            configure_test_datafusion_request_context();

            let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);

            // Drive load_components on a spawned task so initialization actually
            // runs to completion of the first attempt. It retries indefinitely on
            // failure, so the task won't exit on its own — we abort it after
            // shutdown below.
            let load_handle = tokio::spawn({
                let rt = Arc::clone(&rt);
                async move {
                    rt.load_components().await;
                }
            });

            // Wait for the dataset to reach the Error state, signaling that the
            // first initialization attempt has failed (as expected).
            let ds_name = TableReference::from("test_table");
            let entered_error = wait_until_true(Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                let ds_name = ds_name.clone();
                async move {
                    rt.status()
                        .get_dataset_statuses()
                        .get(&ds_name)
                        .is_some_and(|s| matches!(s, ComponentStatus::Error(_)))
                }
            })
            .await;
            assert!(
                entered_error,
                "Dataset never entered Error state; validation may not have fired"
            );

            // Verify that the dataset is not available (failed to initialize)
            let result = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await;
            assert!(
                result.is_err(),
                "Expected query to fail because dataset should not be initialized"
            );

            let err_msg = result.expect_err("Expected error").to_string();
            assert!(
                err_msg.contains("test_table")
                    || err_msg.contains("not found")
                    || err_msg.contains("'datafusion.catalog.spice.public.test_table' not found"),
                "Error message should indicate table not found, got: {err_msg}"
            );
            tracing::info!("Validation correctly rejects append mode without constraints");

            // Stop the infinite-retry loop and release runtime resources before
            // dropping the docker container, then drop the load_components task.
            rt.shutdown().await;
            load_handle.abort();

            running_container.cleanup().await;
            Ok(())
        })
        .await
}

fn cayenne_params() -> (tempfile::TempDir, Params) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let metadata_dir = temp_dir.path().join("cayenne_metadata");
    std::fs::create_dir_all(&metadata_dir).expect("mkdir");
    let mut map = HashMap::new();
    map.insert(
        "cayenne_metadata_dir".to_string(),
        metadata_dir.to_str().expect("valid UTF-8 path").to_string(),
    );
    (temp_dir, Params::from_string_map(map))
}

#[tokio::test]
async fn test_cayenne_append_mode_timestamp() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (_td, params) = cayenne_params();
    test_append_timestamp_for_engine("cayenne", Some(Mode::File), Some(params)).await
}

#[tokio::test]
async fn test_cayenne_append_mode_unix_seconds() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (_td, params) = cayenne_params();
    test_append_unix_seconds_for_engine("cayenne", Some(Mode::File), Some(params)).await
}

#[tokio::test]
async fn test_cayenne_append_mode_iso8601() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (_td, params) = cayenne_params();
    test_append_iso8601_for_engine("cayenne", Some(Mode::File), Some(params)).await
}
