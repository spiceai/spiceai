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

//! Distributed (Ballista) integration tests for Iceberg scans, covering **both**
//! the Iceberg *data connector* (a single `datasets` entry) and the Iceberg
//! *catalog connector* (a `catalogs` entry).
//!
//! # What this guards against
//!
//! Iceberg's `IcebergTableScan` holds a live, non-serializable `Table`, so it
//! cannot cross a Ballista node boundary. PR #11378 fixed this for the **data
//! connector** by wrapping every Iceberg provider in an
//! `IcebergClusterTableProvider`, whose scan emits a serializable
//! `IcebergScanExec` in a distributed session. Catalog-sourced tables, however,
//! are built on a different path (`IcebergCatalogProvider` →
//! `IcebergSchemaProvider`) that never applied the wrapper, so a distributed
//! query over a catalog table failed during plan serialization with:
//!
//! ```text
//! DataFusion error: Internal error: Unsupported plan and extension codec failed
//! with [Internal error: unsupported plan type: IcebergTableScan { table: Table {
//! file_io: FileIO ... }]
//! ```
//!
//! The catalog connector now installs the same wrapper, and `get_table_sync`
//! resolves the catalog's `IcebergSchemaProvider` so a remote executor can
//! reconstruct the scan. The catalog test below is the direct regression for
//! that gap; the dataset test guards that the (already-working) data-connector
//! path keeps working through the cluster.
//!
//! # Credentials
//!
//! Like the rest of the Iceberg suite, these tests read a Glue/S3 fixture and
//! require `AWS_ICEBERG_ACCOUNT_ID`, `AWS_ICEBERG_REGION`,
//! `AWS_ICEBERG_ACCESS_KEY_ID`, and `AWS_ICEBERG_SECRET_ACCESS_KEY`. They skip
//! (and pass) when `AWS_ICEBERG_ACCOUNT_ID` is unset, so a local `cargo test`
//! without credentials is not a spurious failure; CI provides the credentials.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::FutureExt;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The Glue account id for the shared Iceberg test fixture, or `None` when the
/// credentials are not configured (so the test skips instead of failing).
fn iceberg_account_id() -> Option<String> {
    std::env::var("AWS_ICEBERG_ACCOUNT_ID")
        .ok()
        .filter(|s| !s.is_empty())
}

/// S3 storage params resolved from the environment, shared by the dataset and
/// catalog components.
fn iceberg_s3_params() -> HashMap<String, String> {
    HashMap::from([
        (
            "iceberg_s3_region".to_string(),
            "${ env:AWS_ICEBERG_REGION }".to_string(),
        ),
        (
            "iceberg_s3_access_key_id".to_string(),
            "${ env:AWS_ICEBERG_ACCESS_KEY_ID }".to_string(),
        ),
        (
            "iceberg_s3_secret_access_key".to_string(),
            "${ env:AWS_ICEBERG_SECRET_ACCESS_KEY }".to_string(),
        ),
    ])
}

/// A Glue-backed Iceberg `Catalog` component scoped to the test namespaces.
fn make_iceberg_catalog(account_id: &str, catalog_name: &str) -> Catalog {
    let mut catalog = Catalog::new(
        format!(
            "iceberg:https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces"
        ),
        catalog_name.to_string(),
    );
    catalog.include = vec!["testdb_001.*".to_string(), "testdb_002.*".to_string()];
    catalog.params = Some(Params::from_string_map(iceberg_s3_params()));
    catalog
}

/// A single-table Iceberg `Dataset` component (the data-connector path).
fn make_iceberg_dataset(account_id: &str, namespace: &str, table: &str, name: &str) -> Dataset {
    let from = format!(
        "iceberg:https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces/{namespace}/tables/{table}"
    );
    let mut dataset = Dataset::new(from, name.to_string());
    dataset.params = Some(Params::from_string_map(iceberg_s3_params()));
    dataset
}

/// Total number of rows across all batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Run a test body against a [`ClusterHarness`], ensuring shutdown even on an
/// early `Err` or panic (mirrors the cayenne catalog cluster tests).
async fn run_with_harness<F>(harness: ClusterHarness, f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnOnce(
        &'a ClusterHarness,
    )
        -> Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>>,
{
    let result = AssertUnwindSafe(f(&harness)).catch_unwind().await;
    harness.shutdown().await;
    match result {
        Ok(inner) => inner,
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    }
}

// =============================================================================
// Test: distributed scan of an Iceberg *catalog* table
// =============================================================================
//
// Direct regression for the catalog-path wrapper gap. Without the fix the
// scheduler fails to serialize the catalog table's `IcebergTableScan` for the
// executor and the query errors with "unsupported plan type: IcebergTableScan".
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_iceberg_catalog_scan() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let Some(account_id) = iceberg_account_id() else {
        tracing::warn!(
            "skipping test_distributed_iceberg_catalog_scan: AWS_ICEBERG_ACCOUNT_ID not set"
        );
        return Ok(());
    };

    test_request_context()
        .scope(async move {
            configure_test_datafusion();

            let catalog = make_iceberg_catalog(&account_id, "ice_glue");

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_iceberg_catalog")
                        .with_catalog(catalog.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_iceberg_catalog")
                        .with_catalog(catalog)
                        .build(),
                )
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;

                    let sql = "SELECT * FROM ice_glue.testdb_001.iceberg_table_001 LIMIT 10";

                    // EXPLAIN must plan cleanly through the scheduler's
                    // distributed context (this is where the unserializable
                    // scan previously surfaced).
                    let _ = harness.explain(sql).await?;

                    // The scan is shipped to the executor and re-planned there;
                    // a successful, non-empty result proves the catalog table's
                    // scan now serializes and round-trips across the cluster.
                    let batches = harness.query(sql).await?;
                    assert!(
                        total_rows(&batches) > 0,
                        "distributed catalog scan returned no rows"
                    );

                    Ok(())
                })
            })
            .await
        })
        .await
}

// =============================================================================
// Test: distributed scan of an Iceberg *dataset* (data connector)
// =============================================================================
//
// Guards that the data-connector path (the original PR #11378 fix) keeps
// working through the cluster, alongside the new catalog path.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_iceberg_dataset_scan() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let Some(account_id) = iceberg_account_id() else {
        tracing::warn!(
            "skipping test_distributed_iceberg_dataset_scan: AWS_ICEBERG_ACCOUNT_ID not set"
        );
        return Ok(());
    };

    test_request_context()
        .scope(async move {
            configure_test_datafusion();

            let dataset = make_iceberg_dataset(&account_id, "tpch_sf1", "customer", "customer");

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_iceberg_dataset")
                        .with_dataset(dataset.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_iceberg_dataset")
                        .with_dataset(dataset)
                        .build(),
                )
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;

                    let sql = "SELECT * FROM customer LIMIT 10";
                    let _ = harness.explain(sql).await?;

                    let batches = harness.query(sql).await?;
                    assert!(
                        total_rows(&batches) > 0,
                        "distributed dataset scan returned no rows"
                    );

                    Ok(())
                })
            })
            .await
        })
        .await
}

/// Runs `sql` as a Ballista job, so the plan is serialized and shipped to an
/// executor. A plain `harness.query` runs on the scheduler alone. An Iceberg scan
/// crosses to the executor only as a serializable `IcebergScanExec`, so the job
/// succeeding is what shows the scan was rebuilt there.
async fn distributed_query(
    harness: &ClusterHarness,
    sql: &str,
    job_id: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    use futures::TryStreamExt as _;
    let handle = runtime::datafusion::query::QueryBuilder::new(sql, harness.scheduler.datafusion())
        .build()
        .submit_distributed(job_id)
        .await
        .map_err(|e| anyhow::anyhow!("submitting {job_id} failed: {e}"))?;
    handle
        .into_stream()
        .await
        .map_err(|e| anyhow::anyhow!("{job_id} failed: {e}"))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("{job_id} stream failed: {e}"))
}

/// The `id` column of `batches`, in order.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    use arrow::array::AsArray as _;
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect()
}

// =============================================================================
// Test: distributed scan of an Iceberg dataset read from a pinned metadata file
// =============================================================================
//
// A dataset with `iceberg_metadata_path` is read through a catalog that serves
// only the pinned table; the executor must resolve that same provider to replay
// the scan. The table is written to the local filesystem, which the in-process
// scheduler and executor share, so this runs without credentials. Pinning a
// version older than the latest keeps the row check non-vacuous.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_pinned_iceberg_dataset_scan() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();

            let warehouse = tempfile::tempdir()?;
            let table_root = crate::iceberg::metadata_file::write_table(
                warehouse.path(),
                &[&[1], &[2, 3], &[4]],
            )
            .await?;
            let files = crate::iceberg::metadata_file::metadata_file_names(&table_root)?;
            let dataset = crate::iceberg::metadata_file::pinned_dataset(
                &table_root,
                "pinned",
                "iceberg_metadata_path",
                &format!("/metadata/{}", files[2]),
            );

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_pinned_iceberg")
                        .with_dataset(dataset.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_pinned_iceberg")
                        .with_dataset(dataset)
                        .build(),
                )
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;

                    let batches = distributed_query(
                        harness,
                        "SELECT id FROM pinned ORDER BY id",
                        "pinned_iceberg_scan",
                    )
                    .await?;
                    assert_eq!(ids(&batches), vec![1, 2, 3]);
                    Ok(())
                })
            })
            .await
        })
        .await
}

// The same distributed scan over a table AWS Glue manages on S3, pinned to the
// metadata file Glue records for it.
#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_pinned_glue_iceberg_dataset_scan() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    if iceberg_account_id().is_none() {
        tracing::warn!(
            "skipping test_distributed_pinned_glue_iceberg_dataset_scan: AWS_ICEBERG_ACCOUNT_ID not set"
        );
        return Ok(());
    }

    test_request_context()
        .scope(async move {
            configure_test_datafusion();

            let location =
                crate::iceberg::metadata_file::glue_metadata_location("tpch_sf1", "customer")
                    .await?;
            let dataset =
                crate::iceberg::metadata_file::pinned_glue_dataset(&location, "customer_pinned")?;

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_pinned_glue_iceberg")
                        .with_dataset(dataset.clone())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_pinned_glue_iceberg")
                        .with_dataset(dataset)
                        .build(),
                )
                .start()
                .await?;

            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;

                    let batches = distributed_query(
                        harness,
                        "SELECT c_custkey FROM customer_pinned",
                        "pinned_glue_iceberg_scan",
                    )
                    .await?;
                    assert_eq!(total_rows(&batches), 150_000);
                    Ok(())
                })
            })
            .await
        })
        .await
}
