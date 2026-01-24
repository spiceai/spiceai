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

//! Google Cloud Storage (GCS) connector integration tests.
//!
//! These tests verify the GCS integration using:
//! 1. Public datasets that don't require authentication (using `skip_signature`)
//! 2. Application Default Credentials (ADC) for authenticated access
//!
//! To run authenticated tests locally:
//! ```bash
//! gcloud auth application-default login
//! ```
//!
//! Or set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to a service account key file.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

/// Creates a GCS dataset configuration for public bucket access (no auth required).
pub fn get_public_gcs_dataset(gcs_uri: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(gcs_uri, name);
    dataset.params = Some(DatasetParams::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            ("gcs_skip_signature".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

/// Creates a GCS dataset configuration using Application Default Credentials.
pub fn get_adc_gcs_dataset(gcs_uri: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(gcs_uri, name);
    dataset.params = Some(DatasetParams::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            (
                "gcs_application_default_credentials".to_string(),
                "true".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

/// Test GCS federation using a public bucket (Google's public datasets).
///
/// Uses the `BigQuery` public dataset exported to GCS:
/// `gcs://cloud-samples-data/bigquery/us-states/us-states.parquet`
#[tokio::test]
async fn gcs_public_dataset_federation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Google's public sample data bucket
            let app = AppBuilder::new("gcs_public_federation")
                .with_dataset(get_public_gcs_dataset(
                    "gcs://cloud-samples-data/bigquery/us-states/us-states.parquet",
                    "us_states",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Query the public US states dataset
            let query_result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM us_states")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let batches: Vec<_> = query_result.data.try_collect().await?;

            assert!(!batches.is_empty(), "Expected at least one batch");
            let total_rows: i64 = batches
                .iter()
                .map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map_or(0, |a| a.value(0))
                })
                .sum();
            assert!(total_rows > 0, "Expected non-zero row count");

            Ok(())
        })
        .await
}

/// Test GCS using the `gs://` URL scheme.
#[tokio::test]
async fn gs_url_scheme() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Using gs:// prefix (standard Google Cloud Storage scheme)
            let app = AppBuilder::new("gs_url_scheme")
                .with_dataset(get_public_gcs_dataset(
                    "gs://cloud-samples-data/bigquery/us-states/us-states.parquet",
                    "us_states_gs",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM us_states_gs LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let batches: Vec<_> = query_result.data.try_collect().await?;

            assert!(!batches.is_empty(), "Expected at least one batch");
            let total_rows: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(total_rows, 5, "Expected 5 rows across all batches");

            Ok(())
        })
        .await
}

/// Test GCS using the `gcs://` URL scheme (alternative to `gs://`).
#[tokio::test]
async fn gcs_url_scheme() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Using gcs:// prefix (alternative scheme)
            let app = AppBuilder::new("gcs_url_scheme")
                .with_dataset(get_public_gcs_dataset(
                    "gcs://cloud-samples-data/bigquery/us-states/us-states.parquet",
                    "us_states_gcs",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM us_states_gcs LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let batches: Vec<_> = query_result.data.try_collect().await?;

            assert!(!batches.is_empty(), "Expected at least one batch");
            let total_rows: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(total_rows, 5, "Expected 5 rows across all batches");

            Ok(())
        })
        .await
}

/// Test GCS with Application Default Credentials (ADC) using TPC-H SF1 data.
///
/// This test requires:
/// - `gcloud auth application-default login` to be run, OR
/// - `GOOGLE_APPLICATION_CREDENTIALS` environment variable set to a service account key file
///
/// The test uses the `spice-tpch-sf1` bucket which requires authentication.
#[tokio::test]
#[ignore = "requires gcloud auth application-default login or GOOGLE_APPLICATION_CREDENTIALS"]
async fn gcs_adc_authentication() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Use the private spice-tpch-sf1 bucket with ADC authentication
            let app = AppBuilder::new("gcs_adc_test")
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/nation.parquet",
                    "nation",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let query_result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM nation")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let batches: Vec<_> = query_result.data.try_collect().await?;

            assert!(!batches.is_empty(), "Expected at least one batch");
            // TPC-H has 25 nations
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 25, "Expected 25 nations in TPC-H");

            Ok(())
        })
        .await
}

/// Test TPC-H SF1 queries against GCS with multiple tables.
///
/// This test requires authentication and verifies:
/// - Multiple TPC-H tables can be loaded from GCS
/// - Joins work correctly across tables
/// - Row counts match expected TPC-H SF1 values
#[tokio::test]
#[ignore = "requires gcloud auth application-default login or GOOGLE_APPLICATION_CREDENTIALS"]
async fn gcs_tpch_sf1() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("gcs_tpch_test")
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/nation.parquet",
                    "nation",
                ))
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/region.parquet",
                    "region",
                ))
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/customer.parquet",
                    "customer",
                ))
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/orders.parquet",
                    "orders",
                ))
                .with_dataset(get_adc_gcs_dataset(
                    "gcs://spice-tpch-sf1/lineitem.parquet",
                    "lineitem",
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Verify TPC-H SF1 row counts
            // Nation: 25 rows
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM nation")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 25, "Expected 25 nations");

            // Region: 5 rows
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM region")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 5, "Expected 5 regions");

            // Customer: 150,000 rows for SF1
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM customer")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 150_000, "Expected 150,000 customers for SF1");

            // Orders: 1,500,000 rows for SF1
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM orders")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 1_500_000, "Expected 1,500,000 orders for SF1");

            // Lineitem: 6,001,215 rows for SF1
            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM lineitem")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Expected Int64Array")
                .value(0);
            assert_eq!(count, 6_001_215, "Expected 6,001,215 lineitems for SF1");

            // Test a join query (nation-region)
            let result = rt
                .datafusion()
                .query_builder(
                    "SELECT r.r_name, COUNT(*) as nation_count \
                     FROM nation n \
                     JOIN region r ON n.n_regionkey = r.r_regionkey \
                     GROUP BY r.r_name \
                     ORDER BY r.r_name",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let batches: Vec<_> = result.data.try_collect().await?;
            assert!(!batches.is_empty(), "Expected join results");
            // Each region has 5 nations (25 nations / 5 regions)
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 5, "Expected 5 region groups");

            Ok(())
        })
        .await
}

/// Test querying CSV files from GCS.
#[tokio::test]
async fn gcs_csv_format() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut dataset = Dataset::new(
                "gcs://cloud-samples-data/bigquery/us-states/us-states.csv",
                "us_states_csv",
            );
            dataset.params = Some(DatasetParams::from_string_map(
                vec![
                    ("file_format".to_string(), "csv".to_string()),
                    ("client_timeout".to_string(), "120s".to_string()),
                    ("gcs_skip_signature".to_string(), "true".to_string()),
                    ("csv_has_header".to_string(), "true".to_string()),
                ]
                .into_iter()
                .collect(),
            ));

            let app = AppBuilder::new("gcs_csv_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let query_result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) as cnt FROM us_states_csv")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let batches: Vec<_> = query_result.data.try_collect().await?;

            assert!(!batches.is_empty(), "Expected at least one batch");

            Ok(())
        })
        .await
}
