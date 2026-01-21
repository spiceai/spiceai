/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::prelude::*;
use futures::StreamExt;

use runtime::Runtime;
use spicepod::{
    component::dataset::Dataset,
    param::{ParamValue, Params},
};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

pub fn get_s3_dataset(s3_uri: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(s3_uri, name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

pub fn get_s3_hive_partitioned_dataset(name: &str, infer_partitions: bool) -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-public-datasets/hive_partitioned_data/", name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            (
                "hive_partitioning_enabled".to_string(),
                infer_partitions.to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

#[tokio::test]
async fn s3_federation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_federation")
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-demo-datasets/taxi_trips/2024/",
                    "taxi_trips",
                ))
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet",
                    "taxi_sample",
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

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM taxi_trips LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 10);

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM taxi_sample LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 10);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn s3_hive_partitioning() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_hive_partitioning")
                .with_dataset(get_s3_hive_partitioned_dataset("hive_data", true))
                .with_dataset(get_s3_hive_partitioned_dataset("hive_data_no_infer", false))
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

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM hive_data ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let partition_inferred = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(partition_inferred);

            query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM hive_data_no_infer ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let partition_not_inferred = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(partition_not_inferred);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn s3_schema_evolution() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_schema")
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-public-datasets/test_schema_evolution/",
                    "lineitem",
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

            let mut query_result = rt
                .datafusion()
                .query_builder("describe lineitem;")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            // Test S3 bucket contains Parquet files with different schema, inferred schema must represent the lineitem table
            // based on the most recently added file: `/test_schema_evolution/2/data_0_2_11-new.parquet`
            // other Parquet files represent the customer table schema
            let schema = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(schema);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn s3_bulk_bucket_schema() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_schema")
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-public-datasets/tpch_sf1000/lineitem/",
                    "lineitem",
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
                () =cloned_rt.load_components() => {}
            }

            let mut query_result = rt
                .datafusion()
                .query_builder("describe lineitem;")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }
            let schema = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(schema);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn s3_schema_source_path() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut ds1 = get_s3_dataset(
                "s3://spiceai-public-datasets/test_schema_evolution/",
                "ds1_customer",
            );

            // refer to s3 bucket containing `customer` schema objects to infer schema
            if let Some(params) = ds1.params.as_mut() {
                params.data.insert(
                    "schema_source_path".to_string(),
                    ParamValue::String(
                        "s3://spiceai-public-datasets/test_schema_evolution/1/"
                            .to_string(),
                    ),
                );
            }

            let mut ds2 = get_s3_dataset(
                "s3://spiceai-public-datasets/test_schema_evolution/",
                "ds2_customer",
            );

            // refer to s3 object containing `customer` schema to infer schema
            if let Some(params) = ds2.params.as_mut() {
                params.data.insert(
                    "schema_source_path".to_string(),
                    ParamValue::String(
                        "s3://spiceai-public-datasets/test_schema_evolution/1/data_0_0_10.parquet"
                            .to_string(),
                    ),
                );
            }

            let mut ds3 = get_s3_dataset(
                "s3://spiceai-public-datasets/test_schema_evolution/",
                "ds3_lineitem",
            );

            // refer to s3 object containing `lineitem` schema to infer schema
            if let Some(params) = ds3.params.as_mut() {
                params.data.insert(
                    "schema_source_path".to_string(),
                    ParamValue::String(
                        "s3://spiceai-public-datasets/test_schema_evolution/2/data_0_2_11-new.parquet"
                            .to_string(),
                    ),
                );
            }

            let app = AppBuilder::new("s3_schema_source_path")
                .with_dataset(ds1)
                .with_dataset(ds2)
                .with_dataset(ds3)
                .build();

             configure_test_datafusion();
            let rt =
                Runtime::builder()
                    .with_app(app)
                    .build()
                    .await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            for dataset_name in &["ds1_customer", "ds2_customer", "ds3_lineitem"] {
                let query = format!("describe {dataset_name};");
                let mut query_result = rt
                    .datafusion()
                    .query_builder(&query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                let mut batches = vec![];
                while let Some(batch) = query_result.data.next().await {
                    batches.push(batch?);
                }

                let schema = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
                insta::assert_snapshot!(format!("s3_schema_source_path_{dataset_name}"), schema);
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn s3_schema_source_path_authenticated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut parts_ds = get_s3_dataset(
                "s3://databricks-workspace-stack-473ec-bucket/unity-catalog/8429759719709712/__unitystorage/catalogs/fa937541-ca24-4f62-a93b-088db0fb40af/tables/e0dcf887-3bef-4c85-8863-e786e74756de/",
                "parts",
            );

            // refer to s3 bucket containing `customer` schema objects to infer schema
            if let Some(params) = parts_ds.params.as_mut() {
                params.data.insert(
                    "schema_source_path".to_string(),
                    ParamValue::String(
                        "s3://databricks-workspace-stack-473ec-bucket/unity-catalog/8429759719709712/__unitystorage/catalogs/fa937541-ca24-4f62-a93b-088db0fb40af/tables/e0dcf887-3bef-4c85-8863-e786e74756de/part_key=0/part-00091-21d6926a-7a96-4159-afe6-25c9f574110e.c000.snappy.parquet"
                            .to_string(),
                    ),
                );
                params.data.insert(
                    "s3_auth".to_string(),
                    ParamValue::String("key".to_string()),
                );
                params.data.insert(
                    "s3_key".to_string(),
                    ParamValue::String("${secrets:NEW_AWS_DATABRICKS_DELTA_ACCESS_KEY_ID}".to_string()),
                );
                params.data.insert(
                    "s3_secret".to_string(),
                    ParamValue::String("${secrets:NEW_AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY}".to_string()),
                );
                params.data.insert(
                    "s3_region".to_string(),
                    ParamValue::String("us-east-1".to_string()),
                );
                params.data.insert(
                    "hive_partitioning_enabled".to_string(),
                    ParamValue::Bool(true),
                );
            }

            let app = AppBuilder::new("s3_schema_source_path_authenticated")
                .with_dataset(parts_ds)
                .build();

             configure_test_datafusion();
            let rt =
                Runtime::builder()
                    .with_app(app)
                    .build()
                    .await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let query = "describe parts;";
            let mut query_result = rt
                .datafusion()
                .query_builder(query)
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let schema = arrow::util::pretty::pretty_format_batches(&batches)
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(format!("s3_schema_source_path_authenticated_parts"), schema);

            Ok(())
        })
        .await
}

/// Test that URL tables can be queried directly via SQL API.
///
/// This tests the `DynamicUrlSchemaProvider` and `SpiceUrlTableFactory` integration
/// which enables queries like: `SELECT * FROM 's3://bucket/path/'`
#[tokio::test]
async fn s3_url_table_sql_api() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime_datafusion=debug,info"));

    test_request_context()
        .scope(async {
            // Create a minimal app with URL tables enabled via runtime param
            let app = AppBuilder::new("s3_url_table_sql")
                .with_runtime_params(HashMap::from([("url_tables".to_string(), "enabled".to_string())]))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for runtime to start"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Test querying a single parquet file directly via URL in SQL
            let mut query_result = rt
                .datafusion()
                .query_builder(
                    "SELECT * FROM 's3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet' LIMIT 10",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            assert_eq!(batches.len(), 1, "Should have one batch of results");
            assert_eq!(batches[0].num_rows(), 10, "Should have 10 rows");

            // Test querying a directory/prefix via URL in SQL
            // Using the hive_partitioned_data which is a publicly accessible directory
            let mut query_result = rt
                .datafusion()
                .query_builder(
                    "SELECT * FROM 's3://spiceai-public-datasets/hive_partitioned_data/' LIMIT 10",
                )
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            assert_eq!(batches.len(), 1, "Should have one batch of results");
            assert_eq!(batches[0].num_rows(), 10, "Should have 10 rows");

            Ok(())
        })
        .await
}

/// Test that URL tables can be queried directly via `DataFrame` API.
///
/// This tests the `DynamicUrlSchemaProvider` integration with `DataFusion`'s
/// `table()` method for programmatic `DataFrame` access to S3 URLs.
#[tokio::test]
async fn s3_url_table_dataframe_api() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime_datafusion=debug,info"));

    test_request_context()
        .scope(async {
            // Create a minimal app with URL tables enabled via runtime param
            let app = AppBuilder::new("s3_url_table_df")
                .with_runtime_params(HashMap::from([(
                    "url_tables".to_string(),
                    "enabled".to_string(),
                )]))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for runtime to start"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Test querying a single parquet file directly via DataFrame API
            let df = rt
                .datafusion()
                .ctx
                .table("s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet")
                .await?
                .limit(0, Some(10))?;

            let batches = df.collect().await?;
            assert_eq!(batches.len(), 1, "Should have one batch of results");
            assert_eq!(batches[0].num_rows(), 10, "Should have 10 rows");

            // Test querying a directory/prefix via DataFrame API
            // Using the hive_partitioned_data which is a publicly accessible directory
            let df = rt
                .datafusion()
                .ctx
                .table("s3://spiceai-public-datasets/hive_partitioned_data/")
                .await?
                .limit(0, Some(10))?;

            let batches = df.collect().await?;
            assert_eq!(batches.len(), 1, "Should have one batch of results");
            assert_eq!(batches[0].num_rows(), 10, "Should have 10 rows");

            // Test that DataFrame API allows filtering
            // Use col() with proper identifier to preserve case sensitivity
            let df = rt
                .datafusion()
                .ctx
                .table("s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet")
                .await?
                .filter(col("\"VendorID\"").eq(lit(1)))?
                .limit(0, Some(5))?;

            let batches = df.collect().await?;
            assert!(!batches.is_empty(), "Should have results after filtering");

            // Verify LIMIT is applied correctly
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert!(
                total_rows <= 5,
                "LIMIT 5 should return at most 5 rows, got {total_rows}"
            );

            // Verify all rows have VendorID = 1
            // Note: taxi_sample.parquet contains mixed VendorID values (1 and 2),
            // so this assertion validates that the filter is actually working.
            for batch in &batches {
                let vendor_col = batch
                    .column_by_name("VendorID")
                    .expect("VendorID column should exist");
                let vendor_array = vendor_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("VendorID should be Int64Array");
                for i in 0..vendor_array.len() {
                    assert_eq!(vendor_array.value(i), 1, "All VendorID values should be 1");
                }
            }

            Ok(())
        })
        .await
}
