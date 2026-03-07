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

use std::sync::Arc;

use app::AppBuilder;
use datafusion_datasource::metadata::MetadataColumn;
use futures::StreamExt;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, test_request_context},
};

pub fn get_s3_hive_partitioned_dataset(
    name: &str,
    metadata_columns: Vec<MetadataColumn>,
) -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-public-datasets/hive_partitioned_data/", name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    for column in metadata_columns {
        dataset.metadata.insert(
            column.name().to_string(),
            serde_json::Value::String("enabled".to_string()),
        );
    }
    dataset
}

#[tokio::test]
async fn s3_metadata_columns() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_metadata_columns")
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_all",
                    vec![
                        MetadataColumn::Location(None),
                        MetadataColumn::Size,
                        MetadataColumn::LastModified,
                    ],
                ))
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_location",
                    vec![MetadataColumn::Location(None)],
                ))
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_last_modified",
                    vec![MetadataColumn::LastModified],
                ))
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_size",
                    vec![MetadataColumn::Size],
                ))
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_location_last_modified",
                    vec![MetadataColumn::Location(None), MetadataColumn::LastModified],
                ))
                .with_dataset(get_s3_hive_partitioned_dataset(
                    "met_location_size",
                    vec![MetadataColumn::Location(None), MetadataColumn::Size],
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .build()
                    .await,
            );

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM met_all ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let met_all = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("met_all", met_all);

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let met_all_location_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("met_all_location_filtered", met_all_location_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_all_location_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_all_location_filtered", explain_met_all_location_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_location WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_location_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_location_filtered", explain_met_location_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_last_modified WHERE last_modified = '2024-10-10T05:37:00Z' ORDER BY id, last_modified")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_last_modified_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_last_modified_filtered", explain_met_last_modified_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_size WHERE size = 2319 ORDER BY id, size")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_size_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_size_filtered", explain_met_size_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_location_last_modified WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_location_last_modified_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_location_last_modified_filtered", explain_met_location_last_modified_filtered);

            let mut query_result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM met_location_size WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, location")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let explain_met_location_size_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("explain_met_location_size_filtered", explain_met_location_size_filtered);

            // ── Projected queries (not SELECT *) ──

            // Projection with file + metadata columns
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value, location FROM met_all ORDER BY value, location LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let projected_value_location = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("projected_value_location", projected_value_location);

            // Metadata-only projection
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT location FROM met_location ORDER BY location LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let projected_location_only = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("projected_location_only", projected_location_only);

            // Projected query with location filter
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value, location FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY value")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let projected_value_location_filtered = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("projected_value_location_filtered", projected_value_location_filtered);

            // Projection with partition + metadata columns (no file columns)
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT year, month, location FROM met_all ORDER BY year, month, location LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let projected_partition_metadata = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("projected_partition_metadata", projected_partition_metadata);

            // ── EmptyExec schema: non-existent location ──
 
            // Non-existent location with SELECT *
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM met_location WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/nonexistent.parquet' ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let empty_result_star = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("empty_result_star", empty_result_star);

            // Non-existent location with projection
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value, location FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/nonexistent.parquet'")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let empty_result_projected = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("empty_result_projected", empty_result_projected);

            // Location predicate outside configured bucket
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value, location FROM met_all WHERE location = 's3://not/correct.parquet'")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let outside_bucket_projected = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("outside_bucket_projected", outside_bucket_projected);

            // Multiple metadata columns with projection
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT id, location, size FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let projected_multi_metadata = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("projected_multi_metadata", projected_multi_metadata);

            // ── Scalar functions on metadata columns ──
            // Verifies that scalar functions (upper, lower, etc.) can be
            // applied to metadata columns in projections and filters.

            // Scalar function on metadata column with filter
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value, size, upper(location) FROM met_all WHERE location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY value")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let scalar_upper_location = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("scalar_upper_location", scalar_upper_location);

            // ── Mixed column types with alias, multi-predicate filter ──

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT value AS val, location, day FROM met_location WHERE (id = 43) AND (location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2022/month=1/day=2/data_4.parquet') AND (value IS NOT NULL)")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            let mixed_alias_multi_filter = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("mixed_alias_multi_filter", mixed_alias_multi_filter);

            Ok(())
        })
        .await
}
