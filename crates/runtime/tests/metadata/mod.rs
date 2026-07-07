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
use arrow::record_batch::RecordBatch;
use datafusion_datasource::metadata::MetadataColumn;
use futures::StreamExt;
use runtime::Runtime;
use spicepod::{
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};

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

fn get_docs_dataset() -> Dataset {
    let mut dataset = Dataset::new(
        "s3://spiceai-public-datasets/test_documents_partitioned/",
        "docs",
    );
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.time_column = Some("day".to_string());
    dataset.time_format = Some(TimeFormat::Date);
    dataset.metadata.insert(
        "_location".to_string(),
        serde_json::Value::String("enabled".to_string()),
    );
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM met_all ORDER BY id, _location")
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
                .query_builder("SELECT * FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, _location")
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
                .query_builder("EXPLAIN SELECT * FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, _location")
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
                .query_builder("EXPLAIN SELECT * FROM met_location WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, _location")
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
                .query_builder("EXPLAIN SELECT * FROM met_last_modified WHERE _last_modified = '2024-10-10T05:37:00Z' ORDER BY id, _last_modified")
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
                .query_builder("EXPLAIN SELECT * FROM met_size WHERE _size = 2319 ORDER BY id, _size")
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
                .query_builder("EXPLAIN SELECT * FROM met_location_last_modified WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, _location")
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
                .query_builder("EXPLAIN SELECT * FROM met_location_size WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id, _location")
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
                .query_builder("SELECT value, _location FROM met_all ORDER BY value, _location LIMIT 10")
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
                .query_builder("SELECT _location FROM met_location ORDER BY _location LIMIT 10")
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
                .query_builder("SELECT value, _location FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY value")
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
                .query_builder("SELECT year, month, _location FROM met_all ORDER BY year, month, _location LIMIT 10")
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
                .query_builder("SELECT * FROM met_location WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/nonexistent.parquet' ORDER BY id")
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
                .query_builder("SELECT value, _location FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/nonexistent.parquet'")
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
                .query_builder("SELECT value, _location FROM met_all WHERE _location = 's3://not/correct.parquet'")
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
                .query_builder("SELECT id, _location, _size FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY id")
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
                .query_builder("SELECT value, _size, upper(_location) FROM met_all WHERE _location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2023/month=2/day=2/data_1.parquet' ORDER BY value")
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
                .query_builder("SELECT value AS val, _location, day FROM met_location WHERE (id = 43) AND (_location = 's3://spiceai-public-datasets/hive_partitioned_data/year=2022/month=1/day=2/data_4.parquet') AND (value IS NOT NULL)")
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

/// Extended metadata test using the `docs` dataset (10 file columns + 1 partition + 1 metadata).
#[tokio::test]
async fn s3_metadata_columns_extended() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("s3_metadata_columns_extended")
                .with_dataset(get_docs_dataset())
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            // ── Ad-hoc tests for previously known issues ──

            // file + metadata select with location predicate
            run_query_and_snapshot(
                &rt,
                "SELECT doc_content, _location, day, size FROM docs \
                 WHERE document_id = 'doc_1' \
                 AND _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-01/data_0.parquet' \
                 AND doc_content IS NOT NULL",
                "docs_file_metadata_with_location_pred",
            ).await;

            // Same as above but with alias on file column
            run_query_and_snapshot(
                &rt,
                "SELECT doc_content AS content, _location, day, size FROM docs \
                 WHERE document_id = 'doc_1' \
                 AND _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-01/data_0.parquet' \
                 AND doc_content IS NOT NULL",
                "docs_file_metadata_with_location_pred_alias",
            ).await;

            // ── Baseline: SELECT * ──

            run_query_and_snapshot(
                &rt,
                "SELECT * FROM docs ORDER BY document_id LIMIT 5",
                "docs_select_star",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT * FROM docs WHERE day = '2022-01-01' ORDER BY document_id",
                "docs_select_star_partition_filter",
            ).await;

            // ── File-only projection (metadata enabled but not referenced) ──
            // Validates the conditional guard allows the swap optimization
            // when the projection doesn't reference any metadata column.

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, filename FROM docs ORDER BY document_id LIMIT 5",
                "docs_file_only_no_metadata",
            ).await;

            // ── Metadata-only projections ──

            run_query_and_snapshot(
                &rt,
                "SELECT _location FROM docs ORDER BY _location, document_id LIMIT 5",
                "docs_metadata_only",
            ).await;

            // Metadata + partition only (no file columns)
            run_query_and_snapshot(
                &rt,
                "SELECT day, _location FROM docs ORDER BY day, _location, document_id LIMIT 5",
                "docs_partition_metadata_only",
            ).await;

            // ── File + metadata projections ──

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, _location FROM docs ORDER BY document_id LIMIT 5",
                "docs_file_metadata",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, filename, size, _location FROM docs ORDER BY document_id LIMIT 5",
                "docs_multi_file_metadata",
            ).await;

            // All column types: file + partition + metadata
            run_query_and_snapshot(
                &rt,
                "SELECT document_id, day, _location FROM docs ORDER BY document_id LIMIT 5",
                "docs_file_partition_metadata",
            ).await;

            // ── Location pruning ──

            run_query_and_snapshot(
                &rt,
                "SELECT * FROM docs WHERE _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-02/data_0.parquet'",
                "docs_location_eq_star",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, filename FROM docs WHERE _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-02/data_0.parquet'",
                "docs_location_eq_projected",
            ).await;

            // Location IN with multiple files
            run_query_and_snapshot(
                &rt,
                "SELECT document_id, _location FROM docs \
                 WHERE _location IN (\
                 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-01/data_0.parquet', \
                 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-02-01/data_0.parquet') \
                 ORDER BY document_id",
                "docs_location_in",
            ).await;

            // ── File column filter + metadata in select ──

            run_query_and_snapshot(
                &rt,
                "SELECT _location FROM docs WHERE compression_level > 3 ORDER BY document_id LIMIT 5",
                "docs_file_filter_metadata_select",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT filename, _location FROM docs WHERE method = 'api' ORDER BY filename, document_id",
                "docs_file_filter_file_metadata_select",
            ).await;

            // Filter on nullable column (NULL doc_content)
            run_query_and_snapshot(
                &rt,
                "SELECT document_id, _location FROM docs WHERE doc_content IS NOT NULL ORDER BY document_id",
                "docs_not_null_filter",
            ).await;

            // ── Partition filter + metadata ──

            run_query_and_snapshot(
                &rt,
                "SELECT _location FROM docs WHERE day = '2022-03-15' ORDER BY _location",
                "docs_partition_filter_metadata",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, filename, _location FROM docs WHERE day = '2022-01-01' ORDER BY document_id",
                "docs_partition_filter_file_metadata",
            ).await;

            // ── Combined filters ──

            // File + location filter
            run_query_and_snapshot(
                &rt,
                "SELECT filename, _location FROM docs \
                 WHERE document_id = 'doc_1' AND _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-01/data_0.parquet'",
                "docs_file_location_filter",
            ).await;

            // File + partition filter, metadata in select
            run_query_and_snapshot(
                &rt,
                "SELECT filename, _location FROM docs WHERE compression = 'gzip' AND day = '2022-01-01' ORDER BY document_id",
                "docs_file_partition_filter",
            ).await;

            // ── Aggregations ──

            run_query_and_snapshot(
                &rt,
                "SELECT COUNT(*) AS cnt FROM docs WHERE _location = 's3://spiceai-public-datasets/test_documents_partitioned/day=2022-01-01/data_0.parquet'",
                "docs_count_with_location",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT _location, COUNT(*) AS cnt FROM docs GROUP BY _location ORDER BY cnt DESC, _location",
                "docs_group_by_location",
            ).await;

            // ── Aliases and expressions ──

            run_query_and_snapshot(
                &rt,
                "SELECT _location AS loc FROM docs ORDER BY loc, document_id LIMIT 5",
                "docs_alias_metadata",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT document_id AS doc_id, _location FROM docs ORDER BY doc_id LIMIT 5",
                "docs_alias_file_metadata",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT upper(_location) AS loc_upper FROM docs ORDER BY document_id LIMIT 3",
                "docs_scalar_fn_metadata",
            ).await;

            // ── DISTINCT ──

            run_query_and_snapshot(
                &rt,
                "SELECT DISTINCT _location FROM docs ORDER BY _location",
                "docs_distinct_location",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT DISTINCT day, _location FROM docs ORDER BY day, _location",
                "docs_distinct_partition_metadata",
            ).await;

            // ── ORDER BY on metadata ──

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, _location FROM docs ORDER BY _location, document_id",
                "docs_order_by_location",
            ).await;

            run_query_and_snapshot(
                &rt,
                "SELECT document_id, _location FROM docs ORDER BY _location DESC, document_id LIMIT 3",
                "docs_order_by_location_desc",
            ).await;

            Ok(())
        })
        .await
}

/// Executes a query against the runtime, snapshots both the schema and the
/// formatted result rows under `{snapshot_name}_schema` and `{snapshot_name}`.
async fn run_query_and_snapshot(rt: &Runtime, query: &str, snapshot_name: &str) {
    let mut query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .expect("query should succeed");

    let schema = query_result.data.schema();
    insta::assert_snapshot!(format!("{snapshot_name}_schema"), format!("{schema}"));

    let mut batches: Vec<RecordBatch> = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch.expect("batch should be valid"));
    }

    let formatted =
        arrow::util::pretty::pretty_format_batches(&batches).expect("formatting should succeed");
    insta::assert_snapshot!(snapshot_name.to_string(), formatted);
}
