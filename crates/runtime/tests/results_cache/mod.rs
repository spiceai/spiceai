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

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Array, RecordBatch};
use cache::result::CacheStatus;
use futures::TryStreamExt;
use tempfile::TempDir;

use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::{
    component::{caching::ResultsCache, dataset::Dataset},
    param::Params,
};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

fn make_s3_tpch_dataset(name: &str) -> Dataset {
    let mut test_dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/"),
        name.to_string(),
    );
    test_dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));

    test_dataset
}

#[tokio::test]
async fn results_cache_system_queries() -> Result<(), String> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let results_cache = ResultsCache {
                item_ttl: Some("60s".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("cache_test")
                .with_results_cache(results_cache)
                .with_dataset(make_s3_tpch_dataset("customer"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            cloned_rt.load_components().await;

            execute_query_and_check_cache_status(&rt, "show tables", CacheStatus::CacheDisabled)
                .await
                .expect("should run query successfully");

            execute_query_and_check_cache_status(
                &rt,
                "describe customer",
                CacheStatus::CacheDisabled,
            )
            .await
            .expect("should run query successfully");

            Ok(())
        })
        .await
}

async fn execute_query_and_check_cache_status(
    rt: &Runtime,
    query: &str,
    expected_cache_status: CacheStatus,
) -> Result<Vec<RecordBatch>, String> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();

    let query_result = query
        .run()
        .await
        .map_err(|e| format!("Failed to execute query: {e}"))?;

    let records = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect query results: {e}"))?;

    assert_eq!(query_result.cache_status, expected_cache_status);

    Ok(records)
}

/// Test that verifies UDTF arguments are included in `TableScan` names for cache correctness.
///
/// **Critical for**: `DataFusion` fork (`spiceai/datafusion`, spiceai-51-patches)
///
/// This test ensures that different UDTF invocations (e.g., `read_parquet('/path1')`
/// vs `read_parquet('/path2')`) don't incorrectly share cached results. The `DataFusion`
/// patch includes UDTF arguments in the `TableScan` node name, which is used as part of
/// the cache key.
///
/// **What happens without the patch**: Both queries would return the same cached result
/// because the `TableScan` name wouldn't distinguish between different file paths.
#[tokio::test]
async fn test_udtf_cache_key_includes_arguments() -> Result<(), String> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            // Create a temp directory for test parquet files
            let temp_dir =
                TempDir::new().map_err(|e| format!("Failed to create temp dir: {e}"))?;

            // Create two parquet files with distinct data
            let file1_path = temp_dir.path().join("data1.parquet");
            let file2_path = temp_dir.path().join("data2.parquet");

            // Create test data - file1 has values 1, 2, 3; file2 has values 100, 200, 300
            create_test_parquet_file(&file1_path, vec![1, 2, 3])?;
            create_test_parquet_file(&file2_path, vec![100, 200, 300])?;

            // Enable results caching with a long TTL
            let results_cache = ResultsCache {
                item_ttl: Some("300s".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("udtf_cache_test")
                .with_results_cache(results_cache)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let rt = Arc::new(rt);
            Arc::clone(&rt).load_components().await;

            // Query file1 - should return 1, 2, 3
            let query1 = format!(
                "SELECT value FROM read_parquet('{}')",
                file1_path.to_string_lossy()
            );
            let result1 = execute_query_collect(&rt, &query1).await?;
            let values1 = extract_int_column(&result1);

            // Query file2 - should return 100, 200, 300 (NOT cached result from file1)
            let query2 = format!(
                "SELECT value FROM read_parquet('{}')",
                file2_path.to_string_lossy()
            );
            let result2 = execute_query_collect(&rt, &query2).await?;
            let values2 = extract_int_column(&result2);

            // Query file1 again - verify it returns correct cached result
            let result1_cached = execute_query_collect(&rt, &query1).await?;
            let values1_cached = extract_int_column(&result1_cached);

            // Verify the results are distinct and correct
            assert_eq!(values1, vec![1, 2, 3], "File1 should contain [1, 2, 3]");
            assert_eq!(
                values2,
                vec![100, 200, 300],
                "File2 should contain [100, 200, 300], not cached result from file1"
            );
            assert_eq!(
                values1_cached, values1,
                "Cached result for file1 should match original"
            );

            // Critical assertion: the two queries MUST return different results
            // If the DataFusion UDTF patch is missing, both would return the same cached result
            assert_ne!(
                values1, values2,
                "UDTF cache key FAILED to include arguments: Different files returned same results. \
                 This indicates the DataFusion UDTF patch may be missing from spiceai/datafusion."
            );

            Ok(())
        })
        .await
}

fn create_test_parquet_file(path: &std::path::Path, values: Vec<i32>) -> Result<(), String> {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use std::fs::File;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));

    let array = Int32Array::from(values);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .map_err(|e| format!("Failed to create record batch: {e}"))?;

    let file = File::create(path).map_err(|e| format!("Failed to create file: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .map_err(|e| format!("Failed to create parquet writer: {e}"))?;

    writer
        .write(&batch)
        .map_err(|e| format!("Failed to write batch: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("Failed to close writer: {e}"))?;

    Ok(())
}

async fn execute_query_collect(rt: &Arc<Runtime>, query: &str) -> Result<Vec<RecordBatch>, String> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();

    let query_result = query
        .run()
        .await
        .map_err(|e| format!("Failed to execute query: {e}"))?;

    query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect query results: {e}"))
}

fn extract_int_column(batches: &[RecordBatch]) -> Vec<i32> {
    use arrow::array::Int32Array;

    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() > 0 {
            let column = batch.column(0);
            if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        values.push(int_array.value(i));
                    }
                }
            }
        }
    }
    values
}
