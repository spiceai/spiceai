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

use crate::{init_tracing, utils::test_request_context, RecordBatch};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use scopeguard::defer;
use spicepod::component::dataset::Dataset;

fn make_duckdb_dataset(ds_name: &str, fn_name: &str, path_str: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("duckdb:read_{fn_name}({path_str})"),
        fn_name.to_string(),
    );
    dataset.name = ds_name.to_string();
    dataset
}

fn make_test_query(table_name: &str) -> String {
    format!("SELECT DISTINCT(\"VendorID\") FROM {table_name} ORDER BY \"VendorID\" DESC")
}

#[tokio::test]
async fn duckdb_from_functions() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_json_contents = include_str!("../test_data/taxi_sample.json");
            // Write the sample file to a temporary directory
            let temp_dir = std::env::temp_dir().join("spiced_test_data");
            std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");
            let sample_csv_path = temp_dir.join("taxi_sample.csv");
            std::fs::write(&sample_csv_path, sample_csv_contents)
                .expect("failed to write sample file");
            let sample_json_path = temp_dir.join("taxi_sample.json");
            std::fs::write(&sample_json_path, sample_json_contents)
                .expect("failed to write sample file");
            defer! {
                std::fs::remove_dir_all(&temp_dir).expect("failed to remove temp dir");
            }
            let app = AppBuilder::new("duckdb_function_test")
        .with_dataset(make_duckdb_dataset(
            "csv_remote",
            "csv",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.csv', HEADER=true",
        ))
        .with_dataset(make_duckdb_dataset(
            "csv_local",
            "csv",
            &format!("'{}'", sample_csv_path.display()),
        ))
        .with_dataset(make_duckdb_dataset(
            "parquet_remote",
            "parquet",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_remote",
            "json",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.json'",
        ))
        .with_dataset(make_duckdb_dataset(
            "json_local",
            "json",
            &format!("'{}'", sample_json_path.display()),
        ))
        .build();

            let rt = Runtime::builder().with_app(app).build().await;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let queries = vec![
                ("csv_remote", make_test_query("csv_remote")),
                ("csv_local", make_test_query("csv_local")),
                ("parquet_remote", make_test_query("parquet_remote")),
                //("parquet_local", make_test_query("parquet_local")),
                ("json_remote", make_test_query("json_remote")),
                ("json_local", make_test_query("json_local")),
            ];

            let expected_results = [
                "+----------+",
                "| VendorID |",
                "+----------+",
                "| 2        |",
                "| 1        |",
                "+----------+",
            ];

            for (ds_name, query) in queries {
                let query_result = rt
                    .datafusion()
                    .query_builder(&query)
                    .build()
                    .run()
                    .await
                    .map_err(|e| format!("query `{query}` to plan: {e}"))?;
                let data = query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .map_err(|e| format!("{ds_name}: query `{query}` to results: {e}"))?;

                assert_batches_eq!(expected_results, &data);
            }

            Ok(())
        })
        .await
}
