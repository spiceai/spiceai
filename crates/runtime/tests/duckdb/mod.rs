use crate::init_tracing;
use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;
use std::path::PathBuf;

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
    let local_path_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_data");
    let app = AppBuilder::new("duckdb_function_test")
        .with_dataset(make_duckdb_dataset(
            "csv_remote",
            "csv",
            "'s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.csv', HEADER=true",
        ))
        .with_dataset(make_duckdb_dataset(
            "csv_local",
            "csv",
            &format!(
                "'{}'",
                local_path_root.join("taxi_sample.csv").to_str().unwrap_or("invalid_path")
            ),
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
            &format!(
                "'{}'",
                local_path_root.join("taxi_sample.json").to_str().unwrap_or("invalid_path")
            ),
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
        let data = rt
            .datafusion()
            .ctx
            .sql(&query)
            .await
            .map_err(|e| format!("{ds_name}: query `{query}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("{ds_name}: query `{query}` to results: {e}"))?;

        assert_batches_eq!(expected_results, &data);
    }

    Ok(())
}
