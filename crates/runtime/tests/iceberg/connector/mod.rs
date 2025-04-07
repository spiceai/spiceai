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

use crate::{
    get_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use anyhow::Context;
use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::StreamExt;
use runtime::{status, Runtime};
use spicepod::{
    component::dataset::{
        acceleration::{Acceleration, Mode},
        Dataset,
    },
    param::Params as DatasetParams,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn iceberg_integration_test_dataset() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let dataset = make_iceberg_dataset("tpch_sf1", "customer", "customer")?;

            let _ = run_iceberg_test(
                "iceberg_dataset_test",
                dataset,
                "SELECT * FROM customer LIMIT 10",
                true,
                Some("iceberg_integration_test_dataset"),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn iceberg_integration_test_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let mut dataset = make_iceberg_dataset("tpch_sf1", "customer", "customer")?;
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                ..Default::default()
            });

            let _ = run_iceberg_test(
                "iceberg_dataset_test",
                dataset,
                "SELECT * FROM customer LIMIT 10",
                true,
                Some("iceberg_integration_test_duckdb_acceleration"),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn iceberg_integration_test_duckdb_acceleration_restart() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let mut dataset = make_iceberg_dataset("tpch_sf1", "customer", "customer")?;
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                ..Default::default()
            });

            let rt = run_iceberg_test(
                "iceberg_dataset_test",
                dataset.clone(),
                "SELECT * FROM customer LIMIT 10",
                false,
                None,
            )
            .await?;

            drop(rt);

            let _ = run_iceberg_test(
                "iceberg_dataset_test",
                dataset,
                "SELECT * FROM customer LIMIT 10",
                true,
                Some("iceberg_integration_test_duckdb_acceleration_restart"),
            )
            .await?;

            Ok(())
        })
        .await
}

async fn run_iceberg_test(
    app_name: &str,
    dataset: Dataset,
    query: &str,
    assert_snapshot: bool,
    snapshot_name: Option<&str>,
) -> Result<Runtime, anyhow::Error> {
    let app = AppBuilder::new(app_name).with_dataset(dataset).build();

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .with_runtime_status(status)
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
            panic!("Timeout waiting for components to load");
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let mut result = rt.datafusion().query_builder(query).build().run().await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    if assert_snapshot {
        let pretty = arrow::util::pretty::pretty_format_batches(&results)
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;

        insta::assert_snapshot!(snapshot_name.unwrap_or_default(), pretty);
    }

    Ok(rt)
}

fn make_iceberg_dataset(
    namespace: &str,
    table: &str,
    name: &str,
) -> Result<Dataset, anyhow::Error> {
    let account_id =
        std::env::var("AWS_ICEBERG_ACCOUNT_ID").context("AWS_ICEBERG_ACCOUNT_ID is not set")?;

    let from = format!("iceberg:https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces/{namespace}/tables/{table}");
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
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
    ])));
    Ok(dataset)
}
