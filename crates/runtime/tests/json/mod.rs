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

//! Integration tests for JSON file format.

use std::sync::Arc;

use app::AppBuilder;
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;

use runtime::Runtime;
use spicepod::{acceleration::Acceleration, component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

const S3_BASE: &str = "s3://spiceai-public-datasets/test_different_formats/json_format";

fn json_dataset(s3_path: &str, name: &str, json_format: &str, accelerated: bool) -> Dataset {
    json_dataset_with_params(s3_path, name, json_format, accelerated, &[])
}

fn json_dataset_with_params(
    s3_path: &str,
    name: &str,
    json_format: &str,
    accelerated: bool,
    extra_params: &[(&str, &str)],
) -> Dataset {
    let mut dataset = Dataset::new(s3_path, name);
    let mut params: std::collections::HashMap<String, String> = [
        ("file_format".to_string(), "json".to_string()),
        ("client_timeout".to_string(), "120s".to_string()),
    ]
    .into_iter()
    .collect();
    if json_format != "json" {
        params.insert("json_format".to_string(), json_format.to_string());
    }
    for (k, v) in extra_params {
        params.insert((*k).to_string(), (*v).to_string());
    }
    dataset.params = Some(Params::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            ..Default::default()
        });
    }
    dataset
}

async fn run_json_query(rt: &Runtime, query: &str) -> Result<String, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("query `{query}` failed: {e}"))?;

    let batches: Vec<arrow::array::RecordBatch> = result
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("query `{query}` collect failed: {e}"))?;

    Ok(pretty_format_batches(&batches)
        .map_err(|e| anyhow::anyhow!("format failed: {e}"))?
        .to_string())
}

async fn setup_runtime(test_name: &str, datasets: Vec<Dataset>) -> Result<Runtime, anyhow::Error> {
    let mut builder = AppBuilder::new(test_name);
    for ds in datasets {
        builder = builder.with_dataset(ds);
    }
    let app = builder.build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;
    let cloned_rt = Arc::new(rt.clone());

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    crate::utils::runtime_ready_check(&rt).await;
    Ok(rt)
}

#[tokio::test]
async fn json_array_projection_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "json_array_projection",
                vec![json_dataset(
                    &format!("{S3_BASE}/array_standard.json"),
                    "json_array",
                    "array",
                    false,
                )],
            )
            .await?;
            let result = run_json_query(&rt, "SELECT age FROM json_array ORDER BY age").await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn json_array_projection_accelerated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "json_array_projection_accel",
                vec![json_dataset(
                    &format!("{S3_BASE}/array_standard.json"),
                    "json_array",
                    "array",
                    true,
                )],
            )
            .await?;
            let result = run_json_query(&rt, "SELECT age FROM json_array ORDER BY age").await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn jsonl_projection_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "jsonl_projection",
                vec![json_dataset(
                    &format!("{S3_BASE}/jsonl_standard.json"),
                    "json_lines",
                    "jsonl",
                    false,
                )],
            )
            .await?;
            let result = run_json_query(&rt, "SELECT name FROM json_lines ORDER BY name").await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn json_object_projection_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "json_object_projection",
                vec![json_dataset(
                    &format!("{S3_BASE}/json_object_single.json"),
                    "json_object",
                    "object",
                    false,
                )],
            )
            .await?;
            let result = run_json_query(&rt, "SELECT name, age FROM json_object").await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

// ── Auto-detect format ───────────────────────────────────────────────

#[tokio::test]
async fn json_auto_projection_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "json_auto_projection",
                vec![json_dataset(
                    &format!("{S3_BASE}/array_standard.json"),
                    "json_auto",
                    "auto",
                    false,
                )],
            )
            .await?;
            let result = run_json_query(&rt, "SELECT name FROM json_auto ORDER BY name").await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

// ── SODA (Socrata) format ────────────────────────────────────────────

#[tokio::test]
async fn soda_with_metadata_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "soda_with_metadata",
                vec![json_dataset_with_params(
                    &format!("{S3_BASE}/house_price_index.json"),
                    "house_price_index",
                    "soda",
                    false,
                    &[("soda_metadata", "enabled")],
                )],
            )
            .await?;
            let result = run_json_query(
                &rt,
                "SELECT \":sid\", \":id\", \":position\", observation_date, ctsthpi FROM house_price_index ORDER BY observation_date LIMIT 1",
            )
            .await?;
            insta::assert_snapshot!(result);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn soda_auto_detect_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let rt = setup_runtime(
                "soda_auto_detect",
                vec![json_dataset(
                    &format!("{S3_BASE}/house_price_index.json"),
                    "house_price_index",
                    "auto",
                    false,
                )],
            )
            .await?;

            let result = run_json_query(
                &rt,
                "SELECT * FROM house_price_index ORDER BY observation_date LIMIT 2",
            )
            .await?;
            insta::assert_snapshot!("soda_auto_detect-select_all", result);

            let result = run_json_query(
                &rt,
                "SELECT ctsthpi FROM house_price_index ORDER BY ctsthpi LIMIT 3",
            )
            .await?;
            insta::assert_snapshot!("soda_auto_detect-projection", result);

            Ok(())
        })
        .await
}
