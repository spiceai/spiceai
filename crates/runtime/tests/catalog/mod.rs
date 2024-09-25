/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::{get_test_datafusion, init_tracing};
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use runtime::{datafusion::query::Protocol, extension::ExtensionFactory};
use runtime::{status, Runtime};
use spice_cloud::SpiceExtensionFactory;
use spicepod::component::catalog::Catalog;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn spiceai_integration_test_catalog() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let app = AppBuilder::new("spiceai_catalog_test")
        .with_catalog(Catalog::new("spice.ai".to_string(), "spiceai".to_string()))
        .build();

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .with_runtime_status(status)
        .with_autoload_extensions(HashMap::from([(
            "spice_cloud".to_string(),
            Box::new(SpiceExtensionFactory::default()) as Box<dyn ExtensionFactory>,
        )]))
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            panic!("Timeout waiting for components to load");
        }
        () = rt.load_components() => {}
    }

    let mut result = rt
        .datafusion()
        .query_builder(
            "SELECT * FROM spiceai.eth.recent_blocks LIMIT 10",
            Protocol::Internal,
        )
        .build()
        .run()
        .await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 10);

    Ok(())
}

#[tokio::test]
async fn spiceai_integration_test_catalog_include() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let mut catalog = Catalog::new("spice.ai".to_string(), "spiceai".to_string());
    catalog.include = vec![
        "eth.recent_bl*".to_string(),
        "eth.recent_transactions".to_string(),
    ];
    let app = AppBuilder::new("spiceai_catalog_test")
        .with_catalog(catalog)
        .build();

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .with_autoload_extensions(HashMap::from([(
            "spice_cloud".to_string(),
            Box::new(SpiceExtensionFactory::default()) as Box<dyn ExtensionFactory>,
        )]))
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            panic!("Timeout waiting for components to load");
        }
        () = rt.load_components() => {}
    }

    let mut result = rt
        .datafusion()
        .query_builder(
            "SELECT table_catalog, table_schema, table_name, table_type 
             FROM information_schema.tables 
             WHERE table_schema != 'information_schema' 
               AND table_catalog = 'spiceai' 
             ORDER BY table_name",
            Protocol::Internal,
        )
        .build()
        .run()
        .await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    assert_eq!(results.len(), 1);
    assert_batches_eq!(
        &[
            "+---------------+--------------+---------------------+------------+",
            "| table_catalog | table_schema | table_name          | table_type |",
            "+---------------+--------------+---------------------+------------+",
            "| spiceai       | eth          | recent_blocks       | BASE TABLE |",
            "| spiceai       | eth          | recent_transactions | BASE TABLE |",
            "+---------------+--------------+---------------------+------------+",
        ],
        &results
    );

    Ok(())
}
