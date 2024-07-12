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
use runtime::Runtime;
use runtime::{datafusion::query::Protocol, extension::ExtensionFactory};
use spice_cloud::SpiceExtensionFactory;
use spicepod::component::catalog::Catalog;
use spicepod::component::secrets::SpiceSecretStore;
use std::collections::HashMap;

#[tokio::test]
#[cfg(feature = "spiceai-dataset-test")]
async fn spiceai_catalog_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let app = AppBuilder::new("spiceai_catalog_test")
        .with_secret_store(SpiceSecretStore::File)
        .with_catalog(Catalog::new("spiceai".to_string(), "spiceai".to_string()))
        .build();

    let df = get_test_datafusion();

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
            "SELECT * FROM spiceai.eth.recent_blocks LIMIT 10".to_string(),
            Protocol::Flight,
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
#[cfg(feature = "spiceai-dataset-test")]
async fn spiceai_catalog_test_include() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let mut catalog = Catalog::new("spiceai".to_string(), "spiceai".to_string());
    catalog.include = vec![
        "eth.recent_bl*".to_string(),
        "eth.recent_transactions".to_string(),
    ];
    let app = AppBuilder::new("spiceai_catalog_test")
        .with_secret_store(SpiceSecretStore::File)
        .with_catalog(catalog)
        .build();

    let df = get_test_datafusion();

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
             ORDER BY table_name"
                .to_string(),
            Protocol::Flight,
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
