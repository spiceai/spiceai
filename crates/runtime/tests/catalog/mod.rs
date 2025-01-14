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

use crate::{get_test_datafusion, init_tracing, utils::test_request_context};
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use runtime::extension::ExtensionFactory;
use runtime::{status, Runtime};
use spice_cloud::SpiceExtensionFactory;
use spicepod::component::catalog::Catalog;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn spiceai_integration_test_catalog() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("spiceai_catalog_test")
                .with_catalog(Catalog::new(
                    "spice.ai/spiceai/tpch".to_string(),
                    "spc".to_string(),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
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
                .query_builder("SELECT * FROM spc.tpch.customer LIMIT 10")
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
        })
        .await
}

#[tokio::test]
async fn spiceai_integration_test_catalog_include() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let mut catalog = Catalog::new("spice.ai/spiceai/tpch".to_string(), "spc".to_string());
            catalog.include = vec!["tpch.customer".to_string(), "tpch.part*".to_string()];
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
               AND table_catalog = 'spc' 
             ORDER BY table_name",
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
                    "+---------------+--------------+------------+------------+",
                    "| table_catalog | table_schema | table_name | table_type |",
                    "+---------------+--------------+------------+------------+",
                    "| spc           | tpch         | customer   | BASE TABLE |",
                    "| spc           | tpch         | part       | BASE TABLE |",
                    "| spc           | tpch         | partsupp   | BASE TABLE |",
                    "+---------------+--------------+------------+------------+",
                ],
                &results
            );

            Ok(())
        })
        .await
}
