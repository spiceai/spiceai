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
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use anyhow::Context;
use app::AppBuilder;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use runtime::Runtime;
use spicepod::{component::catalog::Catalog, param::Params};
use std::sync::Arc;

#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn glue_iceberg_integration_test_catalog() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    let account_id =
        std::env::var("AWS_ICEBERG_ACCOUNT_ID").context("AWS_ICEBERG_ACCOUNT_ID is not set")?;

    test_request_context()
        .scope(async {
            let mut db_catalog =
                Catalog::new(format!("iceberg:https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces"), "ice_glue".to_string());

            db_catalog.include = vec!["testdb_001.*".to_string(), "testdb_002.*".to_string()];
            db_catalog.params = Some(get_params());

            let app = AppBuilder::new("glue_iceberg_integration_test_catalog")
                .with_catalog(db_catalog)
                .build();

            let rt =
                Runtime::builder()
                    .with_app(app)
                    .with_datafusion_configuration_fn(configure_test_datafusion)
                    .build()
                    .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let mut result = rt.datafusion().query_builder("SELECT * FROM ice_glue.testdb_001.iceberg_table_001 LIMIT 10").build().run().await?;

            let mut results: Vec<RecordBatch> = vec![];
            while let Some(batch) = result.data.next().await {
                results.push(batch?);
            }

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;

            insta::assert_snapshot!("glue_iceberg_integration_test_catalog", pretty);

            Ok(())
        })
        .await
}

fn get_params() -> Params {
    Params::from_string_map(
        vec![
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
        ]
        .into_iter()
        .collect(),
    )
}
