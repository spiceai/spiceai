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

use arrow::array::RecordBatch;
use futures::StreamExt;
use runtime::Runtime;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

#[tokio::test]
async fn iceberg_integration_test_audit_log_query() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let app = test_framework::app_utils::load_app_from_spicepod_str(include_str!(
                "audit_log.yaml"
            )).await?;

            let rt = Runtime::builder()
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

            let mut result = rt.datafusion().query_builder("SELECT * FROM audit_log WHERE audit_log_type = 'sql_query' ORDER BY id DESC LIMIT 10").build().run().await?;

            let mut results: Vec<RecordBatch> = vec![];
            while let Some(batch) = result.data.next().await {
                results.push(batch?);
            }

            let pretty = arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;

            insta::assert_snapshot!("audit_log_query", pretty);

            Ok(())
        })
        .await
}
