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

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use std::{sync::Arc, time::Duration};

use app::AppBuilder;

use runtime::Runtime;
use spicepod::{acceleration::Acceleration, component::dataset::Dataset};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn make_spiceai_dataset(path: &str, name: &str, retention_sql: String) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai/{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        retention_sql: Some(retention_sql),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        ..Default::default()
    });
    ds
}

#[tokio::test]
async fn test_retention_sql() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("retention_sql")
                .with_dataset(make_spiceai_dataset(
                    "spiceai/tpch/datasets/tpch.nation",
                    "nation",
                    "DELETE FROM nation WHERE n_nationkey >= 5 OR n_name NOT LIKE '%A'".to_string(),
                ))
                .build();

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

            tokio::time::sleep(Duration::from_secs(1)).await; // Allow retention to complete

            let query = rt
                .datafusion()
                .query_builder("SELECT * FROM nation")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            // keep only ALGERIA, ARGENTINA and CANADA
            assert_eq!(
                results.iter().map(RecordBatch::num_rows).sum::<usize>(),
                3,
                "Expected retention SQL to filter out all rows except ALGERIA, ARGENTINA and CANADA"
            );

            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!(results_str);

            Ok(())
        })
        .await
}
