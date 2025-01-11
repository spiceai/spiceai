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
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use runtime::{
    accelerated_table::refresh::RefreshOverrides, component::dataset::acceleration::RefreshMode,
    status, Runtime,
};
use spicepod::component::dataset::{acceleration::Acceleration, Dataset};

use crate::{
    get_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context, wait_until_true},
};

fn make_spiceai_dataset(path: &str, name: &str, refresh_sql: String) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai/{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some(refresh_sql),
        ..Default::default()
    });
    ds
}

#[tokio::test]
async fn spiceai_integration_test_refresh_sql_override_append() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("refresh_sql_override_append")
                .with_dataset(make_spiceai_dataset(
                    "spiceai/tpch/datasets/tpch.nation",
                    "nation",
                    "SELECT * FROM nation WHERE n_regionkey != 0".to_string(),
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

            runtime_ready_check(&rt).await;

            let query = rt
                .datafusion()
                .query_builder("SELECT * FROM nation WHERE n_regionkey = 0")
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            assert_eq!(
                results.len(),
                0,
                "Expected refresh SQL to filter out all rows for n_regionkey = 0"
            );

            rt.datafusion()
                .refresh_table(
                    &TableReference::parse_str("nation"),
                    Some(RefreshOverrides {
                        sql: Some("SELECT * FROM nation WHERE n_regionkey = 0".to_string()),
                        mode: Some(RefreshMode::Append),
                        max_jitter: None,
                    }),
                )
                .await?;

            assert!(
                wait_until_true(Duration::from_secs(10), || async {
                    let Ok(query) = rt
                        .datafusion()
                        .query_builder("SELECT * FROM nation WHERE n_regionkey = 0")
                        .build()
                        .run()
                        .await
                    else {
                        return false;
                    };

                    let results: Vec<RecordBatch> =
                        match query.data.try_collect::<Vec<RecordBatch>>().await {
                            Ok(results) => results,
                            Err(_) => return false,
                        };
                    !results.is_empty()
                })
                .await
            );

            let query = rt
                .datafusion()
                .query_builder(
                    "SELECT * FROM nation WHERE n_regionkey = 0 ORDER BY n_nationkey DESC",
                )
                .build()
                .run()
                .await?;

            let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
            let results_str =
                arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
            insta::assert_snapshot!(results_str);

            Ok(())
        })
        .await
}
