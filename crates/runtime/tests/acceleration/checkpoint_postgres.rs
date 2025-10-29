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

use crate::acceleration::wait_for_checkpoints;
use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::TryStreamExt;
use runtime::{Runtime, component::dataset::builder::DatasetBuilder};
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::{collections::HashMap, sync::Arc};

use crate::utils::test_request_context;
use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params, get_random_port},
    utils::runtime_ready_check,
};

#[tokio::test]
async fn test_acceleration_postgres_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;

            let mut dataset =
                Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal");
            dataset.acceleration = Some(Acceleration {
                params: Some(Params::from_string_map(
                    get_pg_params(port)
                        .into_iter()
                        .map(|(k, v)| (k, v.expose_secret().to_string()))
                        .collect::<HashMap<String, String>>(),
                )),
                enabled: true,
                engine: Some("postgres".to_string()),
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: Some("SELECT * FROM decimal".to_string()),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_acceleration_postgres_metadata")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let app_ref = rt.app();
            let app_lock = app_ref.read().await;
            let Some(app) = app_lock.as_ref() else {
                return Err(anyhow!("Failed to obtain app from runtime"));
            };

            let cloned_rt = Arc::clone(&rt);
            let runtime_datasets = app
                .datasets
                .clone()
                .into_iter()
                .map(DatasetBuilder::try_from)
                .map(move |ds_builder| {
                    ds_builder
                        .map_err(|e| anyhow!("Failed to create dataset builder: {e}"))
                        .and_then(|ds_builder| {
                            ds_builder
                                .with_app(Arc::clone(app))
                                .with_runtime(Arc::clone(&cloned_rt))
                                .build()
                                .map_err(|e| anyhow!("Failed to build dataset: {e}"))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify checkpoints are created before shutting down runtime
            wait_for_checkpoints(runtime_datasets, 120).await?;

            rt.shutdown().await;
            drop(rt);

            let db_conn = pool.connect().await.expect("connection can be established");
            let result = db_conn
                .as_async()
                .expect("async connection")
                .query_arrow(
                    "SELECT dataset_name FROM spice_sys_dataset_checkpoint",
                    &[],
                    None,
                )
                .await
                .expect("query arrow")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("try collect");

            let pretty = arrow::util::pretty::pretty_format_batches(&result).expect("pretty print");
            insta::assert_snapshot!(pretty);

            let decimal_result = db_conn
                .as_async()
                .expect("async connection")
                .query_arrow("SELECT * FROM decimal ORDER BY id", &[], None)
                .await
                .expect("query arrow")
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect("try collect");

            let pretty_decimal =
                arrow::util::pretty::pretty_format_batches(&decimal_result).expect("pretty print");
            insta::assert_snapshot!(pretty_decimal);

            running_container.remove().await?;

            Ok(())
        })
        .await
}
