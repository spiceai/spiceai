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
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::TryStreamExt;
use runtime::{Runtime, component::dataset::builder::DatasetBuilder};
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::{collections::HashMap, sync::Arc};

use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_checkpoint_postgres::PostgresDatasetCheckpointer;

use crate::utils::test_request_context;
use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params, get_random_port},
    utils::{register_test_connectors, runtime_ready_check},
};

#[tokio::test]
async fn test_acceleration_postgres_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

/// A schema repair must correct the recorded schema without telling the refresh scheduler
/// the data was just refreshed. Regression test for #13817.
///
/// Drives the real `PostgresDatasetCheckpointer` against a `PostgreSQL` container: the
/// checkpoint crate itself has no test harness, and a container-dependent unit test there
/// would run under `make nextest`, which has no database.
#[tokio::test]
async fn test_postgres_checkpoint_set_schema_preserves_the_freshness_clock()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let port: usize = get_random_port()?;
    let running_container = common::start_postgres_docker_container(port).await?;

    let pool = Arc::new(common::get_postgres_connection_pool(port, None).await?);
    let checkpointer =
        PostgresDatasetCheckpointer::try_new(Arc::clone(&pool), "ds_13817".to_string())
            .await
            .map_err(|e| anyhow!("Failed to open the checkpoint table: {e}"))?;

    let original = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    // What a repair writes back: the same columns, `name` no longer nullable.
    let repaired = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    checkpointer
        .checkpoint(&original, Some("SELECT 1"))
        .await
        .map_err(|e| anyhow!("Failed to seed the checkpoint: {e}"))?;

    // Backdate the recorded refresh by seven days, as a dataset bootstrapping from a
    // legacy snapshot would be.
    pool.connect_direct()
        .await
        .map_err(|e| anyhow!("Failed to connect to PostgreSQL: {e}"))?
        .conn
        .execute(
            "UPDATE spice_sys_dataset_checkpoint SET updated_at = CURRENT_TIMESTAMP - INTERVAL '7 days' WHERE dataset_name = 'ds_13817'",
            &[],
        )
        .await?;

    let before = checkpointer
        .last_checkpoint_time()
        .await
        .map_err(|e| anyhow!("Failed to read the checkpoint time: {e}"))?
        .ok_or_else(|| anyhow!("Expected a checkpoint time"))?;

    checkpointer
        .set_schema(&repaired)
        .await
        .map_err(|e| anyhow!("Failed to write the schema: {e}"))?;

    // Read back through a fresh checkpointer over the same store: acceptance is what the
    // row holds, not what the call returned.
    let reader = PostgresDatasetCheckpointer::try_new(Arc::clone(&pool), "ds_13817".to_string())
        .await
        .map_err(|e| anyhow!("Failed to reopen the checkpoint table: {e}"))?;

    let after = reader
        .last_checkpoint_time()
        .await
        .map_err(|e| anyhow!("Failed to read the checkpoint time: {e}"))?
        .ok_or_else(|| anyhow!("Expected a checkpoint time"))?;
    assert_eq!(
        after, before,
        "a schema-only write must leave the freshness clock alone"
    );

    assert_eq!(
        reader
            .get_schema()
            .await
            .map_err(|e| anyhow!("Failed to read the schema: {e}"))?
            .ok_or_else(|| anyhow!("Expected a stored schema"))?,
        repaired,
        "the repaired schema must be the one stored"
    );

    assert_eq!(
        reader
            .get_refresh_sql()
            .await
            .map_err(|e| anyhow!("Failed to read the refresh SQL: {e}"))?,
        Some("SELECT 1".to_string()),
        "a schema-only write must preserve the stored refresh SQL"
    );

    // A dataset with no checkpoint must not gain one: a row created here would carry a
    // fresh `updated_at`, which is the deferral the schema-only write exists to avoid.
    let absent = PostgresDatasetCheckpointer::try_new(Arc::clone(&pool), "ds_13817_absent".into())
        .await
        .map_err(|e| anyhow!("Failed to open the checkpoint table: {e}"))?;
    absent
        .set_schema(&repaired)
        .await
        .map_err(|e| anyhow!("Failed to write the schema: {e}"))?;
    assert!(
        !absent.exists().await,
        "a schema-only write must not create a checkpoint row"
    );

    running_container.remove().await?;

    Ok(())
}
