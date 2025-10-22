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
use crate::acceleration::refresh::common::{
    execute_ps_sql, execute_rt_sql, get_acceleration_config_append, get_acceleration_config_full,
    initialize_postgres, refresh_table, start_test_runtime,
};
use crate::postgres::common;
use crate::postgres::common::get_random_port;
use crate::{init_tracing, utils::test_request_context};
use spicepod::acceleration::Mode;
use std::sync::Arc;

#[tokio::test]
async fn test_acceleration_refresh_pepper_append() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;
            let mut acceleration_config = get_acceleration_config_append("pepper", None);
            acceleration_config.mode = Mode::File;
            let rt = start_test_runtime(port, acceleration_config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (now());",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_acceleration_refresh_pepper_full() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;
            let mut acceleration_config = get_acceleration_config_full("pepper", None);
            acceleration_config.mode = Mode::File;
            let rt = start_test_runtime(port, acceleration_config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (now());",
            )
            .await?;

            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(
                results
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}
