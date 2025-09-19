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
use crate::acceleration::refresh::common::{
    configure_spice_ai_runtime, get_acceleration_config, initialize_postgres, read_sql,
    refresh_table, run_ps_sql,
};
use crate::postgres::common;
use crate::postgres::common::{PG_PASSWORD, get_random_port};
use crate::{init_tracing, utils::test_request_context};
use datafusion::sql::TableReference;
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_acceleration_refresh_duckdb() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let db_conn = initialize_postgres(port).await?;
            let acceleration_params: HashMap<String, String> = [
                ("pg_host".to_string(), "localhost".to_string()),
                ("pg_user".to_string(), "postgres".to_string()),
                ("pg_pass".to_string(), PG_PASSWORD.to_string()),
                ("pg_db".to_string(), "acceleration".to_string()),
                ("pg_sslmode".to_string(), "disable".to_string()),
                ("pg_port".to_string(), port.to_string()),
            ]
            .iter()
            .cloned()
            .collect();
            let acceleration_config = get_acceleration_config(
                "postgres",
                Some(Params::from_string_map(acceleration_params)),
            );
            let rt = configure_spice_ai_runtime(port, acceleration_config).await?;

            let notifier = rt
                .datafusion()
                .refresh_table(&TableReference::from("test_table"), None)
                .await?;
            notifier.expect("notifier").notified().await;

            let results = read_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(results.len(), 1);
            assert_eq!(results.first().expect("batch").num_rows(), 1);

            run_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (now());",
            )
            .await;
            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = read_sql(Arc::clone(&rt), "SELECT * from test_table").await?;
            assert_eq!(results.len(), 1);
            assert_eq!(results.first().expect("batch").num_rows(), 2);

            running_container.remove().await?;
            Ok(())
        })
        .await
}
