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

use app::AppBuilder;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common,
    utils::{run_query, runtime_ready_check, test_request_context, to_pretty_display},
};

const DUCKDB_FILE_PATH: &str = "./schema_evolution.duckdb";

#[tokio::test]
async fn test_schema_evolution() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    if std::fs::metadata(DUCKDB_FILE_PATH).is_ok() {
        std::fs::remove_file(DUCKDB_FILE_PATH).expect("should remove local database");
    }

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            // Reset the table to the initial state
            reset_pg_table(&db_conn).await;

            let rt = Arc::new(initialize_runtime(port).await?);

            // This query should continue to work across all of the table mutations below.
            let sql = "SELECT id, town, age FROM cham ORDER BY id ASC";
            run_and_verify_query(&rt, sql, "test_schema_evolution_initial").await;

            // Test 1: Add a new column
            rt.shutdown().await;
            drop(rt);
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_add_column").await;

            // Test 2: Drop a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(&db_conn, "ALTER TABLE public.chameleon DROP COLUMN age;").await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_drop_column").await;

            // Test 3: Rename a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon RENAME COLUMN town TO city;",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_rename_column").await;

            // Test 4: Change the data type of a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE chameleon
                ALTER COLUMN age
                TYPE TEXT
                USING (age::TEXT);",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_change_column_type").await;

            // Test 5: Drop the table
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(&db_conn, "DROP TABLE IF EXISTS public.chameleon;").await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_drop_table").await;

            running_container.remove().await?;

            if std::fs::metadata(DUCKDB_FILE_PATH).is_ok() {
                std::fs::remove_file(DUCKDB_FILE_PATH).expect("should remove local database");
            }

            Ok(())
        })
        .await
}

#[expect(clippy::expect_used)]
async fn run_and_verify_query(rt: &Arc<Runtime>, sql: &str, snapshot_name: &str) {
    let record_batch = run_query(rt, sql).await.expect("query should succeed");
    insta::assert_snapshot!(
        snapshot_name,
        to_pretty_display(&record_batch).expect("pretty display")
    );
}

async fn reset_pg_table(db_conn: &PostgresConnection) {
    execute_pg_statement(db_conn, "DROP TABLE IF EXISTS public.chameleon;").await;
    execute_pg_statement(
        db_conn,
        "CREATE TABLE public.chameleon (id varchar NOT NULL, town varchar NULL, age int4 NULL, CONSTRAINT chameleon_pk PRIMARY KEY (id));",
    )
    .await;
    execute_pg_statement(
        db_conn,
        "INSERT INTO public.chameleon (id, town, age) VALUES ('1', 'London', 30), ('2', 'Paris', 25), ('3', 'New York', 35);",
    )
    .await;
}

#[expect(clippy::expect_used)]
async fn execute_pg_statement(db_conn: &PostgresConnection, sql: &str) {
    db_conn
        .conn
        .execute(sql, &[])
        .await
        .expect("statement can be executed");
}

async fn initialize_runtime(port: usize) -> Result<Runtime, anyhow::Error> {
    let mut ds = Dataset::new("postgres:chameleon", "cham");

    let params = Params::from_string_map(
        vec![
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    ds.params = Some(params.clone());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        params: Some(Params::from_string_map(
            vec![("duckdb_file".to_string(), DUCKDB_FILE_PATH.to_string())]
                .into_iter()
                .collect(),
        )),
        ..Acceleration::default()
    });

    let app = AppBuilder::new("test_schema_evolution")
        .with_dataset(ds)
        .build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;

    let cloned_rt = Arc::new(rt.clone());

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(rt)
}
