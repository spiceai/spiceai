/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A dataset and a catalog defined with SQL over a live `PostgreSQL`, which is
//! the oracle for what they serve.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Array, RecordBatch, StringArray};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use futures::TryStreamExt;
use runtime::Runtime;

use crate::postgres::common::{self, PG_PASSWORD};
use crate::utils::{
    init_tracing_with_task_history, register_test_connectors, runtime_ready_check,
    test_request_context,
};

async fn start(instance_dir: &Path) -> Arc<Runtime> {
    register_test_connectors().await;
    let rt = Arc::new(
        Runtime::builder()
            .with_app(AppBuilder::new("sql_ddl_postgres").build())
            .with_instance_dir(instance_dir)
            .build()
            .await,
    );
    tokio::time::timeout(Duration::from_secs(120), Arc::clone(&rt).load_components())
        .await
        .expect("components load within two minutes");
    runtime_ready_check(&rt).await;
    rt
}

/// `sql`'s answer read from the source, never from the results cache: what a
/// write through another dataset over the same table changed.
async fn run_uncached(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .results_cache_mode(runtime::datafusion::query::ResultsCacheMode::Bypass)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| e.to_string())
}

async fn run(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| e.to_string())
}

/// Every row as text, one cell per column, `NULL` for a null.
fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).expect("formatter"))
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect(),
            );
        }
    }
    rows
}

/// The rows `sql` returns from `PostgreSQL` itself, as text.
async fn postgres_rows(port: usize, sql: &str) -> Vec<Vec<String>> {
    let pool = common::get_postgres_connection_pool(port, None)
        .await
        .expect("pool");
    let conn = pool.connect_direct().await.expect("connect");
    conn.conn
        .query(sql, &[])
        .await
        .expect("PostgreSQL query")
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    row.get::<_, Option<String>>(i)
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect()
        })
        .collect()
}

async fn message(rt: &Runtime, sql: &str) -> String {
    let batches = run(rt, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    let batch = batches.first().expect("one batch");
    batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a string column")
        .value(0)
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_defined_postgres_dataset_and_catalog_serve_what_postgres_serves() {
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container()
                .await
                .expect("PostgreSQL starts");
            let port = usize::from(container.host_port(5432).expect("published port"));
            let pool = common::get_postgres_connection_pool(port, None)
                .await
                .expect("pool");
            pool.connect_direct()
                .await
                .expect("connect")
                .conn
                .simple_query(
                    "CREATE TABLE public.orders (id INT PRIMARY KEY, name TEXT, amount NUMERIC(10, 2)); \
                     INSERT INTO public.orders VALUES (1, 'alpha', 10.50), (2, NULL, 20.25), (3, 'gamma', NULL);",
                )
                .await
                .expect("seed");
            let oracle = postgres_rows(
                port,
                "SELECT id::text, name, amount::text FROM public.orders ORDER BY id",
            )
            .await;
            assert_eq!(oracle.len(), 3, "the seed is in place");

            let dir = tempfile::tempdir().expect("temp dir");
            let instance_dir = dir.path().join(".spice");
            let rt = start(&instance_dir).await;
            // Task history records the statements only through this tracing.
            let (_tracing, trace_provider) = init_tracing_with_task_history(None, &rt);
            let connection = format!(
                "pg_host = 'localhost', pg_port = {port}, pg_user = 'postgres', pg_pass = '{PG_PASSWORD}', pg_db = 'postgres', pg_sslmode = 'disable'"
            );

            assert_eq!(
                message(
                    &rt,
                    &format!("CREATE TABLE orders WITH (from = 'postgres:public.orders', {connection})")
                )
                .await,
                "Created dataset 'orders'."
            );
            let select = "SELECT CAST(id AS TEXT) AS id_text, name, CAST(amount AS TEXT) AS amount_text FROM orders ORDER BY id";
            assert_eq!(rows(&run(&rt, select).await.expect("select")), oracle);

            assert_eq!(
                message(
                    &rt,
                    &format!("CREATE CATALOG pg WITH (from = 'pg', include = ARRAY['public.*'], {connection})")
                )
                .await,
                "Created catalog 'pg'."
            );
            let catalog_select = "SELECT CAST(id AS TEXT) AS id_text, name, CAST(amount AS TEXT) AS amount_text FROM pg.public.orders ORDER BY id";
            assert_eq!(rows(&run(&rt, catalog_select).await.expect("select")), oracle);

            // The password is shown nowhere: not by SHOW CREATE, not in task history.
            let shown = message(&rt, "SHOW CREATE TABLE orders").await;
            assert!(shown.contains("pg_pass = '<redacted>'"), "{shown}");
            assert!(!shown.contains(PG_PASSWORD), "{shown}");
            let _ = trace_provider.force_flush();
            let mut recorded = Vec::new();
            let mut last_error = None;
            for _ in 0..100 {
                match run(
                    &rt,
                    "SELECT input FROM runtime.task_history WHERE input LIKE 'CREATE %' ORDER BY start_time",
                )
                .await
                {
                    Ok(batches) => recorded = rows(&batches),
                    Err(e) => last_error = Some(e),
                }
                if recorded.len() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(
                recorded.len(),
                2,
                "both statements are recorded: {recorded:?}, last error: {last_error:?}"
            );
            for row in &recorded {
                assert!(row[0].contains("pg_pass = '<redacted>'"), "{row:?}");
                assert!(!row[0].contains(PG_PASSWORD), "{row:?}");
            }

            // A dataset defined writable takes DML into its source, as a
            // Spicepod `access: read_write` dataset does.
            message(
                &rt,
                &format!(
                    "CREATE TABLE orders_rw WITH (from = 'postgres:public.orders', access = 'read_write', {connection})"
                ),
            )
            .await;
            run(&rt, "INSERT INTO orders_rw VALUES (4, 'delta', 1.00)")
                .await
                .expect("insert into a writable dataset");
            assert_eq!(
                postgres_rows(
                    port,
                    "SELECT id::text, name, amount::text FROM public.orders WHERE id = 4"
                )
                .await,
                vec![vec!["4".to_string(), "delta".to_string(), "1.00".to_string()]]
            );
            let err = run(&rt, "INSERT INTO orders VALUES (5, 'eta', 2.00)")
                .await
                .expect_err("a dataset defined without write access refuses DML");
            assert!(err.contains("read-only dataset 'orders'"), "{err}");
            message(&rt, "DROP TABLE orders_rw").await;
            let oracle = postgres_rows(
                port,
                "SELECT id::text, name, amount::text FROM public.orders ORDER BY id",
            )
            .await;
            assert_eq!(
                rows(&run_uncached(&rt, select).await.expect("select")),
                oracle
            );

            assert_eq!(message(&rt, "DROP CATALOG pg").await, "Dropped catalog 'pg'.");
            let err = run(&rt, catalog_select).await.expect_err("dropped catalog");
            assert!(err.contains("pg"), "{err}");

            // What SQL defined is what a restart serves.
            drop(rt);
            let rt = start(&instance_dir).await;
            assert_eq!(rows(&run(&rt, select).await.expect("select")), oracle);
            let err = run(&rt, catalog_select)
                .await
                .expect_err("the catalog stays dropped");
            assert!(err.contains("pg"), "{err}");
            message(
                &rt,
                &format!("CREATE CATALOG pg WITH (from = 'pg', include = ARRAY['public.*'], {connection})"),
            )
            .await;
            drop(rt);
            let rt = start(&instance_dir).await;
            assert_eq!(rows(&run(&rt, catalog_select).await.expect("select")), oracle);

            drop(rt);
            container.remove().await.expect("remove PostgreSQL");
        })
        .await;
}
