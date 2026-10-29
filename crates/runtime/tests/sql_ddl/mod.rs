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

//! Datasets defined with SQL.
//!
//! `CREATE TABLE ... WITH (from = ...)` defines the dataset a Spicepod
//! `datasets:` entry declares. Each test runs a runtime whose Spicepod declares
//! `pod_orders` over a CSV file, and defines `orders` over the same file with
//! SQL: the Spicepod dataset is the oracle for what the SQL one serves. The
//! runtime is restarted over the same instance directory to show what SQL
//! defined is kept.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app::{App, AppBuilder};
use arrow::array::{RecordBatch, StringArray};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};

const ORDERS_CSV: &str = "id,name,amount\n1,alpha,10.5\n2,beta,20.25\n3,gamma,\n";
const OTHER_CSV: &str = "id,name,amount\n7,eta,1.0\n";

struct Fixture {
    _dir: tempfile::TempDir,
    orders_csv: PathBuf,
    other_csv: PathBuf,
    instance_dir: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let orders_csv = dir.path().join("orders.csv");
        std::fs::write(&orders_csv, ORDERS_CSV).expect("write orders.csv");
        let other_csv = dir.path().join("other.csv");
        std::fs::write(&other_csv, OTHER_CSV).expect("write other.csv");
        let instance_dir = dir.path().join(".spice");
        Self {
            _dir: dir,
            orders_csv,
            other_csv,
            instance_dir,
        }
    }
}

fn csv_dataset(path: &Path, name: &str) -> Dataset {
    Dataset::new(format!("file:{}", path.display()), name).with_params(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ))
}

/// A runtime over `fixture`: its Spicepod declares `pod_orders`, and what SQL
/// defined is kept in the fixture's instance directory.
async fn start(fixture: &Fixture) -> Arc<Runtime> {
    let app = AppBuilder::new("sql_ddl")
        .with_dataset(csv_dataset(&fixture.orders_csv, "pod_orders"))
        .build();
    start_with(fixture, app).await
}

/// A runtime over `fixture` whose Spicepod is `app`.
async fn start_with(fixture: &Fixture, app: App) -> Arc<Runtime> {
    register_test_connectors().await;
    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_instance_dir(&fixture.instance_dir)
            .build()
            .await,
    );
    tokio::time::timeout(Duration::from_secs(60), Arc::clone(&rt).load_components())
        .await
        .expect("components load within a minute");
    runtime_ready_check(&rt).await;
    rt
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

/// The one string a statement returns, as DDL returns its message.
async fn message(rt: &Runtime, sql: &str) -> String {
    let batches = run(rt, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    let batch = batches.first().expect("one batch");
    let column = batch.column(batch.num_columns() - 1);
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a string column")
        .value(0)
        .to_string()
}

async fn table(rt: &Runtime, sql: &str) -> String {
    let batches = run(rt, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format batches")
        .to_string()
}

async fn error(rt: &Runtime, sql: &str) -> String {
    match run(rt, sql).await {
        Ok(batches) => panic!("{sql} succeeded: {batches:?}"),
        Err(e) => e,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sql_dataset_serves_what_its_spicepod_twin_serves_and_outlives_a_restart() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;

            let create = format!(
                "CREATE TABLE orders WITH (from = 'file:{}', file_format = 'csv', description = 'Orders')",
                fixture.orders_csv.display()
            );
            assert_eq!(message(&rt, &create).await, "Created dataset 'orders'.");

            let expected = table(&rt, "SELECT * FROM pod_orders ORDER BY id").await;
            assert_eq!(
                expected,
                "+----+-------+--------+\n\
                 | id | name  | amount |\n\
                 +----+-------+--------+\n\
                 | 1  | alpha | 10.5   |\n\
                 | 2  | beta  | 20.25  |\n\
                 | 3  | gamma |        |\n\
                 +----+-------+--------+"
            );
            assert_eq!(table(&rt, "SELECT * FROM orders ORDER BY id").await, expected);

            assert_eq!(
                message(&rt, "SHOW CREATE TABLE orders").await,
                format!(
                    "CREATE TABLE orders WITH (\n  from = 'file:{}',\n  file_format = 'csv',\n  description = 'Orders'\n)",
                    fixture.orders_csv.display()
                )
            );
            // A Spicepod dataset has its statement too.
            assert_eq!(
                message(&rt, "SHOW CREATE TABLE pod_orders").await,
                format!(
                    "CREATE TABLE pod_orders WITH (\n  from = 'file:{}',\n  file_format = 'csv'\n)",
                    fixture.orders_csv.display()
                )
            );

            assert_eq!(
                message(&rt, "ALTER TABLE orders SET (description = 'All orders')").await,
                "Changed dataset 'orders'."
            );
            assert_eq!(
                message(&rt, "ALTER TABLE orders SET (description = 'All orders')").await,
                "Dataset 'orders' is unchanged."
            );

            drop(rt);
            let rt = start(&fixture).await;
            assert_eq!(table(&rt, "SELECT * FROM orders ORDER BY id").await, expected);
            assert!(
                message(&rt, "SHOW CREATE TABLE orders")
                    .await
                    .contains("description = 'All orders'"),
                "the change is kept"
            );

            assert_eq!(message(&rt, "DROP TABLE orders").await, "Dropped dataset 'orders'.");
            assert!(
                error(&rt, "SELECT * FROM orders").await.contains("not found"),
                "a dropped dataset is no longer queryable"
            );
            drop(rt);
            let rt = start(&fixture).await;
            assert!(
                error(&rt, "SELECT * FROM orders").await.contains("not found"),
                "a dropped dataset stays dropped"
            );
            assert_eq!(table(&rt, "SELECT * FROM pod_orders ORDER BY id").await, expected);
        })
        .await;
}

/// Tools send statements after a comment, which changes nothing about what a
/// statement does; and a definition is never shown from a plan or a result
/// cached before it changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_after_a_comment_defines_and_shows_the_latest_definition() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;
            let create = format!(
                "-- the orders\nCREATE TABLE orders WITH (from = 'file:{}', file_format = 'csv', metadata.owner = 'sales')",
                fixture.orders_csv.display()
            );
            assert_eq!(message(&rt, &create).await, "Created dataset 'orders'.");

            let show = "/* the definition */ SHOW CREATE TABLE orders";
            let before = format!(
                "CREATE TABLE orders WITH (\n  from = 'file:{}',\n  file_format = 'csv',\n  \"metadata.owner\" = 'sales'\n)",
                fixture.orders_csv.display()
            );
            assert_eq!(message(&rt, show).await, before);
            assert_eq!(
                message(&rt, "-- describe it\nALTER TABLE orders SET (description = 'All orders')").await,
                "Changed dataset 'orders'."
            );
            assert_eq!(
                message(&rt, show).await,
                format!(
                    "CREATE TABLE orders WITH (\n  from = 'file:{}',\n  file_format = 'csv',\n  description = 'All orders',\n  \"metadata.owner\" = 'sales'\n)",
                    fixture.orders_csv.display()
                ),
                "the same statement shows the change"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_does_not_change_a_spicepod_dataset() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;
            let create = format!(
                "CREATE TABLE pod_orders WITH (from = 'file:{}', file_format = 'csv')",
                fixture.other_csv.display()
            );
            assert!(
                error(&rt, &create).await.contains(
                    "Dataset 'pod_orders' is defined in the Spicepod, so SQL cannot create it."
                ),
                "create"
            );
            assert!(
                error(&rt, "DROP TABLE pod_orders").await.contains(
                    "Dataset 'pod_orders' is defined in the Spicepod, so SQL cannot drop it."
                ),
                "drop"
            );
            assert!(
                error(&rt, "ALTER TABLE pod_orders SET (description = 'x')")
                    .await
                    .contains(
                        "Dataset 'pod_orders' is defined in the Spicepod, so SQL cannot change it."
                    ),
                "alter"
            );
            assert_eq!(
                table(&rt, "SELECT count(*) AS n FROM pod_orders").await,
                "+---+\n| n |\n+---+\n| 3 |\n+---+"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_definition_that_would_not_load_is_refused_and_not_kept() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;

            let misspelled = format!(
                "CREATE TABLE orders WITH (from = 'file:{}', file_fromat = 'csv')",
                fixture.orders_csv.display()
            );
            let err = error(&rt, &misspelled).await;
            assert!(
                err.contains("Dataset 'orders' has the parameter 'file_fromat', which its connector does not read. Did you mean 'file_format'?"),
                "{err}"
            );

            let unknown_connector = "CREATE TABLE orders WITH (from = 'postgress:public.orders')";
            let err = error(&rt, unknown_connector).await;
            assert!(
                err.contains("Dataset 'orders' reads from 'postgress', which is not a data connector. Did you mean 'postgres'?"),
                "{err}"
            );

            let unwritable = format!(
                "CREATE TABLE orders WITH (from = 'file:{}', file_format = 'csv', access = 'read_write')",
                fixture.orders_csv.display()
            );
            let err = error(&rt, &unwritable).await;
            assert!(
                err.contains("Dataset 'orders' is writable, but its source 'file' cannot be written to."),
                "{err}"
            );

            let missing = format!(
                "CREATE TABLE orders WITH (from = 'file:{}', file_format = 'csv')",
                fixture.orders_csv.with_file_name("missing.csv").display()
            );
            let err = error(&rt, &missing).await;
            assert!(err.contains("Dataset 'orders' cannot"), "{err}");

            drop(rt);
            let rt = start(&fixture).await;
            assert!(
                error(&rt, "SELECT * FROM orders").await.contains("not found"),
                "nothing refused was kept"
            );
        })
        .await;
}

/// The options of a Cayenne file acceleration in `dir` that, holding data, is
/// not refreshed for an hour: what it serves is what it holds.
fn file_acceleration(dir: &Path) -> String {
    format!(
        "\"acceleration.engine\" = 'cayenne', \"acceleration.mode\" = 'file', \
         \"acceleration.refresh_check_interval\" = '1h', \
         \"acceleration.params.cayenne_file_path\" = '{data}', \
         \"acceleration.params.cayenne_metadata_dir\" = '{metadata}'",
        data = dir.join("cayenne").display(),
        metadata = dir.join("cayenne_metadata").display(),
    )
}

/// Wait for `sql` to return `expected`; the last answer when it does not.
async fn eventually(rt: &Runtime, sql: &str, expected: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..150 {
        match run(rt, sql).await {
            Ok(batches) => {
                last = arrow::util::pretty::pretty_format_batches(&batches)
                    .map_or_else(|e| e.to_string(), |t| t.to_string());
                if last == expected {
                    return Ok(());
                }
            }
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(last)
}

/// A dataset defined again under a name serves its own source, never the
/// acceleration data an earlier definition of the name loaded: dropping,
/// replacing, or re-pointing a dataset deletes that data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redefined_dataset_never_serves_the_data_its_name_held_before() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;
            let acceleration_dir = fixture.instance_dir.with_file_name("accelerations");
            let acceleration = file_acceleration(&acceleration_dir);
            let orders = "+----+-------+--------+\n\
                          | id | name  | amount |\n\
                          +----+-------+--------+\n\
                          | 1  | alpha | 10.5   |\n\
                          | 2  | beta  | 20.25  |\n\
                          | 3  | gamma |        |\n\
                          +----+-------+--------+";
            let other = "+----+------+--------+\n\
                         | id | name | amount |\n\
                         +----+------+--------+\n\
                         | 7  | eta  | 1.0    |\n\
                         +----+------+--------+";
            let select = "SELECT * FROM orders ORDER BY id";
            let define = |verb: &str, csv: &Path| {
                format!(
                    "{verb} orders WITH (from = 'file:{}', file_format = 'csv', {acceleration})",
                    csv.display()
                )
            };

            message(&rt, &define("CREATE TABLE", &fixture.orders_csv)).await;
            eventually(&rt, select, orders)
                .await
                .unwrap_or_else(|last| {
                    panic!("the first definition loads its source; served:\n{last}")
                });

            // Dropped, then defined over another source.
            message(&rt, "DROP TABLE orders").await;
            message(&rt, &define("CREATE TABLE", &fixture.other_csv)).await;
            eventually(&rt, select, other)
                .await
                .unwrap_or_else(|last| panic!("after DROP and CREATE; served:\n{last}"));

            // Replaced by a definition over the first source.
            assert_eq!(
                message(&rt, &define("CREATE OR REPLACE TABLE", &fixture.orders_csv)).await,
                "Replaced dataset 'orders'."
            );
            eventually(&rt, select, orders)
                .await
                .unwrap_or_else(|last| panic!("after CREATE OR REPLACE; served:\n{last}"));

            // Pointed at the other source.
            message(
                &rt,
                &format!(
                    "ALTER TABLE orders SET (from = 'file:{}')",
                    fixture.other_csv.display()
                ),
            )
            .await;
            eventually(&rt, select, other).await.unwrap_or_else(|last| {
                panic!("after ALTER TABLE SET (from = ...); served:\n{last}")
            });

            // And it stays that way across a restart.
            drop(rt);
            let rt = start(&fixture).await;
            eventually(&rt, select, other)
                .await
                .unwrap_or_else(|last| panic!("after a restart; served:\n{last}"));
        })
        .await;
}

/// A dataset whose earlier acceleration data cannot be deleted is not served,
/// whoever declares its name, until a retry deletes that data: the Spicepod
/// that declares the name once the instance restarts does not bring it back on
/// the retries in between, which would load and unload it on each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_withheld_dataset_stays_unserved_until_its_data_is_deleted() {
    crate::utils::require_process_per_test(
        "It shuts a runtime down, which marks change data capture and Cayenne compaction shut down for the whole process",
    );
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;
            let duckdb_file = fixture.instance_dir.with_file_name("s1.duckdb");
            message(
                &rt,
                &format!(
                    "CREATE TABLE s1 WITH (from = 'file:{}', file_format = 'csv', \
                     \"acceleration.engine\" = 'duckdb', \"acceleration.mode\" = 'file', \
                     \"acceleration.params.duckdb_file\" = '{}')",
                    fixture.orders_csv.display(),
                    duckdb_file.display()
                ),
            )
            .await;
            let select = "SELECT id FROM s1 ORDER BY id";
            eventually(
                &rt,
                select,
                "+----+\n| id |\n+----+\n| 1  |\n| 2  |\n| 3  |\n+----+",
            )
            .await
            .unwrap_or_else(|last| panic!("s1 loads its source; served:\n{last}"));
            rt.shutdown().await;
            drop(rt);

            // The file the definition kept its data in is no DuckDB database
            // any more, so that data cannot be deleted, and the Spicepod now
            // declares s1 over the other source.
            std::fs::write(&duckdb_file, b"not a DuckDB database")
                .expect("overwrite the DuckDB file");
            let app = AppBuilder::new("sql_ddl")
                .with_dataset(csv_dataset(&fixture.orders_csv, "pod_orders"))
                .with_dataset(csv_dataset(&fixture.other_csv, "s1"))
                .build();
            let rt = start_with(&fixture, app).await;

            // Withheld across more than one retry, every 10 seconds: the time
            // the dataset stays unserved is what is under test.
            let until = tokio::time::Instant::now() + Duration::from_secs(25);
            while tokio::time::Instant::now() < until {
                let e = error(&rt, select).await;
                assert!(
                    e.contains("table 'spice.public.s1' not found"),
                    "s1 is withheld; got: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // Once the fault is gone, the next retry deletes the data, and the
            // Spicepod's s1 is served.
            std::fs::remove_file(&duckdb_file).expect("remove the file");
            eventually(&rt, select, "+----+\n| id |\n+----+\n| 7  |\n+----+")
                .await
                .unwrap_or_else(|last| {
                    panic!("once its earlier data is deleted, s1 is served; served:\n{last}")
                });
        })
        .await;
}

/// `CREATE TABLE` returns once the dataset is registered, so a query right
/// after it finds the table: an accelerated dataset answers with its rows, or,
/// while its acceleration loads, says so. It is never missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accelerated_dataset_is_registered_when_its_statement_returns() {
    test_request_context()
        .scope(async {
            let fixture = Fixture::new();
            let rt = start(&fixture).await;
            for round in 0..10 {
                let name = format!("accelerated_{round}");
                assert_eq!(
                    message(
                        &rt,
                        &format!(
                            "CREATE TABLE {name} WITH (from = 'file:{}', file_format = 'csv', \
                             \"acceleration.engine\" = 'cayenne', \"acceleration.mode\" = 'file')",
                            fixture.orders_csv.display()
                        )
                    )
                    .await,
                    format!("Created dataset '{name}'.")
                );
                match run(&rt, &format!("SELECT count(*) AS n FROM {name}")).await {
                    Ok(batches) => assert_eq!(
                        arrow::util::pretty::pretty_format_batches(&batches)
                            .expect("format batches")
                            .to_string(),
                        "+---+\n| n |\n+---+\n| 3 |\n+---+",
                        "{name}"
                    ),
                    Err(e) => assert!(
                        e.contains(&format!(
                            "Acceleration not ready; loading initial data for {name}"
                        )),
                        "{name}: {e}"
                    ),
                }
            }
        })
        .await;
}
