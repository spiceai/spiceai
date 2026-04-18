/*
Copyright 2026 The Spice.ai OSS Authors

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

//! End-to-end integration test for the Postgres logical-replication path,
//! driven through the full Spice Runtime (Spicepod datasets + `DuckDB`
//! accelerator) using a TPC-H-shaped schema.
//!
//! What this test validates that the pure-library test at `replication.rs`
//! does not:
//!   - A real `AcceleratedTable` pipeline with `refresh_mode: changes`.
//!   - Multiple datasets in one Spice instance, each with its own slot.
//!   - Actual SQL queries through `Runtime::datafusion()` (not raw
//!     `ChangeBatch` assertions) — so we prove end users can query the
//!     replicated data.
//!   - A range of Postgres data types (`int4`, `int8`, `text`, `float8`,
//!     `date`) surviving INSERT/UPDATE/DELETE across the WAL path.
//!
//! Uses a minimal hand-seeded TPC-H subset (nation, region, customer, orders)
//! so the test stays fast and self-contained — no dbgen / external fixtures.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tokio::time::sleep;
use tokio_postgres::NoTls;

use crate::postgres::common;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

/// How long to wait for the replication stream to apply a change before failing.
const CHANGE_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Schema + seed (minimal TPC-H subset).
// ---------------------------------------------------------------------------

const DDL_STATEMENTS: &[&str] = &[
    r"CREATE TABLE public.tpch_region (
        r_regionkey int4 PRIMARY KEY,
        r_name      text NOT NULL,
        r_comment   text
    )",
    r"CREATE TABLE public.tpch_nation (
        n_nationkey int4 PRIMARY KEY,
        n_name      text NOT NULL,
        n_regionkey int4 NOT NULL,
        n_comment   text
    )",
    r"CREATE TABLE public.tpch_customer (
        c_custkey     int8 PRIMARY KEY,
        c_name        text NOT NULL,
        c_nationkey   int4 NOT NULL,
        c_acctbal     float8 NOT NULL,
        c_mktsegment  text
    )",
    r"CREATE TABLE public.tpch_orders (
        o_orderkey    int8 PRIMARY KEY,
        o_custkey     int8 NOT NULL,
        o_orderstatus text NOT NULL,
        o_totalprice  float8 NOT NULL,
        o_orderdate   date NOT NULL
    )",
];

const SEED_STATEMENTS: &[&str] = &[
    // 3 regions
    "INSERT INTO public.tpch_region VALUES \
     (0, 'AFRICA',       'lar deposits. blithely final packages'),\
     (1, 'AMERICA',      'hs use ironic, even requests. s'),\
     (2, 'ASIA',         'ges. thinly even pinto beans ca')",
    // 5 nations
    "INSERT INTO public.tpch_nation VALUES \
     (0, 'ALGERIA', 0, NULL),\
     (1, 'ARGENTINA', 1, 'al foxes promise slyly'),\
     (2, 'BRAZIL',   1, 'y alongside of the pend'),\
     (3, 'CHINA',    2, 'c dependencies. furiously express'),\
     (4, 'JAPAN',    2, 'ously. final, express gifts')",
    // 8 customers (spread across nations, varying acctbal)
    "INSERT INTO public.tpch_customer VALUES \
     (1, 'Customer#000000001', 0,  711.56, 'BUILDING'),\
     (2, 'Customer#000000002', 1,  121.65, 'AUTOMOBILE'),\
     (3, 'Customer#000000003', 1, 7498.12, 'AUTOMOBILE'),\
     (4, 'Customer#000000004', 2, 2866.83, 'MACHINERY'),\
     (5, 'Customer#000000005', 2,  794.47, 'HOUSEHOLD'),\
     (6, 'Customer#000000006', 3, 7638.57, 'AUTOMOBILE'),\
     (7, 'Customer#000000007', 3, 9561.95, 'FURNITURE'),\
     (8, 'Customer#000000008', 4, 6819.74, 'BUILDING')",
    // 10 orders (date values exercise the Date32 path)
    "INSERT INTO public.tpch_orders VALUES \
     (1,  1, 'O', 172799.49, DATE '1996-01-02'),\
     (2,  2, 'O',  38426.09, DATE '1996-12-01'),\
     (3,  3, 'F', 205654.30, DATE '1993-10-14'),\
     (4,  4, 'O',  56000.91, DATE '1995-10-11'),\
     (5,  5, 'F', 105367.67, DATE '1994-07-30'),\
     (6,  6, 'F',  45523.10, DATE '1992-02-21'),\
     (7,  7, 'O', 271885.66, DATE '1996-01-10'),\
     (8,  8, 'O', 116363.16, DATE '1995-07-16'),\
     (9,  3, 'F',  99734.51, DATE '1993-10-14'),\
     (10, 7, 'F',  41613.00, DATE '1993-02-25')",
];

struct TpchDataset {
    dataset_name: &'static str,
    pg_table: &'static str,
    primary_key: &'static str,
    expected_initial_count: u64,
}

const TPCH_DATASETS: &[TpchDataset] = &[
    TpchDataset {
        dataset_name: "tpch_region",
        pg_table: "public.tpch_region",
        primary_key: "r_regionkey",
        expected_initial_count: 3,
    },
    TpchDataset {
        dataset_name: "tpch_nation",
        pg_table: "public.tpch_nation",
        primary_key: "n_nationkey",
        expected_initial_count: 5,
    },
    TpchDataset {
        dataset_name: "tpch_customer",
        pg_table: "public.tpch_customer",
        primary_key: "c_custkey",
        expected_initial_count: 8,
    },
    TpchDataset {
        dataset_name: "tpch_orders",
        pg_table: "public.tpch_orders",
        primary_key: "o_orderkey",
        expected_initial_count: 10,
    },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn connect(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    Ok(client)
}

async fn exec(client: &tokio_postgres::Client, sql: &str) -> Result<(), anyhow::Error> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(())
}

async fn setup_schema_and_seed(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let client = connect(port).await?;
    for ddl in DDL_STATEMENTS {
        exec(&client, ddl).await?;
    }
    for seed in SEED_STATEMENTS {
        exec(&client, seed).await?;
    }
    Ok(client)
}

fn make_dataset(ds: &TpchDataset, pg_params: &HashMap<String, String>) -> Dataset {
    let mut dataset = Dataset::new(
        format!("postgres:{}", ds.pg_table),
        ds.dataset_name.to_string(),
    );
    dataset.params = Some(Params::from_string_map(pg_params.clone()));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some(ds.primary_key.to_string()),
        on_conflict: vec![(ds.primary_key.to_string(), OnConflictBehavior::Upsert)]
            .into_iter()
            .collect(),
        ..Acceleration::default()
    });
    dataset
}

async fn run_query(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow!("query `{sql}` failed: {e}"))?;
    let data = result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow!("collect `{sql}` failed: {e}"))?;
    Ok(data)
}

/// Poll `SELECT count(*) FROM <dataset>` until the accelerator reports the
/// expected count, or time out. This is the primary back-pressure signal that
/// the replication stream has applied changes end-to-end.
async fn wait_for_row_count(
    rt: &Runtime,
    dataset_name: &str,
    expected: u64,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
    let sql = format!("SELECT count(*) AS c FROM {dataset_name}");
    loop {
        let batches = run_query(rt, &sql).await?;
        // Surface schema/type regressions as explicit test errors instead of
        // letting them turn into a 60-second timeout — they'd look like
        // "missing rows" otherwise.
        let batch = batches.first().ok_or_else(|| {
            anyhow!(
                "Query `{sql}` returned no record batches while waiting for \
                 {dataset_name} to reach {expected} rows"
            )
        })?;
        if batch.num_rows() == 0 {
            return Err(anyhow!(
                "Query `{sql}` returned an empty record batch while waiting for \
                 {dataset_name} to reach {expected} rows"
            ));
        }
        let column = batch.column(0);
        let count_i64 = column
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                anyhow!(
                    "Query `{sql}` returned unexpected count column type while \
                     waiting for {dataset_name}: expected Int64, got {}",
                    column.data_type()
                )
            })?
            .value(0);
        let count = u64::try_from(count_i64).map_err(|_| {
            anyhow!(
                "Query `{sql}` returned negative count {count_i64} while waiting \
                 for {dataset_name} to reach {expected} rows"
            )
        })?;
        if count == expected {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "Timed out waiting for {dataset_name} to reach {expected} rows; last saw {count}"
            ));
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_scalar_i64(rt: &Runtime, sql: &str, expected: i64) -> Result<(), anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let Some(batch) = batches.first() else {
        return Err(anyhow!("no rows from `{sql}`"));
    };
    let actual = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| anyhow!("non-Int64 result from `{sql}`"))?
        .value(0);
    if actual != expected {
        return Err(anyhow!(
            "unexpected result for `{sql}`: got {actual}, want {expected}"
        ));
    }
    Ok(())
}

async fn assert_scalar_f64_approx(
    rt: &Runtime,
    sql: &str,
    expected: f64,
    tolerance: f64,
) -> Result<(), anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let Some(batch) = batches.first() else {
        return Err(anyhow!("no rows from `{sql}`"));
    };
    let actual = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| anyhow!("non-Float64 result from `{sql}`"))?
        .value(0);
    if (actual - expected).abs() > tolerance {
        return Err(anyhow!(
            "unexpected result for `{sql}`: got {actual}, want {expected} ± {tolerance}"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tpch_postgres_replication_end_to_end() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,\
         data_components::postgres_replication=trace,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            // -------------------------------------------------------------
            // 1. Create schema + seed on the source.
            // -------------------------------------------------------------
            let source = setup_schema_and_seed(u16::try_from(port)?).await?;

            // -------------------------------------------------------------
            // 2. Build a Spice app with 4 replicated datasets.
            // -------------------------------------------------------------
            let pg_params: HashMap<String, String> = common::get_pg_params(port)
                .into_iter()
                .map(|(k, v)| (k, v.expose_secret().to_string()))
                .collect();

            let mut builder = AppBuilder::new("tpch_replication_integration");
            for ds in TPCH_DATASETS {
                builder = builder.with_dataset(make_dataset(ds, &pg_params));
            }
            let app = builder.build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            // Wait for initial load with a generous timeout — bootstrap of 4
            // tables plus slot setup takes a few seconds.
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(90)) => {
                    return Err(anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // -------------------------------------------------------------
            // 3. Bootstrap validation — every dataset should reach its
            //    expected count from the initial snapshot.
            // -------------------------------------------------------------
            for ds in TPCH_DATASETS {
                wait_for_row_count(&rt, ds.dataset_name, ds.expected_initial_count).await?;
            }

            // Snapshot a few aggregations to prove type fidelity across the
            // WAL → Arrow → DuckDB path.
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_customer WHERE c_mktsegment = 'AUTOMOBILE'",
                3,
            )
            .await?;
            assert_scalar_f64_approx(
                &rt,
                "SELECT sum(o_totalprice) FROM tpch_orders",
                1_153_367.89,
                0.01,
            )
            .await?;
            // Seeded orders with dates < 1995-01-01: ids 3, 5, 6, 9, 10 = 5 rows.
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_orders WHERE o_orderdate < DATE '1995-01-01'",
                5,
            )
            .await?;
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_nation WHERE n_regionkey = 1",
                2,
            )
            .await?;

            // -------------------------------------------------------------
            // 4. INSERT path — add rows via the source and assert they
            //    appear.
            // -------------------------------------------------------------
            exec(
                &source,
                "INSERT INTO public.tpch_customer VALUES \
                 (9,  'Customer#000000009', 4, 8324.07, 'FURNITURE'),\
                 (10, 'Customer#000000010', 0, 2753.54, 'HOUSEHOLD')",
            )
            .await?;
            wait_for_row_count(&rt, "tpch_customer", 10).await?;
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_customer WHERE c_custkey IN (9, 10)",
                2,
            )
            .await?;

            exec(
                &source,
                "INSERT INTO public.tpch_orders VALUES \
                 (11, 9,  'P', 55555.00, DATE '2024-03-15'),\
                 (12, 10, 'O', 12345.67, DATE '2024-03-20')",
            )
            .await?;
            wait_for_row_count(&rt, "tpch_orders", 12).await?;
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_orders WHERE o_orderdate >= DATE '2024-01-01'",
                2,
            )
            .await?;

            // -------------------------------------------------------------
            // 5. UPDATE path — change string, float, and date columns.
            //    (REPLICA IDENTITY DEFAULT gives us the PK in the old image.)
            // -------------------------------------------------------------
            exec(
                &source,
                "UPDATE public.tpch_customer SET c_acctbal = 9999.99, \
                 c_mktsegment = 'UPDATED' WHERE c_custkey = 1",
            )
            .await?;
            // Poll until the row reflects the update — upsert semantics keep
            // the row count at 10 but modify the row in place.
            let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
            loop {
                let batch = run_query(
                    &rt,
                    "SELECT c_mktsegment FROM tpch_customer WHERE c_custkey = 1",
                )
                .await?;
                let seg = batch
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                    })
                    .map(|a| a.value(0).to_string());
                if seg.as_deref() == Some("UPDATED") {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err(anyhow!(
                        "Timed out waiting for UPDATE on customer=1; saw segment={seg:?}"
                    ));
                }
                sleep(Duration::from_millis(250)).await;
            }
            assert_scalar_f64_approx(
                &rt,
                "SELECT c_acctbal FROM tpch_customer WHERE c_custkey = 1",
                9999.99,
                0.01,
            )
            .await?;

            exec(
                &source,
                "UPDATE public.tpch_orders SET o_totalprice = 1.00, \
                 o_orderdate = DATE '2024-01-01' WHERE o_orderkey = 5",
            )
            .await?;
            let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
            loop {
                let batch = run_query(
                    &rt,
                    "SELECT o_totalprice FROM tpch_orders WHERE o_orderkey = 5",
                )
                .await?;
                let price = batch
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                    })
                    .map(|a| a.value(0));
                if (price.unwrap_or_default() - 1.00).abs() < 0.01 {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err(anyhow!(
                        "Timed out waiting for UPDATE on order=5; saw price={price:?}"
                    ));
                }
                sleep(Duration::from_millis(250)).await;
            }

            // -------------------------------------------------------------
            // 6. DELETE path.
            // -------------------------------------------------------------
            exec(
                &source,
                "DELETE FROM public.tpch_orders WHERE o_orderkey IN (1, 2, 3)",
            )
            .await?;
            wait_for_row_count(&rt, "tpch_orders", 9).await?;
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_orders WHERE o_orderkey IN (1, 2, 3)",
                0,
            )
            .await?;

            // Delete from the smaller table too, to exercise different PK type (int4).
            exec(
                &source,
                "DELETE FROM public.tpch_nation WHERE n_nationkey = 4",
            )
            .await?;
            wait_for_row_count(&rt, "tpch_nation", 4).await?;

            // -------------------------------------------------------------
            // 7. JOIN across replicated tables — proves multiple datasets
            //    are consistently replicated at the same point in time.
            // -------------------------------------------------------------
            assert_scalar_i64(
                &rt,
                "SELECT count(*) FROM tpch_customer c \
                 JOIN tpch_nation n  ON c.c_nationkey = n.n_nationkey \
                 JOIN tpch_region r  ON n.n_regionkey = r.r_regionkey \
                 WHERE r.r_name = 'AMERICA'",
                // customers 2, 3, 4, 5 are in AMERICA (nations 1, 2).
                // After the UPDATE, customer 1 still has c_nationkey=0 (AFRICA),
                // so AMERICA count is still {2,3,4,5} = 4.
                4,
            )
            .await?;

            // Final pretty-printed snapshot of the small region table for
            // regression sensitivity on the full row shape. Using an Insta
            // named snapshot to match the rest of the runtime test suite —
            // run `INSTA_UPDATE=1 cargo nextest run ...` to refresh.
            let regions = run_query(
                &rt,
                "SELECT r_regionkey, r_name FROM tpch_region ORDER BY r_regionkey",
            )
            .await?;
            let pretty =
                pretty_format_batches(&regions).map_err(|e| anyhow!("format regions: {e}"))?;
            insta::assert_snapshot!("tpch_postgres_replication_regions", pretty);

            rt.shutdown().await;
            Ok(())
        })
        .await
}
