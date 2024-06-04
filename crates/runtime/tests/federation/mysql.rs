/*
Copyright 2024 The Spice.ai OSS Authors

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
//! Runs federation integration tests for `MySQL`.
//!
//! Expects a Docker daemon to be running.
use std::{collections::HashMap, sync::Arc};

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

use super::*;
use app::AppBuilder;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use bollard::secret::HealthConfig;
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::Runtime;
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use tracing::instrument;

fn make_mysql_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("mysql:{path}"), name.to_string());
    let params = HashMap::from([
        ("mysql_host".to_string(), "localhost".to_string()),
        ("mysql_tcp_port".to_string(), "13306".to_string()),
        ("mysql_user".to_string(), "root".to_string()),
        (
            "mysql_pass".to_string(),
            "federation-integration-test-pw".to_string(),
        ),
        ("mysql_db".to_string(), "federation".to_string()),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

const MYSQL_ROOT_PASSWORD: &str = "federation-integration-test-pw";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-federation-mysql";

#[instrument]
async fn start_mysql_docker_container() -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(MYSQL_DOCKER_CONTAINER)
        .image("mysql:latest")
        .add_port_binding(3306, 13306)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "federation")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!("mysqladmin ping --password={MYSQL_ROOT_PASSWORD}"),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

#[instrument]
fn get_mysql_conn() -> Result<mysql_async::Pool, anyhow::Error> {
    let opts_builder = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(
        "mysql://root:federation-integration-test-pw@localhost:13306/federation",
    )?);
    let opts = mysql_async::Opts::from(opts_builder);

    Ok(mysql_async::Pool::new(opts))
}

#[instrument]
async fn init_mysql_db() -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn()?;
    let mut conn = pool.get_conn().await?;

    tracing::debug!("DROP TABLE IF EXISTS lineitem");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS lineitem", Params::Empty)
        .await?;

    tracing::debug!("Downloading TPCH lineitem...");
    let tpch_lineitem = crate::get_tpch_lineitem().await?;

    let tpch_lineitem_schema = Arc::clone(&tpch_lineitem[0].schema());

    let create_table_stmt = CreateTableBuilder::new(tpch_lineitem_schema, "lineitem").build_mysql();
    tracing::debug!("CREATE TABLE lineitem...");
    let _: Vec<Row> = conn.exec(create_table_stmt, Params::Empty).await?;

    tracing::debug!("INSERT INTO lineitem...");
    let insert_stmt = InsertBuilder::new("lineitem", tpch_lineitem).build_mysql()?;
    let _: Vec<Row> = conn.exec(insert_stmt, Params::Empty).await?;
    tracing::debug!("MySQL initialized!");

    Ok(())
}

#[tokio::test]
async fn mysql_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, Vec<&'a str>, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    let running_container = start_mysql_docker_container().await.map_err(|e| {
        tracing::error!("start_mysql_docker_container: {e}");
        e.to_string()
    })?;
    tracing::debug!("Container started");
    init_mysql_db().await.map_err(|e| {
        tracing::error!("init_mysql_db: {e}");
        e.to_string()
    })?;
    let app = AppBuilder::new("mysql_federation_push_down")
        .with_dataset(make_mysql_dataset("lineitem", "line"))
        .build();

    let rt = Runtime::new(Some(app), Arc::new(vec![])).await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_datasets_and_views() => {}
    }

    let mut rt = crate::modify_runtime_datafusion_options(rt);

    let queries: QueryTests = vec![
        (
            "SELECT * FROM line LIMIT 10",
            vec![
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |",
                "|               |   Projection: lineitem.l_orderkey, lineitem.l_partkey, lineitem.l_suppkey, lineitem.l_linenumber, lineitem.l_quantity, lineitem.l_extendedprice, lineitem.l_discount, lineitem.l_tax, lineitem.l_returnflag, lineitem.l_linestatus, lineitem.l_shipdate, lineitem.l_commitdate, lineitem.l_receiptdate, lineitem.l_shipinstruct, lineitem.l_shipmode, lineitem.l_comment                                                                                                                                                                                             |",
                "|               |     TableScan: lineitem                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |",
                "|               |     VirtualExecutionPlan name=mysql compute_context=host=localhost,port=13306,db=federation,user=root sql=SELECT \"lineitem\".\"l_orderkey\", \"lineitem\".\"l_partkey\", \"lineitem\".\"l_suppkey\", \"lineitem\".\"l_linenumber\", \"lineitem\".\"l_quantity\", \"lineitem\".\"l_extendedprice\", \"lineitem\".\"l_discount\", \"lineitem\".\"l_tax\", \"lineitem\".\"l_returnflag\", \"lineitem\".\"l_linestatus\", \"lineitem\".\"l_shipdate\", \"lineitem\".\"l_commitdate\", \"lineitem\".\"l_receiptdate\", \"lineitem\".\"l_shipinstruct\", \"lineitem\".\"l_shipmode\", \"lineitem\".\"l_comment\" FROM \"lineitem\" LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(|result_batches| {
                for batch in result_batches {
                    assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                    assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                }
            })),
        ),
        (
            "SELECT * FROM line ORDER BY line.l_orderkey DESC LIMIT 10",
            vec![
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |",
                "|               |  Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |",
                "|               |   Sort: lineitem.l_orderkey DESC NULLS FIRST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |",
                "|               |     Projection: lineitem.l_orderkey, lineitem.l_partkey, lineitem.l_suppkey, lineitem.l_linenumber, lineitem.l_quantity, lineitem.l_extendedprice, lineitem.l_discount, lineitem.l_tax, lineitem.l_returnflag, lineitem.l_linestatus, lineitem.l_shipdate, lineitem.l_commitdate, lineitem.l_receiptdate, lineitem.l_shipinstruct, lineitem.l_shipmode, lineitem.l_comment                                                                                                                                                                                                                                             |",
                "|               |       TableScan: lineitem                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |",
                "| physical_plan | SchemaCastScanExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |",
                "|               |   RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |",
                "|               |     VirtualExecutionPlan name=mysql compute_context=host=localhost,port=13306,db=federation,user=root sql=SELECT \"lineitem\".\"l_orderkey\", \"lineitem\".\"l_partkey\", \"lineitem\".\"l_suppkey\", \"lineitem\".\"l_linenumber\", \"lineitem\".\"l_quantity\", \"lineitem\".\"l_extendedprice\", \"lineitem\".\"l_discount\", \"lineitem\".\"l_tax\", \"lineitem\".\"l_returnflag\", \"lineitem\".\"l_linestatus\", \"lineitem\".\"l_shipdate\", \"lineitem\".\"l_commitdate\", \"lineitem\".\"l_receiptdate\", \"lineitem\".\"l_shipinstruct\", \"lineitem\".\"l_shipmode\", \"lineitem\".\"l_comment\" FROM \"lineitem\" ORDER BY \"lineitem\".\"l_orderkey\" DESC NULLS FIRST LIMIT 10 |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            Some(Box::new(|result_batches| {
                for batch in result_batches {
                    assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                    assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                }
            })),
        ),
    ];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result).await?;
    }

    running_container.remove().await.map_err(|e| {
        tracing::error!("running_container.remove: {e}");
        e.to_string()
    })?;

    Ok(())
}
