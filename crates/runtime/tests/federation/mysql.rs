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

use super::*;
use app::AppBuilder;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use bollard::{
    container::{Config, CreateContainerOptions, StartContainerOptions},
    image::CreateImageOptions,
    models::HostConfig,
    secret::PortBinding,
    Docker,
};
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::{datafusion::DataFusion, Runtime};
use spicepod::component::dataset::Dataset;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::instrument;

fn make_mysql_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("mysql:{path}"), name.to_string())
}

const MYSQL_ROOT_PASSWORD: &str = "federation-integration-test-pw";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-federation-mysql";

#[instrument]
async fn start_mysql_docker_container() -> Result<(), anyhow::Error> {
    let docker =
        Docker::connect_with_local_defaults().expect("a running Docker daemon is required");
    let containers = docker.list_containers::<&str>(None).await?;
    for container in containers {
        let Some(names) = container.names else {
            continue;
        };
        if names.iter().any(|n| n == MYSQL_DOCKER_CONTAINER) {
            return Ok(());
        }
    }

    let options = Some(CreateImageOptions::<&str> {
        from_image: "mysql:latest",
        ..Default::default()
    });

    let mut pulling_stream = docker.create_image(options, None, None);
    while let Some(event) = pulling_stream.next().await {
        tracing::info!("Pulling image: {:?}", event?);
    }

    let options = CreateContainerOptions {
        name: MYSQL_DOCKER_CONTAINER,
        platform: None,
    };

    let host_config = Some(HostConfig {
        port_bindings: Some(HashMap::from([(
            "3306/tcp".to_string(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some("13306/tcp".to_string()),
            }]),
        )])),
        ..Default::default()
    });

    let config = Config::<String> {
        image: Some("mysql:latest".to_string()),
        env: Some(vec![
            format!("MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASSWORD}"),
            format!("MYSQL_DATABASE=federation"),
        ]),
        host_config,
        ..Default::default()
    };

    let _ = docker.create_container(Some(options), config).await?;

    docker
        .start_container(
            MYSQL_DOCKER_CONTAINER,
            None::<StartContainerOptions<String>>,
        )
        .await?;

    Ok(())
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

    tracing::debug!("crate::get_tpch_lineitem().await?");
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
async fn list_containers() -> Result<(), anyhow::Error> {
    init_tracing(Some("integration=debug"));
    let _ = start_mysql_docker_container().await;
    tracing::debug!("Containers started");
    init_mysql_db().await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mysql_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, Vec<&'a str>, Option<Box<ValidateFn>>)>;
    init_tracing(None);
    let app = AppBuilder::new("mysql_federation_push_down")
        .with_dataset(make_mysql_dataset("foobar", "test"))
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));

    let mut rt = Runtime::new(Some(app), df).await;

    rt.load_secrets().await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_datasets() => {}
    }

    let queries: QueryTests = vec![
        (
            "SELECT MAX(number) as max_num FROM test",
            vec![
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                            |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Federated                                                                                                                                                                       |",
                "|               |  Projection: MAX(blocks.number) AS max_num                                                                                                                                      |",
                "|               |   Aggregate: groupBy=[[]], aggr=[[MAX(blocks.number)]]                                                                                                                          |",
                "|               |     SubqueryAlias: blocks                                                                                                                                                       |",
                "|               |       TableScan: eth.recent_blocks                                                                                                                                              |",
                "| physical_plan | VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username= sql=SELECT MAX(\"blocks\".\"number\") AS \"max_num\" FROM \"eth\".\"recent_blocks\" AS \"blocks\" |",
                "|               |                                                                                                                                                                                 |",
                "+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
            None,
        ),
    ];

    for (query, expected_plan, validate_result) in queries {
        run_query_and_check_results(&mut rt, query, &expected_plan, validate_result).await?;
    }

    Ok(())
}
