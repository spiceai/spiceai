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

use std::{collections::HashMap, time::Duration};

use bollard::secret::HealthConfig;
use mysql_async::prelude::Queryable;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use tracing::instrument;

use test_framework::source_versions::{Source, source_image};

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub fn make_mysql_dataset(path: &str, name: &str, port: u16, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("mysql:{path}"), name.to_string());
    let params = HashMap::from([
        ("mysql_host".to_string(), "localhost".to_string()),
        ("mysql_tcp_port".to_string(), port.to_string()),
        ("mysql_user".to_string(), "root".to_string()),
        ("mysql_pass".to_string(), "integration-test-pw".to_string()),
        ("mysql_db".to_string(), "mysqldb".to_string()),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

pub const MYSQL_ROOT_PASSWORD: &str = "integration-test-pw";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-mysql";
const MYSQL_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);
const MYSQL_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_mins(1);

#[instrument]
pub async fn start_mysql_docker_container() -> Result<RunningContainer, anyhow::Error> {
    start_mysql_docker_container_with_image(&source_image(Source::MySql)?).await
}

/// How many times [`start_mysql_docker_container_retrying_startup`] tries to
/// bring the container up before reporting the last failure.
#[cfg(not(target_os = "windows"))]
const MYSQL_STARTUP_ATTEMPTS: u32 = 3;

/// Start a `MySQL` container, retrying only the container startup.
///
/// For tests that nextest runs with `retries = 0` (see `.config/nextest.toml`).
/// Nothing after the container is up is repeated, so a lost update cannot be
/// retried into a pass. Each attempt uses a new container name and Docker-owned
/// host port; a failed attempt drops its guard to remove only its container ID.
#[cfg(not(target_os = "windows"))]
pub async fn start_mysql_docker_container_retrying_startup()
-> Result<RunningContainer, anyhow::Error> {
    let mut attempt = 1;
    loop {
        match start_mysql_docker_container().await {
            Ok(container) => return Ok(container),
            Err(e) if attempt < MYSQL_STARTUP_ATTEMPTS => {
                tracing::warn!(
                    "MySQL container failed to start (attempt {attempt}/{MYSQL_STARTUP_ATTEMPTS}), retrying: {e:#}"
                );
                attempt += 1;
            }
            // The cause goes into the message rather than an anyhow context:
            // callers format this with `{e}`, which prints only the outermost
            // context and would hide why the last attempt failed.
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "MySQL container failed to start on all {MYSQL_STARTUP_ATTEMPTS} attempts, the last with: {e:#}"
                ));
            }
        }
    }
}

/// Start a `MySQL` container using a specific image tag. Used by the
/// version matrix to exercise both the `SHOW BINARY LOG STATUS` path (8.2+) and
/// the `SHOW MASTER STATUS` fallback (8.0).
#[instrument]
pub async fn start_mysql_docker_container_with_image(
    image: &str,
) -> Result<RunningContainer, anyhow::Error> {
    start_mysql_container_inner(image, &[], false).await
}

/// Start a `MySQL` container whose health check stops once the server is healthy. Each check
/// opens a connection, which takes a connection id, so a test that arranges which ids its own
/// connections get needs the checks to stop. Needs Docker 25 or later for `start_interval`.
#[instrument]
pub async fn start_quiet_mysql_docker_container() -> Result<RunningContainer, anyhow::Error> {
    start_mysql_container_inner(&source_image(Source::MySql)?, &[], true).await
}

/// Start a single-node `MySQL` container with GTIDs fully enabled
/// (`gtid_mode = ON`, `enforce_gtid_consistency = ON`). Used by the GTID
/// auto-positioning resume test — the source must issue GTIDs for the connector
/// to persist a failover-safe checkpoint.
#[instrument]
pub async fn start_mysql_gtid_docker_container() -> Result<RunningContainer, anyhow::Error> {
    start_mysql_container_inner(
        &source_image(Source::MySql)?,
        &["--gtid-mode=ON", "--enforce-gtid-consistency=ON"],
        false,
    )
    .await
}

/// Shared container startup. `mysqld_args` are appended to the server command
/// (the `MySQL` entrypoint prepends `mysqld` when the first arg starts with
/// `-`), so they configure the server itself (e.g. GTID mode).
async fn start_mysql_container_inner(
    image: &str,
    mysqld_args: &[&str],
    quiet_once_healthy: bool,
) -> Result<RunningContainer, anyhow::Error> {
    let mut builder = ContainerRunnerBuilder::new(MYSQL_DOCKER_CONTAINER)
        .image(image.to_string())
        .publish_port(3306)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "mysqldb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "mysqladmin ping -h127.0.0.1 -uroot --password={MYSQL_ROOT_PASSWORD} --silent"
                ),
            ]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(120),
            start_period: Some(30_000_000_000), // 30s
            start_interval: None,
        });
    if quiet_once_healthy {
        // Checks every second until the first passes within the start period, then hourly.
        builder = builder.healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "mysqladmin ping -h127.0.0.1 -uroot --password={MYSQL_ROOT_PASSWORD} --silent"
                ),
            ]),
            interval: Some(3_600_000_000_000), // 1h
            timeout: Some(5_000_000_000),      // 5s
            retries: Some(120),
            start_period: Some(180_000_000_000), // 3m
            start_interval: Some(1_000_000_000), // 1s
        });
    }
    if !mysqld_args.is_empty() {
        builder = builder.command(mysqld_args.iter().copied());
    }
    let running_container = builder
        .build()?
        .run(Some(MYSQL_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_mysql_host_port(running_container.host_port(3306)?).await?;

    Ok(running_container)
}

async fn wait_for_mysql_host_port(port: u16) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;
    let pool = get_mysql_conn(port)?;

    while start_time.elapsed() <= MYSQL_HOST_PORT_READY_TIMEOUT {
        match pool.get_conn().await {
            Ok(mut conn) => match conn.query_drop("SELECT 1").await {
                Ok(()) => {
                    drop(conn);
                    pool.disconnect().await?;
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                }
            },
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let last_error = last_error.unwrap_or_else(|| "none".to_string());
    pool.disconnect().await.map_err(|error| {
        anyhow::anyhow!(
            "MySQL container started but host port {port} was not ready within {}s. Last error: {last_error}; failed to disconnect readiness pool: {error}",
            MYSQL_HOST_PORT_READY_TIMEOUT.as_secs()
        )
    })?;

    Err(anyhow::anyhow!(
        "MySQL container started but host port {port} was not ready within {}s. Last error: {}",
        MYSQL_HOST_PORT_READY_TIMEOUT.as_secs(),
        last_error
    ))
}

#[instrument]
pub fn get_mysql_conn(port: u16) -> Result<mysql_async::Pool, anyhow::Error> {
    let url = format!("mysql://root:{MYSQL_ROOT_PASSWORD}@localhost:{port}/mysqldb");
    let opts_builder =
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(url.as_str())?);
    let opts = mysql_async::Opts::from(opts_builder);

    Ok(mysql_async::Pool::new(opts))
}

/// The `net_write_timeout` in force on another session, read back from the
/// server rather than inferred from the statement Spice sent.
///
/// `Ok(None)` when `performance_schema` cannot answer — the instrumentation is
/// on by default but can be built out or turned off, and a suite that fails for
/// that reason would be reporting on the image rather than on the runtime.
pub async fn session_net_write_timeout(
    pool: &mysql_async::Pool,
    processlist_id: u64,
) -> Result<Option<u64>, anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    let sql = format!(
        "SELECT v.VARIABLE_VALUE FROM performance_schema.variables_by_thread v \
         JOIN performance_schema.threads t ON t.THREAD_ID = v.THREAD_ID \
         WHERE t.PROCESSLIST_ID = {processlist_id} \
           AND v.VARIABLE_NAME = 'net_write_timeout'"
    );
    let value: Option<String> = match conn.query_first(sql.as_str()).await {
        Ok(value) => value,
        Err(e) => {
            // Said out loud: a silent `None` here would turn a caller's assertion
            // into one that can never fail.
            eprintln!("performance_schema could not answer `{sql}`: {e}");
            return Ok(None);
        }
    };
    // No row is "cannot answer" — the session is not instrumented. A row whose
    // value is not a number is a different thing entirely, and folding it into
    // the same `None` would let a caller pass without ever reading the session
    // it exists to read.
    let Some(value) = value else {
        return Ok(None);
    };
    let seconds = value.parse::<u64>().map_err(|e| {
        anyhow::anyhow!(
            "performance_schema reported net_write_timeout = `{value}`, which is not a number: {e}"
        )
    })?;
    Ok(Some(seconds))
}
