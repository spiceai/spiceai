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

//! Throwaway CH-benCH Debezium harness for comparing current Spice against
//! v1.11.x, where native PostgreSQL CDC is unavailable.
//!
//! In `--cdc-mode debezium`, testoperator starts a local Redpanda + Debezium
//! Connect pair (unless `CHBENCH_DEBEZIUM_CONNECT_URL` is provided), registers
//! one Debezium PostgreSQL connector per CH-benCH changes-mode table, rewrites
//! those Spice datasets from `postgres:<table>` to `debezium:<topic>`, and then
//! waits for the initial Debezium snapshot to catch up before starting OLTP.

use std::{
    collections::HashMap,
    net::{SocketAddr, TcpStream},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use chbench_driver::{ChBenchDriver, PostgresSourceConfig, schema::STALENESS_PROBE_TABLES};
use test_framework::{
    anyhow::{self, Context},
    app::App,
    spicepod::{acceleration::RefreshMode, param::Params},
};
use tokio::time::sleep;

use super::spice::SpiceClients;

const REDPANDA_CONTAINER: &str = "chbench-redpanda";
const DEBEZIUM_CONTAINER: &str = "chbench-debezium";
const DEFAULT_CONNECT_URL: &str = "http://127.0.0.1:8083";
const DEFAULT_KAFKA_BOOTSTRAP: &str = "127.0.0.1:9092";
const DEBEZIUM_KAFKA_BOOTSTRAP: &str = "127.0.0.1:29092";
const REDPANDA_IMAGE: &str = "redpandadata/redpanda:v24.1.13";
const DEBEZIUM_IMAGE: &str = "quay.io/debezium/connect:2.7";

/// A running Debezium/Kafka harness. If this instance owns Docker containers,
/// they are removed when the guard drops.
pub(super) struct DebeziumHarness {
    kafka_bootstrap_servers: String,
    topic_prefix: String,
    owns_containers: bool,
}

impl DebeziumHarness {
    pub(super) async fn start(source: &PostgresSourceConfig) -> anyhow::Result<Self> {
        let connect_url = std::env::var("CHBENCH_DEBEZIUM_CONNECT_URL")
            .unwrap_or_else(|_| DEFAULT_CONNECT_URL.to_string());
        let kafka_bootstrap_servers = std::env::var("CHBENCH_KAFKA_BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| DEFAULT_KAFKA_BOOTSTRAP.to_string());
        let topic_prefix = std::env::var("CHBENCH_DEBEZIUM_TOPIC_PREFIX").unwrap_or_else(|_| {
            // Unique topics avoid replaying old Kafka data when an operator points
            // at a persistent broker during repeated benchmark attempts.
            format!("chbench_{}", std::process::id())
        });

        let owns_containers = std::env::var_os("CHBENCH_DEBEZIUM_CONNECT_URL").is_none();
        if owns_containers {
            start_local_containers()?;
        }

        wait_for_connect(&connect_url).await?;
        delete_existing_connectors(&connect_url, &topic_prefix).await?;
        cleanup_postgres_artifacts(source).await?;
        register_connectors(&connect_url, source, &topic_prefix).await?;

        Ok(Self {
            kafka_bootstrap_servers,
            topic_prefix,
            owns_containers,
        })
    }

    #[must_use]
    pub(super) fn kafka_bootstrap_servers(&self) -> &str {
        &self.kafka_bootstrap_servers
    }

    /// Rewrite CH-benCH changes-mode datasets to consume Debezium topics and
    /// inject Kafka params. Static reference tables remain as their original
    /// full-refresh Postgres datasets.
    pub(super) fn rewrite_app(&self, app: &mut App) -> usize {
        let mut rewritten = 0;
        for dataset in &mut app.datasets {
            let table = dataset
                .name
                .rsplit_once('.')
                .map_or(dataset.name.as_str(), |(_, table)| table);
            if !STALENESS_PROBE_TABLES.contains(&table) {
                continue;
            }

            let Some(acceleration) = dataset.acceleration.as_ref() else {
                continue;
            };
            if acceleration.refresh_mode != Some(RefreshMode::Changes) {
                continue;
            }

            dataset.from = format!("debezium:{}.public.{table}", self.topic_prefix);
            dataset.params = Some(Params::from_string_map(HashMap::from([
                (
                    "kafka_bootstrap_servers".to_string(),
                    self.kafka_bootstrap_servers.clone(),
                ),
                (
                    "kafka_security_protocol".to_string(),
                    "PLAINTEXT".to_string(),
                ),
                ("batch_max_size".to_string(), "50000".to_string()),
                ("batch_max_duration".to_string(), "1s".to_string()),
            ])));
            rewritten += 1;
        }

        println!(
            "CH-benCH Debezium CDC: rewrote {rewritten} changes-mode dataset(s) to topic prefix '{}' (Kafka {})",
            self.topic_prefix, self.kafka_bootstrap_servers
        );
        rewritten
    }

    pub(super) async fn wait_for_initial_catchup(
        &self,
        driver: Arc<dyn ChBenchDriver>,
        spice: &SpiceClients,
        tables: &[String],
        max_wait: Duration,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let mut last_log = Instant::now() - Duration::from_secs(60);
        println!(
            "\nWaiting up to {}s for Debezium initial snapshot to catch up before OLTP...",
            max_wait.as_secs()
        );

        loop {
            let mut pending = Vec::new();
            for table in tables {
                let (src_ts, spice_ts, src_n, spice_n) = tokio::join!(
                    driver.max_bench_ts(table),
                    spice.max_bench_ts(table),
                    driver.row_count(table),
                    spice.count(table),
                );

                match (src_ts, spice_ts, src_n, spice_n) {
                    (Ok(src_ts), Ok(spice_ts), Ok(src_n), Ok(spice_n)) => {
                        if src_ts != spice_ts || src_n != spice_n {
                            pending.push(format!(
                                "{table}: src_count={src_n} spice_count={spice_n} src_ts={src_ts:?} spice_ts={spice_ts:?}"
                            ));
                        }
                    }
                    (src_ts, spice_ts, src_n, spice_n) => pending.push(format!(
                        "{table}: src_ts={src_ts:?} spice_ts={spice_ts:?} src_count={src_n:?} spice_count={spice_n:?}"
                    )),
                }
            }

            if pending.is_empty() {
                println!(
                    "Debezium initial snapshot caught up in {}ms for {} table(s)",
                    start.elapsed().as_millis(),
                    tables.len()
                );
                return Ok(());
            }

            if start.elapsed() >= max_wait {
                anyhow::bail!(
                    "Debezium initial snapshot did not catch up within {}s; pending: {}",
                    max_wait.as_secs(),
                    pending.join("; ")
                );
            }

            if last_log.elapsed() >= Duration::from_secs(15) {
                println!(
                    "Debezium bootstrap still catching up ({}ms elapsed): {}",
                    start.elapsed().as_millis(),
                    pending.join("; ")
                );
                last_log = Instant::now();
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

impl Drop for DebeziumHarness {
    fn drop(&mut self) {
        if !self.owns_containers {
            return;
        }
        let _ = Command::new("docker")
            .args(["stop", "-t", "30", DEBEZIUM_CONTAINER])
            .status();
        let _ = Command::new("docker")
            .args(["rm", "-f", DEBEZIUM_CONTAINER, REDPANDA_CONTAINER])
            .status();
    }
}

fn start_local_containers() -> anyhow::Result<()> {
    // Fixed names + fixed host ports are intentional for this throwaway benchmark
    // harness: a stale previous attempt should be replaced, not reused.
    let _ = Command::new("docker")
        .args(["rm", "-f", DEBEZIUM_CONTAINER, REDPANDA_CONTAINER])
        .status();

    raise_aio_limit_best_effort();

    run_docker(&[
        "run",
        "-d",
        "--name",
        REDPANDA_CONTAINER,
        "--network",
        "host",
        REDPANDA_IMAGE,
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--overprovisioned",
        "--smp",
        "2",
        "--memory",
        "2G",
        "--kafka-addr",
        "INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092",
        "--advertise-kafka-addr",
        "INTERNAL://127.0.0.1:29092,EXTERNAL://127.0.0.1:9092",
    ])?;

    wait_for_tcp(
        DEBEZIUM_KAFKA_BOOTSTRAP,
        REDPANDA_CONTAINER,
        Duration::from_secs(120),
    )?;

    run_docker(&[
        "run",
        "-d",
        "--name",
        DEBEZIUM_CONTAINER,
        "--network",
        "host",
        "-e",
        &format!("BOOTSTRAP_SERVERS={DEBEZIUM_KAFKA_BOOTSTRAP}"),
        "-e",
        "GROUP_ID=chbench-debezium",
        "-e",
        "REST_ADVERTISED_HOST_NAME=127.0.0.1",
        "-e",
        "CONFIG_STORAGE_TOPIC=chbench_debezium_config",
        "-e",
        "OFFSET_STORAGE_TOPIC=chbench_debezium_offsets",
        "-e",
        "STATUS_STORAGE_TOPIC=chbench_debezium_status",
        "-e",
        "CONFIG_STORAGE_REPLICATION_FACTOR=1",
        "-e",
        "OFFSET_STORAGE_REPLICATION_FACTOR=1",
        "-e",
        "STATUS_STORAGE_REPLICATION_FACTOR=1",
        DEBEZIUM_IMAGE,
    ])?;

    println!(
        "Started local CH-benCH Debezium stack: {REDPANDA_CONTAINER} (external {DEFAULT_KAFKA_BOOTSTRAP}, internal {DEBEZIUM_KAFKA_BOOTSTRAP}), {DEBEZIUM_CONTAINER} ({DEFAULT_CONNECT_URL})"
    );
    Ok(())
}

fn raise_aio_limit_best_effort() {
    // Redpanda/Seastar requests AIO capacity proportional to logical CPUs. The
    // xlarge runner can expose many CPUs while the default fs.aio-max-nr is too
    // low, causing Redpanda to exit before Kafka binds. This is best-effort:
    // GitHub/self-hosted Linux runners normally allow passwordless sudo, but if
    // not, the later Docker diagnostics will show the Redpanda error clearly.
    run_diagnostic_command(
        "bash",
        &["-lc", "sudo sysctl -w fs.aio-max-nr=10485760 || true"],
    );
}

fn wait_for_tcp(addr: &str, container_name: &str, timeout: Duration) -> anyhow::Result<()> {
    let addr: SocketAddr = addr.parse()?;
    let started = Instant::now();
    while started.elapsed() < timeout {
        if TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok() {
            return Ok(());
        }
        if !container_running(container_name) {
            dump_docker_diagnostics();
            anyhow::bail!("container '{container_name}' exited before {addr} became reachable");
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    dump_docker_diagnostics();
    anyhow::bail!(
        "Timed out after {}s waiting for {container_name} to listen on {addr}",
        timeout.as_secs()
    )
}

fn run_docker(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .context("failed to execute docker; is Docker installed/running?")?;
    if !output.status.success() {
        anyhow::bail!(
            "docker {} failed (status={}):\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(())
}

async fn wait_for_connect(connect_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let url = format!("{connect_url}/connectors");
    let started = Instant::now();
    let timeout = Duration::from_secs(600);
    let mut last_diagnostics = Instant::now() - Duration::from_secs(60);

    loop {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => eprintln!(
                "Debezium Connect not ready yet: {} returned {}",
                url,
                resp.status()
            ),
            Err(e) => eprintln!("Debezium Connect not ready yet: {e}"),
        }

        if !container_running(DEBEZIUM_CONTAINER) || !container_running(REDPANDA_CONTAINER) {
            dump_docker_diagnostics();
            anyhow::bail!("Debezium stack container exited before Connect became ready");
        }

        if last_diagnostics.elapsed() >= Duration::from_secs(60) {
            dump_docker_diagnostics();
            last_diagnostics = Instant::now();
        }

        if started.elapsed() >= timeout {
            dump_docker_diagnostics();
            anyhow::bail!(
                "Timed out after {}s waiting for Debezium Connect at {connect_url}",
                timeout.as_secs()
            );
        }
        sleep(Duration::from_secs(2)).await;
    }
}

fn container_running(name: &str) -> bool {
    let Ok(output) = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
    else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true"
}

fn dump_docker_diagnostics() {
    eprintln!("===== docker ps -a (CH-benCH Debezium diagnostics) =====");
    run_diagnostic_command("docker", &["ps", "-a"]);
    for container in [REDPANDA_CONTAINER, DEBEZIUM_CONTAINER] {
        eprintln!("===== docker logs --tail 120 {container} =====");
        run_diagnostic_command("docker", &["logs", "--tail", "120", container]);
    }
}

fn run_diagnostic_command(command: &str, args: &[&str]) {
    match Command::new(command).args(args).output() {
        Ok(output) => {
            if !output.stdout.is_empty() {
                eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            }
            if !output.stderr.is_empty() {
                eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            }
        }
        Err(e) => eprintln!("failed to run {command} {}: {e}", args.join(" ")),
    }
}

async fn delete_existing_connectors(connect_url: &str, topic_prefix: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    for connector_name in STALENESS_PROBE_TABLES
        .iter()
        .map(|table| connector_name(topic_prefix, table))
        .chain(std::iter::once(connector_name(topic_prefix, "chbench")))
    {
        let resp = client
            .delete(format!("{connect_url}/connectors/{connector_name}"))
            .send()
            .await;
        match resp {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 404 => {}
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!(
                    "Debezium: deleting stale connector '{connector_name}' returned {status}: {body}"
                );
            }
            Err(e) => {
                eprintln!("Debezium: failed to delete stale connector '{connector_name}': {e}")
            }
        }
    }

    // Give Connect a moment to stop tasks and release/drop slots before we clean
    // up inactive leftovers from crashed runs.
    sleep(Duration::from_secs(3)).await;
    Ok(())
}

async fn cleanup_postgres_artifacts(source: &PostgresSourceConfig) -> anyhow::Result<()> {
    let (client, connection) =
        tokio_postgres::connect(&source.connection_string(), tokio_postgres::NoTls)
            .await
            .context("connect to PostgreSQL to clean Debezium replication artifacts")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Debezium cleanup PostgreSQL connection error: {e}");
        }
    });

    let sql = r"
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT slot_name FROM pg_replication_slots
    WHERE slot_name LIKE 'chbench\_%' ESCAPE '\' AND NOT active
  LOOP
    RAISE NOTICE 'dropping Debezium replication slot %', r.slot_name;
    PERFORM pg_drop_replication_slot(r.slot_name);
  END LOOP;

  FOR r IN
    SELECT pubname FROM pg_publication
    WHERE pubname LIKE 'chbench\_%' ESCAPE '\'
  LOOP
    RAISE NOTICE 'dropping Debezium publication %', r.pubname;
    EXECUTE format('DROP PUBLICATION IF EXISTS %I', r.pubname);
  END LOOP;
END $$;";

    client
        .batch_execute(sql)
        .await
        .context("clean Debezium replication artifacts")?;
    Ok(())
}

async fn register_connectors(
    connect_url: &str,
    source: &PostgresSourceConfig,
    topic_prefix: &str,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let debezium_pg_host =
        std::env::var("CHBENCH_DEBEZIUM_PG_HOST").unwrap_or_else(|_| source.host.clone());

    // Use one PostgreSQL connector per changes-mode CH-benCH table. A single
    // connector snapshots tables serially; at SF100 it can spend tens of
    // minutes on `order_line` before it even creates later topics (`oorder`,
    // `stock`, `warehouse`), so v1.11 spiced never finishes dataset
    // registration. Per-table connectors use independent slots/publications and
    // snapshot in parallel; topics remain {topic_prefix}.public.{table}, so the
    // Spice Debezium datasets do not need to change.
    for table in STALENESS_PROBE_TABLES {
        register_connector(
            connect_url,
            &client,
            source,
            &debezium_pg_host,
            topic_prefix,
            table,
        )
        .await?;
    }

    // Creating connectors causes Connect group rebalances. Wait for all of them
    // after registration, not one-by-one while later registrations can restart
    // earlier tasks.
    for table in STALENESS_PROBE_TABLES {
        wait_for_connector_running(connect_url, &client, &connector_name(topic_prefix, table))
            .await?;
    }

    Ok(())
}

async fn register_connector(
    connect_url: &str,
    client: &reqwest::Client,
    source: &PostgresSourceConfig,
    debezium_pg_host: &str,
    topic_prefix: &str,
    table: &str,
) -> anyhow::Result<()> {
    let connector_name = connector_name(topic_prefix, table);
    let table_include_list = format!("public.{table}");
    let slot_name = pg_identifier(&format!("{topic_prefix}_{table}_slot"));
    let publication_name = pg_identifier(&format!("{topic_prefix}_{table}_pub"));

    let body = serde_json::json!({
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": debezium_pg_host,
            "database.port": source.port.to_string(),
            "database.user": source.user,
            "database.password": source.pass,
            "database.dbname": source.db,
            "topic.prefix": topic_prefix,
            "table.include.list": table_include_list,
            "plugin.name": "pgoutput",
            "slot.name": slot_name,
            "slot.drop.on.stop": "true",
            "publication.name": publication_name,
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": "initial",
            "snapshot.fetch.size": "50000",
            "tasks.max": "1",
            "heartbeat.interval.ms": "10000",
            "include.schema.changes": "false",
            "tombstones.on.delete": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true"
        }
    });

    let resp = client
        .post(format!("{connect_url}/connectors"))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST Debezium connector '{connector_name}'"))?;
    let status = resp.status();
    let response_body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "Debezium connector '{connector_name}' registration failed: {status} {response_body}"
        );
    }

    println!(
        "Debezium: registered connector '{connector_name}' for {table_include_list} -> {topic_prefix}.public.{table}"
    );
    Ok(())
}

async fn wait_for_connector_running(
    connect_url: &str,
    client: &reqwest::Client,
    connector_name: &str,
) -> anyhow::Result<()> {
    let status_url = format!("{connect_url}/connectors/{connector_name}/status");
    let started = Instant::now();
    let timeout = Duration::from_secs(600);
    let mut last_log = Instant::now() - Duration::from_secs(60);

    loop {
        if started.elapsed() > timeout {
            anyhow::bail!(
                "Timed out after {}s waiting for Debezium connector '{connector_name}' to run",
                timeout.as_secs()
            );
        }

        match client.get(&status_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
                let connector_state = body
                    .get("connector")
                    .and_then(|c| c.get("state"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("UNKNOWN");
                let task_states: Vec<&str> = body
                    .get("tasks")
                    .and_then(|t| t.as_array())
                    .map(|tasks| {
                        tasks
                            .iter()
                            .filter_map(|t| t.get("state").and_then(|s| s.as_str()))
                            .collect()
                    })
                    .unwrap_or_default();

                if !task_states.is_empty() && task_states.iter().all(|&s| s == "RUNNING") {
                    println!("Debezium: connector '{connector_name}' tasks RUNNING");
                    return Ok(());
                }
                if connector_state == "FAILED" || task_states.contains(&"FAILED") {
                    anyhow::bail!(
                        "Debezium connector '{connector_name}' entered FAILED state: {body}"
                    );
                }
                if last_log.elapsed() >= Duration::from_secs(15) {
                    eprintln!(
                        "Debezium: waiting for connector '{connector_name}' to run: connector={connector_state}, tasks={task_states:?}"
                    );
                    last_log = Instant::now();
                }
            }
            Ok(resp) => eprintln!(
                "Debezium: status for '{connector_name}' returned {}, retrying",
                resp.status()
            ),
            Err(e) => eprintln!("Debezium: status for '{connector_name}' failed ({e}), retrying"),
        }

        sleep(Duration::from_secs(2)).await;
    }
}

fn connector_name(topic_prefix: &str, table: &str) -> String {
    format!("{}-{table}", pg_identifier(topic_prefix).replace('_', "-"))
}

fn pg_identifier(raw: &str) -> String {
    let mut ident = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if ident.len() > 55 {
        ident.truncate(55);
    }
    ident.trim_matches('_').to_string()
}
