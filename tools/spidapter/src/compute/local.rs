// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use system_adapter_protocol::{DatasetConfig, EtlSinkType};
use tokio::process::{Child, Command as TokioCommand};
use uuid::Uuid;

use super::{
    LocalProcesses, LocalRunState, RunState, SetupConfig, generate_initial_spicepod,
    post_setup_sink_action, write_local_spicepod,
};
use crate::args::StdioArgs;

const LOCAL_BIND_HOST: &str = "0.0.0.0";
const LOCAL_CONNECT_HOST: &str = "127.0.0.1";
const SPIDAPTER_NUM_EXECUTORS_ENV: &str = "SPIDAPTER_NUM_EXECUTORS";
const MAX_LOCAL_EXECUTORS: usize = 16;
const LOCAL_EXECUTOR_REGISTRATION_METRIC_GRACE: Duration = Duration::from_secs(15);
const LOCAL_SPICE_BINARY: &str = "spice";

#[derive(Debug, Clone, Copy)]
struct LocalPorts {
    http: u16,
    flight: u16,
}

#[derive(Debug, Clone)]
struct ClusterPorts {
    scheduler_http: u16,
    scheduler_flight: u16,
    scheduler_node: u16,
    executor_ports: Vec<(u16, u16, u16)>,
}

#[derive(Debug, Clone)]
struct LocalPkiPaths {
    ca_cert: PathBuf,
    scheduler_cert: PathBuf,
    scheduler_key: PathBuf,
    executor_pki: Vec<(PathBuf, PathBuf)>,
}

pub(super) fn build_local_extra_envs(_setup_config: &SetupConfig) -> HashMap<String, String> {
    HashMap::new()
}

pub(super) async fn provision_local_single_node(
    run_id: Uuid,
    ready_wait: Duration,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    args: &StdioArgs,
) -> anyhow::Result<RunState> {
    let num_exec = num_executors()?;
    eprintln!("[stdio] local backend: provisioning cluster with {num_exec} executor(s)");
    let ports = allocate_local_ports(LOCAL_BIND_HOST, num_exec)?;

    let working_dir = create_local_working_dir(run_id).await?;
    let local_flight_api_key = (setup_config.sink_type == Some(EtlSinkType::Adbc))
        .then(|| format!("spidapter-local-{run_id}"));

    let spicepod_path = match async {
        let spicepod = generate_initial_spicepod(
            &run_id,
            setup_config,
            datasets,
            local_flight_api_key.as_deref(),
            args,
        )
        .await?;
        write_local_spicepod(&spicepod, &working_dir).await
    }
    .await
    {
        Ok(path) => path,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let spiced_args = standalone_spiced_args(LOCAL_BIND_HOST, ports, spicepod_path.as_path());
    let extra_envs = build_local_extra_envs(setup_config);
    let mut child = match spawn_local_spiced(
        &args.spiced_binary,
        &working_dir,
        &spiced_args,
        "spiced",
        &extra_envs,
    ) {
        Ok(child) => child,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let http_url = format!("http://{}:{}", LOCAL_CONNECT_HOST, ports.http);
    let sql_url = format!("{http_url}/v1/sql");

    if let Err(error) = wait_for_local_http_ready(&http_url, &mut child, ready_wait, "spiced").await
    {
        let _ = stop_child_process(&mut child, "spiced").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if let Err(error) = wait_for_local_sql_ready(
        &sql_url,
        &mut child,
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        let _ = stop_child_process(&mut child, "spiced").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if let Err(error) = post_setup_sink_action(
        setup_config,
        datasets,
        &sql_url,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        let _ = stop_child_process(&mut child, "spiced").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if let Err(error) = wait_for_runtime_ready(
        &http_url,
        &mut child,
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        let _ = stop_child_process(&mut child, "spiced").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    Ok(RunState::Local(Box::new(LocalRunState {
        processes: LocalProcesses::SingleNode { child },
        flight_url: format!("grpc://{}:{}", LOCAL_CONNECT_HOST, ports.flight),
        flight_api_key: local_flight_api_key,
        sql_url,
        working_dir,
        storage: setup_config.storage.clone(),
    })))
}

pub(super) async fn provision_local_spiced_cluster(
    run_id: Uuid,
    ready_wait: Duration,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    args: &StdioArgs,
) -> anyhow::Result<RunState> {
    let num_exec = num_executors()?;
    eprintln!("[stdio] local backend: provisioning cluster with {num_exec} executor(s)");
    let cluster_ports = allocate_cluster_ports(LOCAL_BIND_HOST, num_exec)?;

    let working_dir = create_local_working_dir(run_id).await?;
    let local_flight_api_key = (setup_config.sink_type == Some(EtlSinkType::Adbc))
        .then(|| format!("spidapter-local-{run_id}"));

    let setup_result = async {
        let scheduler_dir = working_dir.join("scheduler");
        tokio::fs::create_dir_all(&scheduler_dir).await?;

        let mut executor_dirs = Vec::with_capacity(num_exec);
        for i in 0..num_exec {
            let dir = working_dir.join(format!("executor-{i}"));
            tokio::fs::create_dir_all(&dir).await?;
            executor_dirs.push(dir);
        }

        let spicepod = generate_initial_spicepod(
            &run_id,
            setup_config,
            datasets,
            local_flight_api_key.as_deref(),
            args,
        )
        .await?;
        let spicepod_path = write_local_spicepod(&spicepod, &working_dir).await?;

        let run_id_str = run_id.to_string();
        let short_run_id = run_id_str.split('-').next().unwrap_or_default();
        let process_id = std::process::id();
        let scheduler_cert_name = format!("spidapter-scheduler-{short_run_id}-{process_id}");
        let executor_cert_names: Vec<String> = (0..num_exec)
            .map(|i| format!("spidapter-executor{i}-{short_run_id}-{process_id}"))
            .collect();

        let pki_paths = ensure_local_cluster_pki(
            LOCAL_SPICE_BINARY,
            LOCAL_CONNECT_HOST,
            &scheduler_cert_name,
            &executor_cert_names,
        )
        .await?;

        Ok::<_, anyhow::Error>((scheduler_dir, executor_dirs, spicepod_path, pki_paths))
    }
    .await;

    let (scheduler_dir, executor_dirs, spicepod_path, pki_paths) = match setup_result {
        Ok(result) => result,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let scheduler_args = scheduler_spiced_args(
        LOCAL_BIND_HOST,
        LOCAL_CONNECT_HOST,
        &cluster_ports,
        &pki_paths,
        spicepod_path.as_path(),
    );
    let extra_envs = build_local_extra_envs(setup_config);
    let mut scheduler_child = match spawn_local_spiced(
        &args.spiced_binary,
        &scheduler_dir,
        &scheduler_args,
        "scheduler",
        &extra_envs,
    ) {
        Ok(child) => child,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let scheduler_http_url = format!(
        "http://{}:{}",
        LOCAL_CONNECT_HOST, cluster_ports.scheduler_http
    );
    let scheduler_sql_url = format!("{scheduler_http_url}/v1/sql");

    if let Err(error) = wait_for_local_http_ready(
        &scheduler_http_url,
        &mut scheduler_child,
        ready_wait,
        "scheduler",
    )
    .await
    {
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    let executor_http_urls: Vec<String> = cluster_ports
        .executor_ports
        .iter()
        .take(num_exec)
        .map(|p| format!("http://{}:{}", LOCAL_CONNECT_HOST, p.0))
        .collect();

    let mut executor_children = Vec::with_capacity(num_exec);
    for (i, executor_dir) in executor_dirs.iter().enumerate().take(num_exec) {
        let (executor_cert, executor_key) = &pki_paths.executor_pki[i];
        let exec_args = executor_spiced_args(
            LOCAL_BIND_HOST,
            LOCAL_CONNECT_HOST,
            cluster_ports.scheduler_node,
            cluster_ports.executor_ports[i],
            &pki_paths.ca_cert,
            executor_cert,
            executor_key,
        );
        let label = format!("executor-{i}");
        match spawn_local_spiced(
            &args.spiced_binary,
            executor_dir,
            &exec_args,
            &label,
            &extra_envs,
        ) {
            Ok(child) => executor_children.push(child),
            Err(error) => {
                for c in &mut executor_children {
                    let _ = stop_child_process(c, "executor").await;
                }
                let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
                let _ = cleanup_local_artifacts(&working_dir).await;
                return Err(error);
            }
        }
    }

    if let Err(error) = wait_for_local_sql_ready(
        &scheduler_sql_url,
        &mut scheduler_child,
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        for c in &mut executor_children {
            let _ = stop_child_process(c, "executor").await;
        }
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if num_exec > 1 {
        let remaining = num_exec.saturating_sub(1);
        eprintln!(
            "[stdio] local backend: waiting for remaining {remaining} executor(s) to register..."
        );
        if let Err(error) = wait_for_local_executor_count(
            &scheduler_http_url,
            &executor_http_urls,
            &mut scheduler_child,
            &mut executor_children,
            num_exec,
            ready_wait,
        )
        .await
        {
            for c in &mut executor_children {
                let _ = stop_child_process(c, "executor").await;
            }
            let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    }

    if let Err(error) = post_setup_sink_action(
        setup_config,
        datasets,
        &scheduler_sql_url,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        for c in &mut executor_children {
            let _ = stop_child_process(c, "executor").await;
        }
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if let Err(error) = wait_for_runtime_ready(
        &scheduler_http_url,
        &mut scheduler_child,
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        for c in &mut executor_children {
            let _ = stop_child_process(c, "executor").await;
        }
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    Ok(RunState::Local(Box::new(LocalRunState {
        processes: LocalProcesses::Cluster {
            scheduler_child,
            executor_children,
        },
        flight_url: format!(
            "grpc://{}:{}",
            LOCAL_CONNECT_HOST, cluster_ports.scheduler_flight
        ),
        flight_api_key: local_flight_api_key,
        sql_url: scheduler_sql_url,
        working_dir,
        storage: setup_config.storage.clone(),
    })))
}

pub(super) async fn teardown_local_run(local_state: &mut LocalRunState) -> anyhow::Result<()> {
    eprintln!(
        "[stdio] teardown: stopping local process(es) (sql endpoint: {})",
        local_state.sql_url
    );
    match &mut local_state.processes {
        LocalProcesses::SingleNode { child } => {
            stop_child_process(child, "spiced").await?;
        }
        LocalProcesses::Cluster {
            scheduler_child,
            executor_children,
        } => {
            for (i, child) in executor_children.iter_mut().enumerate() {
                stop_child_process(child, &format!("executor-{i}")).await?;
            }
            stop_child_process(scheduler_child, "scheduler").await?;
        }
    }

    cleanup_local_artifacts(&local_state.working_dir).await
}

fn allocate_cluster_ports(host: &str, num_executors: usize) -> anyhow::Result<ClusterPorts> {
    let mut executor_ports = Vec::with_capacity(num_executors);
    for _ in 0..num_executors {
        executor_ports.push((
            reserve_local_port(host)?,
            reserve_local_port(host)?,
            reserve_local_port(host)?,
        ));
    }
    Ok(ClusterPorts {
        scheduler_http: reserve_local_port(host)?,
        scheduler_flight: reserve_local_port(host)?,
        scheduler_node: reserve_local_port(host)?,
        executor_ports,
    })
}

fn allocate_local_ports(host: &str, num_executors: usize) -> anyhow::Result<LocalPorts> {
    let mut executor_ports = Vec::with_capacity(num_executors);
    for _ in 0..num_executors {
        executor_ports.push((
            reserve_local_port(host)?,
            reserve_local_port(host)?,
            reserve_local_port(host)?,
        ));
    }
    Ok(LocalPorts {
        http: reserve_local_port(host)?,
        flight: reserve_local_port(host)?,
    })
}

fn reserve_local_port(host: &str) -> anyhow::Result<u16> {
    let listener = TcpListener::bind((host, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn num_executors() -> anyhow::Result<usize> {
    match std::env::var(SPIDAPTER_NUM_EXECUTORS_ENV) {
        Ok(raw) => {
            let parsed = raw.trim().parse::<usize>().map_err(|error| {
                anyhow::anyhow!(
                    "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value '{raw}': {error}. Expected an integer in the range 1..={MAX_LOCAL_EXECUTORS}."
                )
            })?;

            if !(1..=MAX_LOCAL_EXECUTORS).contains(&parsed) {
                anyhow::bail!(
                    "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value '{parsed}'. Supported range for the local backend is 1..={MAX_LOCAL_EXECUTORS}."
                );
            }

            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(1),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
            "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value: expected valid UTF-8 in the range 1..={MAX_LOCAL_EXECUTORS}."
        ),
    }
}

fn standalone_spiced_args(bind_host: &str, ports: LocalPorts, spicepod_path: &Path) -> Vec<String> {
    vec![
        "--http".to_string(),
        format!("{bind_host}:{}", ports.http),
        "--flight".to_string(),
        format!("{bind_host}:{}", ports.flight),
        spicepod_path.display().to_string(),
    ]
}

fn scheduler_spiced_args(
    bind_host: &str,
    advertise_host: &str,
    ports: &ClusterPorts,
    pki_paths: &LocalPkiPaths,
    spicepod_path: &Path,
) -> Vec<String> {
    vec![
        "--role".to_string(),
        "scheduler".to_string(),
        "--http".to_string(),
        format!("{bind_host}:{}", ports.scheduler_http),
        "--flight".to_string(),
        format!("{bind_host}:{}", ports.scheduler_flight),
        "--node-bind-address".to_string(),
        format!("{bind_host}:{}", ports.scheduler_node),
        "--node-advertise-address".to_string(),
        advertise_host.to_string(),
        "--node-mtls-ca-certificate-file".to_string(),
        pki_paths.ca_cert.display().to_string(),
        "--node-mtls-certificate-file".to_string(),
        pki_paths.scheduler_cert.display().to_string(),
        "--node-mtls-key-file".to_string(),
        pki_paths.scheduler_key.display().to_string(),
        spicepod_path.display().to_string(),
    ]
}

fn executor_spiced_args(
    bind_host: &str,
    scheduler_host: &str,
    scheduler_node_port: u16,
    executor_ports: (u16, u16, u16),
    ca_cert: &Path,
    executor_cert: &Path,
    executor_key: &Path,
) -> Vec<String> {
    vec![
        "--role".to_string(),
        "executor".to_string(),
        "--scheduler-address".to_string(),
        format!("https://{scheduler_host}:{scheduler_node_port}"),
        "--http".to_string(),
        format!("{bind_host}:{}", executor_ports.0),
        "--flight".to_string(),
        format!("{bind_host}:{}", executor_ports.1),
        "--node-bind-address".to_string(),
        format!("{bind_host}:{}", executor_ports.2),
        "--node-advertise-address".to_string(),
        scheduler_host.to_string(),
        "--node-mtls-ca-certificate-file".to_string(),
        ca_cert.display().to_string(),
        "--node-mtls-certificate-file".to_string(),
        executor_cert.display().to_string(),
        "--node-mtls-key-file".to_string(),
        executor_key.display().to_string(),
    ]
}

fn spawn_local_spiced(
    spiced_path: &str,
    current_dir: &Path,
    args: &[String],
    process_name: &str,
    extra_envs: &HashMap<String, String>,
) -> anyhow::Result<Child> {
    let log_path = current_dir.join(format!("{process_name}.log"));
    eprintln!(
        "[stdio] local backend: launching {process_name} process: {spiced_path} {}",
        args.join(" ")
    );
    eprintln!(
        "[stdio] local backend: {process_name} logs: {}",
        log_path.display()
    );

    let log_file = std::fs::File::create(&log_path)
        .map_err(|e| anyhow::anyhow!("Failed to create log file {}: {e}", log_path.display()))?;

    TokioCommand::new(spiced_path)
        .kill_on_drop(true)
        .args(args)
        .current_dir(current_dir)
        .envs(extra_envs)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| anyhow::anyhow!("Failed to start local {process_name} process: {error}"))
}

async fn create_local_working_dir(run_id: Uuid) -> anyhow::Result<PathBuf> {
    let run_dir = std::env::temp_dir().join(format!("spidapter-local-{run_id}"));
    if tokio::fs::metadata(&run_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&run_dir).await?;
    }
    tokio::fs::create_dir_all(&run_dir).await?;
    Ok(run_dir)
}

fn pki_dir() -> anyhow::Result<PathBuf> {
    let home_dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Home directory not found; cannot resolve ~/.spice/pki"))?;
    Ok(home_dir.join(".spice").join("pki"))
}

async fn ensure_local_cluster_pki(
    spice_cli_path: &str,
    host: &str,
    scheduler_cert_name: &str,
    executor_cert_names: &[String],
) -> anyhow::Result<LocalPkiPaths> {
    let pki_dir = pki_dir()?;
    let ca_cert = pki_dir.join("ca.crt");
    let ca_key = pki_dir.join("ca.key");

    if !ca_cert.exists() || !ca_key.exists() {
        eprintln!("[stdio] local backend: generating cluster CA with spice cluster tls init");
        run_spice_cli_command(
            spice_cli_path,
            vec!["cluster".to_string(), "tls".to_string(), "init".to_string()],
        )
        .await?;
    }

    add_tls_certificate(spice_cli_path, scheduler_cert_name, host).await?;
    let mut executor_pki = Vec::with_capacity(executor_cert_names.len());
    for name in executor_cert_names {
        add_tls_certificate(spice_cli_path, name, host).await?;
        executor_pki.push((
            pki_dir.join(format!("{name}.crt")),
            pki_dir.join(format!("{name}.key")),
        ));
    }

    Ok(LocalPkiPaths {
        ca_cert,
        scheduler_cert: pki_dir.join(format!("{scheduler_cert_name}.crt")),
        scheduler_key: pki_dir.join(format!("{scheduler_cert_name}.key")),
        executor_pki,
    })
}

async fn add_tls_certificate(
    spice_cli_path: &str,
    cert_name: &str,
    host: &str,
) -> anyhow::Result<()> {
    let mut cli_args = vec![
        "cluster".to_string(),
        "tls".to_string(),
        "add".to_string(),
        cert_name.to_string(),
    ];
    if !host.is_empty() {
        cli_args.push("--host".to_string());
        cli_args.push(host.to_string());
    }
    run_spice_cli_command(spice_cli_path, cli_args).await
}

async fn run_spice_cli_command(binary_path: &str, cli_args: Vec<String>) -> anyhow::Result<()> {
    use std::process::Command as StdCommand;
    let binary_path = binary_path.to_string();
    let command_display = format!("{binary_path} {}", cli_args.join(" "));
    let display_clone = command_display.clone();
    let args_clone = cli_args;

    let status = tokio::task::spawn_blocking(move || {
        StdCommand::new(&binary_path)
            .args(&args_clone)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| anyhow::anyhow!("Failed to execute '{display_clone}': {e}"))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Failed to join command '{command_display}': {e}"))??;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "Command '{command_display}' failed with status {status}"
        ));
    }
    Ok(())
}

async fn wait_for_runtime_ready(
    http_url: &str,
    child: &mut Child,
    timeout: Duration,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    eprintln!(
        "[stdio] waiting up to {}s for runtime to become ready...",
        timeout.as_secs()
    );

    let ready_url = format!("{http_url}/v1/ready");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let started = tokio::time::Instant::now();

    loop {
        ensure_process_is_running(child, "spiced")?;

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for runtime to become ready",
                timeout.as_secs()
            ));
        }

        let mut request = client.get(&ready_url);
        if let Some(key) = api_key {
            request = request.header("X-API-Key", key);
        }

        if let Ok(response) = request.send().await
            && response.status().is_success()
        {
            eprintln!(
                "[stdio] runtime ready after {}ms",
                started.elapsed().as_millis()
            );
            return Ok(());
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_local_http_ready(
    http_url: &str,
    child: &mut Child,
    timeout: Duration,
    process_name: &str,
) -> anyhow::Result<()> {
    let ready_url = format!("{http_url}/health");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    let started = tokio::time::Instant::now();
    loop {
        ensure_process_is_running(child, process_name)?;

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for local {process_name} readiness at {ready_url}",
                timeout.as_secs()
            ));
        }

        match client.get(&ready_url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn wait_for_local_sql_ready(
    sql_url: &str,
    child: &mut Child,
    timeout: Duration,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    let started = tokio::time::Instant::now();
    loop {
        ensure_process_is_running(child, "spiced")?;

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for local SQL readiness at {sql_url}",
                timeout.as_secs()
            ));
        }

        let mut request = client.post(sql_url).body("SELECT 1");
        if let Some(key) = api_key {
            request = request.header("X-API-Key", key);
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn wait_for_local_executor_count(
    scheduler_http_url: &str,
    executor_http_urls: &[String],
    scheduler_child: &mut Child,
    executor_children: &mut [Child],
    expected_count: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let metrics_url = format!("{scheduler_http_url}/metrics");
    let started = tokio::time::Instant::now();

    loop {
        ensure_process_is_running(scheduler_child, "scheduler")?;
        for (idx, child) in executor_children.iter_mut().enumerate() {
            ensure_process_is_running(child, &format!("executor-{idx}"))?;
        }

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for {expected_count} local executors to register via {metrics_url}",
                timeout.as_secs()
            ));
        }

        if let Ok(response) = client.get(&metrics_url).send().await
            && response.status().is_success()
            && let Ok(body) = response.text().await
            && scheduler_active_executor_count(&body)
                .is_some_and(|active_count| active_count >= expected_count)
        {
            return Ok(());
        }

        if started.elapsed() >= LOCAL_EXECUTOR_REGISTRATION_METRIC_GRACE
            && all_local_executor_http_ready(&client, executor_http_urls).await
        {
            eprintln!(
                "[stdio] local backend: scheduler executor-count metric unavailable; falling back to per-executor health checks"
            );
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn all_local_executor_http_ready(
    client: &reqwest::Client,
    executor_http_urls: &[String],
) -> bool {
    for executor_http_url in executor_http_urls {
        let ready_url = format!("{executor_http_url}/health");
        match client.get(&ready_url).send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(_) | Err(_) => return false,
        }
    }

    true
}

fn scheduler_active_executor_count(metrics_body: &str) -> Option<usize> {
    metrics_body
        .lines()
        .filter(|line| line.starts_with("scheduler_active_executors_count"))
        .filter_map(|line| line.split_whitespace().last())
        .filter_map(|value| value.parse::<usize>().ok())
        .max()
}

fn ensure_process_is_running(child: &mut Child, process_name: &str) -> anyhow::Result<()> {
    if let Some(status) = child.try_wait()? {
        return Err(anyhow::anyhow!(
            "Local {process_name} process exited early with status {status}"
        ));
    }
    Ok(())
}

async fn stop_child_process(child: &mut Child, process_name: &str) -> anyhow::Result<()> {
    if let Some(status) = child.try_wait()? {
        eprintln!("[stdio] local backend: {process_name} already stopped with status {status}");
        return Ok(());
    }

    child.kill().await.map_err(|error| {
        anyhow::anyhow!("Failed to terminate local {process_name} process: {error}")
    })?;

    let status = child.wait().await.map_err(|error| {
        anyhow::anyhow!("Failed to wait for local {process_name} process: {error}")
    })?;

    eprintln!("[stdio] local backend: {process_name} stopped with status {status}");
    Ok(())
}

async fn cleanup_local_artifacts(working_dir: &Path) -> anyhow::Result<()> {
    if tokio::fs::metadata(working_dir).await.is_ok() {
        // Preserve log files outside the working dir before deleting it
        for entry in std::fs::read_dir(working_dir)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "log") {
                let dest = std::env::temp_dir().join(entry.file_name());
                let _ = std::fs::copy(&path, &dest);
                eprintln!("[stdio] local backend: log preserved at {}", dest.display());
            }
        }
        tokio::fs::remove_dir_all(working_dir).await?;
        eprintln!(
            "[stdio] local backend: removed artifacts in {}",
            working_dir.display()
        );
    }
    Ok(())
}
