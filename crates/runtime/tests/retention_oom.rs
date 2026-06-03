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

use std::{
    collections::HashMap,
    io::{BufWriter, Read as _, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use app::AppBuilder;
use arrow::{
    array::{Int64Array, UInt64Array},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use tokio::process::Command;
use tracing_subscriber::EnvFilter;

const INNER_MODE_ENV: &str = "SPICE_OOM_REPRO_INNER";
const INNER_ROWS_ENV: &str = "SPICE_OOM_REPRO_ROWS";
const INNER_PAYLOAD_BYTES_ENV: &str = "SPICE_OOM_REPRO_PAYLOAD_BYTES";

const DEFAULT_ROWS: usize = 1_500_000;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const CONTAINER_MEMORY_LIMIT_MB: usize = 768;
const TEST_CAYENNE_WRITE_CONCURRENCY: &str = "4";

const TEST_NAME: &str = "test_cayenne_pk_delete_oom_repro";

#[derive(Debug, Clone, Copy)]
enum ExecutableFormat {
    Elf,
    MachO,
    Unknown,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_pk_delete_oom_repro() -> Result<(), anyhow::Error> {
    init_tracing();

    if std::env::var_os(INNER_MODE_ENV).is_some() {
        run_inner_workload().await
    } else {
        run_outer_container_repro().await
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("runtime=info,cayenne=debug,info")),
        )
        .with_ansi(true)
        .try_init();
}

async fn run_outer_container_repro() -> Result<(), anyhow::Error> {
    if !docker_is_available().await {
        eprintln!("Docker is not available, skipping retention OOM reproduction test.");
        return Ok(());
    }

    let rows = env_usize(INNER_ROWS_ENV, DEFAULT_ROWS)?;
    let payload_bytes = env_usize(INNER_PAYLOAD_BYTES_ENV, DEFAULT_PAYLOAD_BYTES)?;

    eprintln!(
        "Running containerized retention delete regression with rows={rows}, payload_bytes={payload_bytes}, memory_limit={CONTAINER_MEMORY_LIMIT_MB}MiB"
    );

    let image_tag = format!(
        "spice-retention-oom-repro-{}",
        uuid::Uuid::now_v7().simple()
    );

    let host_test_binary = std::env::current_exe().context("failed to locate test executable")?;
    let executable_format = detect_executable_format(&host_test_binary)?;

    match executable_format {
        ExecutableFormat::Elf => {
            eprintln!(
                "Using Linux fast-path: packaging host ELF test binary at {}",
                host_test_binary.display()
            );
            build_image_from_host_binary(&image_tag, &host_test_binary).await?;
        }
        ExecutableFormat::MachO | ExecutableFormat::Unknown => {
            let workspace_root = workspace_root()?;
            eprintln!(
                "Using source-build Docker fallback for format {:?} (workspace={})",
                executable_format,
                workspace_root.display()
            );
            build_image_from_source(&image_tag, &workspace_root).await?;
        }
    }

    let run_output = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg(format!("--memory={CONTAINER_MEMORY_LIMIT_MB}m"))
        .arg(format!("--memory-swap={CONTAINER_MEMORY_LIMIT_MB}m"))
        .arg("-e")
        .arg(format!("{INNER_MODE_ENV}=1"))
        .arg("-e")
        .arg(format!("{INNER_ROWS_ENV}={rows}"))
        .arg("-e")
        .arg(format!("{INNER_PAYLOAD_BYTES_ENV}={payload_bytes}"))
        .arg(&image_tag)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .output()
        .await
        .context("failed to execute docker run")?;

    let _ = remove_docker_image(&image_tag).await;

    let exit_code = run_output.status.code().unwrap_or_default();
    if exit_code != 0 {
        return Err(anyhow::anyhow!(
            "Expected containerized retention delete to complete without OOM (exit=0), got exit={exit_code}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&run_output.stdout),
            String::from_utf8_lossy(&run_output.stderr)
        ));
    }

    Ok(())
}

async fn run_inner_workload() -> Result<(), anyhow::Error> {
    let rows = env_usize(INNER_ROWS_ENV, DEFAULT_ROWS)?;
    let payload_bytes = env_usize(INNER_PAYLOAD_BYTES_ENV, DEFAULT_PAYLOAD_BYTES)?;

    eprintln!("Running inner retention workload with rows={rows}, payload_bytes={payload_bytes}");

    let temp_dir = tempfile::tempdir()?;
    let csv_path = temp_dir.path().join("source.csv");
    write_large_csv(&csv_path, rows, payload_bytes)?;

    let csv_size_bytes = std::fs::metadata(&csv_path)
        .context("failed to read generated csv metadata")?
        .len();
    eprintln!(
        "Generated source CSV at {} ({} bytes)",
        csv_path.display(),
        csv_size_bytes
    );

    let cayenne_dir = temp_dir.path().join("cayenne");
    let metadata_dir = temp_dir.path().join("metadata");
    std::fs::create_dir_all(&cayenne_dir)?;
    std::fs::create_dir_all(&metadata_dir)?;

    runtime::dataconnector::register_all().await;
    runtime::catalogconnector::register_all().await;

    let mut params = HashMap::new();
    params.insert(
        "cayenne_file_path".to_string(),
        cayenne_dir.display().to_string(),
    );
    params.insert(
        "cayenne_metadata_dir".to_string(),
        metadata_dir.display().to_string(),
    );
    // Keep snapshot write fan-out bounded so this regression test stays deterministic across hosts with different CPU counts
    params.insert(
        "cayenne_write_concurrency".to_string(),
        TEST_CAYENNE_WRITE_CONCURRENCY.to_string(),
    );

    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), "oom_events");
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        primary_key: Some("id".to_string()),
        retention_sql: Some("DELETE FROM oom_events WHERE id >= 0".to_string()),
        retention_check_enabled: true,
        retention_check_interval: Some("5m".to_string()),
        params: Some(Params::from_string_map(params)),
        ..Acceleration::default()
    });

    let app = AppBuilder::new("retention_oom_repro")
        .with_dataset(dataset)
        .build();

    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(300)) => {
            return Err(anyhow::anyhow!("Timed out waiting for runtime components to load"));
        }
        () = Arc::clone(&runtime).load_components() => {}
    }

    wait_until_runtime_ready(&runtime, Duration::from_secs(120)).await?;

    let loaded_rows = query_single_u64(&runtime, "SELECT COUNT(*) FROM oom_events").await?;
    eprintln!("Loaded rows currently visible in Cayenne table: {loaded_rows}");

    let expected_rows = u64::try_from(rows).context("generated row count exceeded u64 range")?;

    if loaded_rows > expected_rows {
        return Err(anyhow::anyhow!(
            "Visible row count {loaded_rows} exceeded generated input rows {expected_rows}"
        ));
    }

    if rows > 0 && loaded_rows == 0 {
        return Err(anyhow::anyhow!(
            "Expected at least one visible row before retention delete, but COUNT(*) returned 0"
        ));
    }

    eprintln!("Waiting for retention worker to execute PK-based delete...");
    let remaining_rows = wait_for_row_count(
        &runtime,
        "SELECT COUNT(*) FROM oom_events",
        0,
        Duration::from_secs(75),
    )
    .await?;
    eprintln!("Remaining rows after retention worker: {remaining_rows}");

    eprintln!("Retention delete completed without OOM.");
    Ok(())
}

async fn wait_for_row_count(
    rt: &Arc<Runtime>,
    sql: &str,
    expected_rows: u64,
    timeout: Duration,
) -> Result<u64, anyhow::Error> {
    let start = std::time::Instant::now();
    let mut last_rows = None;
    let mut last_error = None;

    while start.elapsed() < timeout {
        tokio::time::sleep(Duration::from_secs(1)).await;

        match query_single_u64(rt, sql).await {
            Ok(rows) if rows == expected_rows => return Ok(rows),
            Ok(rows) => {
                last_rows = Some(rows);
                last_error = None;
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    if let Some(error) = last_error {
        return Err(anyhow::anyhow!(
            "Timed out after {timeout:?} waiting for `{sql}` to return {expected_rows}; last query error: {error:#}"
        ));
    }

    Err(anyhow::anyhow!(
        "Timed out after {timeout:?} waiting for `{sql}` to return {expected_rows}; last row count: {}",
        last_rows.map_or_else(|| "<none>".to_string(), |rows| rows.to_string())
    ))
}

fn workspace_root() -> Result<PathBuf, anyhow::Error> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to determine workspace root from CARGO_MANIFEST_DIR")
        })
}

fn detect_executable_format(path: &Path) -> Result<ExecutableFormat, anyhow::Error> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("failed to open executable at {}", path.display()))?;

    let mut header = [0_u8; 4];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(ExecutableFormat::Unknown);
        }

        return Err(error)
            .with_context(|| format!("failed to read executable header from {}", path.display()));
    }

    if header[0] == 0x7F && header[1] == b'E' && header[2] == b'L' && header[3] == b'F' {
        return Ok(ExecutableFormat::Elf);
    }

    let magic = u32::from_be_bytes(header);
    if matches!(
        magic,
        0xFEED_FACE
            | 0xFEED_FACF
            | 0xCEFA_EDFE
            | 0xCFFA_EDFE
            | 0xCAFE_BABE
            | 0xBEBA_FECA
            | 0xCAFE_BABF
            | 0xBFBA_FECA
    ) {
        return Ok(ExecutableFormat::MachO);
    }

    Ok(ExecutableFormat::Unknown)
}

async fn build_image_from_source(
    image_tag: &str,
    workspace_root: &Path,
) -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let dockerfile_path = temp_dir.path().join("Dockerfile.retention-oom");
    std::fs::write(&dockerfile_path, dockerfile_for_source_build())?;

    let build_status = Command::new("docker")
        .arg("build")
        .arg("-f")
        .arg(&dockerfile_path)
        .arg("-t")
        .arg(image_tag)
        .arg(workspace_root)
        .status()
        .await
        .context("failed to execute docker build")?;

    if !build_status.success() {
        return Err(anyhow::anyhow!("docker build failed for image {image_tag}"));
    }

    Ok(())
}

async fn build_image_from_host_binary(
    image_tag: &str,
    host_test_binary: &Path,
) -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let dockerfile_path = temp_dir.path().join("Dockerfile.retention-oom");
    let staged_binary = temp_dir.path().join("retention_oom");

    std::fs::copy(host_test_binary, &staged_binary).with_context(|| {
        format!(
            "failed to copy host test binary from {} to {}",
            host_test_binary.display(),
            staged_binary.display()
        )
    })?;

    std::fs::write(&dockerfile_path, dockerfile_for_host_binary())?;

    let build_status = Command::new("docker")
        .arg("build")
        .arg("-f")
        .arg(&dockerfile_path)
        .arg("-t")
        .arg(image_tag)
        .arg(temp_dir.path())
        .status()
        .await
        .context("failed to execute docker build for host binary fast-path")?;

    if !build_status.success() {
        return Err(anyhow::anyhow!(
            "docker build failed for host binary image {image_tag}"
        ));
    }

    Ok(())
}

fn dockerfile_for_host_binary() -> &'static str {
    r#"FROM rust:1.91-slim-trixie

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libprotobuf-dev \
        libsqlite3-dev \
        libssl-dev \
        unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY retention_oom /usr/local/bin/retention_oom
RUN chmod +x /usr/local/bin/retention_oom

ENTRYPOINT ["/usr/local/bin/retention_oom"]
"#
}

fn dockerfile_for_source_build() -> &'static str {
    r#"FROM rust:1.91-slim-trixie

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        cmake \
        libprotobuf-dev \
        libsqlite3-dev \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
        unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

ENV CARGO_INCREMENTAL=0
ENV PROTOC=/usr/bin/protoc
ENV PROTOC_INCLUDE=/usr/include
RUN cargo test -p runtime --test retention_oom --no-run --locked

RUN cat <<'EOF' >/usr/local/bin/run-retention-oom.sh
#!/usr/bin/env bash
set -euo pipefail

for candidate in /workspace/target/debug/deps/retention_oom-*; do
  case "$candidate" in
    *.d) continue ;;
    *) exec "$candidate" "$@" ;;
  esac
done

echo "retention_oom test binary not found" >&2
exit 1
EOF
RUN chmod +x /usr/local/bin/run-retention-oom.sh

ENTRYPOINT ["/usr/local/bin/run-retention-oom.sh"]
"#
}

fn write_large_csv(path: &Path, rows: usize, payload_bytes: usize) -> Result<(), anyhow::Error> {
    let file = std::fs::File::create(path)?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

    writer.write_all(b"id,payload\n")?;

    let payload = "x".repeat(payload_bytes);
    for i in 0..rows {
        writeln!(writer, "{i},{payload}")?;
    }

    writer.flush()?;
    Ok(())
}

fn env_usize(name: &str, default: usize) -> Result<usize, anyhow::Error> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("Failed to parse {name} as usize: {value}")),
        Err(_) => Ok(default),
    }
}

async fn docker_is_available() -> bool {
    matches!(
        Command::new("docker")
            .arg("info")
            .status()
            .await,
        Ok(status) if status.success()
    )
}

async fn remove_docker_image(image_tag: &str) -> Result<(), anyhow::Error> {
    let _ = Command::new("docker")
        .arg("image")
        .arg("rm")
        .arg("-f")
        .arg(image_tag)
        .status()
        .await;

    Ok(())
}

async fn wait_until_runtime_ready(rt: &Runtime, timeout: Duration) -> Result<(), anyhow::Error> {
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if rt.status().is_ready() {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting for runtime to become ready"
    ))
}

async fn execute_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(sql).build().run().await?;

    let mut batches = Vec::new();
    while let Some(batch) = result.data.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}

async fn query_single_u64(rt: &Arc<Runtime>, sql: &str) -> Result<u64, anyhow::Error> {
    let batches = execute_sql(rt, sql).await?;
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("Query returned no batches: {sql}"))?;

    if batch.num_rows() != 1 {
        return Err(anyhow::anyhow!(
            "Expected single-row scalar result for '{sql}', got {} rows",
            batch.num_rows()
        ));
    }

    let column = batch.column(0);

    if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(values.value(0));
    }

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        let value = values.value(0);
        return u64::try_from(value).context("Scalar count value was negative");
    }

    Err(anyhow::anyhow!(
        "Unexpected scalar array type for query '{sql}': {:?}",
        column.data_type()
    ))
}
