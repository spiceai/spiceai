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

//! Docker orchestration helpers used by the Cayenne snapshot-refresh
//! integration test.
//!
//! The Cayenne acceleration engine writes **absolute** filesystem paths into
//! its catalog metastore (`cayenne_table.path`, `cayenne_partition.path`,
//! ...) which means a snapshot bootstrapped on a node with a different data
//! directory cannot resolve any of its files. See spiceai/spiceai#10642.
//!
//! In production this is fine because every node uses the same
//! `spice_data_base_path()` (e.g. `/spice/data`). The integration test uses
//! Docker to reproduce that environment: each container gets its own host
//! volume bind-mounted at the *same* container path, so the writer's
//! catalog absolute paths resolve cleanly inside the reader container.
//!
//! How the test runs:
//!
//! 1. The orchestrator (`#[tokio::test]`) builds a small image inline.
//!    The image is `ubuntu:24.04` plus the **same integration-test binary**
//!    that is currently executing — copied in via `current_exe()`. This
//!    keeps the image fully self-contained without a separate `cargo build`
//!    step.
//! 2. The orchestrator launches two containers (writer + reader) running
//!    that same test binary, each invoking a different `#[ignore]`d worker
//!    test (`cayenne_inner_writer`, `cayenne_inner_reader`). Each container
//!    sees identical container paths (`/data/cayenne`, `/data/source.csv`)
//!    bind-mounted from per-container host temp directories.
//! 3. Both workers communicate only through the shared S3 snapshot store
//!    (per-test UUID prefix).
//! 4. The orchestrator waits for both containers to exit and asserts each
//!    returned a 0 exit code.
//!
//! The orchestrator skips on non-Linux hosts (the test binary on macOS is
//! a Mach-O executable that cannot run inside a Linux container) and when
//! Docker is unreachable.

use std::{collections::HashMap, env, path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use tokio::{io::AsyncWriteExt, process::Command};

/// Returns true on platforms where the integration-test binary can be
/// executed inside a Linux container (i.e. Linux hosts) and Docker is
/// reachable from the current process.
pub(crate) async fn is_docker_orchestration_supported() -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// A built docker image that removes itself on drop.
pub(crate) struct OrchestratorImage {
    pub tag: String,
}

impl Drop for OrchestratorImage {
    fn drop(&mut self) {
        let tag = self.tag.clone();
        // Best-effort cleanup; no async runtime here so use std::process.
        let _ = std::process::Command::new("docker")
            .args(["rmi", "-f", &tag])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Build a self-contained image whose ENTRYPOINT is the *current* test
/// binary. The Dockerfile is generated inline and the binary is copied
/// from `current_exe()` into the build context.
pub(crate) async fn build_orchestrator_image(tag: &str) -> Result<OrchestratorImage> {
    let test_binary = env::current_exe().context("locating current test binary")?;

    let context_dir = tempfile::tempdir().context("creating docker build context tempdir")?;
    let context_path = context_dir.path();

    // Copy the binary into the build context. We must use the synchronous
    // copy here because the build step shells out and needs the file in
    // place before invoking `docker build`.
    let dest_binary = context_path.join("snapshot_refresh_worker");
    std::fs::copy(&test_binary, &dest_binary)
        .with_context(|| format!("copying {} into docker build context", test_binary.display()))?;
    // Mark executable (cargo already sets +x but the copy preserves mode;
    // belt-and-braces).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(&dest_binary)?.permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&dest_binary, perm)?;
    }

    // Inline Dockerfile. ubuntu:24.04 (glibc 2.39) is required because
    // the GitHub Actions Linux runners build the test binary against a
    // newer glibc than ubuntu:22.04 (2.35) ships; otherwise the
    // dynamically-linked binary fails with `GLIBC_2.38 not found` at
    // exec time inside the container.
    let dockerfile = "\
FROM ubuntu:24.04
RUN apt-get update \\
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \\
 && rm -rf /var/lib/apt/lists/*
COPY snapshot_refresh_worker /usr/local/bin/snapshot_refresh_worker
RUN chmod +x /usr/local/bin/snapshot_refresh_worker
WORKDIR /data
ENTRYPOINT [\"/usr/local/bin/snapshot_refresh_worker\"]
";
    std::fs::write(context_path.join("Dockerfile"), dockerfile)
        .context("writing inline Dockerfile")?;

    let status = Command::new("docker")
        .args(["build", "-t", tag, "."])
        .current_dir(context_path)
        .status()
        .await
        .context("invoking docker build")?;
    if !status.success() {
        bail!(
            "docker build failed with status {} for tag {tag}",
            status
        );
    }

    Ok(OrchestratorImage {
        tag: tag.to_string(),
    })
}

/// Configuration for one worker container.
pub(crate) struct WorkerContainerSpec<'a> {
    /// Container name.
    pub name: &'a str,
    /// Tag of the image built by [`build_orchestrator_image`].
    pub image: &'a str,
    /// Host directory bind-mounted to `/data` inside the container.
    pub host_data_dir: &'a Path,
    /// Environment variables passed in.
    pub env: &'a HashMap<String, String>,
    /// `--ignored --exact <test>` is invoked.
    pub test_name: &'a str,
}

/// Output captured from a finished container.
pub(crate) struct WorkerContainerResult {
    pub exit_code: i64,
    pub stdout: String,
    pub stderr: String,
}

/// Run one worker container to completion. The orchestrator should call
/// this concurrently for writer + reader.
pub(crate) async fn run_worker_container(spec: WorkerContainerSpec<'_>) -> Result<WorkerContainerResult> {
    // Best effort: remove any prior container with the same name.
    let _ = Command::new("docker")
        .args(["rm", "-f", spec.name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;

    let mount_arg = format!(
        "{}:/data",
        spec.host_data_dir
            .to_str()
            .ok_or_else(|| anyhow!("host_data_dir contains invalid utf8"))?
    );

    let mut docker = Command::new("docker");
    docker.args([
        "run",
        "--name",
        spec.name,
        "--rm",
        "-v",
        mount_arg.as_str(),
    ]);

    for (k, v) in spec.env {
        docker.args(["-e", &format!("{k}={v}")]);
    }

    // Image, then args passed to the entrypoint binary. Cargo's libtest
    // harness understands `--ignored --exact <name>`.
    docker.arg(spec.image);
    docker.args(["--ignored", "--exact", "--nocapture", spec.test_name]);

    docker.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = docker.spawn().context("spawning docker run")?;

    let stdout_handle = child.stdout.take().context("capturing container stdout")?;
    let stderr_handle = child.stderr.take().context("capturing container stderr")?;

    let stdout_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut reader = tokio::io::BufReader::new(stdout_handle);
        let _ = reader.read_to_end(&mut buf).await;
        String::from_utf8_lossy(&buf).into_owned()
    });
    let stderr_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut reader = tokio::io::BufReader::new(stderr_handle);
        let _ = reader.read_to_end(&mut buf).await;
        String::from_utf8_lossy(&buf).into_owned()
    });

    let status = child.wait().await.context("awaiting docker run")?;
    let stdout = stdout_task.await.unwrap_or_default();
    let stderr = stderr_task.await.unwrap_or_default();

    Ok(WorkerContainerResult {
        exit_code: status.code().unwrap_or(-1) as i64,
        stdout,
        stderr,
    })
}

/// Forcefully remove a container if still around (e.g. after a hung run).
pub(crate) async fn force_remove(name: &str) {
    let _ = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new("docker")
            .args(["rm", "-f", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    )
    .await;
}

// Quiet a clippy lint about unused field on platforms where Drop is the
// only user.
#[allow(dead_code)]
async fn _unused() {
    let mut c = Command::new("true");
    c.stdin(Stdio::piped());
    if let Ok(mut child) = c.spawn()
        && let Some(mut stdin) = child.stdin.take()
    {
        let _ = stdin.write_all(b"x").await;
    }
}
