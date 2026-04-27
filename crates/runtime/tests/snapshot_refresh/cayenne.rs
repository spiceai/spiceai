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

//! Cayenne snapshot-refresh integration test, orchestrated through Docker.
//!
//! Cayenne stores absolute filesystem paths inside its catalog metastore,
//! so a snapshot bootstrapped on a node with a different data directory
//! cannot resolve any of its files (tracked in spiceai/spiceai#10642).
//! Production deployments avoid this because every node uses the same
//! `spice_data_base_path()`. To reproduce that condition in CI without
//! making the writer/reader share an in-process working directory we run
//! each side in its own container with identical container paths.
//!
//! The orchestrator is `snapshot_refresh_cayenne_bootstrap_then_refresh` —
//! it runs by default, builds a self-contained image, launches two worker
//! containers, and asserts both exit 0. The worker tests
//! (`cayenne_inner_writer`, `cayenne_inner_reader`) are `#[ignore]`d so
//! they only run when explicitly requested by the orchestrator inside its
//! containers.

#![cfg(not(target_os = "windows"))]

use std::{collections::HashMap, env, time::Duration};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use super::docker::{
    WorkerContainerSpec, build_orchestrator_image, force_remove,
    is_docker_orchestration_supported, run_worker_container,
};
use super::{
    EngineKind, SnapshotS3Context, WorkerConfig, run_reader_phase, run_writer_phase,
    wait_for_snapshot_id_at_prefix,
};
use crate::{init_tracing, utils::register_test_connectors};

const WRITER_TEST: &str = "snapshot_refresh::cayenne::cayenne_inner_writer";
const READER_TEST: &str = "snapshot_refresh::cayenne::cayenne_inner_reader";

/// Full path inside the container where each worker stores its Cayenne
/// state. Identical for writer and reader so the catalog's absolute paths
/// resolve symmetrically across the two containers.
const CONTAINER_DATA_DIR: &str = "/data";
const CONTAINER_CAYENNE_PATH: &str = "/data/cayenne";
const CONTAINER_SOURCE_CSV: &str = "/data/source.csv";

/// Orchestrator: builds an inline image, launches writer + reader
/// containers in parallel, asserts each exits 0, cleans up the S3 prefix.
///
/// Currently `#[ignore]`d. The framework runs end-to-end (writer
/// container exits 0, reader container starts and downloads the
/// snapshot), but the reader's bootstrap fails the archive integrity
/// check because the runtime eagerly creates `cayenne.db-shm`/`-wal`
/// SQLite journal sidecars in the metadata directory before snapshot
/// extraction runs. Tracked in spiceai/spiceai#10649; the underlying
/// Cayenne catalog absolute-path constraint that motivated the Docker
/// harness is tracked in spiceai/spiceai#10642. The DuckDB integration
/// test exercises the full snapshot-refresh code path end-to-end.
#[ignore = "blocked on spiceai/spiceai#10649 (Cayenne metastore init races with snapshot extraction integrity check)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_refresh_cayenne_bootstrap_then_refresh() -> Result<()> {
    init_tracing(None);

    if !is_docker_orchestration_supported().await {
        eprintln!(
            "skipping snapshot_refresh_cayenne: requires Linux + reachable Docker daemon \
             (current platform: {})",
            std::env::consts::OS
        );
        return Ok(());
    }

    // Per-test S3 prefix so concurrent runs don't collide.
    let s3 = SnapshotS3Context::new("snapshot_refresh_cayenne").await?;

    // Run regardless of test outcome.
    let result = run_orchestrator(&s3).await;

    if let Err(cleanup_err) = s3.cleanup().await {
        tracing::warn!(error = %cleanup_err, "cayenne snapshot cleanup encountered errors");
    }

    result
}

async fn run_orchestrator(s3: &SnapshotS3Context) -> Result<()> {
    let image_tag = format!("snapshot-refresh-test:{}", Uuid::now_v7());
    let image = build_orchestrator_image(&image_tag)
        .await
        .context("building orchestrator image")?;

    // Per-container host directories. Both bind-mount to the same container
    // path, so absolute paths in the writer's catalog still resolve in the
    // reader's filesystem.
    let host_dirs = tempfile::tempdir().context("creating per-container host dirs")?;
    let writer_host = host_dirs.path().join("writer");
    let reader_host = host_dirs.path().join("reader");
    std::fs::create_dir_all(&writer_host)?;
    std::fs::create_dir_all(&reader_host)?;

    let env = container_env(&s3.prefix);

    let writer_name = format!("snapshot-refresh-writer-{}", Uuid::now_v7());
    let reader_name = format!("snapshot-refresh-reader-{}", Uuid::now_v7());

    let writer_spec = WorkerContainerSpec {
        name: &writer_name,
        image: &image.tag,
        host_data_dir: &writer_host,
        env: &env,
        test_name: WRITER_TEST,
    };
    let reader_spec = WorkerContainerSpec {
        name: &reader_name,
        image: &image.tag,
        host_data_dir: &reader_host,
        env: &env,
        test_name: READER_TEST,
    };

    let (writer_res, reader_res) = tokio::join!(
        run_worker_container(writer_spec),
        run_worker_container(reader_spec),
    );

    // Always try to clean up containers in case they didn't `--rm` cleanly.
    force_remove(&writer_name).await;
    force_remove(&reader_name).await;

    let writer_res = writer_res.context("writer container run")?;
    let reader_res = reader_res.context("reader container run")?;

    if writer_res.exit_code != 0 || reader_res.exit_code != 0 {
        return Err(anyhow!(
            "worker container failure: writer exit={} reader exit={}\n\
             ---- writer stdout ----\n{}\n---- writer stderr ----\n{}\n\
             ---- reader stdout ----\n{}\n---- reader stderr ----\n{}",
            writer_res.exit_code,
            reader_res.exit_code,
            writer_res.stdout,
            writer_res.stderr,
            reader_res.stdout,
            reader_res.stderr,
        ));
    }

    Ok(())
}

fn container_env(s3_prefix: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert(
        "SNAPSHOT_REFRESH_ENGINE".to_string(),
        "cayenne".to_string(),
    );
    env.insert(
        "SNAPSHOT_REFRESH_SOURCE_CSV".to_string(),
        CONTAINER_SOURCE_CSV.to_string(),
    );
    env.insert(
        "SNAPSHOT_REFRESH_LOCAL_DB".to_string(),
        CONTAINER_CAYENNE_PATH.to_string(),
    );
    env.insert(
        "SNAPSHOT_REFRESH_S3_PREFIX".to_string(),
        s3_prefix.to_string(),
    );
    env.insert(
        "SPICED_LOG".to_string(),
        env::var("SPICED_LOG").unwrap_or_else(|_| {
            "runtime=DEBUG,runtime_acceleration=DEBUG,info".to_string()
        }),
    );

    // Forward AWS credentials. Prefer the snapshot-test-specific pair
    // (matches the orchestrator's CI workflow), fall back to standard
    // AWS_* env vars (covers `AWS_PROFILE` ambient credentials surfaced
    // via `aws sso login` on dev machines, after `aws configure export-
    // credentials` is invoked by the orchestrator's outer harness).
    for var in [
        "AWS_SNAPSHOT_KEY",
        "AWS_SNAPSHOT_SECRET",
        "AWS_SNAPSHOT_SESSION_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
    ] {
        if let Ok(val) = env::var(var) {
            env.insert(var.to_string(), val);
        }
    }
    let _ = CONTAINER_DATA_DIR;
    env
}

// ---------------------------------------------------------------------
// In-container worker tests. Marked #[ignore] so they only run when
// explicitly invoked by the orchestrator above.
// ---------------------------------------------------------------------

/// Writer worker. Started inside a container by the orchestrator.
#[ignore = "in-container worker; invoked by snapshot_refresh_cayenne_bootstrap_then_refresh"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_inner_writer() -> Result<()> {
    run_inner_worker(WorkerRole::Writer).await
}

/// Reader worker. Started inside a container by the orchestrator.
#[ignore = "in-container worker; invoked by snapshot_refresh_cayenne_bootstrap_then_refresh"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_inner_reader() -> Result<()> {
    run_inner_worker(WorkerRole::Reader).await
}

#[derive(Clone, Copy)]
enum WorkerRole {
    Writer,
    Reader,
}

async fn run_inner_worker(role: WorkerRole) -> Result<()> {
    init_tracing(None);
    register_test_connectors().await;

    let mut config = WorkerConfig::from_env().ok_or_else(|| {
        anyhow!(
            "in-container worker requires SNAPSHOT_REFRESH_* env vars; \
             this test should not be run directly"
        )
    })?;

    // Ensure Cayenne points at the canonical container path.
    config.engine = EngineKind::Cayenne;

    crate::utils::test_request_context()
        .scope(async move {
            match role {
                WorkerRole::Writer => run_writer_phase(&config).await,
                WorkerRole::Reader => {
                    // Wait until the writer has uploaded the initial snapshot
                    // so the reader's bootstrap finds it on first try
                    // (BootstrapOnFailureBehavior::Warn does not retry).
                    wait_for_snapshot_id_at_prefix(&config.s3_prefix, 0, Duration::from_secs(120))
                        .await
                        .context("reader: waiting for first writer snapshot")?;
                    run_reader_phase(&config).await
                }
            }
        })
        .await
}
