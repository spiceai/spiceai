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

//! Datasets that read acceleration snapshots from S3: `from: s3://…` with
//! `file_format: snapshot`.
//!
//! A writer runtime publishes snapshots of accelerated datasets to one bucket. The
//! test copies the snapshot prefix to a second bucket with identical keys, the way S3
//! replication does (#14425), and a reader runtime whose datasets name only that
//! location and `file_format: snapshot` serves each dataset in the engine that created
//! its snapshots, then follows the newer snapshots the writer publishes. The object
//! store is a local rustfs container, as in `s3_location_pruning`.
//!
//! A snapshot dataset keeps its local copy under the working directory's
//! `.spice/data`, so each test names its datasets uniquely and removes their copies.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use runtime::{Runtime, status::ComponentStatus};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode, SnapshotBehavior, SnapshotsCreationPolicy},
    component::{
        dataset::Dataset,
        snapshot::{BootstrapOnFailureBehavior, Snapshots},
    },
    param::Params,
};
use tempfile::TempDir;

use crate::{
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{run_query, runtime_ready_check, test_request_context},
};

const ACCESS_KEY: &str = "spiceadmin";
const SECRET_KEY: &str = "spiceintegrationsecret";
const WRITER_BUCKET: &str = "writer";
const READER_BUCKET: &str = "reader";

/// The rustfs container one test runs against. Each test has its own, on its own port,
/// so the tests can run in parallel.
#[derive(Clone, Copy)]
struct Rustfs {
    name: &'static str,
    port: u16,
}

const REPLICATED: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_replicated",
    port: 19124,
};
const FIRST_SNAPSHOT: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_first",
    port: 19125,
};

const INITIAL_CSV: &str = "id,name\n1,alpha\n2,bravo\n3,charlie\n";
const GROWN_CSV: &str = "id,name\n1,alpha\n2,bravo\n3,charlie\n4,delta\n5,echo\n";

impl Rustfs {
    fn endpoint(self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// A name unique to one test run, for datasets and snapshot prefixes.
fn unique(name: &str) -> String {
    format!(
        "{name}_{}",
        &uuid::Uuid::now_v7().simple().to_string()[20..]
    )
}

/// The S3 params that reach the rustfs container, as a dataset or `snapshots` block
/// spells them.
fn s3_params(rustfs: Rustfs) -> HashMap<String, String> {
    HashMap::from([
        ("s3_endpoint".to_string(), rustfs.endpoint()),
        ("s3_region".to_string(), "us-east-1".to_string()),
        ("s3_auth".to_string(), "key".to_string()),
        ("s3_key".to_string(), ACCESS_KEY.to_string()),
        ("s3_secret".to_string(), SECRET_KEY.to_string()),
        ("allow_http".to_string(), "true".to_string()),
    ])
}

async fn start_rustfs(rustfs: Rustfs) -> Result<RunningContainer<'static>> {
    use bollard::secret::HealthConfig;

    let container = ContainerRunnerBuilder::new(rustfs.name)
        .image("rustfs/rustfs:latest".to_string())
        .add_port_binding(9000, rustfs.port)
        .add_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .add_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .command(["/data"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 9000 || exit 1".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(20),
            start_period: Some(1_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(1)))
        .await?;
    wait_for_tcp_port("127.0.0.1", rustfs.port, Duration::from_secs(30)).await?;

    for bucket in [WRITER_BUCKET, READER_BUCKET] {
        create_bucket(rustfs, bucket).await?;
    }
    Ok(container)
}

async fn create_bucket(rustfs: Rustfs, bucket: &str) -> Result<()> {
    use aws_sdk_s3::config::{Credentials, Region};

    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .region(Region::new("us-east-1"))
        .endpoint_url(rustfs.endpoint())
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);

    // The container accepts connections before its S3 API answers.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let err = match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => return Ok(()),
            Err(err) => format!("{err:?}"),
        };
        if err.contains("BucketAlreadyOwnedByYou") || err.contains("BucketAlreadyExists") {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("failed to create bucket '{bucket}': {err}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn bucket_store(rustfs: Rustfs, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_endpoint(rustfs.endpoint())
            .with_allow_http(true)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .build()?,
    ))
}

/// Copies the writer's snapshot `prefix` to the reader bucket under the same keys, the
/// way S3 replication does, the metadata last so it never names a snapshot the copy
/// does not have yet.
async fn replicate_snapshots(rustfs: Rustfs, prefix: &str) -> Result<()> {
    let source = bucket_store(rustfs, WRITER_BUCKET)?;
    let target = bucket_store(rustfs, READER_BUCKET)?;

    let mut listed = source.list(Some(&ObjectPath::from(prefix)));
    let mut keys = Vec::new();
    while let Some(meta) = listed.next().await {
        keys.push(meta.context("listing the writer's snapshots")?.location);
    }
    keys.sort_by_key(|key| key.as_ref().ends_with("metadata.json"));

    for key in keys {
        let bytes = source.get(&key).await?.bytes().await?;
        target.put(&key, bytes.into()).await?;
    }
    Ok(())
}

/// The writer's `current-snapshot-id` for `dataset`, once it has published one.
async fn writer_snapshot_id(rustfs: Rustfs, prefix: &str, dataset: &str) -> Result<Option<u64>> {
    let store = bucket_store(rustfs, WRITER_BUCKET)?;
    let path = ObjectPath::from(format!("{prefix}/metadata.json"));
    let bytes = match store.get(&path).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let metadata: serde_json::Value = serde_json::from_slice(&bytes)?;
    Ok(metadata
        .get(dataset)
        .and_then(|entry| entry.get("current-snapshot-id"))
        .and_then(serde_json::Value::as_u64))
}

async fn wait_for_writer_snapshot(
    rustfs: Rustfs,
    prefix: &str,
    dataset: &str,
    minimum_id: u64,
) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut latest = None;
    while Instant::now() < deadline {
        latest = writer_snapshot_id(rustfs, prefix, dataset)
            .await
            .ok()
            .flatten();
        if let Some(id) = latest.filter(|id| *id >= minimum_id) {
            return Ok(id);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!(
        "the writer published no snapshot {minimum_id}+ of '{dataset}'; latest: {latest:?}"
    ))
}

fn writer_snapshots(rustfs: Rustfs, prefix: &str) -> Snapshots {
    Snapshots {
        enabled: true,
        location: Some(format!("s3://{WRITER_BUCKET}/{prefix}/")),
        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::default(),
        params: Some(Params::from_string_map(s3_params(rustfs))),
    }
}

/// An accelerated dataset over a local CSV file that publishes a snapshot after every
/// refresh.
fn writer_dataset(name: &str, csv: &Path, engine: &str, dir: &Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", csv.display()), name);
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    let engine_params = match engine {
        "duckdb" => HashMap::from([(
            "duckdb_file".to_string(),
            dir.join(format!("{name}.duckdb")).display().to_string(),
        )]),
        _ => HashMap::from([
            (
                "cayenne_file_path".to_string(),
                dir.join(format!("{name}_data")).display().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join(format!("{name}_metadata")).display().to_string(),
            ),
        ]),
    };
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: Mode::File,
        params: Some(Params::from_string_map(engine_params)),
        refresh_mode: Some(RefreshMode::Full),
        refresh_check_interval: Some("1s".to_string()),
        snapshots: SnapshotBehavior::CreateOnly,
        snapshots_creation_policy: SnapshotsCreationPolicy::OnChange,
        ..Acceleration::default()
    });
    dataset
}

/// A dataset that names only its snapshots' location, `file_format: snapshot`, how to
/// reach the bucket, and how often to check for a newer snapshot. It names no engine.
fn reader_dataset(rustfs: Rustfs, name: &str, prefix: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{READER_BUCKET}/{prefix}/"), name);
    let mut params = s3_params(rustfs);
    params.insert("file_format".to_string(), "snapshot".to_string());
    dataset.params = Some(Params::from_string_map(params));
    dataset.acceleration = Some(Acceleration {
        refresh_check_interval: Some("1s".to_string()),
        ..Acceleration::default()
    });
    dataset
}

/// The entries of the working directory's `.spice/data` that belong to `dataset`: the
/// `<dataset>-<location hash>.<engine>` file of a single-file engine, and Cayenne's
/// `<dataset>` data directory.
fn local_copies(dataset: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(Path::new(".spice").join("data")) else {
        return Vec::new();
    };
    let prefix = format!("{dataset}-");
    let mut copies: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == dataset || name.starts_with(&prefix))
        })
        .collect();
    copies.sort();
    copies
}

fn remove_local_copies(datasets: &[&str]) {
    for path in datasets.iter().flat_map(|dataset| local_copies(dataset)) {
        let _ = std::fs::remove_dir_all(&path).or_else(|_| std::fs::remove_file(&path));
    }
}

async fn rows(rt: &Arc<Runtime>, table: &str) -> Result<Vec<String>> {
    let batches: Vec<RecordBatch> =
        run_query(rt, &format!("SELECT id, name FROM {table} ORDER BY id")).await?;
    let formatted = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    Ok(formatted
        .lines()
        .filter(|line| line.starts_with("| ") && !line.contains(" id "))
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect())
}

async fn wait_for_rows(rt: &Arc<Runtime>, table: &str, expected: usize) -> Result<()> {
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        seen = rows(rt, table).await.unwrap_or_default();
        if seen.len() == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!(
        "'{table}' should serve {expected} rows, served {}: {seen:?}",
        seen.len()
    ))
}

async fn load(rt: &Arc<Runtime>) -> Result<()> {
    tokio::time::timeout(Duration::from_mins(2), Arc::clone(rt).load_components())
        .await
        .map_err(|_| anyhow!("timed out loading the runtime's components"))?;
    runtime_ready_check(rt).await;
    Ok(())
}

/// A reader configured with nothing but the location and `file_format: snapshot`
/// serves a Cayenne and a `DuckDB` snapshot, each in the engine that created it, from
/// a copy of the writer's snapshots in another bucket, and follows newer snapshots.
#[tokio::test]
async fn reads_replicated_snapshots_in_the_engine_that_created_them() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    let orders = unique("orders");
    test_request_context()
        .scope(async {
            let container = start_rustfs(REPLICATED).await?;
            let result = replicated_snapshots_scenario(REPLICATED, &modules, &orders).await;
            remove_local_copies(&[&modules, &orders]);
            container.remove().await?;
            result
        })
        .await
}

async fn replicated_snapshots_scenario(rustfs: Rustfs, modules: &str, orders: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let modules_csv = dir.path().join("modules.csv");
    let orders_csv = dir.path().join("orders.csv");
    std::fs::write(&modules_csv, INITIAL_CSV)?;
    std::fs::write(&orders_csv, INITIAL_CSV)?;

    let writer_app = AppBuilder::new("snapshot_source_writer")
        .with_snapshots(writer_snapshots(rustfs, &prefix))
        .with_dataset(writer_dataset(modules, &modules_csv, "cayenne", dir.path()))
        .with_dataset(writer_dataset(orders, &orders_csv, "duckdb", dir.path()))
        .build();
    let reader_app = AppBuilder::new("snapshot_source_reader")
        .with_dataset(reader_dataset(rustfs, modules, &prefix))
        .with_dataset(reader_dataset(rustfs, orders, &prefix))
        .build();

    let writer = Arc::new(Runtime::builder().with_app(writer_app).build().await);
    load(&writer).await?;
    wait_for_writer_snapshot(rustfs, &prefix, modules, 0).await?;
    wait_for_writer_snapshot(rustfs, &prefix, orders, 0).await?;
    replicate_snapshots(rustfs, &prefix).await?;

    let reader = Arc::new(Runtime::builder().with_app(reader_app).build().await);
    load(&reader).await?;

    let expected = vec!["| 1 | alpha |", "| 2 | bravo |", "| 3 | charlie |"];
    for table in [modules, orders] {
        assert_eq!(rows(&reader, table).await?, expected, "reader '{table}'");
        assert_eq!(
            rows(&reader, table).await?,
            rows(&writer, table).await?,
            "'{table}' serves the writer's rows"
        );
    }
    // Each snapshot is restored into the engine that created it: Cayenne data in the
    // dataset's data directory, DuckDB in a file named for the dataset and location.
    let modules_copies = local_copies(modules);
    assert!(
        modules_copies
            .iter()
            .any(|path| path.is_dir() && path.ends_with(modules)),
        "{modules_copies:?}"
    );
    let orders_copies = local_copies(orders);
    assert!(
        orders_copies.iter().any(|path| path
            .extension()
            .is_some_and(|extension| extension == "duckdb")),
        "{orders_copies:?}"
    );

    let insert = run_query(
        &reader,
        &format!("INSERT INTO {modules} VALUES (9, 'zulu')"),
    )
    .await;
    assert!(insert.is_err(), "a snapshot dataset is read-only");

    // The writer publishes newer snapshots. Copied continuously, as replication copies
    // them, the reader follows: a writer can publish a snapshot of unchanged data on
    // every refresh, so no single snapshot id says when the new rows have been published.
    std::fs::write(&modules_csv, GROWN_CSV)?;
    std::fs::write(&orders_csv, GROWN_CSV)?;
    for table in [modules, orders] {
        wait_for_rows(&writer, table, 5).await?;
    }
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        replicate_snapshots(rustfs, &prefix).await?;
        let mut served = Vec::new();
        for table in [modules, orders] {
            served.push(rows(&reader, table).await?.len());
        }
        if served == [5, 5] {
            break;
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "the reader should follow the writer to 5 rows per table, served {served:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for table in [modules, orders] {
        assert_eq!(
            rows(&reader, table).await?,
            rows(&writer, table).await?,
            "'{table}' serves the writer's newer rows"
        );
    }

    reader.shutdown().await;
    writer.shutdown().await;
    Ok(())
}

/// A reader that starts before any snapshot is published waits for the first one,
/// reporting why it cannot be queried, and loads it once it appears.
#[tokio::test]
async fn waits_for_the_first_snapshot_to_be_published() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(FIRST_SNAPSHOT).await?;
            let result = first_snapshot_scenario(FIRST_SNAPSHOT, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn first_snapshot_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let modules_csv = dir.path().join("modules.csv");
    std::fs::write(&modules_csv, INITIAL_CSV)?;

    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_early_reader")
                    .with_dataset(reader_dataset(rustfs, modules, &prefix))
                    .build(),
            )
            .build()
            .await,
    );
    // The load cannot finish until a snapshot exists, so it runs alongside the writer.
    let reader_load = tokio::spawn(Arc::clone(&reader).load_components());

    let table = datafusion::sql::TableReference::bare(modules);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut status = None;
    while Instant::now() < deadline {
        status = reader.status().get_dataset_status(&table);
        if status
            .as_ref()
            .and_then(ComponentStatus::error_message)
            .is_some_and(|message| message.contains("has no snapshot to load yet"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let message = status
        .as_ref()
        .and_then(ComponentStatus::error_message)
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains(&format!(
            "Dataset '{modules}' has no snapshot to load yet, so it cannot be queried until one is published: 's3://{READER_BUCKET}/{prefix}/metadata.json' does not exist"
        )),
        "the reader reports that it waits for a snapshot, got {status:?}"
    );

    let writer = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_late_writer")
                    .with_snapshots(writer_snapshots(rustfs, &prefix))
                    .with_dataset(writer_dataset(modules, &modules_csv, "cayenne", dir.path()))
                    .build(),
            )
            .build()
            .await,
    );
    load(&writer).await?;
    wait_for_writer_snapshot(rustfs, &prefix, modules, 0).await?;
    replicate_snapshots(rustfs, &prefix).await?;

    tokio::time::timeout(Duration::from_mins(2), reader_load)
        .await
        .map_err(|_| anyhow!("the reader did not load the published snapshot"))??;
    runtime_ready_check(&reader).await;
    assert_eq!(rows(&reader, modules).await?, rows(&writer, modules).await?);

    reader.shutdown().await;
    writer.shutdown().await;
    Ok(())
}

const RELOAD: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_reload",
    port: 19126,
};
const MOVED: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_moved",
    port: 19127,
};

/// Publishes a `DuckDB` snapshot of `modules` to `prefix` and copies it to the reader
/// bucket, returning the writer.
async fn publish_and_replicate(
    rustfs: Rustfs,
    prefix: &str,
    modules: &str,
    dir: &Path,
) -> Result<Arc<Runtime>> {
    let modules_csv = dir.join("modules.csv");
    std::fs::write(&modules_csv, INITIAL_CSV)?;
    let writer = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_writer")
                    .with_snapshots(writer_snapshots(rustfs, prefix))
                    .with_dataset(writer_dataset(modules, &modules_csv, "duckdb", dir))
                    .build(),
            )
            .build()
            .await,
    );
    load(&writer).await?;
    wait_for_writer_snapshot(rustfs, prefix, modules, 0).await?;
    replicate_snapshots(rustfs, prefix).await?;
    Ok(writer)
}

/// A dataset that queries without acceleration, so a reader has a component to be
/// ready with before a reload adds its snapshot dataset.
fn plain_dataset(dir: &Path) -> Result<Dataset> {
    let csv = dir.join("plain.csv");
    std::fs::write(&csv, INITIAL_CSV)?;
    let mut dataset = Dataset::new(format!("file://{}", csv.display()), "plain");
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    Ok(dataset)
}

/// A snapshot dataset a reload adds loads. The reload starts the dataset's load before
/// it installs its app, so the load must resolve the dataset from the app it was built
/// from.
#[tokio::test]
async fn loads_a_snapshot_dataset_that_a_reload_adds() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(RELOAD).await?;
            let result = reload_scenario(RELOAD, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn reload_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let writer = publish_and_replicate(rustfs, &prefix, modules, dir.path()).await?;

    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_reloaded_reader")
                    .with_dataset(plain_dataset(dir.path())?)
                    .build(),
            )
            .build()
            .await,
    );
    load(&reader).await?;

    let reloaded = AppBuilder::new("snapshot_source_reloaded_reader")
        .with_dataset(plain_dataset(dir.path())?)
        .with_dataset(reader_dataset(rustfs, modules, &prefix))
        .build();
    assert!(Arc::clone(&reader).apply_app(Arc::new(reloaded)).await);

    wait_for_rows(&reader, modules, 3).await?;
    assert_eq!(rows(&reader, modules).await?, rows(&writer, modules).await?);

    reader.shutdown().await;
    writer.shutdown().await;
    Ok(())
}

/// A dataset pointed at another snapshot location never serves the rows it restored
/// from the location it read before: here the new location's snapshot cannot be
/// downloaded, so any rows the dataset serves would be the previous location's.
#[tokio::test]
async fn a_moved_dataset_never_serves_the_previous_locations_rows() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(MOVED).await?;
            let result = moved_scenario(MOVED, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn moved_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let writer = publish_and_replicate(rustfs, &prefix, modules, dir.path()).await?;
    writer.shutdown().await;

    // The first location, restored and served.
    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_first_location")
                    .with_dataset(reader_dataset(rustfs, modules, &prefix))
                    .build(),
            )
            .build()
            .await,
    );
    load(&reader).await?;
    assert_eq!(rows(&reader, modules).await?.len(), 3);
    reader.shutdown().await;

    // The second location lists a snapshot whose file is missing.
    let moved = unique("snapshots");
    let reader_bucket = bucket_store(rustfs, READER_BUCKET)?;
    let metadata = reader_bucket
        .get(&ObjectPath::from(format!("{prefix}/metadata.json")))
        .await?
        .bytes()
        .await?;
    reader_bucket
        .put(
            &ObjectPath::from(format!("{moved}/metadata.json")),
            metadata.into(),
        )
        .await?;

    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_second_location")
                    .with_dataset(reader_dataset(rustfs, modules, &moved))
                    .build(),
            )
            .build()
            .await,
    );
    let load = tokio::spawn(Arc::clone(&reader).load_components());

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(served) = rows(&reader, modules).await {
            assert!(
                served.is_empty(),
                "the dataset now reads '{moved}', whose snapshot is missing, yet it served {served:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    load.abort();
    reader.shutdown().await;
    Ok(())
}

const PROJECTED: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_projected",
    port: 19128,
};

/// A publisher whose `refresh_sql` stores only some of its columns records every column
/// in its snapshots' metadata, so no schema describes both its stored table and its
/// newer snapshots. The dataset is refused, by name, rather than registered with a
/// schema its queries fail on.
#[tokio::test]
async fn refuses_snapshots_of_a_publisher_that_stores_some_columns() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(PROJECTED).await?;
            let result = projected_scenario(PROJECTED, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn projected_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let modules_csv = dir.path().join("modules.csv");
    std::fs::write(&modules_csv, INITIAL_CSV)?;
    let mut projected = writer_dataset(modules, &modules_csv, "duckdb", dir.path());
    if let Some(acceleration) = projected.acceleration.as_mut() {
        acceleration.refresh_sql = Some(format!("SELECT id FROM {modules}"));
    }
    let writer = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_projected_writer")
                    .with_snapshots(writer_snapshots(rustfs, &prefix))
                    .with_dataset(projected)
                    .build(),
            )
            .build()
            .await,
    );
    load(&writer).await?;
    wait_for_writer_snapshot(rustfs, &prefix, modules, 0).await?;
    replicate_snapshots(rustfs, &prefix).await?;
    writer.shutdown().await;

    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_projected_reader")
                    .with_dataset(reader_dataset(rustfs, modules, &prefix))
                    .build(),
            )
            .build()
            .await,
    );
    let load = tokio::spawn(Arc::clone(&reader).load_components());

    let table = datafusion::sql::TableReference::bare(modules);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut status = None;
    while Instant::now() < deadline {
        status = reader.status().get_dataset_status(&table);
        if status
            .as_ref()
            .and_then(ComponentStatus::error_message)
            .is_some_and(|message| message.contains("stores only some of its source's columns"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let message = status
        .as_ref()
        .and_then(ComponentStatus::error_message)
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains(&format!(
            "Dataset '{modules}' reads snapshots of a dataset whose `refresh_sql` stores only some of its source's columns (SELECT id FROM {modules})"
        )),
        "the dataset is refused, naming the publisher's projection, got {status:?}"
    );

    load.abort();
    reader.shutdown().await;
    Ok(())
}

const CUSTOM_PATHS: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_custom_paths",
    port: 19129,
};

/// A Cayenne snapshot dataset that sets `cayenne_file_path` and `cayenne_metadata_dir`
/// restores its copy there, not under `.spice/data`.
#[tokio::test]
async fn restores_cayenne_snapshots_to_the_paths_the_dataset_sets() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(CUSTOM_PATHS).await?;
            let result = custom_paths_scenario(CUSTOM_PATHS, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn custom_paths_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let modules_csv = dir.path().join("modules.csv");
    std::fs::write(&modules_csv, INITIAL_CSV)?;
    let writer = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_custom_paths_writer")
                    .with_snapshots(writer_snapshots(rustfs, &prefix))
                    .with_dataset(writer_dataset(modules, &modules_csv, "cayenne", dir.path()))
                    .build(),
            )
            .build()
            .await,
    );
    load(&writer).await?;
    wait_for_writer_snapshot(rustfs, &prefix, modules, 0).await?;
    replicate_snapshots(rustfs, &prefix).await?;

    let data_dir = dir.path().join("reader").join(modules);
    let metadata_dir = dir.path().join("reader").join("metadata");
    let mut dataset = reader_dataset(rustfs, modules, &prefix);
    if let Some(acceleration) = dataset.acceleration.as_mut() {
        acceleration.params = Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                data_dir.display().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            ),
        ])));
    }
    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_custom_paths_reader")
                    .with_dataset(dataset)
                    .build(),
            )
            .build()
            .await,
    );
    load(&reader).await?;

    wait_for_rows(&reader, modules, 3).await?;
    assert_eq!(rows(&reader, modules).await?, rows(&writer, modules).await?);
    assert!(
        std::fs::read_dir(&data_dir)?.next().is_some(),
        "the copy is restored under `cayenne_file_path` ({})",
        data_dir.display()
    );
    assert!(
        metadata_dir.join("cayenne.db").is_file(),
        "the metastore is restored under `cayenne_metadata_dir` ({})",
        metadata_dir.display()
    );
    assert_eq!(
        local_copies(modules),
        Vec::<PathBuf>::new(),
        "nothing is restored under `.spice/data`"
    );

    reader.shutdown().await;
    writer.shutdown().await;
    Ok(())
}

const OTHER_ENGINE_PATHS: Rustfs = Rustfs {
    name: "spice_test_rustfs_snapshot_source_other_engine_paths",
    port: 19130,
};

/// A snapshot dataset that sets a Cayenne path but reads another engine's snapshots is
/// refused, by name, rather than silently keeping its copy under `.spice/data`.
#[tokio::test]
async fn refuses_cayenne_paths_for_another_engines_snapshots() -> Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=info,info"));
    let modules = unique("modules");
    test_request_context()
        .scope(async {
            let container = start_rustfs(OTHER_ENGINE_PATHS).await?;
            let result = other_engine_paths_scenario(OTHER_ENGINE_PATHS, &modules).await;
            remove_local_copies(&[&modules]);
            container.remove().await?;
            result
        })
        .await
}

async fn other_engine_paths_scenario(rustfs: Rustfs, modules: &str) -> Result<()> {
    let prefix = unique("snapshots");
    let dir = TempDir::new()?;
    let writer = publish_and_replicate(rustfs, &prefix, modules, dir.path()).await?;
    writer.shutdown().await;

    let mut dataset = reader_dataset(rustfs, modules, &prefix);
    if let Some(acceleration) = dataset.acceleration.as_mut() {
        acceleration.params = Some(Params::from_string_map(HashMap::from([(
            "cayenne_file_path".to_string(),
            dir.path().join("reader").display().to_string(),
        )])));
    }
    let reader = Arc::new(
        Runtime::builder()
            .with_app(
                AppBuilder::new("snapshot_source_other_engine_paths_reader")
                    .with_dataset(dataset)
                    .build(),
            )
            .build()
            .await,
    );
    let load = tokio::spawn(Arc::clone(&reader).load_components());

    let table = datafusion::sql::TableReference::bare(modules);
    let expected = format!(
        "Dataset '{modules}' reads snapshots from 's3://{READER_BUCKET}/{prefix}/' that were created with the 'duckdb' engine, so `acceleration.params.cayenne_file_path`, which sets where a Cayenne copy is kept, does not apply"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut status = None;
    while Instant::now() < deadline {
        status = reader.status().get_dataset_status(&table);
        if status
            .as_ref()
            .and_then(ComponentStatus::error_message)
            .is_some_and(|message| message.contains(&expected))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let message = status
        .as_ref()
        .and_then(ComponentStatus::error_message)
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains(&expected),
        "the dataset is refused, naming the param, got {status:?}"
    );
    assert_eq!(
        local_copies(modules),
        Vec::<PathBuf>::new(),
        "nothing is restored under `.spice/data`"
    );

    load.abort();
    reader.shutdown().await;
    Ok(())
}
