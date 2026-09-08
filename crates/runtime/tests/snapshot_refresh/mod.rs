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

//! Shared infrastructure for `refresh_mode: snapshot` integration tests.
//!
//! The shared snapshot store is backed by a real S3 bucket, mirroring the
//! existing `snapshot_integration` tests, because the local filesystem
//! `object_store` backend does not implement conditional `PutMode::Update`
//! and therefore cannot host the per-snapshot `metadata.json` rewrites that
//! these tests need.
//!
//! Required environment for CI:
//!
//! * `AWS_SNAPSHOT_KEY` + `AWS_SNAPSHOT_SECRET`, **or**
//! * `AWS_PROFILE` configured with read/write access to the test bucket.
//!
//! Each test reserves its own UUID-prefixed key range under the shared
//! bucket and cleans up after itself.

use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use app::AppBuilder;
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use futures::StreamExt;
use object_store::ObjectStoreExt;
use object_store::{ClientOptions, ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use runtime::Runtime;
use spicepod::{
    acceleration::{
        Acceleration, Mode, OnConflictBehavior, RefreshMode, RefreshOnStartup, SnapshotBehavior,
        SnapshotsCreationPolicy,
    },
    component::{
        access::AccessMode,
        dataset::Dataset,
        snapshot::{BootstrapOnFailureBehavior, Snapshots},
    },
    param::Params,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{init_tracing, run_query};

#[cfg(not(windows))]
mod cayenne;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

const DATASET_NAME: &str = "trips";
const SNAPSHOT_BUCKET: &str = "spiceai-snapshot-integration-tests";
const SNAPSHOT_REGION: &str = "us-west-2";

/// Engine variants exercised by these tests. Each variant is enabled at
/// compile time only when the corresponding feature is on, mirroring the
/// `AccelerationEngine` enum in `runtime-acceleration`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EngineKind {
    Cayenne,
    #[cfg(feature = "duckdb")]
    DuckDB,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl EngineKind {
    fn engine_name(self) -> &'static str {
        match self {
            Self::Cayenne => "cayenne",
            #[cfg(feature = "duckdb")]
            Self::DuckDB => "duckdb",
            #[cfg(feature = "sqlite")]
            Self::Sqlite => "sqlite",
            #[cfg(feature = "turso")]
            Self::Turso => "turso",
        }
    }

    fn file_extension(self) -> &'static str {
        match self {
            Self::Cayenne => "cayenne",
            #[cfg(feature = "duckdb")]
            Self::DuckDB => "duckdb",
            #[cfg(feature = "sqlite")]
            Self::Sqlite => "sqlite",
            #[cfg(feature = "turso")]
            Self::Turso => "turso",
        }
    }
}

/// Per-test S3 prefix + an `ObjectStore` pointed at it for cleanup.
struct SnapshotS3Context {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl SnapshotS3Context {
    async fn new(test_name: &str) -> Result<Self> {
        let store = build_snapshot_store().await?;
        let prefix = format!("{test_name}/{}", Uuid::now_v7());
        Ok(Self { store, prefix })
    }

    fn location_uri(&self) -> String {
        format!(
            "s3://{SNAPSHOT_BUCKET}/{}/",
            self.prefix.trim_end_matches('/')
        )
    }

    async fn cleanup(&self) -> Result<()> {
        let base = ObjectPath::from(self.prefix.clone());
        let mut stream = self.store.list(Some(&base));
        let mut to_delete = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("listing snapshot bucket for cleanup")?;
            to_delete.push(meta.location);
        }
        for loc in to_delete {
            let _ = self.store.delete(&loc).await;
        }
        Ok(())
    }
}

async fn build_snapshot_store() -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(SNAPSHOT_BUCKET)
        .with_region(SNAPSHOT_REGION)
        .with_client_options(ClientOptions::default());

    if let (Ok(key), Ok(secret)) = (
        env::var("AWS_SNAPSHOT_KEY"),
        env::var("AWS_SNAPSHOT_SECRET"),
    ) {
        builder = builder
            .with_access_key_id(key)
            .with_secret_access_key(secret);
        if let Ok(token) = env::var("AWS_SNAPSHOT_SESSION_TOKEN") {
            builder = builder.with_token(token);
        }
    } else {
        let config = get_or_init_sdk_config()
            .await
            .map_err(|err| anyhow!("Failed to initialize AWS credentials: {err}"))?;
        let Some(config) = config else {
            return Err(anyhow!(
                "AWS credentials are required to run snapshot refresh integration tests. \
                 Provide AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET or configure AWS_PROFILE."
            ));
        };
        builder = builder.with_credentials(Arc::new(
            S3CredentialProvider::from_config(config.as_ref())
                .context("Loading AWS credentials from environment")?,
        ));
    }

    Ok(Arc::new(builder.build().context(
        "Building Amazon S3 object store client for snapshots",
    )?))
}

/// Holds everything a single integration test needs to drive a writer +
/// reader pair against a shared S3 snapshot store.
struct SnapshotRefreshFixture {
    s3: SnapshotS3Context,
    _temp_dir: TempDir,
    source_csv_path: PathBuf,
    writer_local_db: PathBuf,
    reader_local_db: PathBuf,
    engine: EngineKind,
}

impl SnapshotRefreshFixture {
    async fn new(test_name: &str, engine: EngineKind) -> Result<Self> {
        let temp_dir = TempDir::new().context("creating temp dir for snapshot refresh test")?;
        let source_csv_path = temp_dir.path().join("source.csv");
        let writer_local_db = temp_dir
            .path()
            .join(format!("writer.{}", engine.file_extension()));
        let reader_local_db = temp_dir
            .path()
            .join(format!("reader.{}", engine.file_extension()));
        let s3 = SnapshotS3Context::new(test_name).await?;
        Ok(Self {
            s3,
            _temp_dir: temp_dir,
            source_csv_path,
            writer_local_db,
            reader_local_db,
            engine,
        })
    }

    fn source_from_uri(&self) -> String {
        format!("file://{}", self.source_csv_path.display())
    }

    /// Atomically rewrite the source CSV.
    fn write_source(&self, csv: &str) -> Result<()> {
        let tmp = self.source_csv_path.with_extension("csv.tmp");
        std::fs::write(&tmp, csv).context("writing temp source csv")?;
        std::fs::rename(&tmp, &self.source_csv_path).context("renaming source csv into place")?;
        Ok(())
    }

    pub(crate) fn dataset_params() -> HashMap<String, String> {
        HashMap::from([
            ("file_format".to_string(), "csv".to_string()),
            ("csv_has_header".to_string(), "true".to_string()),
        ])
    }

    /// Engine-specific acceleration params that pin the on-disk location to
    /// `local_db_path`. Cayenne is directory-based and uses two distinct
    /// param names (`cayenne_file_path` for data, `cayenne_metadata_dir` for
    /// the catalog metastore), so we route to a sibling `metadata/` directory
    /// to keep writer and reader fully isolated. Other engines are
    /// single-file and use `<engine>_file`.
    fn engine_accel_params(&self, local_db_path: &std::path::Path) -> HashMap<String, String> {
        let mut params = HashMap::new();
        match self.engine {
            EngineKind::Cayenne => {
                params.insert(
                    "cayenne_file_path".to_string(),
                    local_db_path.to_string_lossy().into_owned(),
                );
                let metadata_dir = local_db_path.with_extension("metadata");
                params.insert(
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.to_string_lossy().into_owned(),
                );
            }
            #[cfg(feature = "duckdb")]
            EngineKind::DuckDB => {
                params.insert(
                    "duckdb_file".to_string(),
                    local_db_path.to_string_lossy().into_owned(),
                );
            }
            #[cfg(feature = "sqlite")]
            EngineKind::Sqlite => {
                params.insert(
                    "sqlite_file".to_string(),
                    local_db_path.to_string_lossy().into_owned(),
                );
            }
            #[cfg(feature = "turso")]
            EngineKind::Turso => {
                params.insert(
                    "turso_file".to_string(),
                    local_db_path.to_string_lossy().into_owned(),
                );
            }
        }
        params
    }

    /// Build the writer dataset: full refresh + create-on-change snapshots.
    fn writer_dataset(&self) -> Dataset {
        let accel_params = self.engine_accel_params(&self.writer_local_db);

        let mut dataset = Dataset::new(self.source_from_uri(), DATASET_NAME);
        dataset.params = Some(Params::from_string_map(Self::dataset_params()));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            mode: Mode::File,
            engine: Some(self.engine.engine_name().to_string()),
            params: Some(Params::from_string_map(accel_params)),
            refresh_mode: Some(RefreshMode::Full),
            // Drive refreshes (and therefore snapshot creation) quickly so
            // the test stays in the few-seconds range.
            refresh_check_interval: Some("1s".to_string()),
            refresh_on_startup: RefreshOnStartup::Auto,
            snapshots: SnapshotBehavior::Enabled,
            snapshots_creation_policy: SnapshotsCreationPolicy::OnChange,
            ..Acceleration::default()
        });
        dataset
    }

    /// Build the reader dataset: snapshot refresh + bootstrap from the
    /// shared snapshot store.
    fn reader_dataset(&self) -> Dataset {
        let accel_params = self.engine_accel_params(&self.reader_local_db);

        let mut dataset = Dataset::new(self.source_from_uri(), DATASET_NAME);
        dataset.params = Some(Params::from_string_map(Self::dataset_params()));
        // Mark the reader dataset as `read_write` with a non-empty
        // `on_conflict` map so the runtime's access gate accepts the dataset
        // (read_write requires either replication or on_conflict). The
        // on_conflict configuration itself is never exercised because the
        // refresh_mode: snapshot rejection inside `AcceleratedTable::insert_into`
        // fires before any write reaches the accelerator.
        dataset.access = AccessMode::ReadWrite;
        let mut on_conflict = HashMap::new();
        on_conflict.insert("id".to_string(), OnConflictBehavior::Upsert);
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            mode: Mode::File,
            engine: Some(self.engine.engine_name().to_string()),
            params: Some(Params::from_string_map(accel_params)),
            refresh_mode: Some(RefreshMode::Snapshot),
            // Poll often so the test does not have to wait for the default
            // 1-minute interval to detect the new snapshot.
            refresh_check_interval: Some("1s".to_string()),
            refresh_on_startup: RefreshOnStartup::Auto,
            snapshots: SnapshotBehavior::Enabled,
            primary_key: Some("id".to_string()),
            on_conflict,
            ..Acceleration::default()
        });
        dataset
    }

    fn snapshots_config(&self) -> Snapshots {
        let mut params = HashMap::from([("s3_region".to_string(), SNAPSHOT_REGION.to_string())]);
        if env::var("AWS_PROFILE").is_ok() {
            params.insert("s3_auth".to_string(), "iam_role".to_string());
        } else {
            params.insert("s3_auth".to_string(), "key".to_string());
            params.insert(
                "s3_key".to_string(),
                "${secrets:AWS_SNAPSHOT_KEY}".to_string(),
            );
            params.insert(
                "s3_secret".to_string(),
                "${secrets:AWS_SNAPSHOT_SECRET}".to_string(),
            );
        }
        Snapshots {
            enabled: true,
            location: Some(self.s3.location_uri()),
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
            params: Some(Params::from_string_map(params)),
        }
    }

    /// Read the current `current-snapshot-id` for this dataset from the
    /// shared metadata file. Returns `None` if metadata is not yet written.
    async fn current_snapshot_id(&self) -> Result<Option<i64>> {
        let metadata_path = ObjectPath::from(format!("{}/metadata.json", self.s3.prefix));
        let bytes = match self.s3.store.get(&metadata_path).await {
            Ok(get) => get
                .bytes()
                .await
                .context("reading metadata.json bytes from snapshot store")?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(anyhow::Error::from(e).context("getting metadata.json")),
        };
        let metadata: serde_json::Value =
            serde_json::from_slice(&bytes).context("parsing metadata.json")?;
        let Some(dataset_entry) = metadata.get(DATASET_NAME) else {
            return Ok(None);
        };
        Ok(dataset_entry
            .get("current-snapshot-id")
            .and_then(serde_json::Value::as_i64))
    }

    async fn wait_for_snapshot_id(&self, minimum_id: i64, max_wait: Duration) -> Result<i64> {
        let deadline = Instant::now() + max_wait;
        loop {
            if let Some(id) = self.current_snapshot_id().await?
                && id >= minimum_id
            {
                return Ok(id);
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for snapshot id >= {minimum_id} in s3://{SNAPSHOT_BUCKET}/{}",
                    self.s3.prefix
                ));
            }
            sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn load_runtime(rt: Arc<Runtime>) -> Result<()> {
    timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
        .await
        .map_err(|_| anyhow!("Timed out waiting for runtime components to load"))?;
    runtime_ready_check(rt.as_ref()).await;
    Ok(())
}

/// Run a query and return the total number of rows across all batches. We
/// avoid `SELECT count(*)` so the assertion is robust across engines that
/// reject or pushdown-translate the count differently (Turso in particular
/// rejects the federation-pushed-down count form).
async fn count_rows(rt: &Arc<Runtime>, table: &str) -> Result<usize> {
    let batches = run_query(rt, &format!("SELECT id FROM {table}"))
        .await
        .with_context(|| format!("counting rows in {table}"))?;
    Ok(batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum())
}

/// Initial source CSV with three rows.
const INITIAL_CSV: &str = "\
id,name,score
1,alpha,10
2,bravo,20
3,charlie,30
";

/// Mutated CSV with five rows, distinct content from the initial one. Both
/// sets share the same schema so snapshot reload is allowed.
const MUTATED_CSV: &str = "\
id,name,score
1,alpha,10
2,bravo,20
3,charlie,30
4,delta,40
5,echo,50
";

/// End-to-end scenario: writer creates snapshots; reader bootstraps and
/// follows. Each per-engine `#[tokio::test]` calls this with its engine.
async fn run_bootstrap_then_refresh_cycle(test_name: &str, engine: EngineKind) -> Result<()> {
    init_tracing(None);
    register_test_connectors().await;

    test_request_context()
        .scope(async move {
            let fixture = SnapshotRefreshFixture::new(test_name, engine).await?;
            // Always attempt cleanup, even on test failure.
            let result = run_inner(&fixture).await;
            if let Err(cleanup_err) = fixture.s3.cleanup().await {
                tracing::warn!(
                    test = test_name,
                    error = %cleanup_err,
                    "snapshot cleanup encountered errors (test result preserved)"
                );
            }
            result
        })
        .await
}

async fn run_inner(fixture: &SnapshotRefreshFixture) -> Result<()> {
    // Drive the in-process variant by calling the writer phase up to the
    // first snapshot, then starting the reader and letting the writer
    // publish the mutated snapshot. The two helpers below are also called
    // (separately, in two different processes) by the dockerized Cayenne
    // orchestrator below.
    fixture.write_source(INITIAL_CSV)?;

    // ---------------------- start writer ----------------------
    let writer_app = AppBuilder::new(format!("snapshot_writer_{}", fixture.engine.engine_name()))
        .with_snapshots(fixture.snapshots_config())
        .with_dataset(fixture.writer_dataset())
        .build();
    let writer = Arc::new(Runtime::builder().with_app(writer_app).build().await);
    load_runtime(Arc::clone(&writer)).await?;

    let writer_initial = count_rows(&writer, "trips").await?;
    if writer_initial != 3 {
        return Err(anyhow!(
            "writer should serve the initial 3 rows, got {writer_initial}"
        ));
    }

    let first_id = fixture
        .wait_for_snapshot_id(0, Duration::from_mins(1))
        .await
        .context("waiting for writer to produce initial snapshot")?;
    if first_id != 0 {
        return Err(anyhow!("first snapshot should have id 0, got {first_id}"));
    }

    // ---------------------- start reader ----------------------
    let reader_app = AppBuilder::new(format!("snapshot_reader_{}", fixture.engine.engine_name()))
        .with_snapshots(fixture.snapshots_config())
        .with_dataset(fixture.reader_dataset())
        .build();
    let reader = Arc::new(Runtime::builder().with_app(reader_app).build().await);
    load_runtime(Arc::clone(&reader)).await?;

    let reader_initial = count_rows(&reader, "trips").await?;
    if reader_initial != 3 {
        return Err(anyhow!(
            "reader should bootstrap to 3 rows from snapshot, got {reader_initial}"
        ));
    }

    assert_insert_rejected(&reader).await?;

    // ---------------------- mutate source ---------------------
    fixture.write_source(MUTATED_CSV)?;

    let next_id = fixture
        .wait_for_snapshot_id(first_id + 1, Duration::from_mins(1))
        .await
        .context("waiting for writer to publish a snapshot for the mutated source")?;
    if next_id <= first_id {
        return Err(anyhow!(
            "snapshot id must advance after source change ({first_id} -> {next_id})"
        ));
    }

    wait_for_reader_swap(&reader, 5, Duration::from_mins(1)).await?;
    assert_swap_sanity(&reader).await?;

    reader.shutdown().await;
    writer.shutdown().await;
    Ok(())
}

/// Verify that an INSERT against a snapshot-mode reader is rejected with a
/// snapshot-specific error message.
pub(crate) async fn assert_insert_rejected(reader: &Arc<Runtime>) -> Result<()> {
    let insert_err = run_query(reader, "INSERT INTO trips VALUES (99, 'zulu', 999)")
        .await
        .err()
        .ok_or_else(|| anyhow!("INSERT INTO under refresh_mode: snapshot must fail"))?;
    let msg = format!("{insert_err}");
    if !msg.contains("snapshot") {
        return Err(anyhow!(
            "INSERT error must mention snapshot mode, got: {msg}"
        ));
    }
    Ok(())
}

/// Poll the reader until its row count reaches `expected_rows`, or fail.
pub(crate) async fn wait_for_reader_swap(
    reader: &Arc<Runtime>,
    expected_rows: usize,
    max_wait: Duration,
) -> Result<()> {
    let deadline = Instant::now() + max_wait;
    loop {
        let observed = count_rows(reader, "trips").await?;
        if observed == expected_rows {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "reader did not observe expected snapshot in time; last observed row count: {observed} (expected {expected_rows})"
            ));
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Sanity-check that both new and original rows are present after swap.
pub(crate) async fn assert_swap_sanity(reader: &Arc<Runtime>) -> Result<()> {
    let id5 = run_query(reader, "SELECT name FROM trips WHERE id = 5").await?;
    let id5_pretty = arrow::util::pretty::pretty_format_batches(&id5)
        .map(|fmt| fmt.to_string())
        .context("formatting id=5 row")?;
    if !id5_pretty.contains("echo") {
        return Err(anyhow!(
            "reader should serve the new id=5 row after swap; got:\n{id5_pretty}"
        ));
    }
    let id1 = run_query(reader, "SELECT name FROM trips WHERE id = 1").await?;
    let id1_pretty = arrow::util::pretty::pretty_format_batches(&id1)
        .map(|fmt| fmt.to_string())
        .context("formatting id=1 row")?;
    if !id1_pretty.contains("alpha") {
        return Err(anyhow!(
            "reader should still serve the original id=1 row after swap; got:\n{id1_pretty}"
        ));
    }
    Ok(())
}
