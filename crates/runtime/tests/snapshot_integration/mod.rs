/*
Copyright 2025 The Spice.ai OSS Authors

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
    env,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{run_query, runtime_ready_check, test_request_context, wait_until_true},
};
use anyhow::{Context, Result, anyhow};
use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Int64Type, SchemaRef};
use arrow::util::pretty::pretty_format_batches;
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use chrono::Utc;
use datafusion::common::TableReference;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
use futures::{StreamExt, future::try_join_all};
use object_store::{
    ClientOptions, ObjectMeta, ObjectStore, ObjectStoreExt,
    aws::AmazonS3Builder,
    path::{Path as ObjectPath, PathPart},
};
use runtime::{Runtime, status::ComponentStatus};
use runtime_acceleration::snapshot::{
    AccelerationEngine, ForceCreate, SnapshotBehavior as RuntimeSnapshotBehavior, SnapshotManager,
};
use serde_json::{Value, json};
use spicepod::acceleration::{
    RefreshMode, SnapshotsCompaction, SnapshotsCreationPolicy, SnapshotsTrigger,
};
use spicepod::{
    acceleration::{
        Acceleration, Mode, RefreshOnStartup, SnapshotBehavior as DatasetSnapshotBehavior,
    },
    component::{
        dataset::Dataset,
        snapshot::{BootstrapOnFailureBehavior, Snapshots},
    },
    param::Params,
};
use tempfile::TempDir;
use tokio::{
    fs,
    sync::Mutex,
    time::{sleep, timeout},
};
use uuid::Uuid;

const SNAPSHOT_BUCKET: &str = "spiceai-snapshot-integration-tests";
const SNAPSHOT_REGION: &str = "us-west-2";
const TAXI_TRIPS_DATASET_NAME: &str = "taxi_trips";

static SNAPSHOT_TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct SnapshotS3Context {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    base_path: ObjectPath,
}

impl SnapshotS3Context {
    async fn new(test_name: &str) -> Result<Self> {
        let store = build_snapshot_store().await?;
        let prefix = format!("{test_name}/{}", Uuid::now_v7());
        let base_path = ObjectPath::from(prefix.clone());
        Ok(Self {
            store,
            prefix,
            base_path,
        })
    }

    fn location_uri(&self) -> String {
        format!(
            "s3://{SNAPSHOT_BUCKET}/{}/",
            self.prefix.trim_end_matches('/')
        )
    }

    async fn metadata_json(&self) -> Result<Value> {
        let metadata_path = self.base_path.clone().join(PathPart::from("metadata.json"));
        let data = self
            .store
            .get(&metadata_path)
            .await
            .with_context(|| format!("Downloading snapshot metadata at {metadata_path}"))?
            .bytes()
            .await
            .context("Reading snapshot metadata bytes")?;
        serde_json::from_slice(&data).context("Parsing snapshot metadata as JSON")
    }

    /// The snapshot entries `metadata.json` records for `dataset`, oldest first. One
    /// per publication, unlike the snapshot objects: those are named to the second,
    /// so snapshots published within one second share an object.
    async fn published_snapshots(&self, dataset: &str) -> Result<Vec<Value>> {
        let metadata = self.metadata_json().await?;
        metadata
            .get(dataset)
            .and_then(|entry| entry.get("snapshots"))
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("Snapshot metadata has no snapshots for dataset {dataset}"))
    }

    async fn snapshot_objects(&self, dataset: &str) -> Result<Vec<ObjectMeta>> {
        let mut entries = Vec::new();
        let mut stream = self.store.list(Some(&self.base_path));
        while let Some(entry) = stream.next().await {
            let meta = entry?;
            if meta.location.filename().is_some_and(|filename| {
                Path::new(filename)
                    .extension()
                    .and_then(std::ffi::OsStr::to_str)
                    .is_some_and(|ext| {
                        ext.eq_ignore_ascii_case("duckdb")
                            || ext.eq_ignore_ascii_case("sqlite")
                            || ext.eq_ignore_ascii_case("cayenne")
                    })
                    && meta
                        .location
                        .as_ref()
                        .contains(&format!("dataset={dataset}/"))
            }) {
                entries.push(meta);
            }
        }
        Ok(entries)
    }

    async fn wait_for_snapshot_objects(
        &self,
        dataset: &str,
        minimum: usize,
        max_wait: Duration,
    ) -> Result<Vec<ObjectMeta>> {
        let deadline = Instant::now() + max_wait;
        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "Timed out waiting for at least {minimum} snapshot objects for dataset {dataset}"
                ));
            }

            match self.snapshot_objects(dataset).await {
                Ok(entries) if entries.len() >= minimum => return Ok(entries),
                Ok(entries) => {
                    if Instant::now() >= deadline {
                        return Err(anyhow!(
                            "Timed out waiting for at least {minimum} snapshot objects for dataset {dataset}; last observed {} snapshot objects",
                            entries.len()
                        ));
                    }
                }
                Err(err) => {
                    if Instant::now() >= deadline {
                        return Err(err.context(format!(
                            "Timed out while waiting for snapshot objects for dataset {dataset}"
                        )));
                    }
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn write_metadata(&self, metadata: &Value) -> Result<()> {
        let metadata_path = self.base_path.clone().join(PathPart::from("metadata.json"));
        let bytes =
            serde_json::to_vec_pretty(metadata).context("Serializing snapshot metadata to JSON")?;
        self.store
            .put(&metadata_path, bytes.into())
            .await
            .with_context(|| format!("Uploading modified snapshot metadata to {metadata_path}"))?;
        Ok(())
    }

    async fn cleanup(self) -> Result<()> {
        let mut stream = self.store.list(Some(&self.base_path));
        while let Some(entry) = stream.next().await {
            let meta = entry?;
            self.store
                .delete(&meta.location)
                .await
                .with_context(|| format!("Deleting snapshot object {}", meta.location))?;
        }
        Ok(())
    }
}

struct SnapshotFixture {
    context: SnapshotS3Context,
    _temp_dir: TempDir,
    dataset_from: String,
    local_db_path: PathBuf,
    dataset_params: HashMap<String, String>,
    schema: SchemaRef,
    baseline: Vec<RecordBatch>,
    engine: &'static str,
    initial_snapshot_count: usize,
}

impl SnapshotFixture {
    fn dataset(
        &self,
        snapshot_behavior: DatasetSnapshotBehavior,
        refresh_on_startup: RefreshOnStartup,
        extra_accel_params: &[(&str, &str)],
        dataset_param_overrides: &[(&str, &str)],
    ) -> Dataset {
        let mut dataset_params = self.dataset_params.clone();
        for (key, value) in dataset_param_overrides {
            dataset_params.insert((*key).to_string(), (*value).to_string());
        }

        let mut accel_params: HashMap<String, String> = HashMap::from([(
            format!("{}_file", self.engine),
            self.local_db_path.to_string_lossy().to_string(),
        )]);
        for (key, value) in extra_accel_params {
            accel_params.insert((*key).to_string(), (*value).to_string());
        }

        build_dataset(
            &self.dataset_from,
            TAXI_TRIPS_DATASET_NAME,
            &dataset_params,
            snapshot_behavior,
            &accel_params,
            self.engine,
            refresh_on_startup,
        )
    }

    fn snapshots_config(&self, behavior: BootstrapOnFailureBehavior) -> Snapshots {
        build_snapshots_config(&self.context, behavior)
    }

    fn baseline_pretty(&self) -> Result<String> {
        pretty_format_batches(&self.baseline)
            .map(|fmt| fmt.to_string())
            .context("Formatting baseline snapshot result batches")
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// A runtime that carries only this fixture's `snapshots` config, for a test that
    /// drives its own `SnapshotManager`. Registering `taxi_trips` as well would start the
    /// runtime's own snapshot writer on the same location and acceleration file, racing
    /// the manager under test.
    async fn snapshots_only_runtime(&self, app_name: &str) -> Result<Arc<Runtime>> {
        let app = AppBuilder::new(app_name)
            .with_snapshots(self.snapshots_config(BootstrapOnFailureBehavior::Warn))
            .build();
        configure_test_datafusion();
        let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
        load_runtime(Arc::clone(&runtime)).await?;
        Ok(runtime)
    }

    /// A `SnapshotManager` for `taxi_trips` that snapshots the `DuckDB` file at
    /// `local_db_path` into this fixture's location.
    async fn snapshot_manager(
        &self,
        runtime: &Runtime,
        local_db_path: PathBuf,
        compaction: SnapshotsCompaction,
        policy: SnapshotsCreationPolicy,
    ) -> Result<SnapshotManager> {
        let runtime_snapshots = runtime
            .app()
            .read()
            .await
            .as_ref()
            .and_then(|app| app.snapshots.clone())
            .ok_or_else(|| anyhow!("Runtime snapshots configuration unavailable"))?;
        let snapshot_behavior = RuntimeSnapshotBehavior::enabled(
            runtime_snapshots,
            runtime.secrets_weak(),
            runtime.tokio_io_runtime(),
            compaction,
        );
        Ok(SnapshotManager::try_new(
            TAXI_TRIPS_DATASET_NAME.to_string(),
            snapshot_behavior,
            runtime_acceleration::snapshot::AccelerationLayout::file(local_db_path),
            AccelerationEngine::DuckDB,
        )
        .await
        .ok_or_else(|| anyhow!("Failed to initialize SnapshotManager"))?
        .with_snapshots_creation_policy(policy))
    }

    async fn cleanup(self) -> Result<()> {
        self.context.cleanup().await
    }
}

fn build_dataset(
    from: &str,
    name: &str,
    dataset_params: &HashMap<String, String>,
    snapshot_behavior: DatasetSnapshotBehavior,
    accel_params: &HashMap<String, String>,
    engine: &str,
    refresh_on_startup: RefreshOnStartup,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(dataset_params.clone()));

    let acceleration = Acceleration {
        mode: Mode::File,
        engine: Some(engine.to_string()),
        params: Some(Params::from_string_map(accel_params.clone())),
        refresh_on_startup,
        snapshots: snapshot_behavior,
        ..Default::default()
    };
    dataset.acceleration = Some(acceleration);

    dataset
}

fn build_snapshots_config(
    context: &SnapshotS3Context,
    behavior: BootstrapOnFailureBehavior,
) -> Snapshots {
    let mut param_map = HashMap::from([("s3_region".to_string(), SNAPSHOT_REGION.to_string())]);

    if env::var("AWS_PROFILE").is_ok() {
        param_map.insert("s3_auth".to_string(), "iam_role".to_string());
    } else {
        param_map.insert("s3_auth".to_string(), "key".to_string());
        param_map.insert(
            "s3_key".to_string(),
            "${secrets:AWS_SNAPSHOT_KEY}".to_string(),
        );
        param_map.insert(
            "s3_secret".to_string(),
            "${secrets:AWS_SNAPSHOT_SECRET}".to_string(),
        );
    }

    if let Ok(endpoint) = env::var("AWS_SNAPSHOT_ENDPOINT") {
        param_map.insert(
            "allow_http".to_string(),
            endpoint.starts_with("http://").to_string(),
        );
        param_map.insert("s3_endpoint".to_string(), endpoint);
    }

    Snapshots {
        enabled: true,
        location: Some(context.location_uri()),
        bootstrap_on_failure_behavior: behavior,
        params: Some(Params::from_string_map(param_map)),
    }
}

#[expect(clippy::expect_used)]
fn build_metadata_document(
    context: &SnapshotS3Context,
    dataset_name: &str,
    snapshot_objects: &[ObjectMeta],
    schema: &SchemaRef,
) -> Value {
    let location = context.location_uri();
    let last_updated_ms = Utc::now().timestamp_millis();

    let mut snapshots: Vec<Value> = snapshot_objects
        .iter()
        .enumerate()
        .map(|(idx, meta)| {
            let timestamp_ms = meta.last_modified.timestamp_millis();
            let snapshot_path = format!("s3://{SNAPSHOT_BUCKET}/{}", meta.location);
            let checksum = meta.e_tag.clone().unwrap_or_default();
            json!({
                "snapshot-id": idx,
                "timestamp-ms": timestamp_ms,
                "snapshot": snapshot_path,
                "snapshot-checksum": checksum,
                "snapshot-checksum-algorithm": if checksum.is_empty() { Value::Null } else { Value::from("ETag") },
                "snapshot-size": meta.size,
            })
        })
        .collect();

    snapshots.sort_by_key(|value| {
        value
            .get("timestamp-ms")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    });

    let current_snapshot_id = snapshots
        .last()
        .and_then(|value| value.get("snapshot-id").and_then(Value::as_i64))
        .unwrap_or(0);

    let schema_json = serde_json::to_value(schema.as_ref()).expect("Serializing schema to JSON");

    json!({
        "format-version": 1,
        "location": location,
        "last-updated-ms": last_updated_ms,
        dataset_name: {
            "name": dataset_name,
            "schemas": [
                { "schema-id": 0, "schema": schema_json }
            ],
            "current-schema-id": 0,
            "snapshots": snapshots,
            "current-snapshot-id": current_snapshot_id,
            "properties": {},
        }
    })
}

async fn build_snapshot_store() -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(SNAPSHOT_BUCKET)
        .with_region(SNAPSHOT_REGION)
        .with_client_options(ClientOptions::default());

    // An S3-compatible store (e.g. MinIO) in place of AWS, for running the suite locally.
    if let Ok(endpoint) = env::var("AWS_SNAPSHOT_ENDPOINT") {
        builder = builder
            .with_allow_http(endpoint.starts_with("http://"))
            .with_endpoint(endpoint);
    }

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
                "AWS credentials are required to run snapshot integration tests. Provide AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET or configure AWS_PROFILE."
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

async fn load_runtime(rt: Arc<Runtime>) -> Result<()> {
    timeout(Duration::from_mins(3), Arc::clone(&rt).load_components())
        .await
        .map_err(|_| anyhow!("Timed out waiting for runtime components to load"))?;
    runtime_ready_check(rt.as_ref()).await;
    Ok(())
}

async fn prepare_duckdb_fixture(test_name: &str) -> Result<SnapshotFixture> {
    configure_test_datafusion();

    let context = SnapshotS3Context::new(test_name).await?;
    let temp_dir = TempDir::new().context("Creating temporary directory for DuckDB file")?;
    let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
    let sample_source_path = temp_dir.path().join("taxi_sample.csv");
    fs::write(&sample_source_path, sample_csv_contents)
        .await
        .context("Writing sample CSV for dataset source")?;
    let dataset_from = format!("file://{}", sample_source_path.display());
    let local_db_path = temp_dir.path().join("taxi_trips.duckdb");

    let dataset_params = HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ]);

    let mut accel_params = HashMap::new();
    accel_params.insert(
        "duckdb_file".to_string(),
        local_db_path.to_string_lossy().to_string(),
    );

    let dataset = build_dataset(
        &dataset_from,
        TAXI_TRIPS_DATASET_NAME,
        &dataset_params,
        DatasetSnapshotBehavior::Enabled,
        &accel_params,
        "duckdb",
        RefreshOnStartup::Auto,
    );

    let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

    let app = AppBuilder::new(format!("{test_name}_bootstrap"))
        .with_snapshots(snapshots)
        .with_dataset(dataset)
        .build();

    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
    load_runtime(Arc::clone(&runtime)).await?;

    let baseline = run_query(
        &runtime,
        "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
    )
    .await
    .context("Executing baseline query for DuckDB snapshot")?;

    let schema = run_query(&runtime, "SELECT * FROM taxi_trips LIMIT 1")
        .await
        .context("Retrieving schema for taxi_trips dataset")?
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| anyhow!("Failed to retrieve schema from taxi_trips dataset"))?;

    runtime.shutdown().await;

    let snapshot_objects = context
        .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_mins(1))
        .await?;
    let metadata = build_metadata_document(
        &context,
        TAXI_TRIPS_DATASET_NAME,
        &snapshot_objects,
        &schema,
    );
    context
        .write_metadata(&metadata)
        .await
        .context("Writing initial snapshot metadata")?;

    Ok(SnapshotFixture {
        context,
        _temp_dir: temp_dir,
        dataset_from,
        local_db_path,
        dataset_params,
        schema,
        baseline,
        engine: "duckdb",
        initial_snapshot_count: snapshot_objects.len(),
    })
}

#[cfg(feature = "sqlite")]
async fn prepare_sqlite_fixture(test_name: &str) -> Result<SnapshotFixture> {
    configure_test_datafusion();

    let context = SnapshotS3Context::new(test_name).await?;
    let temp_dir = TempDir::new().context("Creating temporary directory for SQLite file")?;
    let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
    let sample_source_path = temp_dir.path().join("taxi_sample.csv");
    fs::write(&sample_source_path, sample_csv_contents)
        .await
        .context("Writing sample CSV for dataset source")?;
    let dataset_from = format!("file://{}", sample_source_path.display());
    let local_db_path = temp_dir.path().join("taxi_trips.sqlite");

    let dataset_params = HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ]);

    let mut accel_params = HashMap::new();
    accel_params.insert(
        "sqlite_file".to_string(),
        local_db_path.to_string_lossy().to_string(),
    );

    let dataset = build_dataset(
        &dataset_from,
        TAXI_TRIPS_DATASET_NAME,
        &dataset_params,
        DatasetSnapshotBehavior::Enabled,
        &accel_params,
        "sqlite",
        RefreshOnStartup::Auto,
    );

    let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

    let app = AppBuilder::new(format!("{test_name}_bootstrap"))
        .with_snapshots(snapshots)
        .with_dataset(dataset)
        .build();

    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
    load_runtime(Arc::clone(&runtime)).await?;

    let baseline = run_query(
        &runtime,
        "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
    )
    .await
    .context("Executing baseline query for SQLite snapshot")?;

    let schema = run_query(&runtime, "SELECT * FROM taxi_trips LIMIT 1")
        .await
        .context("Retrieving schema for taxi_trips dataset")?
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| anyhow!("Failed to retrieve schema from taxi_trips dataset"))?;

    runtime.shutdown().await;

    let snapshot_objects = context
        .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_mins(1))
        .await?;
    let metadata = build_metadata_document(
        &context,
        TAXI_TRIPS_DATASET_NAME,
        &snapshot_objects,
        &schema,
    );
    context
        .write_metadata(&metadata)
        .await
        .context("Writing initial snapshot metadata")?;

    Ok(SnapshotFixture {
        context,
        _temp_dir: temp_dir,
        dataset_from,
        local_db_path,
        dataset_params,
        schema,
        baseline,
        engine: "sqlite",
        initial_snapshot_count: snapshot_objects.len(),
    })
}

fn remove_existing_local_files(path: &Path) {
    let candidates = [
        path.to_path_buf(),
        path_with_appended_suffix(path, "-wal"),
        path.with_added_extension("wal"),
        path_with_appended_suffix(path, "-shm"),
    ];
    for candidate in candidates {
        if let Err(err) = std::fs::remove_file(&candidate)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                "Failed to remove local acceleration file {}: {err}",
                candidate.display()
            );
        }
    }
}

fn path_with_appended_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map(OsString::from)
        .expect("database path should include a file name");
    file_name.push(suffix);
    path.with_file_name(file_name)
}

/// Appends `count` copies of the CSV's last data row, growing the source without
/// changing its schema so a rebuild from the source is distinguishable from a
/// restore of a snapshot taken before those rows existed.
async fn grow_csv_source(path: &Path, count: usize) -> Result<()> {
    let mut contents = fs::read_to_string(path)
        .await
        .with_context(|| format!("Reading dataset source {}", path.display()))?;
    let last_row = contents
        .lines()
        .last()
        .ok_or_else(|| anyhow!("Dataset source {} has no rows to duplicate", path.display()))?
        .to_string();
    if !contents.ends_with('\n') {
        contents.push('\n');
    }
    for _ in 0..count {
        contents.push_str(&last_row);
        contents.push('\n');
    }
    fs::write(path, contents)
        .await
        .with_context(|| format!("Growing dataset source {}", path.display()))
}

async fn count_rows(runtime: &Arc<Runtime>) -> Result<i64> {
    count_table_rows(runtime, TAXI_TRIPS_DATASET_NAME).await
}

async fn count_table_rows(runtime: &Arc<Runtime>, table: &str) -> Result<i64> {
    let batches = run_query(runtime, &format!("SELECT COUNT(*) FROM {table}"))
        .await
        .with_context(|| format!("Counting rows in {table}"))?;

    batches
        .first()
        .filter(|batch| batch.num_rows() > 0)
        .ok_or_else(|| anyhow!("COUNT(*) returned no rows: {batches:?}"))?
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|counts| counts.value(0))
        .ok_or_else(|| anyhow!("COUNT(*) column is not a BIGINT"))
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test1_duckdb_bootstrap_from_s3() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test1").await?;

            remove_existing_local_files(&fixture.local_db_path);

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test1_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let bootstrap_results = run_query(
                &runtime,
                "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Querying dataset bootstrapped from DuckDB snapshot")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&bootstrap_results)
                .map(|fmt| fmt.to_string())
                .context("Formatting bootstrap result batches")?;
            assert_eq!(
                expected, actual,
                "Bootstrap query results should match snapshot baseline"
            );

            let metadata = fixture.context.metadata_json().await?;
            let location = metadata
                .get("location")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("Snapshot metadata missing 'location' field"))?;
            assert_eq!(
                location,
                fixture.context.location_uri(),
                "Snapshot metadata location should match configured location"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test2_duckdb_bootstrap_without_federation() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test2").await?;

            remove_existing_local_files(&fixture.local_db_path);

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::BootstrapOnly,
                RefreshOnStartup::Always,
                &[("query_federation", "disabled")],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test2_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let statuses = runtime.status().get_dataset_statuses();
            let dataset_status = statuses.get(
                &TableReference::parse_str(TAXI_TRIPS_DATASET_NAME),
            );
            assert_eq!(
                dataset_status,
                Some(&ComponentStatus::Ready),
                "Dataset should be ready using the downloaded snapshot even when federation is disabled"
            );

            let offline_results = run_query(&runtime, "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1")
                .await
                .context("Querying dataset with federation disabled")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&offline_results)
                .map(|fmt| fmt.to_string())
                .context("Formatting offline bootstrap result batches")?;
            assert_eq!(
                expected, actual,
                "Offline query results should match snapshot baseline"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn snapshot_int_test3_sqlite_bootstrap_from_s3() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_sqlite_fixture("snapshot_int_test3").await?;

            remove_existing_local_files(&fixture.local_db_path);

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test3_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let bootstrap_results = run_query(
                &runtime,
                "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Querying dataset bootstrapped from SQLite snapshot")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&bootstrap_results)
                .map(|fmt| fmt.to_string())
                .context("Formatting SQLite bootstrap result batches")?;
            assert_eq!(
                expected, actual,
                "SQLite bootstrap query results should match snapshot baseline"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test4_existing_acceleration_skips_snapshot_download() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test4").await?;

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );

            let mut snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);
            snapshots.location = Some(format!("s3://{SNAPSHOT_BUCKET}/{}/", Uuid::now_v7()));

            let app = AppBuilder::new("snapshot_int_test4_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let results = run_query(&runtime, "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1")
                .await
                .context("Querying dataset with pre-existing acceleration file")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&results)
                .map(|fmt| fmt.to_string())
                .context("Formatting query results with local acceleration file")?;
            assert_eq!(
                expected, actual,
                "Query results should match baseline using existing acceleration file without downloading snapshot"
            );

            assert!(
                fixture.local_db_path.exists(),
                "Local acceleration file should remain intact"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test5_creates_and_uses_snapshot_on_restart() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test5").await?;

            let metadata = fixture.context.metadata_json().await?;
            assert_eq!(
                metadata
                    .get("format-version")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                1,
                "Snapshot metadata should record format version 1"
            );
            let location = metadata
                .get("location")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("Snapshot metadata missing 'location' field"))?;
            assert_eq!(
                location,
                fixture.context.location_uri(),
                "Snapshot metadata location should match configured location"
            );
            let dataset_entry = metadata
                .get(TAXI_TRIPS_DATASET_NAME)
                .ok_or_else(|| anyhow!("Snapshot metadata missing dataset entry"))?;
            assert!(
                dataset_entry.get("snapshots").is_some(),
                "Snapshot metadata should include the 'snapshots' array"
            );
            let snapshots = dataset_entry
                .get("snapshots")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("Snapshot metadata 'snapshots' field should be an array"))?;
            assert!(
                !snapshots.is_empty(),
                "Snapshot metadata should contain at least one snapshot entry"
            );
            if let Some(first_snapshot) = snapshots.first() {
                let snapshot_uri = first_snapshot
                    .get("snapshot")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("Snapshot entry missing 'snapshot' URI"))?;
                assert!(
                    snapshot_uri.starts_with(location),
                    "Snapshot entry should reside under configured location"
                );
            }

            remove_existing_local_files(&fixture.local_db_path);

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test5_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let results = run_query(
                &runtime,
                "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Querying dataset after restart with generated snapshot")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&results)
                .map(|fmt| fmt.to_string())
                .context("Formatting post-restart query results")?;
            assert_eq!(
                expected, actual,
                "Restarted runtime should read data from generated snapshot"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test6_concurrent_snapshot_writes_retry() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test6").await?;
            let schema = Arc::clone(fixture.schema());
            let runtime = fixture
                .snapshots_only_runtime("snapshot_int_test6_concurrent")
                .await?;

            // Each writer stands for an instance: it snapshots its own copy of the
            // acceleration file, so the writers race only on what instances share, the
            // location's writer lease and `metadata.json`. Writers that shared one file
            // would also race on its local staging copy, which no two instances share.
            let initial = fixture
                .context
                .published_snapshots(TAXI_TRIPS_DATASET_NAME)
                .await?
                .len();
            let writers_dir = TempDir::new().context("Creating directory for writer files")?;
            let mut managers = Vec::with_capacity(10);
            for writer in 0..10 {
                let local_db_path = writers_dir.path().join(format!("taxi_trips_{writer}.duckdb"));
                fs::copy(&fixture.local_db_path, &local_db_path)
                    .await
                    .context("Copying the acceleration file for a writer")?;
                // Always: this test is about concurrent creation, not the on_change skip.
                managers.push(
                    fixture
                        .snapshot_manager(
                            &runtime,
                            local_db_path,
                            SnapshotsCompaction::Disabled,
                            SnapshotsCreationPolicy::Always,
                        )
                        .await?,
                );
            }

            // The writers race for the dataset's snapshot writer lease. Every
            // write of the lease that is refused was refused by one that
            // landed, so at least one writer holds the lease and creates a
            // snapshot; a writer whose writes all met a conflicting one
            // stands by and returns `None`.
            let snapshot_results = try_join_all(managers.into_iter().map(|manager| {
                let schema = Arc::clone(&schema);
                async move {
                    let mutex = Arc::new(Mutex::new(()));
                    let lock_guard = mutex.lock_owned().await;
                    manager
                        .create_snapshot(&schema, lock_guard, None, None, ForceCreate(false))
                        .await
                }
            }))
            .await
            .context("Creating snapshots concurrently")?;

            assert_eq!(
                snapshot_results.len(),
                10,
                "Expected every concurrent snapshot request to complete without an error"
            );
            let created = snapshot_results.iter().flatten().count();
            assert!(
                created >= 1,
                "Expected the writer lease holder to create a snapshot; results: {snapshot_results:?}"
            );

            // Every snapshot a writer reports as created is published: none is lost
            // to a concurrent publication.
            assert_eq!(
                fixture
                    .context
                    .published_snapshots(TAXI_TRIPS_DATASET_NAME)
                    .await?
                    .len(),
                initial + created,
                "Expected one published snapshot per created snapshot; results: {snapshot_results:?}"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test7_respects_current_snapshot_metadata_selection() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test7").await?;
            let schema = Arc::clone(fixture.schema());
            let runtime = fixture
                .snapshots_only_runtime("snapshot_int_test7_prepare")
                .await?;
            let manager = fixture
                .snapshot_manager(
                    &runtime,
                    fixture.local_db_path.clone(),
                    SnapshotsCompaction::Disabled,
                    SnapshotsCreationPolicy::Always,
                )
                .await?;

            let conn = Connection::open(&fixture.local_db_path)
                .context("Opening DuckDB acceleration file for modification")?;
            conn.execute("DROP TABLE IF EXISTS taxi_trips_modified", [])
                .context("Cleaning up temporary snapshot modification table")?;
            conn.execute(
                "CREATE TABLE taxi_trips_modified AS SELECT * FROM taxi_trips",
                [],
            )
            .context("Creating temporary snapshot modification table")?;
            conn.execute(
                "UPDATE taxi_trips_modified SET passenger_count = COALESCE(passenger_count, 0) + 100",
                [],
            )
            .context("Updating DuckDB acceleration file to change snapshot contents")?;
            conn.execute("DROP VIEW IF EXISTS taxi_trips", [])
                .context("Dropping existing taxi_trips view prior to replacement")?;
            conn.execute("DROP TABLE IF EXISTS taxi_trips", [])
                .context("Dropping existing taxi_trips table prior to replacement")?;
            conn.execute(
                "CREATE TABLE taxi_trips AS SELECT * FROM taxi_trips_modified",
                [],
            )
            .context("Replacing taxi_trips table with modified data")?;
            conn.execute("DROP TABLE taxi_trips_modified", [])
                .context("Cleaning up temporary snapshot modification table")?;
            drop(conn);

            // Snapshot objects are named to the second. Publishing the modified
            // snapshot within the second the original was published in would
            // overwrite the original's object, leaving nothing to select.
            let into_next_second = 1_000_u64.saturating_sub(u64::from(Utc::now().timestamp_subsec_millis()));
            sleep(Duration::from_millis(into_next_second + 10)).await;

            let mutex = Arc::new(Mutex::new(()));
            let lock_guard = mutex.lock_owned().await;

            manager
                .create_snapshot(&schema, lock_guard, None, None, ForceCreate(false))
                .await
                .context("Creating modified snapshot after deleting data")?
                .context("Snapshot should be created")?;

            // The manager published the modified snapshot as current. Point the
            // metadata back at the original one, which is the first entry: the
            // manager appends each snapshot it publishes.
            let mut metadata = fixture.context.metadata_json().await?;
            let dataset_entry = metadata
                .get_mut(TAXI_TRIPS_DATASET_NAME)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| anyhow!("Snapshot metadata missing dataset entry"))?;
            let snapshots_array = dataset_entry
                .get("snapshots")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("Snapshot metadata missing snapshots array"))?;
            assert_eq!(
                snapshots_array.len(),
                fixture.initial_snapshot_count + 1,
                "Expected the fixture's snapshots plus the modified one"
            );
            let original_snapshot_id = snapshots_array
                .first()
                .and_then(|snapshot| snapshot.get("snapshot-id").cloned())
                .ok_or_else(|| anyhow!("Original snapshot has no snapshot-id"))?;
            dataset_entry.insert("current-snapshot-id".to_string(), original_snapshot_id);
            fixture
                .context
                .write_metadata(&metadata)
                .await
                .context("Updating metadata to reference original snapshot")?;

            runtime.shutdown().await;

            remove_existing_local_files(&fixture.local_db_path);

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test7_restart")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let results = run_query(&runtime, "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1")
                .await
                .context("Querying dataset after metadata-directed bootstrap")?;
            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&results)
                .map(|fmt| fmt.to_string())
                .context("Formatting query results after metadata-directed bootstrap")?;
            assert_eq!(
                expected, actual,
                "Runtime should download and use the snapshot referenced by metadata, not the latest upload"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[expect(clippy::cast_precision_loss)]
#[tokio::test]
async fn snapshot_int_test8_duckdb_compaction_reduces_snapshot_size() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test8").await?;
            let schema = Arc::clone(fixture.schema());

            // Step 1: Create database fragmentation by inserting and deleting data
            // We create a separate table to avoid issues with taxi_trips being a view
            let conn = Connection::open(&fixture.local_db_path)
                .context("Opening DuckDB file to create fragmentation")?;

            // Create a new table for fragmentation testing
            conn.execute(
                "CREATE TABLE frag_test (
                    id INTEGER,
                    data VARCHAR,
                    padding VARCHAR
                )",
                [],
            )
                .context("Creating fragmentation test table")?;

            // Insert a large amount of data to grow the file
            // Using generate_series to create bulk data
            conn.execute(
                "INSERT INTO frag_test
                 SELECT i, 'data_' || i, REPEAT('x', 1000)
                 FROM generate_series(1, 10000) AS t(i)",
                [],
            )
                .context("Inserting initial data for fragmentation")?;

            // Insert more duplicate data multiple times
            for _ in 0..5 {
                conn.execute(
                    "INSERT INTO frag_test SELECT * FROM frag_test WHERE id <= 1000",
                    [],
                )
                    .context("Inserting duplicate data for fragmentation")?;
            }

            // Delete most rows to create dead tuples (fragmentation)
            // Keep only the first 100 rows
            conn.execute(
                "DELETE FROM frag_test WHERE id > 100",
                [],
            )
                .context("Deleting data to create dead tuples")?;

            // Force checkpoint to flush WAL and materialize fragmentation
            conn.execute("CHECKPOINT", [])
                .context("Forcing DuckDB checkpoint")?;
            drop(conn);

            // Record the fragmented file size
            let fragmented_size = std::fs::metadata(&fixture.local_db_path)
                .context("Getting fragmented file size")?
                .len();
            tracing::info!(
                "Fragmented database size: {fragmented_size} bytes. dataset={}",
                TAXI_TRIPS_DATASET_NAME
            );

            // Step 2: Create snapshot WITH compaction enabled
            let runtime = fixture
                .snapshots_only_runtime("snapshot_int_test8_compaction")
                .await?;
            let manager_with_compaction = fixture
                .snapshot_manager(
                    &runtime,
                    fixture.local_db_path.clone(),
                    SnapshotsCompaction::Enabled,
                    SnapshotsCreationPolicy::Always,
                )
                .await?;

            // Create compacted snapshot
            let mutex = Arc::new(Mutex::new(()));
            let lock_guard = mutex.lock_owned().await;

            let compacted_location = manager_with_compaction
                .create_snapshot(&schema, lock_guard, None, None, ForceCreate(false))
                .await
                .context("Creating snapshot with compaction enabled")?
                .context("Snapshot should be created")?;

            tracing::info!(
                "Created compacted snapshot at: {compacted_location}. dataset={}",
                TAXI_TRIPS_DATASET_NAME
            );

            // `create_snapshot` returns once the snapshot is published, so it is listed now.
            let snapshot_objects = fixture
                .context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await
                .context("Listing snapshot objects")?;
            let compacted_snapshot = snapshot_objects
                .iter()
                .find(|obj| obj.location == compacted_location)
                .ok_or_else(|| {
                    anyhow!(
                        "Compacted snapshot {compacted_location} not listed; listed: {:?}",
                        snapshot_objects.iter().map(|obj| obj.location.to_string()).collect::<Vec<_>>()
                    )
                })?;

            let compacted_size = compacted_snapshot.size;
            tracing::info!(
                "Compacted snapshot size: {compacted_size} bytes. dataset={}",
                TAXI_TRIPS_DATASET_NAME
            );

            // Step 3: Verify compaction reduced the file size
            // The compacted file should be smaller because COPY FROM DATABASE
            // creates a fresh database without dead tuples
            assert!(
                compacted_size < fragmented_size,
                "Compacted snapshot ({compacted_size} bytes) should be smaller than \
                 fragmented database ({fragmented_size} bytes). \
                 Compaction should remove dead tuples created by DELETE operations."
            );

            let size_reduction_percent =
                ((fragmented_size - compacted_size) as f64 / fragmented_size as f64) * 100.0;
            tracing::info!(
                "Compaction reduced size by {size_reduction_percent:.1}%. \
                 fragmented={fragmented_size} compacted={compacted_size} dataset={}",
                TAXI_TRIPS_DATASET_NAME
            );

            runtime.shutdown().await;

            // Step 4: Verify the compacted snapshot can be downloaded and used
            remove_existing_local_files(&fixture.local_db_path);

            // The manager published the compacted snapshot as current, so a restart
            // bootstraps from it.
            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test8_bootstrap_compacted")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // Query the bootstrapped data
            let results = run_query(
                &runtime,
                "SELECT * FROM taxi_trips ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
                .await
                .context("Querying dataset bootstrapped from compacted snapshot")?;

            let expected = fixture.baseline_pretty()?;
            let actual = pretty_format_batches(&results)
                .map(|fmt| fmt.to_string())
                .context("Formatting results from compacted snapshot")?;

            assert_eq!(
                expected, actual,
                "Data from compacted snapshot should match baseline"
            );

            // Verify row count is preserved (compaction shouldn't lose data)
            let count_results = run_query(&runtime, "SELECT COUNT(*) as cnt FROM taxi_trips")
                .await
                .context("Counting rows in bootstrapped dataset")?;

            assert!(
                !count_results.is_empty(),
                "Should have count results from compacted snapshot"
            );

            runtime.shutdown().await;

            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test9_onchange_policy_skips_when_no_changes() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test9").await?;
            let schema = Arc::clone(fixture.schema());
            let runtime = fixture
                .snapshots_only_runtime("snapshot_int_test9_onchange")
                .await?;
            let manager = fixture
                .snapshot_manager(
                    &runtime,
                    fixture.local_db_path.clone(),
                    SnapshotsCompaction::Disabled,
                    SnapshotsCreationPolicy::OnChange,
                )
                .await?;
            let mutex = Arc::new(Mutex::new(()));

            // The published snapshots' `last_updated_at`, oldest first.
            let published = || async {
                Ok::<_, anyhow::Error>(
                    fixture
                        .context
                        .published_snapshots(TAXI_TRIPS_DATASET_NAME)
                        .await?
                        .iter()
                        .map(|snapshot| {
                            snapshot
                                .get("snapshot-last-updated-at-ms")
                                .and_then(Value::as_i64)
                        })
                        .collect::<Vec<_>>(),
                )
            };
            let initial = published().await?;

            let first = manager
                .create_snapshot(
                    &schema,
                    Arc::clone(&mutex).lock_owned().await,
                    Some(12345),
                    None,
                    ForceCreate(false),
                )
                .await
                .context("Creating first snapshot with OnChange policy")?;
            assert!(
                first.is_some(),
                "First snapshot should be created since no prior snapshot has this last_updated_at"
            );
            let after_first = published().await?;
            assert_eq!(
                after_first,
                [initial.as_slice(), &[Some(12345)]].concat(),
                "The first snapshot should be published with its last_updated_at"
            );

            let second = manager
                .create_snapshot(
                    &schema,
                    Arc::clone(&mutex).lock_owned().await,
                    Some(12345),
                    None,
                    ForceCreate(false),
                )
                .await
                .context("Attempting second snapshot with same last_updated_at")?;
            assert!(
                second.is_none(),
                "Second snapshot should be skipped since last_updated_at hasn't changed"
            );
            assert_eq!(
                published().await?,
                after_first,
                "No snapshot should be published when last_updated_at matches"
            );

            let third = manager
                .create_snapshot(
                    &schema,
                    Arc::clone(&mutex).lock_owned().await,
                    Some(99999),
                    None,
                    ForceCreate(false),
                )
                .await
                .context("Creating snapshot with new last_updated_at")?;
            assert!(
                third.is_some(),
                "Snapshot should be created when last_updated_at changes"
            );
            assert_eq!(
                published().await?,
                [after_first.as_slice(), &[Some(99999)]].concat(),
                "A snapshot should be published when last_updated_at changes"
            );

            runtime.shutdown().await;
            fixture.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test10_onchange_policy_skips_interval_based_snapshots() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            // Create a fresh S3 context without any pre-existing snapshots
            let context = SnapshotS3Context::new("snapshot_int_test10").await?;
            let temp_dir = TempDir::new().context("Creating temporary directory")?;

            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_source_path = temp_dir.path().join("taxi_sample.csv");
            fs::write(&sample_source_path, sample_csv_contents)
                .await
                .context("Writing sample CSV")?;

            let dataset_from = format!("file://{}", sample_source_path.display());
            let local_db_path = temp_dir.path().join("taxi_trips_test10.duckdb");

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                local_db_path.to_string_lossy().to_string(),
            );

            // Build dataset WITHOUT creating any initial snapshots
            let mut dataset = build_dataset(
                &dataset_from,
                TAXI_TRIPS_DATASET_NAME,
                &dataset_params,
                DatasetSnapshotBehavior::CreateOnly,
                &accel_params,
                "duckdb",
                RefreshOnStartup::Auto,
            );
            if let Some(ref mut accel) = dataset.acceleration {
                accel.snapshots_trigger = Some(SnapshotsTrigger::TimeInterval);
                accel.snapshots_trigger_threshold = Some("5s".to_string());
                accel.snapshots_creation_policy = SnapshotsCreationPolicy::OnChange;
            }

            let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test10_initial")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // Verify no snapshots exist yet
            let initial_snapshots = context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await
                .unwrap_or_default();
            assert!(
                initial_snapshots.is_empty(),
                "Should start with no snapshots in this fresh context"
            );

            tokio::time::sleep(Duration::from_secs(20)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_mins(1))
                .await?;

            assert_eq!(
                snapshots_after.len(),
                1,
                "Exactly one snapshot should be created"
            );

            runtime.shutdown().await;
            context.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test11_interval_based_snapshots() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            // Create a fresh S3 context without any pre-existing snapshots
            let context = SnapshotS3Context::new("snapshot_int_test10").await?;
            let temp_dir = TempDir::new().context("Creating temporary directory")?;

            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_source_path = temp_dir.path().join("taxi_sample.csv");
            fs::write(&sample_source_path, sample_csv_contents)
                .await
                .context("Writing sample CSV")?;

            let dataset_from = format!("file://{}", sample_source_path.display());
            let local_db_path = temp_dir.path().join("taxi_trips_test10.duckdb");

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                local_db_path.to_string_lossy().to_string(),
            );

            // Build dataset WITHOUT creating any initial snapshots
            let mut dataset = build_dataset(
                &dataset_from,
                TAXI_TRIPS_DATASET_NAME,
                &dataset_params,
                DatasetSnapshotBehavior::CreateOnly,
                &accel_params,
                "duckdb",
                RefreshOnStartup::Auto,
            );
            if let Some(ref mut accel) = dataset.acceleration {
                accel.snapshots_trigger = Some(SnapshotsTrigger::TimeInterval);
                accel.snapshots_trigger_threshold = Some("5s".to_string());
                accel.snapshots_creation_policy = SnapshotsCreationPolicy::Always;
            }

            let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test10_initial")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            // Verify no snapshots exist yet
            let initial_snapshots = context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await
                .unwrap_or_default();
            assert!(
                initial_snapshots.is_empty(),
                "Should start with no snapshots in this fresh context"
            );

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // The initial snapshot and two 5s interval ticks after it show the
            // interval keeps creating snapshots. An exact count after a fixed sleep
            // would also measure how long startup took on the machine.
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 3, Duration::from_mins(1))
                .await?;

            assert!(
                snapshots_after.len() >= 3,
                "The interval should keep creating snapshots; listed {}",
                snapshots_after.len()
            );

            runtime.shutdown().await;
            context.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test12_onchange_policy_skips_refresh_based_snapshots() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            // Create a fresh S3 context without any pre-existing snapshots
            let context = SnapshotS3Context::new("snapshot_int_test10").await?;
            let temp_dir = TempDir::new().context("Creating temporary directory")?;

            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_source_path = temp_dir.path().join("taxi_sample.csv");
            fs::write(&sample_source_path, sample_csv_contents)
                .await
                .context("Writing sample CSV")?;

            let dataset_from = format!("file://{}", sample_source_path.display());
            let local_db_path = temp_dir.path().join("taxi_trips_test10.duckdb");

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                local_db_path.to_string_lossy().to_string(),
            );

            // Build dataset WITHOUT creating any initial snapshots
            let mut dataset = build_dataset(
                &dataset_from,
                TAXI_TRIPS_DATASET_NAME,
                &dataset_params,
                DatasetSnapshotBehavior::CreateOnly,
                &accel_params,
                "duckdb",
                RefreshOnStartup::Auto,
            );
            dataset.time_column = Some("tpep_pickup_datetime".to_string());
            if let Some(ref mut accel) = dataset.acceleration {
                accel.refresh_mode = Some(RefreshMode::Append);
                accel.snapshots_trigger = Some(SnapshotsTrigger::RefreshComplete);
                accel.snapshots_creation_policy = SnapshotsCreationPolicy::OnChange;
            }

            let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test10_initial")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // Verify no snapshots exist yet
            let initial_snapshots = context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await
                .unwrap_or_default();
            assert!(
                initial_snapshots.is_empty(),
                "Should start with no snapshots in this fresh context"
            );

            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_mins(1))
                .await?;

            assert_eq!(
                snapshots_after.len(),
                1,
                "Exactly one snapshot should be created"
            );

            runtime.shutdown().await;
            context.cleanup().await
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test13_refresh_based_snapshots() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            // Create a fresh S3 context without any pre-existing snapshots
            let context = SnapshotS3Context::new("snapshot_int_test10").await?;
            let temp_dir = TempDir::new().context("Creating temporary directory")?;

            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_source_path = temp_dir.path().join("taxi_sample.csv");
            fs::write(&sample_source_path, sample_csv_contents)
                .await
                .context("Writing sample CSV")?;

            let dataset_from = format!("file://{}", sample_source_path.display());
            let local_db_path = temp_dir.path().join("taxi_trips_test10.duckdb");

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            let mut accel_params = HashMap::new();
            accel_params.insert(
                "duckdb_file".to_string(),
                local_db_path.to_string_lossy().to_string(),
            );

            // Build dataset WITHOUT creating any initial snapshots
            let mut dataset = build_dataset(
                &dataset_from,
                TAXI_TRIPS_DATASET_NAME,
                &dataset_params,
                DatasetSnapshotBehavior::CreateOnly,
                &accel_params,
                "duckdb",
                RefreshOnStartup::Auto,
            );
            if let Some(ref mut accel) = dataset.acceleration {
                accel.snapshots_trigger = Some(SnapshotsTrigger::RefreshComplete);
                accel.snapshots_creation_policy = SnapshotsCreationPolicy::Always;
            }

            let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test10_initial")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            tokio::time::sleep(Duration::from_secs(10)).await;

            // Verify initial snapshot exists
            let initial_snapshots = context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await
                .unwrap_or_default();
            assert_eq!(
                initial_snapshots.len(),
                1,
                "Exactly one snapshot should be created"
            );

            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .wait()
                .await;
            tokio::time::sleep(Duration::from_secs(10)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_mins(1))
                .await?;

            assert_eq!(
                snapshots_after.len(),
                4,
                "Exactly fours snapshots should be created"
            );

            runtime.shutdown().await;
            context.cleanup().await
        })
        .await
}

/// Rows appended to the source between the two arms of
/// `snapshot_int_test14_file_create_skips_snapshot_bootstrap`, which is what makes a
/// bootstrap and a rebuild-from-source land on different row counts.
#[cfg(feature = "duckdb")]
const FILE_CREATE_EXTRA_ROWS: usize = 5;

/// `mode: file_create` must not bootstrap the snapshot it just discarded.
///
/// `file_create` snapshots the outgoing acceleration and deletes it so the next
/// refresh rebuilds from the source. Bootstrapping the snapshot back would
/// restore the data and the stored schema the operator asked to drop, leaving
/// the mode with no effect (fixes #13005).
///
/// The first arm establishes the counterfactual: with `mode: file` the very same
/// snapshot is bootstrapped, and because the restored acceleration carries its
/// checkpoint, `refresh_on_startup: auto` skips the startup refresh. The second
/// arm grows the source and switches to `file_create`, so a bootstrap and a
/// rebuild-from-source produce different row counts.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn snapshot_int_test14_file_create_skips_snapshot_bootstrap() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = prepare_duckdb_fixture("snapshot_int_test14").await?;
            let source_path = PathBuf::from(
                fixture
                    .dataset_from
                    .strip_prefix("file://")
                    .ok_or_else(|| {
                        anyhow!("Dataset source is not a file:// URI: {}", fixture.dataset_from)
                    })?,
            );

            remove_existing_local_files(&fixture.local_db_path);

            let snapshot_rows = {
                let dataset = fixture.dataset(
                    DatasetSnapshotBehavior::Enabled,
                    RefreshOnStartup::Auto,
                    &[],
                    &[],
                );
                let app = AppBuilder::new("snapshot_int_test14_file")
                    .with_snapshots(fixture.snapshots_config(BootstrapOnFailureBehavior::Warn))
                    .with_dataset(dataset)
                    .build();

                configure_test_datafusion();

                let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
                load_runtime(Arc::clone(&runtime)).await?;
                let rows = count_rows(&runtime)
                    .await
                    .context("Counting rows bootstrapped by mode: file")?;
                runtime.shutdown().await;
                rows
            };
            assert!(
                snapshot_rows > 0,
                "mode: file should have bootstrapped the fixture's snapshot, but the dataset is empty"
            );
            assert!(
                fixture.local_db_path.exists(),
                "mode: file should have restored the acceleration file from the snapshot"
            );

            // Grow the source so restoring the snapshot and rebuilding from the
            // source no longer agree on the row count.
            grow_csv_source(&source_path, FILE_CREATE_EXTRA_ROWS)
                .await
                .context("Growing the dataset source before the file_create restart")?;

            let mut dataset = fixture.dataset(
                DatasetSnapshotBehavior::Enabled,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            dataset
                .acceleration
                .as_mut()
                .ok_or_else(|| anyhow!("Fixture dataset is missing its acceleration"))?
                .mode = Mode::FileCreate;

            let app = AppBuilder::new("snapshot_int_test14_file_create")
                .with_snapshots(fixture.snapshots_config(BootstrapOnFailureBehavior::Warn))
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;
            let rebuilt_rows = count_rows(&runtime)
                .await
                .context("Counting rows after the file_create restart")?;
            runtime.shutdown().await;

            assert_eq!(
                rebuilt_rows,
                snapshot_rows + i64::try_from(FILE_CREATE_EXTRA_ROWS)?,
                "file_create should have rebuilt the acceleration from the grown source; \
                 {snapshot_rows} rows means the snapshot it deleted was bootstrapped back"
            );

            fixture.cleanup().await
        })
        .await
}

/// Cayenne datasets sharing one metadata directory each choose their own snapshot
/// setting. A Cayenne snapshot carries only its own dataset's metastore slice, never the
/// shared `cayenne.db`, so a dataset with snapshots enabled places no constraint on one
/// that has them disabled. The runtime used to refuse such a pod outright and load neither
/// dataset (#14562).
///
/// First start: both datasets load, and only the enabled dataset publishes. Restart from
/// wiped local state with both sources shrunk: the enabled dataset bootstraps from its
/// snapshot beside the disabled one, which reloads from its source — each dataset's row
/// count says where it came from.
#[tokio::test]
async fn snapshot_int_test_cayenne_mixed_snapshot_settings_share_metadata_dir() -> Result<()> {
    let _guard = init_tracing(Some(
        "integration=debug,runtime::dataaccelerator=trace,info",
    ));

    test_request_context()
        .scope(async {
            let temp_dir =
                TempDir::new().context("Creating temporary directory for Cayenne files")?;

            // Create sample CSV files for two datasets
            let sample_csv_contents = include_str!("../test_data/taxi_sample.csv");
            let sample_source_path1 = temp_dir.path().join("taxi_sample1.csv");
            let sample_source_path2 = temp_dir.path().join("taxi_sample2.csv");
            fs::write(&sample_source_path1, sample_csv_contents)
                .await
                .context("Writing sample CSV for dataset 1")?;
            fs::write(&sample_source_path2, sample_csv_contents)
                .await
                .context("Writing sample CSV for dataset 2")?;
            let expected_rows = i64::try_from(sample_csv_contents.lines().count() - 1)
                .context("Counting sample CSV rows")?;

            let dataset_from1 = format!("file://{}", sample_source_path1.display());
            let dataset_from2 = format!("file://{}", sample_source_path2.display());

            // Separate data directories, one shared metadata directory, and a local
            // snapshot store so the test needs no object-store credentials.
            let data_dir1 = temp_dir.path().join("cayenne_data1");
            let data_dir2 = temp_dir.path().join("cayenne_data2");
            let metadata_dir = temp_dir.path().join("cayenne_metadata");
            let snapshot_dir = temp_dir.path().join("snapshots");

            fs::create_dir_all(&data_dir1)
                .await
                .context("Creating data directory 1")?;
            fs::create_dir_all(&data_dir2)
                .await
                .context("Creating data directory 2")?;
            fs::create_dir_all(&metadata_dir)
                .await
                .context("Creating metadata directory")?;
            fs::create_dir_all(&snapshot_dir)
                .await
                .context("Creating snapshot directory")?;

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);
            let cayenne_acceleration = |data_dir: &Path,
                                        metadata_dir: &Path,
                                        snapshots: DatasetSnapshotBehavior| {
                Acceleration {
                    mode: Mode::File,
                    engine: Some("cayenne".to_string()),
                    params: Some(Params::from_string_map(HashMap::from([
                        (
                            "cayenne_file_path".to_string(),
                            data_dir.to_string_lossy().to_string(),
                        ),
                        (
                            "cayenne_metadata_dir".to_string(),
                            metadata_dir.to_string_lossy().to_string(),
                        ),
                    ]))),
                    refresh_on_startup: RefreshOnStartup::Auto,
                    snapshots,
                    ..Default::default()
                }
            };

            // Dataset 1 has snapshots fully enabled (create and bootstrap); dataset 2 opts
            // out entirely. Built by a closure because the restart below declares the
            // same pod again over different local directories.
            let build_app = |name: &str, data_dir1: &Path, data_dir2: &Path, metadata_dir: &Path| {
                let mut dataset1 = Dataset::new(&dataset_from1, "taxi_trips_1");
                dataset1.params = Some(Params::from_string_map(dataset_params.clone()));
                dataset1.acceleration = Some(cayenne_acceleration(
                    data_dir1,
                    metadata_dir,
                    DatasetSnapshotBehavior::Enabled,
                ));

                let mut dataset2 = Dataset::new(&dataset_from2, "taxi_trips_2");
                dataset2.params = Some(Params::from_string_map(dataset_params.clone()));
                dataset2.acceleration = Some(cayenne_acceleration(
                    data_dir2,
                    metadata_dir,
                    DatasetSnapshotBehavior::Disabled,
                ));

                AppBuilder::new(name)
                    .with_snapshots(Snapshots {
                        enabled: true,
                        location: Some(format!("file://{}/", snapshot_dir.display())),
                        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
                        params: None,
                    })
                    .with_dataset(dataset1)
                    .with_dataset(dataset2)
                    .build()
            };
            let count_rows = |runtime: Arc<Runtime>, dataset: &'static str| async move {
                let batches = run_query(&runtime, &format!("SELECT COUNT(*) FROM {dataset}"))
                    .await
                    .with_context(|| format!("Counting rows of {dataset}"))?;
                batches
                    .first()
                    .map(|batch| batch.column(0).as_primitive::<Int64Type>().value(0))
                    .ok_or_else(|| anyhow!("COUNT(*) over {dataset} returned no rows"))
            };

            configure_test_datafusion();

            let runtime = Arc::new(
                Runtime::builder()
                    .with_app(build_app(
                        "snapshot_mixed_settings_test",
                        &data_dir1,
                        &data_dir2,
                        &metadata_dir,
                    ))
                    .build()
                    .await,
            );
            load_runtime(Arc::clone(&runtime)).await?;

            // Both datasets load and serve every source row.
            for dataset in ["taxi_trips_1", "taxi_trips_2"] {
                let count = count_rows(Arc::clone(&runtime), dataset).await?;
                assert_eq!(
                    count, expected_rows,
                    "{dataset} must serve every source row alongside a dataset with a different snapshot setting"
                );
            }

            // The enabled dataset publishes once its first refresh completes; the disabled
            // dataset never appears in the store's metadata document.
            let metadata_path = snapshot_dir.join("metadata.json");
            let read_metadata = || async {
                let bytes = fs::read(&metadata_path).await.ok()?;
                serde_json::from_slice::<Value>(&bytes).ok()
            };
            let published = wait_until_true(Duration::from_mins(1), || async {
                read_metadata().await.is_some_and(|metadata| {
                    metadata
                        .get("taxi_trips_1")
                        .and_then(|entry| entry.get("snapshots"))
                        .and_then(Value::as_array)
                        .is_some_and(|snapshots| !snapshots.is_empty())
                })
            })
            .await;
            let metadata = read_metadata().await;
            assert!(
                published,
                "taxi_trips_1 (snapshots enabled) must publish a snapshot; store metadata: {metadata:?}"
            );
            assert!(
                metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.get("taxi_trips_2").is_none()),
                "taxi_trips_2 (snapshots disabled) must not publish a snapshot; store metadata: {metadata:?}"
            );

            runtime.shutdown().await;

            // Restart on a node with none of that local state, with both sources shrunk to
            // a few rows. The enabled dataset must come back from its snapshot (every
            // original row) while the disabled dataset, which never had one, reloads from
            // its shrunk source. The bootstrap decision is per dataset, so the disabled
            // sibling opening the shared metastore first must not make the enabled one
            // skip its snapshot.
            //
            // The fresh node is modelled with new directories rather than by deleting the
            // first ones: within one process Cayenne keeps the metastore handle open per
            // metadata directory, so a deleted-and-recreated directory would be read
            // through the stale handle, which no real restart does.
            let restart_data_dir1 = temp_dir.path().join("restart_cayenne_data1");
            let restart_data_dir2 = temp_dir.path().join("restart_cayenne_data2");
            let restart_metadata_dir = temp_dir.path().join("restart_cayenne_metadata");
            let shrunk_rows: i64 = 7;
            let shrunk_csv: String = sample_csv_contents
                .lines()
                .take(usize::try_from(shrunk_rows).context("Shrunk row count")? + 1)
                .collect::<Vec<_>>()
                .join("\n");
            for path in [&sample_source_path1, &sample_source_path2] {
                fs::write(path, &shrunk_csv)
                    .await
                    .with_context(|| format!("Shrinking {}", path.display()))?;
            }

            let runtime = Arc::new(
                Runtime::builder()
                    .with_app(build_app(
                        "snapshot_mixed_settings_test_restart",
                        &restart_data_dir1,
                        &restart_data_dir2,
                        &restart_metadata_dir,
                    ))
                    .build()
                    .await,
            );
            load_runtime(Arc::clone(&runtime)).await?;

            let restored = count_rows(Arc::clone(&runtime), "taxi_trips_1").await?;
            assert_eq!(
                restored, expected_rows,
                "taxi_trips_1 (snapshots enabled) must bootstrap from its snapshot beside a disabled sibling, not reload from its shrunk source"
            );
            let reloaded = count_rows(Arc::clone(&runtime), "taxi_trips_2").await?;
            assert_eq!(
                reloaded, shrunk_rows,
                "taxi_trips_2 (snapshots disabled) must reload from its source, not from any snapshot"
            );

            runtime.shutdown().await;

            Ok(())
        })
        .await
}

/// Cayenne datasets sharing one `cayenne_metadata_dir`, enough of them that their
/// accelerator inits overlap.
const SHARED_METASTORE_DATASETS: usize = 6;

/// A writer that has published one snapshot for each of [`SHARED_METASTORE_DATASETS`]
/// Cayenne datasets sharing one metastore, and the rows each of them holds.
struct SharedMetastoreFixture {
    context: SnapshotS3Context,
    names: Vec<String>,
    expected_rows: i64,
}

impl SharedMetastoreFixture {
    async fn new(test_name: &str) -> Result<Self> {
        let mut fixture = Self::unpublished(test_name).await?;
        fixture.publish().await?;
        Ok(fixture)
    }

    /// A fixture whose writer has not published yet.
    async fn unpublished(test_name: &str) -> Result<Self> {
        Ok(Self {
            context: SnapshotS3Context::new(test_name).await?,
            names: (0..SHARED_METASTORE_DATASETS)
                .map(|i| format!("taxi_trips_{i}"))
                .collect(),
            expected_rows: 0,
        })
    }

    /// Runs the writer until it has published a snapshot of every dataset.
    async fn publish(&mut self) -> Result<()> {
        let (context, names) = (&self.context, &self.names);
        let temp_dir = TempDir::new().context("Creating the writer's Cayenne directory")?;
        let source_path = temp_dir.path().join("taxi_sample.csv");
        fs::write(&source_path, include_str!("../test_data/taxi_sample.csv"))
            .await
            .context("Writing sample CSV for the writer's datasets")?;
        let from = format!("file://{}", source_path.display());

        let mut app = AppBuilder::new("snapshot_writer").with_snapshots(build_snapshots_config(
            context,
            BootstrapOnFailureBehavior::Warn,
        ));
        for name in names {
            let mut dataset = shared_metastore_dataset(
                &from,
                name,
                temp_dir.path(),
                DatasetSnapshotBehavior::CreateOnly,
            );
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.refresh_mode = Some(RefreshMode::Full);
            }
            dataset.params = Some(Params::from_string_map(HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ])));
            app = app.with_dataset(dataset);
        }

        configure_test_datafusion();
        let runtime = Arc::new(Runtime::builder().with_app(app.build()).build().await);
        load_runtime(Arc::clone(&runtime)).await?;
        let expected_rows = count_table_rows(&runtime, &names[0]).await?;
        let wait = context
            .wait_for_current_snapshots(names, Duration::from_mins(2))
            .await;
        runtime.shutdown().await;
        wait?;
        assert!(
            expected_rows > 0,
            "the writer loaded no rows from the sample CSV"
        );
        self.expected_rows = expected_rows;
        Ok(())
    }

    /// A runtime serving only from snapshots: each dataset's source is a placeholder that
    /// is never read, so a dataset that does not bootstrap never loads.
    fn reader_app(
        &self,
        name: &str,
        root: &Path,
        datasets: &[String],
        refresh_check_interval: &str,
    ) -> app::App {
        let mut app = AppBuilder::new(name).with_snapshots(build_snapshots_config(
            &self.context,
            BootstrapOnFailureBehavior::Warn,
        ));
        for dataset in datasets {
            let mut dataset = shared_metastore_dataset(
                &format!("file:/nonexistent/{dataset}.csv"),
                dataset,
                root,
                DatasetSnapshotBehavior::BootstrapOnly,
            );
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.refresh_mode = Some(RefreshMode::Snapshot);
                acceleration.refresh_check_interval = Some(refresh_check_interval.to_string());
            }
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));
            app = app.with_dataset(dataset);
        }
        app.build()
    }

    /// Starts a reader over `root` and waits until every one of `datasets` serves the
    /// writer's rows, failing with each dataset's last observed state.
    async fn assert_reader_serves(
        &self,
        name: &str,
        root: &Path,
        datasets: &[String],
    ) -> Result<()> {
        let (runtime, load) = self.start_reader(name, root, datasets, "1h").await;
        let outcome = self.wait_until_served(&runtime, name, datasets).await;
        runtime.shutdown().await;
        load.abort();
        outcome
    }

    async fn start_reader(
        &self,
        name: &str,
        root: &Path,
        datasets: &[String],
        refresh_check_interval: &str,
    ) -> (Arc<Runtime>, tokio::task::JoinHandle<()>) {
        configure_test_datafusion();
        let runtime = Arc::new(
            Runtime::builder()
                .with_app(self.reader_app(name, root, datasets, refresh_check_interval))
                .build()
                .await,
        );
        // A dataset that did not bootstrap retries its placeholder source for good, so the
        // load is not awaited: the per-dataset queries in `wait_until_served` are the condition.
        let load = tokio::spawn(Arc::clone(&runtime).load_components());
        (runtime, load)
    }

    async fn wait_until_served(
        &self,
        runtime: &Arc<Runtime>,
        name: &str,
        datasets: &[String],
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let mut observed = Vec::with_capacity(datasets.len());
            for dataset in datasets {
                observed.push(match count_table_rows(runtime, dataset).await {
                    Ok(rows) if rows == self.expected_rows => None,
                    Ok(rows) => Some(format!("{dataset}: {rows} rows")),
                    Err(err) => Some(format!("{dataset}: {err:#}")),
                });
            }
            let missing = observed.into_iter().flatten().collect::<Vec<_>>();
            if missing.is_empty() {
                break Ok(());
            }
            if Instant::now() >= deadline {
                break Err(anyhow!(
                    "{name}: {} of {} datasets never served the {} rows of their snapshot: {}",
                    missing.len(),
                    datasets.len(),
                    self.expected_rows,
                    missing.join("; ")
                ));
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn cleanup(self) -> Result<()> {
        self.context.cleanup().await
    }
}

impl SnapshotS3Context {
    /// Waits until `metadata.json` names a current snapshot for every one of `datasets`.
    async fn wait_for_current_snapshots(
        &self,
        datasets: &[String],
        max_wait: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + max_wait;
        loop {
            let metadata = self.metadata_json().await.ok();
            let pending = datasets
                .iter()
                .filter(|dataset| {
                    metadata
                        .as_ref()
                        .and_then(|metadata| metadata.get(dataset.as_str()))
                        .and_then(|entry| entry.get("current-snapshot-id"))
                        .is_none_or(Value::is_null)
                })
                .cloned()
                .collect::<Vec<_>>();
            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "Timed out waiting for a snapshot of {} in {}",
                    pending.join(", "),
                    self.location_uri()
                ));
            }
            sleep(Duration::from_millis(500)).await;
        }
    }
}

/// A file-mode Cayenne dataset whose data directory is its own but whose metastore is
/// the one every dataset under `root` shares.
fn shared_metastore_dataset(
    from: &str,
    name: &str,
    root: &Path,
    snapshots: DatasetSnapshotBehavior,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        mode: Mode::File,
        engine: Some("cayenne".to_string()),
        params: Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                root.join("data").join(name).to_string_lossy().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                root.join("data")
                    .join("metadata")
                    .to_string_lossy()
                    .to_string(),
            ),
        ]))),
        refresh_on_startup: RefreshOnStartup::Auto,
        snapshots,
        ..Default::default()
    });
    dataset
}

/// Every Cayenne dataset sharing a metastore bootstraps from its own snapshot on a fresh
/// start. Their accelerator inits run concurrently and the first to open the metastore
/// creates the shared `cayenne_metadata_dir`, so deciding "this dataset already has local
/// data" from that directory skipped the bootstrap of whichever datasets came after it.
#[tokio::test]
async fn snapshot_int_test_cayenne_shared_metastore_fresh_bootstrap() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = SharedMetastoreFixture::new("snapshot_int_cayenne_fresh").await?;
            let reader_dir = TempDir::new().context("Creating the reader's Cayenne directory")?;

            let outcome = fixture
                .assert_reader_serves("fresh_reader", reader_dir.path(), &fixture.names)
                .await;
            fixture.cleanup().await?;
            outcome
        })
        .await
}

/// A restart where only some of the datasets sharing a metastore have local data
/// bootstraps the rest from their snapshots. The metastore directory exists from the
/// first boot, so reading it as "local data exists" skipped every dataset added since.
#[tokio::test]
async fn snapshot_int_test_cayenne_shared_metastore_partial_restart() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let fixture = SharedMetastoreFixture::new("snapshot_int_cayenne_restart").await?;
            let reader_dir = TempDir::new().context("Creating the reader's Cayenne directory")?;

            let outcome = async {
                fixture
                    .assert_reader_serves(
                        "first_boot_reader",
                        reader_dir.path(),
                        &fixture.names[..1],
                    )
                    .await?;
                fixture
                    .assert_reader_serves("restarted_reader", reader_dir.path(), &fixture.names)
                    .await
            }
            .await;
            fixture.cleanup().await?;
            outcome
        })
        .await
}

/// A reader started before any snapshot exists loads once the writer publishes one,
/// instead of retrying its placeholder source forever.
#[tokio::test]
async fn snapshot_int_test_cayenne_reader_started_before_first_snapshot() -> Result<()> {
    let _guard = init_tracing(Some("integration=debug,info"));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            let mut fixture =
                SharedMetastoreFixture::unpublished("snapshot_int_cayenne_early_reader").await?;
            let reader_dir = TempDir::new().context("Creating the reader's Cayenne directory")?;
            let (runtime, load) = fixture
                .start_reader("early_reader", reader_dir.path(), &fixture.names, "2s")
                .await;

            let outcome = async {
                let deadline = Instant::now() + Duration::from_mins(1);
                while !fixture.names.iter().all(|name| {
                    runtime
                        .status()
                        .get_dataset_status(&TableReference::parse_str(name))
                        .is_some_and(|status| status != ComponentStatus::Ready)
                }) {
                    if Instant::now() >= deadline {
                        return Err(anyhow!(
                            "the reader's datasets never started loading: {:?}",
                            runtime.status().get_dataset_statuses()
                        ));
                    }
                    sleep(Duration::from_millis(200)).await;
                }
                // Two of the reader's 2s snapshot checks, so it has found the location
                // empty before the writer publishes.
                sleep(Duration::from_secs(4)).await;

                fixture.publish().await?;
                fixture
                    .wait_until_served(&runtime, "early_reader", &fixture.names)
                    .await
            }
            .await;

            runtime.shutdown().await;
            load.abort();
            fixture.cleanup().await?;
            outcome
        })
        .await
}
