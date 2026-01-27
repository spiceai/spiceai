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
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{run_query, runtime_ready_check, test_request_context},
};
use anyhow::{Context, Result, anyhow};
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use chrono::Utc;
use datafusion::sql::TableReference;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
use futures::{StreamExt, future::try_join_all};
use object_store::{
    ClientOptions, ObjectMeta, ObjectStore,
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
        let metadata_path = self.base_path.child(PathPart::from("metadata.json"));
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

    async fn snapshot_objects(&self, dataset: &str) -> Result<Vec<ObjectMeta>> {
        let mut entries = Vec::new();
        let mut stream = self.store.list(Some(&self.base_path));
        while let Some(entry) = stream.next().await {
            let meta = entry?;
            if meta.location.filename().is_some_and(|filename| {
                Path::new(filename)
                    .extension()
                    .and_then(std::ffi::OsStr::to_str)
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
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
        let metadata_path = self.base_path.child(PathPart::from("metadata.json"));
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
    timeout(Duration::from_secs(180), Arc::clone(&rt).load_components())
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
        .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
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
        .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
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
        PathBuf::from(format!("{}-wal", path.to_string_lossy())),
        PathBuf::from(format!("{}.wal", path.to_string_lossy())),
        PathBuf::from(format!("{}-shm", path.to_string_lossy())),
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

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::CreateOnly,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test6_concurrent")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

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
                SnapshotsCompaction::Disabled,
            );

            let manager = SnapshotManager::try_new(
                TAXI_TRIPS_DATASET_NAME.to_string(),
                snapshot_behavior,
                runtime_acceleration::snapshot::AccelerationLayout::file(
                    fixture.local_db_path.clone(),
                ),
                AccelerationEngine::DuckDB,
            )
            .await
            .ok_or_else(|| anyhow!("Failed to initialize SnapshotManager for concurrent test"))?
            // Use Always policy since this test is about concurrent snapshot creation,
            // not about the on_change optimization
            .with_snapshots_creation_policy(SnapshotsCreationPolicy::Always);

            let snapshot_results = try_join_all((0..10).map(|_| {
                let manager_clone = manager.clone();
                let schema = Arc::clone(&schema);
                async move {
                    let mutex = Arc::new(Mutex::new(()));
                    let lock_guard = mutex.lock_owned().await;
                    manager_clone
                        .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
                        .await
                        .map(|opt| opt.expect("snapshot should be created"))
                }
            }))
            .await
            .context("Creating snapshots concurrently")?;

            assert_eq!(
                snapshot_results.len(),
                10,
                "Expected to create ten snapshots concurrently"
            );

            let expected_minimum = fixture.initial_snapshot_count + 1;
            let snapshot_objects = fixture
                .context
                .wait_for_snapshot_objects(
                    TAXI_TRIPS_DATASET_NAME,
                    expected_minimum,
                    Duration::from_secs(60),
                )
                .await?;
            assert!(
                snapshot_objects.len() >= expected_minimum,
                "Expected accumulated snapshot uploads after concurrent writes"
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

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::CreateOnly,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test7_prepare")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let runtime_snapshots = runtime
                .app()
                .read()
                .await
                .as_ref()
                .and_then(|app| app.snapshots.clone())
                .ok_or_else(|| anyhow!("Runtime snapshots configuration unavailable"))?;
            let snapshot_behavior =
                RuntimeSnapshotBehavior::enabled(runtime_snapshots, runtime.secrets_weak(), runtime.tokio_io_runtime(), SnapshotsCompaction::Disabled);
            let manager = SnapshotManager::try_new(
                TAXI_TRIPS_DATASET_NAME.to_string(),
                snapshot_behavior,
                runtime_acceleration::snapshot::AccelerationLayout::file(fixture.local_db_path.clone()),
                AccelerationEngine::DuckDB,
            )
            .await
            .ok_or_else(|| anyhow!("Failed to initialize SnapshotManager for metadata test"))?
            .with_snapshots_creation_policy(SnapshotsCreationPolicy::Always);

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

            let mutex = Arc::new(Mutex::new(()));
            let lock_guard = mutex.lock_owned().await;

            manager
                .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
                .await
                .context("Creating modified snapshot after deleting data")?
                .context("Snapshot should be created")?;

            let updated_objects = fixture
                .context
                .wait_for_snapshot_objects(
                    TAXI_TRIPS_DATASET_NAME,
                    fixture.initial_snapshot_count + 1,
                    Duration::from_secs(60),
                )
                .await?;
            let updated_metadata = build_metadata_document(
                &fixture.context,
                TAXI_TRIPS_DATASET_NAME,
                &updated_objects,
                &schema,
            );
            fixture
                .context
                .write_metadata(&updated_metadata)
                .await
                .context("Updating snapshot metadata after modification")?;

            let mut metadata = fixture.context.metadata_json().await?;
            let dataset_entry = metadata
                .get_mut(TAXI_TRIPS_DATASET_NAME)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| anyhow!("Snapshot metadata missing dataset entry"))?;
            let snapshots_array = dataset_entry
                .get_mut("snapshots")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow!("Snapshot metadata missing snapshots array"))?;
            assert!(
                snapshots_array.len() >= 2,
                "Expected at least two snapshots to exist"
            );
            let original_snapshot = snapshots_array
                .first()
                .ok_or_else(|| anyhow!("Snapshots array unexpectedly empty"))?
                .clone();
            if let Some(snapshot_id) = original_snapshot.get("snapshot-id").cloned() {
                dataset_entry.insert("current-snapshot-id".to_string(), snapshot_id);
            }
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
            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::CreateOnly,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test8_compaction")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            let runtime_snapshots = runtime
                .app()
                .read()
                .await
                .as_ref()
                .and_then(|app| app.snapshots.clone())
                .ok_or_else(|| anyhow!("Runtime snapshots configuration unavailable"))?;

            // Create snapshot behavior with compaction ENABLED (last param = true)
            let snapshot_behavior_with_compaction = RuntimeSnapshotBehavior::enabled(
                Arc::clone(&runtime_snapshots),
                runtime.secrets_weak(),
                runtime.tokio_io_runtime(),
                SnapshotsCompaction::Enabled,
            );

            let manager_with_compaction = SnapshotManager::try_new(
                TAXI_TRIPS_DATASET_NAME.to_string(),
                snapshot_behavior_with_compaction,
                runtime_acceleration::snapshot::AccelerationLayout::file(fixture.local_db_path.clone()),
                AccelerationEngine::DuckDB,
            )
                .await
                .ok_or_else(|| anyhow!("Failed to create SnapshotManager with compaction enabled"))?
                .with_snapshots_creation_policy(SnapshotsCreationPolicy::Always);

            // Create compacted snapshot
            let mutex = Arc::new(Mutex::new(()));
            let lock_guard = mutex.lock_owned().await;

            let compacted_location = manager_with_compaction
                .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
                .await
                .context("Creating snapshot with compaction enabled")?
                .context("Snapshot should be created")?;

            tracing::info!(
                "Created compacted snapshot at: {compacted_location}. dataset={}",
                TAXI_TRIPS_DATASET_NAME
            );

            // Wait for compacted snapshot to appear
            let compacted_objects = fixture
                .context
                .wait_for_snapshot_objects(
                    TAXI_TRIPS_DATASET_NAME,
                    fixture.initial_snapshot_count + 1,
                    Duration::from_secs(90),
                )
                .await
                .context("Waiting for compacted snapshot objects")?;

            let compacted_snapshot = compacted_objects
                .iter()
                .max_by_key(|obj| obj.last_modified)
                .ok_or_else(|| anyhow!("No compacted snapshot found in object storage"))?;

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

            // Update metadata to reference the compacted snapshot
            let updated_metadata = build_metadata_document(
                &fixture.context,
                TAXI_TRIPS_DATASET_NAME,
                &compacted_objects,
                &schema,
            );
            fixture
                .context
                .write_metadata(&updated_metadata)
                .await
                .context("Writing metadata for compacted snapshot")?;

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

            let dataset = fixture.dataset(
                DatasetSnapshotBehavior::CreateOnly,
                RefreshOnStartup::Auto,
                &[],
                &[],
            );
            let snapshots = fixture.snapshots_config(BootstrapOnFailureBehavior::Warn);

            let app = AppBuilder::new("snapshot_int_test9_onchange")
                .with_snapshots(snapshots)
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

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
                SnapshotsCompaction::Disabled,
            );

            let manager = SnapshotManager::try_new(
                TAXI_TRIPS_DATASET_NAME.to_string(),
                snapshot_behavior,
                runtime_acceleration::snapshot::AccelerationLayout::file(fixture.local_db_path.clone()),
                AccelerationEngine::DuckDB,
            )
                .await
                .ok_or_else(|| anyhow!("Failed to initialize SnapshotManager"))?
                .with_snapshots_creation_policy(SnapshotsCreationPolicy::OnChange);

            // Create first snapshot with a specific last_updated_at timestamp
            let last_updated_at = Some(12345i64);
            let mutex = Arc::new(Mutex::new(()));
            let lock_guard = Arc::clone(&mutex).lock_owned().await;

            let first_result = manager
                .create_snapshot(&schema, lock_guard, last_updated_at, ForceCreate(false))
                .await
                .context("Creating first snapshot with OnChange policy")?;

            assert!(
                first_result.is_some(),
                "First snapshot should be created since no prior snapshot exists with this timestamp"
            );

            // Wait for snapshot to appear in storage
            let snapshots_after_first = fixture
                .context
                .wait_for_snapshot_objects(
                    TAXI_TRIPS_DATASET_NAME,
                    fixture.initial_snapshot_count + 1,
                    Duration::from_secs(60),
                )
                .await?;

            // Update metadata to include the new snapshot
            let updated_metadata = build_metadata_document(
                &fixture.context,
                TAXI_TRIPS_DATASET_NAME,
                &snapshots_after_first,
                &schema,
            );

            // Manually set the snapshot_last_updated_at_ms in metadata
            let mut metadata = updated_metadata;
            if let Some(dataset_entry) = metadata.get_mut(TAXI_TRIPS_DATASET_NAME)
                && let Some(snapshots_arr) =
                    dataset_entry.get_mut("snapshots").and_then(Value::as_array_mut)
                && let Some(last_snapshot) = snapshots_arr.last_mut()
                && let Some(obj) = last_snapshot.as_object_mut()
            {
                obj.insert("snapshot-last-updated-at-ms".to_string(), json!(12345u64));
            }
            fixture.context.write_metadata(&metadata).await?;

            let snapshot_count_after_first = snapshots_after_first.len();

            // Try to create another snapshot with the SAME last_updated_at
            let lock_guard = Arc::clone(&mutex).lock_owned().await;
            let second_result = manager
                .create_snapshot(&schema, lock_guard, last_updated_at, ForceCreate(false))
                .await
                .context("Attempting second snapshot with same last_updated_at")?;

            assert!(
                second_result.is_none(),
                "Second snapshot should be skipped since last_updated_at hasn't changed"
            );

            // Verify no new snapshot was created
            sleep(Duration::from_secs(2)).await;
            let snapshots_after_second = fixture
                .context
                .snapshot_objects(TAXI_TRIPS_DATASET_NAME)
                .await?;

            assert_eq!(
                snapshots_after_second.len(),
                snapshot_count_after_first,
                "No new snapshot should be created when last_updated_at matches"
            );

            // Now create a snapshot with a DIFFERENT last_updated_at
            let new_last_updated_at = Some(99999i64);
            let lock_guard = Arc::clone(&mutex).lock_owned().await;
            let third_result = manager
                .create_snapshot(&schema, lock_guard, new_last_updated_at, ForceCreate(false))
                .await
                .context("Creating snapshot with new last_updated_at")?;

            assert!(
                third_result.is_some(),
                "Snapshot should be created when last_updated_at changes"
            );

            // Wait and verify new snapshot was created
            let snapshots_after_third = fixture
                .context
                .wait_for_snapshot_objects(
                    TAXI_TRIPS_DATASET_NAME,
                    snapshot_count_after_first + 1,
                    Duration::from_secs(60),
                )
                .await?;

            assert!(
                snapshots_after_third.len() > snapshot_count_after_first,
                "New snapshot should be created when last_updated_at changes"
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
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
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

            tokio::time::sleep(Duration::from_secs(20)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
                .await?;

            assert_eq!(
                snapshots_after.len(),
                4,
                "Exactly 4 snapshots should be created"
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
                .notified()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .notified()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .notified()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .notified()
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
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
                .notified()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .notified()
                .await;
            runtime
                .datafusion()
                .refresh_table(&TableReference::parse_str("taxi_trips"), None)
                .await
                .expect("Table refresh")
                .expect("Notify")
                .notified()
                .await;
            tokio::time::sleep(Duration::from_secs(10)).await;

            // Wait for snapshot to appear
            let snapshots_after = context
                .wait_for_snapshot_objects(TAXI_TRIPS_DATASET_NAME, 1, Duration::from_secs(60))
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

/// Test that Cayenne datasets with inconsistent snapshot settings are rejected.
///
/// When multiple Cayenne datasets share the same metadata directory, they must all have
/// the same snapshot configuration (either all enabled or all disabled). This test verifies
/// that the runtime correctly detects and rejects inconsistent configurations - the datasets
/// with inconsistent settings will not be loaded.
#[tokio::test]
async fn snapshot_int_test_cayenne_inconsistent_snapshots_rejected() -> Result<()> {
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

            let dataset_from1 = format!("file://{}", sample_source_path1.display());
            let dataset_from2 = format!("file://{}", sample_source_path2.display());

            // Create data directories for cayenne (separate data dirs, but shared metadata dir)
            let data_dir1 = temp_dir.path().join("cayenne_data1");
            let data_dir2 = temp_dir.path().join("cayenne_data2");
            let metadata_dir = temp_dir.path().join("cayenne_metadata");

            fs::create_dir_all(&data_dir1)
                .await
                .context("Creating data directory 1")?;
            fs::create_dir_all(&data_dir2)
                .await
                .context("Creating data directory 2")?;
            fs::create_dir_all(&metadata_dir)
                .await
                .context("Creating metadata directory")?;

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            // Build dataset 1 WITH snapshots enabled
            let mut dataset1 = Dataset::new(&dataset_from1, "taxi_trips_1");
            dataset1.params = Some(Params::from_string_map(dataset_params.clone()));
            dataset1.acceleration = Some(Acceleration {
                mode: Mode::File,
                engine: Some("cayenne".to_string()),
                params: Some(Params::from_string_map(HashMap::from([
                    (
                        "cayenne_file_path".to_string(),
                        data_dir1.to_string_lossy().to_string(),
                    ),
                    (
                        "cayenne_metadata_dir".to_string(),
                        metadata_dir.to_string_lossy().to_string(),
                    ),
                ]))),
                refresh_on_startup: RefreshOnStartup::Auto,
                snapshots: DatasetSnapshotBehavior::Enabled, // ENABLED
                ..Default::default()
            });

            // Build dataset 2 WITHOUT snapshots (disabled)
            let mut dataset2 = Dataset::new(&dataset_from2, "taxi_trips_2");
            dataset2.params = Some(Params::from_string_map(dataset_params.clone()));
            dataset2.acceleration = Some(Acceleration {
                mode: Mode::File,
                engine: Some("cayenne".to_string()),
                params: Some(Params::from_string_map(HashMap::from([
                    (
                        "cayenne_file_path".to_string(),
                        data_dir2.to_string_lossy().to_string(),
                    ),
                    (
                        "cayenne_metadata_dir".to_string(),
                        metadata_dir.to_string_lossy().to_string(),
                    ),
                ]))),
                refresh_on_startup: RefreshOnStartup::Auto,
                snapshots: DatasetSnapshotBehavior::Disabled, // DISABLED - inconsistent!
                ..Default::default()
            });

            // Parse the datasets to create acceleration sources
            let app = AppBuilder::new("snapshot_inconsistent_test")
                .with_dataset(dataset1)
                .with_dataset(dataset2)
                .build();

            configure_test_datafusion();

            // Build the runtime - the validation happens during dataset loading
            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

            // Give the runtime time to attempt dataset loading
            tokio::time::sleep(Duration::from_secs(3)).await;

            // The datasets should NOT be registered because validation failed
            // Check that neither dataset is available (the validation rejects all datasets
            // with inconsistent configuration)
            let taxi_trips_1_result = runtime
                .datafusion()
                .query_builder("SELECT COUNT(*) FROM taxi_trips_1")
                .build()
                .run()
                .await;
            let taxi_trips_2_result = runtime
                .datafusion()
                .query_builder("SELECT COUNT(*) FROM taxi_trips_2")
                .build()
                .run()
                .await;

            // Both queries should fail because the datasets were not loaded
            // due to the validation error
            assert!(
                taxi_trips_1_result.is_err() || taxi_trips_2_result.is_err(),
                "Expected at least one dataset to be unavailable due to validation failure. \
                 taxi_trips_1: {:?}, taxi_trips_2: {:?}",
                taxi_trips_1_result.is_ok(),
                taxi_trips_2_result.is_ok()
            );

            runtime.shutdown().await;

            Ok(())
        })
        .await
}

/// Test for issue #9060: Snapshots bootstrapping fails with multiple cayenne accelerations.
///
/// When having multiple cayenne accelerations with snapshots enabled, bootstrapping fails
/// because all datasets share the same metadata directory. When extracting the tar archive
/// for the second dataset, it tries to unpack the metadata files (e.g. `cayenne.db-wal`)
/// which already exist from the first dataset's extraction.
///
/// This test verifies that multiple cayenne datasets can bootstrap from snapshots
/// without conflicting on shared metadata files.
#[tokio::test]
async fn snapshot_int_test11_cayenne_multiple_datasets_bootstrap() -> Result<()> {
    let _guard = init_tracing(Some(
        "integration=debug,runtime_acceleration::snapshot=debug,info",
    ));
    let _test_lock = SNAPSHOT_TEST_MUTEX.lock().await;
    test_request_context()
        .scope(async {
            // Create S3 context for snapshots
            let context = SnapshotS3Context::new("snapshot_int_test11_cayenne").await?;
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

            let dataset_from1 = format!("file://{}", sample_source_path1.display());
            let dataset_from2 = format!("file://{}", sample_source_path2.display());

            // Create data directories for cayenne (separate data dirs, but shared metadata dir)
            let data_dir1 = temp_dir.path().join("cayenne_data1");
            let data_dir2 = temp_dir.path().join("cayenne_data2");
            let metadata_dir = temp_dir.path().join("cayenne_metadata");

            fs::create_dir_all(&data_dir1)
                .await
                .context("Creating data directory 1")?;
            fs::create_dir_all(&data_dir2)
                .await
                .context("Creating data directory 2")?;
            fs::create_dir_all(&metadata_dir)
                .await
                .context("Creating metadata directory")?;

            let dataset_params = HashMap::from([
                ("file_format".to_string(), "csv".to_string()),
                ("csv_has_header".to_string(), "true".to_string()),
            ]);

            // Build dataset 1
            let mut dataset1 = Dataset::new(&dataset_from1, "taxi_trips_1");
            dataset1.params = Some(Params::from_string_map(dataset_params.clone()));
            dataset1.acceleration = Some(Acceleration {
                mode: Mode::File,
                engine: Some("cayenne".to_string()),
                params: Some(Params::from_string_map(HashMap::from([
                    (
                        "cayenne_file_path".to_string(),
                        data_dir1.to_string_lossy().to_string(),
                    ),
                    (
                        "cayenne_metadata_dir".to_string(),
                        metadata_dir.to_string_lossy().to_string(),
                    ),
                ]))),
                refresh_on_startup: RefreshOnStartup::Auto,
                snapshots: DatasetSnapshotBehavior::Enabled,
                snapshots_trigger: Some(SnapshotsTrigger::RefreshComplete),
                snapshots_creation_policy: SnapshotsCreationPolicy::Always,
                ..Default::default()
            });

            // Build dataset 2
            let mut dataset2 = Dataset::new(&dataset_from2, "taxi_trips_2");
            dataset2.params = Some(Params::from_string_map(dataset_params.clone()));
            dataset2.acceleration = Some(Acceleration {
                mode: Mode::File,
                engine: Some("cayenne".to_string()),
                params: Some(Params::from_string_map(HashMap::from([
                    (
                        "cayenne_file_path".to_string(),
                        data_dir2.to_string_lossy().to_string(),
                    ),
                    (
                        "cayenne_metadata_dir".to_string(),
                        metadata_dir.to_string_lossy().to_string(),
                    ),
                ]))),
                refresh_on_startup: RefreshOnStartup::Auto,
                snapshots: DatasetSnapshotBehavior::Enabled,
                snapshots_trigger: Some(SnapshotsTrigger::RefreshComplete),
                snapshots_creation_policy: SnapshotsCreationPolicy::Always,
                ..Default::default()
            });

            let snapshots = build_snapshots_config(&context, BootstrapOnFailureBehavior::Fallback);

            // First run: create snapshots for both datasets
            let app = AppBuilder::new("snapshot_int_test11_cayenne_create")
                .with_snapshots(snapshots.clone())
                .with_dataset(dataset1.clone())
                .with_dataset(dataset2.clone())
                .build();

            configure_test_datafusion();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // Capture baseline results for both datasets
            let baseline1 = run_query(
                &runtime,
                "SELECT * FROM taxi_trips_1 ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Executing baseline query for dataset 1")?;

            let baseline2 = run_query(
                &runtime,
                "SELECT * FROM taxi_trips_2 ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Executing baseline query for dataset 2")?;

            let schema = run_query(&runtime, "SELECT * FROM taxi_trips_1 LIMIT 1")
                .await
                .context("Retrieving schema")?
                .first()
                .map(RecordBatch::schema)
                .ok_or_else(|| anyhow!("Failed to retrieve schema"))?;

            runtime.shutdown().await;

            // Wait for snapshots to be created for both datasets
            let snapshot_objects1 = context
                .wait_for_snapshot_objects("taxi_trips_1", 1, Duration::from_secs(60))
                .await
                .context("Waiting for dataset 1 snapshots")?;
            let snapshot_objects2 = context
                .wait_for_snapshot_objects("taxi_trips_2", 1, Duration::from_secs(60))
                .await
                .context("Waiting for dataset 2 snapshots")?;

            // Build metadata for both datasets
            let metadata1 =
                build_metadata_document(&context, "taxi_trips_1", &snapshot_objects1, &schema);
            let metadata2 =
                build_metadata_document(&context, "taxi_trips_2", &snapshot_objects2, &schema);

            // Merge both metadata documents
            let mut combined_metadata = metadata1;
            if let (Some(combined_obj), Some(meta2_obj)) =
                (combined_metadata.as_object_mut(), metadata2.as_object())
            {
                for (key, value) in meta2_obj {
                    if key != "format-version"
                        && key != "location"
                        && key != "last-updated-ms"
                    {
                        combined_obj.insert(key.clone(), value.clone());
                    }
                }
            }

            context
                .write_metadata(&combined_metadata)
                .await
                .context("Writing combined snapshot metadata")?;

            // Clean up local files to force bootstrap from snapshots
            fs::remove_dir_all(&data_dir1)
                .await
                .context("Removing data directory 1")?;
            fs::remove_dir_all(&data_dir2)
                .await
                .context("Removing data directory 2")?;
            fs::remove_dir_all(&metadata_dir)
                .await
                .context("Removing metadata directory")?;

            // Second run: bootstrap from snapshots
            // This is where the bug manifests - the second dataset fails to bootstrap
            // because the metadata files from the first dataset's tar extraction conflict
            let app = AppBuilder::new("snapshot_int_test11_cayenne_bootstrap")
                .with_snapshots(snapshots)
                .with_dataset(dataset1)
                .with_dataset(dataset2)
                .build();

            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime(Arc::clone(&runtime)).await?;

            // Verify both datasets bootstrapped correctly
            let bootstrap_results1 = run_query(
                &runtime,
                "SELECT * FROM taxi_trips_1 ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Querying dataset 1 after bootstrap")?;

            let bootstrap_results2 = run_query(
                &runtime,
                "SELECT * FROM taxi_trips_2 ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime LIMIT 1",
            )
            .await
            .context("Querying dataset 2 after bootstrap")?;

            let expected1 = pretty_format_batches(&baseline1)
                .map(|fmt| fmt.to_string())
                .context("Formatting baseline 1")?;
            let actual1 = pretty_format_batches(&bootstrap_results1)
                .map(|fmt| fmt.to_string())
                .context("Formatting bootstrap result 1")?;
            assert_eq!(
                expected1, actual1,
                "Dataset 1 bootstrap results should match baseline"
            );

            let expected2 = pretty_format_batches(&baseline2)
                .map(|fmt| fmt.to_string())
                .context("Formatting baseline 2")?;
            let actual2 = pretty_format_batches(&bootstrap_results2)
                .map(|fmt| fmt.to_string())
                .context("Formatting bootstrap result 2")?;
            assert_eq!(
                expected2, actual2,
                "Dataset 2 bootstrap results should match baseline"
            );

            runtime.shutdown().await;
            context.cleanup().await
        })
        .await
}
