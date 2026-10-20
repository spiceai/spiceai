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

//! `mode: datalake` keeps every acknowledged row through the loss of the local
//! volume (spiceai/spiceai#15211): a node writes, its local disk is deleted, and
//! a new node with an empty disk opens the same tables and finds every row.
//!
//! The `s3` tests run against the S3-compatible store named by
//! `CAYENNE_DATALAKE_TEST_ENDPOINT`, `_BUCKET`, `_KEY` and `_SECRET` (`MinIO` or
//! `RustFS`, say), and return immediately when they are unset.

#![expect(clippy::expect_used, reason = "test failures carry their context")]

use std::path::Path;
use std::sync::Arc;

use accelerator_cayenne::CayenneAccelerator;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry};
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode, RefreshMode},
    testing::TestAccelerationSource,
};
use spicepod::component::runtime::{Runtime, RuntimeState};
use spicepod::param::Params;

const DATASETS: [&str; 2] = ["ha_probe", "ha_second"];
const DOCS: &str = "https://spiceai.org/docs/components/data-accelerators/cayenne";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("time_unix_nano", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Where a node keeps its tables.
#[derive(Clone)]
struct Setup {
    state: Option<RuntimeState>,
    mode: Mode,
    params: Vec<(&'static str, String)>,
}

impl Setup {
    fn state(location: String, params: &[(&str, String)]) -> Self {
        Self {
            state: Some(RuntimeState {
                location,
                params: (!params.is_empty()).then(|| {
                    Params::from_string_map(
                        params
                            .iter()
                            .map(|(k, v)| ((*k).to_string(), v.clone()))
                            .collect(),
                    )
                }),
            }),
            mode: Mode::Datalake,
            params: Vec::new(),
        }
    }

    fn source(&self, metadata_dir: &Path, dataset: &str) -> TestAccelerationSource {
        let runtime = Runtime {
            state: self.state.clone(),
            ..Default::default()
        };
        TestAccelerationSource::new(dataset)
            .with_app(
                app::AppBuilder::new("dlprobe")
                    .with_runtime(runtime)
                    .build(),
            )
            .with_acceleration(Acceleration {
                enabled: true,
                engine: Engine::Cayenne,
                mode: self.mode,
                refresh_mode: Some(RefreshMode::Append),
                primary_key: Some(
                    datafusion_table_providers::util::column_reference::ColumnReference::try_from(
                        "time_unix_nano",
                    )
                    .expect("primary key"),
                ),
                params: std::iter::once((
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.to_string_lossy().into_owned(),
                ))
                .chain(
                    self.params
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), v.clone())),
                )
                .collect(),
                ..Default::default()
            })
    }
}

/// A node: one accelerator and query context, with its tables registered.
struct Node {
    ctx: SessionContext,
    tables: Vec<Arc<dyn TableProvider>>,
    _accelerator: Arc<CayenneAccelerator>,
}

impl Node {
    /// Open `datasets` on a node whose local disk is `disk`.
    async fn open(setup: &Setup, disk: &Path, datasets: &[&str]) -> Self {
        let ctx = SessionContext::new();
        let accelerator = Arc::new(CayenneAccelerator::new());
        let registry = AcceleratorEngineRegistry::new();
        registry
            .register_accelerator_engine(
                Engine::Cayenne,
                Arc::clone(&accelerator) as Arc<dyn data_accelerator_api::DataAccelerator>,
            )
            .await;
        let mut tables = Vec::new();
        for dataset in datasets {
            let source = setup.source(disk, dataset);
            let acceleration = source.acceleration().expect("acceleration").clone();
            data_accelerator_api::DataAccelerator::init(accelerator.as_ref(), &source)
                .await
                .expect("init table");
            // The runtime drops an accelerated dataset whose engine reports it
            // uninitialized after `init`, as it would a new volume's empty disk.
            assert!(
                data_accelerator_api::DataAccelerator::is_initialized(
                    accelerator.as_ref(),
                    &source
                ),
                "a table is initialized after `init`, even on an empty disk"
            );
            let table = registry
                .create_accelerator_table(
                    TableReference::bare(*dataset),
                    schema(),
                    None,
                    &acceleration,
                    source.secrets(),
                    Some(&source),
                    Arc::new(ctx.clone()),
                )
                .await
                .expect("create table");
            ctx.register_table(*dataset, Arc::clone(&table))
                .expect("register");
            tables.push(table);
        }
        Self {
            ctx,
            tables,
            _accelerator: accelerator,
        }
    }

    /// Stop the node: background maintenance, which commits too, ends before
    /// another node takes the tables over, as it does when the process exits.
    async fn stop(self) {
        for table in &self.tables {
            let cayenne = spice_table::find_concrete::<cayenne::CayenneTableProvider>(
                table.as_ref(),
                spice_table::LayerWalk::Write,
            )
            .expect("a Cayenne table");
            cayenne
                .flush_pending_maintenance()
                .await
                .expect("flush maintenance");
            cayenne.quiesce().await;
        }
    }

    async fn sql(&self, query: &str) -> Vec<arrow::array::RecordBatch> {
        self.ctx
            .sql(query)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute")
    }

    /// `dataset`'s stored values, in order.
    async fn values(&self, dataset: &str) -> Vec<i64> {
        self.sql(&format!("SELECT value FROM {dataset} ORDER BY value"))
            .await
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("values")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Write `count` single-row exports to `dataset`, as an OTLP sink receives
    /// them.
    async fn write(&self, dataset: &str, from: i64, count: i64) {
        for value in from..from + count {
            self.sql(&format!(
                "INSERT INTO {dataset} VALUES ({}, {value})",
                1_000_000 + value
            ))
            .await;
        }
    }
}

/// Two datasets write to one location; the first node's disk is deleted; a
/// node with an empty disk finds every row of both, and keeps writing.
async fn survive_volume_loss(setup: &Setup) {
    let dir = tempfile::tempdir().expect("temp dir");
    let node_a = dir.path().join("node-a");
    let node_b = dir.path().join("node-b");

    let a = Node::open(setup, &node_a, &DATASETS).await;
    a.write(DATASETS[0], 0, 40).await;
    a.write(DATASETS[1], 100, 10).await;
    let first: Vec<i64> = (0..40).collect();
    let second: Vec<i64> = (100..110).collect();
    assert_eq!(a.values(DATASETS[0]).await, first);
    assert_eq!(a.values(DATASETS[1]).await, second);
    a.stop().await;

    // Node loss: the local disk is gone.
    std::fs::remove_dir_all(&node_a).expect("delete node-a's disk");

    let b = Node::open(setup, &node_b, &DATASETS).await;
    assert_eq!(
        b.values(DATASETS[0]).await,
        first,
        "every acknowledged row survives the loss of the local volume"
    );
    assert_eq!(b.values(DATASETS[1]).await, second);
    // And the restored tables keep accepting writes.
    b.write(DATASETS[0], 40, 5).await;
    assert_eq!(b.values(DATASETS[0]).await, (0..45).collect::<Vec<_>>());
}

/// The relative paths of the files under `root`, sorted, without the local
/// store's lock files.
fn files(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('.'))
            {
                found.push(
                    path.strip_prefix(root)
                        .expect("under root")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn file_state_location_survives_volume_loss() {
    let state = tempfile::tempdir().expect("state dir");
    let location = url::Url::from_directory_path(state.path())
        .expect("file url")
        .to_string();
    survive_volume_loss(&Setup::state(location, &[])).await;

    // One catalog for both datasets, beside each dataset's data, and the
    // location's format record.
    let files = files(state.path());
    let top: std::collections::BTreeSet<&str> = files
        .iter()
        .filter_map(|file| {
            let mut parts = file.split('/');
            let first = parts.next()?;
            if first == "cayenne" {
                parts.next()
            } else {
                Some(first)
            }
        })
        .collect();
    assert_eq!(
        top,
        ["_spice", "catalog", DATASETS[0], DATASETS[1]]
            .into_iter()
            .collect(),
        "files: {files:?}"
    );
    assert!(
        files.contains(&"_spice/format.json".to_string()),
        "files: {files:?}"
    );
}

fn s3_env() -> Option<(String, String, String, String)> {
    let env = |name: &str| std::env::var(format!("CAYENNE_DATALAKE_TEST_{name}")).ok();
    let found = (
        env("ENDPOINT")?,
        env("BUCKET")?,
        env("KEY")?,
        env("SECRET")?,
    );
    Some(found)
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_state_location_survives_volume_loss() {
    let Some((endpoint, bucket, key, secret)) = s3_env() else {
        eprintln!("CAYENNE_DATALAKE_TEST_* is unset; skipping the S3 datalake test");
        return;
    };
    let location = format!("s3://{bucket}/datalake-test-{}/", uuid::Uuid::now_v7());
    let setup = Setup::state(
        location,
        &[
            ("s3_auth", "key".to_string()),
            ("s3_key", key),
            ("s3_secret", secret),
            ("s3_endpoint", endpoint),
            ("s3_region", "us-east-1".to_string()),
            ("allow_http", "true".to_string()),
        ],
    );
    survive_volume_loss(&setup).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_deprecated_datalake_location_survives_volume_loss() {
    let Some((endpoint, bucket, key, secret)) = s3_env() else {
        eprintln!("CAYENNE_DATALAKE_TEST_* is unset; skipping the S3 datalake test");
        return;
    };
    let location = format!("s3://{bucket}/datalake-test-{}/", uuid::Uuid::now_v7());
    let setup = Setup {
        state: None,
        mode: Mode::Datalake,
        params: vec![
            ("cayenne_datalake_location", location),
            ("cayenne_datalake_s3_auth", "key".to_string()),
            ("cayenne_datalake_s3_key", key),
            ("cayenne_datalake_s3_secret", secret),
            ("cayenne_datalake_s3_endpoint", endpoint),
            ("cayenne_datalake_s3_region", "us-east-1".to_string()),
            ("cayenne_datalake_s3_allow_http", "true".to_string()),
        ],
    };
    survive_volume_loss(&setup).await;
}

/// Switching a dataset from `mode: file` to `mode: datalake` starts a new
/// table, and leaves the `mode: file` table as it is.
#[tokio::test(flavor = "multi_thread")]
async fn switching_from_file_mode_starts_a_new_table() {
    let state = tempfile::tempdir().expect("state dir");
    let location = url::Url::from_directory_path(state.path())
        .expect("file url")
        .to_string();
    let disk = tempfile::tempdir().expect("disk");
    let datalake = Setup::state(location, &[]);
    let file = Setup {
        mode: Mode::File,
        ..datalake.clone()
    };
    let dataset = &DATASETS[..1];

    let node = Node::open(&file, disk.path(), dataset).await;
    node.write(DATASETS[0], 0, 3).await;
    node.stop().await;

    let node = Node::open(&datalake, disk.path(), dataset).await;
    assert_eq!(node.values(DATASETS[0]).await, Vec::<i64>::new());
    node.write(DATASETS[0], 10, 2).await;
    assert_eq!(node.values(DATASETS[0]).await, vec![10, 11]);
    node.stop().await;

    let node = Node::open(&file, disk.path(), dataset).await;
    assert_eq!(node.values(DATASETS[0]).await, vec![0, 1, 2]);
}

#[tokio::test]
async fn datalake_mode_without_a_state_location_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let setup = Setup {
        state: None,
        mode: Mode::Datalake,
        params: Vec::new(),
    };
    let source = setup.source(dir.path(), DATASETS[0]);
    let Err(error) =
        data_accelerator_api::DataAccelerator::init(&CayenneAccelerator::new(), &source).await
    else {
        panic!("no location must be refused");
    };
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to register dataset 'ha_probe' (cayenne): `mode: datalake` stores the table in `runtime.state.location`, which is not set. Set it, or use `mode: file`. See: {DOCS}"
        )
    );
}
