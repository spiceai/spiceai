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

//! A Cayenne acceleration indexes every file its secondary index does not
//! cover before the table is returned to be served — whether it is opened at
//! startup or reopened by `refresh_mode: snapshot` before being swapped in —
//! so its first lookup reads no file in full. Regression test for #14803,
//! driven through the registry's table creation and the accelerator's
//! `reload_from_snapshot`, as the runtime opens and reloads a dataset.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use data_accelerator_api::{
    AccelerationSource, AcceleratorEngineRegistry, DataAccelerator, ReloadProviderFactory,
};
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use datafusion_table_providers::util::column_reference::ColumnReference;
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, IndexType, Mode},
    testing::TestAccelerationSource,
};

const ROWS: i64 = 40_000;
const PARTITIONS: i64 = 3;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new("p", DataType::Int64, false),
    ]))
}

/// A file-mode Cayenne acceleration of `t (id, w, p)` under `dir`, indexed on
/// `w` and partitioned by `p` when `partitioned`.
fn source(dir: &Path, partitioned: bool) -> TestAccelerationSource {
    TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        partition_by: if partitioned {
            vec![spicepod::partitioning::PartitionedBy {
                name: "p".to_string(),
                expression: "p".to_string(),
            }]
        } else {
            Vec::new()
        },
        indexes: [(
            ColumnReference::new(vec!["w".to_string()]),
            IndexType::Enabled,
        )]
        .into_iter()
        .collect(),
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join("data").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    })
}

/// Opens `t` through `registry`, as the runtime creates an accelerated table.
async fn open(
    registry: &Arc<AcceleratorEngineRegistry>,
    source: &TestAccelerationSource,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(SessionContext::new()),
        )
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
}

/// Every persisted index run file under `dir`.
fn run_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.expect("dir entry").path())
        .flat_map(|path| {
            if path.is_dir() {
                run_files(&path)
            } else if path.extension().is_some_and(|ext| ext == "run") {
                vec![path]
            } else {
                Vec::new()
            }
        })
        .collect()
}

/// Every `CayenneAccelerationExec` line of `EXPLAIN ANALYZE` for a lookup of
/// `w = 7` on `table`, and the rows the lookup returns.
async fn lookup(table: Arc<dyn TableProvider>) -> (Vec<String>, usize) {
    let ctx = SessionContext::new();
    ctx.register_table("t", table).expect("register");
    let sql = "SELECT id FROM t WHERE w = 7";
    let rows = ctx
        .sql(sql)
        .await
        .expect("plan lookup")
        .collect()
        .await
        .expect("lookup")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let plan = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("plan explain")
        .collect()
        .await
        .expect("explain");
    let text = arrow::util::pretty::pretty_format_batches(&plan)
        .expect("format plan")
        .to_string();
    let scans = text
        .lines()
        .filter(|line| line.contains("CayenneAccelerationExec"))
        .map(str::to_string)
        .collect();
    (scans, rows)
}

/// Stores `ROWS` rows in `t`, one write per partition, and waits until every
/// write's index run is on disk: one run per partition, or one in all.
async fn store(registry: &Arc<AcceleratorEngineRegistry>, dir: &Path, partitioned: bool) {
    let table = open(registry, &source(dir, partitioned))
        .await
        .expect("create table");
    let ctx = SessionContext::new();
    ctx.register_table("t", table).expect("register");
    let ids: Vec<i64> = (0..ROWS).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 1_000).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % PARTITIONS).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch");
    ctx.register_batch("src", batch).expect("source");
    ctx.sql("INSERT INTO t SELECT * FROM src")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("insert");
    let expected = if partitioned { PARTITIONS } else { 1 };
    let expected = usize::try_from(expected).expect("small");
    let deadline = Instant::now() + Duration::from_secs(30);
    while run_files(dir).len() < expected {
        assert!(
            Instant::now() < deadline,
            "the writes' runs were not persisted: {:?}",
            run_files(dir)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// How the table whose index runs are missing is opened.
#[derive(Clone, Copy)]
enum Opened {
    /// At startup, as a restart or a snapshot bootstrap opens it.
    AtStartup,
    /// By `refresh_mode: snapshot`, replacing the table being served.
    BySnapshotReload,
}

/// Opens `t` from the files under `dir` with their index runs gone (a snapshot
/// that did not carry them, runs that could not be loaded) and checks that the
/// first lookup on the opened table reads no file in full.
async fn table_without_its_runs_is_indexed_before_it_is_served(partitioned: bool, opened: Opened) {
    let dir = tempfile::tempdir().expect("dir");
    let accelerator = Arc::new(CayenneAccelerator::new());
    let registry = Arc::new(AcceleratorEngineRegistry::new());
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
        )
        .await;
    store(&registry, dir.path(), partitioned).await;
    let source = source(dir.path(), partitioned);

    let table = match opened {
        Opened::AtStartup => {
            drop_runs(dir.path());
            open(&registry, &source).await.expect("open table")
        }
        Opened::BySnapshotReload => {
            // The table being served when a new snapshot arrives.
            let previous = open(&registry, &source).await.expect("open table");
            drop_runs(dir.path());
            let factory: ReloadProviderFactory = {
                let registry = Arc::clone(&registry);
                let source = source.clone();
                Arc::new(move || {
                    let registry = Arc::clone(&registry);
                    let source = source.clone();
                    Box::pin(async move { open(&registry, &source).await })
                })
            };
            accelerator
                .reload_from_snapshot(&source, previous, factory)
                .await
                .expect("reload from snapshot")
        }
    };

    let (scans, rows) = lookup(table).await;
    assert_eq!(rows, usize::try_from(ROWS / 1_000).expect("small"));
    let files: Vec<&String> = scans
        .iter()
        .filter(|scan| scan.contains("candidate_files="))
        .collect();
    assert!(
        !files.is_empty()
            && files.iter().all(|scan| {
                !scan.contains("lookup_index=none") && scan.contains("uncovered_files=0")
            }),
        "the first lookup must be answered from the index alone:\n{}",
        scans.join("\n")
    );
}

/// Deletes every persisted index run file under `dir`.
fn drop_runs(dir: &Path) {
    let runs = run_files(dir);
    assert!(!runs.is_empty(), "the writes persisted their runs");
    for run in runs {
        std::fs::remove_file(run).expect("drop a run file");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_opened_without_its_runs_is_indexed_before_it_is_served() {
    table_without_its_runs_is_indexed_before_it_is_served(false, Opened::AtStartup).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_table_opened_without_its_runs_is_indexed_before_it_is_served() {
    table_without_its_runs_is_indexed_before_it_is_served(true, Opened::AtStartup).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_reloaded_without_its_runs_is_indexed_before_it_is_served() {
    table_without_its_runs_is_indexed_before_it_is_served(false, Opened::BySnapshotReload).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_table_reloaded_without_its_runs_is_indexed_before_it_is_served() {
    table_without_its_runs_is_indexed_before_it_is_served(true, Opened::BySnapshotReload).await;
}
