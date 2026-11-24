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

//! Two in-process Cayenne accelerators with the same table name must not mix
//! rows. Regression for #16748 / backport of #16749.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_plan::collect;
use datafusion_expr::dml::InsertOp;
use runtime_acceleration::Engine;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::testing::TestAccelerationSource;

use crate::CayenneAccelerator;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn dataset(base: &Path, name: &str) -> TestAccelerationSource {
    TestAccelerationSource::new(name).with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: [
            (
                "cayenne_metadata_dir".to_string(),
                base.join("metadata").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_file_path".to_string(),
                base.join("data").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    })
}

fn session() -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;
    SessionContext::new_with_config(config)
}

async fn start(
    source: &TestAccelerationSource,
) -> (Arc<CayenneAccelerator>, Arc<dyn TableProvider>) {
    start_with(Arc::new(CayenneAccelerator::new()), source).await
}

async fn start_with(
    accelerator: Arc<CayenneAccelerator>,
    source: &TestAccelerationSource,
) -> (Arc<CayenneAccelerator>, Arc<dyn TableProvider>) {
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
        )
        .await;
    accelerator.init(source).await.expect("init");
    let acceleration = source.acceleration().expect("acceleration").clone();
    let table = registry
        .create_accelerator_table(
            TableReference::bare(source.name().to_string()),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(session()),
        )
        .await
        .expect("create the table");
    (accelerator, table)
}

async fn load(table: &Arc<dyn TableProvider>, ids: std::ops::Range<i64>) {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let ctx = session();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("input");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Overwrite)
        .await
        .expect("plan the load");
    collect(plan, ctx.task_ctx()).await.expect("load");
}

async fn scanned_ids(table: &Arc<dyn TableProvider>) -> Vec<i64> {
    let ctx = session();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("plan the scan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        ids.extend(column.iter().map(|v| v.expect("id is not null")));
    }
    ids.sort_unstable();
    ids
}

/// Two accelerator instances, same table name, different data and metadata
/// directories: each must serve only the rows it loaded.
#[tokio::test]
async fn two_accelerators_same_table_name_different_dirs_keep_separate_rows() {
    let left_dir = tempfile::tempdir().expect("left dir");
    let right_dir = tempfile::tempdir().expect("right dir");
    let left = dataset(left_dir.path(), "orders");
    let right = dataset(right_dir.path(), "orders");

    let (_left_acc, left_table) = start(&left).await;
    load(&left_table, 1..4).await;
    let (_right_acc, right_table) = start(&right).await;
    load(&right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "left accelerator must not serve the right accelerator's rows"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "right accelerator must not serve the left accelerator's rows"
    );
}

/// One accelerator, two sources with the same table name and different
/// directories. The catalog `OnceCell` must not bind both to the first
/// metadata directory.
#[tokio::test]
async fn one_accelerator_same_table_name_different_dirs_keep_separate_rows() {
    let left_dir = tempfile::tempdir().expect("left dir");
    let right_dir = tempfile::tempdir().expect("right dir");
    let left = dataset(left_dir.path(), "orders");
    let right = dataset(right_dir.path(), "orders");
    let accelerator = Arc::new(CayenneAccelerator::new());

    let (accelerator, left_table) = start_with(Arc::clone(&accelerator), &left).await;
    load(&left_table, 1..4).await;
    let (_accelerator, right_table) = start_with(accelerator, &right).await;
    load(&right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "the first table must keep its own rows after a second table of the same name is created on another directory"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "the second table must not open the first table's catalog entry"
    );
}

/// Two accelerator instances with no `cayenne_file_path` / `cayenne_metadata_dir`
/// share the process default `.spice/data` catalog. Same table name must still
/// not mix rows: that is the in-process multi-Runtime case.
#[tokio::test]
async fn two_accelerators_same_table_name_default_dirs_keep_separate_rows() {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "this test changes the working directory, so it must run in a process of its own: run it with `cargo nextest run -p accelerator-cayenne --lib`"
    );
    let cwd = tempfile::tempdir().expect("cwd");
    std::env::set_current_dir(cwd.path()).expect("change the working directory");

    let left = TestAccelerationSource::new("orders").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        ..Default::default()
    });
    let right = TestAccelerationSource::new("orders").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        ..Default::default()
    });

    let (_left_acc, left_table) = start(&left).await;
    load(&left_table, 1..4).await;
    let (_right_acc, right_table) = start(&right).await;
    load(&right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "default-path left accelerator must not serve the right accelerator's rows"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "default-path right accelerator must not serve the left accelerator's rows"
    );
}

/// Two accelerator instances, same table name, same explicit data and metadata
/// directories, both alive: each must still serve only the rows it loaded.
#[tokio::test]
async fn two_accelerators_same_explicit_dirs_keep_separate_rows() {
    let dir = tempfile::tempdir().expect("shared dir");
    let left = dataset(dir.path(), "orders");
    let right = dataset(dir.path(), "orders");

    let (_left_acc, left_table) = start(&left).await;
    load(&left_table, 1..4).await;
    let (_right_acc, right_table) = start(&right).await;
    load(&right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "left accelerator must not serve the right accelerator's rows when configured paths collide"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "right accelerator must not serve the left accelerator's rows when configured paths collide"
    );
}

/// A restart drops the previous accelerator and opens the same directories
/// again. The instance that released the path must not leave a suffix that
/// would miss the files still on disk.
#[tokio::test]
async fn dropped_accelerator_releases_dir_so_restart_reuses_path() {
    let dir = tempfile::tempdir().expect("dir");
    let source = dataset(dir.path(), "orders");

    let first = CayenneAccelerator::new();
    let first_data = first.cayenne_data_dir(&source).expect("first data dir");
    let first_meta = first.metadata_dir_for(source.acceleration());
    drop(first);

    let second = CayenneAccelerator::new();
    let second_data = second.cayenne_data_dir(&source).expect("second data dir");
    let second_meta = second.metadata_dir_for(source.acceleration());

    assert_eq!(
        first_data, second_data,
        "a restart must reopen the original data directory, not an instance-suffixed sibling"
    );
    assert_eq!(
        first_meta, second_meta,
        "a restart must reopen the original metadata directory, not an instance-suffixed sibling"
    );
    assert!(
        !second_data.contains("-instance-"),
        "restart path must be the configured directory: {second_data}"
    );
}
