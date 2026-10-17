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
//! a new node with an empty disk opens the same table and finds every row.
//!
//! The `s3` test runs against the S3-compatible store named by
//! `CAYENNE_DATALAKE_TEST_ENDPOINT`, `_BUCKET`, `_KEY` and `_SECRET` (`MinIO` or
//! `RustFS`, say), and returns immediately when they are unset.

#![expect(clippy::expect_used, reason = "test failures carry their context")]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

const DATASET: &str = "ha_probe";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("time_unix_nano", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn source(
    metadata_dir: &Path,
    state: Option<String>,
    params: &[(&str, String)],
) -> TestAccelerationSource {
    let runtime = Runtime {
        state: state.map(|location| RuntimeState {
            location,
            params: None,
        }),
        ..Default::default()
    };
    TestAccelerationSource::new(DATASET)
        .with_app(
            app::AppBuilder::new("dlprobe")
                .with_runtime(runtime)
                .build(),
        )
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode: Mode::Datalake,
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
            .chain(params.iter().map(|(k, v)| ((*k).to_string(), v.clone())))
            .collect(),
            ..Default::default()
        })
}

/// Open the table on a node whose local disk is `node`.
async fn open(
    source: &TestAccelerationSource,
) -> (
    Arc<dyn TableProvider>,
    SessionContext,
    Arc<CayenneAccelerator>,
) {
    let ctx = SessionContext::new();
    let accelerator = Arc::new(CayenneAccelerator::new());
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn data_accelerator_api::DataAccelerator>,
        )
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    data_accelerator_api::DataAccelerator::init(accelerator.as_ref(), source)
        .await
        .expect("init datalake table");
    // The runtime drops an accelerated dataset whose engine reports it
    // uninitialized after `init`, as it would a new volume's empty disk.
    assert!(
        data_accelerator_api::DataAccelerator::is_initialized(accelerator.as_ref(), source),
        "a datalake table is initialized on a node with an empty disk"
    );
    let table = registry
        .create_accelerator_table(
            TableReference::bare(DATASET),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(ctx.clone()),
        )
        .await
        .expect("create datalake table");
    ctx.register_table(DATASET, Arc::clone(&table))
        .expect("register");
    (table, ctx, accelerator)
}

async fn sql(ctx: &SessionContext, query: &str) -> Vec<arrow::array::RecordBatch> {
    ctx.sql(query)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute")
}

/// The stored values, in order.
async fn values(ctx: &SessionContext) -> Vec<i64> {
    sql(ctx, &format!("SELECT value FROM {DATASET} ORDER BY value"))
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

/// Write `count` single-row exports, as an OTLP sink receives them.
async fn write(ctx: &SessionContext, from: i64, count: i64) {
    for value in from..from + count {
        sql(
            ctx,
            &format!(
                "INSERT INTO {DATASET} VALUES ({}, {value})",
                1_000_000 + value
            ),
        )
        .await;
    }
}

async fn survive_volume_loss(state: Option<String>, params: &[(&str, String)], settle: Duration) {
    let dir = tempfile::tempdir().expect("temp dir");
    let node_a = dir.path().join("node-a");
    let node_b = dir.path().join("node-b");

    let source_a = source(&node_a, state.clone(), params);
    let (table, ctx, _accelerator) = open(&source_a).await;
    write(&ctx, 0, 40).await;
    let written: Vec<i64> = (0..40).collect();
    assert_eq!(values(&ctx).await, written);
    // Long enough for the datalake tier to move warm files, when it runs.
    tokio::time::sleep(settle).await;
    assert_eq!(values(&ctx).await, written);
    // Node A stops: its background maintenance, which commits too, ends before
    // node B takes the table over, as it does when the process exits.
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
    drop((table, ctx));

    // Node loss: the local disk is gone.
    std::fs::remove_dir_all(&node_a).expect("delete node-a's disk");

    let source_b = source(&node_b, state, params);
    let (_table, ctx, _accelerator) = open(&source_b).await;
    assert_eq!(
        values(&ctx).await,
        written,
        "every acknowledged row survives the loss of the local volume"
    );
    // And the restored table keeps accepting writes.
    write(&ctx, 40, 5).await;
    assert_eq!(values(&ctx).await, (0..45).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn file_state_location_survives_volume_loss() {
    let state = tempfile::tempdir().expect("state dir");
    let location = url::Url::from_directory_path(state.path())
        .expect("file url")
        .to_string();
    survive_volume_loss(Some(location), &[], Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_datalake_location_survives_volume_loss() {
    let env = |name: &str| std::env::var(format!("CAYENNE_DATALAKE_TEST_{name}")).ok();
    let (Some(endpoint), Some(bucket), Some(key), Some(secret)) =
        (env("ENDPOINT"), env("BUCKET"), env("KEY"), env("SECRET"))
    else {
        eprintln!("CAYENNE_DATALAKE_TEST_* is unset; skipping the S3 datalake test");
        return;
    };
    let location = format!("s3://{bucket}/datalake-test-{}/", uuid::Uuid::now_v7());
    let params = [
        ("cayenne_datalake_location", location),
        ("cayenne_datalake_s3_auth", "key".to_string()),
        ("cayenne_datalake_s3_key", key),
        ("cayenne_datalake_s3_secret", secret),
        ("cayenne_datalake_s3_endpoint", endpoint),
        ("cayenne_datalake_s3_region", "us-east-1".to_string()),
        ("cayenne_datalake_s3_allow_http", "true".to_string()),
        // Move warm files to the datalake tier at once, as #15211's repro did.
        ("cayenne_datalake_warm_max_bytes", "1".to_string()),
        (
            "cayenne_datalake_tiering_check_interval_ms",
            "500".to_string(),
        ),
    ];
    survive_volume_loss(None, &params, Duration::from_secs(4)).await;
}

#[tokio::test]
async fn datalake_mode_without_a_location_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = source(dir.path(), None, &[]);
    let Err(error) =
        data_accelerator_api::DataAccelerator::init(&CayenneAccelerator::new(), &source).await
    else {
        panic!("no location must be refused");
    };
    assert_eq!(
        error.to_string(),
        "Failed to register dataset 'ha_probe' (cayenne): `mode: datalake` stores the table in object storage, but neither `cayenne_datalake_location` nor `runtime.state.location` is set. Set one, or use `mode: file`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    );
}
