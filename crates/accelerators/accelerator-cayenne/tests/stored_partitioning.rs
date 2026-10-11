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

//! A file-mode Cayenne acceleration refuses to open data stored under a
//! different partitioning than its `partition_by`. Cayenne does not
//! re-partition stored data, so opening it would serve none of its rows.

use std::{path::Path, sync::Arc};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry};
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode},
    testing::TestAccelerationSource,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// A file-mode Cayenne acceleration of `t (id, w, v)` stored under `dir` and
/// partitioned by the columns `partition_by`, as the runtime creates it.
async fn table(
    ctx: &SessionContext,
    dir: &Path,
    partition_by: &[&str],
) -> Result<Arc<dyn TableProvider>, String> {
    let source = TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        partition_by: partition_by
            .iter()
            .map(|column| spicepod::partitioning::PartitionedBy {
                name: (*column).to_string(),
                expression: (*column).to_string(),
            })
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
    });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(Engine::Cayenne, Arc::new(CayenneAccelerator::new()))
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(ctx.clone()),
        )
        .await
        .map_err(|error| error.to_string())
}

/// Stores two rows in `t` partitioned by `partition_by`, then closes it.
async fn store(dir: &Path, partition_by: &[&str]) {
    let ctx = SessionContext::new();
    let table = table(&ctx, dir, partition_by).await.expect("table");
    ctx.register_table("t", table).expect("register");
    ctx.sql("INSERT INTO t VALUES (1, 0, 0), (2, 1, 0)")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("insert");
}

/// The `id` of every row of `t`, ordered.
async fn ids(ctx: &SessionContext) -> Vec<i64> {
    let batches = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("rows");
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_data_reopens_under_its_own_partitioning() {
    for partition_by in [&[][..], &["w"][..]] {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), partition_by).await;

        let ctx = SessionContext::new();
        let table = table(&ctx, dir.path(), partition_by).await.expect("reopen");
        ctx.register_table("t", table).expect("register");
        assert_eq!(ids(&ctx).await, [1, 2], "partition_by: {partition_by:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioning_unpartitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &[]).await;

    let error = table(&SessionContext::new(), dir.path(), &["w"])
        .await
        .expect_err("unpartitioned data must not open as a partitioned table");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written without 'partition_by', but the dataset now sets 'partition_by: [w]'"
        ),
        "{error}"
    );
    assert!(
        error.contains("To keep using the existing data, remove 'partition_by'"),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "the Cayenne metastore directory '{}', then start Spice to reload the dataset from the source",
            dir.path().join("metadata").display()
        )),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpartitioning_partitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &["w"]).await;

    let error = table(&SessionContext::new(), dir.path(), &[])
        .await
        .expect_err("partitioned data must not open as an unpartitioned table");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written partitioned by [w], but the dataset now sets no 'partition_by'"
        ),
        "{error}"
    );
    assert!(
        error.contains("restore the 'partition_by' it was written with"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repartitioning_partitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &["w"]).await;

    let error = table(&SessionContext::new(), dir.path(), &["v"])
        .await
        .expect_err("data partitioned by w must not open partitioned by v");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written partitioned by [w], but the dataset now sets 'partition_by: [v]'"
        ),
        "{error}"
    );
}
