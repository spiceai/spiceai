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

//! A dataset whose `runtime.state.location` is a local directory is placed in
//! its dataset folder there and takes the same data path as one in an `s3://`
//! location: the table path is the folder's `data` prefix, every write puts its
//! files once at their final keys (no staging prefix, staging WAL, copy or
//! rename), and a reopen from the directory alone serves the exact rows after
//! appends, upserts, deletes and a compaction.
//!
//! The store is the one the runtime builds for a `file://` location
//! (`runtime_state_location::build_object_store`): a durable
//! `LocalConditionalPut` keyed by absolute paths, so the `file://` URL of the
//! dataset folder names the objects the catalog's keys name.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use object_store::ObjectStore;
use object_store::path::Path;
use object_store_occ::LocalConditionalPut;
use object_store_occ::sim::Op;
use object_store_occ::sim::fault::FaultStore;

const NAME: &str = "orders";

/// The location's store as the runtime builds it, under a `FaultStore` with no
/// faults armed, whose log records every request the catalog and table make.
fn location_store(state: &std::path::Path) -> Arc<FaultStore> {
    std::fs::create_dir_all(state).expect("state directory");
    let local = LocalConditionalPut::new_durable_keyed_by_absolute_path(state)
        .expect("durable local store");
    Arc::new(FaultStore::new(Arc::new(local)))
}

/// `state` as the store keys it: its absolute path without the leading `/`.
fn state_key(state: &std::path::Path) -> String {
    state.to_string_lossy().trim_matches('/').to_string()
}

async fn open_catalog(store: &Arc<FaultStore>, state: &std::path::Path) -> ObjectNativeCatalog {
    let key = state_key(state);
    let catalog = ObjectNativeCatalog::open_or_create_at(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        Path::from(format!("{key}/cayenne/v1/catalog")),
        &format!("file:///{key}/cayenne/v1"),
    )
    .await
    .expect("open the object-native catalog");
    catalog.init().await.expect("init");
    catalog
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn object_store_config(store: &Arc<FaultStore>, data_url: &str) -> ObjectStoreConfig {
    ObjectStoreConfig {
        url: url::Url::parse(data_url).expect("data url"),
        store: Arc::clone(store) as Arc<dyn ObjectStore>,
    }
}

async fn rows(ctx: &SessionContext) -> BTreeMap<i64, i64> {
    let batches = ctx
        .sql(&format!("SELECT id, v FROM {NAME}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("scan");
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v column");
        for row in 0..batch.num_rows() {
            assert!(
                rows.insert(ids.value(row), values.value(row)).is_none(),
                "id {} scanned twice",
                ids.value(row)
            );
        }
    }
    rows
}

async fn run(ctx: &SessionContext, sql: &str) {
    ctx.sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");
}

/// Every `.vortex` file under `directory`, as paths relative to it.
fn vortex_files(directory: &std::path::Path) -> Vec<String> {
    fn walk(root: &std::path::Path, directory: &std::path::Path, found: &mut Vec<String>) {
        for entry in std::fs::read_dir(directory).expect("read directory") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                walk(root, &path, found);
            } else if path.extension().is_some_and(|extension| extension == "vortex") {
                found.push(
                    path.strip_prefix(root)
                        .expect("under the root")
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }
    let mut found = Vec::new();
    walk(directory, directory, &mut found);
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_location_places_data_and_takes_the_object_store_path() {
    let directory = tempfile::tempdir().expect("temp dir");
    let state = directory.path().join("state");
    let store = location_store(&state);
    let catalog = open_catalog(&store, &state).await;
    let folder = new_dataset_folder(NAME);
    let data_url = format!("file:///{}/{folder}/data", catalog.datasets_prefix());
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let ctx = SessionContext::new();
    let table = Arc::new(
        crate::common::table_builder_on_store(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            object_store_config(&store, &data_url),
        )
        .create(CreateTableOptions {
            table_name: NAME.to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_url.clone(),
            partition_column: None,
            // Every write becomes data files, not rows inlined into the metastore.
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create the table"),
    );
    ctx.register_table(NAME, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");

    // Placed: the table path the metastore records is the dataset folder's
    // `data` prefix in the location, the test the table makes for
    // `data_in_object_native_location`.
    let recorded = catalog.get_table(NAME).await.expect("stored table").path;
    assert_eq!(recorded, data_url, "the recorded table path is the placement");
    assert!(
        cayenne::metastore_layout::is_object_native_data_path(&recorded),
        "'{recorded}' is not a dataset folder's data prefix in the location"
    );

    // Appends, upserts and deletes, each checked against a model.
    let mut expected = BTreeMap::new();
    run(
        &ctx,
        &format!(
            "INSERT INTO {NAME} SELECT value AS id, value * 10 AS v FROM generate_series(1, 1000)"
        ),
    )
    .await;
    expected.extend((1..=1000).map(|id| (id, id * 10)));
    assert_eq!(rows(&ctx).await, expected, "after the first append");
    run(
        &ctx,
        &format!(
            "INSERT INTO {NAME} SELECT value AS id, value * 10 AS v FROM generate_series(1001, 2000)"
        ),
    )
    .await;
    expected.extend((1001..=2000).map(|id| (id, id * 10)));
    assert_eq!(rows(&ctx).await, expected, "after the second append");
    run(
        &ctx,
        &format!(
            "INSERT INTO {NAME} SELECT value AS id, value * 100 AS v FROM generate_series(1500, 2500)"
        ),
    )
    .await;
    expected.extend((1500..=2500).map(|id| (id, id * 100)));
    assert_eq!(rows(&ctx).await, expected, "after the upsert");
    run(&ctx, &format!("DELETE FROM {NAME} WHERE id % 7 = 0")).await;
    expected.retain(|id, _| id % 7 != 0);
    assert_eq!(rows(&ctx).await, expected, "after the delete");

    // A compaction of the snapshot's small files keeps the exact rows.
    let compacted = table
        .compact_current_snapshot_small_files()
        .await
        .expect("compact");
    assert!(compacted, "the compaction had small files to merge");
    assert_eq!(rows(&ctx).await, expected, "after the compaction");

    // The object-store data path: no request staged a file, wrote a staging
    // WAL, or copied or renamed one.
    let log = store.log();
    let staged: Vec<String> = log
        .iter()
        .filter(|applied| {
            matches!(applied.op, Op::Copy)
                || applied.path.as_ref().contains("_staging")
                || applied.path.as_ref().ends_with("_wal.json")
        })
        .map(|applied| format!("{:?} {}", applied.op, applied.path))
        .collect();
    assert!(
        staged.is_empty(),
        "writes at final keys stage, copy and rename nothing: {staged:?}"
    );
    let data_puts = log
        .iter()
        .filter(|applied| {
            applied.path.as_ref().ends_with(".vortex")
                && applied.path.as_ref().contains(&format!("/{folder}/data/"))
        })
        .count();
    assert!(data_puts > 0, "the writes put data files in the dataset folder");

    // On disk the data files are in the dataset folder in the location, and
    // nowhere else in the directory.
    let files = vortex_files(directory.path());
    assert!(!files.is_empty(), "no data file on disk");
    let outside: Vec<&String> = files
        .iter()
        .filter(|file| !file.starts_with(&format!("state/cayenne/v1/datasets/{folder}/data/")))
        .collect();
    assert!(
        outside.is_empty(),
        "data files outside the dataset folder: {outside:?}"
    );

    // Reopen from the directory alone: a new store, catalog and table.
    drop(ctx);
    drop(table);
    drop(catalog);
    let store = location_store(&state);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&store, &state).await);
    let ctx = SessionContext::new();
    let table: Arc<CayenneTableProvider> = Arc::new(
        crate::common::table_builder_on_store(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            object_store_config(&store, &data_url),
        )
        .open(NAME)
        .await
        .expect("reopen the table"),
    );
    table.wait_for_orphan_sweep().await;
    ctx.register_table(NAME, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    assert_eq!(rows(&ctx).await, expected, "after the reopen");
    assert_eq!(
        catalog.get_table(NAME).await.expect("stored table").path,
        data_url,
        "the reopened table is still placed"
    );
}
