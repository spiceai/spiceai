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
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store_occ::LocalConditionalPut;
use object_store_sim::Op;
use object_store_sim::fault::FaultStore;

/// A `runtime.state.location` and the store the runtime builds for it.
enum Location {
    /// A local directory: a durable `LocalConditionalPut` keyed by absolute paths.
    Local(std::path::PathBuf),
    /// An `s3://` bucket, in memory.
    Bucket(Arc<InMemory>),
}

impl Location {
    /// The location's store, under a `FaultStore` with no faults armed, whose
    /// log records every request the catalog and table make. Each call is a new
    /// client over the same objects, as a restart builds.
    fn store(&self) -> Arc<FaultStore> {
        let inner: Arc<dyn ObjectStore> = match self {
            Self::Local(state) => {
                std::fs::create_dir_all(state).expect("state directory");
                Arc::new(
                    LocalConditionalPut::new_durable_keyed_by_absolute_path(state)
                        .expect("durable local store"),
                )
            }
            Self::Bucket(objects) => Arc::clone(objects) as Arc<dyn ObjectStore>,
        };
        Arc::new(FaultStore::new(inner))
    }

    /// The location's key prefix in its store, and the URL the store's keys are
    /// relative to.
    fn prefix_and_root(&self) -> (String, &'static str) {
        match self {
            Self::Local(state) => (
                state.to_string_lossy().trim_matches('/').to_string(),
                "file://",
            ),
            Self::Bucket(_) => ("state".to_string(), "s3://bucket"),
        }
    }

    async fn open_catalog(&self, store: &Arc<FaultStore>) -> ObjectNativeCatalog {
        let (prefix, root) = self.prefix_and_root();
        let catalog = ObjectNativeCatalog::open_or_create_at(
            Arc::clone(store) as Arc<dyn ObjectStore>,
            Path::from(format!("{prefix}/cayenne/v1/catalog")),
            &format!("{root}/{prefix}/cayenne/v1"),
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        catalog
    }
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

async fn rows(ctx: &SessionContext, name: &str) -> BTreeMap<i64, i64> {
    let batches = ctx
        .sql(&format!("SELECT id, v FROM {name}"))
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
                "{name}: id {} scanned twice",
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
            } else if path
                .extension()
                .is_some_and(|extension| extension == "vortex")
            {
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
    placed_writes_and_reopen(&Location::Local(directory.path().join("state"))).await;
    // On disk the data files are in the datasets' folders in the location, and
    // nowhere else in the directory.
    let files = vortex_files(directory.path());
    assert!(!files.is_empty(), "no data file on disk");
    let outside: Vec<&String> = files
        .iter()
        .filter(|file| {
            !((file.starts_with("state/cayenne/v1/datasets/events-")
                || file.starts_with("state/cayenne/v1/datasets/orders-"))
                && file.contains("/data/"))
        })
        .collect();
    assert!(
        outside.is_empty(),
        "data files outside the dataset folders: {outside:?}"
    );
}

/// The same scenario in an `s3://` location, the path a local one must match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bucket_location_places_data_and_takes_the_object_store_path() {
    placed_writes_and_reopen(&Location::Bucket(Arc::new(InMemory::new()))).await;
}

/// A dataset placed in a location: its table and the URL of its data folder.
struct Placed {
    name: &'static str,
    data_url: String,
    table: Arc<CayenneTableProvider>,
}

/// Create `name` in `catalog`, placed in a new dataset folder of `location`,
/// with `id` as its primary key (upserting) when `keyed`.
async fn create_placed(
    location: &Location,
    store: &Arc<FaultStore>,
    catalog: &Arc<dyn MetadataCatalog>,
    datasets_prefix: &str,
    ctx: &SessionContext,
    name: &'static str,
    keyed: bool,
) -> Placed {
    let (_, root) = location.prefix_and_root();
    let data_url = format!("{root}/{datasets_prefix}/{}/data", new_dataset_folder(name));
    let table = Arc::new(
        crate::common::table_builder_on_store(
            Arc::clone(catalog),
            ctx.runtime_env(),
            object_store_config(store, &data_url),
        )
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: if keyed {
                vec!["id".to_string()]
            } else {
                Vec::new()
            },
            on_conflict: keyed
                .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: data_url.clone(),
            partition_column: None,
            // Every write is data files, not rows inlined into the metastore,
            // and each is a small file, three of which qualify for compaction.
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                target_vortex_file_size_mb: 1,
                compaction_trigger_files: 3,
                compaction_max_levels: 3,
                compaction_max_files_per_pick: 3,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create the table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    // Placed: the table path the metastore records is the dataset folder's
    // `data` prefix in the location, the test the table makes for
    // `data_in_object_native_location`.
    let recorded = catalog.get_table(name).await.expect("stored table").path;
    assert_eq!(
        recorded, data_url,
        "{name}: the recorded table path is the placement"
    );
    assert!(
        cayenne::metastore_layout::is_object_native_data_path(&recorded),
        "{name}: '{recorded}' is not a dataset folder's data prefix in the location"
    );
    Placed {
        name,
        data_url,
        table,
    }
}

/// Run `sql` on `placed`, check the exact rows against `expected`, and check
/// that the write took the object-store data path: it staged no file, wrote no
/// staging WAL and renamed nothing into place, and listed nothing.
async fn write(
    store: &Arc<FaultStore>,
    ctx: &SessionContext,
    placed: &Placed,
    step: &str,
    sql: &str,
    expected: &BTreeMap<i64, i64>,
) {
    let seen = store.log().len();
    run(ctx, sql).await;
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    assert_eq!(
        &rows(ctx, placed.name).await,
        expected,
        "{}: rows after {step}",
        placed.name
    );
    let staged: Vec<String> = store.log()[seen..]
        .iter()
        .filter(|applied| {
            matches!(applied.op, Op::Rename | Op::List)
                || applied.path.as_ref().contains("_staging")
                || applied.path.as_ref().ends_with("_wal.json")
        })
        .map(|applied| format!("{:?} {}", applied.op, applied.path))
        .collect();
    assert!(
        staged.is_empty(),
        "{}: {step} wrote at final keys, but staged, renamed or listed: {staged:?}",
        placed.name
    );
}

/// Two datasets placed in `location`, one keyed, one not: placement, writes at
/// final keys, and exact rows after appends, an upsert, deletes, a compaction
/// and a reopen from the location alone.
async fn placed_writes_and_reopen(location: &Location) {
    let store = location.store();
    let native = location.open_catalog(&store).await;
    let datasets_prefix = native.datasets_prefix().to_string();
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(native);
    let ctx = SessionContext::new();
    let events = create_placed(
        location,
        &store,
        &catalog,
        &datasets_prefix,
        &ctx,
        "events",
        false,
    )
    .await;
    let orders = create_placed(
        location,
        &store,
        &catalog,
        &datasets_prefix,
        &ctx,
        "orders",
        true,
    )
    .await;

    // Appends, then a compaction of the current snapshot's small files (after a
    // write, or the pass below), which rewrites it under a new snapshot id, then
    // a delete.
    let current = || async {
        catalog
            .get_table("events")
            .await
            .expect("stored table")
            .current_snapshot_id
    };
    let before_compaction = current().await;
    let mut events_rows = BTreeMap::new();
    for append in 0..5_i64 {
        let (first, last) = (append * 1000 + 1, append * 1000 + 1000);
        events_rows.extend((first..=last).map(|id| (id, id * 10)));
        write(
            &store,
            &ctx,
            &events,
            &format!("append {append}"),
            &format!("INSERT INTO events SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"),
            &events_rows,
        )
        .await;
    }
    for _ in 0..40 {
        if current().await != before_compaction
            || events
                .table
                .compact_current_snapshot_small_files()
                .await
                .expect("compact")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    events
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    assert_ne!(
        current().await,
        before_compaction,
        "the small files were compacted into a new snapshot"
    );
    assert_eq!(
        rows(&ctx, "events").await,
        events_rows,
        "events: rows after the compaction"
    );
    eprintln!(
        "copies after the compaction: {}",
        store
            .log()
            .iter()
            .filter(|applied| matches!(applied.op, Op::Copy))
            .count()
    );
    events_rows.retain(|id, _| !(1001..=1500).contains(id));
    write(
        &store,
        &ctx,
        &events,
        "delete",
        "DELETE FROM events WHERE id >= 1001 AND id <= 1500",
        &events_rows,
    )
    .await;

    // A keyed dataset: appends, an upsert over part of them and new keys, a delete.
    let mut orders_rows = BTreeMap::new();
    for append in 0..2_i64 {
        let (first, last) = (append * 1000 + 1, append * 1000 + 1000);
        orders_rows.extend((first..=last).map(|id| (id, id * 10)));
        write(
            &store,
            &ctx,
            &orders,
            &format!("append {append}"),
            &format!("INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"),
            &orders_rows,
        )
        .await;
    }
    orders_rows.extend((1500..=2500).map(|id| (id, id * 100)));
    write(
        &store,
        &ctx,
        &orders,
        "upsert",
        "INSERT INTO orders SELECT value AS id, value * 100 AS v FROM generate_series(1500, 2500)",
        &orders_rows,
    )
    .await;
    orders_rows.retain(|id, _| id % 7 != 0);
    write(
        &store,
        &ctx,
        &orders,
        "delete",
        "DELETE FROM orders WHERE id % 7 = 0",
        &orders_rows,
    )
    .await;

    // Reopen from the location alone: a new client, catalog and tables.
    let urls = [
        (events.name, events.data_url.clone(), events_rows),
        (orders.name, orders.data_url.clone(), orders_rows),
    ];
    drop(events);
    drop(orders);
    drop(ctx);
    drop(catalog);
    let store = location.store();
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(location.open_catalog(&store).await);
    let ctx = SessionContext::new();
    for (name, data_url, expected) in urls {
        let table = Arc::new(
            crate::common::table_builder_on_store(
                Arc::clone(&catalog),
                ctx.runtime_env(),
                object_store_config(&store, &data_url),
            )
            .open(name)
            .await
            .expect("reopen the table"),
        );
        table.wait_for_orphan_sweep().await;
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        assert_eq!(
            rows(&ctx, name).await,
            expected,
            "{name}: rows after the reopen"
        );
        assert_eq!(
            catalog.get_table(name).await.expect("stored table").path,
            data_url,
            "{name}: the reopened table is still placed"
        );
    }
}
