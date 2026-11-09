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

//! A table whose data files are in the object-native location: they are written
//! under its dataset folder, named in its commits, and read from the committed
//! state, so a scan lists nothing and a reopen from the store alone serves every
//! acknowledged row.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store_occ::sim::Op;
use object_store_occ::sim::fault::{Fault, FaultStore};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

const CATALOG: &str = "state/cayenne/v1/catalog";

/// An in-memory store that counts LIST requests under one prefix.
#[derive(Debug)]
struct CountingStore {
    inner: InMemory,
    watched: parking_lot::RwLock<String>,
    lists: AtomicUsize,
    /// Requests of any kind under this prefix are counted in `forbidden_hits`.
    forbidden: parking_lot::RwLock<String>,
    forbidden_hits: AtomicUsize,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            watched: parking_lot::RwLock::new(String::new()),
            lists: AtomicUsize::new(0),
            forbidden: parking_lot::RwLock::new(String::new()),
            forbidden_hits: AtomicUsize::new(0),
        }
    }

    /// Count every request under `prefix` from now on.
    fn forbid(&self, prefix: &str) {
        *self.forbidden.write() = prefix.to_string();
        self.forbidden_hits.store(0, Ordering::SeqCst);
    }

    fn forbidden_hits(&self) -> usize {
        self.forbidden_hits.load(Ordering::SeqCst)
    }

    fn note(&self, location: Option<&Path>) {
        let forbidden = self.forbidden.read();
        let location = location.map_or("", Path::as_ref);
        if !forbidden.is_empty() && (location.starts_with(forbidden.as_str()) || forbidden.starts_with(location)) {
            if std::env::var("C3_DEBUG").is_ok() {
                eprintln!("DEBUG forbidden request: '{location}'");
            }
            self.forbidden_hits.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn watch(&self, prefix: &str) {
        *self.watched.write() = prefix.to_string();
        self.lists.store(0, Ordering::SeqCst);
    }

    fn lists(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }

    fn note_list(&self, prefix: Option<&Path>) {
        let watched = self.watched.read();
        let prefix = prefix.map_or("", Path::as_ref);
        if !watched.is_empty() && (prefix.starts_with(watched.as_str()) || watched.starts_with(prefix)) {
            self.lists.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Display for CountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.note(Some(location));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.note(Some(location));
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.note(Some(location));
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
        self.note(Some(location));
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note(prefix);
        self.note_list(prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note(prefix);
        self.note_list(prefix);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.note(prefix);
        self.note_list(prefix);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.note(Some(from));
        self.note(Some(to));
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

struct Setup {
    /// The bottom store, which counts LIST requests.
    counting: Arc<CountingStore>,
    /// The store the catalog and the table use: `counting`, under faults when armed.
    store: Arc<dyn ObjectStore>,
    faults: Arc<FaultStore>,
    data_url: String,
    data_prefix: String,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn vortex_config() -> VortexConfig {
    // Every write becomes data files, not rows inlined into the metastore.
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        ..VortexConfig::default()
    }
}

async fn open_catalog(store: &Arc<dyn ObjectStore>) -> ObjectNativeCatalog {
    let catalog = ObjectNativeCatalog::open_or_create_at(Arc::clone(store), Path::from(CATALOG), "s3://bucket/state/cayenne/v1")
        .await
        .expect("open the object-native catalog");
    catalog.init().await.expect("init");
    catalog
}

fn builder(
    setup: &Setup,
    catalog: Arc<dyn MetadataCatalog>,
    ctx: &SessionContext,
) -> CayenneTableProviderBuilder {
    let url = url::Url::parse(&setup.data_url).expect("data url");
    CayenneTableProviderBuilder::new(catalog, ctx.runtime_env()).with_object_store(ObjectStoreConfig {
        url,
        store: Arc::clone(&setup.store),
    })
}

async fn create(name: &str) -> (Setup, Arc<dyn MetadataCatalog>, Arc<CayenneTableProvider>, SessionContext) {
    let counting = Arc::new(CountingStore::new());
    let faults = Arc::new(FaultStore::new(Arc::clone(&counting) as Arc<dyn ObjectStore>));
    let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
    let catalog = open_catalog(&store).await;
    let folder = new_dataset_folder(name);
    let data_prefix = format!("{}/{folder}/data", catalog.datasets_prefix());
    let setup = Setup {
        counting,
        store,
        faults,
        data_url: format!("s3://bucket/{data_prefix}"),
        data_prefix,
    };
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let ctx = SessionContext::new();
    let table = Arc::new(
        builder(&setup, Arc::clone(&catalog), &ctx)
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: setup.data_url.clone(),
                partition_column: None,
                vortex_config: vortex_config(),
            })
            .await
            .expect("create the table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    (setup, catalog, table, ctx)
}

async fn count_and_sum(ctx: &SessionContext, name: &str) -> (i64, i64) {
    let batches = ctx
        .sql(&format!("SELECT count(*), coalesce(sum(v), 0) FROM {name}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");
    let batch = &batches[0];
    let count = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("count").value(0);
    let sum = batch.column(1).as_any().downcast_ref::<Int64Array>().expect("sum").value(0);
    (count, sum)
}

async fn run(ctx: &SessionContext, sql: &str) {
    ctx.sql(sql).await.expect("plan").collect().await.expect("execute");
}

/// Sum of `v = id * 10` for `id` in `first..=last`.
fn expected_sum(first: i64, last: i64) -> i64 {
    (first..=last).map(|id| id * 10).sum()
}

async fn data_objects(setup: &Setup) -> Vec<String> {
    setup
        .counting
        .inner
        .list(Some(&Path::from(setup.data_prefix.as_str())))
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .expect("list data objects")
}

/// The `.vortex` and `.arrow` objects under `prefix` in the bottom store.
async fn objects_under(setup: &Setup, prefix: &str, extension: &str) -> Vec<String> {
    setup
        .counting
        .inner
        .list(Some(&Path::from(prefix)))
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .expect("list objects")
        .into_iter()
        .filter(|key| key.ends_with(extension))
        .collect()
}

/// A new dataset `name` placed in `setup`'s location through `catalog`, with
/// `primary_key`; returns its data prefix, its table and its session.
async fn add_dataset(
    setup: &Setup,
    catalog: &Arc<dyn MetadataCatalog>,
    name: &str,
    primary_key: &[&str],
) -> (String, String, Arc<CayenneTableProvider>, SessionContext) {
    let datasets = catalog_datasets_prefix(&setup.data_prefix);
    let data_prefix = format!("{datasets}/{}/data", new_dataset_folder(name));
    let data_url = format!("s3://bucket/{data_prefix}");
    let ctx = SessionContext::new();
    let config = if primary_key.is_empty() {
        vortex_config()
    } else {
        VortexConfig {
            deletion_mode: cayenne::metadata::DeletionMode::Key,
            compaction_trigger_files: 1_000_000,
            compaction_trigger_protected_snapshots: 1_000_000,
            compaction_trigger_snapshot_age_ms: 0,
            compaction_background_interval_ms: 0,
            ..vortex_config()
        }
    };
    let keys: Vec<String> = primary_key.iter().map(ToString::to_string).collect();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(catalog), ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(&data_url).expect("data url"),
                store: Arc::clone(&setup.store),
            })
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: keys.clone(),
                on_conflict: (!keys.is_empty()).then(|| {
                    datafusion_table_providers::util::on_conflict::OnConflict::Upsert(
                        datafusion_table_providers::util::column_reference::ColumnReference::new(
                            keys.clone(),
                        ),
                    )
                }),
                base_path: data_url.clone(),
                partition_column: None,
                vortex_config: config,
            })
            .await
            .expect("create the table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    (data_url, data_prefix, table, ctx)
}

/// `<location>/cayenne/v1/datasets` from a dataset's data prefix.
fn catalog_datasets_prefix(data_prefix: &str) -> String {
    let (datasets, _) = data_prefix
        .rsplit_once('/')
        .and_then(|(rest, _)| rest.rsplit_once('/'))
        .expect("a dataset data prefix");
    datasets.to_string()
}

/// A catalog and a table over `store`'s objects, as a restart opens them.
async fn reopen_in(
    store: &Arc<dyn ObjectStore>,
    data_url: &str,
    name: &str,
) -> (Arc<dyn MetadataCatalog>, Arc<CayenneTableProvider>, SessionContext) {
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(store).await);
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(store),
            })
            .open(name)
            .await
            .expect("reopen the table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    (catalog, table, ctx)
}

/// The exact text of a query over a committed file the store lost.
fn missing_text(dataset: &str, kind: &str, object: &str, location: &str, consequence: &str) -> String {
    format!(
        "Failed to query dataset '{dataset}' (cayenne): {kind} '{object}' is missing from '{location}', so {consequence}. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    )
}

const PARTIAL: &str = "the query fails rather than return partial results";

/// Appends and a full refresh: after each acknowledged write a scan returns the
/// exact rows without listing the data prefix, every data file is under the
/// dataset folder, and a reopen from the store alone serves the same rows.
#[tokio::test]
async fn writes_scan_their_committed_files_and_reopen_from_the_store() {
    let (setup, catalog, _table, ctx) = create("orders").await;
    assert_eq!(
        catalog.get_table("orders").await.expect("stored table").path,
        setup.data_url,
        "the table path the metastore records is the placement"
    );

    let mut last = 0;
    for batch in 0..3 {
        let first = last + 1;
        last = first + 999;
        run(
            &ctx,
            &format!("INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"),
        )
        .await;
        setup.counting.watch(&setup.data_prefix);
        assert_eq!(
            count_and_sum(&ctx, "orders").await,
            (last, expected_sum(1, last)),
            "rows after append {batch}"
        );
        assert_eq!(setup.counting.lists(), 0, "a scan after append {batch} listed the data prefix");
    }

    run(
        &ctx,
        "INSERT OVERWRITE orders SELECT value AS id, value * 10 AS v FROM generate_series(1, 500)",
    )
    .await;
    setup.counting.watch(&setup.data_prefix);
    assert_eq!(count_and_sum(&ctx, "orders").await, (500, expected_sum(1, 500)));
    assert_eq!(setup.counting.lists(), 0, "a scan after the full refresh listed the data prefix");

    let objects = data_objects(&setup).await;
    assert!(
        objects.iter().any(|key| key.ends_with(".vortex")),
        "data files are under the dataset folder: {objects:?}"
    );
    let all: Vec<String> = setup
        .counting
        .inner
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .expect("list everything");
    let outside: Vec<&String> = all
        .iter()
        .filter(|key| key.ends_with(".vortex") && !key.starts_with(&setup.data_prefix))
        .collect();
    assert!(outside.is_empty(), "data files outside the dataset folder: {outside:?}");

    // Reopen from the store alone: a new catalog and a new table over the same objects.
    drop(ctx);
    drop(catalog);
    let reopened: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&setup.store).await);
    let ctx = SessionContext::new();
    let table = Arc::new(
        builder(&setup, reopened, &ctx)
            .open("orders")
            .await
            .expect("reopen the table"),
    );
    ctx.register_table("orders", Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    setup.counting.watch(&setup.data_prefix);
    assert_eq!(count_and_sum(&ctx, "orders").await, (500, expected_sum(1, 500)));
    assert_eq!(setup.counting.lists(), 0, "a scan after reopening listed the data prefix");
}

/// A data file the committed state names but the store lost is an error, never
/// fewer rows.
#[tokio::test]
async fn a_missing_data_file_is_an_error_not_fewer_rows() {
    let (setup, catalog, table, ctx) = create("events").await;
    run(
        &ctx,
        "INSERT INTO events SELECT value AS id, value * 10 AS v FROM generate_series(1, 2000)",
    )
    .await;
    let victim = data_objects(&setup)
        .await
        .into_iter()
        .find(|key| key.ends_with(".vortex"))
        .expect("a data file");
    setup
        .counting
        .inner
        .delete(&Path::from(victim.as_str()))
        .await
        .expect("delete one data file");

    // Reopen from the store, so nothing this process cached from the write hides
    // the loss, and read every row.
    drop((table, ctx, catalog));
    let reopened: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&setup.store).await);
    let ctx = SessionContext::new();
    let table = Arc::new(
        builder(&setup, reopened, &ctx)
            .open("events")
            .await
            .expect("reopen the table"),
    );
    ctx.register_table("events", table as Arc<dyn TableProvider>)
        .expect("register");
    let result = ctx
        .sql("SELECT id, v FROM events")
        .await
        .expect("plan")
        .collect()
        .await;
    let error = match result {
        Ok(batches) => panic!(
            "a scan over a lost data file returned {} rows instead of failing",
            batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>()
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    let object = victim
        .strip_prefix(&format!("{}/", setup.data_prefix))
        .expect("the file is under the data prefix");
    let expected = missing_text("events", "data file", object, &setup.data_url, PARTIAL);
    assert!(
        message.contains(&expected),
        "the error is the missing-data-file text\n expected: {expected}\n got: {message}"
    );
}

/// A data file the committed state names that is shorter than its commit
/// recorded is an error that says it is unreadable, never fewer rows.
#[tokio::test]
async fn a_truncated_data_file_is_an_error_not_fewer_rows() {
    let (setup, catalog, table, ctx) = create("truncated").await;
    run(
        &ctx,
        "INSERT INTO truncated SELECT value AS id, value * 10 AS v FROM generate_series(1, 2000)",
    )
    .await;
    let victim = data_objects(&setup)
        .await
        .into_iter()
        .find(|key| key.ends_with(".vortex"))
        .expect("a data file");
    let location = Path::from(victim.as_str());
    let bytes = setup
        .counting
        .inner
        .get(&location)
        .await
        .expect("read the data file")
        .bytes()
        .await
        .expect("bytes");
    let kept = bytes.len() / 2;
    setup
        .counting
        .inner
        .put(&location, bytes.slice(..kept).into())
        .await
        .expect("truncate the data file");

    drop((table, ctx, catalog));
    let (_catalog, _table, ctx) = reopen_in(&setup.store, &setup.data_url, "truncated").await;
    let result = ctx
        .sql("SELECT id, v FROM truncated")
        .await
        .expect("plan")
        .collect()
        .await;
    let error = match result {
        Ok(batches) => panic!(
            "a scan over a truncated data file returned {} rows instead of failing",
            batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>()
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    let object = victim
        .strip_prefix(&format!("{}/", setup.data_prefix))
        .expect("the file is under the data prefix");
    let head = format!(
        "Failed to query dataset 'truncated' (cayenne): data file '{object}' in '{}' is unreadable (it has {kept} bytes, fewer than the ",
        setup.data_url
    );
    let tail = format!(
        " the read needs, so it was truncated), so {PARTIAL}. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    );
    let at = message
        .find(&head)
        .unwrap_or_else(|| panic!("the error is the unreadable-data-file text\n expected: {head}…{tail}\n got: {message}"));
    let rest = &message[at + head.len()..];
    let needed_end = rest.find(&tail).unwrap_or_else(|| {
        panic!("the error is the unreadable-data-file text\n expected: {head}…{tail}\n got: {message}")
    });
    let needed: u64 = rest[..needed_end]
        .parse()
        .unwrap_or_else(|_| panic!("a byte count between the two parts: {message}"));
    assert!(
        needed > u64::try_from(kept).expect("size"),
        "the read needed more than the {kept} bytes left: {message}"
    );
}

/// A deletion vector a dataset in the location still names, missing from the
/// store, fails the dataset's load with the deletion-file text: reading on
/// without it would bring back the rows it deletes.
#[tokio::test]
async fn a_missing_deletion_vector_of_a_placed_dataset_is_an_error() {
    let (setup, catalog, _table, _ctx) = create("unused").await;
    let (data_url, data_prefix, table, ctx) =
        add_dataset(&setup, &catalog, "keyed", &["id"]).await;
    run(&ctx, "INSERT INTO keyed VALUES (1, 10), (2, 20)").await;
    run(&ctx, "INSERT INTO keyed VALUES (1, 11)").await;
    if std::env::var("C3_DEBUG").is_ok() {
        let stored = catalog.get_table("keyed").await.expect("t");
        eprintln!("DEBUG current={} manifest={:?}", stored.current_snapshot_id, catalog.get_all_snapshot_files(&stored.table_id).await.expect("m").iter().map(|f| format!("{}/{}", f.snapshot_id, f.file_path)).collect::<Vec<_>>());
        eprintln!("DEBUG protected={:?}", catalog.get_all_snapshot_sequences(&stored.table_id).await.expect("s"));
        eprintln!("DEBUG objects={:?}", objects_under(&setup, &data_prefix, "").await);
        eprintln!("DEBUG inline={:?}", catalog.get_inlined_data_count(&stored.table_id).await);
    }
    assert_eq!(count_and_sum(&ctx, "keyed").await, (2, 31));
    let vectors = objects_under(&setup, &data_prefix, ".arrow").await;
    let victim = vectors.first().expect("the upsert wrote a deletion vector").clone();
    setup
        .counting
        .inner
        .delete(&Path::from(victim.as_str()))
        .await
        .expect("delete the deletion vector");

    drop((table, ctx, catalog));
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&setup.store).await);
    let ctx = SessionContext::new();
    let opened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .with_object_store(ObjectStoreConfig {
            url: url::Url::parse(&data_url).expect("data url"),
            store: Arc::clone(&setup.store),
        })
        .open("keyed")
        .await;
    let message = match opened {
        Err(error) => error.to_string(),
        Ok(table) => {
            ctx.register_table("keyed", Arc::new(table) as Arc<dyn TableProvider>)
                .expect("register");
            match ctx.sql("SELECT id, v FROM keyed").await.expect("plan").collect().await {
                Ok(batches) => panic!(
                    "a dataset that lost a deletion vector served {} rows",
                    batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>()
                ),
                Err(error) => error.to_string(),
            }
        }
    };
    let object = victim
        .strip_prefix(&format!("{data_prefix}/"))
        .expect("the vector is under the data prefix");
    let expected = missing_text(
        "keyed",
        "deletion file",
        object,
        &data_url,
        "the query fails rather than return rows the dataset deleted",
    );
    assert!(
        message.contains(&expected),
        "the error is the missing-deletion-file text\n expected: {expected}\n got: {message}"
    );
}

/// A copy of the location's objects at another prefix, opened there, serves the
/// same rows from its own objects and sends no request to the old prefix: the
/// dataset's table path is recorded relative to the location.
#[tokio::test]
async fn a_copied_location_serves_the_same_rows_from_its_own_objects() {
    let (setup, catalog, table, ctx) = create("orders").await;
    for (first, last) in [(1, 3000), (3001, 5000)] {
        run(
            &ctx,
            &format!("INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"),
        )
        .await;
    }
    let expected = count_and_sum(&ctx, "orders").await;
    assert_eq!(expected, (5000, expected_sum(1, 5000)));
    // The first instance stops before the copy, as a copied location's would.
    table.flush_pending_maintenance().await.expect("maintenance");
    table.drain_in_flight_maintenance().await.expect("drain");
    drop((table, ctx, catalog));

    let objects: Vec<ObjectMeta> = setup
        .counting
        .inner
        .list(Some(&Path::from("state")))
        .try_collect()
        .await
        .expect("list the location");
    assert!(!objects.is_empty());
    for meta in &objects {
        let rest = meta
            .location
            .as_ref()
            .strip_prefix("state/")
            .expect("under the location");
        setup
            .counting
            .inner
            .copy(&meta.location, &Path::from(format!("moved/{rest}")))
            .await
            .expect("copy an object");
    }
    setup.counting.forbid("state/");

    let moved = ObjectNativeCatalog::open_or_create_at(
        Arc::clone(&setup.store),
        Path::from("moved/cayenne/v1/catalog"),
        "s3://bucket/moved/cayenne/v1",
    )
    .await
    .expect("open the copied catalog");
    moved.init().await.expect("init");
    let moved_url = setup.data_url.replacen("/state/", "/moved/", 1);
    assert_eq!(
        moved.get_table("orders").await.expect("stored table").path,
        moved_url,
        "the table path resolves against the location it was opened at"
    );
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(moved);
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(&moved_url).expect("data url"),
                store: Arc::clone(&setup.store),
            })
            .open("orders")
            .await
            .expect("open the copied table"),
    );
    ctx.register_table("orders", table as Arc<dyn TableProvider>)
        .expect("register");
    assert_eq!(count_and_sum(&ctx, "orders").await, expected);
    run(
        &ctx,
        "INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series(5001, 6000)",
    )
    .await;
    assert_eq!(count_and_sum(&ctx, "orders").await, (6000, expected_sum(1, 6000)));
    assert_eq!(
        setup.counting.forbidden_hits(),
        0,
        "the copied location sent requests to the old one"
    );
}

/// A file planted in a placed dataset's live snapshot directory, named by no
/// commit, is never read: an empty committed file set scans as empty, and
/// neither the post-write manifest rebuild nor a reopen adopts the file.
#[tokio::test]
async fn a_planted_file_is_never_adopted_by_a_placed_dataset() {
    const ORPHAN_FIRST: i64 = 1_000_001;
    let (setup, catalog, table, ctx) = create("victim").await;
    let (_, donor_prefix, _donor, donor_ctx) = add_dataset(&setup, &catalog, "donor", &[]).await;
    run(
        &donor_ctx,
        &format!("INSERT INTO donor SELECT value AS id, value * 10 AS v FROM generate_series({ORPHAN_FIRST}, {})", ORPHAN_FIRST + 999),
    )
    .await;
    let donor_file = objects_under(&setup, &donor_prefix, ".vortex").await;
    let donor_file = donor_file.first().expect("the donor wrote a data file");
    let bytes = setup
        .counting
        .inner
        .get(&Path::from(donor_file.as_str()))
        .await
        .expect("read the donor file")
        .bytes()
        .await
        .expect("bytes");
    let stored = catalog.get_table("victim").await.expect("stored table");
    let orphan = format!("orphan-{}.vortex", uuid::Uuid::now_v7().simple());
    setup
        .counting
        .inner
        .put(
            &Path::from(format!(
                "{}/{}/{}/{orphan}",
                setup.data_prefix, stored.table_id, stored.current_snapshot_id
            )),
            bytes.into(),
        )
        .await
        .expect("plant a file");
    let orphan_rows = |ctx: SessionContext| async move {
        let batches = ctx
            .sql(&format!("SELECT count(*) FROM victim WHERE id >= {ORPHAN_FIRST}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0)
    };
    let manifest_names = |catalog: Arc<dyn MetadataCatalog>, table_id: String| async move {
        catalog
            .get_all_snapshot_files(&table_id)
            .await
            .expect("manifest")
            .into_iter()
            .map(|file| file.file_path)
            .collect::<Vec<_>>()
    };

    // An empty committed file set scans as empty, without listing.
    setup.counting.watch(&setup.data_prefix);
    assert_eq!(count_and_sum(&ctx, "victim").await, (0, 0), "empty committed file set");
    assert_eq!(setup.counting.lists(), 0, "the scan of an empty committed file set listed");

    // A write, then its post-write maintenance: the manifest names only the write's files.
    run(
        &ctx,
        "INSERT INTO victim SELECT value AS id, value * 10 AS v FROM generate_series(1, 1000)",
    )
    .await;
    table.flush_pending_maintenance().await.expect("maintenance");
    table.drain_in_flight_maintenance().await.expect("drain");
    assert_eq!(count_and_sum(&ctx, "victim").await, (1000, expected_sum(1, 1000)));
    assert_eq!(orphan_rows(ctx.clone()).await, 0, "after a write");
    assert!(
        !manifest_names(Arc::clone(&catalog), stored.table_id.clone()).await.contains(&orphan),
        "the post-write maintenance adopted the planted file"
    );

    // A reopen, then the committed file set emptied: the open adopts nothing, and
    // the empty set scans as empty rather than falling back to the directory.
    drop((table, ctx, catalog));
    let (catalog, table, ctx) = reopen_in(&setup.store, &setup.data_url, "victim").await;
    assert_eq!(count_and_sum(&ctx, "victim").await, (1000, expected_sum(1, 1000)));
    assert!(!manifest_names(Arc::clone(&catalog), stored.table_id.clone()).await.contains(&orphan));
    catalog
        .clear_snapshot_files_except(&stored.table_id, "no-such-snapshot")
        .await
        .expect("empty the committed file set");
    drop((table, ctx, catalog));
    let (catalog, _table, ctx) = reopen_in(&setup.store, &setup.data_url, "victim").await;
    setup.counting.watch(&setup.data_prefix);
    assert_eq!(
        count_and_sum(&ctx, "victim").await,
        (0, 0),
        "an empty committed file set after a reopen scans as empty"
    );
    assert_eq!(setup.counting.lists(), 0, "the scan listed the data prefix");
    assert!(
        manifest_names(catalog, stored.table_id.clone()).await.is_empty(),
        "the reopen backfilled the manifest from a listing"
    );
}


/// A write whose files fail to reach their snapshot is not visible, and after a
/// reopen it is all there or not there at all, never part of it.
#[tokio::test]
async fn a_failed_move_is_invisible_and_resolves_all_or_nothing() {
    let (setup, catalog, table, ctx) = create("ledger").await;
    run(
        &ctx,
        "INSERT INTO ledger SELECT value AS id, value * 10 AS v FROM generate_series(1, 1000)",
    )
    .await;
    assert_eq!(count_and_sum(&ctx, "ledger").await, (1000, expected_sum(1, 1000)));

    // The next write's copy from staging into the snapshot fails before I/O.
    let data_prefix = setup.data_prefix.clone();
    setup.faults.arm(
        Op::Copy,
        move |path: &Path| path.as_ref().starts_with(&data_prefix),
        Fault::Fail,
    );
    let failed = ctx
        .sql("INSERT INTO ledger SELECT value AS id, value * 10 AS v FROM generate_series(1001, 3000)")
        .await
        .expect("plan")
        .collect()
        .await;
    assert!(failed.is_err(), "the write whose move failed reports an error");
    assert_eq!(
        count_and_sum(&ctx, "ledger").await,
        (1000, expected_sum(1, 1000)),
        "none of the failed write's rows are visible"
    );

    drop((table, ctx, catalog));
    let reopened: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&setup.store).await);
    let ctx = SessionContext::new();
    let table = Arc::new(
        builder(&setup, reopened, &ctx)
            .open("ledger")
            .await
            .expect("reopen the table"),
    );
    ctx.register_table("ledger", table as Arc<dyn TableProvider>)
        .expect("register");
    let after = count_and_sum(&ctx, "ledger").await;
    assert!(
        after == (1000, expected_sum(1, 1000)) || after == (3000, expected_sum(1, 3000)),
        "after a reopen the failed write is all there or not at all: {after:?}"
    );
    eprintln!("a_failed_move: after reopen {after:?}");
}


/// Whether `key` is a deletion vector (`….arrow`).
fn is_deletion_vector(key: &str) -> bool {
    std::path::Path::new(key)
        .extension()
        .is_some_and(|extension| extension == "arrow")
}

/// What a reclaim of orphaned deletion vectors leaves when every object delete
/// fails: the deletion-vector rows the catalog still names, and the `.arrow`
/// objects in storage, before and after.
struct SweepOutcome {
    rows_before: usize,
    rows_after: usize,
    objects_before: usize,
    objects_after: usize,
}

/// 26 upserts of one key on a key-delete table (25 deletion vectors), a
/// merge of its protected snapshots that leaves the vectors orphaned, then the
/// orphaned-deletion-vector reclaim with every `.arrow` delete failing.
async fn sweep_with_failing_deletes(
    catalog: Arc<dyn MetadataCatalog>,
    faults: &Arc<FaultStore>,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    name: &str,
) -> SweepOutcome {
    use cayenne::CayenneContext;
    let parked = VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        deletion_mode: cayenne::metadata::DeletionMode::Key,
        compaction_trigger_files: 1_000_000,
        compaction_trigger_protected_snapshots: 1_000_000,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    let config = ObjectStoreConfig {
        url: url::Url::parse(base_path).expect("data url"),
        store: Arc::clone(&store),
    };
    let ctx = SessionContext::new();
    let table = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .with_object_store(config.clone())
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(datafusion_table_providers::util::on_conflict::OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            )),
            base_path: base_path.to_string(),
            partition_column: None,
            vortex_config: parked.clone(),
        })
        .await
        .expect("create the table");
    let table = Arc::new(table);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    for value in 0..26 {
        run(&ctx, &format!("INSERT INTO {name} VALUES (1, {value})")).await;
    }
    table.flush_pending_maintenance().await.expect("maintenance");
    table.drain_in_flight_maintenance().await.expect("drain");
    let table_id = table.table_id().to_string();
    drop((table, ctx));

    let merging = VortexConfig {
        compaction_trigger_protected_snapshots: VortexConfig::default()
            .compaction_trigger_protected_snapshots,
        ..parked
    };
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .with_object_store(config)
            .with_context(CayenneContext::new(&merging, ctx.runtime_env(), name))
            .open(name)
            .await
            .expect("reopen the table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    table.drain_in_flight_maintenance().await.expect("drain");
    let arrow_objects = |store: Arc<dyn ObjectStore>| async move {
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .into_iter()
            .filter(|meta| is_deletion_vector(meta.location.as_ref()))
            .count()
    };
    let rows_before = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("deletion vectors")
        .len();
    let objects_before = arrow_objects(Arc::clone(&store)).await;

    faults.fail_while(Op::Delete, |path: &Path| is_deletion_vector(path.as_ref()));
    assert!(
        table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge"),
        "the protected-snapshot merge commits"
    );
    table.drain_orphan_dv_sweep(1).await;
    table.drain_in_flight_maintenance().await.expect("drain");
    let rows_after = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("deletion vectors")
        .len();
    let objects_after = arrow_objects(Arc::clone(&store)).await;
    faults.clear_rules();
    assert_eq!(count_and_sum(&ctx, name).await, (1, 25), "rows after the reclaim");
    SweepOutcome {
        rows_before,
        rows_after,
        objects_before,
        objects_after,
    }
}

/// A dataset in the location removes an orphaned deletion vector's row before
/// its object: when the object delete fails the row is gone and the object is
/// left for later reclamation, and a reopen does not miss it.
#[tokio::test]
async fn a_placed_dataset_reclaims_orphaned_deletion_vectors_rows_first() {
    let (setup, catalog, _table, _ctx) = create("unused").await;
    let datasets = catalog_datasets_prefix(&setup.data_prefix);
    let data_url = format!("s3://bucket/{datasets}/{}/data", new_dataset_folder("swept"));
    let outcome = sweep_with_failing_deletes(
        Arc::clone(&catalog),
        &setup.faults,
        Arc::clone(&setup.store),
        &data_url,
        "swept",
    )
    .await;
    assert!(outcome.rows_before >= 25, "the upserts wrote deletion vectors: {}", outcome.rows_before);
    assert_eq!(outcome.objects_before, outcome.rows_before);
    assert_eq!(
        outcome.rows_after, 0,
        "the reclaim removed every orphaned row although no object delete succeeded"
    );
    assert_eq!(
        outcome.objects_after, outcome.objects_before,
        "no object was deleted, so all are left"
    );
    drop(catalog);
    let (_catalog, _table, ctx) = reopen_in(&setup.store, &data_url, "swept").await;
    assert_eq!(count_and_sum(&ctx, "swept").await, (1, 25), "rows after a reopen");
}

/// A dataset outside the location keeps today's order, object first: when the
/// object delete fails the row stays, so a later sweep retries it.
#[tokio::test]
async fn an_unplaced_dataset_reclaims_orphaned_deletion_vectors_objects_first() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
    let fixture = crate::common::TestFixture::with_data_store(
        crate::common::BackendType::Sqlite,
        Some(Arc::clone(&store)),
    )
    .await
    .expect("fixture");
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let outcome = sweep_with_failing_deletes(
        catalog,
        &faults,
        store,
        &fixture.data_store.base_path,
        "swept",
    )
    .await;
    assert!(outcome.rows_before >= 25, "the upserts wrote deletion vectors: {}", outcome.rows_before);
    assert_eq!(
        outcome.rows_after, outcome.rows_before,
        "a failed object delete keeps its row"
    );
    assert_eq!(outcome.objects_after, outcome.objects_before);
}
