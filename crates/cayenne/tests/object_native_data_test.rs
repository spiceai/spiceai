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
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            watched: parking_lot::RwLock::new(String::new()),
            lists: AtomicUsize::new(0),
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
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note_list(prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note_list(prefix);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.note_list(prefix);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
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
    let file_name = victim.rsplit('/').next().expect("file name");
    assert!(
        message.contains(file_name),
        "the error names the lost file '{file_name}': {message}"
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

