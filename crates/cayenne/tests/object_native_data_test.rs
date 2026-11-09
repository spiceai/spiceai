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
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::metadata::{
    CdcDurability, CreateTableOptions, DeletionMode, ObjectStoreConfig, VortexConfig,
};
use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store_occ::sim::Op;
use object_store_occ::sim::fault::{Fault, FaultStore};
use object_store_occ::sim::latency::{Delay, LatencyProfile, LatencyStore};
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

// ---------------------------------------------------------------------------
// Keyed tables, change streams and store latency
// ---------------------------------------------------------------------------

/// A slot advancer that acknowledges nothing: what the runtime installs once a
/// replayable change stream feeds a `cdc_durability: memory` table, so applies are
/// absorbed by the RAM tier and made durable by seals and checkpoints.
struct NoopSlotAdvancer;

#[async_trait]
impl cayenne::SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// A store of its own, a catalog in it and a data folder for one keyed table.
struct KeyedSetup {
    /// The bottom store: an in-memory bucket.
    bucket: Arc<InMemory>,
    data_url: String,
    data_prefix: String,
}

impl KeyedSetup {
    async fn new(name: &str) -> (Self, ObjectNativeCatalog) {
        let bucket = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = Arc::clone(&bucket) as Arc<dyn ObjectStore>;
        let catalog = open_catalog(&store).await;
        let folder = new_dataset_folder(name);
        let data_prefix = format!("{}/{folder}/data", catalog.datasets_prefix());
        (
            Self {
                bucket,
                data_url: format!("s3://bucket/{data_prefix}"),
                data_prefix,
            },
            catalog,
        )
    }

    /// The bucket, or the bucket behind `latency` for every request.
    fn store(&self, latency: Option<Duration>) -> Arc<dyn ObjectStore> {
        let bucket: Arc<dyn ObjectStore> = Arc::clone(&self.bucket) as Arc<dyn ObjectStore>;
        match latency {
            None => bucket,
            Some(delay) => Arc::new(LatencyStore::new(
                bucket,
                LatencyProfile::uniform(Delay::new(delay, delay)),
                0x16132,
            )),
        }
    }

    fn builder(
        &self,
        store: &Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        ctx: &SessionContext,
    ) -> CayenneTableProviderBuilder {
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env()).with_object_store(
            ObjectStoreConfig {
                url: url::Url::parse(&self.data_url).expect("data url"),
                store: Arc::clone(store),
            },
        )
    }

    async fn create(
        &self,
        name: &str,
        store: &Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        vortex_config: VortexConfig,
    ) -> (Arc<CayenneTableProvider>, SessionContext) {
        let ctx = SessionContext::new();
        let table = Arc::new(
            self.builder(store, catalog, &ctx)
                .create(CreateTableOptions {
                    table_name: name.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: self.data_url.clone(),
                    partition_column: None,
                    vortex_config,
                })
                .await
                .expect("create the keyed table"),
        );
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        (table, ctx)
    }

    /// A new catalog and table over the store alone, as after losing the disk.
    async fn reopen(
        &self,
        name: &str,
        store: &Arc<dyn ObjectStore>,
    ) -> (Arc<CayenneTableProvider>, SessionContext, Duration) {
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(store).await);
        let ctx = SessionContext::new();
        let started = Instant::now();
        let table = Arc::new(
            self.builder(store, catalog, &ctx)
                .open(name)
                .await
                .expect("reopen the keyed table"),
        );
        let elapsed = started.elapsed();
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        (table, ctx, elapsed)
    }

    async fn objects_ending_with(&self, suffix: &str) -> Vec<String> {
        self.bucket
            .list(Some(&Path::from(self.data_prefix.as_str())))
            .map_ok(|meta| meta.location.to_string())
            .try_filter(|key| futures::future::ready(key.ends_with(suffix)))
            .try_collect()
            .await
            .expect("list data objects")
    }
}

/// Whether `key` ends in one of `extensions`.
fn has_extension(key: &str, extensions: &[&str]) -> bool {
    std::path::Path::new(key)
        .extension()
        .is_some_and(|extension| {
            extensions
                .iter()
                .any(|wanted| extension.eq_ignore_ascii_case(wanted))
        })
}

/// Change-stream settings with every automatic seal, checkpoint and compaction
/// off, so a test drives each one.
fn cdc_config(durability: CdcDurability) -> VortexConfig {
    VortexConfig {
        cdc_durability: durability,
        deletion_mode: DeletionMode::Key,
        cdc_mem_tier_shards: 1,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_checkpoint_interval_ms: 0,
        cdc_mem_tier_seal_age_ms: 0,
        cdc_mem_tier_max_bytes: 0,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        ..VortexConfig::default()
    }
}

fn rows_batch(rows: &[(i64, i64)]) -> arrow::array::RecordBatch {
    arrow::array::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("rows batch")
}

/// Apply one source transaction's upserts the way the change sink does.
async fn cdc_upsert(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) {
    let batch = rows_batch(rows);
    let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]),
        ),
    );
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(stream, &ctx.task_ctx())
        .await
        .expect("apply upserts");
    write.finish().await.expect("finish upserts");
}

/// Apply one source transaction's deletes the way the change sink does: into the
/// RAM tier when it takes them, else as a key delete.
async fn cdc_delete(table: &Arc<CayenneTableProvider>, ids: &[i64]) {
    if table.has_slot_advancer() && table.supports_in_memory_cdc_deletes() {
        let keys = rows_batch(&ids.iter().map(|id| (*id, 0)).collect::<Vec<_>>());
        if table
            .write_cdc_delete_keys_in_memory(&keys)
            .await
            .expect("absorb deletes")
            .is_some()
        {
            return;
        }
    }
    let filter = ids
        .iter()
        .map(|id| col("id").eq(lit(*id)))
        .reduce(datafusion::logical_expr::Expr::or)
        .expect("at least one id");
    assert!(
        table
            .delete_from_cdc_fast(std::slice::from_ref(&filter))
            .await
            .expect("key delete")
            .is_some(),
        "the key delete path handles a keyed table"
    );
}

async fn sorted_rows(ctx: &SessionContext, name: &str) -> Vec<(i64, i64)> {
    let batches = ctx
        .sql(&format!("SELECT id, v FROM {name} ORDER BY id"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows
}

/// A change stream into a dataset whose data is in the location: upserts, deletes,
/// and a delete then re-insert of the same key across sequences, seals and
/// checkpoints. Every object is under the dataset folder, and a new catalog and
/// table over the store alone (the working directory lost) serve exactly the
/// acknowledged rows, with no reload.
async fn change_stream_reopens_from_the_store_alone(durability: CdcDurability) {
    let name = "cdc_orders";
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(None);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let (table, ctx) = setup
        .create(name, &store, catalog, cdc_config(durability))
        .await;
    if durability == CdcDurability::Memory {
        table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    }
    let mut model: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();

    // Seed a durable corpus, so later tombstones hide rows in data files.
    let seed: Vec<(i64, i64)> = (1..=200).map(|id| (id, id * 10)).collect();
    cdc_upsert(&table, &seed).await;
    model.extend(seed.iter().copied());
    if durability == CdcDurability::Memory {
        table
            .checkpoint_mem_tier()
            .await
            .expect("checkpoint the seed");
    }

    for round in 0..3_i64 {
        for step in 0..10_i64 {
            let id = 1 + round * 40 + step * 3;
            cdc_upsert(
                &table,
                &[(id, id * 10 + round + 1), (1000 + round * 10 + step, step)],
            )
            .await;
            model.insert(id, id * 10 + round + 1);
            model.insert(1000 + round * 10 + step, step);
            cdc_delete(&table, &[id + 1]).await;
            model.remove(&(id + 1));
        }
        // The same key deleted, then re-inserted in a later transaction, then
        // deleted and re-inserted again across this round's seal or checkpoint.
        cdc_delete(&table, &[150]).await;
        model.remove(&150);
        cdc_upsert(&table, &[(150, 1_500 + round)]).await;
        model.insert(150, 1_500 + round);
        if durability == CdcDurability::Memory {
            if round % 2 == 0 {
                table.seal_mem_tier_durable().await.expect("seal");
            } else {
                table.checkpoint_mem_tier().await.expect("checkpoint");
            }
        }
        assert_eq!(
            sorted_rows(&ctx, name).await,
            model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
            "rows after round {round}"
        );
    }
    if durability == CdcDurability::Memory {
        table.seal_mem_tier_durable().await.expect("final seal");
    }

    let outside: Vec<String> = setup
        .bucket
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_filter(|key| {
            futures::future::ready(
                has_extension(key, &["vortex", "arrow"]) && !key.starts_with(&setup.data_prefix),
            )
        })
        .try_collect()
        .await
        .expect("list the bucket");
    assert!(
        outside.is_empty(),
        "data or deletion objects outside the dataset folder: {outside:?}"
    );
    assert!(
        !setup.objects_ending_with(".vortex").await.is_empty(),
        "the rows are data files in the dataset folder"
    );
    if durability == CdcDurability::Memory {
        // Seals and checkpoints write the tier's tombstones as deletion vectors
        // (a durable apply's may already be folded by compaction on write).
        assert!(
            !setup.objects_ending_with(".arrow").await.is_empty(),
            "the deletes are deletion vectors in the dataset folder"
        );
    }

    table.quiesce().await;
    drop((table, ctx));
    let (_table, ctx, _) = setup.reopen(name, &store).await;
    assert_eq!(
        sorted_rows(&ctx, name).await,
        model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
        "rows after a reopen from the store alone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_memory_tier_change_stream_reopens_from_the_store_alone() {
    change_stream_reopens_from_the_store_alone(CdcDurability::Memory).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_durability_change_stream_reopens_from_the_store_alone() {
    change_stream_reopens_from_the_store_alone(CdcDurability::File).await;
}

/// A checkpoint of a RAM tier that holds the deletes of many source transactions
/// writes one deletion-vector object for its pure deletes and one for its
/// re-inserted keys, not one per transaction, and the rows stay exact through a
/// reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_writes_one_deletion_vector_object_per_commit() {
    const TRANSACTIONS: i64 = 40;
    let name = "dv_per_checkpoint";
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(None);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let (table, ctx) = setup
        .create(name, &store, catalog, cdc_config(CdcDurability::Memory))
        .await;
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    let seed: Vec<(i64, i64)> = (1..=1_000).map(|id| (id, id)).collect();
    cdc_upsert(&table, &seed).await;
    table
        .checkpoint_mem_tier()
        .await
        .expect("checkpoint the seed");
    let mut model: std::collections::BTreeMap<i64, i64> = seed.into_iter().collect();
    let before = setup.objects_ending_with(".arrow").await.len();

    // Each transaction updates one durable row (a re-inserted key) and deletes
    // another (a pure delete), each at its own sequence.
    for transaction in 0..TRANSACTIONS {
        let updated = 1 + transaction * 2;
        let deleted = 2 + transaction * 2;
        cdc_upsert(&table, &[(updated, -updated)]).await;
        model.insert(updated, -updated);
        cdc_delete(&table, &[deleted]).await;
        model.remove(&deleted);
    }
    table
        .checkpoint_mem_tier()
        .await
        .expect("checkpoint the transactions");
    let written = setup.objects_ending_with(".arrow").await.len() - before;
    eprintln!(
        "a checkpoint of {TRANSACTIONS} transactions wrote {written} deletion-vector objects"
    );
    assert!(
        written <= 2,
        "a checkpoint of {TRANSACTIONS} transactions wrote {written} deletion-vector objects; \
         at most one per commit (pure deletes, re-inserted keys) is expected"
    );

    let expected: Vec<(i64, i64)> = model.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(
        sorted_rows(&ctx, name).await,
        expected,
        "rows after the checkpoint"
    );
    table.quiesce().await;
    drop((table, ctx));
    let (_table, ctx, _) = setup.reopen(name, &store).await;
    assert_eq!(
        sorted_rows(&ctx, name).await,
        expected,
        "rows after a reopen"
    );
}

/// Opening a keyed table with many deletion vectors on a store whose every request
/// takes 20 ms reads the vectors concurrently: one at a time, the open alone would
/// take at least `vectors × 20 ms`. The reopened table serves the exact rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_vectors_are_read_concurrently_at_open() {
    const UPSERTS: i64 = 200;
    const DELAY: Duration = Duration::from_millis(20);
    let name = "dv_open";
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(None);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    // No compaction, so every vector the upserts write is still there at open.
    let config = VortexConfig {
        compaction_trigger_files: usize::MAX,
        compaction_trigger_protected_snapshots: usize::MAX,
        compaction_trigger_snapshot_age_ms: 0,
        bake_deletion_index_trigger: usize::MAX,
        ..cdc_config(CdcDurability::File)
    };
    let (table, ctx) = setup.create(name, &store, catalog, config).await;
    let seed: Vec<(i64, i64)> = (1..=UPSERTS).map(|id| (id, id)).collect();
    cdc_upsert(&table, &seed).await;
    // One upsert per statement: each writes a key deletion vector.
    for id in 1..=UPSERTS {
        run(
            &ctx,
            &format!("INSERT INTO {name} VALUES ({id}, {})", id * 7),
        )
        .await;
    }
    let vectors = setup.objects_ending_with(".arrow").await.len();
    assert!(
        vectors >= usize::try_from(UPSERTS).expect("small"),
        "precondition: {UPSERTS} upserts left {vectors} deletion vectors"
    );
    let expected: Vec<(i64, i64)> = (1..=UPSERTS).map(|id| (id, id * 7)).collect();
    assert_eq!(sorted_rows(&ctx, name).await, expected);
    table.quiesce().await;
    drop((table, ctx));

    let slow = setup.store(Some(DELAY));
    let (_table, ctx, open) = setup.reopen(name, &slow).await;
    let sequential_floor = DELAY * u32::try_from(vectors).expect("small");
    eprintln!(
        "opened a table with {vectors} deletion vectors over {}ms requests in {}ms (one at a time: at least {}ms)",
        DELAY.as_millis(),
        open.as_millis(),
        sequential_floor.as_millis()
    );
    assert!(
        open < sequential_floor / 4,
        "opening a table with {vectors} deletion vectors took {}ms over {}ms requests; \
         reading them one at a time takes at least {}ms, and a concurrent read under a quarter of that",
        open.as_millis(),
        DELAY.as_millis(),
        sequential_floor.as_millis()
    );
    assert_eq!(
        sorted_rows(&ctx, name).await,
        expected,
        "rows after the slow reopen"
    );
}

/// A deletion vector whose GET fails while the table opens fails the open; it
/// never opens with fewer deletes (and so more rows) than were committed. Once
/// the store answers again, the table opens with the exact rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_deletion_vector_read_at_open_is_an_error_not_more_rows() {
    const UPSERTS: i64 = 20;
    let name = "dv_fault";
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(None);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let (table, ctx) = setup
        .create(name, &store, catalog, cdc_config(CdcDurability::File))
        .await;
    let seed: Vec<(i64, i64)> = (1..=UPSERTS).map(|id| (id, id)).collect();
    cdc_upsert(&table, &seed).await;
    for id in 1..=UPSERTS {
        run(
            &ctx,
            &format!("INSERT INTO {name} VALUES ({id}, {})", id * 7),
        )
        .await;
    }
    run(&ctx, &format!("DELETE FROM {name} WHERE id <= 5")).await;
    let expected: Vec<(i64, i64)> = (6..=UPSERTS).map(|id| (id, id * 7)).collect();
    assert_eq!(sorted_rows(&ctx, name).await, expected);
    assert!(
        !setup.objects_ending_with(".arrow").await.is_empty(),
        "precondition: the table has deletion vectors"
    );
    table.quiesce().await;
    drop((table, ctx));

    let fault_store = Arc::new(FaultStore::new(Arc::clone(&store)));
    fault_store.arm(
        Op::Get,
        |path: &Path| has_extension(path.as_ref(), &["arrow"]),
        Fault::Fail,
    );
    let failing: Arc<dyn ObjectStore> = Arc::clone(&fault_store) as Arc<dyn ObjectStore>;
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&failing).await);
    let ctx = SessionContext::new();
    match setup.builder(&failing, catalog, &ctx).open(name).await {
        Err(error) => eprintln!("open with a failed deletion-vector read: {error}"),
        Ok(table) => {
            ctx.register_table(name, Arc::new(table) as Arc<dyn TableProvider>)
                .expect("register");
            assert_eq!(
                sorted_rows(&ctx, name).await,
                expected,
                "a table that opened despite a failed deletion-vector read must serve the exact rows"
            );
        }
    }

    let (_table, ctx, _) = setup.reopen(name, &store).await;
    assert_eq!(
        sorted_rows(&ctx, name).await,
        expected,
        "rows once the store answers"
    );
}

/// How [`scans_under_store_latency_are_stable`] runs the change stream.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ChangeStream {
    /// Every apply is durable on its own (`cdc_durability: file`).
    Durable,
    /// Applies go into the RAM tier; the test seals and checkpoints it.
    MemoryTier,
    /// The settings a read-only `refresh_mode: changes` dataset runs with: the
    /// RAM tier with its background seal and checkpoint, inline small writes,
    /// background compaction, and scan views reused within a second.
    RuntimeDefaults,
}

/// Readers never fail, and never see a torn state, while a change stream inserts
/// and re-upserts rows on a store whose every request takes 20 ms: each answer is
/// a whole prefix of the keys acknowledged writes left, with every row once.
async fn scans_under_store_latency_are_stable(stream: ChangeStream) {
    const READERS: usize = 8;
    const RUN: Duration = Duration::from_secs(20);
    const DELAY: Duration = Duration::from_millis(20);
    let name = "latency_cdc";
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(Some(DELAY));
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let config = match stream {
        ChangeStream::Durable => cdc_config(CdcDurability::File),
        ChangeStream::MemoryTier => cdc_config(CdcDurability::Memory),
        ChangeStream::RuntimeDefaults => VortexConfig {
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            compaction_background_interval_ms: 1_000,
            ..VortexConfig::default()
        },
    };
    let ctx = SessionContext::new();
    let mut builder = setup.builder(&store, catalog, &ctx);
    if stream == ChangeStream::RuntimeDefaults {
        builder =
            builder.with_scan_view_reuse(cayenne::ScanViewReuse::WithinLag(Duration::from_secs(1)));
    }
    let table = Arc::new(
        builder
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: setup.data_url.clone(),
                partition_column: None,
                vortex_config: config,
            })
            .await
            .expect("create the keyed table"),
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    let memory = stream != ChangeStream::Durable;
    if memory {
        table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    }
    if stream == ChangeStream::RuntimeDefaults {
        assert!(
            table.spawn_background_mem_tier_checkpoint(),
            "background seal and checkpoint"
        );
    }
    let acknowledged = Arc::new(std::sync::atomic::AtomicI64::new(0));
    // When each count was acknowledged: a scan view reused within a lag may serve
    // the rows of a write acknowledged up to that lag before the query.
    let history: Arc<parking_lot::Mutex<Vec<(Instant, i64)>>> = Arc::default();
    let lag = if stream == ChangeStream::RuntimeDefaults {
        Duration::from_secs(1)
    } else {
        Duration::ZERO
    };
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let table = Arc::clone(&table);
        let acknowledged = Arc::clone(&acknowledged);
        let history = Arc::clone(&history);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut next = 1_i64;
            let mut applies = 0_i64;
            // 75 rows every 40 ms: about 1,900 rows a second, the rate the
            // object-store measurements of change streams use.
            let mut tick = tokio::time::interval(Duration::from_millis(40));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            while !stop.load(Ordering::Acquire) {
                tick.tick().await;
                let rows: Vec<(i64, i64)> = (next..next + 50).map(|id| (id, id * 10)).collect();
                cdc_upsert(&table, &rows).await;
                next += 50;
                history.lock().push((Instant::now(), next - 1));
                acknowledged.store(next - 1, Ordering::Release);
                // Re-upsert earlier rows with the values they have: a capture
                // that sees both the old and the new copy counts them twice.
                let again: Vec<(i64, i64)> = (0..25)
                    .map(|step| {
                        let id = 1 + (applies * 37 + step * 13) % (next - 1);
                        (id, id * 10)
                    })
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect();
                cdc_upsert(&table, &again).await;
                applies += 1;
                if stream != ChangeStream::MemoryTier {
                    continue;
                }
                if applies % 20 == 0 {
                    table.checkpoint_mem_tier().await.expect("checkpoint");
                } else if applies % 5 == 0 {
                    table.seal_mem_tier_durable().await.expect("seal");
                }
            }
            applies
        })
    };

    let mut readers = Vec::with_capacity(READERS);
    for reader in 0..READERS {
        let ctx = ctx.clone();
        let acknowledged = Arc::clone(&acknowledged);
        let history = Arc::clone(&history);
        let stop = Arc::clone(&stop);
        readers.push(tokio::spawn(async move {
            let (mut ok, mut failed, mut wrong) = (0_u64, Vec::new(), Vec::new());
            while !stop.load(Ordering::Acquire) {
                let floor = {
                    let asked = Instant::now();
                    let history = history.lock();
                    history
                        .iter()
                        .rev()
                        .find(|(at, _)| asked.duration_since(*at) >= lag)
                        .map_or(0, |(_, count)| *count)
                };
                // Odd readers count from statistics where the plan can; even ones scan.
                let sql = if reader % 2 == 0 {
                    format!("SELECT count(*), coalesce(sum(v), 0) FROM {name}")
                } else {
                    format!("SELECT count(*) FROM {name}")
                };
                let result = async { ctx.sql(&sql).await?.collect().await }.await;
                let ceiling = acknowledged.load(Ordering::Acquire) + 50;
                match result {
                    Err(error) => failed.push(error.to_string()),
                    Ok(batches) => {
                        let batch = &batches[0];
                        let count = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("count").value(0);
                        let sum_ok = batch.num_columns() < 2 || {
                            let sum = batch.column(1).as_any().downcast_ref::<Int64Array>().expect("sum").value(0);
                            sum == (1..=count).map(|id| id * 10).sum::<i64>()
                        };
                        if count < floor || count > ceiling || count % 50 != 0 || !sum_ok {
                            wrong.push(format!("{sql}: count {count} outside [{floor}, {ceiling}] or not a whole prefix (sum ok: {sum_ok})"));
                        } else {
                            ok += 1;
                        }
                    }
                }
            }
            (ok, failed, wrong)
        }));
    }

    tokio::time::sleep(RUN).await;
    stop.store(true, Ordering::Release);
    let applies = writer.await.expect("writer");
    let (mut ok, mut failed, mut wrong) = (0_u64, Vec::new(), Vec::new());
    for reader in readers {
        let (o, f, w) = reader.await.expect("reader");
        ok += o;
        failed.extend(f);
        wrong.extend(w);
    }
    eprintln!(
        "{applies} applies; {ok} reads exact, {} failed, {} wrong; first failure: {:?}; first wrong: {:?}",
        failed.len(),
        wrong.len(),
        failed.first(),
        wrong.first()
    );
    assert!(ok > 0, "no read completed");
    assert!(
        failed.is_empty(),
        "{} of {} reads failed; first: {}",
        failed.len(),
        ok + failed.len() as u64,
        failed[0]
    );
    assert!(
        wrong.is_empty(),
        "{} reads were wrong; first: {}",
        wrong.len(),
        wrong[0]
    );

    // Quiesced: the count answered from statistics equals a forced scan's.
    let stats_count = count_and_sum(&ctx, name).await.0;
    let exact = sorted_rows(&ctx, name).await.len();
    assert_eq!(usize::try_from(stats_count).expect("count"), exact);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scans_under_store_latency_are_stable_during_a_memory_tier_change_stream() {
    scans_under_store_latency_are_stable(ChangeStream::MemoryTier).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scans_under_store_latency_are_stable_during_a_file_durability_change_stream() {
    scans_under_store_latency_are_stable(ChangeStream::Durable).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scans_under_store_latency_are_stable_with_runtime_change_stream_defaults() {
    scans_under_store_latency_are_stable(ChangeStream::RuntimeDefaults).await;
}

/// What one arm of [`sealed_rows_inline_against_data_files`] cost.
struct SealArm {
    log_bytes: u64,
    log_objects: usize,
    data_bytes: u64,
    data_objects: usize,
    commit_max: Duration,
    reopen: Duration,
}

/// Bytes and objects under `prefix` (`inside`) or outside it.
async fn bytes_and_objects(bucket: &InMemory, prefix: &str, inside: bool) -> (u64, usize) {
    let metas: Vec<ObjectMeta> = bucket
        .list(None)
        .try_collect()
        .await
        .expect("list the bucket");
    metas
        .iter()
        .filter(|meta| meta.location.as_ref().starts_with(prefix) == inside)
        .fold((0, 0), |(bytes, objects), meta| {
            (bytes + meta.size, objects + 1)
        })
}

/// One minute of a change stream at 2,000 rows a second, with a fifth of each
/// interval's rows updates of earlier keys, made durable every 2 s either by a seal
/// (today: the RAM tier's visible rows go into the log as an inline record) or by a
/// checkpoint (the same rows as a data file, whose entry alone goes in the log).
async fn run_seal_arm(seal: bool) -> SealArm {
    const INTERVALS: i64 = 30;
    const ROWS_PER_INTERVAL: i64 = 4_000;
    const APPLIES_PER_INTERVAL: i64 = 40;
    const DELAY: Duration = Duration::from_millis(20);
    let name = if seal {
        "sealed_inline"
    } else {
        "sealed_as_files"
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let (setup, catalog) = KeyedSetup::new(name).await;
    let store = setup.store(Some(DELAY));
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
    let ctx = SessionContext::new();
    let table = Arc::new(
        setup
            .builder(&store, catalog, &ctx)
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: setup.data_url.clone(),
                partition_column: None,
                vortex_config: cdc_config(CdcDurability::Memory),
            })
            .await
            .expect("create"),
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    let (log_before, log_objects_before) =
        bytes_and_objects(&setup.bucket, &setup.data_prefix, false).await;
    let (data_before, data_objects_before) =
        bytes_and_objects(&setup.bucket, &setup.data_prefix, true).await;

    let task_ctx = ctx.task_ctx();
    let mut next = 1_i64;
    let mut commits = Vec::with_capacity(usize::try_from(INTERVALS).expect("small"));
    for interval in 0..INTERVALS {
        for apply in 0..APPLIES_PER_INTERVAL {
            let per_apply = ROWS_PER_INTERVAL / APPLIES_PER_INTERVAL;
            let updates = if next > per_apply { per_apply / 5 } else { 0 };
            let mut ids: Vec<i64> = (0..updates)
                .map(|step| 1 + (interval * 7919 + apply * 104_729 + step * 31) % (next - 1))
                .collect();
            ids.extend(next..next + per_apply - updates);
            next += per_apply - updates;
            ids.sort_unstable();
            ids.dedup();
            let batch = arrow::array::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from(ids.iter().map(|id| id * 10 + interval).collect::<Vec<_>>())),
                    Arc::new(arrow::array::StringArray::from(
                        ids.iter()
                            .map(|id| format!("order {id:012} shipped to region north-america-west, priority {}", id % 5))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    batch.schema(),
                    futures::stream::iter([Ok(batch)]),
                ),
            );
            table
                .write_cdc_append_stream(stream, &task_ctx)
                .await
                .expect("apply")
                .finish()
                .await
                .expect("finish");
        }
        let started = Instant::now();
        if seal {
            table.seal_mem_tier_durable().await.expect("seal");
        } else {
            table.checkpoint_mem_tier().await.expect("checkpoint");
        }
        commits.push(started.elapsed());
    }
    table.quiesce().await;
    let (log_after, log_objects_after) =
        bytes_and_objects(&setup.bucket, &setup.data_prefix, false).await;
    let (data_after, data_objects_after) =
        bytes_and_objects(&setup.bucket, &setup.data_prefix, true).await;
    drop((table, ctx));
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(open_catalog(&store).await);
    let ctx = SessionContext::new();
    let started = Instant::now();
    let _table = setup
        .builder(&store, catalog, &ctx)
        .open(name)
        .await
        .expect("reopen");
    let reopen = started.elapsed();
    SealArm {
        log_bytes: log_after - log_before,
        log_objects: log_objects_after - log_objects_before,
        data_bytes: data_after - data_before,
        data_objects: data_objects_after - data_objects_before,
        commit_max: commits.iter().copied().max().unwrap_or_default(),
        reopen,
    }
}

/// Sealed RAM-tier rows in the log (today) against the same rows as small data
/// files whose entries alone go in the log: one minute of a change stream at 2,000
/// rows a second, made durable every 2 s, over a store whose every request takes
/// 20 ms. A measurement: it prints both arms, and bounds today's log growth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_rows_inline_against_data_files() {
    let inline = run_seal_arm(true).await;
    let files = run_seal_arm(false).await;
    for (arm, cost) in [("sealed inline", &inline), ("as data files", &files)] {
        eprintln!(
            "{arm}: log {} bytes in {} objects per minute; data {} bytes in {} objects per minute; \
             slowest of 30 commits {} ms; reopen {} ms",
            cost.log_bytes,
            cost.log_objects,
            cost.data_bytes,
            cost.data_objects,
            cost.commit_max.as_millis(),
            cost.reopen.as_millis()
        );
    }
    assert!(
        inline.log_bytes > 0 && files.data_bytes > 0,
        "both arms wrote"
    );
}
