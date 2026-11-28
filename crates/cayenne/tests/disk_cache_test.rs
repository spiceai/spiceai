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

//! The node disk cache under tables whose data is in an object-native
//! location: what a table's reconciliation prefetches and evicts, and what the
//! cache serves after a restart, from a replica, and while the store is down.
//!
//! Each node stacks its data store as the runtime does: a store that counts the
//! data-file reads reaching the location, wrapped by the node's cache, under the
//! table. Every node's cache has a fixed, roomy volume, so no test depends on
//! the free space of the machine it runs on.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::disk_cache::DiskCacheBinding;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{Access, NativeCatalogConfig, ObjectNativeCatalog};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use object_store_cache::testing::Tuning;
use object_store_cache::{NodeCache, NodeCacheConfig, Space, VolumeSpace};
use tempfile::TempDir;

const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const NAMESPACE: &str = "test-location";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Every write a data file, and no automatic compaction.
fn vortex_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        compaction_trigger_files: 1_000_000,
        compaction_trigger_protected_snapshots: 1_000_000,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// A volume with plenty of room, so a cache's budget alone sets its capacity.
#[derive(Debug)]
struct RoomyVolume;

impl VolumeSpace for RoomyVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        Ok(Space {
            total: 1 << 40,
            available: 1 << 40,
        })
    }
}

/// The location's store as one node reaches it: counts the reads of data files
/// and deletion vectors that reach the location, and can be taken down or made
/// to fail deletes.
#[derive(Debug)]
struct NodeStore {
    inner: Arc<dyn ObjectStore>,
    data_reads: AtomicUsize,
    delete_requests: AtomicUsize,
    offline: AtomicBool,
    fail_deletes: AtomicBool,
}

impl NodeStore {
    fn new(inner: &Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            data_reads: AtomicUsize::new(0),
            delete_requests: AtomicUsize::new(0),
            offline: AtomicBool::new(false),
            fail_deletes: AtomicBool::new(false),
        })
    }

    fn take_data_reads(&self) -> usize {
        self.data_reads.swap(0, Ordering::SeqCst)
    }

    fn check_online(&self, location: &Path) -> object_store::Result<()> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "NodeStore",
                source: format!("the location is down: {location}").into(),
            });
        }
        Ok(())
    }

    fn note_read(&self, location: &Path) {
        if cayenne::dataset_storage::DataFileKind::of(location.as_ref()).is_some() {
            self.data_reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Display for NodeStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "NodeStore")
    }
}

#[async_trait]
impl ObjectStore for NodeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check_online(location)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check_online(location)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check_online(location)?;
        if !options.head {
            self.note_read(location);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.check_online(location)?;
        self.note_read(location);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.delete_requests.fetch_add(1, Ordering::SeqCst);
        if self.fail_deletes.load(Ordering::SeqCst) || self.offline.load(Ordering::SeqCst) {
            return locations
                .map(|location| {
                    location.and_then(|location| {
                        Err(object_store::Error::Generic {
                            store: "NodeStore",
                            source: format!("delete refused: {location}").into(),
                        })
                    })
                })
                .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// A node's disk cache in `dir`, of `budget` bytes on a roomy volume. A cache
/// dropped in this process releases the directory once its last entry write
/// has finished, so `Locked` is retried for a bounded time.
async fn open_cache(dir: &TempDir, budget: u64) -> Arc<NodeCache> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match NodeCache::open(NodeCacheConfig {
            dir: dir.path().to_path_buf(),
            budget_bytes: budget,
            volume: Arc::new(RoomyVolume),
        })
        .await
        {
            Ok(cache) => return cache,
            Err(object_store_cache::Error::Locked { .. })
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => panic!("open the disk cache: {error}"),
        }
    }
}

/// One instance on the shared location: its catalog, a session, and its data
/// store stacked as the runtime stacks it (the counting store, then the cache).
struct Node {
    catalog: Arc<ObjectNativeCatalog>,
    ctx: SessionContext,
    requests: Arc<NodeStore>,
    data_store: Arc<dyn ObjectStore>,
    /// The cache over a location that is always offline, for probing
    /// residency: a range not fully resident fails instead of being fetched,
    /// so a probe never fills the cache.
    probe: Arc<dyn ObjectStore>,
    binding: Option<DiskCacheBinding>,
}

impl Node {
    async fn open(
        objects: &Arc<dyn ObjectStore>,
        access: Access,
        cache: Option<&Arc<NodeCache>>,
    ) -> Self {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(objects),
            Path::from(crate::common::LOCATION_CATALOG),
            LOCATION,
            NativeCatalogConfig {
                access,
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        let requests = NodeStore::new(objects);
        let counted: Arc<dyn ObjectStore> = Arc::clone(&requests) as Arc<dyn ObjectStore>;
        let binding = cache.map(|cache| {
            DiskCacheBinding::new(
                Arc::clone(cache),
                NAMESPACE,
                catalog.datasets_prefix().clone(),
            )
        });
        let offline = NodeStore::new(objects);
        offline.offline.store(true, Ordering::SeqCst);
        let offline: Arc<dyn ObjectStore> = offline;
        // The probe is wrapped first: the namespace's prefetches read the
        // store wrapped last, which must be the data store.
        let (probe, data_store) = match &binding {
            Some(binding) => {
                let probe = binding
                    .cache()
                    .wrap(NAMESPACE, offline, binding.cacheable());
                (probe, binding.wrap(Arc::clone(&counted)))
            }
            None => (offline, Arc::clone(&counted)),
        };
        Self {
            catalog: Arc::new(catalog),
            ctx: SessionContext::new(),
            requests,
            data_store,
            probe,
            binding,
        }
    }

    fn data_url(&self, table: &str) -> String {
        crate::common::location_data_url(&self.catalog, table)
    }

    fn builder(&self, data_url: &str) -> cayenne::CayenneTableProviderBuilder {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::clone(&self.catalog) as Arc<dyn MetadataCatalog>;
        let builder = crate::common::table_builder_on_store(
            catalog,
            self.ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(&self.data_store),
            },
        );
        match &self.binding {
            Some(binding) => builder.with_disk_cache(binding.clone()),
            None => builder,
        }
    }

    async fn create(&self, table: &str, data_url: &str) -> Arc<CayenneTableProvider> {
        let provider = self
            .builder(data_url)
            .create(CreateTableOptions {
                table_name: table.to_string(),
                schema: schema(),
                primary_key: Vec::new(),
                on_conflict: None,
                base_path: data_url.to_string(),
                partition_column: None,
                vortex_config: vortex_config(),
            })
            .await
            .expect("create the table");
        self.register(table, provider)
    }

    async fn open_table(&self, table: &str, data_url: &str) -> Arc<CayenneTableProvider> {
        let provider = self
            .builder(data_url)
            .open(table)
            .await
            .expect("open the table");
        self.register(table, provider)
    }

    fn register(&self, table: &str, provider: CayenneTableProvider) -> Arc<CayenneTableProvider> {
        let provider = Arc::new(provider);
        provider.init_scan_view_cache();
        self.ctx
            .register_table(
                table,
                Arc::clone(&provider) as Arc<dyn datafusion::datasource::TableProvider>,
            )
            .expect("register");
        provider
    }

    async fn sum(&self, table: &str) -> (i64, i64) {
        let batches = self
            .ctx
            .sql(&format!(
                "SELECT count(*), coalesce(sum(v), 0) FROM {table}"
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let value = |column: usize| {
            batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64")
                .value(0)
        };
        (value(0), value(1))
    }

    /// The data files of `table` in the location, by object path.
    async fn data_files(&self, table: &CayenneTableProvider, data_url: &str) -> Vec<(Path, u64)> {
        let prefix = cayenne::dataset_storage::object_key_of_url(
            &url::Url::parse(data_url).expect("data url"),
        )
        .expect("data prefix")
        .join(table.table_id());
        let files = self
            .catalog
            .cacheable_files(table.table_id())
            .await
            .expect("read the table's files")
            .expect("the table's files are settled");
        let mut data: Vec<_> = files.data;
        // Oldest first, as the reconciler orders them: by sequence, then by
        // name, which starts with a time-ordered id.
        data.sort_by(|a, b| a.seq.cmp(&b.seq).then_with(|| a.key.cmp(&b.key)));
        data.into_iter()
            .map(|file| (prefix.clone().join(file.key.as_str()), file.size))
            .collect()
    }
}

/// A batch of rows `first..=last` with `v = id * 10`.
fn rows(first: i64, last: i64) -> RecordBatch {
    let ids: Vec<i64> = (first..=last).collect();
    let vs: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .expect("batch")
}

/// `(count, sum(v))` of rows `first..=last`.
fn expected(first: i64, last: i64) -> (i64, i64) {
    (last - first + 1, (first..=last).map(|id| id * 10).sum())
}

async fn write(table: &CayenneTableProvider, batch: RecordBatch, op: InsertOp) {
    crate::common::write_batches(table, vec![batch], op)
        .await
        .expect("write");
}

/// Copies the cache directory `from`, with its slab files, into `to`.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    for entry in std::fs::read_dir(from).expect("list the cache dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            std::fs::create_dir_all(&target).expect("create dir");
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}

/// Whether `path`'s bytes `range` are all resident in `node`'s cache. The
/// probe's location is offline, so a range the cache cannot serve fails and
/// nothing is filled.
async fn resident(node: &Node, path: &Path, range: Range<u64>) -> bool {
    node.probe.get_range(path, range).await.is_ok()
}

/// A writer that restarts on the same cache directory serves the files it
/// committed before the restart from the cache: its first round prefetches the
/// live files (none count as written by the new process), and a query then
/// reads no data file from the location.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_serves_from_disk_cache_on_data_store() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().expect("cache dir");
    let data_url;
    {
        let cache = open_cache(&dir, 64 << 20).await;
        let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
        data_url = node.data_url("orders");
        let table = node.create("orders", &data_url).await;
        // No query here: the process's in-memory footer and segment caches
        // would serve the query after the restart, which runs in this process.
        for (first, last) in [(1, 100), (101, 200), (201, 300)] {
            write(&table, rows(first, last), InsertOp::Append).await;
        }
        cache.close().await.expect("close the cache");
    }

    let cache = open_cache(&dir, 64 << 20).await;
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let table = node.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    let files = node.data_files(&table, &data_url).await;
    assert_eq!(files.len(), 3, "one data file a write: {files:?}");
    node.requests.take_data_reads();
    assert_eq!(node.sum("orders").await, expected(1, 300));
    assert_eq!(
        node.requests.take_data_reads(),
        0,
        "a query after the restart reads no data file from the location"
    );
    let stats = cache.stats();
    assert!(
        stats.accesses > 0 && stats.hits == stats.accesses,
        "the query read through the cache and found every block: {stats:?}"
    );
}

/// A writer never prefetches the files it uploaded: they are in its page cache
/// and the segment cache already, and fetching them back would only cost
/// requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_never_prefetches_own_uploads_on_data_store() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let data_url = node.data_url("orders");
    let table = node.create("orders", &data_url).await;
    for (first, last) in [(1, 100), (101, 200)] {
        write(&table, rows(first, last), InsertOp::Append).await;
    }
    let files = node.data_files(&table, &data_url).await;
    assert_eq!(files.len(), 2, "{files:?}");
    for (path, _) in &files {
        assert!(
            cache.written_here(NAMESPACE, path),
            "{path} was uploaded here"
        );
    }
    node.requests.take_data_reads();
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    assert_eq!(
        cache.stats().fetched_bytes_prefetch,
        0,
        "{:?}",
        cache.stats()
    );
    assert_eq!(node.requests.take_data_reads(), 0);
}

/// A replica prefetches the files the writer commits, at open and after each
/// commit it follows, so its queries read no data file from the location.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_prefetches_writer_files() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Node::open(&objects, Access::Writer, None).await;
    let data_url = writer.data_url("orders");
    let written = writer.create("orders", &data_url).await;
    write(&written, rows(1, 100), InsertOp::Append).await;

    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let replica = Node::open(&objects, Access::Replica, Some(&cache)).await;
    let table = replica.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    assert!(
        cache.stats().fetched_bytes_prefetch > 0,
        "{:?}",
        cache.stats()
    );
    assert!(
        replica.requests.take_data_reads() > 0,
        "prefetch read the writer's file"
    );
    assert_eq!(replica.sum("orders").await, expected(1, 100));
    assert_eq!(
        replica.requests.take_data_reads(),
        0,
        "served from the cache"
    );

    write(&written, rows(101, 200), InsertOp::Append).await;
    // The replica follows the commit without a query reading the new file;
    // the round prefetches it, and the query then reads it from the cache.
    let prefetched = cache.stats().fetched_bytes_prefetch;
    replica
        .catalog
        .follow_commits(table.table_id())
        .await
        .expect("follow the writer");
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    assert!(
        cache.stats().fetched_bytes_prefetch > prefetched,
        "the new file was prefetched: {:?}",
        cache.stats()
    );
    let plan = replica
        .ctx
        .sql("SELECT count(*), coalesce(sum(v), 0) FROM orders")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    replica.requests.take_data_reads();
    let batches = datafusion::physical_plan::collect(plan, replica.ctx.task_ctx())
        .await
        .expect("execute");
    let column = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0)
    };
    assert_eq!((column(0), column(1)), expected(1, 200));
    assert_eq!(
        replica.requests.take_data_reads(),
        0,
        "the file committed after the replica opened was prefetched"
    );
}

/// A retired file stays cached while a scan that started before its retirement
/// may still read it, and is evicted once its retention has passed; the live
/// files stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_kept_until_retention_then_evicted() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Node::open(&objects, Access::Writer, None).await;
    let data_url = writer.data_url("orders");
    let written = writer.create("orders", &data_url).await;
    write(&written, rows(1, 100), InsertOp::Append).await;
    let old = writer.data_files(&written, &data_url).await;
    assert_eq!(old.len(), 1);

    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let replica = Node::open(&objects, Access::Replica, Some(&cache)).await;
    let table = replica.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;

    write(&written, rows(1_000, 1_049), InsertOp::Overwrite).await;
    assert_eq!(replica.sum("orders").await, expected(1_000, 1_049));
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    let (old_path, old_size) = old[0].clone();
    assert_eq!(cache.stats().evicted_bytes_unreferenced, 0);
    assert!(
        resident(&replica, &old_path, 0..old_size).await,
        "the retired file is kept within its retention"
    );

    table.elapse_data_file_retention();
    table.reconcile_disk_cache_now().await;
    assert!(
        cache.stats().evicted_bytes_unreferenced >= old_size,
        "{:?}",
        cache.stats()
    );
    let new = writer.data_files(&written, &data_url).await;
    assert_eq!(new.len(), 1);
    assert!(
        resident(&replica, &new[0].0, 0..new[0].1).await,
        "the live file stays"
    );
    assert!(
        !resident(&replica, &old_path, 0..old_size).await,
        "the retired file is evicted after its retention"
    );
}

/// A delete of a data file evicts it from the cache before the delete reaches
/// the location: a delete that fails there leaves the file in the location
/// but not in the cache, across a restart too, and a read then gets the
/// location's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_evicts_before_store_delete() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let data_url = node.data_url("orders");
    let table = node.create("orders", &data_url).await;
    write(&table, rows(1, 100), InsertOp::Append).await;
    let (old_path, old_size) = node.data_files(&table, &data_url).await[0].clone();
    let old_bytes = objects
        .get(&old_path)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    // Cache the whole file, then retire it.
    node.data_store
        .get_range(&old_path, 0..old_size)
        .await
        .expect("read");
    assert!(resident(&node, &old_path, 0..old_size).await);
    write(&table, rows(1_000, 1_049), InsertOp::Overwrite).await;

    // The first sweep observes the retirement; the second, after the
    // retention, sends the delete, which the location refuses.
    node.requests.fail_deletes.store(true, Ordering::SeqCst);
    table.sweep_retired_files_now().await;
    table.elapse_data_file_retention();
    let deletes = node.requests.delete_requests.load(Ordering::SeqCst);
    table.sweep_retired_files_now().await;
    assert!(
        node.requests.delete_requests.load(Ordering::SeqCst) > deletes,
        "the sweep sent the delete"
    );
    assert!(
        objects.head(&old_path).await.is_ok(),
        "the location refused the delete"
    );
    assert!(
        !resident(&node, &old_path, 0..old_size).await,
        "evicted before the delete was sent"
    );

    let live = node.data_files(&table, &data_url).await;
    assert_eq!(live.len(), 1);
    node.data_store
        .get_range(&live[0].0, 0..live[0].1)
        .await
        .expect("read the live file");
    // Closing skips entry writes still queued, so the fill must land first.
    cache.wait_idle().await;
    assert!(resident(&node, &live[0].0, 0..live[0].1).await);

    // A restart finds the eviction recorded: the cache does not serve the file
    // it evicted, and a read gets the location's bytes.
    // Background tasks of the old table can keep the closed cache, and so its
    // directory lock, alive; the new process opens a copy of the directory.
    cache.close().await.expect("close");
    drop((node, table, cache));
    let restarted = TempDir::new().expect("cache dir");
    copy_dir(dir.path(), restarted.path());
    let cache = open_cache(&restarted, 64 << 20).await;
    assert!(
        cache.stats().entries > 0,
        "the copy carries the resident live file: {:?}",
        cache.stats()
    );
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let _table = node.open_table("orders", &data_url).await;
    node.requests.take_data_reads();
    let read = node
        .data_store
        .get_range(&old_path, 0..old_size)
        .await
        .expect("read");
    assert_eq!(
        node.requests.take_data_reads(),
        1,
        "the evicted file is read from the location"
    );
    assert_eq!(read, old_bytes);
}

/// A table's reconciliation evicts only within its own dataset folder: a file
/// under it that its committed state does not name goes, and another
/// dataset's files, named or not, stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_leaves_other_datasets_alone() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let orders_url = node.data_url("orders");
    let orders = node.create("orders", &orders_url).await;
    let lines_url = node.data_url("lines");
    let lines = node.create("lines", &lines_url).await;
    write(&orders, rows(1, 100), InsertOp::Append).await;
    write(&lines, rows(1, 100), InsertOp::Append).await;

    // A data file in each folder that no commit names, cached by a read.
    let stray = |table: &CayenneTableProvider, url: &str| {
        cayenne::dataset_storage::object_key_of_url(&url::Url::parse(url).expect("url"))
            .expect("prefix")
            .join(table.table_id())
            .join("stray.vortex")
    };
    let orders_stray = stray(&orders, &orders_url);
    let lines_stray = stray(&lines, &lines_url);
    let mut cached = Vec::new();
    for path in [&orders_stray, &lines_stray] {
        objects
            .put(path, Bytes::from(vec![7_u8; 1000]).into())
            .await
            .expect("put");
        cached.push((path.clone(), 1000));
    }
    cached.extend(node.data_files(&orders, &orders_url).await);
    cached.extend(node.data_files(&lines, &lines_url).await);
    for (path, size) in &cached {
        node.data_store
            .get_range(path, 0..*size)
            .await
            .expect("read");
    }

    orders.reconcile_disk_cache_now().await;
    assert_eq!(
        cache.stats().evicted_bytes_unreferenced,
        1000,
        "{:?}",
        cache.stats()
    );
    for (path, size) in &cached {
        assert_eq!(
            resident(&node, path, 0..*size).await,
            path != &orders_stray,
            "{path}"
        );
    }
}

/// While the location is down, a writer serves what it has cached: after a
/// restart and its first round, every query of the working set succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outage_serves_resident_working_set() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().expect("cache dir");
    let data_url;
    {
        let cache = open_cache(&dir, 64 << 20).await;
        let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
        data_url = node.data_url("orders");
        let table = node.create("orders", &data_url).await;
        for (first, last) in [(1, 100), (101, 200)] {
            write(&table, rows(first, last), InsertOp::Append).await;
        }
        cache.close().await.expect("close");
    }
    let cache = open_cache(&dir, 64 << 20).await;
    let node = Node::open(&objects, Access::Writer, Some(&cache)).await;
    let table = node.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    assert!(
        node.requests.take_data_reads() > 0,
        "the round prefetched the files"
    );

    node.requests.offline.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        assert_eq!(node.sum("orders").await, expected(1, 200));
    }
    assert_eq!(node.requests.take_data_reads(), 0);
    let stats = cache.stats();
    assert!(
        stats.accesses > 0 && stats.hits == stats.accesses,
        "{stats:?}"
    );
}

/// Prefetched tails stay within a tenth of the cache's capacity, newest file
/// first; whole files then go into the room that is left, and an older file
/// whose tail did not fit gets none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_prefetch_capped_at_tenth_of_budget() {
    const BLOCK: u64 = 64 << 10;
    const BUDGET: u64 = 4 << 20;
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Node::open(&objects, Access::Writer, None).await;
    let data_url = writer.data_url("orders");
    let written = writer.create("orders", &data_url).await;
    // Data files of up to a few MiB, of values that do not compress.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for file in 0..6_i64 {
        let first = file * 400_000;
        let ids: Vec<i64> = (first..first + 400_000).collect();
        let vs: Vec<i64> = ids
            .iter()
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                i64::from_ne_bytes(state.to_ne_bytes())
            })
            .collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .expect("batch");
        write(&written, batch, InsertOp::Append).await;
    }
    let files = writer.data_files(&written, &data_url).await;
    assert!(files.len() >= 6, "{files:?}");
    // Newest first: the tails that fit a tenth of the budget, by the blocks
    // each spans.
    let newest: Vec<(Path, u64)> = files.iter().rev().cloned().collect();
    let tail_cost = |size: u64| {
        let start = size - cayenne::disk_cache::TAIL_BYTES;
        ((size - 1) / BLOCK * BLOCK + BLOCK).min(size) - start / BLOCK * BLOCK
    };
    let mut spent = 0;
    let tails = newest
        .iter()
        .take_while(|(_, size)| {
            spent += tail_cost(*size);
            spent <= BUDGET / 10
        })
        .count();
    assert!(
        (2..newest.len() - 1).contains(&tails),
        "some tails fit and some do not: {tails} of {newest:?}"
    );
    // The newest file fits the room whole; the next does not, so no older
    // whole file is prefetched.
    let room = BUDGET * 9 / 10;
    assert!(newest[0].1 < room / 2 && newest[1].1 > room, "{newest:?}");

    let dir = TempDir::new().expect("cache dir");
    let cache = object_store_cache::testing::open_with(
        NodeCacheConfig {
            dir: dir.path().to_path_buf(),
            budget_bytes: BUDGET,
            volume: Arc::new(RoomyVolume),
        },
        Tuning {
            block_bytes: BLOCK,
            ..Tuning::default()
        },
    )
    .await
    .expect("open the cache");
    let replica = Node::open(&objects, Access::Replica, Some(&cache)).await;
    let table = replica.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;

    let tail = |size: u64| size - cayenne::disk_cache::TAIL_BYTES..size;
    let mut resident_tails = Vec::new();
    for (path, size) in &newest {
        resident_tails.push(resident(&replica, path, tail(*size)).await);
    }
    let mut expected_tails = vec![false; newest.len()];
    expected_tails[..tails].fill(true);
    assert_eq!(
        resident_tails,
        expected_tails,
        "newest first: {:?}",
        cache.stats()
    );
    assert!(
        cache.stats().fetched_bytes_prefetch >= newest[0].1,
        "the newest file was prefetched whole: {:?}",
        cache.stats()
    );
}

/// A cache installed after the process's cache was closed (by a runtime's
/// shutdown) replaces it; one installed while it is open does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn install_replaces_a_closed_disk_cache() {
    crate::common::require_process_per_test("it installs the process's disk cache");
    let (first_dir, second_dir) = (TempDir::new().expect("dir"), TempDir::new().expect("dir"));
    let first = open_cache(&first_dir, 8 << 20).await;
    let second = open_cache(&second_dir, 8 << 20).await;
    assert!(cayenne::disk_cache::install(
        Arc::clone(&first),
        first_dir.path().to_path_buf()
    ));
    assert!(!cayenne::disk_cache::install(
        Arc::clone(&second),
        second_dir.path().to_path_buf()
    ));
    cayenne::disk_cache::close_process_disk_cache(std::time::Duration::from_secs(5)).await;
    assert!(cayenne::disk_cache::process_disk_cache().is_none());
    assert!(
        cayenne::disk_cache::install(Arc::clone(&second), second_dir.path().to_path_buf()),
        "a closed cache is replaced"
    );
    let installed = cayenne::disk_cache::process_disk_cache().expect("installed");
    assert!(Arc::ptr_eq(&installed, &second));
}

/// A shutdown with no time left still closes the cache cleanly: the slab
/// being written gets its index, so the next start reads indexes only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_with_no_time_left_still_seals_the_cache() {
    crate::common::require_process_per_test("it installs the process's disk cache");
    let dir = TempDir::new().expect("dir");
    let cache = open_cache(&dir, 8 << 20).await;
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("t/a.vortex");
    objects
        .put(&path, Bytes::from(vec![7_u8; 1000]).into())
        .await
        .expect("put");
    let cached = cache.wrap(NAMESPACE, Arc::clone(&objects), Arc::new(|_: &Path| true));
    cached.get_range(&path, 0..1000).await.expect("read");
    cache.wait_idle().await;
    let idx = |dir: &TempDir| {
        std::fs::read_dir(dir.path().join("slabs"))
            .expect("list")
            .filter(|e| {
                e.as_ref()
                    .expect("entry")
                    .path()
                    .extension()
                    .is_some_and(|x| x == "idx")
            })
            .count()
    };
    assert_eq!(idx(&dir), 0, "the slab being written has no index yet");
    assert!(cayenne::disk_cache::install(
        Arc::clone(&cache),
        dir.path().to_path_buf()
    ));
    cayenne::disk_cache::close_process_disk_cache(std::time::Duration::ZERO).await;
    assert_eq!(idx(&dir), 1, "the close sealed the slab");
}

/// Polls `cond` every 20 ms until it holds; fails after `secs` seconds.
async fn poll_until(secs: u64, what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while !cond().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: not reached within {secs}s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A new file whose prefetch the cache declined (the prefetch queue was full)
/// is retried by the next round after a reload, without waiting for a full
/// round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declined_prefetch_is_retried_by_the_next_reload_round() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Node::open(&objects, Access::Writer, None).await;
    let data_url = writer.data_url("orders");
    let written = writer.create("orders", &data_url).await;
    write(&written, rows(1, 2_000), InsertOp::Append).await;

    let dir = TempDir::new().expect("cache dir");
    let cache = object_store_cache::testing::open_with(
        NodeCacheConfig {
            dir: dir.path().to_path_buf(),
            budget_bytes: 64 << 20,
            volume: Arc::new(RoomyVolume),
        },
        Tuning {
            block_bytes: 4096,
            prefetch_queue_jobs: 2,
            ..Tuning::default()
        },
    )
    .await
    .expect("open the cache");
    let replica = Node::open(&objects, Access::Replica, Some(&cache)).await;
    // Fill both prefetch slots: jobs for unrelated objects, held back by a
    // read whose response is not consumed yet.
    let fillers = [
        Path::from("other/filler-1.bin"),
        Path::from("other/filler-2.bin"),
    ];
    let held_path = Path::from("other/held.bin");
    for p in fillers.iter().chain([&held_path]) {
        objects
            .put(p, Bytes::from(vec![1_u8; 10_000]).into())
            .await
            .expect("put");
    }
    let held = replica
        .data_store
        .get_opts(&held_path, GetOptions::default())
        .await
        .expect("held read");
    for filler in &fillers {
        let queued = cache
            .prefetch(NAMESPACE, filler, 10_000, None)
            .expect("prefetch");
        assert!(queued.queued_bytes > 0, "{queued:?}");
    }

    let table = replica.open_table("orders", &data_url).await;
    // The reconciler's first (full) round declines the first file.
    poll_until(10, "full round declined F1", async || {
        cache.stats().prefetch_declined_bytes > 0
    })
    .await;
    let declined_full = cache.stats().prefetch_declined_bytes;

    // F2 committed; the reload wakes a round that is not full, which
    // declines F2 too (the queue is still full).
    write(&written, rows(2_001, 4_000), InsertOp::Append).await;
    replica
        .catalog
        .follow_commits(table.table_id())
        .await
        .expect("follow");
    table.refresh(&table).await.expect("reload");
    poll_until(10, "reload round declined F2", async || {
        cache.stats().prefetch_declined_bytes > declined_full
    })
    .await;

    // The queue drains.
    drop(held);
    cache.wait_idle().await;
    poll_until(10, "filler fetched", async || {
        cache.stats().fetched_bytes_prefetch >= 20_000
    })
    .await;
    cache.wait_idle().await;

    // F3 committed; rounds after reloads, and no full round, prefetch F3 and
    // retry F1 and F2, two jobs at a time.
    write(&written, rows(4_001, 6_000), InsertOp::Append).await;
    replica
        .catalog
        .follow_commits(table.table_id())
        .await
        .expect("follow");
    let files = writer.data_files(&written, &data_url).await;
    assert_eq!(files.len(), 3, "{files:?}");
    poll_until(30, "reload rounds prefetched F1, F2 and F3", async || {
        table.refresh(&table).await.expect("reload");
        cache.wait_idle().await;
        let mut all = true;
        for (path, size) in &files {
            all &= resident(&replica, path, 0..*size).await;
        }
        all
    })
    .await;
}

/// A deletion vector of an object-native table is keyed under the table's
/// folder, a replica's round prefetches it at the object that key names, and
/// later rounds neither evict it nor fetch it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_vector_stays_cached_across_rounds() {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::{col, lit};
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Node::open(&objects, Access::Writer, None).await;
    let data_url = writer.data_url("orders");
    let written = writer.create("orders", &data_url).await;
    write(&written, rows(1, 100), InsertOp::Append).await;
    let plan = written
        .delete_from(&writer.ctx.state(), vec![col("id").lt(lit(10_i64))])
        .await
        .expect("plan the delete");
    datafusion::physical_plan::collect(plan, writer.ctx.task_ctx())
        .await
        .expect("delete");
    let files = writer
        .catalog
        .cacheable_files(written.table_id())
        .await
        .expect("files")
        .expect("settled");
    let data_prefix =
        cayenne::dataset_storage::object_key_of_url(&url::Url::parse(&data_url).expect("url"))
            .expect("prefix");
    assert_eq!(files.deletions.len(), 1, "one deletion vector");
    let dv_key = files.deletions[0].key.clone();
    let dv_path = Path::parse(format!("{data_prefix}/{dv_key}")).expect("deletion vector path");
    assert!(
        objects.head(&dv_path).await.is_ok(),
        "the deletion vector is at {dv_path}"
    );
    let dv_size = files.deletions[0].size;

    let dir = TempDir::new().expect("cache dir");
    let cache = open_cache(&dir, 64 << 20).await;
    let replica = Node::open(&objects, Access::Replica, Some(&cache)).await;
    let table = replica.open_table("orders", &data_url).await;
    table.reconcile_disk_cache_now().await;
    cache.wait_idle().await;
    let first = cache.stats();
    let resident_first = resident(&replica, &dv_path, 0..dv_size).await;
    for _ in 0..3 {
        table.reconcile_disk_cache_now().await;
        cache.wait_idle().await;
    }
    let later = cache.stats();
    let resident_later = resident(&replica, &dv_path, 0..dv_size).await;
    assert!(resident_first && resident_later);
    assert_eq!(later.evicted_bytes_unreferenced, 0);
    assert_eq!(later.fetched_bytes_prefetch, first.fetched_bytes_prefetch);
}
