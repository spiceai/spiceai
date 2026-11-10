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

//! The orphan sweep of a dataset in the object-native location
//! (`cayenne::provider::orphan_sweep`), on fault-injecting stores: an
//! unreferenced object goes only once the store dates it older than the floor
//! and no write in flight holds it; an object whose commit outcome is unknown,
//! a reader's snapshot and a committed file the listing misses are kept;
//! unfinished multipart uploads are aborted after the floor. A table that is
//! never written again is swept when it opens, the sweep deletes in batched
//! requests, and it counts what it reclaimed.

use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{
    NativeCatalogConfig, ObjectNativeCatalog, new_dataset_folder,
};
use cayenne::provider::orphan_sweep::{UnfinishedUpload, UnfinishedUploads};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use chrono::Utc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    UploadPart,
};
use object_store_occ::sim::Op;
use object_store_occ::sim::fault::{Fault, FaultStore};
use parking_lot::Mutex;

const CATALOG: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";
/// The floor the tests sweep with: long enough that a file the test just wrote
/// is young, short enough to wait out.
const FLOOR: Duration = Duration::from_millis(400);

/// An in-memory store that tracks its unfinished multipart uploads, initiated
/// on its own clock, can hide keys from listings, lists some keys as last
/// modified earlier than they were, and records the keys of every delete call.
#[derive(Debug, Default)]
struct TrackingStore {
    inner: InMemory,
    uploads: Arc<Mutex<BTreeMap<String, UnfinishedUpload>>>,
    hidden: Mutex<HashSet<String>>,
    /// Keys listed as last modified this long before the store stamped them.
    aged: Mutex<BTreeMap<String, Duration>>,
    /// The keys of each `delete_stream` call, in order.
    deletes: Arc<Mutex<Vec<Vec<String>>>>,
}

impl TrackingStore {
    /// `meta` as the listings report it: hidden keys dropped, aged keys backdated.
    fn listed(
        hidden: &HashSet<String>,
        aged: &BTreeMap<String, Duration>,
        mut meta: ObjectMeta,
    ) -> Option<ObjectMeta> {
        if hidden.contains(meta.location.as_ref()) {
            return None;
        }
        if let Some(age) = aged.get(meta.location.as_ref()) {
            meta.last_modified -= chrono::Duration::from_std(*age).expect("an age");
        }
        Some(meta)
    }

    /// The `DeleteObjects` requests an S3 store sends for the recorded delete
    /// calls that touch a key under `prefix`: each call's keys in requests of
    /// up to 1000 keys.
    fn delete_requests_under(&self, prefix: &str) -> Vec<usize> {
        self.deletes
            .lock()
            .iter()
            .filter(|keys| keys.iter().any(|key| key.starts_with(prefix)))
            .flat_map(|keys| keys.chunks(1000).map(<[String]>::len))
            .collect()
    }
}

impl Display for TrackingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TrackingStore")
    }
}

#[derive(Debug)]
struct TrackedUpload {
    inner: Box<dyn MultipartUpload>,
    id: String,
    uploads: Arc<Mutex<BTreeMap<String, UnfinishedUpload>>>,
}

#[async_trait]
impl MultipartUpload for TrackedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.uploads.lock().remove(&self.id);
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.uploads.lock().remove(&self.id);
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for TrackingStore {
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
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        let id = uuid::Uuid::now_v7().to_string();
        self.uploads.lock().insert(
            id.clone(),
            UnfinishedUpload {
                location: location.clone(),
                upload_id: id.clone(),
                initiated: Utc::now(),
            },
        );
        Ok(Box::new(TrackedUpload {
            inner,
            id,
            uploads: Arc::clone(&self.uploads),
        }))
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let deletes = Arc::clone(&self.deletes);
        let call = {
            let mut calls = deletes.lock();
            calls.push(Vec::new());
            calls.len() - 1
        };
        self.inner.delete_stream(
            locations
                .inspect(move |path| {
                    if let Ok(path) = path {
                        deletes.lock()[call].push(path.to_string());
                    }
                })
                .boxed(),
        )
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let hidden = self.hidden.lock().clone();
        let aged = self.aged.lock().clone();
        self.inner
            .list(prefix)
            .try_filter_map(move |meta| {
                futures::future::ready(Ok(Self::listed(&hidden, &aged, meta)))
            })
            .boxed()
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let hidden = self.hidden.lock().clone();
        let aged = self.aged.lock().clone();
        self.inner
            .list_with_offset(prefix, offset)
            .try_filter_map(move |meta| {
                futures::future::ready(Ok(Self::listed(&hidden, &aged, meta)))
            })
            .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        let hidden = self.hidden.lock().clone();
        let aged = self.aged.lock().clone();
        result.objects = std::mem::take(&mut result.objects)
            .into_iter()
            .filter_map(|meta| Self::listed(&hidden, &aged, meta))
            .collect();
        Ok(result)
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

/// The tracking store's unfinished uploads, as the sweep sees them.
#[derive(Debug)]
struct Uploads(Arc<TrackingStore>);

#[async_trait]
impl UnfinishedUploads for Uploads {
    async fn list(&self, prefix: &Path) -> object_store::Result<Vec<UnfinishedUpload>> {
        Ok(self
            .0
            .uploads
            .lock()
            .values()
            .filter(|upload| upload.location.as_ref().starts_with(prefix.as_ref()))
            .cloned()
            .collect())
    }
    async fn abort(&self, upload: &UnfinishedUpload) -> object_store::Result<()> {
        self.0.uploads.lock().remove(&upload.upload_id);
        Ok(())
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

struct Rig {
    tracking: Arc<TrackingStore>,
    faults: Arc<FaultStore>,
    store: Arc<dyn ObjectStore>,
}

struct Placed {
    native: Arc<ObjectNativeCatalog>,
    data_url: String,
    data_prefix: String,
    table: Arc<CayenneTableProvider>,
    ctx: SessionContext,
}

impl Rig {
    fn new() -> Self {
        let tracking = Arc::new(TrackingStore::default());
        let faults = Arc::new(FaultStore::new(
            Arc::clone(&tracking) as Arc<dyn ObjectStore>
        ));
        let store = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        Self {
            tracking,
            faults,
            store,
        }
    }

    /// A rig whose tables use the tracking store directly: the fault layer
    /// sends each key of a delete call on its own, so the tracking store sees
    /// the calls the table makes only without it.
    fn without_faults() -> Self {
        let tracking = Arc::new(TrackingStore::default());
        let faults = Arc::new(FaultStore::new(
            Arc::clone(&tracking) as Arc<dyn ObjectStore>
        ));
        let store = Arc::clone(&tracking) as Arc<dyn ObjectStore>;
        Self {
            tracking,
            faults,
            store,
        }
    }

    async fn catalog(&self) -> Arc<ObjectNativeCatalog> {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(&self.store),
            Path::from(CATALOG),
            LOCATION,
            NativeCatalogConfig {
                publish_attempts: 2,
                retry_delay: Duration::from_millis(10),
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .expect("open the catalog");
        catalog.init().await.expect("init");
        Arc::new(catalog)
    }

    /// `name` in the location, created (`data_url` `None`) or reopened, with a
    /// compaction trigger of 3 files.
    async fn place(
        &self,
        native: Arc<ObjectNativeCatalog>,
        name: &str,
        data_url: Option<String>,
    ) -> Placed {
        let data_url = data_url.unwrap_or_else(|| {
            format!(
                "s3://bucket/state/cayenne/v1/datasets/{}/data",
                new_dataset_folder(name)
            )
        });
        let data_prefix = data_url.trim_start_matches("s3://bucket/").to_string();
        let ctx = SessionContext::new();
        let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let builder = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(&data_url).expect("data url"),
                store: Arc::clone(&self.store),
            })
            .with_unfinished_uploads(Arc::new(Uploads(Arc::clone(&self.tracking))));
        let table = if native.get_table(name).await.is_ok() {
            builder.open(name).await.expect("open the table")
        } else {
            builder
                .create(CreateTableOptions {
                    table_name: name.to_string(),
                    schema: schema(),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: data_url.clone(),
                    partition_column: None,
                    vortex_config: VortexConfig {
                        inline_max_rows: 0,
                        inline_max_bytes: 0,
                        compaction_trigger_files: 3,
                        target_vortex_file_size_mb: 1,
                        compaction_max_levels: 3,
                        compaction_max_files_per_pick: 3,
                        compaction_background_interval_ms: 0,
                        ..VortexConfig::default()
                    },
                })
                .await
                .expect("create the table")
        };
        let table = Arc::new(table);
        table.set_orphan_age_floor(FLOOR);
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        Placed {
            native,
            data_url,
            data_prefix,
            table,
            ctx,
        }
    }

    /// `.vortex` keys under `prefix` in the bottom store.
    async fn data_files(&self, prefix: &str) -> Vec<String> {
        let mut keys: Vec<String> = self
            .tracking
            .inner
            .list(Some(&Path::from(prefix)))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .into_iter()
            .filter(|key| key.ends_with(".vortex"))
            .collect();
        keys.sort();
        keys
    }
}

impl Placed {
    async fn insert(&self, first: i64, last: i64) -> datafusion::error::Result<()> {
        self.ctx
            .sql(&format!(
                "INSERT INTO {} SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})",
                self.table.table_name()
            ))
            .await?
            .collect()
            .await
            .map(|_| ())
    }

    async fn count(&self) -> datafusion::error::Result<(i64, i64)> {
        let batches = self
            .ctx
            .sql(&format!(
                "SELECT count(*), count(DISTINCT id) FROM {}",
                self.table.table_name()
            ))
            .await?
            .collect()
            .await?;
        let column = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64")
                .value(0)
        };
        Ok((column(0), column(1)))
    }

    /// Every `(snapshot, file)` key the table's state names, as full keys.
    async fn named_keys(&self) -> HashSet<String> {
        let stored = self
            .native
            .get_table(self.table.table_name())
            .await
            .expect("table");
        self.native
            .get_all_snapshot_files(&stored.table_id)
            .await
            .expect("manifest")
            .into_iter()
            .map(|file| {
                format!(
                    "{}/{}/{}/{}",
                    self.data_prefix, stored.table_id, file.snapshot_id, file.file_path
                )
            })
            .collect()
    }
}

fn head_of(native: &ObjectNativeCatalog, dataset: &str) -> Path {
    native
        .dataset_log(dataset)
        .expect("a dataset log")
        .join("head")
}

async fn eventually<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{what} within 15 s");
}

/// An instance whose log another instance took over writes data files its
/// commit can never name. The new writer keeps them while the store dates them
/// younger than the floor, then deletes them, and never adopts them: its rows
/// are only its own, and every file it names is still there.
#[tokio::test]
async fn an_orphan_of_a_fenced_writer_is_reclaimed_after_the_floor_and_never_adopted() {
    let rig = Rig::new();
    let first = rig.place(rig.catalog().await, "orders", None).await;
    first.insert(1, 1000).await.expect("first write");
    first
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");

    // A second instance takes the dataset over; the first one's next write uploads
    // its files, then its commit is fenced.
    let second = rig
        .place(rig.catalog().await, "orders", Some(first.data_url.clone()))
        .await;
    first
        .insert(1001, 2000)
        .await
        .expect_err("the fenced instance's write is not saved");
    let named = second.named_keys().await;
    // Its staged originals sit beside a staging WAL, which staging recovery
    // owns; the copies in the live snapshot directory are this sweep's.
    let orphans: Vec<String> = rig
        .data_files(&second.data_prefix)
        .await
        .into_iter()
        .filter(|key| !named.contains(key) && !key.contains("/_staging/"))
        .collect();
    assert!(
        !orphans.is_empty(),
        "the fenced write left files no commit names"
    );

    let young = second.table.sweep_orphan_files_now().await.expect("sweep");
    assert!(
        young.deleted.is_empty(),
        "nothing is deleted inside the floor: {young:?}"
    );
    assert!(
        young.kept_young >= orphans.len(),
        "the orphans are kept as young: {young:?}"
    );

    // The floor is time under test: wait it out on the store's clock.
    tokio::time::sleep(FLOOR + Duration::from_millis(200)).await;
    let swept = second.table.sweep_orphan_files_now().await.expect("sweep");
    let remaining = rig.data_files(&second.data_prefix).await;
    for orphan in &orphans {
        assert!(
            !remaining.contains(orphan),
            "orphan {orphan} was not reclaimed: {swept:?}"
        );
    }
    for key in second.named_keys().await {
        assert!(
            remaining.contains(&key),
            "the sweep deleted committed file {key}"
        );
    }
    assert_eq!(
        second.count().await.expect("count"),
        (1000, 1000),
        "never adopted"
    );
    second
        .insert(2001, 2500)
        .await
        .expect("the new writer writes");
    assert_eq!(second.count().await.expect("count"), (1500, 1500));
    // A restart from the store serves the same rows.
    let data_url = second.data_url.clone();
    drop((first, second));
    let third = rig
        .place(rig.catalog().await, "orders", Some(data_url))
        .await;
    assert_eq!(
        third.count().await.expect("count"),
        (1500, 1500),
        "a restart adopts nothing"
    );
}

/// While a commit's outcome is unknown the dataset's state cannot be read, so
/// the sweep deletes nothing; once it reads its state back, the files of the
/// commit that landed are named and kept.
#[tokio::test]
async fn an_upload_whose_commit_outcome_is_unknown_is_not_reclaimed() {
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("first write");
    placed.table.set_orphan_age_floor(Duration::ZERO);
    let head = head_of(&placed.native, "orders");
    let cas_head = head.clone();
    rig.faults.arm(
        Op::Cas,
        move |path: &Path| path == &cas_head,
        Fault::DropResponse,
    );
    let get_head = head.clone();
    rig.faults
        .fail_while(Op::Get, move |path: &Path| path == &get_head);
    placed
        .insert(1001, 2000)
        .await
        .expect_err("the write is not acknowledged");
    let before = rig.data_files(&placed.data_prefix).await;
    let refused = placed.table.sweep_orphan_files_now().await;
    assert!(
        refused
            .as_ref()
            .map_or(true, |sweep| sweep.deleted.is_empty()),
        "the sweep deleted while the outcome was unknown: {refused:?}"
    );
    assert_eq!(rig.data_files(&placed.data_prefix).await, before);

    rig.faults.clear_rules();
    eventually("the dataset accepts writes again", || async {
        placed.insert(2001, 2100).await.is_ok()
    })
    .await;
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    let remaining = rig.data_files(&placed.data_prefix).await;
    for key in placed.named_keys().await {
        assert!(
            remaining.contains(&key),
            "the sweep deleted committed file {key}: {swept:?}"
        );
    }
    assert_eq!(placed.count().await.expect("count"), (2100, 2100));
}

/// A write in flight whose data files are uploaded at their final keys and
/// whose commit has not landed yet: a sweep with no floor at all that runs
/// meanwhile deletes none of its files (it reads the dataset's state only once
/// the commit is out of the way, and the write's own record holds the files
/// until then), and the write then commits all of them.
#[tokio::test]
async fn a_write_in_flight_keeps_its_uploaded_files_whatever_their_age() {
    let rig = Rig::new();
    let placed = Arc::new(rig.place(rig.catalog().await, "orders", None).await);
    placed.insert(1, 1000).await.expect("first write");
    placed.table.set_orphan_age_floor(Duration::ZERO);
    // Hold the write's commit: its data files are uploaded by then, and no
    // commit names them yet.
    let dataset_head = head_of(&placed.native, "orders");
    rig.faults.arm(
        Op::Cas,
        move |path: &Path| path == &dataset_head,
        Fault::Hold,
    );
    let writer = {
        let placed = Arc::clone(&placed);
        tokio::spawn(async move { placed.insert(1001, 2000).await })
    };
    let held = rig.faults.wait_held(None).await;
    let uploaded = rig.data_files(&placed.data_prefix).await;
    let sweeper = {
        let placed = Arc::clone(&placed);
        tokio::spawn(async move { placed.table.sweep_orphan_files_now().await })
    };
    rig.faults.deliver(held).await.expect("the commit lands");
    rig.faults.respond(held).expect("the writer hears it");
    writer.await.expect("join").expect("the write commits");
    let swept = sweeper.await.expect("join the sweep").expect("sweep");
    assert!(
        swept.deleted.is_empty(),
        "the sweep deleted a write's files in flight: {swept:?}"
    );
    let remaining = rig.data_files(&placed.data_prefix).await;
    for key in &uploaded {
        assert!(
            remaining.contains(key),
            "the sweep deleted {key}, uploaded before the commit: {swept:?}"
        );
    }
    assert_eq!(placed.count().await.expect("count"), (2000, 2000));
}

/// A query that started before a compaction retired its snapshot and before
/// the sweeps ran, with no floor, still returns every row.
#[tokio::test]
async fn a_reader_that_opened_before_a_sweep_finishes_with_its_rows() {
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    for i in 0..5 {
        placed
            .insert(i * 1000 + 1, (i + 1) * 1000)
            .await
            .expect("append");
    }
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    placed.table.set_orphan_age_floor(Duration::ZERO);
    let mut stream = placed
        .ctx
        .sql("SELECT id FROM orders")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");
    let mut rows = 0;
    if let Some(batch) = stream.next().await {
        rows += batch.expect("first batch").num_rows();
    }
    let before = placed
        .native
        .get_table("orders")
        .await
        .expect("table")
        .current_snapshot_id;
    // The background pass may have compacted already; more appends give the
    // explicit pass small files to replace the reader's snapshot with.
    for i in 5..8 {
        placed
            .insert(i * 1000 + 1, (i + 1) * 1000)
            .await
            .expect("append");
    }
    for _ in 0..40 {
        let _ = placed
            .table
            .compact_current_snapshot_small_files()
            .await
            .expect("compaction");
        placed
            .table
            .drain_in_flight_maintenance()
            .await
            .expect("drain");
        if placed
            .native
            .get_table("orders")
            .await
            .expect("table")
            .current_snapshot_id
            != before
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(
        placed
            .native
            .get_table("orders")
            .await
            .expect("table")
            .current_snapshot_id,
        before,
        "a compaction replaced the snapshot the reader reads"
    );
    placed.table.sweep_retired_snapshot_dirs_now().await;
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    while let Some(batch) = stream.next().await {
        rows += batch.expect("a later batch").num_rows();
    }
    assert_eq!(rows, 5000, "the reader finished with every row: {swept:?}");
    assert_eq!(placed.count().await.expect("count"), (8000, 8000));
}

/// A multipart upload under the table's prefix that its writer never finished
/// is aborted once the store dates it older than the floor, not before.
#[tokio::test]
async fn an_unfinished_multipart_upload_is_aborted_after_the_floor() {
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("first write");
    let stored = placed.native.get_table("orders").await.expect("table");
    let key = Path::from(format!(
        "{}/{}/{}/abandoned.vortex",
        placed.data_prefix, stored.table_id, stored.current_snapshot_id
    ));
    let mut upload = rig
        .store
        .put_multipart(&key)
        .await
        .expect("begin an upload");
    upload
        .put_part(PutPayload::from_static(&[0_u8; 1024]))
        .await
        .expect("a part");
    // The writer goes away without completing or aborting it.
    drop(upload);
    let pending = |rig: &Rig| rig.tracking.uploads.lock().len();
    assert_eq!(pending(&rig), 1);

    let young = placed.table.sweep_orphan_files_now().await.expect("sweep");
    assert!(
        young.aborted_uploads.is_empty(),
        "aborted inside the floor: {young:?}"
    );
    assert_eq!(pending(&rig), 1);
    tokio::time::sleep(FLOOR + Duration::from_millis(200)).await;
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    assert_eq!(
        swept.aborted_uploads,
        vec![format!("{}/abandoned.vortex", stored.current_snapshot_id)],
        "the upload is aborted after the floor"
    );
    assert_eq!(pending(&rig), 0);
    assert_eq!(placed.count().await.expect("count"), (1000, 1000));
}

/// A committed file that the store's listings do not return is still read,
/// compacted and carried, is never deleted by the sweep, and stays in the
/// manifest across a reopen.
#[tokio::test]
async fn a_committed_file_the_listing_misses_is_not_dropped() {
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("append");
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let hidden = rig.data_files(&placed.data_prefix).await;
    assert_eq!(hidden.len(), 1, "one committed file: {hidden:?}");
    rig.tracking.hidden.lock().extend(hidden.iter().cloned());
    for i in 1..5 {
        placed
            .insert(i * 1000 + 1, (i + 1) * 1000)
            .await
            .expect("append");
    }
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    for _ in 0..40 {
        if placed
            .table
            .compact_current_snapshot_small_files()
            .await
            .expect("compaction")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    placed.table.set_orphan_age_floor(Duration::ZERO);
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    assert_eq!(
        placed.count().await.expect("count"),
        (5000, 5000),
        "{swept:?}"
    );
    let remaining = rig.data_files(&placed.data_prefix).await;
    for key in placed.named_keys().await {
        assert!(
            remaining.contains(&key),
            "committed file {key} was deleted: {swept:?}"
        );
    }
    let data_url = placed.data_url.clone();
    drop(placed);
    let reopened = rig
        .place(rig.catalog().await, "orders", Some(data_url))
        .await;
    assert_eq!(reopened.count().await.expect("count"), (5000, 5000));
}

/// A snapshot directory nothing refers to under `placed`'s table, holding one
/// planted object of `size` bytes per entry of `sizes`; returns the directory
/// and the planted keys.
async fn plant_orphans(rig: &Rig, placed: &Placed, sizes: &[usize]) -> (String, Vec<String>) {
    let stored = placed.native.get_table("orders").await.expect("table");
    let dir = format!(
        "{}/{}/{}",
        placed.data_prefix,
        stored.table_id,
        uuid::Uuid::now_v7()
    );
    let mut keys = Vec::with_capacity(sizes.len());
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("{dir}/orphan-{i:05}.vortex");
        rig.store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(vec![0_u8; *size]),
            )
            .await
            .expect("plant an orphan");
        keys.push(key);
    }
    (dir, keys)
}

/// A crashed write left objects no commit names, and the dataset is never
/// written again. Its next open sweeps them on the one-hour floor: the object
/// the store dates two hours old goes, the one it dates half an hour old stays,
/// and every committed file and row is kept.
#[tokio::test]
async fn an_orphan_of_a_table_that_is_not_written_again_is_reclaimed_at_open() {
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("write");
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let stored = placed.native.get_table("orders").await.expect("table");
    let dir = format!(
        "{}/{}/{}",
        placed.data_prefix, stored.table_id, stored.current_snapshot_id
    );
    let old = format!("{dir}/crashed-old.vortex");
    let young = format!("{dir}/crashed-young.vortex");
    for key in [&old, &young] {
        rig.store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from_static(b"orphan"),
            )
            .await
            .expect("plant an orphan");
    }
    rig.tracking.aged.lock().extend([
        (old.clone(), Duration::from_hours(2)),
        (young.clone(), Duration::from_mins(30)),
    ]);
    let named = placed.named_keys().await;
    let before = rig.data_files(&placed.data_prefix).await;
    assert!(
        before.contains(&old)
            && before.contains(&young)
            && named.iter().all(|key| before.contains(key)),
        "the orphans and the committed files are stored: {before:?}"
    );
    let data_url = placed.data_url.clone();
    drop(placed);

    // A restart: a new catalog and table over the same store, and no write.
    let reopened = rig
        .place(rig.catalog().await, "orders", Some(data_url))
        .await;
    eventually("the orphan older than the floor is reclaimed", || async {
        !rig.data_files(&reopened.data_prefix).await.contains(&old)
    })
    .await;
    let remaining = rig.data_files(&reopened.data_prefix).await;
    let mut lost: Vec<&String> = named
        .iter()
        .filter(|key| !remaining.contains(key))
        .collect();
    lost.sort();
    assert_eq!(
        (
            remaining.contains(&young),
            lost,
            reopened.count().await.expect("count")
        ),
        (true, Vec::<&String>::new(), (1000, 1000)),
        "the orphan younger than the floor is kept, no committed file is lost, rows"
    );
}

/// The sweep deletes what it reclaims in batched requests: 1001 orphans go in
/// two `DeleteObjects` requests, not one request each.
#[tokio::test]
async fn the_orphan_sweep_deletes_in_batched_requests() {
    const ORPHANS: usize = 1001;
    let rig = Rig::without_faults();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("write");
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let (dir, keys) = plant_orphans(&rig, &placed, &[8; ORPHANS]).await;
    assert_eq!(rig.data_files(&dir).await, keys, "the orphans are stored");
    placed.table.set_orphan_age_floor(Duration::ZERO);
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    assert_eq!(
        (
            swept.deleted.len(),
            rig.tracking.delete_requests_under(&dir).len(),
            rig.data_files(&dir).await.len(),
            placed.count().await.expect("count"),
        ),
        (ORPHANS, ORPHANS.div_ceil(1000), 0, (1000, 1000)),
        "orphans deleted, delete requests, orphans left, rows"
    );
}

/// The sweep counts the files and bytes it reclaims in
/// `cayenne_maintenance_reclaimed_{files,bytes}_total` under its own `op`.
#[tokio::test]
async fn the_orphan_sweep_counts_what_it_reclaims() {
    crate::common::require_process_per_test("it installs the process-wide meter provider");
    let reader = telemetry::metrics_reader::MetricsReader::new_cumulative();
    opentelemetry::global::set_meter_provider(
        opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build(),
    );
    let rig = Rig::new();
    let placed = rig.place(rig.catalog().await, "orders", None).await;
    placed.insert(1, 1000).await.expect("write");
    placed
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let sizes = [100, 200, 300];
    let (_dir, keys) = plant_orphans(&rig, &placed, &sizes).await;
    placed.table.set_orphan_age_floor(Duration::ZERO);
    let swept = placed.table.sweep_orphan_files_now().await.expect("sweep");
    let mut deleted = swept.deleted.clone();
    deleted.sort();
    let stored = placed.native.get_table("orders").await.expect("table");
    let table_prefix = format!("{}/{}/", placed.data_prefix, stored.table_id);
    let planted: Vec<String> = keys
        .iter()
        .map(|key| key.trim_start_matches(&table_prefix).to_string())
        .collect();
    assert_eq!(
        (
            deleted,
            reclaimed(&reader, "cayenne_maintenance_reclaimed_files_total"),
            reclaimed(&reader, "cayenne_maintenance_reclaimed_bytes_total"),
        ),
        (planted, 3, 600),
        "objects the sweep deleted, and the files and bytes it counted"
    );
}

/// The cumulative value of the counter `name` for the `orders` table and the
/// orphan sweep's `op`.
fn reclaimed(reader: &telemetry::metrics_reader::MetricsReader, name: &str) -> u64 {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader as _;
    let mut resource_metrics = ResourceMetrics::default();
    reader
        .collect(&mut resource_metrics)
        .expect("collect metrics");
    let mut total = 0;
    for metric in resource_metrics
        .scope_metrics()
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
    {
        if metric.name() != name {
            continue;
        }
        if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
            for point in sum.data_points() {
                let label = |key: &str| {
                    point
                        .attributes()
                        .find(|attribute| attribute.key.as_str() == key)
                        .map(|attribute| attribute.value.as_str().to_string())
                        .unwrap_or_default()
                };
                if label("table") == "orders" && label("op") == "orphan_file_sweep" {
                    total += point.value();
                }
            }
        }
    }
    total
}
