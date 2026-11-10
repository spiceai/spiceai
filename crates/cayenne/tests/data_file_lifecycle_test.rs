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

//! What happens to a data file in the object-native location once a commit
//! stops using it. The commit that takes a snapshot out of the live set is its
//! retirement record; a sweep deletes the snapshot's files only after that
//! commit, in batched requests, never while a query still reads them or a
//! restore point is being taken, and records the deletion in one commit per
//! batch. A restart in the middle of a sweep leaves the rest to the next
//! instance, and a delete still in flight from a replaced instance removes no
//! file the new instance uses.
//!
//! The stores stack as: [`Instance`] (one per process: a kill switch, a gate
//! for deletes, a log of its requests and of the paths each delete carried) over
//! a [`FaultStore`] over a [`Probe`] (the shared storage) over an `InMemory`. The fault store sends each path of a delete on its own, so delete
//! requests are counted above it, as a process sends them.

#![expect(clippy::expect_used, reason = "test helpers")]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::metadata::{
    CdcDurability, CreateTableOptions, DeletionMode, ObjectStoreConfig, VortexConfig,
};
use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use cayenne::{CayenneContext, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use object_store_occ::sim::Op;
use object_store_occ::sim::fault::FaultStore;

use crate::common;

const CATALOG: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";

/// Upserts of the same keys: each writes a protected snapshot and a key
/// deletion vector.
const UPSERTS: usize = 8;
/// Rows per upsert, and keys of the keyed table.
const KEYS: i64 = 100;
/// No automatic compaction trigger fires, so a test runs each pass itself.
const PARKED: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

/// The shared storage under every instance.
#[derive(Debug)]
struct Probe {
    inner: Arc<InMemory>,
}

impl Probe {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
        }
    }

    /// Every object under `prefix`, with its size.
    async fn objects(&self, prefix: &str) -> BTreeMap<String, u64> {
        self.inner
            .list(Some(&Path::from(prefix)))
            .map_ok(|meta| (meta.location.to_string(), meta.size))
            .try_collect()
            .await
            .expect("list the bottom store")
    }
}

impl Display for Probe {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Probe")
    }
}

#[async_trait]
impl ObjectStore for Probe {
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

/// One process's view of storage: every request it sends goes through here.
/// Once dead (killed, or out of its request budget) it fails every request, as
/// a killed process sends none; deletes can be held at a gate.
#[derive(Debug)]
struct InstanceState {
    /// Requests left before the instance dies; negative once dead.
    budget: AtomicI64,
    dead: AtomicBool,
    /// Every request sent, as `<op> <path>`.
    steps: parking_lot::Mutex<Vec<String>>,
    gate: parking_lot::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    /// Delete requests waiting at the gate.
    gated: AtomicUsize,
    /// Every delete request sent, as the paths it carried.
    deletes: parking_lot::Mutex<Vec<Vec<String>>>,
}

#[derive(Debug)]
struct Instance {
    inner: Arc<dyn ObjectStore>,
    state: Arc<InstanceState>,
}

impl Instance {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            state: Arc::new(InstanceState {
                budget: AtomicI64::new(i64::MAX),
                dead: AtomicBool::new(false),
                steps: parking_lot::Mutex::new(Vec::new()),
                gate: parking_lot::Mutex::new(None),
                gated: AtomicUsize::new(0),
                deletes: parking_lot::Mutex::new(Vec::new()),
            }),
        }
    }

    /// Die after `requests` more requests.
    fn die_after(&self, requests: usize) {
        self.state
            .budget
            .store(i64::try_from(requests).expect("budget"), Ordering::SeqCst);
    }

    fn kill(&self) {
        self.state.dead.store(true, Ordering::SeqCst);
    }

    fn steps(&self) -> Vec<String> {
        self.state.steps.lock().clone()
    }

    fn clear_steps(&self) {
        self.state.steps.lock().clear();
    }

    /// Hold every delete request from now on until [`Self::open_gate`].
    fn close_gate(&self) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *self.state.gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    fn gated(&self) -> usize {
        self.state.gated.load(Ordering::SeqCst)
    }

    /// The delete requests this instance sent since the last call, each as the
    /// paths it carried.
    fn take_deletes(&self) -> Vec<Vec<String>> {
        std::mem::take(&mut *self.state.deletes.lock())
    }
}

impl InstanceState {
    /// Admit one request, or fail it because this instance is dead.
    fn admit(&self, op: &str, path: &str) -> object_store::Result<()> {
        if self.dead.load(Ordering::SeqCst) || self.budget.fetch_sub(1, Ordering::SeqCst) <= 0 {
            self.dead.store(true, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "Instance",
                source: format!("the process is dead: {op} {path} was never sent").into(),
            });
        }
        self.steps.lock().push(format!("{op} {path}"));
        Ok(())
    }
}

impl Display for Instance {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Instance")
    }
}

#[derive(Debug)]
struct InstanceUpload {
    inner: Box<dyn MultipartUpload>,
    state: Arc<InstanceState>,
    path: String,
}

#[async_trait]
impl MultipartUpload for InstanceUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if let Err(error) = self.state.admit("part", &self.path) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.state.admit("complete", &self.path)?;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.admit("abort", &self.path)?;
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for Instance {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.state.admit("put", location.as_ref())?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.state.admit("multipart", location.as_ref())?;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(InstanceUpload {
            inner,
            state: Arc::clone(&self.state),
            path: location.to_string(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.state.admit("get", location.as_ref())?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.state.admit("get", location.as_ref())?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let gate = self.state.gate.lock().clone();
        stream::once(async move {
            let paths: Vec<object_store::Result<Path>> = locations.collect().await;
            let first = paths
                .iter()
                .find_map(|path| path.as_ref().ok().map(ToString::to_string))
                .unwrap_or_default();
            state.deletes.lock().push(
                paths
                    .iter()
                    .filter_map(|path| path.as_ref().ok().map(ToString::to_string))
                    .collect(),
            );
            if let Err(error) = state.admit("delete", &first) {
                let failed: Vec<object_store::Result<Path>> = paths
                    .into_iter()
                    .map(|_| {
                        Err(object_store::Error::Generic {
                            store: "Instance",
                            source: error.to_string().into(),
                        })
                    })
                    .collect();
                return stream::iter(failed).boxed();
            }
            if let Some(gate) = gate {
                state.gated.fetch_add(1, Ordering::SeqCst);
                let _permit = gate.acquire().await.expect("the gate stays open");
            }
            inner.delete_stream(stream::iter(paths).boxed())
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(error) = self.state.admit("list", prefix.map_or("", Path::as_ref)) {
            return stream::once(async move { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(error) = self.state.admit("list", prefix.map_or("", Path::as_ref)) {
            return stream::once(async move { Err(error) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.state.admit("list", prefix.map_or("", Path::as_ref))?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.state.admit("copy", to.as_ref())?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.state.admit("rename", to.as_ref())?;
        self.inner.rename_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

/// Storage shared by every instance, and where one dataset's data lives in it.
struct Location {
    probe: Arc<Probe>,
    data_url: String,
    data_prefix: String,
}

/// One process: its store stack, catalog, table and session.
struct Process {
    instance: Arc<Instance>,
    faults: Arc<FaultStore>,
    catalog: Arc<dyn MetadataCatalog>,
    table: Arc<CayenneTableProvider>,
    ctx: SessionContext,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Every write a data file, key deletes, and every automatic trigger parked.
fn parked() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        deletion_mode: DeletionMode::Key,
        compaction_trigger_files: PARKED,
        compaction_trigger_protected_snapshots: PARKED,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// [`parked`], with the merge of protected snapshots and of small files armed for
/// a pass a test runs itself.
fn armed() -> VortexConfig {
    VortexConfig {
        compaction_trigger_protected_snapshots: 2,
        // Every test write is a small file (under a quarter of the target).
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 2,
        compaction_max_levels: 3,
        compaction_max_files_per_pick: 32,
        ..parked()
    }
}

/// One file per partition, so a scan reads the files one after another.
fn session(config: SessionConfig) -> SessionContext {
    SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build(),
    )
}

impl Location {
    async fn new(name: &str) -> (Self, ObjectNativeCatalog, Arc<Instance>, Arc<FaultStore>) {
        let probe = Arc::new(Probe::new());
        let (instance, faults, store) = Self::stack(&probe);
        let catalog = ObjectNativeCatalog::open_or_create_at(store, Path::from(CATALOG), LOCATION)
            .await
            .expect("create the object-native catalog");
        catalog.init().await.expect("init");
        let folder = new_dataset_folder(name);
        let data_prefix = format!("{}/{folder}/data", catalog.datasets_prefix());
        (
            Self {
                probe,
                data_url: format!("s3://bucket/{data_prefix}"),
                data_prefix,
            },
            catalog,
            instance,
            faults,
        )
    }

    fn stack(probe: &Arc<Probe>) -> (Arc<Instance>, Arc<FaultStore>, Arc<dyn ObjectStore>) {
        let faults = Arc::new(FaultStore::new(Arc::clone(probe) as Arc<dyn ObjectStore>));
        let instance = Arc::new(Instance::new(Arc::clone(&faults) as Arc<dyn ObjectStore>));
        let store: Arc<dyn ObjectStore> = Arc::clone(&instance) as Arc<dyn ObjectStore>;
        (instance, faults, store)
    }

    fn builder(
        &self,
        store: Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        ctx: &SessionContext,
        name: &str,
        config: &VortexConfig,
    ) -> CayenneTableProviderBuilder {
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(&self.data_url).expect("data url"),
                store,
            })
            .with_context(CayenneContext::new(config, ctx.runtime_env(), name))
    }

    /// Create `name` in a new location; `keyed` upserts on `id`.
    async fn create(name: &str, keyed: bool, config: SessionConfig) -> (Self, Process) {
        Self::create_with(name, keyed, config, &parked()).await
    }

    /// [`Self::create`] with the table's settings.
    async fn create_with(
        name: &str,
        keyed: bool,
        config: SessionConfig,
        vortex: &VortexConfig,
    ) -> (Self, Process) {
        let (location, catalog, instance, faults) = Self::new(name).await;
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
        let ctx = session(config);
        let table = Arc::new(
            location
                .builder(
                    Arc::clone(&instance) as Arc<dyn ObjectStore>,
                    Arc::clone(&catalog),
                    &ctx,
                    name,
                    vortex,
                )
                .create(CreateTableOptions {
                    table_name: name.to_string(),
                    schema: schema(),
                    primary_key: if keyed {
                        vec!["id".to_string()]
                    } else {
                        vec![]
                    },
                    on_conflict: keyed
                        .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
                    base_path: location.data_url.clone(),
                    partition_column: None,
                    vortex_config: vortex.clone(),
                })
                .await
                .expect("create the table"),
        );
        // As the runtime does: a cached scan view pins the snapshots it read
        // until a later scan replaces it or it goes idle.
        table.init_scan_view_cache();
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        (
            location,
            Process {
                instance,
                faults,
                catalog,
                table,
                ctx,
            },
        )
    }

    /// A new process over this location: a catalog that takes every log over,
    /// and `name` opened with `config`.
    async fn open(
        &self,
        name: &str,
        config: &VortexConfig,
        session_config: SessionConfig,
    ) -> Process {
        let (instance, faults, store) = Self::stack(&self.probe);
        let catalog = ObjectNativeCatalog::open_or_create_at(
            Arc::clone(&store),
            Path::from(CATALOG),
            LOCATION,
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog);
        let ctx = session(session_config);
        let table = Arc::new(
            self.builder(store, Arc::clone(&catalog), &ctx, name, config)
                .open(name)
                .await
                .expect("open the table"),
        );
        // The orphan sweep an open runs lists the dataset; wait for it, so a
        // test counts only the requests of what it does next.
        table.wait_for_orphan_sweep().await;
        // As the runtime does: a cached scan view pins the snapshots it read
        // until a later scan replaces it or it goes idle.
        table.init_scan_view_cache();
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        Process {
            instance,
            faults,
            catalog,
            table,
            ctx,
        }
    }

    /// Every data file under the dataset, with its size.
    async fn data_files(&self) -> BTreeMap<String, u64> {
        self.probe
            .objects(&self.data_prefix)
            .await
            .into_iter()
            .filter(|(key, _)| has_extension(key, "vortex"))
            .collect()
    }

    /// The data files of `snapshots`, by key.
    async fn files_of(&self, table_id: &str, snapshots: &[String]) -> BTreeSet<String> {
        self.data_files()
            .await
            .into_keys()
            .filter(|key| {
                snapshots
                    .iter()
                    .any(|id| key.contains(&format!("/{table_id}/{id}/")))
            })
            .collect()
    }
}

impl Process {
    /// Every file this process's committed state names that a scan reads: the
    /// data files of every snapshot not retired, and every deletion vector.
    async fn named_files(&self, location: &Location) -> BTreeSet<String> {
        let table_id = self.table.table_id().to_string();
        let retired: BTreeSet<String> = self
            .catalog
            .retired_snapshots(&table_id)
            .await
            .expect("read the retired snapshots")
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut named: BTreeSet<String> = self
            .catalog
            .get_all_snapshot_files(&table_id)
            .await
            .expect("read the manifest")
            .into_iter()
            .filter(|file| !retired.contains(&file.snapshot_id))
            .map(|file| {
                format!(
                    "{}/{table_id}/{}/{}",
                    location.data_prefix, file.snapshot_id, file.file_path
                )
            })
            .collect();
        for dv in self
            .catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("read the deletion vectors")
        {
            named.insert(dv.path.trim_start_matches("s3://bucket/").to_string());
        }
        named
    }

    /// Every file the committed state names is stored.
    async fn assert_named_files_stored(&self, location: &Location, when: &str) {
        let stored = location.probe.objects(&location.data_prefix).await;
        let missing: Vec<String> = self
            .named_files(location)
            .await
            .into_iter()
            .filter(|key| !stored.contains_key(key))
            .collect();
        assert!(
            missing.is_empty(),
            "{when}: files the committed state names were deleted: {missing:?}"
        );
    }

    async fn retired(&self) -> Option<Vec<String>> {
        self.catalog
            .retired_snapshots(self.table.table_id())
            .await
            .expect("read the retired snapshots")
    }

    async fn rows(&self, name: &str) -> Vec<(i64, i64)> {
        collect_rows(
            self.ctx
                .sql(&format!("SELECT id, v FROM {name} ORDER BY id"))
                .await
                .expect("plan")
                .collect()
                .await
                .expect("scan"),
        )
    }
}

/// Whether the object `key` has the file extension `extension`.
fn has_extension(key: &str, extension: &str) -> bool {
    std::path::Path::new(key)
        .extension()
        .is_some_and(|found| found == extension)
}

fn collect_rows(batches: Vec<RecordBatch>) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64");
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows
}

fn batch(ids: Range<i64>, value: impl Fn(i64) -> i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone().collect::<Vec<_>>())),
            Arc::new(Int64Array::from(ids.map(value).collect::<Vec<_>>())),
        ],
    )
    .expect("batch")
}

/// The keyed table after `upserts` upserts of every key: key `id` holds
/// `id * 1000 + upserts - 1`.
fn upserted(upserts: usize) -> Vec<(i64, i64)> {
    upserted_keys(upserts, KEYS)
}

fn upserted_keys(upserts: usize, keys: i64) -> Vec<(i64, i64)> {
    let last = i64::try_from(upserts).expect("upserts") - 1;
    (0..keys).map(|id| (id, id * 1000 + last)).collect()
}

/// A keyed table holding `upserts` protected snapshots, reopened with its merge
/// armed: a new process over the same location takes no write at open.
async fn keyed_with_protected_snapshots(name: &str, upserts: usize) -> (Location, Process) {
    keyed_table(name, upserts, KEYS, SessionConfig::new()).await
}

/// [`keyed_with_protected_snapshots`] over `keys` keys, scanned with `session`.
async fn keyed_table(
    name: &str,
    upserts: usize,
    keys: i64,
    session: SessionConfig,
) -> (Location, Process) {
    keyed_table_reopened_with(name, upserts, keys, session, &armed()).await
}

/// [`keyed_table`], reopened with `config`.
async fn keyed_table_reopened_with(
    name: &str,
    upserts: usize,
    keys: i64,
    session: SessionConfig,
    config: &VortexConfig,
) -> (Location, Process) {
    let (location, process) = Location::create(name, true, session.clone()).await;
    for round in 0..upserts {
        let round = i64::try_from(round).expect("round");
        common::insert_batch(&process.table, batch(0..keys, |id| id * 1000 + round))
            .await
            .expect("upsert");
    }
    process
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    process.table.quiesce().await;
    drop(process);
    let process = location.open(name, config, session).await;
    process
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    assert_eq!(process.rows(name).await, upserted_keys(upserts, keys));
    (location, process)
}

/// Wait until `done` holds, or fail with what it last saw.
async fn eventually<T: fmt::Debug>(
    within: Duration,
    what: &str,
    mut check: impl AsyncFnMut() -> (bool, T),
) {
    let deadline = Instant::now() + within;
    loop {
        let (done, seen) = check().await;
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} within {within:?}; last saw {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Retirement, batching, pins
// ---------------------------------------------------------------------------

/// A merge retires its input snapshots in its own commit. Their files are kept
/// while a restore point is being taken, then deleted in one batched request
/// once it is done, and the deletion is recorded: the retired set is empty and
/// the rows are unchanged. The merge's orphaned deletion vectors go out in one
/// batched request too.
#[tokio::test(flavor = "multi_thread")]
async fn retired_files_are_deleted_in_batched_requests_after_the_retiring_commit() {
    let (location, process) = keyed_with_protected_snapshots("orders", UPSERTS).await;
    let table_id = process.table.table_id().to_string();
    assert_eq!(process.retired().await, Some(vec![]));

    let hold = process.table.hold_file_deletions().await;
    assert!(
        process
            .table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge"),
        "the merge commits"
    );
    let retired = process.retired().await.expect("the merge committed");
    assert!(
        retired.len() >= 2,
        "the merge retires its inputs: {retired:?}"
    );
    // A scan after the merge replaces the cached view of the merged inputs.
    assert_eq!(process.rows("orders").await, upserted(UPSERTS));
    let retired_files = location.files_of(&table_id, &retired).await;
    assert!(
        retired_files.len() >= 2,
        "the retired snapshots hold data files: {retired_files:?}"
    );

    // A restore point is being taken: nothing is deleted.
    process.instance.take_deletes();
    process.table.reclaim_retired_snapshots_now().await;
    assert_eq!(location.files_of(&table_id, &retired).await, retired_files);
    assert_eq!(process.retired().await, Some(retired.clone()));

    drop(hold);
    process.table.reclaim_retired_snapshots_now().await;
    assert_eq!(
        location.files_of(&table_id, &retired).await,
        BTreeSet::new(),
        "the retired snapshots' files are deleted once the restore point is done"
    );
    assert_eq!(
        process.retired().await,
        Some(vec![]),
        "the deletion is recorded"
    );
    let data_requests: Vec<Vec<String>> = process
        .instance
        .take_deletes()
        .into_iter()
        .filter(|paths| paths.iter().any(|path| retired_files.contains(path)))
        .collect();
    assert_eq!(
        data_requests.len(),
        1,
        "{} retired data files go out in one delete request: {data_requests:?}",
        retired_files.len()
    );
    assert!(
        retired_files
            .iter()
            .all(|file| data_requests[0].contains(file))
    );

    // The merge orphaned one key deletion vector per merged upsert.
    let dvs_before: Vec<String> = location
        .probe
        .objects(&location.data_prefix)
        .await
        .into_keys()
        .filter(|key| has_extension(key, "arrow"))
        .collect();
    process.table.drain_orphan_dv_sweep(1).await;
    let dvs_after: BTreeSet<String> = location
        .probe
        .objects(&location.data_prefix)
        .await
        .into_keys()
        .filter(|key| has_extension(key, "arrow"))
        .collect();
    let removed_dvs: Vec<&String> = dvs_before
        .iter()
        .filter(|key| !dvs_after.contains(*key))
        .collect();
    assert!(
        removed_dvs.len() >= 2,
        "the orphaned deletion vectors are reclaimed: before {dvs_before:?}, after {dvs_after:?}"
    );
    let dv_requests: Vec<Vec<String>> = process
        .instance
        .take_deletes()
        .into_iter()
        .filter(|paths| paths.iter().any(|path| has_extension(path, "arrow")))
        .collect();
    assert_eq!(
        dv_requests.len(),
        1,
        "{} orphaned deletion vectors go out in one delete request: {dv_requests:?}",
        removed_dvs.len()
    );

    assert_eq!(process.rows("orders").await, upserted(UPSERTS));
    process
        .assert_named_files_stored(&location, "after the sweeps")
        .await;
}

/// A scan that started before a merge retired the files it reads, and is polled
/// only after the sweep ran, finishes with every row: the sweep keeps the files
/// while the scan holds them, and deletes them once it is done. (Without the
/// scan's pin the sweep deletes the files and evicts them from the caches, and
/// the scan fails reading them.)
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_that_opened_before_a_sweep_finishes_with_its_rows() {
    const READER_KEYS: i64 = 3_000;
    // One partition, small batches: the scan reads the files one after another.
    let reader_config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(256);
    let (location, process) = keyed_table("events", UPSERTS, READER_KEYS, reader_config).await;
    let table_id = process.table.table_id().to_string();
    let expected = upserted_keys(UPSERTS, READER_KEYS);
    let old_files: BTreeSet<String> = location.data_files().await.into_keys().collect();
    assert!(
        old_files.len() >= 2,
        "the upserts wrote several files: {old_files:?}"
    );

    let mut reader = process
        .ctx
        .sql("SELECT id, v FROM events")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("start the scan");

    // The scan is planned and started; it reads its files as it is polled.
    assert!(
        process
            .table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge"),
        "the merge commits"
    );
    let retired = process.retired().await.expect("the merge committed");
    let retired_files = location.files_of(&table_id, &retired).await;
    assert!(
        retired_files.len() >= 2 && retired_files.is_subset(&old_files),
        "the merge retires files the scan reads: {retired_files:?}"
    );
    process.table.reclaim_retired_snapshots_now().await;
    assert_eq!(
        location.files_of(&table_id, &retired).await,
        retired_files,
        "the sweep keeps the files a running scan reads"
    );
    assert_eq!(process.retired().await, Some(retired.clone()));

    let mut read = Vec::new();
    while let Some(next) = reader.next().await {
        read.extend(collect_rows(vec![
            next.expect("the scan reads on after the sweep"),
        ]));
    }
    read.sort_unstable();
    assert_eq!(read, expected, "the scan returns every row");
    drop(reader);

    process.table.reclaim_retired_snapshots_now().await;
    assert_eq!(
        location.files_of(&table_id, &retired).await,
        BTreeSet::new(),
        "the files are deleted once the scan is done"
    );
    assert_eq!(process.retired().await, Some(vec![]));
    assert_eq!(process.rows("events").await, expected);
    process
        .assert_named_files_stored(&location, "after the sweep")
        .await;
}

/// A process stops in the middle of a sweep: some retired files are deleted,
/// the rest are not, and the deletion is never recorded. The next process
/// finishes the sweep on its own, from the committed retirement record, and the
/// rows are unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_in_the_middle_of_a_sweep_finishes_it_from_the_retirement_record() {
    let (location, first) = keyed_with_protected_snapshots("orders", UPSERTS).await;
    let table_id = first.table.table_id().to_string();
    assert!(
        first
            .table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge")
    );
    let retired = first.retired().await.expect("the merge committed");
    // A scan after the merge replaces the cached view of the merged inputs.
    assert_eq!(first.rows("orders").await, upserted(UPSERTS));
    let retired_files: Vec<String> = location
        .files_of(&table_id, &retired)
        .await
        .into_iter()
        .collect();
    assert!(retired_files.len() >= 2, "{retired_files:?}");
    let failing: BTreeSet<String> = retired_files.iter().skip(1).step_by(2).cloned().collect();
    first.faults.fail_while(Op::Delete, {
        let failing = failing.clone();
        move |path: &Path| failing.contains(path.as_ref())
    });
    first.table.reclaim_retired_snapshots_now().await;
    let left = location.files_of(&table_id, &retired).await;
    assert_eq!(
        left, failing,
        "the sweep deleted only what the store let it"
    );
    assert_eq!(
        first.retired().await,
        Some(retired.clone()),
        "a sweep that left files behind records nothing"
    );

    // The process stops.
    first.instance.kill();
    drop(first);

    let second = location
        .open("orders", &armed(), SessionConfig::new())
        .await;
    eventually(
        Duration::from_secs(30),
        "the next process finishes the sweep",
        async || {
            let left = location.files_of(&table_id, &retired).await;
            let recorded = second.retired().await;
            (
                left.is_empty() && recorded == Some(vec![]),
                (left, recorded),
            )
        },
    )
    .await;
    assert_eq!(second.rows("orders").await, upserted(UPSERTS));
    second
        .assert_named_files_stored(&location, "after the next process's sweep")
        .await;
}

/// A replaced process's delete is still in flight when the next process takes
/// the location over and writes on. When it lands, it removes only the retired
/// files it was sent for: every file the new process uses is stored and its
/// rows are whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_that_lands_after_a_takeover_removes_no_live_file() {
    let (location, first) = keyed_with_protected_snapshots("orders", UPSERTS).await;
    let table_id = first.table.table_id().to_string();
    assert!(
        first
            .table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge")
    );
    let retired = first.retired().await.expect("the merge committed");
    // A scan after the merge replaces the cached view of the merged inputs.
    assert_eq!(first.rows("orders").await, upserted(UPSERTS));
    let retired_files = location.files_of(&table_id, &retired).await;
    assert!(!retired_files.is_empty());

    let gate = first.instance.close_gate();
    let sweep = tokio::spawn({
        let table = Arc::clone(&first.table);
        async move { table.reclaim_retired_snapshots_now().await }
    });
    eventually(
        Duration::from_secs(10),
        "the sweep's delete is held",
        async || (first.instance.gated() > 0, first.instance.gated()),
    )
    .await;

    // The next process takes over and writes on while the delete is held.
    let second = location
        .open("orders", &armed(), SessionConfig::new())
        .await;
    for round in 0..3 {
        common::insert_batch(&second.table, batch(0..KEYS, |id| id * 1000 + 100 + round))
            .await
            .expect("upsert after the takeover");
    }
    assert!(
        second
            .table
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("merge after the takeover")
    );
    let expected: Vec<(i64, i64)> = (0..KEYS).map(|id| (id, id * 1000 + 102)).collect();
    assert_eq!(second.rows("orders").await, expected);

    // The held delete lands.
    gate.add_permits(usize::MAX >> 4);
    sweep.await.expect("the replaced process's sweep ends");
    assert_eq!(
        location.files_of(&table_id, &retired).await,
        BTreeSet::new(),
        "the held delete landed"
    );
    second
        .assert_named_files_stored(&location, "after the held delete landed")
        .await;
    assert_eq!(second.rows("orders").await, expected);
    second.table.reclaim_retired_snapshots_now().await;
    assert_eq!(second.rows("orders").await, expected);
    second
        .assert_named_files_stored(&location, "after the new process's sweep")
        .await;
    assert_eq!(second.retired().await, Some(vec![]));
}

/// An overwrite rolled back before its commit leaves nothing behind in the
/// store: neither the data files of its snapshot nor the deletion vectors it
/// wrote for the copies its input repeated. The rows are the ones before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_overwrite_deletes_its_files_and_deletion_vectors() {
    let position = VortexConfig {
        deletion_mode: DeletionMode::Position,
        ..parked()
    };
    let (location, process) =
        Location::create_with("orders", true, SessionConfig::new(), &position).await;
    common::insert_batch(&process.table, batch(0..KEYS, |id| id))
        .await
        .expect("load");
    process
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let before = location.probe.objects(&location.data_prefix).await;

    // Every key twice, in two batches: the overwrite hides the first copy of
    // each with a position delete.
    let repeated: datafusion::execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema(),
            stream::iter([
                Ok(batch(0..KEYS, |id| -id)),
                Ok(batch(0..KEYS, |id| -2 * id)),
            ]),
        ),
    );
    let prepared = process
        .table
        .begin_overwrite(repeated, 1)
        .await
        .expect("prepare the overwrite");
    let snapshot = prepared.new_snapshot_id().to_string();
    let written: Vec<String> = location
        .probe
        .objects(&location.data_prefix)
        .await
        .into_keys()
        .filter(|key| !before.contains_key(key))
        .collect();
    assert!(
        written
            .iter()
            .any(|key| key.contains(&snapshot) && has_extension(key, "vortex")),
        "the overwrite wrote its snapshot's files: {written:?}"
    );
    assert!(
        written.iter().any(|key| has_extension(key, "arrow")),
        "the overwrite wrote deletion vectors for the repeated copies: {written:?}"
    );

    prepared.rollback().await.expect("roll back");
    let after = location.probe.objects(&location.data_prefix).await;
    let left: Vec<&String> = after
        .keys()
        .filter(|key| !before.contains_key(*key))
        .collect();
    assert!(
        left.is_empty(),
        "the rolled-back overwrite left objects in the store: {left:?}"
    );
    assert_eq!(
        process.rows("orders").await,
        (0..KEYS).map(|id| (id, id)).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Dropped datasets
// ---------------------------------------------------------------------------

impl Location {
    /// The dataset's folder (its log and its data) in the location.
    fn folder(&self) -> String {
        self.data_prefix
            .strip_suffix("/data")
            .expect("the data path ends in the folder's data")
            .to_string()
    }
}

/// A dataset dropped while one of its scans has not finished keeps its folder,
/// past the grace, until that scan is done; the scan returns every row; then the
/// instance that dropped it deletes the whole folder, log and data, with no
/// restart. (The previous rule deleted it at a start at least an hour later.)
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_dataset_is_deleted_once_no_scan_reads_it() {
    let (location, process) = Location::create(
        "orders",
        false,
        SessionConfig::new().with_target_partitions(1),
    )
    .await;
    common::insert_batch(&process.table, batch(0..2_000, |id| id * 7))
        .await
        .expect("load");
    process
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let folder = location.folder();
    assert!(
        location
            .probe
            .objects(&folder)
            .await
            .keys()
            .any(|key| has_extension(key, "vortex")),
        "the dataset's folder holds its data"
    );

    let mut reader = process
        .ctx
        .sql("SELECT id, v FROM orders")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("start the scan");
    assert!(process.catalog.drop_table("orders").await.expect("drop"));

    // Past the grace the folder is still there: the scan holds it.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        location
            .probe
            .objects(&folder)
            .await
            .keys()
            .any(|key| has_extension(key, "vortex")),
        "a dropped dataset's folder is kept while a scan reads it"
    );
    let mut read = Vec::new();
    while let Some(next) = reader.next().await {
        read.extend(collect_rows(vec![
            next.expect("the scan reads on after the drop"),
        ]));
    }
    read.sort_unstable();
    assert_eq!(read, (0..2_000).map(|id| (id, id * 7)).collect::<Vec<_>>());
    drop(reader);

    eventually(
        Duration::from_secs(30),
        "the dropped dataset's folder is deleted",
        async || {
            let left = location.probe.objects(&folder).await;
            (left.is_empty(), left)
        },
    )
    .await;
}

/// A dataset dropped with nothing reading it has its folder deleted by the
/// running instance after the grace; the location's other datasets keep theirs.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_dataset_with_no_reader_is_deleted_after_the_grace() {
    let (location, process) = Location::create("orders", false, SessionConfig::new()).await;
    common::insert_batch(&process.table, batch(0..500, |id| id))
        .await
        .expect("load");
    process
        .table
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let folder = location.folder();
    let started = Instant::now();
    assert!(process.catalog.drop_table("orders").await.expect("drop"));
    eventually(
        Duration::from_secs(30),
        "the dropped dataset's folder is deleted",
        async || {
            let left = location.probe.objects(&folder).await;
            (left.is_empty(), left)
        },
    )
    .await;
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "the folder outlived the grace: deleted after {:?}",
        started.elapsed()
    );
    // The catalog itself is untouched: a new dataset under the same name works.
    let catalog_objects = location.probe.objects(CATALOG).await;
    assert!(!catalog_objects.is_empty(), "the catalog's own log is kept");
}

// ---------------------------------------------------------------------------
// Kills during compaction and overwrite
// ---------------------------------------------------------------------------

/// What a kill test runs, from the same starting table each time.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// Merge the protected snapshots of a keyed table.
    Merge,
    /// Replace an append-only table's rows.
    Overwrite,
    /// Append rows to an append-only table.
    Append,
    /// Upsert every key of a keyed table: a new snapshot and a key deletion vector.
    Upsert,
    /// [`Self::Upsert`] as a change stream applies it, durable on its own: the
    /// write publishes a new protected snapshot when it finishes.
    ChangeStreamUpsert,
}

impl Step {
    fn keyed(self) -> bool {
        matches!(self, Self::Merge | Self::Upsert | Self::ChangeStreamUpsert)
    }

    /// The settings a process runs the step with: a merge and an overwrite with
    /// the merge armed; an append and an upsert with every automatic trigger
    /// parked, so the step's requests are its own.
    fn config(self) -> VortexConfig {
        match self {
            Self::Merge | Self::Overwrite => armed(),
            Self::Append | Self::Upsert => parked(),
            Self::ChangeStreamUpsert => VortexConfig {
                cdc_durability: CdcDurability::File,
                ..parked()
            },
        }
    }

    async fn start(self) -> (Location, Process) {
        if self.keyed() {
            return keyed_table_reopened_with(
                "orders",
                4,
                KEYS,
                SessionConfig::new(),
                &self.config(),
            )
            .await;
        }
        let (location, process) = Location::create("orders", false, SessionConfig::new()).await;
        for append in 0..4 {
            common::insert_batch(
                &process.table,
                batch(append * 500..(append + 1) * 500, |id| id),
            )
            .await
            .expect("append");
        }
        process
            .table
            .drain_in_flight_maintenance()
            .await
            .expect("drain");
        process.table.quiesce().await;
        drop(process);
        let process = location
            .open("orders", &self.config(), SessionConfig::new())
            .await;
        (location, process)
    }

    fn before(self) -> Vec<(i64, i64)> {
        if self.keyed() {
            upserted(4)
        } else {
            (0..2_000).map(|id| (id, id)).collect()
        }
    }

    fn after(self) -> Vec<(i64, i64)> {
        match self {
            Self::Overwrite => (5_000..5_700).map(|id| (id, -id)).collect(),
            Self::Merge => self.before(),
            Self::Append => {
                let mut rows = self.before();
                rows.extend((5_000..5_500).map(|id| (id, id)));
                rows
            }
            Self::Upsert | Self::ChangeStreamUpsert => upserted(5),
        }
    }

    /// The step, then the sweep that deletes what it retired: every storage
    /// request of the step and of its reclamation.
    async fn run(self, table: Arc<CayenneTableProvider>) -> Result<bool, String> {
        let committed = match self {
            Self::Merge => table
                .compact_protected_snapshots_subset(usize::MAX)
                .await
                .map_err(|error| error.to_string()),
            Self::Overwrite => common::write_batches(
                &table,
                vec![batch(5_000..5_700, |id| -id)],
                InsertOp::Overwrite,
            )
            .await
            .map(|_| true)
            .map_err(|error| error.to_string()),
            Self::Append => common::insert_batch(&table, batch(5_000..5_500, |id| id))
                .await
                .map(|_| true)
                .map_err(|error| error.to_string()),
            Self::Upsert => common::insert_batch(&table, batch(0..KEYS, |id| id * 1000 + 4))
                .await
                .map(|_| true)
                .map_err(|error| error.to_string()),
            Self::ChangeStreamUpsert => {
                let rows = batch(0..KEYS, |id| id * 1000 + 4);
                let input: datafusion::execution::SendableRecordBatchStream = Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        rows.schema(),
                        stream::iter([Ok(rows)]),
                    ),
                );
                let ctx = SessionContext::new();
                match table.write_cdc_append_stream(input, &ctx.task_ctx()).await {
                    Ok(write) => write
                        .finish()
                        .await
                        .map(|_| true)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.to_string()),
                }
            }
        }?;
        table.reclaim_retired_snapshots_now().await;
        Ok(committed)
    }

    /// Run the step in `process` as a process that is killed once its store
    /// refuses a request: the step stops there, as a killed process does, and
    /// does not retry. `None` when it was killed before it returned.
    async fn run_until_killed(self, process: &Process) -> Option<Result<bool, String>> {
        let task = tokio::spawn(self.run(Arc::clone(&process.table)));
        loop {
            if task.is_finished() {
                return Some(task.await.expect("the step does not panic"));
            }
            if process.instance.state.dead.load(Ordering::SeqCst) {
                task.abort();
                return None;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// The head commit of `dataset`'s log.
async fn head_commit(store: &Arc<dyn ObjectStore>, dataset: &str) -> u64 {
    let log = cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &Path::from(CATALOG),
        dataset,
    )
    .await
    .expect("read the registry")
    .expect("the dataset has a log");
    let (reader, _) = object_store_occ::headlog::Reader::open(Arc::clone(store), log)
        .await
        .expect("open the head log");
    reader.state().head.commit
}

/// The action names of each record committed to `dataset`'s log after `after`,
/// oldest first.
async fn log_records_after(
    store: &Arc<dyn ObjectStore>,
    dataset: &str,
    after: u64,
) -> Vec<Vec<String>> {
    use prost::Message;
    let log = cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &Path::from(CATALOG),
        dataset,
    )
    .await
    .expect("read the registry")
    .expect("the dataset has a log");
    let (_, opened) = object_store_occ::headlog::Reader::open(Arc::clone(store), log)
        .await
        .expect("open the head log");
    opened
        .records
        .into_iter()
        .filter(|entry| entry.commit > after)
        .map(|entry| {
            object_store_occ::headlog::Group::decode(entry.record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("record")
                        .actions
                        .iter()
                        .map(|action| action.name().to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .collect()
}

/// Run `step` once to count its storage requests, then once per request with the
/// process killed just before it. After each kill, a new process over the same
/// storage serves either every row from before or every row from after (after
/// only, when the step was acknowledged), stores every file its committed state
/// names, and keeps both true through its own sweeps; its orphan sweep then
/// leaves no data file and no deletion vector that no commit names.
async fn kill_at_every_request(step: Step) {
    let (location, process) = step.start().await;
    let log_store = Arc::clone(&location.probe.inner) as Arc<dyn ObjectStore>;
    let before = head_commit(&log_store, "orders").await;
    process.instance.clear_steps();
    assert_eq!(
        step.run(Arc::clone(&process.table)).await,
        Ok(true),
        "{step:?} commits"
    );
    let steps = process.instance.steps();
    process.table.quiesce().await;
    drop(process);
    assert!(!steps.is_empty());
    eprintln!("{step:?}: {} storage requests", steps.len());
    if matches!(step, Step::Append | Step::Upsert | Step::ChangeStreamUpsert) {
        // Each data file is written once, at its final key: nothing is staged,
        // copied, or listed on the write path.
        let staged_or_copied: Vec<&String> = steps
            .iter()
            .filter(|step| {
                step.starts_with("copy ")
                    || step.starts_with("list ")
                    || step.contains("/_staging/")
            })
            .collect();
        assert!(
            staged_or_copied.is_empty(),
            "{step:?}: requests that stage, copy or list: {staged_or_copied:?}"
        );
    }
    if matches!(step, Step::ChangeStreamUpsert) {
        // A keyed write into a new protected snapshot commits in one record: its
        // files, its deletes and its sequence. The only other record the step
        // may write is the sequence allocator's block reservation (one per
        // block of sequences, here the first write after the table opened).
        let heads = steps
            .iter()
            .filter(|step| step.starts_with("put ") && step.ends_with("/head"))
            .count();
        let records = log_records_after(&log_store, "orders", before).await;
        let written = &records[..heads.min(records.len())];
        let with_data: Vec<&Vec<String>> = written
            .iter()
            .filter(|record| record.iter().any(|action| action != "AdvanceSeq"))
            .collect();
        assert_eq!(
            with_data.len(),
            1,
            "{step:?}: one record holds the write, the rest only reserve sequences: {written:?}"
        );
        for kind in ["AddFiles", "AddDeleteFiles", "SetGroup"] {
            assert!(
                with_data[0].iter().any(|action| action == kind),
                "{step:?}: the write's record has no {kind}: {:?}",
                with_data[0]
            );
        }
    }

    let mut acknowledged = 0;
    let mut killed = 0;
    let mut unnamed_total = 0;
    for kill_before in 0..steps.len() {
        let (location, process) = step.start().await;
        process.instance.clear_steps();
        process.instance.die_after(kill_before);
        let outcome = step.run_until_killed(&process).await;
        process.instance.kill();
        let sent = process.instance.steps();
        drop(process);

        let next = location
            .open("orders", &step.config(), SessionConfig::new())
            .await;
        let mut rows = next.rows("orders").await;
        rows.sort_unstable();
        let what = format!(
            "{step:?} killed before request {kill_before} (the clean run's {}), outcome {outcome:?}, last sent {:?}",
            steps
                .get(kill_before)
                .map_or("past the end", String::as_str),
            sent.last()
        );
        match &outcome {
            Some(Ok(true)) => {
                acknowledged += 1;
                assert_eq!(
                    rows,
                    step.after(),
                    "{what}: an acknowledged step is present"
                );
            }
            None => killed += 1,
            Some(_) => {}
        }
        assert!(
            rows == step.before() || rows == step.after(),
            "{what}: the rows are neither all from before nor all from after: {} rows",
            rows.len()
        );
        next.assert_named_files_stored(&location, &what).await;
        next.table.reclaim_retired_snapshots_now().await;
        let mut again = next.rows("orders").await;
        again.sort_unstable();
        assert_eq!(
            again, rows,
            "{what}: the next process's sweep changed the rows"
        );
        next.assert_named_files_stored(
            &location,
            &format!("{what}, after the next process's sweep"),
        )
        .await;
        assert_eq!(
            next.retired().await,
            Some(vec![]),
            "{what}: the next process's sweep deletes and forgets every retired snapshot"
        );
        // Files no commit names: an aborted write's, left for the orphan rules.
        let named = next.named_files(&location).await;
        let unnamed = location
            .data_files()
            .await
            .into_keys()
            .filter(|key| !named.contains(key))
            .count();
        unnamed_total += unnamed;
        // The orphan sweep, with no floor, takes every one of them, data files
        // and deletion vectors alike, and nothing a commit names.
        next.table.set_orphan_age_floor(Duration::ZERO);
        let report = next
            .table
            .sweep_orphan_files_now()
            .await
            .expect("the orphan sweep");
        assert_eq!(report.skipped, None, "{what}: the orphan sweep ran");
        let named = next.named_files(&location).await;
        let left: Vec<String> = location
            .probe
            .objects(&location.data_prefix)
            .await
            .into_keys()
            .filter(|key| has_extension(key, "vortex") || has_extension(key, "arrow"))
            .filter(|key| !named.contains(key))
            .collect();
        assert!(
            left.is_empty(),
            "{what}: files no commit names are left after the orphan sweep: {left:?} (it deleted {:?})",
            report.deleted
        );
        next.assert_named_files_stored(&location, &format!("{what}, after the orphan sweep"))
            .await;
        let mut swept = next.rows("orders").await;
        swept.sort_unstable();
        assert_eq!(swept, rows, "{what}: the orphan sweep changed the rows");
        next.table.quiesce().await;
    }
    eprintln!(
        "{step:?}: {} kill points: {killed} killed mid-step, {acknowledged} acknowledged before the kill, {unnamed_total} data files no commit names left in all",
        steps.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_a_change_stream_upsert_is_all_or_nothing() {
    kill_at_every_request(Step::ChangeStreamUpsert).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_a_protected_merge_loses_nothing() {
    kill_at_every_request(Step::Merge).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_an_overwrite_is_all_or_nothing() {
    kill_at_every_request(Step::Overwrite).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_an_append_leaves_only_orphans() {
    kill_at_every_request(Step::Append).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_an_upsert_leaves_only_orphans() {
    kill_at_every_request(Step::Upsert).await;
}
