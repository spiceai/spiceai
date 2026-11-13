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

//! A process killed at any storage request of two pipelined CDC bursts, on a
//! dataset whose data lives in the object-native location. The change sink
//! runs the second burst's preparation while the first burst's finalizer is
//! still running, so the first burst's record can carry metastore changes the
//! second burst prepared. After the kill, a new process over the same storage
//! must serve the rows from before both bursts, after the first, or after both,
//! with a saved source position that never runs ahead of those rows.
//!
//! The stores stack as: [`Instance`] (one per process: a kill switch and a log
//! of its requests) over a `FaultStore` over an `InMemory` shared by every
//! process.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use arrow::array::{AsArray, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use cayenne::metadata::{
    CdcDurability, CreateTableOptions, DeletionMode, ObjectStoreConfig, VortexConfig,
};
use cayenne::metastore::object_native::{
    NativeCatalogConfig, ObjectNativeCatalog, new_dataset_folder,
};
use cayenne::{CayenneContext, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::common::TableReference;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use object_store_sim::fault::FaultStore;
use runtime_acceleration::change_sink::ChangeSink;
use tokio::runtime::Handle;

use super::*;

const NAME: &str = "orders";
const CATALOG: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";
/// Where the change sink's caller saves the source position of `NAME`.
const POSITION_KEY: &str = "spice_sys_applied_lsn/orders";
/// Keys every burst upserts; each burst writes them to a data file.
const FILE_KEYS: Range<i64> = 0..100;
/// Seeded inline keys: the first burst replaces the first, the second burst the
/// second, so each burst also writes an inline tombstone.
const INLINE_KEYS: [i64; 2] = [1_000, 1_001];
/// No automatic compaction trigger fires.
const PARKED: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

/// One process's view of storage. Once out of its request budget (or killed)
/// it fails every request, as a killed process sends none.
#[derive(Debug)]
struct InstanceState {
    /// Requests left before the instance dies.
    budget: AtomicI64,
    dead: AtomicBool,
    /// Whether only requests that write count against the budget: reads run
    /// concurrently in an order that varies between runs, writes do not.
    writes_only: AtomicBool,
    /// Every request sent, as `<op> <path>`.
    steps: parking_lot::Mutex<Vec<String>>,
}

/// Whether a request only reads.
fn is_read(op: &str) -> bool {
    matches!(op, "get" | "list")
}

impl InstanceState {
    fn admit(&self, op: &str, path: &str) -> object_store::Result<()> {
        let counted = !(is_read(op) && self.writes_only.load(Ordering::SeqCst));
        if self.dead.load(Ordering::SeqCst)
            || (counted && self.budget.fetch_sub(1, Ordering::SeqCst) <= 0)
        {
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
                writes_only: AtomicBool::new(false),
                steps: parking_lot::Mutex::new(Vec::new()),
            }),
        }
    }

    /// Die after `writes` more requests that write; reads pass until then.
    fn die_after_writes(&self, writes: usize) {
        self.state.writes_only.store(true, Ordering::SeqCst);
        self.die_after(writes);
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

    fn is_dead(&self) -> bool {
        self.state.dead.load(Ordering::SeqCst)
    }

    fn steps(&self) -> Vec<String> {
        self.state.steps.lock().clone()
    }

    fn clear_steps(&self) {
        self.state.steps.lock().clear();
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

    /// One request per delete stream, as a batched delete sends.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        stream::once(async move {
            let paths: Vec<object_store::Result<Path>> = locations.collect().await;
            let first = paths
                .iter()
                .find_map(|path| path.as_ref().ok().map(ToString::to_string))
                .unwrap_or_default();
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
// The change sink, with a signal when a burst's preparation returns
// ---------------------------------------------------------------------------

/// Counts the bursts whose preparation (`apply`) has returned. Every other
/// method forwards to the Cayenne backend.
struct PreparedSignal {
    inner: Arc<dyn ChangeSinkBackend>,
    prepared: tokio::sync::watch::Sender<usize>,
}

#[async_trait]
impl ChangeSinkBackend for PreparedSignal {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn capabilities(&self) -> ChangeCapabilities {
        self.inner.capabilities()
    }

    async fn apply(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        ctx: &SessionContext,
        positions: Vec<PositionWrite>,
    ) -> Result<BackendWrite> {
        let write = self.inner.apply(batch, options, ctx, positions).await;
        self.prepared.send_modify(|count| *count += 1);
        write
    }

    fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>) {
        self.inner.set_durability_observer(observer);
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()> {
        self.inner.evolve_schema(plan).await
    }
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// The table the bursts write: durable CDC in files, every write to a data
/// file, key deletes, and every automatic trigger parked.
fn cdc_config() -> VortexConfig {
    VortexConfig {
        cdc_durability: CdcDurability::File,
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

/// [`cdc_config`], with a one-row write inlined: how the seed writes its inline keys.
fn seed_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 1,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    }
}

/// The value key `id` holds after the write of `round` (0 is the seed).
fn value(id: i64, round: i64) -> i64 {
    id * 10 + round
}

fn rows_batch(ids: &[i64], round: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(
                ids.iter().map(|&id| value(id, round)).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("rows")
}

/// The keys burst `round` (1 or 2) upserts: every file key and one inline key.
fn burst_keys(round: i64) -> Vec<i64> {
    let mut ids: Vec<i64> = FILE_KEYS.collect();
    ids.push(INLINE_KEYS[usize::try_from(round - 1).expect("round")]);
    ids
}

/// The source changes of burst `round`: an insert of each of its keys.
fn burst(round: i64) -> ChangeBatch {
    changes(&burst_keys(round), round)
}

/// The source changes that insert `ids` with their values after `round`.
fn changes(ids: &[i64], round: i64) -> ChangeBatch {
    let count = ids.len();
    let offsets: Vec<i32> = (0..=i32::try_from(count).expect("count")).collect();
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(vec!["id"; count])),
        None,
    )
    .expect("keys");
    let record = RecordBatch::try_new(
        Arc::new(data_components::cdc::changes_schema(&schema())),
        vec![
            Arc::new(StringArray::from(vec!["c"; count])),
            Arc::new(keys),
            Arc::new(StructArray::from(rows_batch(ids, round))),
        ],
    )
    .expect("source changes");
    ChangeBatch::cdc(CdcBatch::try_new(record).expect("CDC batch"))
}

/// The table's rows after the writes up to `round`, by key.
fn expected(round: i64) -> Vec<(i64, i64)> {
    let mut rows: BTreeMap<i64, i64> = FILE_KEYS
        .chain(INLINE_KEYS)
        .map(|id| (id, value(id, 0)))
        .collect();
    for burst in 1..=round {
        for id in burst_keys(burst) {
            rows.insert(id, value(id, burst));
        }
    }
    rows.into_iter().collect()
}

/// Storage shared by every process, and where the dataset's data lives in it.
struct Location {
    bottom: Arc<InMemory>,
    data_url: String,
    data_prefix: String,
}

/// One process: its store stack, catalog, table and session.
struct Process {
    instance: Arc<Instance>,
    catalog: Arc<ObjectNativeCatalog>,
    table: Arc<CayenneTableProvider>,
    ctx: SessionContext,
}

fn stack(bottom: &Arc<InMemory>) -> (Arc<Instance>, Arc<dyn ObjectStore>) {
    let faults = Arc::new(FaultStore::new(Arc::clone(bottom) as Arc<dyn ObjectStore>));
    let instance = Arc::new(Instance::new(faults as Arc<dyn ObjectStore>));
    let store = Arc::clone(&instance) as Arc<dyn ObjectStore>;
    (instance, store)
}

/// Crash tests exhaust the real resolve-and-retry path against a permanently dead
/// instance. Keep every retry, but shorten its waits: elapsed backoff time is not
/// under test, and quiesce must drain pending publication before the next reopen.
fn crash_catalog_config() -> NativeCatalogConfig {
    NativeCatalogConfig {
        retry_delay: Duration::from_millis(1),
        ..NativeCatalogConfig::default()
    }
}

impl Location {
    fn builder(
        &self,
        store: Arc<dyn ObjectStore>,
        catalog: &Arc<ObjectNativeCatalog>,
        ctx: &SessionContext,
        config: &VortexConfig,
    ) -> CayenneTableProviderBuilder {
        CayenneTableProviderBuilder::new(
            Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .with_object_store(ObjectStoreConfig {
            url: url::Url::parse(&self.data_url).expect("data url"),
            store,
        })
        .with_context(CayenneContext::new(config, ctx.runtime_env(), NAME))
    }

    /// The seeded table, opened by a new process with [`cdc_config`]: two inline
    /// keys and the file keys, each at round 0.
    async fn seeded() -> (Self, Process) {
        Self::seeded_with(&INLINE_KEYS).await
    }

    /// [`Self::seeded`], with `inline_keys` as the inline keys.
    async fn seeded_with(inline_keys: &[i64]) -> (Self, Process) {
        Self::seeded_as(inline_keys, &seed_config()).await
    }

    /// [`Self::seeded_with`], with the table created with `created` (which
    /// fixes its CDC durability) and seeded with a one-row write inlined.
    async fn seeded_as(inline_keys: &[i64], created: &VortexConfig) -> (Self, Process) {
        let created = VortexConfig {
            inline_max_rows: 1,
            inline_max_bytes: 1 << 20,
            ..created.clone()
        };
        let bottom = Arc::new(InMemory::new());
        let (instance, store) = stack(&bottom);
        let catalog = Arc::new(
            ObjectNativeCatalog::open_or_create_at_with_config(
                Arc::clone(&store),
                Path::from(CATALOG),
                LOCATION,
                crash_catalog_config(),
            )
            .await
            .expect("create the object-native catalog"),
        );
        catalog.init().await.expect("init");
        let data_prefix = format!(
            "{}/{}/data",
            catalog.datasets_prefix(),
            new_dataset_folder(NAME)
        );
        let location = Self {
            bottom,
            data_url: format!("s3://bucket/{data_prefix}"),
            data_prefix,
        };
        let ctx = SessionContext::new();
        let table = Arc::new(
            location
                .builder(store, &catalog, &ctx, &created)
                .create(CreateTableOptions {
                    table_name: NAME.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: location.data_url.clone(),
                    partition_column: None,
                    vortex_config: created,
                })
                .await
                .expect("create the table"),
        );
        for &id in inline_keys {
            insert(&table, &ctx, rows_batch(&[id], 0)).await;
        }
        insert(&table, &ctx, rows_batch(&FILE_KEYS.collect::<Vec<_>>(), 0)).await;
        table
            .drain_in_flight_maintenance()
            .await
            .expect("drain the seed's maintenance");
        table.quiesce().await;
        drop((table, catalog, instance));
        let process = location.open().await;
        let seed: BTreeMap<i64, i64> = FILE_KEYS
            .chain(inline_keys.iter().copied())
            .map(|id| (id, value(id, 0)))
            .collect();
        assert_eq!(
            process.rows().await,
            seed.into_iter().collect::<Vec<_>>(),
            "the seed's rows"
        );
        (location, process)
    }

    /// A new process over this location, as a restart opens it.
    async fn open(&self) -> Process {
        self.open_with(&cdc_config()).await
    }

    /// [`Self::open`], with the table configured by `config`.
    async fn open_with(&self, config: &VortexConfig) -> Process {
        self.open_with_retention(config, Vec::new()).await
    }

    /// [`Self::open_with`], with `retention` as the table's retention filters.
    async fn open_with_retention(
        &self,
        config: &VortexConfig,
        retention: Vec<datafusion::logical_expr::Expr>,
    ) -> Process {
        let (instance, store) = stack(&self.bottom);
        let catalog = Arc::new(
            ObjectNativeCatalog::open_or_create_at_with_config(
                Arc::clone(&store),
                Path::from(CATALOG),
                LOCATION,
                crash_catalog_config(),
            )
            .await
            .expect("open the object-native catalog"),
        );
        catalog.init().await.expect("init");
        let ctx = SessionContext::new();
        let table = Arc::new(
            self.builder(store, &catalog, &ctx, config)
                .with_retention_filters(retention)
                .open(NAME)
                .await
                .expect("open the table"),
        );
        ctx.register_table(NAME, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        Process {
            instance,
            catalog,
            table,
            ctx,
        }
    }

    /// Every data file and deletion vector stored under the dataset.
    async fn stored_files(&self) -> BTreeSet<String> {
        use futures::TryStreamExt;
        self.bottom
            .list(Some(&Path::from(self.data_prefix.as_str())))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<BTreeSet<_>>()
            .await
            .expect("list the shared storage")
            .into_iter()
            .filter(|key| {
                let extension = std::path::Path::new(key).extension();
                extension.is_some_and(|found| found == "vortex" || found == "arrow")
            })
            .collect()
    }
}

async fn insert(table: &Arc<CayenneTableProvider>, ctx: &SessionContext, rows: RecordBatch) {
    let input = MemorySourceConfig::try_new_exec(&[vec![rows]], schema(), None).expect("input");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("plan the insert");
    collect(plan, ctx.task_ctx()).await.expect("insert");
}

impl Process {
    async fn rows(&self) -> Vec<(i64, i64)> {
        self.try_rows().await.expect("scan")
    }

    /// The table's rows, or why the query was refused.
    async fn try_rows(&self) -> std::result::Result<Vec<(i64, i64)>, String> {
        let batches = self
            .ctx
            .sql(&format!("SELECT id, v FROM {NAME} ORDER BY id"))
            .await
            .map_err(|error| error.to_string())?
            .collect()
            .await
            .map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let values = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>();
            rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
        }
        Ok(rows)
    }

    async fn saved_position(&self) -> Option<String> {
        self.catalog
            .sidecar_value(POSITION_KEY)
            .await
            .expect("read the saved position")
            .map(|bytes| String::from_utf8(bytes).expect("a text position"))
    }

    /// Every file the committed state names: the data files not retired, and
    /// every deletion vector. The table is placed in the location, so its data
    /// files are flat: a file of a snapshot lies in the table's directory and
    /// is keyed by its name, and only a file of a staging write keeps its
    /// folder (`<folder>/<name>`).
    async fn named_files(&self, location: &Location) -> BTreeSet<String> {
        let table_id = self.table.table_id().to_string();
        let retired: BTreeSet<String> = self
            .catalog
            .retired_files(&table_id)
            .await
            .expect("read the retired files")
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut named: BTreeSet<String> = self
            .catalog
            .get_all_snapshot_files(&table_id)
            .await
            .expect("read the manifest")
            .into_iter()
            .map(|file| {
                if uuid::Uuid::parse_str(&file.snapshot_id).is_ok() {
                    file.file_path
                } else {
                    format!("{}/{}", file.snapshot_id, file.file_path)
                }
            })
            .filter(|key| !retired.contains(key))
            .map(|key| format!("{}/{table_id}/{key}", location.data_prefix))
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
}

// ---------------------------------------------------------------------------
// The two bursts
// ---------------------------------------------------------------------------

/// How far the pair got before it returned or was killed.
#[derive(Debug, Default)]
struct Acknowledged {
    first: AtomicBool,
    second: AtomicBool,
}

/// The source position write for burst `round`: saves `lsn <round>`.
fn save_position(catalog: &Arc<ObjectNativeCatalog>, round: i64) -> PositionWrite {
    let catalog = Arc::clone(catalog);
    Box::pin(async move {
        catalog
            .set_sidecar_value(POSITION_KEY, Some(format!("lsn {round}").into_bytes()))
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
    })
}

/// Submit burst 1, then burst 2, through one change sink, pipelined: burst 1's
/// finalizer makes its rows visible, then waits (in its source position write)
/// until burst 2's preparation has returned, so burst 2 prepares while burst
/// 1's record is still open. Then both publish.
async fn run_pair(
    table: Arc<CayenneTableProvider>,
    catalog: Arc<ObjectNativeCatalog>,
    acknowledged: Arc<Acknowledged>,
) -> std::result::Result<(), String> {
    let (prepared, mut prepared_count) = tokio::sync::watch::channel(0_usize);
    let inner = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare(NAME),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let backend: Arc<dyn ChangeSinkBackend> = Arc::new(PreparedSignal { inner, prepared });
    let sink = ChangeSink::new(backend, SessionContext::new(), &Handle::current(), 2);
    let options = WriteOptions {
        recovery: Recovery::Durable,
        ..WriteOptions::default()
    };

    let (first_finalized, first_waiting) = tokio::sync::oneshot::channel::<()>();
    let save_first = save_position(&catalog, 1);
    let first_position: PositionWrite = Box::pin(async move {
        // Polled only once burst 1's own finalize has returned.
        let _ = first_finalized.send(());
        prepared_count
            .wait_for(|count| *count >= 2)
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save_first.await
    });
    let first = sink
        .reserve()
        .await
        .map_err(|error| error.to_string())?
        .submit_with_position(burst(1), options, Some(first_position))
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| format!("burst 1: {error}"))?;
    first_waiting
        .await
        .map_err(|_| "burst 1's finalizer stopped before its position write".to_string())?;
    let second = sink
        .reserve()
        .await
        .map_err(|error| error.to_string())?
        .submit_with_position(burst(2), options, Some(save_position(&catalog, 2)))
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| format!("burst 2: {error}"))?;
    first
        .published()
        .await
        .map_err(|error| format!("burst 1 publish: {error}"))?;
    acknowledged.first.store(true, Ordering::SeqCst);
    second
        .published()
        .await
        .map_err(|error| format!("burst 2 publish: {error}"))?;
    acknowledged.second.store(true, Ordering::SeqCst);
    Ok(())
}

/// Run the pair in `process` until it returns or its store refuses a request,
/// as a killed process stops there. `None` when it was killed first.
async fn run_until_killed(
    process: &Process,
    acknowledged: &Arc<Acknowledged>,
) -> Option<std::result::Result<(), String>> {
    let task = tokio::spawn(run_pair(
        Arc::clone(&process.table),
        Arc::clone(&process.catalog),
        Arc::clone(acknowledged),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if task.is_finished() {
            return Some(task.await.expect("the pair does not panic"));
        }
        if process.instance.is_dead() {
            task.abort();
            return None;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pair neither finished nor was killed within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The action names of each commit to the dataset's log after `after`, oldest
/// first; a source position shows its value, and an inline tombstone written
/// inactive says so.
async fn records_after(store: &Arc<dyn ObjectStore>, after: u64) -> Vec<Vec<String>> {
    records_between(store, after, u64::MAX).await
}

/// [`records_after`], up to and including commit `through`.
async fn records_between(
    store: &Arc<dyn ObjectStore>,
    after: u64,
    through: u64,
) -> Vec<Vec<String>> {
    use prost::Message;
    let log = cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &Path::from(CATALOG),
        NAME,
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
        .filter(|entry| entry.commit > after && entry.commit <= through)
        .map(|entry| {
            object_store_occ::headlog::Group::decode(entry.record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("record")
                        .actions
                        .into_iter()
                        .map(|action| match &action.kind {
                            Some(cayenne_metastore_actions::Kind::AdvanceSourcePosition(
                                position,
                            )) => format!(
                                "AdvanceSourcePosition({})",
                                String::from_utf8_lossy(&position.position)
                            ),
                            Some(cayenne_metastore_actions::Kind::AddInlinedDelete(delete)) => {
                                if delete.published {
                                    "AddInlinedDelete".to_string()
                                } else {
                                    "AddInlinedDelete(inactive)".to_string()
                                }
                            }
                            _ => action.name().to_string(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .collect()
}

async fn head_commit(store: &Arc<dyn ObjectStore>) -> u64 {
    let log = cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &Path::from(CATALOG),
        NAME,
    )
    .await
    .expect("read the registry")
    .expect("the dataset has a log");
    let (reader, _) = object_store_occ::headlog::Reader::open(Arc::clone(store), log)
        .await
        .expect("open the head log");
    reader.state().head.commit
}

fn count(record: &[String], kind: &str) -> usize {
    record.iter().filter(|action| *action == kind).count()
}

/// Two pipelined keyed CDC bursts through the change sink on a dataset in the
/// object-native location, killed before each storage request of the pair in
/// turn. After each kill, a new process serves the rows from before both
/// bursts, after the first, or after both (and every acknowledged burst), its
/// saved source position never runs ahead of those rows, and its orphan sweep
/// leaves no data file or deletion vector that no commit names.
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_two_pipelined_cdc_bursts_is_all_or_nothing_per_burst() {
    // A clean run: count the pair's requests, and show that each burst commits
    // in one record of its own, although burst 2 prepares while burst 1's record
    // is still open.
    let (location, process) = Location::seeded().await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let before = head_commit(&bottom).await;
    process.instance.clear_steps();
    let acknowledged = Arc::new(Acknowledged::default());
    assert_eq!(
        run_until_killed(&process, &acknowledged).await,
        Some(Ok(())),
        "the pair commits"
    );
    let steps = process.instance.steps();
    // Every head write the pair causes, its background maintenance included.
    wait_until_idle(&process).await;
    process.table.quiesce().await;
    let heads_once_idle = process
        .instance
        .steps()
        .iter()
        .filter(|step| is_head_write(step))
        .count();
    drop(process);
    let records = records_after(&bottom, before).await;
    eprintln!("clean run: {} storage requests", steps.len());
    for (index, step) in steps.iter().enumerate() {
        eprintln!("  {index:3} {step}");
    }
    eprintln!("clean run records: {records:?}");
    let first_record = records
        .iter()
        .position(|record| {
            record
                .iter()
                .any(|action| action == "AdvanceSourcePosition(lsn 1)")
        })
        .expect("a record saves burst 1's position");
    let second_record = records
        .iter()
        .position(|record| {
            record
                .iter()
                .any(|action| action == "AdvanceSourcePosition(lsn 2)")
        })
        .expect("a record saves burst 2's position");
    assert_eq!(
        first_record + 1,
        second_record,
        "one record per burst: {records:?}"
    );
    // Later records are the table statistics' background record: no data, no
    // deletes, no position, and no tombstone flip.
    for record in &records[second_record + 1..] {
        assert!(
            record.as_slice() == ["SetTableStats"],
            "a later record changes more than statistics: {records:?}"
        );
    }
    // Each burst writes one key deletion vector and one inline tombstone, and
    // its record holds them with its files and its sequence: nothing burst 2
    // prepares rides in burst 1's record. The tombstone commits active.
    for (burst, record) in [(1, first_record), (2, second_record)] {
        assert_eq!(
            (
                count(&records[record], "AddDeleteFiles"),
                count(&records[record], "AddInlinedDelete"),
                count(&records[record], "AddInlinedDelete(inactive)"),
                count(&records[record], "AddFiles"),
                count(&records[record], "SetGroup"),
            ),
            (1, 1, 0, 1, 1),
            "burst {burst}'s record holds its own files, deletes, tombstone and sequence: {:?}",
            records[record]
        );
    }
    // Each data file is written once, at its final key: nothing is staged,
    // copied or listed, and each burst writes the dataset head once.
    let staged_or_copied: Vec<&String> = steps
        .iter()
        .filter(|step| {
            step.starts_with("copy ") || step.starts_with("list ") || step.contains("/_staging/")
        })
        .collect();
    assert!(
        staged_or_copied.is_empty(),
        "requests that stage, copy or list: {staged_or_copied:?}"
    );
    let statistics_records = records[second_record + 1..].len();
    eprintln!(
        "clean run: {heads_once_idle} head writes once idle, {statistics_records} of them statistics records"
    );
    assert_eq!(
        heads_once_idle,
        2 + statistics_records,
        "one head write per burst once idle, besides the statistics records: {records:?}"
    );
    let reopened = location.open().await;
    assert_eq!(reopened.rows().await, expected(2), "the clean run's rows");
    assert_eq!(reopened.saved_position().await.as_deref(), Some("lsn 2"));
    reopened.table.quiesce().await;
    drop(reopened);

    // The window: burst 1's record has committed and burst 2's has not, while
    // burst 2's files are already stored. A kill there must leave burst 2 out
    // entirely.
    let heads: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter(|(_, step)| step.starts_with("put ") && step.ends_with("/head"))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(heads.len(), 2, "one head write per burst: {steps:?}");
    // Killed before request k: requests 0..k were sent.
    let window = heads[0] + 1..=heads[1];
    eprintln!("window: kill points {window:?} (head writes at requests {heads:?})");

    let mut outcomes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut failures = Vec::new();
    for kill_before in 0..steps.len() {
        let (location, process) = Location::seeded().await;
        process.instance.clear_steps();
        process.instance.die_after(kill_before);
        let acknowledged = Arc::new(Acknowledged::default());
        let outcome = run_until_killed(&process, &acknowledged).await;
        process.instance.kill();
        let sent = process.instance.steps();
        process.table.quiesce().await;
        drop(process);

        let next = location.open().await;
        let rows = next.rows().await;
        let position = next.saved_position().await;
        let what = format!(
            "killed before request {kill_before}{} (the clean run's {}), outcome {outcome:?}, last sent {:?}",
            if window.contains(&kill_before) {
                " [window]"
            } else {
                ""
            },
            steps
                .get(kill_before)
                .map_or("past the end", String::as_str),
            sent.last()
        );
        let state = if rows == expected(0) {
            "before"
        } else if rows == expected(1) {
            "after burst 1"
        } else if rows == expected(2) {
            "after burst 2"
        } else {
            "neither"
        };
        outcomes
            .entry(format!("{state}, position {position:?}"))
            .or_default()
            .push(kill_before);
        let mut problems = Vec::new();
        if state == "neither" {
            let expected = expected(1);
            let missing: Vec<_> = expected
                .iter()
                .filter(|row| !rows.contains(row))
                .take(5)
                .collect();
            problems.push(format!(
                "the rows are from neither before, after burst 1, nor after burst 2: {} rows; first rows missing against after burst 1: {missing:?}",
                rows.len()
            ));
        }
        let floor = match position.as_deref() {
            None => 0,
            Some("lsn 1") => 1,
            Some("lsn 2") => 2,
            Some(other) => {
                problems.push(format!("an unknown saved position {other:?}"));
                0
            }
        };
        let reached = match state {
            "before" => Some(0),
            "after burst 1" => Some(1),
            "after burst 2" => Some(2),
            _ => None,
        };
        if reached.is_some_and(|reached| reached < floor) {
            problems.push(format!(
                "the saved position {position:?} runs ahead of the rows ({state})"
            ));
        }
        if acknowledged.second.load(Ordering::SeqCst) && reached != Some(2) {
            problems.push(format!(
                "burst 2 was acknowledged, but the rows are {state}"
            ));
        } else if acknowledged.first.load(Ordering::SeqCst) && reached.is_none_or(|r| r < 1) {
            problems.push(format!(
                "burst 1 was acknowledged, but the rows are {state}"
            ));
        }

        // Every named file is stored; the orphan sweep leaves no unnamed one.
        let stored = location.stored_files().await;
        let named = next.named_files(&location).await;
        let missing: Vec<&String> = named.iter().filter(|key| !stored.contains(*key)).collect();
        if !missing.is_empty() {
            problems.push(format!("named files are not stored: {missing:?}"));
        }
        next.table.set_orphan_age_floor(Duration::ZERO);
        let report = next
            .table
            .sweep_orphan_files_now()
            .await
            .expect("the orphan sweep");
        let named = next.named_files(&location).await;
        let left: Vec<String> = location
            .stored_files()
            .await
            .into_iter()
            .filter(|key| !named.contains(key))
            .collect();
        if report.skipped.is_some() || !left.is_empty() {
            problems.push(format!(
                "the orphan sweep (skipped {:?}) left files no commit names: {left:?}",
                report.skipped
            ));
        }
        let swept = next.rows().await;
        if swept != rows {
            problems.push("the orphan sweep changed the rows".to_string());
        }
        next.table.quiesce().await;
        if !problems.is_empty() {
            failures.push(format!("{what}: {}", problems.join("; ")));
        }
    }
    eprintln!(
        "{} kill points, {} in the window; outcomes by kill point: {outcomes:?}",
        steps.len(),
        window.clone().count()
    );
    assert!(
        failures.is_empty(),
        "{} of {} kill points broke the guarantees:\n{}",
        failures.len(),
        steps.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Head writes per burst, and what a reader sees while a burst's record is held
// ---------------------------------------------------------------------------

/// A change sink over `table` that commits durably.
fn durable_sink(table: &Arc<CayenneTableProvider>) -> (ChangeSink, WriteOptions) {
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare(NAME),
        Arc::clone(table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, SessionContext::new(), &Handle::current(), 2);
    let options = WriteOptions {
        recovery: Recovery::Durable,
        ..WriteOptions::default()
    };
    (sink, options)
}

/// Submit `batch` with `position` and wait until its rows are published.
async fn apply_burst(
    sink: &ChangeSink,
    options: WriteOptions,
    batch: ChangeBatch,
    position: PositionWrite,
) -> std::result::Result<(), String> {
    sink.reserve()
        .await
        .map_err(|error| error.to_string())?
        .submit_with_position(batch, options, Some(position))
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| error.to_string())?
        .published()
        .await
        .map_err(|error| error.to_string())
}

fn is_head_write(step: &str) -> bool {
    step.starts_with("put ") && step.ends_with("/head")
}

/// Wait until the table is idle: no inline tombstone waits for its durable
/// activation and no storage request was sent for a while.
async fn wait_until_idle(process: &Process) {
    let table_id = process.table.table_id().to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        process
            .table
            .drain_in_flight_maintenance()
            .await
            .expect("drain maintenance");
        let sent = process.instance.steps().len();
        // Polls for idleness: any background request in this interval restarts it.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let inactive = process
            .catalog
            .get_unpublished_inlined_delete_ids(&table_id)
            .await
            .expect("read the inactive tombstones");
        if inactive.is_empty() && process.instance.steps().len() == sent {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the table is not idle within 30 s: inactive tombstones {inactive:?}, {} requests sent",
            process.instance.steps().len()
        );
    }
}

/// Keyed CDC bursts through the change sink, one after another, each
/// replacing every file key and one inline key. Once the table is idle (every
/// inline tombstone durably active), each burst has written the dataset head
/// once: its record carries its tombstone active, and no later record flips
/// it. The only other head write is the table statistics' background record.
#[tokio::test(flavor = "multi_thread")]
async fn each_keyed_cdc_burst_writes_the_dataset_head_once() {
    const BURST_INLINE_KEYS: [i64; 3] = [2_000, 2_001, 2_002];
    let (location, process) = Location::seeded_with(&BURST_INLINE_KEYS).await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let before = head_commit(&bottom).await;
    process.instance.clear_steps();
    let (sink, options) = durable_sink(&process.table);
    let mut heads_per_burst = Vec::new();
    for (round, inline_key) in (1_i64..).zip(BURST_INLINE_KEYS) {
        let sent = process.instance.steps().len();
        let mut ids: Vec<i64> = FILE_KEYS.collect();
        ids.push(inline_key);
        apply_burst(
            &sink,
            options,
            changes(&ids, round),
            save_position(&process.catalog, round),
        )
        .await
        .unwrap_or_else(|error| panic!("burst {round}: {error}"));
        heads_per_burst.push(
            process.instance.steps()[sent..]
                .iter()
                .filter(|step| is_head_write(step))
                .count(),
        );
    }
    wait_until_idle(&process).await;
    let steps = process.instance.steps();
    process.table.quiesce().await;
    drop(sink);
    let records = records_after(&bottom, before).await;
    let heads = steps.iter().filter(|step| is_head_write(step)).count();
    eprintln!(
        "{} bursts: {heads} head writes once idle ({} storage requests); head writes while each burst ran: {heads_per_burst:?}",
        BURST_INLINE_KEYS.len(),
        steps.len()
    );
    for (index, record) in records.iter().enumerate() {
        eprintln!("  record {index}: {record:?}");
    }
    let mut rows: BTreeMap<i64, i64> = FILE_KEYS
        .chain(BURST_INLINE_KEYS)
        .map(|id| (id, value(id, 0)))
        .collect();
    for (round, inline_key) in (1_i64..).zip(BURST_INLINE_KEYS) {
        for id in FILE_KEYS.chain([inline_key]) {
            rows.insert(id, value(id, round));
        }
    }
    let reopened = location.open().await;
    assert_eq!(
        reopened.rows().await,
        rows.into_iter().collect::<Vec<_>>(),
        "the bursts' rows after a reopen"
    );
    reopened.table.quiesce().await;
    let (burst_records, other_records): (Vec<&Vec<String>>, Vec<&Vec<String>>) =
        records.iter().partition(|record| {
            record
                .iter()
                .any(|action| action.starts_with("AdvanceSourcePosition"))
        });
    assert_eq!(
        (
            burst_records.len(),
            burst_records
                .iter()
                .map(|record| count(record, "AddInlinedDelete"))
                .collect::<Vec<_>>(),
            records
                .iter()
                .flatten()
                .filter(|action| action.as_str() == "SetInlinedDeletesPublished"
                    || action.as_str() == "AddInlinedDelete(inactive)")
                .count(),
        ),
        (BURST_INLINE_KEYS.len(), vec![1; BURST_INLINE_KEYS.len()], 0),
        "one record per burst, each with its tombstone active, and nothing to flip later: {records:?}"
    );
    // Every other record is the table statistics' background record.
    assert!(
        other_records
            .iter()
            .all(|record| record.as_slice() == ["SetTableStats"]),
        "a record other than a burst's changes more than statistics: {records:?}"
    );
    assert_eq!(
        (heads_per_burst, heads),
        (
            vec![1; BURST_INLINE_KEYS.len()],
            BURST_INLINE_KEYS.len() + other_records.len()
        ),
        "one head write per burst once idle, besides the statistics record: {records:?}"
    );
}

/// What the catalog serves a reader of the table: inline tombstones (active
/// and all), deletion vectors, data files, the table sequence and the saved
/// source position.
async fn catalog_view(process: &Process) -> String {
    let table_id = process.table.table_id().to_string();
    let catalog = &process.catalog;
    format!(
        "active tombstones {}, tombstones {}, deletion vectors {}, data files {}, sequence {}, position {:?}",
        catalog
            .get_published_inlined_deletes(&table_id)
            .await
            .expect("tombstones")
            .len(),
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("tombstones")
            .len(),
        catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("deletion vectors")
            .len(),
        catalog
            .get_all_snapshot_files(&table_id)
            .await
            .expect("data files")
            .len(),
        catalog
            .get_sequence_number(&table_id)
            .await
            .expect("sequence"),
        process.saved_position().await,
    )
}

impl Process {
    async fn query(&self, sql: &str) -> Vec<(i64, i64)> {
        let batches = self
            .ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("query");
        let mut rows = Vec::new();
        for batch in batches {
            let first = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let second = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>();
            rows.extend((0..batch.num_rows()).map(|i| (first.value(i), second.value(i))));
        }
        rows
    }

    /// The rows of `id`, by a point lookup.
    async fn key_rows(&self, id: i64) -> Vec<(i64, i64)> {
        self.query(&format!("SELECT id, v FROM {NAME} WHERE id = {id}"))
            .await
    }

    /// `(count(*), sum(v))`.
    async fn aggregates(&self) -> Vec<(i64, i64)> {
        self.query(&format!(
            "SELECT count(*), CAST(sum(v) AS BIGINT) FROM {NAME}"
        ))
        .await
    }

    /// Every row, the rows of `id` and the aggregates, after the inline view is
    /// rebuilt from the catalog.
    async fn rebuilt_view(&self, id: i64) -> (Vec<(i64, i64)>, Vec<(i64, i64)>, Vec<(i64, i64)>) {
        self.table.rebuild_scan_inputs_on_next_read();
        let rows = self.rows().await;
        self.table.rebuild_scan_inputs_on_next_read();
        let key = self.key_rows(id).await;
        self.table.rebuild_scan_inputs_on_next_read();
        let aggregates = self.aggregates().await;
        (rows, key, aggregates)
    }
}

/// Poll until the dataset head is past `commit`.
async fn wait_for_head_past(store: &Arc<dyn ObjectStore>, commit: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let head = head_commit(store).await;
        if head > commit {
            return head;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the head stays at {head} for 60 s, expected past {commit}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A keyed CDC burst that replaces an inline key, upserts every file key and
/// inserts a new key is held twice: first after its record is in the catalog's
/// buffer and before the record's head write, then after the head write and
/// before its publish. While held, a reader (with the inline view and scan
/// views rebuilt from the catalog) sees exactly the rows from before the
/// burst: one row for the key, with its old value, and the old aggregates.
/// Once the burst publishes, a reader sees its rows. The burst's record carries
/// its inline tombstone active; the tombstone must not hide the old inline row
/// before the publish adds the replacement to the scanned snapshots.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_sees_no_row_of_a_burst_whose_record_is_not_committed() {
    const KEY: i64 = 700;
    /// A key the burst inserts.
    const NEW_KEY: i64 = 900;
    let (location, process) = Location::seeded_with(&[KEY]).await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let (rows_before, key_before, aggregates_before) = process.rebuilt_view(KEY).await;
    assert_eq!(key_before, vec![(KEY, value(KEY, 0))], "the seeded key");
    let catalog_before = catalog_view(&process).await;
    let head_before = head_commit(&bottom).await;

    let mut ids: Vec<i64> = FILE_KEYS.collect();
    ids.extend([KEY, NEW_KEY]);
    let (sink, options) = durable_sink(&process.table);
    let (held, mut holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let save = save_position(&process.catalog, 1);
    // Polled once the burst's record is in the catalog's buffer: held there.
    let position: PositionWrite = Box::pin(async move {
        let _ = held.send(());
        released
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save.await
    });
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(&sink, options, changes(&ids, 1), position).await;
        drop(sink);
        outcome
    });
    tokio::time::timeout(Duration::from_secs(60), &mut holding)
        .await
        .expect("the burst reaches its position write within 60 s")
        .expect("the burst reaches its position write");

    // Held before the head write.
    let head_held = head_commit(&bottom).await;
    let catalog_held = catalog_view(&process).await;
    let cached_key_held = process.key_rows(KEY).await;
    let (rows_held, key_held, aggregates_held) = process.rebuilt_view(KEY).await;

    // Held after the head write, before the publish.
    let publications = process.table.hold_publications().await;
    release.send(()).expect("release the burst");
    let head_committed = wait_for_head_past(&bottom, head_before).await;
    let catalog_committed = catalog_view(&process).await;
    let (rows_committed, key_committed, aggregates_committed) = process.rebuilt_view(KEY).await;
    drop(publications);

    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let head_after = head_commit(&bottom).await;
    let catalog_after = catalog_view(&process).await;
    let (rows_after, key_after, aggregates_after) = process.rebuilt_view(KEY).await;

    eprintln!("before:    head {head_before}, catalog [{catalog_before}]");
    eprintln!("before:    key {KEY} {key_before:?}, (count, sum) {aggregates_before:?}");
    eprintln!("held:      head {head_held}, catalog [{catalog_held}]");
    eprintln!(
        "held:      key {KEY} cached {cached_key_held:?}, rebuilt {key_held:?}, (count, sum) {aggregates_held:?}, rows {} (equal to before: {})",
        rows_held.len(),
        rows_held == rows_before
    );
    eprintln!("committed: head {head_committed}, catalog [{catalog_committed}]");
    eprintln!(
        "committed: key {KEY} rebuilt {key_committed:?}, (count, sum) {aggregates_committed:?}, rows {} (equal to before: {})",
        rows_committed.len(),
        rows_committed == rows_before
    );
    eprintln!("after:     head {head_after}, catalog [{catalog_after}]");
    eprintln!(
        "after:     key {KEY} {key_after:?}, (count, sum) {aggregates_after:?}, rows {}",
        rows_after.len()
    );

    let mut expected_after: BTreeMap<i64, i64> = rows_before.iter().copied().collect();
    for id in FILE_KEYS.chain([KEY, NEW_KEY]) {
        expected_after.insert(id, value(id, 1));
    }
    let expected_after: Vec<(i64, i64)> = expected_after.into_iter().collect();
    let sum_after: i64 = expected_after.iter().map(|(_, v)| v).sum();
    assert_eq!(head_held, head_before, "the held record is not committed");
    assert_eq!(
        (
            cached_key_held,
            key_held,
            aggregates_held,
            rows_held == rows_before
        ),
        (
            key_before.clone(),
            key_before.clone(),
            aggregates_before.clone(),
            true
        ),
        "before its head write, a reader sees exactly the committed rows"
    );
    assert_eq!(
        (
            key_committed,
            aggregates_committed,
            rows_committed == rows_before
        ),
        (key_before, aggregates_before, true),
        "after its head write and before its publish, a reader sees the rows from before the burst"
    );
    assert_eq!(
        (key_after, aggregates_after, rows_after == expected_after),
        (
            vec![(KEY, value(KEY, 1))],
            vec![(
                i64::try_from(expected_after.len()).expect("count"),
                sum_after
            )],
            true
        ),
        "once the burst publishes, a reader sees its rows"
    );
    process.table.quiesce().await;
}

/// A keyed CDC burst small enough to be written inline, read while its record
/// is buffered (before its head write): once it is acknowledged, a reader sees
/// its row, both from the inline view it already holds and from one rebuilt
/// from the catalog. The inline write publishes its row in memory before the
/// record commits, and the inline view's incremental refresh reads the rows
/// above its watermark from the catalog, so a catalog that served readers only
/// committed records would leave the refreshed view without the row for good.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_burst_read_while_its_record_is_buffered_is_visible_once_acknowledged() {
    const KEY: i64 = 700;
    const NEW_KEY: i64 = 5_000;
    let (location, process) = Location::seeded_with(&[KEY]).await;
    process.table.quiesce().await;
    drop(process);
    let inline = VortexConfig {
        inline_max_rows: 1_024,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    };
    let process = location.open_with(&inline).await;
    let rows_before = process.rows().await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let head_before = head_commit(&bottom).await;

    let (sink, options) = durable_sink(&process.table);
    let (held, mut holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let save = save_position(&process.catalog, 1);
    let position: PositionWrite = Box::pin(async move {
        let _ = held.send(());
        released
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save.await
    });
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(&sink, options, changes(&[NEW_KEY], 1), position).await;
        drop(sink);
        outcome
    });
    tokio::time::timeout(Duration::from_secs(60), &mut holding)
        .await
        .expect("the burst reaches its position write within 60 s")
        .expect("the burst reaches its position write");
    let head_held = head_commit(&bottom).await;
    let new_key_held = process.key_rows(NEW_KEY).await;
    let rows_held = process.rows().await;
    release.send(()).expect("release the burst");
    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let new_key_after = process.key_rows(NEW_KEY).await;
    let rows_after = process.rows().await;
    let (rows_rebuilt, new_key_rebuilt, _) = process.rebuilt_view(NEW_KEY).await;
    eprintln!(
        "inline burst: held (head {head_held}, before {head_before}): key {NEW_KEY} {new_key_held:?}, rows {} (before {}); acknowledged: key {NEW_KEY} {new_key_after:?}, rows {}; rebuilt: key {NEW_KEY} {new_key_rebuilt:?}, rows {}",
        rows_held.len(),
        rows_before.len(),
        rows_after.len(),
        rows_rebuilt.len()
    );
    let mut expected: BTreeMap<i64, i64> = rows_before.into_iter().collect();
    expected.insert(NEW_KEY, value(NEW_KEY, 1));
    let expected: Vec<(i64, i64)> = expected.into_iter().collect();
    assert_eq!(
        (
            new_key_after,
            rows_after == expected,
            rows_rebuilt == expected
        ),
        (vec![(NEW_KEY, value(NEW_KEY, 1))], true, true),
        "once acknowledged, the inline burst's row is visible"
    );
    process.table.quiesce().await;
}

/// A keyed CDC burst small enough to be written inline, which replaces an
/// inline key and inserts a new key, is held twice: first after its record is
/// in the catalog's buffer and before the record's head write, then after the
/// head write and before its publish. While held, a reader (with the inline
/// view and scan views it holds, and with both rebuilt from the catalog) sees
/// exactly the rows from before the burst. Once the burst is acknowledged, a
/// reader sees each of its rows exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_sees_no_row_of_an_inline_burst_whose_record_is_not_committed() {
    const KEY: i64 = 700;
    const NEW_KEY: i64 = 5_000;
    let (location, process) = Location::seeded_with(&[KEY]).await;
    process.table.quiesce().await;
    drop(process);
    let inline = VortexConfig {
        inline_max_rows: 1_024,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    };
    let process = location.open_with(&inline).await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let (rows_before, key_before, aggregates_before) = process.rebuilt_view(KEY).await;
    assert_eq!(
        key_before,
        vec![(KEY, value(KEY, 0))],
        "the seeded inline key"
    );
    let head_before = head_commit(&bottom).await;

    let (sink, options) = durable_sink(&process.table);
    let (held, mut holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let save = save_position(&process.catalog, 1);
    // Polled once the burst's record is in the catalog's buffer: held there.
    let position: PositionWrite = Box::pin(async move {
        let _ = held.send(());
        released
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save.await
    });
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(&sink, options, changes(&[KEY, NEW_KEY], 1), position).await;
        drop(sink);
        outcome
    });
    tokio::time::timeout(Duration::from_secs(60), &mut holding)
        .await
        .expect("the burst reaches its position write within 60 s")
        .expect("the burst reaches its position write");

    // Held before the head write: first what the reader already holds, then
    // views rebuilt from the catalog.
    let head_held = head_commit(&bottom).await;
    let cached_held = (
        process.rows().await,
        process.key_rows(KEY).await,
        process.key_rows(NEW_KEY).await,
    );
    let (rows_held, key_held, aggregates_held) = process.rebuilt_view(KEY).await;
    let (_, new_key_held, _) = process.rebuilt_view(NEW_KEY).await;

    // Held after the head write, before the publish.
    let publications = process.table.hold_publications().await;
    release.send(()).expect("release the burst");
    let head_committed = wait_for_head_past(&bottom, head_before).await;
    let (rows_committed, key_committed, aggregates_committed) = process.rebuilt_view(KEY).await;
    let (_, new_key_committed, _) = process.rebuilt_view(NEW_KEY).await;
    drop(publications);

    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let cached_after = (
        process.rows().await,
        process.key_rows(KEY).await,
        process.key_rows(NEW_KEY).await,
    );
    let (rows_after, key_after, aggregates_after) = process.rebuilt_view(KEY).await;
    let (_, new_key_after, _) = process.rebuilt_view(NEW_KEY).await;

    eprintln!(
        "inline burst: before: head {head_before}, key {KEY} {key_before:?}, (count, sum) {aggregates_before:?}, rows {}",
        rows_before.len()
    );
    eprintln!(
        "inline burst: held (head {head_held}): cached key {KEY} {:?}, key {NEW_KEY} {:?}, rows {}; rebuilt key {KEY} {key_held:?}, key {NEW_KEY} {new_key_held:?}, (count, sum) {aggregates_held:?}, rows {} (equal to before: {})",
        cached_held.1,
        cached_held.2,
        cached_held.0.len(),
        rows_held.len(),
        rows_held == rows_before
    );
    eprintln!(
        "inline burst: committed (head {head_committed}): rebuilt key {KEY} {key_committed:?}, key {NEW_KEY} {new_key_committed:?}, (count, sum) {aggregates_committed:?}, rows {} (equal to before: {})",
        rows_committed.len(),
        rows_committed == rows_before
    );
    eprintln!(
        "inline burst: acknowledged: cached key {KEY} {:?}, key {NEW_KEY} {:?}, rows {}; rebuilt key {KEY} {key_after:?}, key {NEW_KEY} {new_key_after:?}, (count, sum) {aggregates_after:?}, rows {}",
        cached_after.1,
        cached_after.2,
        cached_after.0.len(),
        rows_after.len()
    );

    let mut expected_after: BTreeMap<i64, i64> = rows_before.iter().copied().collect();
    expected_after.insert(KEY, value(KEY, 1));
    expected_after.insert(NEW_KEY, value(NEW_KEY, 1));
    let expected_after: Vec<(i64, i64)> = expected_after.into_iter().collect();
    let sum_after: i64 = expected_after.iter().map(|(_, v)| v).sum();
    assert_eq!(head_held, head_before, "the held record is not committed");
    assert_eq!(
        (
            cached_held,
            (
                key_held,
                new_key_held,
                aggregates_held,
                rows_held == rows_before
            )
        ),
        (
            (rows_before.clone(), key_before.clone(), Vec::new()),
            (
                key_before.clone(),
                Vec::new(),
                aggregates_before.clone(),
                true
            )
        ),
        "before its head write, a reader sees exactly the committed rows"
    );
    assert_eq!(
        (
            key_committed,
            new_key_committed,
            aggregates_committed,
            rows_committed == rows_before
        ),
        (key_before, Vec::new(), aggregates_before, true),
        "after its head write and before its publish, a reader sees the rows from before the burst"
    );
    assert_eq!(
        (
            cached_after,
            (
                key_after,
                new_key_after,
                aggregates_after,
                rows_after == expected_after
            )
        ),
        (
            (
                expected_after.clone(),
                vec![(KEY, value(KEY, 1))],
                vec![(NEW_KEY, value(NEW_KEY, 1))]
            ),
            (
                vec![(KEY, value(KEY, 1))],
                vec![(NEW_KEY, value(NEW_KEY, 1))],
                vec![(
                    i64::try_from(expected_after.len()).expect("count"),
                    sum_after
                )],
                true
            )
        ),
        "once acknowledged, a reader sees each of the burst's rows exactly once"
    );
    process.table.quiesce().await;
}

/// The source changes that apply `ops` (`(op, id)`, with `op` one of `c` and
/// `d`) in order, with values after `round`.
fn mixed_changes(ops: &[(&str, i64)], round: i64) -> ChangeBatch {
    let count = ops.len();
    let ids: Vec<i64> = ops.iter().map(|(_, id)| *id).collect();
    let offsets: Vec<i32> = (0..=i32::try_from(count).expect("count")).collect();
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(vec!["id"; count])),
        None,
    )
    .expect("keys");
    let record = RecordBatch::try_new(
        Arc::new(data_components::cdc::changes_schema(&schema())),
        vec![
            Arc::new(StringArray::from(
                ops.iter().map(|(op, _)| *op).collect::<Vec<_>>(),
            )),
            Arc::new(keys),
            Arc::new(StructArray::from(rows_batch(&ids, round))),
        ],
    )
    .expect("source changes");
    ChangeBatch::cdc(CdcBatch::try_new(record).expect("CDC batch"))
}

/// A keyed CDC burst that deletes a file key and an inline key, held after its
/// record is in the catalog's buffer and before the record's head write: a
/// reader (with what it holds, and with views rebuilt from the catalog) still
/// sees both rows. Once acknowledged, neither row is visible.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_sees_no_delete_of_a_burst_whose_record_is_not_committed() {
    const INLINE_KEY: i64 = 700;
    const FILE_KEY: i64 = 7;
    let (location, process) = Location::seeded_with(&[INLINE_KEY]).await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let (rows_before, inline_before, aggregates_before) = process.rebuilt_view(INLINE_KEY).await;
    let head_before = head_commit(&bottom).await;

    let (sink, options) = durable_sink(&process.table);
    let (held, mut holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let save = save_position(&process.catalog, 1);
    let position: PositionWrite = Box::pin(async move {
        let _ = held.send(());
        released
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save.await
    });
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(
            &sink,
            options,
            mixed_changes(&[("d", FILE_KEY), ("d", INLINE_KEY)], 1),
            position,
        )
        .await;
        drop(sink);
        outcome
    });
    tokio::time::timeout(Duration::from_secs(60), &mut holding)
        .await
        .expect("the burst reaches its position write within 60 s")
        .expect("the burst reaches its position write");
    let head_held = head_commit(&bottom).await;
    let cached_held = (
        process.rows().await,
        process.key_rows(FILE_KEY).await,
        process.key_rows(INLINE_KEY).await,
    );
    let (rows_held, inline_held, aggregates_held) = process.rebuilt_view(INLINE_KEY).await;
    let (_, file_held, _) = process.rebuilt_view(FILE_KEY).await;
    release.send(()).expect("release the burst");
    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let (rows_after, inline_after, _) = process.rebuilt_view(INLINE_KEY).await;
    let (_, file_after, _) = process.rebuilt_view(FILE_KEY).await;
    eprintln!(
        "delete burst: before: head {head_before}, rows {}, (count, sum) {aggregates_before:?}; held (head {head_held}): cached rows {}, key {FILE_KEY} {:?}, key {INLINE_KEY} {:?}; rebuilt rows {}, key {FILE_KEY} {file_held:?}, key {INLINE_KEY} {inline_held:?}, (count, sum) {aggregates_held:?}; acknowledged: rows {}, key {FILE_KEY} {file_after:?}, key {INLINE_KEY} {inline_after:?}",
        rows_before.len(),
        cached_held.0.len(),
        cached_held.1,
        cached_held.2,
        rows_held.len(),
        rows_after.len()
    );
    let expected_after: Vec<(i64, i64)> = rows_before
        .iter()
        .copied()
        .filter(|(id, _)| *id != FILE_KEY && *id != INLINE_KEY)
        .collect();
    assert_eq!(head_held, head_before, "the held record is not committed");
    assert_eq!(
        (
            cached_held,
            (rows_held == rows_before, file_held, inline_held.clone())
        ),
        (
            (
                rows_before.clone(),
                vec![(FILE_KEY, value(FILE_KEY, 0))],
                inline_before.clone()
            ),
            (true, vec![(FILE_KEY, value(FILE_KEY, 0))], inline_before)
        ),
        "before its head write, a reader sees the rows the burst deletes"
    );
    assert_eq!(
        (rows_after == expected_after, file_after, inline_after),
        (true, Vec::new(), Vec::new()),
        "once acknowledged, the deleted rows are gone"
    );
    process.table.quiesce().await;
}

/// The same inline CDC burst on a dataset whose metastore is the embedded
/// `SQLite` catalog: the burst's catalog change is committed to the catalog
/// file before its publish makes its rows visible. With every publish held, a
/// second connection to that file already reads the burst's inline rows while
/// a reader does not see them; once the publish runs, the reader does.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_burst_on_the_sqlite_catalog_is_committed_before_it_is_visible() {
    const KEY: i64 = 700;
    const NEW_KEY: i64 = 5_000;
    let dir = tempfile::tempdir().expect("table directory");
    let catalog_url = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::new(cayenne::CayenneCatalog::new(catalog_url.clone()).expect("catalog"));
    catalog.init().await.expect("init");
    let config = VortexConfig {
        inline_max_rows: 1_024,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    };
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .with_context(CayenneContext::new(&config, ctx.runtime_env(), NAME))
            .create(CreateTableOptions {
                table_name: NAME.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: dir.path().join("data").to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: config.clone(),
            })
            .await
            .expect("create the table"),
    );
    ctx.register_table(NAME, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    insert(&table, &ctx, rows_batch(&[KEY], 0)).await;
    let table_id = table.table_id().to_string();
    let inline_rows = |catalog: Arc<dyn MetadataCatalog>| {
        let table_id = table_id.clone();
        async move {
            catalog
                .get_inlined_data(&table_id)
                .await
                .expect("read the inline rows")
                .iter()
                .map(|entry| entry.record_count)
                .sum::<i64>()
        }
    };
    let count = |ctx: SessionContext| async move {
        ctx.sql(&format!("SELECT id, v FROM {NAME} ORDER BY id"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("scan")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
    };
    let rows_before = count(ctx.clone()).await;
    let committed_before = inline_rows(Arc::clone(&catalog)).await;

    let (sink, options) = durable_sink(&table);
    let publications = table.hold_publications().await;
    let no_position: PositionWrite = Box::pin(async { Ok(()) });
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(&sink, options, changes(&[KEY, NEW_KEY], 1), no_position).await;
        drop(sink);
        outcome
    });
    // Poll a second connection until the burst's rows are in the catalog file.
    let second: Arc<dyn MetadataCatalog> =
        Arc::new(cayenne::CayenneCatalog::new(catalog_url).expect("second connection"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let committed_held = loop {
        let committed = inline_rows(Arc::clone(&second)).await;
        if committed != committed_before {
            break committed;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the burst's rows are not in the catalog file within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let rows_held = count(ctx.clone()).await;
    let published_early = burst.is_finished();
    drop(publications);
    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let rows_after = count(ctx.clone()).await;
    eprintln!(
        "sqlite inline burst: before: rows {rows_before}, inline rows in the catalog file {committed_before}; publish held: inline rows in the catalog file {committed_held}, rows a reader sees {rows_held}, burst finished {published_early}; published: rows {rows_after}"
    );
    assert_eq!(
        (committed_held, rows_held, published_early, rows_after),
        (2, rows_before, false, rows_before + 1),
        "the burst's rows are in the catalog file before a reader sees them"
    );
    table.quiesce().await;
}

/// [`cdc_config`], with a write of up to 1,024 rows inlined.
fn inline_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 1_024,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    }
}

/// What readers saw of one multi-group burst, and what the table held after it.
struct MultiGroupOutcome {
    before: Vec<(i64, i64)>,
    expected: Vec<(i64, i64)>,
    /// Held after the record is buffered, before its head write: the rows a
    /// reader sees with what it already holds, then with views rebuilt from the
    /// catalog, then of each key the burst names.
    held_cached: Vec<(i64, i64)>,
    held_rebuilt: Vec<(i64, i64)>,
    held_keys: Vec<(i64, i64)>,
    /// After the head write, before the burst's publish.
    committed_rebuilt: Vec<(i64, i64)>,
    committed_keys: Vec<(i64, i64)>,
    head_held: u64,
    head_before: u64,
    acknowledged: Vec<(i64, i64)>,
    rebuilt: Vec<(i64, i64)>,
    reopened: Vec<(i64, i64)>,
    position_reopened: Option<String>,
}

impl MultiGroupOutcome {
    fn report(&self, label: &str) -> String {
        format!(
            "{label}: held (head {}, before {}): cached rows {} (equal to before: {}), rebuilt rows {} (equal to before: {}), keys {:?}; committed: rebuilt rows {} (equal to before: {}), keys {:?}; acknowledged rows {} (equal to expected: {}), rebuilt {} ({}), reopened {} ({}), position {:?}",
            self.head_held,
            self.head_before,
            self.held_cached.len(),
            self.held_cached == self.before,
            self.held_rebuilt.len(),
            self.held_rebuilt == self.before,
            self.held_keys,
            self.committed_rebuilt.len(),
            self.committed_rebuilt == self.before,
            self.committed_keys,
            self.acknowledged.len(),
            self.acknowledged == self.expected,
            self.rebuilt.len(),
            self.rebuilt == self.expected,
            self.reopened.len(),
            self.reopened == self.expected,
            self.position_reopened,
        )
    }

    /// The rows of the keys `ops` names, from `rows`.
    fn keys_of(rows: &[(i64, i64)], ops: &[(&str, i64)]) -> Vec<(i64, i64)> {
        rows.iter()
            .copied()
            .filter(|(id, _)| ops.iter().any(|(_, key)| key == id))
            .collect()
    }
}

/// Apply `ops` as one CDC burst to a table seeded with the file keys and the
/// inline key 700, opened with `config`, and record what a reader sees while
/// the burst's record is buffered (before its head write), after the head write
/// and before the publish, once acknowledged, and after a reopen.
async fn multi_group_burst(config: &VortexConfig, ops: &[(&str, i64)]) -> MultiGroupOutcome {
    multi_group_burst_with_retention(config, &[], ops).await
}

/// [`multi_group_burst`] on a table opened with `retention` as its retention
/// filters.
async fn multi_group_burst_with_retention(
    config: &VortexConfig,
    retention: &[datafusion::logical_expr::Expr],
    ops: &[(&str, i64)],
) -> MultiGroupOutcome {
    let (location, process) = Location::seeded_with(&[700]).await;
    process.table.quiesce().await;
    drop(process);
    let process = location
        .open_with_retention(config, retention.to_vec())
        .await;
    let before = process.rows().await;
    let (rebuilt_before, _, _) = process.rebuilt_view(700).await;
    assert_eq!(rebuilt_before, before, "a rebuilt view of the seed");
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let head_before = head_commit(&bottom).await;
    let mut expected: BTreeMap<i64, i64> = before.iter().copied().collect();
    for &(op, id) in ops {
        if op == "d" {
            expected.remove(&id);
        } else {
            expected.insert(id, value(id, 1));
        }
    }
    let expected: Vec<(i64, i64)> = expected.into_iter().collect();

    let (sink, options) = durable_sink(&process.table);
    let (held, mut holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let save = save_position(&process.catalog, 1);
    // Polled once the burst's record is in the catalog's buffer: held there.
    let position: PositionWrite = Box::pin(async move {
        let _ = held.send(());
        released
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        save.await
    });
    let batch = mixed_changes(ops, 1);
    let burst = tokio::spawn(async move {
        let outcome = apply_burst(&sink, options, batch, position).await;
        drop(sink);
        outcome
    });
    tokio::time::timeout(Duration::from_secs(60), &mut holding)
        .await
        .expect("the burst reaches its position write within 60 s")
        .expect("the burst reaches its position write");

    let head_held = head_commit(&bottom).await;
    let held_cached = process.rows().await;
    let mut held_keys = Vec::new();
    for &(_, id) in ops {
        if !held_keys.iter().any(|(key, _)| *key == id) {
            held_keys.extend(process.key_rows(id).await);
        }
    }
    let (held_rebuilt, _, _) = process.rebuilt_view(700).await;

    let publications = process.table.hold_publications().await;
    release.send(()).expect("release the burst");
    wait_for_head_past(&bottom, head_before).await;
    let (committed_rebuilt, _, _) = process.rebuilt_view(700).await;
    let committed_keys = MultiGroupOutcome::keys_of(&committed_rebuilt, ops);
    drop(publications);

    burst
        .await
        .expect("the burst does not panic")
        .expect("the burst commits");
    let acknowledged = process.rows().await;
    let (rebuilt, _, _) = process.rebuilt_view(700).await;
    process.table.quiesce().await;
    drop(process);
    let reopened_process = location
        .open_with_retention(config, retention.to_vec())
        .await;
    let reopened = reopened_process.rows().await;
    let position_reopened = reopened_process.saved_position().await;
    reopened_process.table.quiesce().await;
    MultiGroupOutcome {
        before,
        expected,
        held_cached,
        held_rebuilt,
        held_keys,
        committed_rebuilt,
        committed_keys,
        head_held,
        head_before,
        acknowledged,
        rebuilt,
        reopened,
        position_reopened,
    }
}

/// Assert that a reader saw exactly the rows from before the burst until its
/// publish, and exactly the burst's outcome from its acknowledgement on.
fn assert_multi_group_outcome(label: &str, outcome: &MultiGroupOutcome, ops: &[(&str, i64)]) {
    assert_eq!(
        outcome.head_held, outcome.head_before,
        "{label}: the held record is not committed"
    );
    assert_eq!(
        (
            outcome.held_cached == outcome.before,
            outcome.held_rebuilt == outcome.before,
            outcome.held_keys.clone(),
        ),
        (true, true, MultiGroupOutcome::keys_of(&outcome.before, ops)),
        "{label}: before its head write, a reader sees exactly the rows from before the burst: held {:?}",
        MultiGroupOutcome::keys_of(&outcome.held_rebuilt, ops)
    );
    assert_eq!(
        (
            outcome.committed_rebuilt == outcome.before,
            outcome.committed_keys.clone()
        ),
        (true, MultiGroupOutcome::keys_of(&outcome.before, ops)),
        "{label}: after its head write and before its publish, a reader sees the rows from before the burst"
    );
    assert_eq!(
        (
            outcome.acknowledged == outcome.expected,
            outcome.rebuilt == outcome.expected,
            outcome.reopened == outcome.expected,
            outcome.position_reopened.clone(),
        ),
        (true, true, true, Some("lsn 1".to_string())),
        "{label}: the burst's outcome, exactly: acknowledged {:?}, reopened {:?}, expected {:?}",
        MultiGroupOutcome::keys_of(&outcome.acknowledged, ops),
        MultiGroupOutcome::keys_of(&outcome.reopened, ops),
        MultiGroupOutcome::keys_of(&outcome.expected, ops),
    );
}

/// A keyed CDC burst whose changes split into several groups, each written
/// inline: an upsert of an inline key, a delete of that key, and an upsert of a
/// new key. Until the burst's record commits and the burst publishes, a reader
/// (with what it holds, and with views rebuilt from the catalog) sees exactly
/// the rows from before the burst. Once acknowledged, and after a reopen from
/// the committed state, the table holds exactly the burst's outcome.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_group_inline_burst_holds_exactly_its_outcome() {
    let ops = [("c", 700), ("d", 700), ("c", 5_000)];
    let outcome = multi_group_burst(&inline_config(), &ops).await;
    eprintln!("{}", outcome.report("multi-group inline burst"));
    assert_multi_group_outcome("multi-group inline burst", &outcome, &ops);
}

/// Every multi-group burst shape (a key upserted then deleted, deleted then
/// upserted, or upserted in two groups), for an inline key, a file key and a
/// new key, written inline and written to data files: until the burst
/// publishes, a reader sees exactly the rows from before it, and from its
/// acknowledgement on, exactly its outcome, also after a reopen.
#[tokio::test(flavor = "multi_thread")]
async fn every_multi_group_burst_is_invisible_until_its_record_commits() {
    let shapes: [(&str, &[(&str, i64)]); 10] = [
        (
            "upsert, delete inline key, upsert new",
            &[("c", 700), ("d", 700), ("c", 5_000)],
        ),
        ("delete, upsert inline key", &[("d", 700), ("c", 700)]),
        ("upsert, delete file key", &[("c", 7), ("d", 7)]),
        ("delete, upsert file key", &[("d", 7), ("c", 7)]),
        (
            "upsert, delete, upsert new key",
            &[("c", 5_000), ("d", 5_000), ("c", 5_000)],
        ),
        (
            "two upsert groups of an inline key",
            &[("c", 700), ("c", 5_000), ("d", 5_000), ("c", 700)],
        ),
        (
            "two upsert groups of a file key",
            &[("c", 7), ("c", 5_000), ("d", 5_000), ("c", 7)],
        ),
        (
            "two upsert groups of a new key",
            &[("c", 5_000), ("c", 6_000), ("d", 6_000), ("c", 5_000)],
        ),
        (
            "delete, upsert, delete inline key",
            &[("d", 700), ("c", 700), ("d", 700)],
        ),
        (
            "a new key upserted before and after another key's delete",
            &[
                ("c", 5_000),
                ("c", 6_000),
                ("d", 6_000),
                ("c", 6_000),
                ("c", 5_000),
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (path, config) in [("inline", inline_config()), ("data files", cdc_config())] {
        for (shape, ops) in shapes {
            let label = format!("{path}: {shape}");
            let outcome = multi_group_burst(&config, ops).await;
            eprintln!("{}", outcome.report(&label));
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert_multi_group_outcome(&label, &outcome, ops);
            })) {
                failures.push(
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .unwrap_or_else(|| label.clone()),
                );
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} shapes failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A multi-group burst on a table with retention filters, whose staged write
/// clears the key set when its record is buffered: an inline upsert of a new
/// key, a staged upsert of many file keys, and a second inline upsert of the
/// new key. The second upsert must replace the first, though the key set it
/// rebuilds is read while the first is unpublished.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_group_burst_on_a_table_with_retention_filters_holds_exactly_its_outcome() {
    use datafusion::prelude::{col, lit};
    let mut ops = vec![("c", 5_000), ("c", 6_000), ("d", 6_000)];
    ops.extend((10..70).map(|id| ("c", id)));
    ops.extend([("d", 20), ("c", 5_000)]);
    // Matches no row, so the retention pass deletes nothing.
    let retention = [col("v").lt(lit(-1_000_000_i64))];
    let outcome =
        multi_group_burst_with_retention(&staged_then_inline_config(), &retention, &ops).await;
    let label = "retention: inline, staged, inline";
    eprintln!("{}", outcome.report(label));
    assert_multi_group_outcome(label, &outcome, &ops);
}

// ---------------------------------------------------------------------------
// A burst written to data files followed by an inline burst, pipelined
// ---------------------------------------------------------------------------

/// [`cdc_config`], with a write of up to 50 rows inlined: a burst of every file
/// key goes to a data file, a burst of two keys inline.
fn staged_then_inline_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 50,
        inline_max_bytes: 1 << 20,
        ..cdc_config()
    }
}

/// The inline burst: an upsert of the seeded inline key and of a new key. Only
/// a burst of upserts prepares while the burst before it is still committing
/// (the change sink settles that burst first for one that deletes).
const STAGED_THEN_INLINE_SECOND: [(&str, i64); 2] = [("c", 700), ("c", 5_000)];

/// The rows after the staged burst (every file key) and then the inline burst,
/// up to `round`, over the seed of [`Location::seeded_with`] `[700]`.
fn staged_then_inline_expected(round: i64) -> Vec<(i64, i64)> {
    let mut rows: BTreeMap<i64, i64> = FILE_KEYS
        .chain([700])
        .map(|id| (id, value(id, 0)))
        .collect();
    if round >= 1 {
        for id in FILE_KEYS {
            rows.insert(id, value(id, 1));
        }
    }
    if round >= 2 {
        for (op, id) in STAGED_THEN_INLINE_SECOND {
            if op == "d" {
                rows.remove(&id);
            } else {
                rows.insert(id, value(id, 2));
            }
        }
    }
    rows.into_iter().collect()
}

/// Gates of [`run_staged_then_inline_pair`] a test holds to look at the table.
#[derive(Default)]
struct PairGates {
    /// Signalled when the inline burst's position write is polled: its record
    /// is buffered and its head write not sent.
    second_held: Option<tokio::sync::oneshot::Sender<()>>,
    /// Released by the test to let the inline burst's position write go on.
    second_release: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Whether the catalog's readable state holds an inline entry of `rows` rows,
/// polled for up to a second. The catalog applies a buffered change to it at
/// once, so this sees burst 2's inline write as soon as it is buffered; a
/// write that waits for burst 1's commit never appears here first, which the
/// bounded poll lets burst 1 go on without.
async fn inline_entry_buffered(catalog: &ObjectNativeCatalog, table_id: &str, rows: i64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let buffered = catalog
            .get_inlined_data(table_id)
            .await
            .is_ok_and(|entries| entries.iter().any(|entry| entry.record_count == rows));
        if buffered || tokio::time::Instant::now() >= deadline {
            return buffered;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Submit a burst of every file key (written to a data file), then an inline
/// burst, through one change sink, pipelined:
/// the first burst's finalizer waits (in its source position write) for the
/// second burst's inline write to be buffered, up to a second, so the second
/// burst prepares while the first burst's record is still open. Then both
/// publish.
async fn run_staged_then_inline_pair(
    table: Arc<CayenneTableProvider>,
    catalog: Arc<ObjectNativeCatalog>,
    acknowledged: Arc<Acknowledged>,
    gates: PairGates,
) -> std::result::Result<(), String> {
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare(NAME),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, SessionContext::new(), &Handle::current(), 2);
    let options = WriteOptions {
        recovery: Recovery::Durable,
        ..WriteOptions::default()
    };

    let (first_finalized, first_waiting) = tokio::sync::oneshot::channel::<()>();
    let save_first = save_position(&catalog, 1);
    let watched = Arc::clone(&catalog);
    let table_id = table.table_id().to_string();
    let first_position: PositionWrite = Box::pin(async move {
        let _ = first_finalized.send(());
        let seen = inline_entry_buffered(&watched, &table_id, 2).await;
        eprintln!("burst 1's position write: burst 2's inline write buffered before it: {seen}");
        save_first.await
    });
    let first = sink
        .reserve()
        .await
        .map_err(|error| error.to_string())?
        .submit_with_position(
            changes(&FILE_KEYS.collect::<Vec<_>>(), 1),
            options,
            Some(first_position),
        )
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| format!("burst 1: {error}"))?;
    first_waiting
        .await
        .map_err(|_| "burst 1's finalizer stopped before its position write".to_string())?;
    let save_second = save_position(&catalog, 2);
    let PairGates {
        second_held,
        second_release,
    } = gates;
    let second_position: PositionWrite = Box::pin(async move {
        if let Some(held) = second_held {
            let _ = held.send(());
        }
        if let Some(release) = second_release {
            release
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        save_second.await
    });
    let second = sink
        .reserve()
        .await
        .map_err(|error| error.to_string())?
        .submit_with_position(
            mixed_changes(&STAGED_THEN_INLINE_SECOND, 2),
            options,
            Some(second_position),
        )
        .map_err(|error| error.to_string())?
        .wait()
        .await
        .map_err(|error| format!("burst 2: {error}"))?;
    first
        .published()
        .await
        .map_err(|error| format!("burst 1 publish: {error}"))?;
    acknowledged.first.store(true, Ordering::SeqCst);
    second
        .published()
        .await
        .map_err(|error| format!("burst 2 publish: {error}"))?;
    acknowledged.second.store(true, Ordering::SeqCst);
    Ok(())
}

/// A burst written to a data file (burst 1), then an inline burst (burst 2)
/// that prepares while burst 1's record is still open, so burst 2's inline
/// write can be buffered before burst 1's record commits, and that record then
/// carries it. Burst 2's rows still commit whole, in one record that is not
/// before burst 1's; until burst 2's own record (its position) commits, a
/// reader sees the rows after burst 1 and none of burst 2; the saved position
/// never runs ahead of the rows; and a kill at any storage request leaves the
/// rows from before both bursts, after burst 1, or after both.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_burst_buffered_while_a_staged_burst_commits_is_all_or_nothing() {
    let config = staged_then_inline_config();
    let seeded = || async {
        let (location, process) = Location::seeded_with(&[700]).await;
        process.table.quiesce().await;
        drop(process);
        let process = location.open_with(&config).await;
        (location, process)
    };

    // A clean run, held before burst 2's head write.
    let (location, process) = seeded().await;
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let before = head_commit(&bottom).await;
    process.instance.clear_steps();
    let acknowledged = Arc::new(Acknowledged::default());
    let (held, holding) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(run_staged_then_inline_pair(
        Arc::clone(&process.table),
        Arc::clone(&process.catalog),
        Arc::clone(&acknowledged),
        PairGates {
            second_held: Some(held),
            second_release: Some(released),
        },
    ));
    tokio::time::timeout(Duration::from_secs(60), holding)
        .await
        .expect("burst 2 reaches its position write within 60 s")
        .expect("burst 2 reaches its position write");
    let held_records = records_after(&bottom, before).await;
    let held_position = process.saved_position().await;
    let held_cached = process.rows().await;
    let (held_rebuilt, _, _) = process.rebuilt_view(700).await;
    release.send(()).expect("release burst 2");
    assert_eq!(
        task.await.expect("the pair does not panic"),
        Ok(()),
        "the pair commits"
    );
    let steps = process.instance.steps();
    let acknowledged_rows = process.rows().await;
    wait_until_idle(&process).await;
    process.table.quiesce().await;
    drop(process);
    let records = records_after(&bottom, before).await;
    eprintln!("clean run: {} storage requests", steps.len());
    eprintln!("clean run records: {records:?}");
    eprintln!(
        "held before burst 2's head write: records {held_records:?}, position {held_position:?}, cached rows equal after burst 1 {} (after burst 2 {}), rebuilt equal after burst 1 {}; keys {:?}",
        held_cached == staged_then_inline_expected(1),
        held_cached == staged_then_inline_expected(2),
        held_rebuilt == staged_then_inline_expected(1),
        MultiGroupOutcome::keys_of(&held_rebuilt, &STAGED_THEN_INLINE_SECOND),
    );
    let first_record = records
        .iter()
        .position(|record| {
            record
                .iter()
                .any(|action| action == "AdvanceSourcePosition(lsn 1)")
        })
        .expect("a record saves burst 1's position");
    let second_record = records
        .iter()
        .position(|record| {
            record
                .iter()
                .any(|action| action == "AdvanceSourcePosition(lsn 2)")
        })
        .expect("a record saves burst 2's position");
    let mut failures = Vec::new();
    // Burst 2's inline rows commit in one record: burst 1's, when they were
    // buffered before it committed, or burst 2's own.
    let inline_records: Vec<usize> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| count(record, "AddInlinedData") > 0)
        .map(|(index, _)| index)
        .collect();
    eprintln!(
        "burst 2's inline rows commit in record(s) {inline_records:?}; burst 1's position in {first_record}, burst 2's in {second_record}"
    );
    if !(first_record < second_record
        && inline_records.len() == 1
        && (inline_records[0] == first_record || inline_records[0] == second_record))
    {
        failures.push(format!(
            "burst 2's inline rows must commit whole in burst 1's record or its own, after burst 1's position: records {records:?}"
        ));
    }
    let held = (
        held_position.as_deref(),
        held_cached == staged_then_inline_expected(1),
        held_rebuilt == staged_then_inline_expected(1),
        acknowledged_rows == staged_then_inline_expected(2),
    );
    if held != (Some("lsn 1"), true, true, true) {
        failures.push(format!(
            "before burst 2's head write a reader must see the rows after burst 1 with its position, and once acknowledged, after burst 2: (position, cached, rebuilt, acknowledged) {held:?}"
        ));
    }
    let reopened = location.open_with(&config).await;
    let clean = (
        reopened.rows().await == staged_then_inline_expected(2),
        reopened.saved_position().await,
    );
    if clean != (true, Some("lsn 2".to_string())) {
        failures.push(format!(
            "the clean run's rows and position after a reopen: {clean:?}"
        ));
    }
    reopened.table.quiesce().await;
    drop(reopened);

    // A kill before each storage request of the pair.
    let mut outcomes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for kill_before in 0..steps.len() {
        let (location, process) = seeded().await;
        process.instance.clear_steps();
        process.instance.die_after(kill_before);
        let acknowledged = Arc::new(Acknowledged::default());
        let task = tokio::spawn(run_staged_then_inline_pair(
            Arc::clone(&process.table),
            Arc::clone(&process.catalog),
            Arc::clone(&acknowledged),
            PairGates::default(),
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let outcome = loop {
            if task.is_finished() {
                break Some(task.await.expect("the pair does not panic"));
            }
            if process.instance.is_dead() {
                task.abort();
                break None;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the pair neither finished nor was killed within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        };
        process.instance.kill();
        process.table.quiesce().await;
        drop(process);
        let next = location.open_with(&config).await;
        let rows = next.rows().await;
        let position = next.saved_position().await;
        let reached = (0..=2).find(|round| rows == staged_then_inline_expected(*round));
        let state = reached.map_or_else(|| "neither".to_string(), |round| format!("after {round}"));
        outcomes
            .entry(format!("{state}, position {position:?}"))
            .or_default()
            .push(kill_before);
        let floor = match position.as_deref() {
            None => 0,
            Some("lsn 1") => 1,
            Some("lsn 2") => 2,
            Some(_) => 3,
        };
        let mut problems = Vec::new();
        if reached.is_none() {
            problems.push(format!(
                "the rows are from neither before, after burst 1, nor after burst 2: keys {:?}",
                MultiGroupOutcome::keys_of(&rows, &[("c", 0), ("c", 7), ("c", 700), ("c", 5_000)])
            ));
        }
        if reached.is_some_and(|reached| reached < floor) {
            problems.push(format!(
                "the saved position {position:?} runs ahead of the rows ({state})"
            ));
        }
        if acknowledged.second.load(Ordering::SeqCst) && reached != Some(2) {
            problems.push(format!(
                "burst 2 was acknowledged, but the rows are {state}"
            ));
        } else if acknowledged.first.load(Ordering::SeqCst) && reached.is_none_or(|r| r < 1) {
            problems.push(format!(
                "burst 1 was acknowledged, but the rows are {state}"
            ));
        }
        next.table.quiesce().await;
        if !problems.is_empty() {
            failures.push(format!(
                "killed before request {kill_before} ({}), outcome {outcome:?}: {}",
                steps
                    .get(kill_before)
                    .map_or("past the end", String::as_str),
                problems.join("; ")
            ));
        }
    }
    eprintln!(
        "{} kill points; outcomes by kill point: {outcomes:?}",
        steps.len()
    );
    assert!(
        failures.is_empty(),
        "{} problems ({} kill points):\n{}",
        failures.len(),
        steps.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// `cdc_durability: memory`: bursts kept in the in-memory tier, checkpoints
// inside a burst, and what the source is told
// ---------------------------------------------------------------------------

/// Require nextest's process isolation before changing the global CDC tier budget.
fn require_process_per_test(why: &str) {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "{why}, so it must run in a process of its own. Run it with \
         `cargo nextest run -p accelerator-cayenne --lib <test name>`: plain \
         `cargo test` runs tests on threads of one process"
    );
}

/// Bytes the in-memory tier budget admits: an upsert of a few rows fits, a
/// delete that names [`OVERFLOW_KEYS`] keys (16 bytes a key) does not, so it
/// takes the durable path.
const BUDGET_BYTES: u64 = 8_192;
/// Keys a delete names besides the ones it deletes, none of them stored.
const OVERFLOW_KEYS: i64 = 1_000;

/// [`cdc_config`] with `cdc_durability: memory`: CDC rows wait in the
/// in-memory tier until a checkpoint, and only the runtime's checkpoint (the
/// change sink's flush) runs on its own.
fn memory_config() -> VortexConfig {
    VortexConfig {
        cdc_durability: CdcDurability::Memory,
        cdc_mem_tier_checkpoint_interval_ms: 0,
        ..cdc_config()
    }
}

/// A delete of `id` that also names [`OVERFLOW_KEYS`] absent keys from
/// `absent`, so that it overflows the in-memory tier budget.
fn overflowing_delete(id: i64, absent: i64) -> Vec<(&'static str, i64)> {
    std::iter::once(("d", id))
        .chain((absent..absent + OVERFLOW_KEYS).map(|key| ("d", key)))
        .collect()
}

/// The source, as the runtime drives it for a dataset whose writes wait in the
/// in-memory tier: a burst kept there is acknowledged only once a checkpoint
/// reports its fence durable, after the checkpoint has recorded the newest
/// position at or below that fence; a burst written durably is acknowledged
/// after the runtime's flush, with its position saved.
struct SourceModel {
    catalog: Arc<ObjectNativeCatalog>,
    /// `(fence, round)` of every burst not yet acknowledged, oldest first.
    queued: parking_lot::Mutex<Vec<(u64, i64)>>,
    durable_fence: parking_lot::Mutex<Option<u64>>,
    /// The newest round the source was told it need not send again.
    acknowledged: AtomicI64,
}

impl SourceModel {
    fn new(catalog: &Arc<ObjectNativeCatalog>) -> Arc<Self> {
        Arc::new(Self {
            catalog: Arc::clone(catalog),
            queued: parking_lot::Mutex::new(Vec::new()),
            durable_fence: parking_lot::Mutex::new(None),
            acknowledged: AtomicI64::new(0),
        })
    }

    /// Queue `round`, kept in the tier until `fence` is durable. A checkpoint
    /// may have reported that fence before the round was queued.
    async fn enqueue(&self, fence: u64, round: i64) {
        self.queued.lock().push((fence, round));
        let known = *self.durable_fence.lock();
        if let Some(known) = known {
            self.on_durable(known).await;
        }
    }

    fn acknowledge(&self, round: i64) {
        self.acknowledged.fetch_max(round, Ordering::SeqCst);
    }
}

#[async_trait]
impl DurabilityObserver for SourceModel {
    async fn on_durable(&self, fence: u64) {
        {
            let mut known = self.durable_fence.lock();
            *known = Some(known.map_or(fence, |known| known.max(fence)));
        }
        let ready: Vec<i64> = {
            let mut queued = self.queued.lock();
            let split = queued.partition_point(|(queued_fence, _)| *queued_fence <= fence);
            queued.drain(..split).map(|(_, round)| round).collect()
        };
        if let Some(&round) = ready.last() {
            self.acknowledge(round);
        }
    }

    async fn record_position(
        &self,
        fence: u64,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let round = self
            .queued
            .lock()
            .iter()
            .take_while(|(queued_fence, _)| *queued_fence <= fence)
            .map(|(_, round)| *round)
            .last();
        match round {
            Some(round) => save_position(&self.catalog, round).await,
            None => Ok(()),
        }
    }
}

/// Submit each `(round, ops)` burst through one change sink, two in flight at
/// a time as the runtime's ingress pipelines them, and tell the source what
/// the runtime would; then flush, as the runtime's checkpoint does.
async fn run_memory_bursts(
    table: Arc<CayenneTableProvider>,
    catalog: Arc<ObjectNativeCatalog>,
    source: Arc<SourceModel>,
    rounds: Vec<(i64, Vec<(&'static str, i64)>)>,
) -> std::result::Result<(), String> {
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare(NAME),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, SessionContext::new(), &Handle::current(), 2);
    sink.set_durability_observer(Arc::clone(&source) as Arc<dyn DurabilityObserver>);
    let options = WriteOptions {
        recovery: Recovery::Replayable,
        ..WriteOptions::default()
    };
    let settle = |round: i64, mut submission: runtime_acceleration::change_sink::Submission| {
        let sink = &sink;
        let catalog = &catalog;
        let source = &source;
        async move {
            let receipt = submission
                .wait()
                .await
                .map_err(|error| format!("burst {round}: {error}"))?;
            receipt
                .published()
                .await
                .map_err(|error| format!("burst {round} publish: {error}"))?;
            eprintln!("burst {round}: {:?}", receipt.durability);
            match receipt.durability {
                StorageDurability::Deferred(fence) => source.enqueue(fence, round).await,
                StorageDurability::Durable => {
                    sink.flush()
                        .await
                        .map_err(|error| format!("flush after burst {round}: {error}"))?;
                    save_position(catalog, round)
                        .await
                        .map_err(|error| format!("burst {round}'s position: {error}"))?;
                    source.acknowledge(round);
                }
                StorageDurability::NotPromised => {
                    return Err(format!("burst {round} promised no durability"));
                }
            }
            Ok::<(), String>(())
        }
    };
    let mut in_flight = std::collections::VecDeque::new();
    for (round, ops) in rounds {
        let submission = sink
            .reserve()
            .await
            .map_err(|error| error.to_string())?
            .submit_with_position(
                mixed_changes(&ops, round),
                options,
                Some(save_position(&catalog, round)),
            )
            .map_err(|error| error.to_string())?;
        in_flight.push_back((round, submission));
        if in_flight.len() == 2
            && let Some((round, submission)) = in_flight.pop_front()
        {
            settle(round, submission).await?;
        }
    }
    while let Some((round, submission)) = in_flight.pop_front() {
        settle(round, submission).await?;
    }
    sink.flush()
        .await
        .map_err(|error| format!("final flush: {error}"))?;
    sink.begin_close()
        .wait()
        .await
        .map_err(|error| format!("close: {error}"))
}

/// `before` with each burst of `rounds` applied in order.
fn rows_after(
    before: &[(i64, i64)],
    rounds: &[(i64, Vec<(&'static str, i64)>)],
) -> Vec<(i64, i64)> {
    let mut rows: BTreeMap<i64, i64> = before.iter().copied().collect();
    for (round, ops) in rounds {
        for &(op, id) in ops {
            if op == "d" {
                rows.remove(&id);
            } else {
                rows.insert(id, value(id, *round));
            }
        }
    }
    rows.into_iter().collect()
}

/// The bursts of the memory-mode tests. An upsert group's rows are not
/// adjacent in its burst, so the group is copied out of the burst's rows and
/// is charged its own few bytes against the budget, while each delete group
/// overflows it.
/// 1. upserts, kept in the tier;
/// 2. upserts kept in the tier, then a delete that overflows the budget: its
///    spill checkpoints the tier inside the burst, recording burst 1's
///    position, and the delete is written durably;
/// 3. a delete that overflows the budget (written durably), an upsert of the
///    same key and of a new key (kept in the tier), and a second overflowing
///    delete of that key, whose spill checkpoints the tier after the burst's
///    first delete;
/// 4. the same, with the second overflowing delete naming other keys, so the
///    re-inserted key the checkpoint makes durable survives the first delete;
/// 5. an upsert, kept in the tier.
fn memory_rounds() -> Vec<(i64, Vec<(&'static str, i64)>)> {
    let mut second = vec![("c", 202), ("d", 9_002), ("c", 203)];
    second.extend(overflowing_delete(7, 10_000));
    let mut third = overflowing_delete(5, 11_000);
    third.extend([("c", 5), ("d", 9_003), ("c", 300)]);
    third.extend(overflowing_delete(5, 12_000));
    let mut fourth = overflowing_delete(6, 13_000);
    fourth.extend([("c", 6), ("d", 9_004), ("c", 301)]);
    fourth.extend(overflowing_delete(9_005, 14_000));
    vec![
        (1, vec![("c", 200), ("c", 201)]),
        (2, second),
        (3, third),
        (4, fourth),
        (5, vec![("c", 204)]),
    ]
}

/// The saved position as a round: `None` is 0.
fn saved_round(position: Option<&str>) -> std::result::Result<i64, String> {
    match position {
        None => Ok(0),
        Some(position) => position
            .strip_prefix("lsn ")
            .and_then(|round| round.parse().ok())
            .ok_or_else(|| format!("an unknown saved position {position:?}")),
    }
}

/// `cdc_durability: memory` bursts whose deletes overflow the in-memory tier
/// budget, so their spills checkpoint the tier between a burst's groups, two of
/// them after an earlier delete of the same burst was written durably. Every
/// burst commits, and the table, after the runtime's checkpoint and after
/// a reopen, holds exactly the bursts' outcome, with the last burst's position
/// saved and acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn memory_mode_bursts_whose_deletes_overflow_the_tier_budget_commit_exactly() {
    require_process_per_test("this test installs the process-global in-memory CDC tier budget");
    let (location, process) = memory_location().await;
    assert!(
        process.table.is_cdc_memory_mode(),
        "the table keeps CDC rows in the in-memory tier"
    );
    let before = process.rows().await;
    let rounds = memory_rounds();
    let expected = rows_after(&before, &rounds);
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let head_before = head_commit(&bottom).await;
    cayenne::set_global_mem_tier_bytes(BUDGET_BYTES);
    let source = SourceModel::new(&process.catalog);
    let outcome = run_memory_bursts(
        Arc::clone(&process.table),
        Arc::clone(&process.catalog),
        Arc::clone(&source),
        rounds,
    )
    .await;
    cayenne::set_global_mem_tier_bytes(0);
    let rows = process.try_rows().await;
    let acknowledged = source.acknowledged.load(Ordering::SeqCst);
    process.table.quiesce().await;
    drop(process);
    let records = records_after(&bottom, head_before).await;
    let reopened = location.open_with(&memory_config()).await;
    let reopened_rows = reopened.rows().await;
    let position = reopened.saved_position().await;
    reopened.table.quiesce().await;
    eprintln!("memory-mode bursts: records {records:?}");
    eprintln!(
        "memory-mode bursts: outcome {outcome:?}, rows {}, reopened equal expected: {}, position {position:?}, acknowledged round {acknowledged}",
        match &rows {
            Ok(rows) => format!("equal expected: {}", *rows == expected),
            Err(error) => format!("refused: {error}"),
        },
        reopened_rows == expected
    );
    // Non-vacuous: the five overflowing deletes were written durably (each
    // record of a durable delete adds a deletion vector).
    let durable_deletes = records
        .iter()
        .map(|record| count(record, "AddDeleteFiles"))
        .sum::<usize>();
    assert_eq!(outcome, Ok(()), "every burst commits");
    assert_eq!(
        (
            rows.as_ref().ok() == Some(&expected),
            reopened_rows == expected,
            position.as_deref(),
            acknowledged
        ),
        (true, true, Some("lsn 5"), 5),
        "the bursts' exact outcome, before and after a reopen, with the last position saved and acknowledged"
    );
    assert!(
        durable_deletes >= 5,
        "the five overflowing deletes are written durably: {records:?}"
    );
    // Each checkpoint, a spill's included, commits as one record: the files of
    // its snapshot with the snapshot's sequence.
    let checkpoints: Vec<&Vec<String>> = records
        .iter()
        .filter(|record| record.iter().any(|action| action == "AddFiles"))
        .collect();
    assert!(
        checkpoints.len() >= 3
            && checkpoints
                .iter()
                .all(|record| record.iter().any(|action| action == "SetGroup")),
        "each checkpoint commits its files and its sequence in one record: {records:?}"
    );
}

/// A process with the memory-mode table: seeded with the inline and file keys,
/// then opened by a new process.
async fn memory_location() -> (Location, Process) {
    let (location, process) = Location::seeded_as(&INLINE_KEYS, &memory_config()).await;
    process.table.quiesce().await;
    drop(process);
    let process = location.open_with(&memory_config()).await;
    (location, process)
}

/// Run `rounds` in `process`, with the budget installed, until they return or
/// its store refuses a request, as a killed process stops there. `None` when
/// it was killed first.
async fn run_memory_until_killed(
    process: &Process,
    source: &Arc<SourceModel>,
    rounds: Vec<(i64, Vec<(&'static str, i64)>)>,
) -> Option<std::result::Result<(), String>> {
    cayenne::set_global_mem_tier_bytes(BUDGET_BYTES);
    let task = tokio::spawn(run_memory_bursts(
        Arc::clone(&process.table),
        Arc::clone(&process.catalog),
        Arc::clone(source),
        rounds,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let outcome = loop {
        if task.is_finished() {
            break Some(task.await.expect("the bursts do not panic"));
        }
        if process.instance.is_dead() {
            task.abort();
            break None;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the bursts neither finished nor were killed within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    };
    cayenne::set_global_mem_tier_bytes(0);
    outcome
}

/// The `cdc_durability: memory` bursts of [`memory_rounds`] through the change
/// sink, killed before each storage write in turn (reads, whose order varies
/// between runs, pass until then), with the source told what the runtime tells
/// it. Overflowing deletes' spills checkpoint the in-memory tier between a
/// burst's groups, which records an earlier burst's position and acknowledges
/// the source. After each kill, a new process
/// resumes from the newer of the saved position and the source's acknowledged
/// one (the source sends nothing it was acknowledged for) and replays the rest:
/// its rows must then equal every burst's outcome, so neither position ever ran
/// ahead of the durable rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_request_of_memory_mode_bursts_loses_no_acknowledged_change() {
    require_process_per_test("this test installs the process-global in-memory CDC tier budget");
    let rounds = memory_rounds();
    let (_location, process) = memory_location().await;
    let before = process.rows().await;
    let expected = rows_after(&before, &rounds);
    process.instance.clear_steps();
    let source = SourceModel::new(&process.catalog);
    let clean = run_memory_until_killed(&process, &source, rounds.clone()).await;
    let steps: Vec<String> = process
        .instance
        .steps()
        .into_iter()
        .filter(|step| !is_read(step.split(' ').next().unwrap_or_default()))
        .collect();
    let clean_rows = process.try_rows().await;
    process.table.quiesce().await;
    drop(process);
    eprintln!(
        "clean run: {} storage writes, outcome {clean:?}, acknowledged round {}",
        steps.len(),
        source.acknowledged.load(Ordering::SeqCst)
    );
    for (index, step) in steps.iter().enumerate() {
        eprintln!("  {index:3} {step}");
    }
    assert_eq!(clean, Some(Ok(())), "the clean run commits every burst");
    assert!(
        clean_rows.as_ref().ok() == Some(&expected),
        "the clean run's rows: {clean_rows:?}"
    );

    let states: Vec<(String, Vec<(i64, i64)>)> = (0..=rounds.len())
        .map(|applied| {
            (
                format!("after burst {applied}"),
                rows_after(&before, &rounds[..applied]),
            )
        })
        .collect();
    let mut outcomes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut failures = Vec::new();
    for kill_before in 0..=steps.len() {
        let (location, process) = memory_location().await;
        process.instance.clear_steps();
        process.instance.die_after_writes(kill_before);
        let source = SourceModel::new(&process.catalog);
        let outcome = run_memory_until_killed(&process, &source, rounds.clone()).await;
        process.instance.kill();
        let sent: Vec<String> = process
            .instance
            .steps()
            .into_iter()
            .filter(|step| !is_read(step.split(' ').next().unwrap_or_default()))
            .collect();
        eprintln!(
            "kill before write {kill_before}: {} writes sent, head writes at {:?}, outcome {outcome:?}",
            sent.len(),
            sent.iter()
                .enumerate()
                .filter(|(_, step)| is_head_write(step))
                .map(|(index, _)| index)
                .collect::<Vec<_>>()
        );
        process.table.quiesce().await;
        drop(process);
        let acknowledged = source.acknowledged.load(Ordering::SeqCst);

        let next = location.open_with(&memory_config()).await;
        let reopened = next.try_rows().await;
        let position = next.saved_position().await;
        let mut problems = Vec::new();
        let saved = saved_round(position.as_deref()).unwrap_or_else(|error| {
            problems.push(error);
            0
        });
        let state = match &reopened {
            Ok(rows) => states.iter().find(|(_, state)| state == rows).map_or_else(
                || {
                    // The burst prefix it is closest to, and how it differs.
                    let (name, nearest) = states
                        .iter()
                        .min_by_key(|(_, state)| {
                            state.iter().filter(|row| !rows.contains(row)).count()
                                + rows.iter().filter(|row| !state.contains(row)).count()
                        })
                        .expect("states");
                    let missing: Vec<_> =
                        nearest.iter().filter(|row| !rows.contains(row)).collect();
                    let extra: Vec<_> = rows.iter().filter(|row| !nearest.contains(row)).collect();
                    format!("between bursts ({name}, without {missing:?}, with {extra:?})")
                },
                |(name, _)| name.clone(),
            ),
            Err(error) => format!("refused: {error}"),
        };
        let resume = saved.max(acknowledged);
        let replay: Vec<_> = rounds
            .iter()
            .filter(|(round, _)| *round > resume)
            .cloned()
            .collect();
        let replayed = run_memory_bursts(
            Arc::clone(&next.table),
            Arc::clone(&next.catalog),
            SourceModel::new(&next.catalog),
            replay,
        )
        .await;
        let rows = next.try_rows().await;
        next.table.quiesce().await;
        outcomes
            .entry(format!(
                "{state}, saved {saved}, acknowledged {acknowledged}"
            ))
            .or_default()
            .push(kill_before);
        if let Err(error) = &replayed {
            problems.push(format!("the replay failed: {error}"));
        }
        match rows {
            Ok(rows) if rows == expected => {}
            Ok(rows) => {
                let missing: Vec<_> = expected
                    .iter()
                    .filter(|row| !rows.contains(row))
                    .take(5)
                    .collect();
                let extra: Vec<_> = rows
                    .iter()
                    .filter(|row| !expected.contains(row))
                    .take(5)
                    .collect();
                problems.push(format!(
                    "after the replay from burst {} the rows differ from every burst's outcome: missing {missing:?}, extra {extra:?}",
                    resume + 1
                ));
            }
            Err(error) => problems.push(format!("the replayed table refuses queries: {error}")),
        }
        if !problems.is_empty() {
            failures.push(format!(
                "killed before write {kill_before} ({}), outcome {outcome:?}, reopened {state}, saved {saved}, acknowledged {acknowledged}: {}",
                steps
                    .get(kill_before)
                    .map_or("past the end", String::as_str),
                problems.join("; ")
            ));
        }
    }
    eprintln!(
        "{} kill points; outcomes by kill point: {outcomes:?}",
        steps.len() + 1
    );
    assert!(
        failures.is_empty(),
        "{} problems ({} kill points):\n{}",
        failures.len(),
        steps.len() + 1,
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// `cdc_durability: memory`: the periodic in-memory tier checkpoint
// ---------------------------------------------------------------------------

/// [`memory_config`] where any non-empty in-memory tier is worth a periodic
/// checkpoint well before its age deadline, so the periodic tick takes its
/// best-effort path, and no seal advances the source in its place.
fn tick_config() -> VortexConfig {
    VortexConfig {
        cdc_mem_tier_min_flush_bytes: 1,
        cdc_mem_tier_max_age_ms: 3_600_000,
        cdc_mem_tier_seal_age_ms: 0,
        ..memory_config()
    }
}

/// The bursts of the periodic-checkpoint tests, each kept in the in-memory
/// tier and checkpointed by the tick that follows it: upserts of new keys, of
/// a file key and of an inline key; deletes of a file key and of a key a
/// checkpoint made durable; a re-insert of a deleted key.
fn tick_rounds() -> Vec<(i64, Vec<(&'static str, i64)>)> {
    vec![
        (1, vec![("c", 200), ("c", INLINE_KEYS[0]), ("c", 5)]),
        (
            2,
            vec![("d", 7), ("c", 201), ("c", INLINE_KEYS[1]), ("d", 200)],
        ),
        (3, vec![("c", 7), ("c", 202), ("c", 5)]),
        (4, vec![("c", 203), ("d", 201)]),
    ]
}

/// What one periodic tick wrote: the round it followed, the dataset head
/// before and after it, and the indices of the storage writes it sent.
#[derive(Debug, Clone)]
struct TickSpan {
    round: i64,
    head: (u64, u64),
    writes: Range<usize>,
}

fn writes_sent(instance: &Instance) -> usize {
    instance
        .steps()
        .iter()
        .filter(|step| !is_read(step.split(' ').next().unwrap_or_default()))
        .count()
}

/// Submit each `(round, ops)` burst through one change sink, one at a time,
/// tell the source what the runtime would, and run the periodic in-memory tier
/// checkpoint tick after each, noting what it wrote in `spans`; then flush, as
/// the runtime's checkpoint does.
async fn run_ticked_bursts(
    table: Arc<CayenneTableProvider>,
    catalog: Arc<ObjectNativeCatalog>,
    instance: Arc<Instance>,
    bottom: Arc<dyn ObjectStore>,
    source: Arc<SourceModel>,
    rounds: Vec<(i64, Vec<(&'static str, i64)>)>,
    spans: Arc<parking_lot::Mutex<Vec<TickSpan>>>,
) -> std::result::Result<(), String> {
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare(NAME),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, SessionContext::new(), &Handle::current(), 2);
    sink.set_durability_observer(Arc::clone(&source) as Arc<dyn DurabilityObserver>);
    let options = WriteOptions {
        recovery: Recovery::Replayable,
        ..WriteOptions::default()
    };
    for (round, ops) in rounds {
        let receipt = sink
            .reserve()
            .await
            .map_err(|error| error.to_string())?
            .submit_with_position(
                mixed_changes(&ops, round),
                options,
                Some(save_position(&catalog, round)),
            )
            .map_err(|error| error.to_string())?
            .wait()
            .await
            .map_err(|error| format!("burst {round}: {error}"))?;
        receipt
            .published()
            .await
            .map_err(|error| format!("burst {round} publish: {error}"))?;
        match receipt.durability {
            StorageDurability::Deferred(fence) => source.enqueue(fence, round).await,
            other => return Err(format!("burst {round} was not kept in the tier: {other:?}")),
        }
        let head_before = head_commit(&bottom).await;
        let writes_before = writes_sent(&instance);
        table.run_mem_tier_checkpoint_tick_now().await;
        let span = TickSpan {
            round,
            head: (head_before, head_commit(&bottom).await),
            writes: writes_before..writes_sent(&instance),
        };
        spans.lock().push(span);
    }
    sink.flush()
        .await
        .map_err(|error| format!("final flush: {error}"))?;
    sink.begin_close()
        .wait()
        .await
        .map_err(|error| format!("close: {error}"))
}

/// A process with the periodic-checkpoint table: seeded with the inline and
/// file keys, then opened by a new process.
async fn tick_location() -> (Location, Process) {
    let (location, process) = Location::seeded_as(&INLINE_KEYS, &tick_config()).await;
    process.table.quiesce().await;
    drop(process);
    let process = location.open_with(&tick_config()).await;
    (location, process)
}

/// Run [`run_ticked_bursts`] in `process` until it returns or its store
/// refuses a request, as a killed process stops there. `None` when it was
/// killed first.
async fn run_ticked_until_killed(
    process: &Process,
    bottom: &Arc<dyn ObjectStore>,
    source: &Arc<SourceModel>,
    rounds: Vec<(i64, Vec<(&'static str, i64)>)>,
    spans: &Arc<parking_lot::Mutex<Vec<TickSpan>>>,
) -> Option<std::result::Result<(), String>> {
    let task = tokio::spawn(run_ticked_bursts(
        Arc::clone(&process.table),
        Arc::clone(&process.catalog),
        Arc::clone(&process.instance),
        Arc::clone(bottom),
        Arc::clone(source),
        rounds,
        Arc::clone(spans),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if task.is_finished() {
            return Some(task.await.expect("the bursts do not panic"));
        }
        if process.instance.is_dead() {
            task.abort();
            return None;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the bursts neither finished nor were killed within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// `cdc_durability: memory` bursts, each kept in the in-memory tier and then
/// checkpointed by the periodic tick (its best-effort, size-triggered path).
/// Each tick commits its checkpoint as one record: the snapshot's files, its
/// sequence, and the source position it covers. The table holds exactly the
/// bursts' outcome, before and after a reopen.
#[tokio::test(flavor = "multi_thread")]
async fn a_periodic_checkpoint_of_the_in_memory_tier_commits_as_one_record() {
    let (location, process) = tick_location().await;
    let before = process.rows().await;
    let rounds = tick_rounds();
    let expected = rows_after(&before, &rounds);
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let source = SourceModel::new(&process.catalog);
    let spans = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let outcome = run_ticked_until_killed(&process, &bottom, &source, rounds, &spans).await;
    let rows = process.try_rows().await;
    let acknowledged = source.acknowledged.load(Ordering::SeqCst);
    process.table.quiesce().await;
    drop(process);
    let spans = spans.lock().clone();
    let mut ticks = Vec::new();
    for span in &spans {
        let records = records_between(&bottom, span.head.0, span.head.1).await;
        eprintln!(
            "tick after burst {}: head {} -> {}, records {records:?}",
            span.round, span.head.0, span.head.1
        );
        ticks.push((span.round, records));
    }
    let reopened = location.open_with(&tick_config()).await;
    let reopened_rows = reopened.rows().await;
    let position = reopened.saved_position().await;
    reopened.table.quiesce().await;
    eprintln!(
        "periodic checkpoints: outcome {outcome:?}, rows equal expected: {}, reopened equal expected: {}, position {position:?}, acknowledged round {acknowledged}",
        rows.as_ref().ok() == Some(&expected),
        reopened_rows == expected
    );
    assert_eq!(outcome, Some(Ok(())), "every burst commits");
    assert_eq!(
        (
            rows.as_ref().ok() == Some(&expected),
            reopened_rows == expected,
            position.as_deref(),
            acknowledged
        ),
        (true, true, Some("lsn 4"), 4),
        "the bursts' exact outcome, before and after a reopen, with the last position saved and acknowledged"
    );
    // Each tick checkpointed the burst before it (non-vacuous: its record adds
    // the snapshot's files), in one record that also advances its sequence and
    // records the burst's source position.
    let shapes: Vec<(i64, usize, bool)> = ticks
        .iter()
        .map(|(round, records)| {
            let position = format!("AdvanceSourcePosition(lsn {round})");
            (
                *round,
                records.len(),
                records.iter().any(|record| {
                    count(record, "AddFiles") > 0
                        && count(record, "SetGroup") > 0
                        && count(record, &position) == 1
                }),
            )
        })
        .collect();
    assert_eq!(
        shapes,
        vec![(1, 1, true), (2, 1, true), (3, 1, true), (4, 1, true)],
        "each periodic checkpoint commits its files, sequence and source position in one record: {ticks:?}"
    );
}

/// The bursts of [`tick_rounds`], each checkpointed by the periodic tick,
/// killed before each storage write in turn (reads, whose order varies between
/// runs, pass until then), with the source told what the runtime tells it.
/// After each kill, a new process holds the rows of a whole number of bursts
/// (no burst, and no checkpoint, half applied), at least every burst the saved
/// or acknowledged position covers; it resumes from the newer of those and
/// replays the rest, and then holds exactly every burst's outcome.
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_at_any_storage_write_of_a_periodic_checkpoint_loses_no_change() {
    let rounds = tick_rounds();
    let (location, process) = tick_location().await;
    let before = process.rows().await;
    let expected = rows_after(&before, &rounds);
    process.instance.clear_steps();
    let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
    let source = SourceModel::new(&process.catalog);
    let spans = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let clean = run_ticked_until_killed(&process, &bottom, &source, rounds.clone(), &spans).await;
    let steps: Vec<String> = process
        .instance
        .steps()
        .into_iter()
        .filter(|step| !is_read(step.split(' ').next().unwrap_or_default()))
        .collect();
    let clean_rows = process.try_rows().await;
    process.table.quiesce().await;
    drop(process);
    let spans = spans.lock().clone();
    eprintln!(
        "clean run: {} storage writes, outcome {clean:?}, acknowledged round {}, ticks {spans:?}",
        steps.len(),
        source.acknowledged.load(Ordering::SeqCst)
    );
    for (index, step) in steps.iter().enumerate() {
        eprintln!("  {index:3} {step}");
    }
    assert_eq!(clean, Some(Ok(())), "the clean run commits every burst");
    assert!(
        clean_rows.as_ref().ok() == Some(&expected),
        "the clean run's rows: {clean_rows:?}"
    );
    // Non-vacuous: every tick wrote.
    assert!(
        spans.len() == rounds.len() && spans.iter().all(|span| !span.writes.is_empty()),
        "every tick checkpoints the burst before it: {spans:?}"
    );
    let in_tick = |index: usize| {
        spans
            .iter()
            .find(|span| span.writes.contains(&index))
            .map_or_else(String::new, |span| {
                format!(", in the tick after burst {}", span.round)
            })
    };

    let states: Vec<Vec<(i64, i64)>> = (0..=rounds.len())
        .map(|applied| rows_after(&before, &rounds[..applied]))
        .collect();
    let mut outcomes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut failures = Vec::new();
    for kill_before in 0..=steps.len() {
        let (location, process) = tick_location().await;
        process.instance.clear_steps();
        process.instance.die_after_writes(kill_before);
        let bottom = Arc::clone(&location.bottom) as Arc<dyn ObjectStore>;
        let source = SourceModel::new(&process.catalog);
        let spans = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let outcome =
            run_ticked_until_killed(&process, &bottom, &source, rounds.clone(), &spans).await;
        process.instance.kill();
        process.table.quiesce().await;
        drop(process);
        let acknowledged = source.acknowledged.load(Ordering::SeqCst);

        let next = location.open_with(&tick_config()).await;
        let reopened = next.try_rows().await;
        let position = next.saved_position().await;
        let mut problems = Vec::new();
        let saved = saved_round(position.as_deref()).unwrap_or_else(|error| {
            problems.push(error);
            0
        });
        let resume = saved.max(acknowledged);
        let state = match &reopened {
            Ok(rows) => {
                if let Some(applied) = states.iter().position(|state| state == rows) {
                    if i64::try_from(applied).expect("round") < resume {
                        problems.push(format!(
                            "the rows hold {applied} bursts, but the source resumes after burst {resume}"
                        ));
                    }
                    format!("after burst {applied}")
                } else {
                    let (applied, nearest) = states
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, state)| {
                            state.iter().filter(|row| !rows.contains(row)).count()
                                + rows.iter().filter(|row| !state.contains(row)).count()
                        })
                        .expect("states");
                    let missing: Vec<_> =
                        nearest.iter().filter(|row| !rows.contains(row)).collect();
                    let extra: Vec<_> = rows.iter().filter(|row| !nearest.contains(row)).collect();
                    let duplicated =
                        rows.len() - rows.iter().map(|(id, _)| id).collect::<BTreeSet<_>>().len();
                    problems.push(format!(
                        "the rows are no whole number of bursts (nearest: after burst {applied}, without {missing:?}, with {extra:?}, {duplicated} duplicated keys)"
                    ));
                    format!("between bursts (near burst {applied})")
                }
            }
            Err(error) => {
                problems.push(format!("the reopened table refuses queries: {error}"));
                "refused".to_string()
            }
        };
        let replay: Vec<_> = rounds
            .iter()
            .filter(|(round, _)| *round > resume)
            .cloned()
            .collect();
        let replayed = run_memory_bursts(
            Arc::clone(&next.table),
            Arc::clone(&next.catalog),
            SourceModel::new(&next.catalog),
            replay,
        )
        .await;
        let rows = next.try_rows().await;
        next.table.quiesce().await;
        outcomes
            .entry(format!(
                "{state}, saved {saved}, acknowledged {acknowledged}"
            ))
            .or_default()
            .push(kill_before);
        if let Err(error) = &replayed {
            problems.push(format!("the replay failed: {error}"));
        }
        match rows {
            Ok(rows) if rows == expected => {}
            Ok(rows) => {
                let missing: Vec<_> = expected
                    .iter()
                    .filter(|row| !rows.contains(row))
                    .take(5)
                    .collect();
                let extra: Vec<_> = rows
                    .iter()
                    .filter(|row| !expected.contains(row))
                    .take(5)
                    .collect();
                problems.push(format!(
                    "after the replay from burst {} the rows differ from every burst's outcome: missing {missing:?}, extra {extra:?}",
                    resume + 1
                ));
            }
            Err(error) => problems.push(format!("the replayed table refuses queries: {error}")),
        }
        if !problems.is_empty() {
            failures.push(format!(
                "killed before write {kill_before} ({}{}), outcome {outcome:?}, reopened {state}, saved {saved}, acknowledged {acknowledged}: {}",
                steps
                    .get(kill_before)
                    .map_or("past the end", String::as_str),
                in_tick(kill_before),
                problems.join("; ")
            ));
        }
    }
    eprintln!(
        "{} kill points; outcomes by kill point: {outcomes:?}",
        steps.len() + 1
    );
    assert!(
        failures.is_empty(),
        "{} problems ({} kill points):\n{}",
        failures.len(),
        steps.len() + 1,
        failures.join("\n")
    );
}
