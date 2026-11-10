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
use cayenne::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
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
use object_store_occ::sim::fault::FaultStore;
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
    /// Every request sent, as `<op> <path>`.
    steps: parking_lot::Mutex<Vec<String>>,
}

impl InstanceState {
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
        let bottom = Arc::new(InMemory::new());
        let (instance, store) = stack(&bottom);
        let catalog = Arc::new(
            ObjectNativeCatalog::open_or_create_at(
                Arc::clone(&store),
                Path::from(CATALOG),
                LOCATION,
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
                .builder(store, &catalog, &ctx, &seed_config())
                .create(CreateTableOptions {
                    table_name: NAME.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: location.data_url.clone(),
                    partition_column: None,
                    vortex_config: seed_config(),
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
        let (instance, store) = stack(&self.bottom);
        let catalog = Arc::new(
            ObjectNativeCatalog::open_or_create_at(
                Arc::clone(&store),
                Path::from(CATALOG),
                LOCATION,
            )
            .await
            .expect("open the object-native catalog"),
        );
        catalog.init().await.expect("init");
        let ctx = SessionContext::new();
        let table = Arc::new(
            self.builder(store, &catalog, &ctx, config)
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
        let batches = self
            .ctx
            .sql(&format!("SELECT id, v FROM {NAME} ORDER BY id"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("scan");
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
        rows
    }

    async fn saved_position(&self) -> Option<String> {
        self.catalog
            .sidecar_value(POSITION_KEY)
            .await
            .expect("read the saved position")
            .map(|bytes| String::from_utf8(bytes).expect("a text position"))
    }

    /// Every file the committed state names: the data files of every snapshot
    /// not retired, and every deletion vector.
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
        (key_committed, aggregates_committed, rows_committed == rows_before),
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
        (new_key_after, rows_after == expected, rows_rebuilt == expected),
        (vec![(NEW_KEY, value(NEW_KEY, 1))], true, true),
        "once acknowledged, the inline burst's row is visible"
    );
    process.table.quiesce().await;
}
