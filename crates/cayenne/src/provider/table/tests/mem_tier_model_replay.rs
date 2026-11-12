// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Replay model actions at completed engine boundaries. Capture hooks run after
//! shard locks and the capture write lock drop and visible corpus is computed.
//! `CkPrecompute` follows insert-record and statistics preparation (the empty arm
//! has neither). `CkStage` follows the
//! single metadata call, before any RAM publication. `CkPublish` follows the fenced
//! snapshot/deletion/tier swap, before `fire_slot_advancer`. `SealStage` follows the
//! shadow/vector call, before marking segments sealed and acknowledging. Rewrite
//! capture follows the coherent `ScanView`/cutoff/folded-set capture, off `write_lock`.
//! End actions release the task to completion. No hook splits a held scan fence.
//!
//! Recovery probes copy all store objects (or the `SQLite` DB and WAL) and the
//! local data directory. Since persisted paths are absolute, with every operation
//! parked, the original directory is renamed aside and the copy occupies its path
//! for the probe; the original is restored before releasing any task. The probe
//! performs only memory applies, never checkpoints. A `Drop` guard restores paths
//! even if a comparison fails. Catalog copies are opened independently. A read
//! hold on the original provider's file-deletion fence spans copying and restoring
//! data; background reclamation cannot touch the probe. Probe maintenance is
//! quiesced before source redelivery; every scan also drains detached footer-
//! statistics persistence before copying durable state or restoring data paths.
//! Crash quiesces detached maintenance after aborting the parked operation,
//! without checkpointing its RAM tier.
//!
//! Bake uses the existing threshold 2 (zero kept-recent inputs) and the model
//! requires two eligible inputs. No engine candidate gate is bypassed.
//! Durable comparison reads the modeled data-content projection, normalizing
//! allocator gaps. Exactly one coherent durable or durable-plus-all-pending view
//! and its recovery result must match; an observed early flush is carried into
//! subsequent model prefixes. `SQLite` uses immediate catalog-call publication.

use super::*;
use crate::metadata::{CdcDurability, DeletionMode};
use crate::metastore::object_native::{NativeCatalogConfig, ObjectNativeCatalog};
use crate::provider::mem_tier::SlotAdvancer;
use arrow::array::BinaryArray;
use cayenne_metastore_model::MemTierCatalogView;
use datafusion_table_providers::util::column_reference::ColumnReference;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};
use object_store_occ::headlog::{Group, Reader};
use prost::Message;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::sync::{Semaphore, mpsc};

type Rows = Vec<(usize, usize)>;
struct Probe {
    rows: Rows,
    catalog: MemTierCatalogView,
}

#[derive(Default)]
struct Ack {
    epochs: ParkingMutex<BTreeMap<u64, usize>>,
    position: AtomicUsize,
}
impl Ack {
    fn position(&self) -> usize {
        self.position.load(Ordering::SeqCst)
    }
}
#[async_trait::async_trait]
impl SlotAdvancer for Ack {
    async fn on_checkpoint_durable(&self, epoch: u64) {
        let position = self
            .epochs
            .lock()
            .range(..=epoch)
            .map(|(_, p)| *p)
            .max()
            .unwrap_or(0);
        self.position.fetch_max(position, Ordering::SeqCst);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}
fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .expect("batch")
}
async fn apply(table: &CayenneTableProvider, key: usize, value: Option<usize>) -> u64 {
    let key = i64::try_from(key).expect("key");
    if let Some(value) = value {
        let write = table
            .write_cdc_append_stream(
                single_batch_stream(batch(&[(key, i64::try_from(value).expect("value"))])),
                &SessionContext::new().task_ctx(),
            )
            .await
            .expect("upsert");
        let epoch = write.in_memory_epoch().expect("upsert must land in memory");
        if write.has_pending_finalize() {
            write.finish().await.expect("finalize");
        }
        epoch
    } else {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![key]))],
        )
        .expect("delete batch");
        table
            .write_cdc_delete_keys_in_memory(&batch)
            .await
            .expect("delete")
            .expect("absorbed delete")
    }
}
async fn rows(table: &Arc<CayenneTableProvider>) -> Rows {
    let ctx = SessionContext::new();
    ctx.register_table("replay", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql("SELECT id, value FROM replay ORDER BY id, value")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("scan");
    // Footer-statistics persistence runs in detached tasks outside the caller's
    // deferred scope. Settle them before copying durable state or restoring paths.
    // A timeout reports an unmapped observation boundary rather than skipping it.
    tokio::time::timeout(
        Duration::from_secs(30),
        table.wait_for_scan_file_statistics_persistence(),
    )
    .await
    .expect(
        "unmapped observation boundary: scan statistics cannot drain while an operation is parked",
    );
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("values");
            (0..batch.num_rows())
                .map(|i| {
                    (
                        usize::try_from(ids.value(i)).expect("id"),
                        usize::try_from(values.value(i)).expect("value"),
                    )
                })
                .collect::<Rows>()
        })
        .collect()
}

fn decode_key(bytes: &[u8]) -> usize {
    usize::try_from(i64::from_be_bytes(
        bytes.try_into().expect("Int64 key bytes"),
    ))
    .expect("model key")
}
fn batch_rows(batches: &[RecordBatch]) -> Rows {
    let mut out = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value");
        for row in 0..batch.num_rows() {
            out.push((
                usize::try_from(keys.value(row)).expect("key"),
                usize::try_from(values.value(row)).expect("value"),
            ));
        }
    }
    out.sort_unstable();
    out
}
async fn physical_rows(table: &CayenneTableProvider, snapshot: &str) -> Rows {
    let ctx = SessionContext::new();
    // A table whose data is in the object-native location reads it through its
    // own store, which this session must know.
    if let Some(storage) = table.dataset_storage() {
        storage.register(&ctx.runtime_env());
    }
    let plan = table
        .create_snapshot_scan_plan(&ctx.state(), snapshot, None, &[], None)
        .await
        .expect("raw snapshot plan");
    let batches = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("raw snapshot rows");
    table.wait_for_scan_file_statistics_persistence().await;
    batch_rows(&batches)
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    Object,
    Sqlite,
}
struct Disk {
    root: TempDir,
    /// The table's data directory: the dataset's `data` folder in the local
    /// object-native location (`file://…`) for the object-native metastore, a
    /// plain directory for SQLite.
    data: PathBuf,
    /// The catalog's key prefix in `store`, beside the dataset folders of the
    /// location `data` is in.
    prefix: String,
    db: PathBuf,
    store: Arc<dyn ObjectStore>,
    backend: Backend,
}
fn config() -> NativeCatalogConfig {
    NativeCatalogConfig {
        checkpoint_after_batches: usize::MAX,
        ..NativeCatalogConfig::default()
    }
}
impl Disk {
    fn new(backend: Backend) -> Self {
        let root =
            tempfile::tempdir_in(std::env::current_dir().expect("cwd")).expect("workspace tempdir");
        let location = root.path().join("state").join("cayenne").join("v1");
        let data = match backend {
            Backend::Object => location
                .join("datasets")
                .join(crate::metastore::object_native::new_dataset_folder(
                    "replay",
                ))
                .join("data"),
            Backend::Sqlite => root.path().join("data"),
        };
        Self {
            data,
            prefix: format!("{}/catalog", location.to_string_lossy().trim_matches('/')),
            db: root.path().join("catalog.db"),
            root,
            store: Arc::new(InMemory::new()),
            backend,
        }
    }
    async fn catalog(&self, create: bool) -> Arc<dyn MetadataCatalog> {
        match self.backend {
            Backend::Object => Arc::new(if create {
                ObjectNativeCatalog::create_with_config(
                    Arc::clone(&self.store),
                    ObjectPath::from(self.prefix.as_str()),
                    config(),
                )
                .await
                .expect("create catalog")
            } else {
                ObjectNativeCatalog::open_with_config(
                    Arc::clone(&self.store),
                    ObjectPath::from(self.prefix.as_str()),
                    config(),
                )
                .await
                .expect("open catalog")
            }),
            Backend::Sqlite => {
                let catalog =
                    CayenneCatalog::new(format!("sqlite://{}", self.db.display())).expect("sqlite");
                catalog.init().await.expect("init");
                Arc::new(catalog)
            }
        }
    }
    async fn probe(
        &self,
        source: &[(usize, Option<usize>)],
        acknowledged: usize,
        sequences: &BTreeMap<i64, usize>,
    ) -> Probe {
        let mut clone = Self::new(self.backend);
        // The copied objects keep their keys, so the clone's catalog is at the
        // same prefix; the table reads the data directory restored below.
        clone.prefix.clone_from(&self.prefix);
        let objects = self
            .store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        for object in objects {
            let bytes = self
                .store
                .get(&object.location)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes");
            clone
                .store
                .put(&object.location, bytes.into())
                .await
                .expect("copy object");
        }
        for suffix in ["", "-wal", "-shm"] {
            let from = PathBuf::from(format!("{}{suffix}", self.db.display()));
            if from.exists() {
                std::fs::copy(from, format!("{}{suffix}", clone.db.display()))
                    .expect("copy sqlite");
            }
        }
        let saved = self.root.path().join("saved-data");
        std::fs::rename(&self.data, &saved).expect("park original data");
        let restore = Restore {
            live: self.data.clone(),
            saved,
        };
        copy_dir(&restore.saved, &restore.live);
        let catalog = clone.catalog(false).await;
        let table_id = catalog
            .get_table("replay")
            .await
            .expect("durable table")
            .table_id;
        let records = catalog
            .get_insert_records(&table_id)
            .await
            .expect("durable inserts");
        let snapshots = catalog
            .get_all_snapshot_sequences(&table_id)
            .await
            .expect("durable snapshots");
        let inline = catalog
            .get_inlined_data(&table_id)
            .await
            .expect("durable inline");
        let deletes = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("durable vectors");
        let normalize = |seq: i64| {
            *sequences
                .get(&seq)
                .unwrap_or_else(|| panic!("unmapped sequence {seq}, map={sequences:?}"))
        };
        let mut insert_records = [0; 2];
        for (key, seq) in records {
            insert_records[decode_key(&key)] = normalize(seq);
        }
        let mut inline_rows = Vec::new();
        for data in inline {
            for (key, value) in
                batch_rows(&deserialize_ipc_to_batch(&data.data_ipc).expect("inline IPC"))
            {
                inline_rows.push((key, value, normalize(data.sequence_number)));
            }
        }
        inline_rows.sort_unstable();
        let mut delete_entries = Vec::new();
        for file in deletes {
            let reader = arrow_ipc::reader::FileReader::try_new(
                // A data path in a local location is a `file://` URL.
                std::fs::File::open(
                    url::Url::parse(&file.path)
                        .ok()
                        .and_then(|url| url.to_file_path().ok())
                        .unwrap_or_else(|| PathBuf::from(&file.path)),
                )
                .expect("vector file"),
                None,
            )
            .expect("vector IPC");
            for batch in reader {
                let batch = batch.expect("vector batch");
                let keys = batch
                    .column_by_name("row_key")
                    .expect("row_key")
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("binary keys");
                for row in 0..batch.num_rows() {
                    delete_entries.push((
                        decode_key(keys.value(row)),
                        normalize(file.sequence_number),
                        normalize(file.reinsert_sequence.unwrap_or(0)),
                    ));
                }
            }
        }
        delete_entries.sort_unstable();
        let table = open(self, catalog).await;
        table.quiesce().await;
        let current_rows = physical_rows(&table, &table.get_current_snapshot_id()).await;
        let mut protected_rows = Vec::new();
        for (id, sequence) in snapshots {
            protected_rows.push((normalize(sequence), physical_rows(&table, &id).await));
        }
        protected_rows.sort_unstable();
        let durable = MemTierCatalogView {
            current_rows,
            protected_rows,
            inline_rows,
            delete_entries,
            insert_records,
        };
        for &(key, value) in &source[acknowledged..] {
            apply(&table, key, value).await;
        }
        let result = rows(&table).await;
        drop(table);
        drop(restore);
        Probe {
            rows: result,
            catalog: durable,
        }
    }
}
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        if entry.file_type().expect("type").is_dir() {
            copy_dir(&entry.path(), &to.join(entry.file_name()));
        } else {
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy file");
        }
    }
}
struct Restore {
    live: PathBuf,
    saved: PathBuf,
}
impl Drop for Restore {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.live).expect("remove probe data");
        std::fs::rename(&self.saved, &self.live).expect("restore original data");
    }
}
/// The builder for the table on `disk`: with its data in the object-native
/// location, the store a `file://` location's data is read through.
fn builder(disk: &Disk, catalog: Arc<dyn MetadataCatalog>) -> CayenneTableProviderBuilder {
    let builder = CayenneTableProviderBuilder::new(catalog, SessionContext::new().runtime_env());
    match disk.backend {
        Backend::Object => builder.with_object_store(crate::metadata::ObjectStoreConfig {
            url: url::Url::from_directory_path(&disk.data).expect("data url"),
            store: crate::dataset_storage::local_directory_store(),
        }),
        Backend::Sqlite => builder,
    }
}
fn base_path(disk: &Disk) -> String {
    match disk.backend {
        Backend::Object => url::Url::from_file_path(&disk.data)
            .expect("data url")
            .to_string(),
        Backend::Sqlite => disk.data.to_string_lossy().into_owned(),
    }
}
async fn open(disk: &Disk, catalog: Arc<dyn MetadataCatalog>) -> Arc<CayenneTableProvider> {
    let table = builder(disk, catalog).open("replay").await.expect("reopen");
    table.install_slot_advancer(Arc::new(Ack::default()));
    Arc::new(table)
}
async fn initial(disk: &Disk, catalog: Arc<dyn MetadataCatalog>) -> Arc<CayenneTableProvider> {
    let table = builder(disk, Arc::clone(&catalog))
        .create(CreateTableOptions {
            table_name: "replay".into(),
            schema: schema(),
            primary_key: vec!["id".into()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".into()]))),
            base_path: base_path(disk),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                deletion_mode: DeletionMode::Key,
                inline_max_rows: 0,
                cdc_mem_tier_min_flush_bytes: 0,
                cdc_mem_tier_seal_age_ms: 0,
                cdc_mem_tier_max_age_ms: 0,
                cdc_mem_tier_max_bytes: 0,
                // K=min(3, trigger-2)=0; the real bake still needs two inputs.
                compaction_trigger_protected_snapshots: 2,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create table");
    // Seed the model's genesis through the real file writer, at exactly S=1.
    let sequence = table
        .reserve_sequences_local(1)
        .await
        .expect("genesis sequence");
    assert_eq!(sequence, 1);
    table
        .insert_to_new_snapshot_with_sequence(
            single_batch_stream(batch(&[(0, 1), (1, 2)])),
            sequence,
            1,
            None,
        )
        .await
        .expect("genesis snapshot");
    catalog
        .commit_pending_for(table.table_id())
        .await
        .expect("genesis flush");
    assert_eq!(
        catalog
            .get_all_snapshot_sequences(table.table_id())
            .await
            .expect("snapshots")
            .len(),
        1
    );
    let table = Arc::new(table);
    assert_eq!(rows(&table).await, vec![(0, 1), (1, 2)]);
    table
}

struct Operation {
    task: tokio::task::JoinHandle<()>,
    arrived: mpsc::UnboundedReceiver<&'static str>,
    release: Arc<Semaphore>,
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Operation {
    fn start(table: Arc<CayenneTableProvider>, action: &str) -> Self {
        let (send, arrived) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let gate = Arc::clone(&release);
        *table.test_mem_tier_step_hook.lock() = Some(Arc::new(move |step| {
            let send = send.clone();
            let gate = Arc::clone(&gate);
            Box::pin(async move {
                send.send(step).expect("driver alive");
                gate.acquire().await.expect("release").forget();
            })
        }));
        let action = action.to_string();
        let task = tokio::spawn(async move {
            match action.as_str() {
                "SealCapture" => {
                    table.seal_mem_tier_durable().await.expect("seal");
                }
                "CkCapture" => {
                    table.checkpoint_mem_tier().await.expect("checkpoint");
                }
                "FullRewriteCapture" => {
                    assert!(
                        table
                            .rewrite_current_snapshot_holding_bounded(None, true)
                            .await
                            .expect("full rewrite"),
                        "model FullRewrite must commit"
                    );
                }
                _ => panic!("unknown operation {action}"),
            }
        });
        Self {
            task,
            arrived,
            release,
        }
    }
    async fn at(&mut self, action: &str) {
        let arrived = tokio::time::timeout(Duration::from_secs(30), self.arrived.recv())
            .await
            .expect("operation boundary timed out");
        assert_eq!(arrived, Some(action), "wrong operation boundary");
    }
    async fn cancel(mut self) {
        self.task.abort();
        let result = (&mut self.task).await;
        assert!(
            result.is_err_and(|e| e.is_cancelled()),
            "cancel parked task"
        );
    }
    async fn finish(mut self) {
        self.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(30), &mut self.task)
            .await
            .expect("operation end timed out")
            .expect("operation task");
    }
}

#[derive(Default, Debug)]
struct Counts {
    actions: BTreeMap<String, usize>,
    live: usize,
    recovery: usize,
    acknowledgements: usize,
    durable_state_checks: usize,
    matched_durable: usize,
    matched_durable_plus_pending: usize,
    bake_pruned_entries: usize,
    bakes_committed: usize,
    bakes_declined: usize,
}
async fn replay(
    backend: Backend,
    run: &str,
    actions: &[String],
    counts: &mut Counts,
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut model_actions = Vec::new();
    let mut sequences = BTreeMap::from([(0, 0), (1, 1)]);
    let mut model_high = 1;
    let disk = Disk::new(backend);
    let mut catalog = Some(disk.catalog(true).await);
    let mut table = Some(initial(&disk, Arc::clone(catalog.as_ref().expect("catalog"))).await);
    let ack = Arc::new(Ack::default());
    table
        .as_ref()
        .expect("table")
        .install_slot_advancer(Arc::clone(&ack) as Arc<dyn SlotAdvancer>);
    let mut operation: Option<Operation> = None;
    let mut rewrite: Option<Operation> = None;
    let mut source = vec![];
    let mut redelivery = None;
    for (index, action) in actions.iter().enumerate() {
        model_actions.push(action.clone());
        let expected = cayenne_metastore_model::mem_tier_steps(run, &model_actions)
            .unwrap_or_else(|| panic!("disabled model prefix {run} {model_actions:?}"))
            .pop()
            .expect("step");
        let label = format!(
            "{backend:?} {run} step {index} {action}; trace={:?}",
            &actions[..=index]
        );
        *counts
            .actions
            .entry(action.split('(').next().expect("kind").into())
            .or_default() += 1;
        let numeric = action.split_once('(').map(|(_, rest)| {
            rest.trim_end_matches(')')
                .split(',')
                .map(|v| v.trim().parse::<usize>().expect("numeric action"))
                .collect::<Vec<_>>()
        });
        match action.as_str() {
            _ if action.starts_with("Upsert(") || action.starts_with("Delete(") => {
                let args = numeric.expect("arguments");
                let key = args[0];
                let value = args.get(1).copied();
                let position = if let Some(cursor) = redelivery {
                    assert_eq!(source[cursor], (key, value), "redelivery");
                    cursor + 1
                } else {
                    source.push((key, value));
                    source.len()
                };
                redelivery = (position < source.len()).then_some(position);
                let epoch = apply(table.as_ref().expect("table"), key, value).await;
                ack.epochs.lock().insert(epoch, position);
            }
            "SealCapture" | "CkCapture" | "FullRewriteCapture" => {
                let mut task = Operation::start(Arc::clone(table.as_ref().expect("table")), action);
                task.at(action).await;
                if action == "FullRewriteCapture" {
                    assert!(rewrite.replace(task).is_none());
                } else {
                    assert!(operation.replace(task).is_none());
                }
            }
            "SealStage" | "CkPrecompute" | "CkStage" | "CkPublish" => {
                let task = operation.as_mut().expect("operation");
                task.release.add_permits(1);
                task.at(action).await;
            }
            "SealEnd" | "CkEnd" => {
                operation.take().expect("operation").finish().await;
            }
            "FullRewrite" => {
                rewrite.take().expect("rewrite").finish().await;
            }
            "Flush" => {
                let t = table.as_ref().expect("table");
                catalog
                    .as_ref()
                    .expect("catalog")
                    .commit_pending_for(t.table_id())
                    .await
                    .expect("flush");
            }
            "Bake" => {
                let table = table.as_ref().expect("table");
                let before = table.pk_deletion_snapshot().delete_len();
                let inputs = table.protected_snapshots.load().len();
                let keep = table.bake_keep_recent_snapshots();
                let committed = table
                    .bake_seq_prefix_protected_snapshots()
                    .await
                    .expect("bake");
                let after = table.pk_deletion_snapshot().delete_len();
                eprintln!(
                    "BAKE {label} inputs={inputs} keep_recent={keep} committed={committed} deletes_before={before} deletes_after={after}"
                );
                if committed {
                    counts.bakes_committed += 1;
                    counts.bake_pruned_entries += before.saturating_sub(after);
                } else {
                    counts.bakes_declined += 1;
                }
            }
            "Crash" => {
                if let Some(task) = operation.take() {
                    task.cancel().await;
                }
                if let Some(task) = rewrite.take() {
                    task.cancel().await;
                }
                table.as_ref().expect("table").quiesce().await;
                table.take();
                catalog.take();
                ack.epochs.lock().clear();
            }
            "Recover" => {
                catalog = Some(disk.catalog(false).await);
                let reopened = open(&disk, Arc::clone(catalog.as_ref().expect("catalog"))).await;
                reopened.install_slot_advancer(Arc::clone(&ack) as Arc<dyn SlotAdvancer>);
                table = Some(reopened);
                redelivery = (ack.position() < source.len()).then_some(ack.position());
            }
            _ => panic!("unmapped/control-only action: {label}"),
        }
        if action.starts_with("Upsert(")
            || action.starts_with("Delete(")
            || matches!(action.as_str(), "CkCapture" | "SealCapture")
        {
            let count = expected
                .sequence_high_water
                .checked_sub(model_high)
                .expect("monotone reservation");
            let actual_high = table.as_ref().expect("table").sequence_high_water().await;
            for offset in 0..count {
                sequences.insert(
                    actual_high - i64::try_from(offset).expect("offset"),
                    expected.sequence_high_water - offset,
                );
            }
        }
        model_high = expected.sequence_high_water;
        // available also excludes incomplete source redelivery; live_rows is still
        // defined there, so check every prefix except an outstanding Crash.
        if let Some(table) = &table {
            counts.live += 1;
            compare(
                &mut failures,
                "live",
                &label,
                &rows(table).await,
                &expected.live_rows,
            );
        }
        counts.acknowledgements += 1;
        compare(
            &mut failures,
            "ack",
            &label,
            &ack.position(),
            &expected.acknowledged_position,
        );
        counts.recovery += 1;
        let _files = if let Some(table) = &table {
            Some(Arc::clone(&table.file_deletion_fence).read_owned().await)
        } else {
            None
        };
        let probe = disk.probe(&source, ack.position(), &sequences).await;
        counts.durable_state_checks += 1;
        let durable = probe.catalog == expected.durable && probe.rows == expected.recovered_rows;
        let early = matches!(backend, Backend::Object)
            && probe.catalog == expected.durable_plus_pending
            && probe.rows == expected.recovered_rows_plus_pending;
        if durable {
            counts.matched_durable += 1;
        } else if early {
            counts.matched_durable_plus_pending += 1;
            // Carry this exact observed publication into subsequent model prefixes,
            // including Crash/Recover. An explicit later Flush remains a no-op.
            assert!(!expected.pending_changes.is_empty());
            model_actions.push("Flush".into());
        } else {
            let error = format!(
                "durable state: {label}\nengine={:?} recovered={:?}\nmodel durable={:?} recovered={:?}\nmodel durable+pending={:?} recovered={:?} pending={:?}",
                probe.catalog,
                probe.rows,
                expected.durable,
                expected.recovered_rows,
                expected.durable_plus_pending,
                expected.recovered_rows_plus_pending,
                expected.pending_changes
            );
            eprintln!("{error}");
            failures.push(error);
        }
    }
    if let Some(task) = operation.take() {
        task.cancel().await;
    }
    if let Some(task) = rewrite.take() {
        task.cancel().await;
    }
    if let Some(table) = table {
        table.quiesce().await;
    }
    failures
}
fn compare<T: std::fmt::Debug + PartialEq>(
    failures: &mut Vec<String>,
    check: &str,
    label: &str,
    actual: &T,
    expected: &T,
) {
    if actual != expected {
        let error = format!("{check}: {label}\nengine={actual:?} model={expected:?}");
        eprintln!("{error}");
        failures.push(error);
    }
}
async fn suite(backend: Backend) {
    let mut counts = Counts::default();
    let mut failures = Vec::new();
    let native = include_str!("../../../../tests/traces/mem-tier-seal-delete-only.jsonl");
    let native_compatible = native.lines().all(|line| {
        let value: serde_json::Value = serde_json::from_str(line).expect("trace");
        let actions = value["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .map(|a| a.as_str().expect("action").to_string())
            .collect::<Vec<_>>();
        cayenne_metastore_model::mem_tier_steps("mem-tier-seal-delete-only-sqlite", &actions)
            .is_some()
    });
    eprintln!("{backend:?} object-native traces enabled in SQLite model: {native_compatible}");
    let traces = if matches!(backend, Backend::Sqlite) && !native_compatible {
        include_str!("../../../../tests/traces/mem-tier-seal-delete-only-sqlite.jsonl")
    } else {
        native
    };
    for line in traces.lines() {
        let value: serde_json::Value = serde_json::from_str(line).expect("trace JSON");
        let actions = value["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .map(|a| a.as_str().expect("action").to_string())
            .collect::<Vec<_>>();
        failures.extend(
            replay(
                backend,
                match backend {
                    Backend::Object => "mem-tier-seal-delete-only",
                    Backend::Sqlite => "mem-tier-seal-delete-only-sqlite",
                },
                &actions,
                &mut counts,
            )
            .await,
        );
    }
    eprintln!("{backend:?} {counts:?}");
    assert_eq!(counts.bakes_declined, 0, "enabled model bakes must commit");
    assert!(
        counts.bakes_committed > 0 && counts.bake_pruned_entries > 0,
        "exercise real bake and prune"
    );
    assert!(
        failures.is_empty(),
        "{} divergences (traces printed above)",
        failures.len()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_model_replay_object() {
    suite(Backend::Object).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_model_replay_sqlite() {
    suite(Backend::Sqlite).await;
}

/// The known-defect witnesses must satisfy the candidate model on the guarded
/// engine. Temporarily removing a safeguard makes these ordinary comparisons
/// fail; the oracle is never switched to accepting the defective model's rows.
async fn control_witness(text: &str) {
    let trace: serde_json::Value = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON"))
        .find(|value| {
            value["status"] == "FAIL" && value["property"] == "recovered reads match the source"
        })
        .expect("FAIL witness");
    // The split model has extra phase actions. On the one-call engine these
    // boundaries do not exist; the mutation inserts its second call between
    // CkStage and CkPublish, with the partial commit exposed at CkStage.
    let actions = trace["actions"]
        .as_array()
        .expect("actions")
        .iter()
        .map(|a| a.as_str().expect("action"))
        .filter(|a| !matches!(*a, "CkStageRest" | "SealStageRest"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut counts = Counts::default();
    let failures = replay(
        Backend::Object,
        "mem-tier-seal-delete-only",
        &actions,
        &mut counts,
    )
    .await;
    eprintln!("control witness {counts:?}");
    assert!(
        failures.is_empty(),
        "{} control divergences (traces printed above)",
        failures.len()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_model_replay_control_seal() {
    control_witness(include_str!(
        "../../../../tests/traces/mem-tier-current-3-exhaust.jsonl"
    ))
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_model_replay_control_one_call() {
    control_witness(include_str!(
        "../../../../tests/traces/mem-tier-no-one-call.jsonl"
    ))
    .await;
}

async fn log_records(disk: &Disk, label: &str) -> Vec<(u64, Vec<&'static str>)> {
    let log = crate::metastore::object_native::dataset_log_at(
        Arc::clone(&disk.store),
        &ObjectPath::from(disk.prefix.as_str()),
        "replay",
    )
    .await
    .expect("registry")
    .expect("dataset log");
    let (_, opened) = Reader::open(Arc::clone(&disk.store), log)
        .await
        .expect("read log");
    assert_eq!(opened.floor, 0, "retain all records for attribution");
    let records = opened
        .records
        .into_iter()
        .map(|entry| {
            let names = Group::decode(entry.record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("record")
                        .actions
                })
                .map(|a| a.name())
                .collect();
            (entry.commit, names)
        })
        .collect::<Vec<_>>();
    eprintln!("PUBLISHER {label}: {records:?}");
    records
}

/// Isolate the footer-statistics task instead of inferring its publication from
/// a row scan that has already returned. The checkpoint remains parked throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_model_replay_early_publisher() {
    let disk = Disk::new(Backend::Object);
    let catalog = disk.catalog(true).await;
    let table = initial(&disk, catalog).await;
    table.install_slot_advancer(Arc::new(Ack::default()));
    apply(&table, 0, Some(10)).await;
    apply(&table, 0, Some(11)).await;
    let mut checkpoint = Operation::start(Arc::clone(&table), "CkCapture");
    checkpoint.at("CkCapture").await;
    checkpoint.release.add_permits(1);
    checkpoint.at("CkPrecompute").await;
    checkpoint.release.add_permits(1);
    checkpoint.at("CkStage").await;
    let stage = log_records(&disk, "CkStage").await;
    checkpoint.release.add_permits(1);
    checkpoint.at("CkPublish").await;
    let before = log_records(&disk, "CkPublish before SQL").await;
    assert_eq!(stage, before);
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let release = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&release);
    *table.test_file_statistics_publish_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            entered.send(()).expect("driver");
            gate.acquire()
                .await
                .expect("release footer statistics")
                .forget();
        })
    }));
    let counters = table.scan_file_statistics_counters();
    let scan = {
        let table = Arc::clone(&table);
        tokio::spawn(async move { rows(&table).await })
    };
    tokio::time::timeout(Duration::from_secs(30), arrived)
        .await
        .expect("footer hook reached")
        .expect("footer hook");
    let parked = log_records(
        &disk,
        "CkPublish SQL parked before persist_scan_file_statistics",
    )
    .await;
    assert_eq!(before, parked);
    release.add_permits(1);
    assert_eq!(scan.await.expect("SQL scan"), vec![(0, 11), (1, 2)]);
    let after = log_records(&disk, "CkPublish after footer task drained").await;
    let persisted =
        table.scan_file_statistics_counters().persisted_upserts - counters.persisted_upserts;
    eprintln!("PUBLISHER detached footer persisted_upserts={persisted}");
    assert!(persisted > 0);
    let added = &after[before.len()..];
    assert!(!added.is_empty());
    let checkpoints = added
        .iter()
        .filter(|(_, actions)| actions.contains(&"SetGroup"))
        .collect::<Vec<_>>();
    assert_eq!(checkpoints.len(), 1, "one checkpoint publication");
    for name in [
        "AddDeleteFiles",
        "SetGroup",
        "AddInsertRecords",
        "SetTableStats",
        "RemoveFileStats",
    ] {
        assert!(
            checkpoints[0].1.contains(&name),
            "checkpoint record missing {name}: {added:?}"
        );
    }
    assert!(
        added
            .iter()
            .any(|(_, actions)| actions.contains(&"SetFileStats"))
    );
    checkpoint.cancel().await;
    table.quiesce().await;
}
