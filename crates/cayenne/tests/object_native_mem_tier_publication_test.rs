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

//! The object-native metastore publishes each mem-tier seal and checkpoint of a
//! `cdc_durability: memory` table as one record, so no crash and no concurrent
//! commit exposes part of one (#16147, #16167).
//!
//! One scenario drives the real provider through a data-bearing checkpoint, a
//! seal with an upsert and a delete, a checkpoint that clears the seal's
//! shadow, a tombstones-only checkpoint, a re-insert of a key deleted by an
//! earlier checkpoint, and a final seal. An in-memory model of the rows,
//! independent of Cayenne, gives the version each seal or checkpoint makes
//! durable.
//!
//! - Every seal and checkpoint stages its changes in one call: with each staged
//!   change committed as its own record, it adds exactly one record to the
//!   dataset's log. Staged in more than one call, a concurrent commit of the
//!   table's pending changes, as a CDC apply's acknowledgement point makes,
//!   could publish one part alone.
//! - The process crashes at every metastore write of the scenario in turn
//!   ([`CrashStore`]), then the catalog and table reopen from what reached the
//!   store. The table must serve one of the versions the model says could be
//!   durable at that point: every key once, with its value from that version,
//!   and `COUNT(*)` equal to the rows served. It runs again while another task
//!   commits the table's pending changes in a loop.

#![expect(
    clippy::expect_used,
    reason = "test setup and exact scenario assertions"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::metastore::object_native::{NativeCatalogConfig, ObjectNativeCatalog, dataset_log_at};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_occ::headlog::{Group, Reader};
use object_store_sim::crash::CrashStore;
use prost::Message;

use crate::common;

const PREFIX: &str = crate::common::LOCATION_CATALOG;
const TABLE: &str = "orders";

/// One step of the scenario.
#[derive(Clone, Copy, Debug)]
enum Step {
    Upsert(&'static [(i64, i64)]),
    Delete(&'static [i64]),
    /// A seal: the version it makes durable is every row applied so far.
    Seal,
    /// A checkpoint, likewise.
    Checkpoint,
}

impl Step {
    fn is_durable(self) -> bool {
        matches!(self, Step::Seal | Step::Checkpoint)
    }
}

const SCENARIO: &[Step] = &[
    Step::Upsert(&[(1, 10), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10)]),
    // Data-bearing checkpoint into a file.
    Step::Checkpoint,
    // A supersede of a file row, a new key and a delete of a file row: the seal
    // writes shadow rows, a reinsert vector and a delete-only vector.
    Step::Upsert(&[(2, 20), (7, 20)]),
    Step::Delete(&[3]),
    Step::Seal,
    // Data-bearing checkpoint that also clears the seal's shadow.
    Step::Upsert(&[(4, 30)]),
    Step::Delete(&[5]),
    Step::Checkpoint,
    // Tombstones-only checkpoint.
    Step::Delete(&[1, 6]),
    Step::Checkpoint,
    // A re-insert of key 3, whose delete-only vector an earlier checkpoint made
    // durable.
    Step::Upsert(&[(3, 40)]),
    Step::Checkpoint,
    Step::Upsert(&[(8, 50)]),
    Step::Seal,
];

type Rows = BTreeMap<i64, i64>;

/// The versions a reopen may find, oldest first: the empty table, then the rows
/// each seal or checkpoint makes durable. Rows applied after the last durable
/// step are lost by a crash, as `cdc_durability: memory` allows: the source
/// replays them.
fn durable_versions() -> Vec<Rows> {
    let mut rows = Rows::new();
    let mut versions = vec![rows.clone()];
    for step in SCENARIO {
        match step {
            Step::Upsert(pairs) => rows.extend(pairs.iter().copied()),
            Step::Delete(keys) => {
                for key in *keys {
                    rows.remove(key);
                }
            }
            Step::Seal | Step::Checkpoint => versions.push(rows.clone()),
        }
    }
    versions
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Where a table's data files go.
#[derive(Clone)]
enum Data {
    /// A local directory, for the SQLite metastore.
    Local(std::path::PathBuf),
    /// The object-native location of a catalog, on a store of its own, so a
    /// crash point counts only the metastore's writes.
    Location {
        url: String,
        store: Arc<dyn ObjectStore>,
    },
}

impl Data {
    fn location(catalog: &ObjectNativeCatalog) -> Self {
        Self::Location {
            url: crate::common::location_data_url(catalog, TABLE),
            store: Arc::new(InMemory::new()),
        }
    }

    fn base_path(&self) -> String {
        match self {
            Self::Local(path) => path.to_string_lossy().into_owned(),
            Self::Location { url, .. } => url.clone(),
        }
    }

    fn builder(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> CayenneTableProviderBuilder {
        match self {
            Self::Local(_) => CayenneTableProviderBuilder::new(catalog, runtime_env),
            Self::Location { url, store } => crate::common::table_builder_on_store(
                catalog,
                runtime_env,
                cayenne::metadata::ObjectStoreConfig {
                    url: url::Url::parse(url).expect("data url"),
                    store: Arc::clone(store),
                },
            ),
        }
    }
}

fn options(data: &Data, shards: usize) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: data.base_path(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            // The scenario seals and checkpoints explicitly.
            cdc_mem_tier_min_flush_bytes: 0,
            cdc_mem_tier_shards: shards,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    }
}

/// Every record stays in the head log's replay, so the test can read them all.
fn config() -> NativeCatalogConfig {
    NativeCatalogConfig {
        checkpoint_after_batches: usize::MAX,
        ..NativeCatalogConfig::default()
    }
}

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

async fn open_table(
    catalog: Arc<dyn MetadataCatalog>,
    data: &Data,
    shards: usize,
    create: bool,
) -> Arc<CayenneTableProvider> {
    let runtime_env = SessionContext::new().runtime_env();
    let table = if create {
        data.builder(catalog, runtime_env)
            .create(options(data, shards))
            .await
            .expect("create the table")
    } else {
        data.builder(catalog, runtime_env)
            .open(TABLE)
            .await
            .expect("reopen the table")
    };
    assert!(
        table.is_cdc_memory_mode(),
        "the in-memory CDC tier is armed"
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    Arc::new(table)
}

async fn run_step(table: &CayenneTableProvider, step: Step) {
    match step {
        Step::Upsert(pairs) => {
            let ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
            let values: Vec<i64> = pairs.iter().map(|(_, value)| *value).collect();
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .expect("upsert batch");
            let ctx = SessionContext::new();
            let write = table
                .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
                .await
                .expect("CDC upsert");
            assert!(
                write.in_memory_epoch().is_some(),
                "the upsert lands in the mem tier"
            );
            if write.has_pending_finalize() {
                write.finish().await.expect("finish the upsert");
            }
        }
        Step::Delete(keys) => {
            let ids = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(ids, vec![Arc::new(Int64Array::from(keys.to_vec()))])
                .expect("delete batch");
            table
                .write_cdc_delete_keys_in_memory(&batch)
                .await
                .expect("CDC delete")
                .expect("the delete is absorbed into the mem tier");
        }
        Step::Seal => {
            table.seal_mem_tier_durable().await.expect("seal");
        }
        Step::Checkpoint => {
            table.checkpoint_mem_tier().await.expect("checkpoint");
        }
    }
}

/// `COUNT(*)` as `table` answers it, which may come from its statistics
/// rather than a scan.
async fn counted(table: &Arc<CayenneTableProvider>) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) FROM {TABLE}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("count");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 count")
        .value(0)
}

/// The rows `table` serves.
async fn served(table: &Arc<CayenneTableProvider>) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&format!("SELECT id, value FROM {TABLE} ORDER BY id"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("scan");
    let column = |batch: &RecordBatch, index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column")
            .clone()
    };
    batches
        .iter()
        .flat_map(|batch| {
            let (ids, values) = (column(batch, 0), column(batch, 1));
            (0..ids.len())
                .map(|row| (ids.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The dataset log's records with their commit numbers, each as the names of
/// its actions.
async fn records(store: &Arc<dyn ObjectStore>) -> Vec<(u64, Vec<&'static str>)> {
    let log = dataset_log_at(Arc::clone(store), &Path::from(PREFIX), TABLE)
        .await
        .expect("read the registry")
        .expect("the dataset has a log");
    let (_, opened) = Reader::open(Arc::clone(store), log)
        .await
        .expect("open the dataset log");
    assert_eq!(opened.floor, 0, "the log was never checkpointed");
    opened
        .records
        .into_iter()
        .map(|entry| {
            let actions = Group::decode(entry.record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("catalog record")
                        .actions
                })
                .map(|action| action.name())
                .collect();
            (entry.commit, actions)
        })
        .collect()
}

/// Each seal and checkpoint stages all of its changes in one call: with every
/// staged change committed as its own record, each adds exactly one record to
/// the dataset's log. A seal or checkpoint staged in two calls would add two,
/// and a concurrent acknowledgement point could publish the first alone, which
/// a crash then exposes (#16147).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_seal_and_checkpoint_stages_its_changes_in_one_call() {
    one_record_per_step(true).await;
}

/// With the deferred commits production uses, each seal and checkpoint
/// commits its own changes at its end, as one record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_seal_and_checkpoint_publishes_one_record() {
    one_record_per_step(false).await;
}

async fn one_record_per_step(commit_each_change: bool) {
    let mut per_step = Vec::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let native = ObjectNativeCatalog::create_with_config(
        Arc::clone(&store),
        Path::from(PREFIX),
        NativeCatalogConfig {
            commit_each_change,
            ..config()
        },
    )
    .await
    .expect("create the catalog");
    let data = Data::location(&native);
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(native);
    let table = open_table(catalog, &data, 1, true).await;
    for (index, &step) in SCENARIO.iter().enumerate() {
        let before = records(&store).await.len();
        run_step(&table, step).await;
        let added = records(&store).await.split_off(before);
        if step.is_durable() {
            per_step.push((index, format!("{step:?}"), added));
        }
    }
    assert_eq!(
        served(&table).await,
        durable_versions()
            .last()
            .expect("the final version")
            .iter()
            .map(|(id, value)| (*id, *value))
            .collect::<Vec<_>>(),
        "the live table serves the model's rows"
    );
    let split: Vec<_> = per_step
        .iter()
        .filter(|(_, _, added)| added.len() != 1)
        .collect();
    assert!(
        split.is_empty(),
        "every seal and checkpoint adds exactly one record; these did not \
         (step index, step, records added as (commit, actions)): {split:#?}"
    );
}

/// What a crash at one write left behind.
#[derive(Debug, PartialEq)]
struct Outcome {
    crash_write: usize,
    /// The step the crash interrupted.
    step: usize,
    rows: Vec<(i64, i64)>,
    /// `COUNT(*)`, which must equal the number of rows.
    count: i64,
}

/// Run the scenario on a fresh store, crashing at write `crash_write` of it
/// (or not at all when the scenario writes fewer), then reopen from the store.
/// Returns `None` when the scenario finished before the crash point.
async fn crash_at(
    crash_write: usize,
    shards: usize,
    concurrent_flushes: bool,
) -> Option<(Outcome, Vec<Rows>)> {
    let storage: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let crash = Arc::new(CrashStore::new(Arc::clone(&storage)));
    let store: Arc<dyn ObjectStore> = Arc::clone(&crash) as Arc<dyn ObjectStore>;
    let catalog = Arc::new(
        ObjectNativeCatalog::create_with_config(Arc::clone(&store), Path::from(PREFIX), config())
            .await
            .expect("create the catalog"),
    );
    let data = Data::location(&catalog);
    let table = open_table(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        &data,
        shards,
        true,
    )
    .await;

    crash.crash_after(crash_write);
    // As a CDC apply's acknowledgement point does, commit the table's pending
    // changes whenever it can. After the crash it waits forever, like the rest
    // of the process.
    let flusher = concurrent_flushes.then(|| {
        let table_id = table.table_id().to_string();
        tokio::spawn(async move {
            loop {
                if catalog.commit_pending_for(&table_id).await.is_err() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
    });
    let progress = Arc::new(AtomicUsize::new(0));
    let scenario = {
        let progress = Arc::clone(&progress);
        tokio::spawn(async move {
            for (index, &step) in SCENARIO.iter().enumerate() {
                progress.store(index, Ordering::Release);
                run_step(&table, step).await;
            }
            progress.store(SCENARIO.len(), Ordering::Release);
        })
    };
    let mut scenario = scenario;
    tokio::select! {
        finished = &mut scenario => {
            finished.expect("the scenario runs without a crash");
            if !crash.has_crashed() {
                if let Some(flusher) = flusher {
                    flusher.abort();
                }
                return None;
            }
        }
        () = crash.crashed() => {
            // Stop the scenario before the flusher, so it never sees the state
            // an aborted flusher hands back.
            scenario.abort();
            assert!(
                scenario.await.is_err_and(|error| error.is_cancelled()),
                "the scenario stops at the crash"
            );
        }
    }
    if let Some(flusher) = flusher {
        flusher.abort();
    }
    let step = progress.load(Ordering::Acquire);

    // Restart: a new process opens the catalog and the table from what reached
    // the store.
    let reopened: Arc<dyn MetadataCatalog> = Arc::new(
        ObjectNativeCatalog::open_with_config(Arc::clone(&storage), Path::from(PREFIX), config())
            .await
            .expect("reopen the catalog after the crash"),
    );
    let table = open_table(reopened, &data, shards, false).await;
    let rows = served(&table).await;
    let count = counted(&table).await;

    // The durable steps completed before the interrupted one, and the
    // interrupted one if it is durable: its record may or may not have landed.
    let versions = durable_versions();
    let done = SCENARIO[..step.min(SCENARIO.len())]
        .iter()
        .filter(|step| step.is_durable())
        .count();
    let in_flight = SCENARIO.get(step).is_some_and(|step| step.is_durable());
    let allowed = versions[done..=done + usize::from(in_flight)].to_vec();
    Some((
        Outcome {
            crash_write,
            step,
            rows,
            count,
        },
        allowed,
    ))
}

async fn crash_at_every_write(shards: usize, concurrent_flushes: bool) {
    let mut wrong = Vec::new();
    let mut crash_points = 0;
    let mut interrupted = std::collections::BTreeSet::new();
    for crash_write in 0.. {
        let Some((outcome, allowed)) = crash_at(crash_write, shards, concurrent_flushes).await
        else {
            break;
        };
        crash_points += 1;
        interrupted.insert(outcome.step);
        let matches = usize::try_from(outcome.count).ok() == Some(outcome.rows.len())
            && allowed.iter().any(|version| {
                outcome.rows
                    == version
                        .iter()
                        .map(|(id, value)| (*id, *value))
                        .collect::<Vec<_>>()
            });
        eprintln!(
            "crash at write {crash_write}: step {} {:?}, rows {:?}, count {}, matches {matches}",
            outcome.step,
            SCENARIO.get(outcome.step),
            outcome.rows,
            outcome.count
        );
        if !matches {
            wrong.push((outcome, allowed));
        }
    }
    let durable_steps: std::collections::BTreeSet<usize> = SCENARIO
        .iter()
        .enumerate()
        .filter(|(_, step)| step.is_durable())
        .map(|(index, _)| index)
        .collect();
    // With concurrent flushes a write may publish a step that has already
    // returned, so which step a crash interrupts is only known without them.
    assert!(
        concurrent_flushes || durable_steps.is_subset(&interrupted),
        "a crash interrupted every seal and checkpoint: durable steps {durable_steps:?}, \
         interrupted steps {interrupted:?}"
    );
    assert!(
        wrong.is_empty(),
        "shards={shards}: of {crash_points} crash points, these reopened to rows that \
         are not a version the table had (outcome, allowed versions): {wrong:#?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_at_any_metastore_write_recovers_a_version_the_table_had() {
    crash_at_every_write(1, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_at_any_metastore_write_recovers_a_version_the_table_had_sharded() {
    crash_at_every_write(4, false).await;
}

/// The same crash points, while another task commits the table's pending
/// changes whenever it can: a seal or checkpoint staged in several calls would
/// be published in parts, and a crash between them would expose one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_at_any_metastore_write_under_concurrent_flushes_recovers_a_version_the_table_had()
{
    crash_at_every_write(1, true).await;
}

/// Regression test for #16278 on both metastores: rows in the current
/// snapshot and no protected snapshot (a full rewrite folded it), an upsert of
/// one of them in the mem tier, a seal, then a restart before any checkpoint.
/// Returns the rows served after the restart.
async fn seal_over_current_snapshot_then_restart(native: bool) -> Vec<(i64, i64)> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = || async {
        if native {
            Arc::new(
                ObjectNativeCatalog::open_or_create_at(
                    Arc::clone(&storage),
                    Path::from(PREFIX),
                    "memory",
                )
                .await
                .expect("native catalog"),
            ) as Arc<dyn MetadataCatalog>
        } else {
            let catalog = cayenne::CayenneCatalog::new(format!(
                "sqlite://{}",
                tmp.path().join("catalog.db").display()
            ))
            .expect("sqlite catalog");
            catalog.init().await.expect("init");
            Arc::new(catalog) as Arc<dyn MetadataCatalog>
        }
    };
    let first = catalog().await;
    let data = match first.as_any().downcast_ref::<ObjectNativeCatalog>() {
        Some(native) => Data::location(native),
        None => Data::Local(tmp.path().join("data")),
    };
    let mut table_options = options(&data, 1);
    // Durable writes land in Vortex files of the current snapshot.
    table_options.vortex_config.inline_max_rows = 0;
    let runtime_env = SessionContext::new().runtime_env();
    let table = Arc::new(
        data.builder(first, runtime_env)
            .create(table_options)
            .await
            .expect("create"),
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    let initial = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("batch");
    common::insert_batch(table.as_ref(), initial)
        .await
        .expect("durable insert");
    table
        .flush_pending_maintenance()
        .await
        .expect("drain maintenance");
    assert_eq!(
        served(&table).await,
        vec![(0, 1), (1, 2)],
        "before the rewrite"
    );
    // Fold the write's protected snapshot into a new current snapshot.
    table
        .sort_and_rewrite_data(64 << 20)
        .await
        .expect("rewrite");
    let protected = table
        .catalog()
        .get_all_snapshot_sequences(table.table_id())
        .await
        .expect("snapshots");
    assert!(
        protected.is_empty(),
        "the rewrite folded every protected snapshot: {protected:?}"
    );
    assert_eq!(
        served(&table).await,
        vec![(0, 1), (1, 2)],
        "before the upsert"
    );
    run_step(&table, Step::Upsert(&[(0, 10)])).await;
    assert_eq!(
        served(&table).await,
        vec![(0, 10), (1, 2)],
        "live after the upsert"
    );
    run_step(&table, Step::Seal).await;
    assert_eq!(
        served(&table).await,
        vec![(0, 10), (1, 2)],
        "live after the seal"
    );
    drop(table);
    let reopened_catalog = catalog().await;
    let reopened = open_table(reopened_catalog, &data, 1, false).await;
    served(&reopened).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_after_a_full_rewrite_serves_each_key_once_after_a_restart() {
    let sqlite = seal_over_current_snapshot_then_restart(false).await;
    let native = seal_over_current_snapshot_then_restart(true).await;
    assert_eq!(
        (sqlite, native),
        (vec![(0, 10), (1, 2)], vec![(0, 10), (1, 2)]),
        "(SQLite, object-native) rows after the restart"
    );
}
