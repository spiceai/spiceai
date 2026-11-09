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

//! A valid data file that no commit names, planted in the live snapshot
//! directory of a dataset in the object-native location, is never adopted: no
//! scan shows its rows and no manifest names it, after a write and its
//! maintenance, a subset compaction, an empty committed file set, a reopen or
//! the sweeps. This is the shape a crash between an upload and its commit leaves
//! behind once files are written at their final keys.
//!
//! The dataset's store follows `CAYENNE_TEST_DATA_STORE`: `local` is a local
//! directory (through `LocalConditionalPut`), `memory` an in-memory store, and
//! `fault` the in-memory store under `FaultStore` with no faults armed. A
//! control on the fixture's trunk path (SQLite metastore, data not in the
//! location) shows the planted file is valid data that a listing does adopt.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
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
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use object_store_occ::sim::fault::FaultStore;

use crate::common;

const CATALOG: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const ORPHAN_FIRST: i64 = 1_000_001;
const ORPHAN_LAST: i64 = 1_001_000;

/// Counts LIST requests whose prefix overlaps one watched prefix, and COPY
/// requests and writes into a staging directory under it.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    watched: parking_lot::RwLock<String>,
    lists: AtomicUsize,
    copies: AtomicUsize,
    staged_writes: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            watched: parking_lot::RwLock::new(String::new()),
            lists: AtomicUsize::new(0),
            copies: AtomicUsize::new(0),
            staged_writes: AtomicUsize::new(0),
        }
    }

    fn watch(&self, prefix: &str) {
        *self.watched.write() = prefix.to_string();
        self.lists.store(0, Ordering::SeqCst);
        self.copies.store(0, Ordering::SeqCst);
        self.staged_writes.store(0, Ordering::SeqCst);
    }

    fn lists(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }

    fn copies(&self) -> usize {
        self.copies.load(Ordering::SeqCst)
    }

    fn staged_writes(&self) -> usize {
        self.staged_writes.load(Ordering::SeqCst)
    }

    fn watched(&self, location: &Path) -> bool {
        let watched = self.watched.read();
        !watched.is_empty() && location.as_ref().starts_with(watched.as_str())
    }

    fn note_write(&self, location: &Path) {
        if self.watched(location) && location.as_ref().contains("/_staging/") {
            self.staged_writes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn note_list(&self, prefix: Option<&Path>) {
        let watched = self.watched.read();
        let prefix = prefix.map_or("", Path::as_ref);
        if !watched.is_empty()
            && (prefix.starts_with(watched.as_str()) || watched.starts_with(prefix))
        {
            self.lists.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Display for CountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
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
        self.note_write(location);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.note_write(location);
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
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        if self.watched(to) {
            self.copies.fetch_add(1, Ordering::SeqCst);
        }
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

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn vortex_config(compaction_trigger_files: usize) -> VortexConfig {
    VortexConfig {
        // Every write becomes a data file, never rows inlined into the metastore.
        inline_max_rows: 0,
        inline_max_bytes: 0,
        compaction_trigger_files,
        // The small-files compaction suite's settings: a 1 MiB target makes every
        // write a small file and no interval scheduler runs on its own; a pick of
        // 3 lets the append-only picker qualify the small tier by file count.
        target_vortex_file_size_mb: 1,
        compaction_max_levels: 3,
        compaction_max_files_per_pick: 3,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// Sum of `v = id * 10` for `id` in `first..=last`.
fn expected_sum(first: i64, last: i64) -> i64 {
    (first..=last).map(|id| id * 10).sum()
}

/// A dataset in the object-native location, over the store
/// `CAYENNE_TEST_DATA_STORE` names.
struct Rig {
    label: String,
    /// The bottom store, read and written by the test outside Cayenne.
    raw: Arc<dyn ObjectStore>,
    counting: Arc<CountingStore>,
    /// The store the catalog and the tables use.
    store: Arc<dyn ObjectStore>,
    catalog: parking_lot::RwLock<Arc<dyn MetadataCatalog>>,
    datasets_prefix: String,
    _dir: Option<tempfile::TempDir>,
}

struct Table {
    name: String,
    data_prefix: String,
    data_url: String,
    provider: Arc<CayenneTableProvider>,
    ctx: SessionContext,
}

impl Rig {
    async fn new() -> Self {
        let kind = std::env::var("CAYENNE_TEST_DATA_STORE").unwrap_or_else(|_| "local".into());
        let (raw, dir): (Arc<dyn ObjectStore>, Option<tempfile::TempDir>) = match kind.as_str() {
            "" | "local" => {
                let dir = tempfile::tempdir().expect("temp dir");
                let store =
                    object_store_occ::local_conditional_put::LocalConditionalPut::new(dir.path())
                        .expect("local store");
                (Arc::new(store), Some(dir))
            }
            "memory" | "fault" => (Arc::new(object_store::memory::InMemory::new()), None),
            other => panic!(
                "CAYENNE_TEST_DATA_STORE is '{other}'; this test runs on 'local', 'memory' or 'fault'"
            ),
        };
        let counting = Arc::new(CountingStore::new(Arc::clone(&raw)));
        let store: Arc<dyn ObjectStore> = if kind == "fault" {
            Arc::new(FaultStore::new(
                Arc::clone(&counting) as Arc<dyn ObjectStore>
            ))
        } else {
            Arc::clone(&counting) as Arc<dyn ObjectStore>
        };
        let catalog = Self::open_catalog(&store).await;
        let datasets_prefix = catalog.datasets_prefix().to_string();
        Self {
            label: format!(
                "placed[{}]",
                if kind.is_empty() {
                    "local"
                } else {
                    kind.as_str()
                }
            ),
            raw,
            counting,
            store,
            catalog: parking_lot::RwLock::new(Arc::new(catalog)),
            datasets_prefix,
            _dir: dir,
        }
    }

    async fn open_catalog(store: &Arc<dyn ObjectStore>) -> ObjectNativeCatalog {
        let catalog = ObjectNativeCatalog::open_or_create_at(
            Arc::clone(store),
            Path::from(CATALOG),
            LOCATION,
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        catalog
    }

    fn catalog(&self) -> Arc<dyn MetadataCatalog> {
        Arc::clone(&self.catalog.read())
    }

    fn builder(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        ctx: &SessionContext,
        data_url: &str,
    ) -> CayenneTableProviderBuilder {
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env()).with_object_store(
            ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(&self.store),
            },
        )
    }

    async fn create(&self, name: &str, compaction_trigger_files: usize) -> Table {
        let data_prefix = format!("{}/{}/data", self.datasets_prefix, new_dataset_folder(name));
        let data_url = format!("s3://bucket/{data_prefix}");
        let ctx = SessionContext::new();
        let provider = Arc::new(
            self.builder(self.catalog(), &ctx, &data_url)
                .create(CreateTableOptions {
                    table_name: name.to_string(),
                    schema: schema(),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: data_url.clone(),
                    partition_column: None,
                    vortex_config: vortex_config(compaction_trigger_files),
                })
                .await
                .expect("create the table"),
        );
        ctx.register_table(name, Arc::clone(&provider) as Arc<dyn TableProvider>)
            .expect("register");
        Table {
            name: name.to_string(),
            data_prefix,
            data_url,
            provider,
            ctx,
        }
    }

    /// A fresh catalog over the same store (which fences the previous one) and
    /// `table` opened over it in a fresh session.
    async fn reopen(&self, table: Table) -> Table {
        let Table {
            name,
            data_prefix,
            data_url,
            provider,
            ctx,
        } = table;
        provider
            .drain_in_flight_maintenance()
            .await
            .expect("drain maintenance");
        drop((provider, ctx));
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(Self::open_catalog(&self.store).await);
        *self.catalog.write() = Arc::clone(&catalog);
        let ctx = SessionContext::new();
        let provider = Arc::new(
            self.builder(catalog, &ctx, &data_url)
                .open(&name)
                .await
                .expect("reopen the table"),
        );
        ctx.register_table(
            name.as_str(),
            Arc::clone(&provider) as Arc<dyn TableProvider>,
        )
        .expect("register");
        Table {
            name,
            data_prefix,
            data_url,
            provider,
            ctx,
        }
    }

    async fn current_snapshot(&self, name: &str) -> (String, String) {
        let meta = self
            .catalog()
            .get_table(name)
            .await
            .expect("table metadata");
        (meta.table_id, meta.current_snapshot_id)
    }

    /// Every file name the table's manifest names, in any snapshot.
    async fn manifest_names(&self, table_id: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .catalog()
            .get_all_snapshot_files(table_id)
            .await
            .expect("manifest")
            .into_iter()
            .map(|f| f.file_path)
            .collect();
        names.sort();
        names
    }

    /// The files the committed manifest of `snapshot_id` names.
    async fn snapshot_manifest_names(&self, table_id: &str, snapshot_id: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .catalog()
            .get_snapshot_files(table_id, snapshot_id)
            .await
            .expect("manifest")
            .into_iter()
            .map(|f| f.file_path)
            .collect();
        names.sort();
        names
    }

    /// `.vortex` names directly under one snapshot directory of `table`.
    async fn listed(&self, table: &Table, table_id: &str, snapshot_id: &str) -> Vec<String> {
        let dir = format!("{}/{table_id}/{snapshot_id}", table.data_prefix);
        let mut names: Vec<String> = self
            .raw
            .list(Some(&Path::from(dir.as_str())))
            .try_collect::<Vec<_>>()
            .await
            .expect("list a snapshot directory")
            .into_iter()
            .filter_map(|meta| {
                let key = meta.location.to_string();
                let rest = key.strip_prefix(&format!("{dir}/"))?.to_string();
                (!rest.contains('/') && rest.ends_with(".vortex")).then_some(rest)
            })
            .collect();
        names.sort();
        names
    }

    /// Writes the orphan ids through a donor table, then copies its data file
    /// into `table`'s current snapshot directory under a name no commit names.
    async fn plant_orphan(&self, table: &Table) -> String {
        let donor = self.create(&format!("{}_donor", table.name), 1_000).await;
        donor.insert(ORPHAN_FIRST, ORPHAN_LAST).await;
        donor
            .provider
            .drain_in_flight_maintenance()
            .await
            .expect("drain");
        let (donor_id, donor_snapshot) = self.current_snapshot(&donor.name).await;
        let donor_files = self.listed(&donor, &donor_id, &donor_snapshot).await;
        assert_eq!(
            donor_files.len(),
            1,
            "the donor wrote one data file: {donor_files:?}"
        );
        let bytes = self
            .raw
            .get(&Path::from(format!(
                "{}/{donor_id}/{donor_snapshot}/{}",
                donor.data_prefix, donor_files[0]
            )))
            .await
            .expect("read the donor file")
            .bytes()
            .await
            .expect("bytes");
        let (table_id, snapshot) = self.current_snapshot(&table.name).await;
        let orphan = format!("orphan-{}.vortex", uuid::Uuid::now_v7().simple());
        self.raw
            .put(
                &Path::from(format!(
                    "{}/{table_id}/{snapshot}/{orphan}",
                    table.data_prefix
                )),
                bytes.into(),
            )
            .await
            .expect("plant the file");
        assert!(
            self.listed(table, &table_id, &snapshot)
                .await
                .contains(&orphan),
            "the planted file is in the live snapshot directory"
        );
        orphan
    }

    /// Asserts the planted file is in no manifest and no scan shows its rows.
    async fn assert_not_adopted(&self, table: &Table, orphan: &str, path: &str) {
        let (table_id, _) = self.current_snapshot(&table.name).await;
        assert!(
            !self
                .manifest_names(&table_id)
                .await
                .contains(&orphan.to_string()),
            "{} {path}: a manifest names the planted file {orphan}",
            self.label
        );
        assert_eq!(
            table.orphan_rows_visible().await,
            0,
            "{} {path}: a scan shows the planted file's rows",
            self.label
        );
    }
}

impl Table {
    /// Appends `first..=last` through `insert_into`, one data file per call.
    async fn insert(&self, first: i64, last: i64) {
        let ids: Vec<i64> = (first..=last).collect();
        let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("batch");
        common::insert_batch(&self.provider, batch)
            .await
            .expect("insert");
    }

    async fn count_and_sum(&self) -> (i64, i64) {
        let batches = self
            .ctx
            .sql(&format!(
                "SELECT count(*), coalesce(sum(v), 0) FROM {}",
                self.name
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let batch = &batches[0];
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0);
        let sum = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum")
            .value(0);
        (count, sum)
    }

    /// Rows with an orphan id, by a scan that reads every file.
    async fn orphan_rows_visible(&self) -> i64 {
        let batches = self
            .ctx
            .sql(&format!(
                "SELECT count(*) FROM {} WHERE id >= {ORPHAN_FIRST}",
                self.name
            ))
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
    }
}

/// Calls the small-file compaction entry until it reports a pass, within a bound.
async fn try_compact(table: &Table) -> bool {
    for _ in 0..40 {
        match table.provider.compact_current_snapshot_small_files().await {
            Ok(true) => return true,
            Ok(false) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(e) => panic!("compaction failed: {e}"),
        }
    }
    false
}

/// A scan right after planting, and the next write with its maintenance.
async fn after_a_write(rig: &Rig) {
    let table = rig.create("p_write", 1_000).await;
    table.insert(1, 1000).await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let orphan = rig.plant_orphan(&table).await;
    rig.assert_not_adopted(&table, &orphan, "scan after planting")
        .await;
    rig.counting.watch(&table.data_prefix);
    table.insert(1001, 1200).await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    assert_eq!(
        rig.counting.lists(),
        0,
        "{}: an append and its maintenance listed the data prefix",
        rig.label
    );
    // Each data file is written once, at its final key, and committed by name.
    assert_eq!(
        rig.counting.copies(),
        0,
        "{}: an append and its maintenance copied a data file",
        rig.label
    );
    assert_eq!(
        rig.counting.staged_writes(),
        0,
        "{}: an append wrote into a staging directory",
        rig.label
    );
    rig.assert_not_adopted(&table, &orphan, "write and its maintenance")
        .await;
    assert_eq!(table.count_and_sum().await, (1200, expected_sum(1, 1200)));
}

/// Subset compaction picks its inputs from the committed file set: the planted
/// file is not merged, and every committed row is in its output.
async fn subset_compaction(rig: &Rig) {
    let table = rig.create("p_compact", 3).await;
    for i in 0..5 {
        table.insert(i * 2000 + 1, (i + 1) * 2000).await;
    }
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let orphan = rig.plant_orphan(&table).await;
    let (table_id, old_snapshot) = rig.current_snapshot(&table.name).await;
    let mut last = 10_000;
    let mut compacted = try_compact(&table).await;
    if !compacted {
        // The background pass spent the one-shot credit; one more append restores it.
        table.insert(10_001, 12_000).await;
        last = 12_000;
        compacted = try_compact(&table).await;
    }
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let (_, new_snapshot) = rig.current_snapshot(&table.name).await;
    assert!(
        compacted && new_snapshot != old_snapshot,
        "{}: the compaction ran and published a new snapshot",
        rig.label
    );
    rig.assert_not_adopted(&table, &orphan, "subset compaction")
        .await;
    assert_eq!(
        table.count_and_sum().await,
        (last, expected_sum(1, last)),
        "{}: subset compaction kept exactly the committed rows",
        rig.label
    );
    // The snapshot the compaction published; the one it retired keeps its rows
    // until the retirement sweep deletes its files.
    let mut committed = rig.snapshot_manifest_names(&table_id, &new_snapshot).await;
    assert!(
        !committed.is_empty(),
        "{}: the published snapshot names its files",
        rig.label
    );
    committed.retain(|name| !name.starts_with("orphan-"));
    let listed = rig.listed(&table, &table_id, &new_snapshot).await;
    for name in &committed {
        assert!(
            listed.contains(name),
            "{}: committed file {name} is missing",
            rig.label
        );
    }
}

/// An empty committed file set scans as empty, before and after a reopen.
async fn empty_committed_set(rig: &Rig) {
    let table = rig.create("p_empty", 1_000).await;
    table.insert(1, 1000).await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let orphan = rig.plant_orphan(&table).await;
    let (table_id, _) = rig.current_snapshot(&table.name).await;
    rig.catalog()
        .clear_snapshot_files_except(&table_id, "no-such-snapshot")
        .await
        .expect("empty the committed file set");
    assert_eq!(
        table.count_and_sum().await,
        (0, 0),
        "{}: empty set, same session",
        rig.label
    );
    let table = rig.reopen(table).await;
    assert_eq!(
        table.count_and_sum().await,
        (0, 0),
        "{}: empty set after a reopen",
        rig.label
    );
    rig.assert_not_adopted(&table, &orphan, "empty committed set and reopen")
        .await;
}

/// A reopen with a populated manifest, then a write after it.
async fn reopen(rig: &Rig) {
    let table = rig.create("p_reopen", 1_000).await;
    table.insert(1, 1000).await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let orphan = rig.plant_orphan(&table).await;
    let table = rig.reopen(table).await;
    rig.assert_not_adopted(&table, &orphan, "reopen").await;
    table.insert(1001, 2000).await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    rig.assert_not_adopted(&table, &orphan, "write after a reopen")
        .await;
    assert_eq!(table.count_and_sum().await, (2000, expected_sum(1, 2000)));
}

/// The retired-directory sweep after a compaction: no committed file goes, and
/// the planted file in the live directory is not adopted.
async fn sweeps(rig: &Rig) {
    let table = rig.create("p_sweep", 3).await;
    for i in 0..5 {
        table.insert(i * 2000 + 1, (i + 1) * 2000).await;
    }
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    let orphan = rig.plant_orphan(&table).await;
    let (table_id, _) = rig.current_snapshot(&table.name).await;
    let mut last = 10_000;
    if !try_compact(&table).await {
        table.insert(10_001, 12_000).await;
        last = 12_000;
        assert!(
            try_compact(&table).await,
            "{}: the compaction ran",
            rig.label
        );
    }
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    table.provider.sweep_retired_snapshot_dirs_now().await;
    table
        .provider
        .drain_in_flight_maintenance()
        .await
        .expect("drain");
    rig.assert_not_adopted(&table, &orphan, "sweeps").await;
    let (_, current) = rig.current_snapshot(&table.name).await;
    let listed = rig.listed(&table, &table_id, &current).await;
    let committed = rig
        .catalog()
        .get_snapshot_files(&table_id, &current)
        .await
        .expect("manifest");
    for file in committed {
        assert!(
            listed.contains(&file.file_path),
            "{}: the sweep removed committed file {}",
            rig.label,
            file.file_path
        );
    }
    assert_eq!(table.count_and_sum().await, (last, expected_sum(1, last)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planted_file_is_not_adopted_after_a_write() {
    after_a_write(&Rig::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planted_file_is_not_merged_by_subset_compaction() {
    subset_compaction(&Rig::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planted_file_is_not_adopted_by_an_empty_committed_set() {
    empty_committed_set(&Rig::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planted_file_is_not_adopted_at_reopen() {
    reopen(&Rig::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planted_file_is_not_adopted_by_the_sweeps() {
    sweeps(&Rig::new().await).await;
}

/// Control on the trunk path (SQLite metastore, data not in the location): the
/// post-write rebuild lists the snapshot directory and adopts the planted file,
/// so the planted file is valid data and the placed tests above are not vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_the_listing_path_adopts_a_planted_file() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let provider = Arc::new(
        fixture
            .data_store
            .builder(Arc::clone(&catalog), ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: "control".to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_store.base_path.clone(),
                partition_column: None,
                vortex_config: vortex_config(1_000),
            })
            .await
            .expect("create"),
    );
    ctx.register_table("control", Arc::clone(&provider) as Arc<dyn TableProvider>)
        .expect("register");
    let table = Table {
        name: "control".to_string(),
        data_prefix: String::new(),
        data_url: String::new(),
        provider: Arc::clone(&provider),
        ctx,
    };
    table.insert(1, 1000).await;
    provider.drain_in_flight_maintenance().await.expect("drain");
    // The donor's file: same schema, orphan ids, written by the same fixture.
    let donor_ctx = SessionContext::new();
    let donor = Arc::new(
        fixture
            .data_store
            .builder(Arc::clone(&catalog), donor_ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: "control_donor".to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_store.base_path.clone(),
                partition_column: None,
                vortex_config: vortex_config(1_000),
            })
            .await
            .expect("create the donor"),
    );
    let donor_table = Table {
        name: "control_donor".to_string(),
        data_prefix: String::new(),
        data_url: String::new(),
        provider: Arc::clone(&donor),
        ctx: donor_ctx,
    };
    donor_table.insert(ORPHAN_FIRST, ORPHAN_LAST).await;
    donor.drain_in_flight_maintenance().await.expect("drain");
    let donor_meta = catalog.get_table("control_donor").await.expect("donor");
    let donor_files = donor
        .list_snapshot_files_with_sizes(&donor_meta.current_snapshot_id)
        .await
        .expect("donor files");
    let meta = catalog.get_table("control").await.expect("table");
    let orphan = format!("orphan-{}.vortex", uuid::Uuid::now_v7().simple());
    match &fixture.data_store.config {
        None => {
            let from = fixture
                .data_path
                .join(&donor_meta.table_id)
                .join(&donor_meta.current_snapshot_id)
                .join(&donor_files[0].0);
            let to = fixture
                .data_path
                .join(&meta.table_id)
                .join(&meta.current_snapshot_id)
                .join(&orphan);
            tokio::fs::copy(&from, &to).await.expect("plant");
        }
        Some(config) => {
            let prefix = config.url.path().trim_matches('/').to_string();
            let bytes = config
                .store
                .get(&Path::from(format!(
                    "{prefix}/{}/{}/{}",
                    donor_meta.table_id, donor_meta.current_snapshot_id, donor_files[0].0
                )))
                .await
                .expect("read the donor file")
                .bytes()
                .await
                .expect("bytes");
            config
                .store
                .put(
                    &Path::from(format!(
                        "{prefix}/{}/{}/{orphan}",
                        meta.table_id, meta.current_snapshot_id
                    )),
                    bytes.into(),
                )
                .await
                .expect("plant");
        }
    }
    table.insert(1001, 1200).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while table.orphan_rows_visible().await == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        table.orphan_rows_visible().await,
        ORPHAN_LAST - ORPHAN_FIRST + 1,
        "the trunk path's listing adopts the planted file"
    );
}
