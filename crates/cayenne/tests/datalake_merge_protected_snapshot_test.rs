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

//! A datalake merge can run for a minute against a remote store. Appends keep
//! landing as small protected snapshots meanwhile, and every scan lists each
//! one, so the protected-snapshot merge has to keep running for as long as the
//! datalake merge does.
//!
//! The datalake store here parks the merge's first upload until the test
//! releases it, which holds the merge in flight for as long as the test needs.
//! Appends upsert new keys and keys that live in the datalake while it is
//! parked, through the real write-driven and background compaction triggers.
//! The protected set must be merged back down while the datalake merge is
//! still parked, and every answer afterwards must match an in-memory oracle of
//! the same writes exactly.

use crate::common;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Notify;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "datalake_merge_protected_t";

/// Datalake generations written before the merge; at least the four a merge
/// needs.
const GENERATIONS: i64 = 5;

/// Upserts that land while the datalake merge is parked. Each one publishes
/// its own protected snapshot.
const PARKED_APPENDS: i64 = 40;

/// The protected-snapshot count the merge has to hold the table to while the
/// datalake merge is parked: a few size tiers' worth of unmerged runs, far
/// below one snapshot per parked append.
const PROTECTED_BOUND: usize = 16;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// The datalake is the local `file://` store, so it is reached through the
/// session's object store registry, where [`ParkedUploadStore`] sits.
fn table_options(fixture: &common::TestFixture, cold_dir: &std::path::Path) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cluster_by: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            // Every promotion and datalake merge is an explicit call below.
            cold_tier_background_interval_ms: 0,
            cold_tier_gc_interval_ms: 300_000,
            // The background compactor runs the same trigger it runs in a
            // server, on a short interval.
            compaction_background_interval_ms: 50,
            // Every write lands in a snapshot file, so each upsert publishes a
            // protected snapshot rather than inline rows.
            inline_max_rows: 0,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    }
}

/// When armed, the first upload into the datalake directory waits until the
/// test releases it. Everything else, including reads of datalake files and
/// every warm-tier write, goes straight through.
#[derive(Debug)]
struct ParkedUploadStore {
    inner: Arc<dyn ObjectStore>,
    datalake_prefix: String,
    armed: AtomicBool,
    parked: Arc<Notify>,
    release: Arc<Notify>,
}

impl ParkedUploadStore {
    /// Registers a new, disarmed store as `ctx`'s `file://` store.
    fn install(ctx: &SessionContext, datalake_dir: &std::path::Path) -> TestResult<Arc<Self>> {
        let store = Arc::new(Self {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            datalake_prefix: datalake_dir
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string(),
            armed: AtomicBool::new(false),
            parked: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        ctx.runtime_env().register_object_store(
            &url::Url::parse("file:///")?,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        Ok(store)
    }

    /// Parks the caller when this is the armed store's first datalake upload.
    async fn park_datalake_upload(&self, location: &Path) {
        if location.as_ref().starts_with(self.datalake_prefix.as_str())
            && self.armed.swap(false, Ordering::AcqRel)
        {
            self.parked.notify_one();
            self.release.notified().await;
        }
    }
}

impl fmt::Display for ParkedUploadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ParkedUploadStore")
    }
}

#[async_trait]
impl ObjectStore for ParkedUploadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.park_datalake_upload(location).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.park_datalake_upload(location).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
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
}

/// Upsert `rows` and record them in the oracle.
async fn upsert(
    table: &CayenneTableProvider,
    oracle: &mut BTreeMap<i64, i64>,
    rows: &[(i64, i64)],
) -> TestResult<()> {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    for (id, value) in rows {
        oracle.insert(*id, *value);
    }
    Ok(())
}

async fn protected_snapshot_ids(
    fixture: &common::TestFixture,
    table: &CayenneTableProvider,
) -> TestResult<Vec<String>> {
    Ok(fixture
        .catalog
        .get_all_snapshot_sequences(table.table_id())
        .await?
        .into_keys()
        .collect())
}

async fn datalake_generations(
    fixture: &common::TestFixture,
    table: &CayenneTableProvider,
) -> TestResult<usize> {
    Ok(fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|file| file.max_sequence)
        .collect::<std::collections::BTreeSet<_>>()
        .len())
}

/// Every `(id, value)` row the table returns, in key order.
async fn select_rows(ctx: &SessionContext) -> TestResult<Vec<(i64, i64)>> {
    let batches = ctx
        .sql(&format!("SELECT id, value FROM {TABLE} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("value is not Int64")?;
        rows.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    Ok(rows)
}

async fn assert_matches_oracle(
    ctx: &SessionContext,
    oracle: &BTreeMap<i64, i64>,
    stage: &str,
) -> TestResult<()> {
    let expected: Vec<(i64, i64)> = oracle.iter().map(|(id, value)| (*id, *value)).collect();
    let actual = select_rows(ctx).await?;
    if actual != expected {
        return Err(format!(
            "{stage}: the table returned {} rows, the oracle holds {}; first difference at {:?}",
            actual.len(),
            expected.len(),
            actual
                .iter()
                .zip(expected.iter())
                .find(|(a, e)| a != e)
                .map(|(a, e)| format!("table {a:?} vs oracle {e:?}"))
        )
        .into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_snapshots_keep_merging_while_a_datalake_merge_runs() -> TestResult<()> {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let ctx = SessionContext::new();
    let store = ParkedUploadStore::install(&ctx, &cold_dir)?;
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(table_options(&fixture, &cold_dir))
        .await?,
    );
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let mut oracle: BTreeMap<i64, i64> = BTreeMap::new();

    // Five datalake generations: each adds a hundred new keys and supersedes
    // keys 0..20 of the generation before, so tombstones hide datalake rows.
    for generation in 0..GENERATIONS {
        let rows: Vec<(i64, i64)> = (0..20)
            .map(|id| (id, generation * 1_000 + id))
            .chain((100 + generation * 100..200 + generation * 100).map(|id| (id, id * 2)))
            .collect();
        upsert(&table, &mut oracle, &rows).await?;
        let _ = table.checkpoint_inlined_data().await;
        let _ = table.checkpoint_mem_tier().await;
        assert!(
            table.promote_warm_to_cold().await?,
            "promotion {generation} moves the warm tier to the datalake"
        );
    }
    assert_eq!(
        datalake_generations(&fixture, &table).await?,
        usize::try_from(GENERATIONS)?,
        "each promotion leaves its own datalake generation"
    );
    assert_matches_oracle(&ctx, &oracle, "before the datalake merge").await?;

    assert!(
        table.spawn_background_compaction(Arc::new(tokio::sync::Semaphore::new(1))),
        "the background compactor must be running for this test to mean anything"
    );

    // Park the datalake merge at its first upload.
    store.armed.store(true, Ordering::Release);
    let merging = Arc::clone(&table);
    let merge = tokio::spawn(async move { merging.compact_cold_tier().await });
    tokio::time::timeout(Duration::from_secs(60), store.parked.notified())
        .await
        .map_err(|_| "the datalake merge never reached its upload within 60 s")?;

    // Appends while the merge is parked: ten new keys each, plus one key that
    // lives in the datalake, which leaves a tombstone newer than every input
    // the merge read. Every protected snapshot seen after an append is
    // recorded, which shows the appends did publish one each.
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for append in 0..PARKED_APPENDS {
        let rows: Vec<(i64, i64)> = (10_000 + append * 10..10_010 + append * 10)
            .map(|id| (id, id + 7))
            .chain(std::iter::once((append % 30, 50_000 + append)))
            .collect();
        upsert(&table, &mut oracle, &rows).await?;
        seen.extend(protected_snapshot_ids(&fixture, &table).await?);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        seen.len() >= usize::try_from(PARKED_APPENDS)?,
        "precondition: {PARKED_APPENDS} appends published only {} protected snapshots",
        seen.len()
    );

    // The protected set has to come back down while the merge is still parked.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut protected = protected_snapshot_ids(&fixture, &table).await?.len();
    while protected > PROTECTED_BOUND && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        protected = protected_snapshot_ids(&fixture, &table).await?.len();
    }
    let merge_in_flight = !merge.is_finished();
    assert!(
        merge_in_flight,
        "the datalake merge finished before it was released, so nothing here ran beside it"
    );
    assert!(
        protected <= PROTECTED_BOUND,
        "{PARKED_APPENDS} appends during a datalake merge left {protected} protected snapshots \
         after 30 s (bound {PROTECTED_BOUND}; {} published): the protected-snapshot merge did \
         not run while the datalake merge was in flight",
        seen.len()
    );
    assert_matches_oracle(&ctx, &oracle, "while the datalake merge is parked").await?;

    store.release.notify_one();
    let merged = tokio::time::timeout(Duration::from_secs(60), merge)
        .await
        .map_err(|_| "the datalake merge did not finish within 60 s of its release")??;
    assert!(merged?, "the datalake merge commits after its release");
    assert_eq!(
        datalake_generations(&fixture, &table).await?,
        1,
        "the merge folds every generation into one"
    );

    table.flush_pending_maintenance().await?;
    assert_matches_oracle(&ctx, &oracle, "after the datalake merge").await?;
    Ok(())
}
