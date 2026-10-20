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

//! A listing of a snapshot directory through `DataFusion`'s shared list-files
//! cache writes what it read into the entry every later scan plans from. A
//! secondary-index build that lists the current snapshot without the listing
//! fence can lose the race to a writer that changes the directory and evicts
//! its entry, then cache its own, older listing: a file retention unlinked
//! stays listed, and a file an append published goes missing.
//!
//! Each case pauses the build's LIST after it has read the directory, runs the
//! writer, then lets the build finish and queries the table. A build that lists
//! under the fence holds the writer off until its listing is cached.

use crate::common;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, ScanViewReuse,
    TimeRetentionFilterBuilder,
};
use common::TestFixture;
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col, lit};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Notify;

test_with_backends!(index_build_listing_racing_retention);
test_with_backends!(index_build_listing_before_retention);
test_with_backends!(index_build_listing_racing_retention_then_cache_cleared);
test_with_backends!(index_build_listing_racing_retention_unscanned);
test_with_backends!(index_build_listing_racing_append);
test_with_backends!(index_build_listing_before_append);
test_with_backends!(query_protected_listing_racing_retention);
test_with_backends!(query_protected_listing_before_retention);

/// The race: the index build LISTs, retention unlinks and evicts, then the
/// build writes its listing into the cache.
async fn index_build_listing_racing_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Retention, true, false, true).await
}

/// The race on a table no query has scanned yet, so no per-file statistics
/// were collected for the file retention unlinks.
async fn index_build_listing_racing_retention_unscanned(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Retention, true, false, false).await
}

/// Perturbation: the race, then the list-files cache is cleared before the
/// queries. Passing here pins the failure on the cached listing.
async fn index_build_listing_racing_retention_then_cache_cleared(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Retention, true, true, true).await
}

/// Control: the same steps with the index build finished before retention.
async fn index_build_listing_before_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Retention, false, false, true).await
}

/// The race with an append in place of retention: the build LISTs, the append
/// publishes a new file into the directory, then the build caches a listing
/// without it.
async fn index_build_listing_racing_append(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Append, true, false, true).await
}

/// Control: the index build finished before the append.
async fn index_build_listing_before_append(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Writer::Append, false, false, true).await
}

/// The write that changes the current snapshot directory while the index
/// build's listing is paused.
#[derive(Clone, Copy, Debug)]
enum Writer {
    /// File-based retention, as the runtime runs it: `delete_from` on the
    /// accelerator with the `time_column < threshold` filter and no scan of
    /// the table first. Unlinks the file holding `event_time = 1`.
    Retention,
    /// An `INSERT` that publishes one new file holding `event_time = 6`.
    Append,
}

impl Writer {
    /// What scans read once the write has landed, before and after a later
    /// append of `event_time = 5`.
    fn expected(self) -> Vec<Result<Vec<i64>, String>> {
        match self {
            Self::Retention => vec![Ok(vec![3, 3]), Ok(vec![3, 3, 5])],
            Self::Append => vec![Ok(vec![1, 1, 3, 3, 6]), Ok(vec![1, 1, 3, 3, 5, 6])],
        }
    }

    /// How the number of data files in the table changes.
    fn file_delta(self) -> isize {
        match self {
            Self::Retention => -1,
            Self::Append => 1,
        }
    }

    async fn write(
        self,
        ctx: SessionContext,
        table: Arc<CayenneTableProvider>,
    ) -> Result<(), String> {
        match self {
            Self::Retention => {
                let retention = table
                    .delete_from(
                        &ctx.state(),
                        vec![
                            col("event_time")
                                .lt(lit(ScalarValue::TimestampNanosecond(Some(2), None))),
                        ],
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                collect(retention, ctx.task_ctx())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Self::Append => {
                ctx.sql("INSERT INTO t VALUES (6, to_timestamp_nanos(6))")
                    .await
                    .map_err(|e| e.to_string())?
                    .collect()
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }
}

async fn run(
    fixture: TestFixture,
    writer: Writer,
    race: bool,
    clear_listing_cache: bool,
    scan_before: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    assert!(
        ctx.runtime_env()
            .cache_manager
            .get_list_files_cache()
            .is_some(),
        "premise: the default runtime env caches directory listings, as spiced's does"
    );
    let store = Arc::new(PausedListStore {
        inner: Arc::new(object_store::local::LocalFileSystem::new()),
        armed: Arc::new(AtomicBool::new(false)),
        only: parking_lot::Mutex::new(None),
        listed: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    });
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file:///")?,
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let table = retention_table(&fixture, &ctx).await?;
    ctx.register_table("t", Arc::clone(&table) as _)?;
    for sql in [
        "INSERT INTO t VALUES (1, to_timestamp_nanos(1)), (2, to_timestamp_nanos(1))",
        "INSERT INTO t VALUES (3, to_timestamp_nanos(3)), (4, to_timestamp_nanos(3))",
    ] {
        ctx.sql(sql).await?.collect().await?;
    }
    let select = "SELECT event_time FROM t ORDER BY event_time";
    if scan_before {
        assert_eq!(
            timestamps(&ctx.sql(select).await?.collect().await?),
            vec![1, 1, 3, 3]
        );
    }
    let vortex_files_before = count_vortex_files(&fixture.data_path);

    // The directory's listing is not cached, as after any evicting publish or
    // an LRU eviction: the next lister reads the store and caches what it read.
    ctx.runtime_env()
        .cache_manager
        .get_list_files_cache()
        .expect("list-files cache")
        .clear();

    store.armed.store(race, Ordering::Release);
    let build_table = Arc::clone(&table);
    let build = tokio::spawn(async move { build_table.catch_up_lookup_index().await });
    let pending_build = if race {
        tokio::time::timeout(Duration::from_secs(10), store.listed.notified()).await?;
        Some(build)
    } else {
        build.await?;
        None
    };

    let mut write = tokio::spawn(writer.write(ctx.clone(), Arc::clone(&table)));
    // While the paused listing holds the fence, the writer must wait for it.
    // Keep the task alive when the timeout expires.
    let (writer_waited_for_listing, written) = match pending_build {
        Some(build) => {
            let while_listing = tokio::time::timeout(Duration::from_millis(500), &mut write).await;
            store.release.notify_one();
            build.await?;
            match while_listing {
                Ok(written) => (false, written),
                Err(_) => (true, write.await),
            }
        }
        None => (false, write.await),
    };
    written??;
    let vortex_files_after = count_vortex_files(&fixture.data_path);
    assert_eq!(
        vortex_files_after.cast_signed() - vortex_files_before.cast_signed(),
        writer.file_delta(),
        "premise: {writer:?} changed exactly one data file"
    );

    if clear_listing_cache {
        ctx.runtime_env()
            .cache_manager
            .get_list_files_cache()
            .expect("list-files cache")
            .clear();
    }

    // Every query from here on, including after a later append.
    let mut outcomes = Vec::new();
    outcomes.push(query(&ctx, select).await);
    ctx.sql("INSERT INTO t VALUES (5, to_timestamp_nanos(5))")
        .await?
        .collect()
        .await?;
    outcomes.push(query(&ctx, select).await);
    eprintln!(
        "writer={writer:?} race={race} writer_waited_for_listing={writer_waited_for_listing} \
         outcomes={outcomes:#?}"
    );
    assert_eq!(
        outcomes,
        writer.expected(),
        "scans after the write must read exactly the files the directory holds"
    );
    assert_eq!(
        writer_waited_for_listing, race,
        "a write must wait for an in-flight listing of the directory it changes"
    );
    Ok(())
}

/// The race on the query path: a query plans its protected-snapshot branch,
/// listing that snapshot's directory, while retention unlinks one of its files.
async fn query_protected_listing_racing_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run_protected(fixture, true).await
}

/// Control: the query finished before retention.
async fn query_protected_listing_before_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run_protected(fixture, false).await
}

async fn run_protected(fixture: TestFixture, race: bool) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let store = Arc::new(PausedListStore {
        inner: Arc::new(object_store::local::LocalFileSystem::new()),
        armed: Arc::new(AtomicBool::new(false)),
        only: parking_lot::Mutex::new(None),
        listed: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    });
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file:///")?,
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let table = upsert_retention_table(&fixture, &ctx).await?;
    ctx.register_table("t", Arc::clone(&table) as _)?;
    let select = "SELECT event_time FROM t ORDER BY event_time";
    ctx.sql("INSERT INTO t VALUES (1, to_timestamp_nanos(3))")
        .await?
        .collect()
        .await?;
    let base_snapshot = current_snapshot_dir(&fixture, &table).await?;
    // One upsert writes a protected snapshot of two files, one of them all
    // expired: retention unlinks that file and keeps the snapshot. (The first
    // insert's file is a protected snapshot of its own.)
    let batch = |id: i64, ts: i64| {
        RecordBatch::try_new(
            table.schema(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![id])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![ts])),
            ],
        )
    };
    common::insert_batches(table.as_ref(), vec![batch(2, 1)?, batch(3, 3)?]).await?;
    assert_eq!(
        timestamps(&ctx.sql(select).await?.collect().await?),
        vec![1, 3, 3]
    );
    let protected = protected_snapshot_dirs(&fixture, &table, &base_snapshot)?;
    let two_files: Vec<_> = protected.iter().filter(|(_, files)| *files == 2).collect();
    assert_eq!(
        (protected.len(), two_files.len()),
        (2, 1),
        "premise: two protected snapshots, the upsert's of two files, got {protected:?}"
    );
    let upserted = two_files[0].0.clone();

    // Every directory's listing is uncached, and the pause lands on the query's
    // listing of the upserted snapshot's directory.
    ctx.runtime_env()
        .cache_manager
        .get_list_files_cache()
        .expect("list-files cache")
        .clear();
    *store.only.lock() = upserted.rsplit('/').next().map(str::to_string);
    store.armed.store(race, Ordering::Release);
    let query_ctx = ctx.clone();
    let planning = tokio::spawn(async move { query(&query_ctx, select).await });
    let pending_query = if race {
        tokio::time::timeout(Duration::from_secs(10), store.listed.notified()).await?;
        Some(planning)
    } else {
        planning
            .await?
            .map_err(|e| format!("query before retention: {e}"))?;
        None
    };

    let mut write = tokio::spawn(Writer::Retention.write(ctx.clone(), Arc::clone(&table)));
    let (writer_waited_for_listing, written) = match pending_query {
        Some(planning) => {
            let while_listing = tokio::time::timeout(Duration::from_millis(500), &mut write).await;
            store.release.notify_one();
            // The racing query's own answer may predate retention either way.
            let _ = planning.await?;
            match while_listing {
                Ok(written) => (false, written),
                Err(_) => (true, write.await),
            }
        }
        None => (false, write.await),
    };
    written??;
    let after = protected_snapshot_dirs(&fixture, &table, &base_snapshot)?;
    assert!(
        after.contains(&(upserted.clone(), 1)) && after.len() == 2,
        "premise: retention unlinked one of the upserted snapshot's files, got {after:?}"
    );

    let mut outcomes = Vec::new();
    outcomes.push(query(&ctx, select).await);
    ctx.sql("INSERT INTO t VALUES (5, to_timestamp_nanos(5))")
        .await?
        .collect()
        .await?;
    outcomes.push(query(&ctx, select).await);
    eprintln!(
        "protected race={race} writer_waited_for_listing={writer_waited_for_listing} \
         outcomes={outcomes:#?}"
    );
    assert_eq!(
        outcomes,
        vec![Ok(vec![3, 3]), Ok(vec![3, 3, 5])],
        "scans after retention must read only the protected snapshot's remaining file"
    );
    assert_eq!(
        writer_waited_for_listing, race,
        "retention must wait for a query's in-flight protected-snapshot listing"
    );
    Ok(())
}

/// The current snapshot's directory, as an object-store path prefix.
async fn current_snapshot_dir(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = fixture
        .catalog
        .get_table(&table.metadata().table_name)
        .await?;
    let dir = fixture
        .data_path
        .join(&metadata.table_id)
        .join(&metadata.current_snapshot_id);
    Ok(Path::from_filesystem_path(dir)?.to_string())
}

/// Every snapshot directory other than `base` that holds data files, with its
/// count of them.
fn protected_snapshot_dirs(
    fixture: &TestFixture,
    table: &CayenneTableProvider,
    base: &str,
) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join(&table.metadata().table_id);
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(table_dir)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let files = count_vortex_files(&path);
        let dir = Path::from_filesystem_path(&path)?.to_string();
        if files > 0 && dir != base {
            dirs.push((dir, files));
        }
    }
    dirs.sort();
    Ok(dirs)
}

async fn upsert_retention_table(
    fixture: &TestFixture,
    ctx: &SessionContext,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        // Every scan captures its own view, so each query lists the directories.
        .with_scan_view_reuse(ScanViewReuse::WithinLag(Duration::ZERO))
        .with_time_retention_filter_builder(TimeRetentionFilterBuilder::try_new(
            "event_time",
            u64::MAX,
            &schema,
        )?)
        .create(CreateTableOptions {
            table_name: "listing_cache_protected_race".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(
                datafusion_table_providers::util::on_conflict::OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                ),
            ),
            base_path: fixture.data_path.to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                write_concurrency: Some(2),
                compaction_background_interval_ms: 0,
                compaction_trigger_files: usize::MAX,
                compaction_trigger_protected_snapshots: usize::MAX,
                compaction_trigger_snapshot_age_ms: u64::MAX,
                ..VortexConfig::default()
            },
        })
        .await?,
    ))
}

async fn query(ctx: &SessionContext, sql: &str) -> Result<Vec<i64>, String> {
    match ctx.sql(sql).await {
        Ok(df) => df
            .collect()
            .await
            .map(|batches| timestamps(&batches))
            .map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn count_vortex_files(dir: &std::path::Path) -> usize {
    let mut count = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('_'))
                {
                    stack.push(path);
                }
            } else if path.extension().is_some_and(|ext| ext == "vortex") {
                count += 1;
            }
        }
    }
    count
}

async fn retention_table(
    fixture: &TestFixture,
    ctx: &SessionContext,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .with_secondary_indexes(vec![vec!["id".to_string()]])
        .with_time_retention_filter_builder(TimeRetentionFilterBuilder::try_new(
            "event_time",
            u64::MAX,
            &schema,
        )?)
        .create(CreateTableOptions {
            table_name: "listing_cache_retention_race".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                compaction_background_interval_ms: 0,
                compaction_trigger_files: usize::MAX,
                compaction_trigger_protected_snapshots: usize::MAX,
                compaction_trigger_snapshot_age_ms: u64::MAX,
                ..VortexConfig::default()
            },
        })
        .await?,
    ))
}

fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
    use arrow::array::AsArray;
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

/// When armed, the next LIST reads the whole listing from the store, then
/// holds it until released before handing it to the caller. When `only` is
/// set, a LIST whose prefix does not name it goes through without pausing or
/// disarming.
#[derive(Debug)]
struct PausedListStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
    only: parking_lot::Mutex<Option<String>>,
    listed: Arc<Notify>,
    release: Arc<Notify>,
}

impl fmt::Display for PausedListStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PausedListStore")
    }
}

#[async_trait]
impl ObjectStore for PausedListStore {
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

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let elsewhere = self.only.lock().as_ref().is_some_and(|only| {
            !prefix.is_some_and(|prefix| prefix.as_ref().contains(only.as_str()))
        });
        if elsewhere || !self.armed.swap(false, Ordering::AcqRel) {
            return self.inner.list(prefix);
        }
        let listing = self.inner.list(prefix);
        let listed = Arc::clone(&self.listed);
        let release = Arc::clone(&self.release);
        futures::stream::once(async move {
            let items: Vec<_> = listing.collect().await;
            listed.notify_one();
            release.notified().await;
            futures::stream::iter(items)
        })
        .flatten()
        .boxed()
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
