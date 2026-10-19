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

//! A secondary-index build lists the current snapshot directory without the
//! listing fence. When file-based retention unlinks a file from that directory
//! between the build's LIST and its write into `DataFusion`'s list-files cache,
//! the cache keeps the unlinked file, and every later scan plans over it.

use crate::common;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, TimeRetentionFilterBuilder};
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

/// The race: the index build LISTs, retention unlinks and evicts, then the
/// build writes its listing into the cache.
async fn index_build_listing_racing_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, true, false, true).await
}

/// The race on a table no query has scanned yet, so no per-file statistics
/// were collected for the file retention unlinks.
async fn index_build_listing_racing_retention_unscanned(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, true, false, false).await
}

/// Perturbation: the race, then the list-files cache is cleared before the
/// queries. Passing here pins the failure on the cached listing.
async fn index_build_listing_racing_retention_then_cache_cleared(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, true, true, true).await
}

/// Control: the same steps with the index build finished before retention.
async fn index_build_listing_before_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, false, false, true).await
}

async fn run(
    fixture: TestFixture,
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

    // Retention as the runtime runs it: `delete_from` on the accelerator with
    // the `time_column < threshold` filter, with no scan of the table first.
    let retention = table
        .delete_from(
            &ctx.state(),
            vec![col("event_time").lt(lit(ScalarValue::TimestampNanosecond(Some(2), None)))],
        )
        .await?;
    collect(retention, ctx.task_ctx()).await?;
    let vortex_files_after = count_vortex_files(&fixture.data_path);
    assert_eq!(
        vortex_files_after + 1,
        vortex_files_before,
        "premise: retention unlinked exactly one file"
    );

    if let Some(build) = pending_build {
        store.release.notify_one();
        build.await?;
    }

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
    eprintln!("race={race} outcomes={outcomes:#?}");
    assert_eq!(
        outcomes,
        vec![Ok(vec![3, 3]), Ok(vec![3, 3, 5])],
        "scans after retention must read only the files that remain"
    );
    Ok(())
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
            Arc::clone(&fixture.catalog) as Arc<dyn cayenne::MetadataCatalog>,
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
/// holds it until released before handing it to the caller.
#[derive(Debug)]
struct PausedListStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
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
        if !self.armed.swap(false, Ordering::AcqRel) {
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
