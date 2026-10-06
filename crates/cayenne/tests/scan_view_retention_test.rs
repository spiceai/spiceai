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

//! Scan-view selection while file-based retention holds the listing fence.

mod common;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, ScanViewReuse, TimeRetentionFilterBuilder,
};
use common::TestFixture;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Notify;

test_with_backends!(scan_view_selection_during_retention);

async fn scan_view_selection_during_retention(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    for (index, reuse) in [
        ScanViewReuse::UntilInvalidated,
        ScanViewReuse::WithinLag(Duration::from_hours(1)),
        ScanViewReuse::WithinLag(Duration::ZERO),
    ]
    .into_iter()
    .enumerate()
    {
        let ctx = SessionContext::new();
        let store = Arc::new(PausedDeleteStore {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            armed: Arc::new(AtomicBool::new(false)),
            deleted: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        ctx.runtime_env().register_object_store(
            &url::Url::parse("file:///")?,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let table = retention_table(&fixture, &ctx, index, reuse).await?;
        table.init_scan_view_cache();
        ctx.register_table("t", table)?;
        for sql in [
            "INSERT INTO t VALUES (to_timestamp_nanos(1)), (to_timestamp_nanos(1))",
            "INSERT INTO t VALUES (to_timestamp_nanos(3)), (to_timestamp_nanos(3))",
        ] {
            ctx.sql(sql).await?.collect().await?;
        }
        let select = "SELECT event_time FROM t ORDER BY event_time";
        for _ in 0..2 {
            assert_eq!(
                timestamps(&ctx.sql(select).await?.collect().await?),
                vec![1, 1, 3, 3]
            );
        }

        store.armed.store(true, Ordering::Release);
        let delete_ctx = ctx.clone();
        let deletion = tokio::spawn(async move {
            delete_ctx
                .sql("DELETE FROM t WHERE event_time < to_timestamp_nanos(2)")
                .await?
                .collect()
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), store.deleted.notified()).await?;

        // Start planning after the file is absent, while retention still owns
        // the write fence. Keep the future alive when the timeout expires.
        let planning = async { ctx.sql(select).await?.create_physical_plan().await };
        tokio::pin!(planning);
        let while_deleting = tokio::time::timeout(Duration::from_millis(100), &mut planning).await;
        store.release.notify_one();
        deletion.await??;
        assert!(
            while_deleting.is_err(),
            "{reuse:?} must wait for retention before capturing a file set"
        );
        let plan = planning.await?;
        assert_eq!(
            timestamps(&collect(plan, ctx.task_ctx()).await?),
            vec![3, 3]
        );
        assert_eq!(
            timestamps(&ctx.sql(select).await?.collect().await?),
            vec![3, 3]
        );
    }
    Ok(())
}

async fn retention_table(
    fixture: &TestFixture,
    ctx: &SessionContext,
    index: usize,
    reuse: ScanViewReuse,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]));
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn cayenne::MetadataCatalog>,
            ctx.runtime_env(),
        )
        .with_scan_view_reuse(reuse)
        .with_time_retention_filter_builder(TimeRetentionFilterBuilder::try_new(
            "event_time",
            u64::MAX,
            &schema,
        )?)
        .create(CreateTableOptions {
            table_name: format!("retention_scan_view_{index}"),
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
        })
        .collect()
}

/// Pause after the first armed unlink succeeds, before returning it to retention.
#[derive(Debug)]
struct PausedDeleteStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
    deleted: Arc<Notify>,
    release: Arc<Notify>,
}

impl fmt::Display for PausedDeleteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PausedDeleteStore")
    }
}

#[async_trait]
impl ObjectStore for PausedDeleteStore {
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
        let armed = Arc::clone(&self.armed);
        let deleted = Arc::clone(&self.deleted);
        let release = Arc::clone(&self.release);
        self.inner
            .delete_stream(locations)
            .then(move |result| {
                let armed = Arc::clone(&armed);
                let deleted = Arc::clone(&deleted);
                let release = Arc::clone(&release);
                async move {
                    if result.is_ok() && armed.swap(false, Ordering::AcqRel) {
                        deleted.notify_one();
                        release.notified().await;
                    }
                    result
                }
            })
            .boxed()
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
