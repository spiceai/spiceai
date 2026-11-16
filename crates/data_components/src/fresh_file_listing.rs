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

//! Lists a file dataset's directory on every scan.
//!
//! `DataFusion`'s `ListingTable` reads its file list through the session's list-files
//! cache, which has no expiry by default and which nothing invalidates when files change
//! outside `DataFusion`. A file dataset read through that cache answers from its first
//! listing for as long as the runtime runs: an added file is never read, and a replaced
//! one is read with the old file's size and `ETag` (#15547).

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session},
    datasource::{TableProvider, listing::ListingTableUrl},
    error::{DataFusionError, Result as DFResult},
    execution::cache::TableScopedPath,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

use crate::file_schema_drift::scanned_files;

/// Whether a listed object should count as a data file for this dataset.
pub type IsListedDataFile = Arc<dyn Fn(&ObjectMeta) -> bool + Send + Sync>;

/// Whether the table beneath answers a scan with these filters without listing its
/// paths (for example a `_location` predicate served by `head()`), so the empty-listing
/// check must not list either.
pub type ScanSkipsListing = Arc<dyn Fn(&[Expr]) -> bool + Send + Sync>;

/// The [`TableLayer`] that drops a file dataset's cached file list before each scan, so the
/// `ListingTable` beneath lists its paths again. Per-file caches keyed on the listed
/// `ObjectMeta` (Parquet footers, file statistics) stay valid for unchanged files.
///
/// When constructed with [`Self::over_rejecting_empty`], a scan whose listing has no data
/// file is an error instead of an empty table. A file that exists and matches the
/// listing (header-only CSV, zero-row Parquet) still scans as zero rows.
///
/// The check reads the files the scan beneath already listed, so a scan that reads a
/// data file costs no object-store request beyond the scan's own. Only a scan that
/// reads no data file (the source is gone, or its filters pruned every file) lists the
/// paths again, to tell the two apart.
pub struct FreshFileListingLayer {
    table_paths: Vec<ListingTableUrl>,
    dataset_name: String,
    is_data_file: Option<IsListedDataFile>,
    scan_skips_listing: Option<ScanSkipsListing>,
}

impl std::fmt::Debug for FreshFileListingLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreshFileListingLayer")
            .field("table_paths", &self.table_paths)
            .field("dataset_name", &self.dataset_name)
            .field("reject_empty_listing", &self.is_data_file.is_some())
            .field("scan_skips_listing", &self.scan_skips_listing.is_some())
            .finish()
    }
}

impl FreshFileListingLayer {
    #[must_use]
    pub fn new(table_paths: Vec<ListingTableUrl>) -> Self {
        Self {
            table_paths,
            dataset_name: String::new(),
            is_data_file: None,
            scan_skips_listing: None,
        }
    }

    /// `table`, listing `table_paths` again on every scan.
    #[must_use]
    pub fn over(
        table: Arc<dyn TableProvider>,
        table_paths: Vec<ListingTableUrl>,
    ) -> Arc<dyn TableProvider> {
        SpiceTable::over(Arc::new(Self::new(table_paths)), table)
    }

    /// Like [`Self::over`], and a listing with no object `is_data_file` accepts is an
    /// error rather than an empty result.
    #[must_use]
    pub fn over_rejecting_empty(
        table: Arc<dyn TableProvider>,
        table_paths: Vec<ListingTableUrl>,
        dataset_name: impl Into<String>,
        is_data_file: impl Fn(&ObjectMeta) -> bool + Send + Sync + 'static,
    ) -> Arc<dyn TableProvider> {
        Self::over_rejecting_empty_unless(table, table_paths, dataset_name, is_data_file, None)
    }

    /// Like [`Self::over_rejecting_empty`], except a scan `scan_skips_listing` accepts is
    /// not checked: the table beneath answers it without listing, so listing here would
    /// add a list call (and a list permission) the scan does not otherwise need.
    #[must_use]
    pub fn over_rejecting_empty_unless(
        table: Arc<dyn TableProvider>,
        table_paths: Vec<ListingTableUrl>,
        dataset_name: impl Into<String>,
        is_data_file: impl Fn(&ObjectMeta) -> bool + Send + Sync + 'static,
        scan_skips_listing: Option<ScanSkipsListing>,
    ) -> Arc<dyn TableProvider> {
        SpiceTable::over(
            Arc::new(Self {
                table_paths,
                dataset_name: dataset_name.into(),
                is_data_file: Some(Arc::new(is_data_file)),
                scan_skips_listing,
            }),
            table,
        )
    }

    fn forget_listing(&self, state: &dyn Session) {
        let Some(cache) = state.runtime_env().cache_manager.get_list_files_cache() else {
            return;
        };
        for path in &self.table_paths {
            // The key `ListingTable` caches its listing under.
            let _ = cache.remove(&TableScopedPath {
                table: path.get_table_ref().clone(),
                path: path.prefix().clone(),
            });
        }
    }

    async fn ensure_data_files(&self, state: &dyn Session) -> DFResult<()> {
        let Some(is_data_file) = &self.is_data_file else {
            return Ok(());
        };
        for path in &self.table_paths {
            if self.path_has_data_file(state, path, is_data_file).await? {
                continue;
            }
            let display_path = redacted_listing_url(path);
            tracing::warn!(
                dataset = %self.dataset_name,
                path = %display_path,
                "No data files are yet available for dataset {} at {display_path}. The source path is missing or has no matching files.",
                self.dataset_name
            );
            return Err(DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No data files are yet available for dataset {} at {display_path}. The source path is missing or has no matching files.",
                    self.dataset_name
                ),
            ))));
        }
        Ok(())
    }

    async fn path_has_data_file(
        &self,
        state: &dyn Session,
        path: &ListingTableUrl,
        is_data_file: &IsListedDataFile,
    ) -> DFResult<bool> {
        let store = state.runtime_env().object_store(path)?;
        let mut listing = path.list_all_files(state, store.as_ref(), "").await?;
        while let Some(meta) = listing.try_next().await? {
            if is_data_file(&meta) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether `plan` scans at least one data file, read from the files the scan beneath
    /// listed. `true` when this layer does not reject empty listings.
    fn plan_reads_data_file(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        let Some(is_data_file) = &self.is_data_file else {
            return true;
        };
        scanned_files(plan).is_some_and(|(_, files)| files.iter().any(&**is_data_file))
    }
}

fn redacted_listing_url(path: &ListingTableUrl) -> url::Url {
    let mut redacted = <ListingTableUrl as AsRef<url::Url>>::as_ref(path).clone();
    redacted.set_fragment(None);
    redacted
}

#[async_trait]
impl TableLayer for FreshFileListingLayer {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind for this
        // layer without anyone deciding what it should say.
        match walk {
            // Listing again changes no row, column, or type a scan of the same files
            // returns, so every walk sees past this layer.
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::Write
            | LayerWalk::RetentionDelete
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
        }
    }

    async fn scan_with_args<'a>(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        self.forget_listing(state);
        let skips_listing = self
            .scan_skips_listing
            .as_ref()
            .is_some_and(|skips| skips(args.filters().unwrap_or_default()));
        let result = below.scan_with_args(state, args).await?;
        if !skips_listing && !self.plan_reads_data_file(result.plan()) {
            self.ensure_data_files(state).await?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use parquet::arrow::ArrowWriter;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn write_ids(path: &Path, ids: impl IntoIterator<Item = i64>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(ids))],
        )
        .expect("batch");
        let file = std::fs::File::create(path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet file");
    }

    /// `count(*)` and `max(id)`, the maximum `None` when it is NULL (no rows).
    async fn count_and_max(ctx: &SessionContext) -> (i64, Option<i64>) {
        let batches = ctx
            .sql("SELECT count(*), max(id) FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let column = |i: usize| {
            let column = batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            if column.is_null(0) {
                None
            } else {
                Some(column.value(0))
            }
        };
        (column(0).expect("count is never NULL"), column(1))
    }

    /// Files added and replaced after the first query are read by the next one. Without
    /// the layer the second query answers (100, 100) from the cached listing.
    #[tokio::test]
    async fn scans_read_files_changed_after_the_first_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ids(&dir.path().join("part-0.parquet"), 1..=100);

        let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let ctx = SessionContext::new();
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .expect("infer schema");
        let table: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        ctx.register_table("t", FreshFileListingLayer::over(table, vec![url]))
            .expect("register");

        assert_eq!(count_and_max(&ctx).await, (100, Some(100)));

        write_ids(&dir.path().join("part-0.parquet"), 1..=1000);
        write_ids(&dir.path().join("part-1.parquet"), 1001..=3000);

        assert_eq!(count_and_max(&ctx).await, (3000, Some(3000)));
    }

    fn parquet_is_data_file(meta: &object_store::ObjectMeta) -> bool {
        meta.size > 0 && meta.location.as_ref().ends_with(".parquet")
    }

    async fn register_rejecting(ctx: &SessionContext, dir: &Path) {
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .expect("infer schema");
        let table: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        ctx.register_table(
            "t",
            FreshFileListingLayer::over_rejecting_empty(
                table,
                vec![url],
                "t",
                parquet_is_data_file,
            ),
        )
        .expect("register");
    }

    async fn query_err(ctx: &SessionContext) -> String {
        match ctx.sql("SELECT count(*) FROM t").await {
            Err(e) => e.to_string(),
            Ok(df) => match df.collect().await {
                Err(e) => e.to_string(),
                Ok(_) => String::new(),
            },
        }
    }

    /// Removing every listed file is an error, not an empty table. A file that
    /// still exists and holds no rows still answers zero.
    #[tokio::test]
    async fn scans_error_when_listed_files_are_gone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part0 = dir.path().join("part-0.parquet");
        write_ids(&part0, 1..=100);

        let ctx = SessionContext::new();
        register_rejecting(&ctx, dir.path()).await;
        assert_eq!(count_and_max(&ctx).await, (100, Some(100)));

        std::fs::remove_file(&part0).expect("remove listed file");
        let err = query_err(&ctx).await;
        assert!(
            err.contains("No data files are yet available"),
            "expected missing-source error, got {err}"
        );

        write_ids(&part0, std::iter::empty());
        assert_eq!(count_and_max(&ctx).await, (0, None));

        write_ids(&part0, 1..=50);
        assert_eq!(count_and_max(&ctx).await, (50, Some(50)));
    }

    #[test]
    fn listing_diagnostics_omit_url_fragments() {
        let path = ListingTableUrl::parse(
            "s3://bucket/facts/#key=fixture&secret=fixture-secret&session_token=fixture-token",
        )
        .expect("url");
        let display = redacted_listing_url(&path).to_string();
        assert!(
            !display.contains("fixture-secret"),
            "secret leaked in {display}"
        );
        assert!(
            !display.contains("session_token"),
            "session token leaked in {display}"
        );
        assert!(!display.contains('#'), "fragment remained in {display}");
        assert!(display.contains("s3://bucket/facts"), "{display}");
        assert!(
            <ListingTableUrl as AsRef<url::Url>>::as_ref(&path)
                .fragment()
                .is_some(),
            "storage URL must keep its fragment"
        );
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0)
    }

    /// The error `sql` fails with, or an empty string when it succeeds.
    async fn sql_err(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => match df.collect().await {
                Err(e) => e.to_string(),
                Ok(_) => String::new(),
            },
        }
    }

    /// A partitioned scan whose filter prunes every file reads no data file while its
    /// source still has one, so it answers zero rows. Once the source is gone, the same
    /// query fails: the check lists again when the scan read nothing.
    #[tokio::test]
    async fn a_scan_whose_filter_prunes_every_file_answers_zero_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let partition = dir.path().join("year=2024");
        std::fs::create_dir(&partition).expect("create partition directory");
        let part0 = partition.join("part-0.parquet");
        write_ids(&part0, 1..=100);

        let ctx = SessionContext::new();
        let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("year".to_string(), DataType::Utf8)]);
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .expect("infer schema");
        let table: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        ctx.register_table(
            "t",
            FreshFileListingLayer::over_rejecting_empty(
                table,
                vec![url],
                "t",
                parquet_is_data_file,
            ),
        )
        .expect("register");

        let pruned = "SELECT count(*) FROM t WHERE year = '2025'";
        assert_eq!(count(&ctx, pruned).await, 0);
        assert_eq!(
            count(&ctx, "SELECT count(*) FROM t WHERE year = '2024'").await,
            100
        );

        std::fs::remove_file(&part0).expect("remove listed file");
        let err = sql_err(&ctx, pruned).await;
        assert!(
            err.contains("No data files are yet available for dataset t"),
            "expected missing-source error, got {err:?}"
        );
    }

    /// Object-store LIST and HEAD requests, counted.
    #[derive(Debug, Default)]
    struct RequestCounts {
        list: AtomicUsize,
        head: AtomicUsize,
    }

    impl RequestCounts {
        fn read(&self) -> (usize, usize) {
            (
                self.list.load(Ordering::SeqCst),
                self.head.load(Ordering::SeqCst),
            )
        }
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        counts: RequestCounts,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.counts.list.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.counts.list.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.counts.head.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn parquet_payload(ids: std::ops::RangeInclusive<i64>) -> PutPayload {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(ids))],
        )
        .expect("batch");
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet writer");
        PutPayload::from(buffer)
    }

    /// The `ListingTable` over `url` the way a file dataset builds it, with the empty-listing
    /// check (`guarded`) or without it.
    fn listing_over(url: &str, guarded: bool) -> Arc<dyn TableProvider> {
        let url = ListingTableUrl::parse(url).expect("listing url");
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(
                ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet"),
            )
            .with_schema(Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])));
        let table: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        if guarded {
            FreshFileListingLayer::over_rejecting_empty(
                table,
                vec![url],
                "facts",
                parquet_is_data_file,
            )
        } else {
            FreshFileListingLayer::over(table, vec![url])
        }
    }

    /// The LIST and HEAD requests one `SELECT count(*)` of `table` sends, checking it
    /// answers `rows`.
    async fn requests_per_scan(
        ctx: &SessionContext,
        store: &CountingStore,
        table: &str,
        rows: i64,
    ) -> (usize, usize) {
        let (list, head) = store.counts.read();
        assert_eq!(
            count(ctx, &format!("SELECT count(*) FROM {table}")).await,
            rows
        );
        let (list_after, head_after) = store.counts.read();
        (list_after - list, head_after - head)
    }

    /// The empty-listing check reads the files the scan listed, so a scan that reads a
    /// data file sends exactly the requests the same scan sends without the check: one
    /// LIST for a folder, whether or not the session caches listings, and one HEAD for a
    /// single object. The check is live on the same table: deleting the source fails it.
    #[tokio::test]
    async fn rejecting_empty_listings_adds_no_object_store_request() {
        let cases = [
            ("memory://probe/facts/", true, 20, (1, 0)),
            ("memory://probe/facts/", false, 20, (1, 0)),
            ("memory://probe/facts/part-0.parquet", true, 10, (0, 1)),
            ("memory://probe/facts/part-0.parquet", false, 10, (0, 1)),
        ];
        for (url, list_files_cache, rows, expected) in cases {
            let cache = if list_files_cache {
                CacheManagerConfig::default()
            } else {
                CacheManagerConfig::default().with_list_files_cache_limit(0)
            };
            let runtime = RuntimeEnvBuilder::new()
                .with_cache_manager(cache)
                .build_arc()
                .expect("runtime");
            let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
            assert_eq!(
                ctx.runtime_env()
                    .cache_manager
                    .get_list_files_cache()
                    .is_some(),
                list_files_cache
            );
            let store = Arc::new(CountingStore {
                inner: InMemory::new(),
                counts: RequestCounts::default(),
            });
            let parts = [
                ObjectPath::from("facts/part-0.parquet"),
                ObjectPath::from("facts/part-1.parquet"),
            ];
            store
                .inner
                .put(&parts[0], parquet_payload(1..=10))
                .await
                .expect("put part-0");
            store
                .inner
                .put(&parts[1], parquet_payload(11..=20))
                .await
                .expect("put part-1");
            ctx.runtime_env().register_object_store(
                &url::Url::parse("memory://probe").expect("store url"),
                Arc::clone(&store) as Arc<dyn ObjectStore>,
            );
            ctx.register_table("plain", listing_over(url, false))
                .expect("register plain");
            ctx.register_table("guarded", listing_over(url, true))
                .expect("register guarded");

            let label = format!("{url} with list-files cache {list_files_cache}");
            assert_eq!(
                requests_per_scan(&ctx, &store, "plain", rows).await,
                expected,
                "{label}: without the check"
            );
            assert_eq!(
                requests_per_scan(&ctx, &store, "guarded", rows).await,
                expected,
                "{label}: with the check"
            );

            for part in &parts {
                store.inner.delete(part).await.expect("delete part");
            }
            let err = sql_err(&ctx, "SELECT count(*) FROM guarded").await;
            assert!(
                err.contains("No data files are yet available for dataset facts"),
                "{label}: expected missing-source error, got {err:?}"
            );
        }
    }
}
