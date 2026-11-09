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
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session},
    datasource::{TableProvider, listing::ListingTableUrl},
    error::{DataFusionError, Result as DFResult},
    execution::cache::TableScopedPath,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

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
        if !skips_listing {
            self.ensure_data_files(state).await?;
        }
        below.scan_with_args(state, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::path::Path;

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

    async fn count_and_max(ctx: &SessionContext) -> (i64, i64) {
        let batches = ctx
            .sql("SELECT count(*), max(id) FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let column = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .value(0)
        };
        (column(0), column(1))
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

        assert_eq!(count_and_max(&ctx).await, (100, 100));

        write_ids(&dir.path().join("part-0.parquet"), 1..=1000);
        write_ids(&dir.path().join("part-1.parquet"), 1001..=3000);

        assert_eq!(count_and_max(&ctx).await, (3000, 3000));
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
        assert_eq!(count_and_max(&ctx).await, (100, 100));

        std::fs::remove_file(&part0).expect("remove listed file");
        let err = query_err(&ctx).await;
        assert!(
            err.contains("No data files are yet available"),
            "expected missing-source error, got {err}"
        );

        write_ids(&part0, std::iter::empty());
        assert_eq!(count_and_max(&ctx).await, (0, 0));

        write_ids(&part0, 1..=50);
        assert_eq!(count_and_max(&ctx).await, (50, 50));
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
}
