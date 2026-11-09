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
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session},
    datasource::{TableProvider, listing::ListingTableUrl},
    error::Result as DFResult,
    execution::cache::TableScopedPath,
};
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// The [`TableLayer`] that drops a file dataset's cached file list before each scan, so the
/// `ListingTable` beneath lists its paths again. Per-file caches keyed on the listed
/// `ObjectMeta` (Parquet footers, file statistics) stay valid for unchanged files.
#[derive(Debug)]
pub struct FreshFileListingLayer {
    table_paths: Vec<ListingTableUrl>,
}

impl FreshFileListingLayer {
    #[must_use]
    pub fn new(table_paths: Vec<ListingTableUrl>) -> Self {
        Self { table_paths }
    }

    /// `table`, listing `table_paths` again on every scan.
    #[must_use]
    pub fn over(
        table: Arc<dyn TableProvider>,
        table_paths: Vec<ListingTableUrl>,
    ) -> Arc<dyn TableProvider> {
        SpiceTable::over(Arc::new(Self::new(table_paths)), table)
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

    fn write_ids(path: &Path, ids: std::ops::RangeInclusive<i64>) {
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
}
