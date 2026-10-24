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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::Result as DFResult;
use datafusion::execution::cache::TableScopedPath;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// Lists a dataset's files on every scan.
///
/// `ListingTable` reads a directory's listing through the session's list-files cache,
/// which `DataFusion` enables by default with no expiry. Nothing invalidates a dataset's
/// entry when its source files change, so without this layer a directory dataset keeps
/// serving the files it listed on its first scan. Evicting the entry before each scan makes
/// the scan list the files present at query time.
#[derive(Debug)]
pub(crate) struct FreshListingLayer {
    key: TableScopedPath,
}

impl FreshListingLayer {
    /// The layer for a table built over `table_path`.
    pub(crate) fn new(table_path: &ListingTableUrl) -> Self {
        Self {
            key: TableScopedPath {
                table: table_path.get_table_ref().clone(),
                path: table_path.prefix().clone(),
            },
        }
    }

    /// `table` with its directory listing refreshed on every scan.
    pub(crate) fn over(self, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        SpiceTable::over(Arc::new(self), table)
    }
}

#[async_trait]
impl TableLayer for FreshListingLayer {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // A scan returns the rows of the table beneath, so every walk sees past it. That
        // includes passthrough, so the Iceberg API refuses a file dataset as not Iceberg.
        match walk {
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
        if let Some(cache) = state.runtime_env().cache_manager.get_list_files_cache() {
            cache.remove(&self.key);
        }
        below.scan_with_args(state, args).await
    }
}
