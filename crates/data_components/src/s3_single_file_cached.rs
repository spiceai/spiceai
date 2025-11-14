/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! A wrapper around `ListingTable` for single S3 files that caches `ETag` and Version ID
//! to avoid unnecessary re-scans when the file hasn't changed.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use tokio::sync::RwLock;

use crate::refresh_skip::RefreshSkipTableProvider;

/// Normalize an optional string by trimming whitespace and treating empty strings as None
fn normalize_optional_string(s: Option<&String>) -> Option<String> {
    s.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Check if two `ObjectMeta` represent the same file version based on version ID and `ETag`
fn is_same_file_version(
    cached: &object_store::ObjectMeta,
    current: &object_store::ObjectMeta,
) -> bool {
    let cached_version = normalize_optional_string(cached.version.as_ref());
    let current_version = normalize_optional_string(current.version.as_ref());
    let cached_etag = normalize_optional_string(cached.e_tag.as_ref());
    let current_etag = normalize_optional_string(current.e_tag.as_ref());

    // Both version and etag are absent in both - no versioning info available, consider same
    if cached_version.is_none()
        && current_version.is_none()
        && cached_etag.is_none()
        && current_etag.is_none()
    {
        return true;
    }

    // Check if version or etag presence differs (one has it, other doesn't) - different files
    if (cached_version.is_some() != current_version.is_some())
        || (cached_etag.is_some() != current_etag.is_some())
    {
        return false;
    }

    // If version is present in BOTH and matches, it's the authoritative check
    if let (Some(cv), Some(curv)) = (cached_version, current_version) {
        return cv == curv;
    }

    // If etag is present in BOTH and matches, files are the same
    if let (Some(ce), Some(cure)) = (cached_etag, current_etag)
        && ce == cure
    {
        return true;
    }

    // Otherwise, files are different
    false
}

/// A wrapper around `ListingTable` that caches file metadata (`ETag`, Version ID) for single S3 files.
/// The wrapper enables skipping refresh operations when the file's metadata hasn't changed,
/// thereby avoiding unnecessary S3 data fetching during full refreshes. The scan operation itself
/// always delegates to the inner `ListingTable` and does not perform metadata checks.
#[derive(Debug)]
pub struct S3SingleFileCached {
    inner: Arc<ListingTable>,
    object_store: Arc<dyn ObjectStore>,
    file_path: ObjectStorePath,
    cached_metadata: Arc<RwLock<Option<object_store::ObjectMeta>>>,
    dataset_name: String,
}

impl S3SingleFileCached {
    /// Creates a new cached wrapper around a `ListingTable` for a single file.
    ///
    /// # Arguments
    /// * `listing_table` - The underlying `ListingTable` (must point to a single file, not a collection)
    /// * `object_store` - The object store to use for fetching file metadata
    /// * `dataset_name` - The name of the dataset (for logging purposes)
    ///
    /// # Returns
    /// * `Some(S3SingleFileCached)` if the listing table points to a single file
    /// * `None` if the table points to multiple files or a collection (folder)
    pub fn try_new(
        listing_table: Arc<ListingTable>,
        object_store: Arc<dyn ObjectStore>,
        dataset_name: String,
    ) -> Option<Self> {
        let table_paths = listing_table.table_paths();

        // Only wrap single-file tables
        if table_paths.len() != 1 || table_paths[0].is_collection() {
            return None;
        }

        let file_path = ObjectStorePath::from(table_paths[0].prefix().as_ref());

        Some(Self {
            inner: listing_table,
            object_store,
            file_path,
            cached_metadata: Arc::new(RwLock::new(None)),
            dataset_name,
        })
    }

    /// Fetches the current metadata (`ETag`, Version ID, size, `last_modified`) for the file from S3.
    async fn fetch_current_metadata(&self) -> DataFusionResult<Option<object_store::ObjectMeta>> {
        match self.object_store.head(&self.file_path).await {
            Ok(meta) => Ok(Some(meta)),
            Err(e) => {
                tracing::debug!(
                    "Failed to fetch S3 file metadata for {}: {}",
                    self.dataset_name,
                    e
                );
                Ok(None)
            }
        }
    }

    /// Checks if the file's metadata has changed since the last scan.
    /// Returns `true` if the file is unchanged and can be skipped.
    async fn is_file_unchanged(&self) -> DataFusionResult<bool> {
        let Some(current_metadata) = self.fetch_current_metadata().await? else {
            return Ok(false); // Can't determine, assume changed
        };

        let cached = self.cached_metadata.read().await;

        if let Some(cached_meta) = cached.as_ref() {
            // Check if refresh should be skipped:
            // Skip if size, last_modified, AND version info all match
            if current_metadata.size == cached_meta.size
                && current_metadata.last_modified == cached_meta.last_modified
                && is_same_file_version(cached_meta, &current_metadata)
            {
                tracing::debug!(
                    "Skipping refresh for {} (file unchanged at {})",
                    self.dataset_name,
                    current_metadata.location
                );
                return Ok(true);
            }
        }

        // Update cache with new metadata
        drop(cached);
        *self.cached_metadata.write().await = Some(current_metadata);

        Ok(false)
    }

    /// Public method to check if a refresh should be skipped for this file.
    /// Returns `true` if the file's metadata (`ETag`, Version, size, timestamp) hasn't changed.
    /// This should be called during refresh operations before fetching data.
    pub async fn should_skip_refresh(&self) -> DataFusionResult<bool> {
        self.is_file_unchanged().await
    }
}

#[async_trait]
impl RefreshSkipTableProvider for S3SingleFileCached {
    async fn should_skip_refresh(&self) -> DataFusionResult<bool> {
        self.is_file_unchanged().await
    }
}

#[async_trait]
impl TableProvider for S3SingleFileCached {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Always delegate to inner ListingTable
        // The metadata check happens during refresh, not during scan
        self.inner.scan(state, projection, filters, limit).await
    }
}
