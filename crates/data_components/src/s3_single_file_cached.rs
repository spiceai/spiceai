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
use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::dml::InsertOp;
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

    // If version is present in BOTH, it's the authoritative check (regardless of etag)
    if let (Some(cv), Some(curv)) = (&cached_version, &current_version) {
        return cv == curv;
    }

    // If version presence differs (one has it, other doesn't), files are different
    if cached_version.is_some() != current_version.is_some() {
        return false;
    }

    // Version is absent in both, fall back to etag comparison
    if let (Some(ce), Some(cure)) = (&cached_etag, &current_etag) {
        return ce == cure;
    }

    // If etag presence differs (one has it, other doesn't), files are different
    if cached_etag.is_some() != current_etag.is_some() {
        return false;
    }

    // Both version and etag are absent - no versioning info available, assume different to be safe
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
    cached_metadata: RwLock<Option<object_store::ObjectMeta>>,
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
            cached_metadata: RwLock::new(None),
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

        let cached_metadata = {
            let cached = self.cached_metadata.read().await;
            cached.clone()
        };

        if cached_metadata
            .as_ref()
            .is_some_and(|cached| Self::should_skip_with_metadata(cached, &current_metadata))
        {
            let etag = normalize_optional_string(current_metadata.e_tag.as_ref())
                .unwrap_or_else(|| "unknown".to_string());
            let version = normalize_optional_string(current_metadata.version.as_ref())
                .unwrap_or_else(|| "unknown".to_string());
            tracing::info!(
                "Skipping refresh for {} (location: {}, size_bytes: {}, last_modified: {}, etag: {}, version: {})",
                self.dataset_name,
                current_metadata.location,
                current_metadata.size,
                current_metadata.last_modified,
                etag,
                version
            );
            return Ok(true);
        }

        // Update cache with new metadata
        *self.cached_metadata.write().await = Some(current_metadata);

        Ok(false)
    }

    fn should_skip_with_metadata(
        cached_metadata: &object_store::ObjectMeta,
        current_metadata: &object_store::ObjectMeta,
    ) -> bool {
        cached_metadata.size == current_metadata.size
            && cached_metadata.last_modified == current_metadata.last_modified
            && is_same_file_version(cached_metadata, current_metadata)
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
    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, overwrite).await
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use chrono::{TimeZone, Utc};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use object_store::{ObjectStore, path::Path};
    use std::collections::VecDeque;
    use tokio::sync::Mutex;

    fn make_meta(
        location: &str,
        size: u64,
        last_modified_secs: i64,
        etag: Option<&str>,
        version: Option<&str>,
    ) -> object_store::ObjectMeta {
        object_store::ObjectMeta {
            location: ObjectStorePath::from(location),
            last_modified: Utc
                .timestamp_opt(last_modified_secs, 0)
                .single()
                .expect("valid timestamp"),
            size,
            e_tag: etag.map(std::string::ToString::to_string),
            version: version.map(std::string::ToString::to_string),
        }
    }

    fn build_cached_table(
        store: Arc<dyn ObjectStore>,
        cached_meta: Option<object_store::ObjectMeta>,
    ) -> S3SingleFileCached {
        let file_path = ObjectStorePath::from("dummy-file.parquet");
        let schema = Arc::new(Schema::empty());
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table_url = ListingTableUrl::parse("s3://dummy-bucket/dummy-file.parquet")
            .expect("listing table url");
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(options)
            .with_schema(Arc::clone(&schema));
        let listing_table = ListingTable::try_new(config).expect("listing table");

        S3SingleFileCached {
            inner: Arc::new(listing_table),
            object_store: store,
            file_path,
            cached_metadata: RwLock::new(cached_meta),
            dataset_name: "test_dataset".to_string(),
        }
    }

    #[derive(Debug)]
    struct HeadOnlyObjectStore {
        responses: Mutex<VecDeque<object_store::ObjectMeta>>,
    }

    impl HeadOnlyObjectStore {
        fn new(responses: Vec<object_store::ObjectMeta>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
            }
        }
    }

    impl std::fmt::Display for HeadOnlyObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HeadOnlyObjectStore")
        }
    }

    #[async_trait]
    impl ObjectStore for HeadOnlyObjectStore {
        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            stream::empty().boxed()
        }

        async fn head(&self, _location: &Path) -> object_store::Result<object_store::ObjectMeta> {
            let mut guard = self.responses.lock().await;
            guard.pop_front().ok_or(object_store::Error::NotImplemented)
        }

        async fn put(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, _location: &Path) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn delete_stream<'a>(
            &'a self,
            _locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    #[test]
    fn test_is_same_file_version_prefers_version_id() {
        let cached = make_meta("file", 100, 1, Some("etag-a"), Some("v1"));
        let matching_version = make_meta("file", 100, 1, Some("etag-b"), Some("v1"));
        let mismatched_version = make_meta("file", 100, 1, Some("etag-a"), Some("v2"));

        assert!(is_same_file_version(&cached, &matching_version));
        assert!(!is_same_file_version(&cached, &mismatched_version));
    }

    #[test]
    fn test_is_same_file_version_falls_back_to_etag() {
        let cached = make_meta("file", 100, 1, Some("etag-a"), None);
        let matching_etag = make_meta("file", 100, 1, Some("etag-a"), None);
        let mismatched_etag = make_meta("file", 100, 1, Some("etag-b"), None);

        assert!(is_same_file_version(&cached, &matching_etag));
        assert!(!is_same_file_version(&cached, &mismatched_etag));
    }

    #[tokio::test]
    async fn test_should_skip_refresh_when_metadata_matches() {
        let meta = make_meta("file", 128, 10, Some("etag"), Some("v1"));
        let store = Arc::new(HeadOnlyObjectStore::new(vec![meta.clone()])) as Arc<dyn ObjectStore>;
        let cached_table = build_cached_table(store, Some(meta));

        assert!(
            cached_table
                .should_skip_refresh()
                .await
                .expect("skip result")
        );
    }

    #[tokio::test]
    async fn test_should_update_cache_when_metadata_changes() {
        let old_meta = make_meta("file", 128, 10, Some("etag"), Some("v1"));
        let new_meta = make_meta("file", 256, 20, Some("etag2"), Some("v2"));
        let store = Arc::new(HeadOnlyObjectStore::new(vec![
            new_meta.clone(),
            new_meta.clone(),
        ])) as Arc<dyn ObjectStore>;
        let cached_table = build_cached_table(store, Some(old_meta));

        assert!(
            !cached_table
                .should_skip_refresh()
                .await
                .expect("first attempt")
        );
        assert!(
            cached_table
                .should_skip_refresh()
                .await
                .expect("second attempt")
        );
    }
}
