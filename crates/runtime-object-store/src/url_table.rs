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

//! Where a URL table on a cloud object store (`s3://`, `abfs(s)://`, `gs://`) gets its
//! credentials.
//!
//! A URL table borrows credentials only from a dataset whose configured `from` covers the URL,
//! and reads only the objects that dataset would read: objects under its `from` path with the
//! suffix it lists by. Any other URL is read without credentials. [`ObjectLocation`] decides
//! coverage from the URLs as written; [`ScopedStore`] enforces the dataset's scope on every
//! request the URL table's store serves.

use std::{fmt, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use snafu::Snafu;
use url::Url;

/// Scheme prefix of the store a URL table reads with a dataset's credentials. The store is
/// registered for each read, so the registry never builds one from a URL with this prefix.
pub const DATASET_SCOPED_SCHEME_PREFIX: &str = "spice-url-table-dataset+";

/// URL fragment keys a query may set on an object-store URL table and that an unsigned read
/// keeps. `auth` is not among them: an unsigned read never authenticates.
pub const ANONYMOUS_FRAGMENT_KEYS: &[&str] = &["account", "client_timeout", "region", "url_style"];

/// The object-store location an anonymous URL table store registered under `url` reads, or
/// `None` when `url` is not one (an `http`/`https` URL table store, or no URL table at all).
#[must_use]
pub fn anonymous_target(url: &Url) -> Option<Url> {
    let target = url
        .as_str()
        .strip_prefix(crate::ssrf::URL_TABLE_SCHEME_PREFIX)?;
    let target = Url::parse(target).ok()?;
    StoreFamily::of_scheme(target.scheme()).map(|_| target)
}

/// Which object store a URL addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFamily {
    S3,
    Azure,
    Gcs,
}

impl StoreFamily {
    /// The family of a URL table or dataset scheme, or `None` for a scheme that is not a cloud
    /// object store.
    #[must_use]
    pub fn of_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "s3" => Some(Self::S3),
            "abfs" | "abfss" => Some(Self::Azure),
            "gs" | "gcs" => Some(Self::Gcs),
            _ => None,
        }
    }
}

/// A cloud object-store location as written: the store, the bucket (or Azure account and
/// container), and the path, normalized for comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectLocation {
    family: StoreFamily,
    /// The bucket, or `account/container` for Azure; lowercased.
    bucket: String,
    /// Path segments before the first one with a wildcard, percent-decoded, with `.` and `..`
    /// resolved and empty segments dropped.
    segments: Vec<String>,
}

impl ObjectLocation {
    /// The location `url` addresses. `account` is the Azure storage account when it is not in
    /// the URL (`abfss://container/path` with an `account` parameter); it is ignored for other
    /// stores. `None` for a URL that is not on a cloud object store.
    #[must_use]
    pub fn parse(url: &Url, account: Option<&str>) -> Option<Self> {
        let family = StoreFamily::of_scheme(url.scheme())?;
        let host = url.host_str()?.to_ascii_lowercase();
        let bucket = match family {
            StoreFamily::S3 | StoreFamily::Gcs => host,
            StoreFamily::Azure => {
                if url.username().is_empty() {
                    let account = account.map(str::to_ascii_lowercase).unwrap_or_default();
                    format!("{account}/{host}")
                } else {
                    let account = host.split('.').next().unwrap_or_default();
                    format!("{account}/{}", url.username().to_ascii_lowercase())
                }
            }
        };
        Some(Self {
            family,
            bucket,
            segments: normalized_segments(url.path()),
        })
    }

    /// Whether `self`, a dataset's location, covers `other`, a URL table's: the same store and
    /// bucket, and `other`'s path at or below `self`'s at a `/` boundary.
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        self.family == other.family
            && self.bucket == other.bucket
            && other.segments.starts_with(&self.segments)
    }

    /// Whether `other` is on the same store and bucket, covered or not.
    #[must_use]
    pub fn same_bucket(&self, other: &Self) -> bool {
        self.family == other.family && self.bucket == other.bucket
    }

    /// How specific the location is: the number of path segments it names.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    /// How many leading path segments `self` and `other` share.
    #[must_use]
    pub fn shared_depth(&self, other: &Self) -> usize {
        self.segments
            .iter()
            .zip(&other.segments)
            .take_while(|(a, b)| a == b)
            .count()
    }
}

/// `path`'s segments up to the first one with a wildcard, percent-decoded, with `.` and `..`
/// resolved and empty segments dropped.
fn normalized_segments(path: &str) -> Vec<String> {
    let mut segments: Vec<String> = Vec::new();
    for raw in path.split('/') {
        let segment = percent_encoding::percent_decode_str(raw)
            .decode_utf8_lossy()
            .into_owned();
        if segment.contains(['*', '?', '[', '{']) {
            break;
        }
        match segment.as_str() {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            _ => segments.push(segment),
        }
    }
    segments
}

/// The objects a dataset reads: those its `from` contains, with the suffix it lists by.
#[derive(Clone, Debug)]
pub struct DatasetScope {
    dataset: Arc<str>,
    from: Arc<str>,
    table_url: ListingTableUrl,
    /// `None` when the dataset reads every object under its `from`.
    suffix: Option<String>,
}

impl DatasetScope {
    /// The scope of `dataset`, which reads `from` and lists objects by `listing_suffix`
    /// (`.parquet`, or a `*`-prefixed suffix for a dataset that lists every object).
    ///
    /// # Errors
    ///
    /// Returns an error when `from` is not a URL a listing table can read.
    pub fn new(dataset: &str, from: &str, listing_suffix: &str) -> datafusion::error::Result<Self> {
        let suffix = (!listing_suffix.is_empty() && !listing_suffix.starts_with('*'))
            .then(|| listing_suffix.to_string());
        Ok(Self {
            dataset: Arc::from(dataset),
            from: Arc::from(from),
            table_url: ListingTableUrl::parse(from)?,
            suffix,
        })
    }

    /// Whether the dataset reads the object at `path`.
    #[must_use]
    pub fn admits(&self, path: &Path) -> bool {
        self.table_url.contains(path, false)
            && self
                .suffix
                .as_deref()
                .is_none_or(|suffix| path.as_ref().ends_with(suffix))
    }

    /// Whether objects under `prefix` can be admitted, so a listing below it is worth making.
    fn may_admit_below(&self, prefix: &Path) -> bool {
        let dataset_prefix = self.table_url.prefix();
        prefix.prefix_matches(dataset_prefix) || dataset_prefix.prefix_matches(prefix)
    }

    fn refused(&self, path: &Path) -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: path.to_string(),
            source: Box::new(OutsideDatasetScope {
                dataset: Arc::clone(&self.dataset),
                from: Arc::clone(&self.from),
                suffix: self.suffix.clone(),
            }),
        }
    }
}

/// A URL table asked for an object the dataset whose credentials it borrows does not read.
#[derive(Clone, Debug, Snafu)]
#[snafu(display(
    "dataset '{dataset}' covers this location but does not read this object ({}), and a URL table reads only the objects its covering dataset reads",
    suffix.as_deref().map_or_else(
        || format!("it reads objects under '{from}'"),
        |suffix| format!("it reads '{suffix}' objects under '{from}'")
    )
))]
pub struct OutsideDatasetScope {
    pub dataset: Arc<str>,
    pub from: Arc<str>,
    pub suffix: Option<String>,
}

/// A read-only store that serves a URL table the objects one dataset reads, with that
/// dataset's credentials, and refuses the rest.
pub struct ScopedStore {
    inner: Arc<dyn ObjectStore>,
    scope: DatasetScope,
}

impl ScopedStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, scope: DatasetScope) -> Self {
        Self { inner, scope }
    }

    fn read_only(&self) -> object_store::Error {
        object_store::Error::NotSupported {
            source: format!(
                "a URL table reads dataset '{}' read-only",
                self.scope.dataset
            )
            .into(),
        }
    }
}

impl fmt::Debug for ScopedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedStore")
            .field("dataset", &self.scope.dataset)
            .field("from", &self.scope.from)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ScopedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScopedStore({})", self.scope.from)
    }
}

#[async_trait]
impl ObjectStore for ScopedStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(self.read_only())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(self.read_only())
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !self.scope.admits(location) {
            return Err(self.scope.refused(location));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        if !self.scope.admits(location) {
            return Err(self.scope.refused(location));
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let refused = self.read_only().to_string();
        locations
            .map(move |_| {
                Err(object_store::Error::NotSupported {
                    source: refused.clone().into(),
                })
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let scope = self.scope.clone();
        self.inner
            .list(prefix)
            .filter(move |meta| {
                let keep = meta
                    .as_ref()
                    .map_or(true, |meta| scope.admits(&meta.location));
                std::future::ready(keep)
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let scope = self.scope.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .filter(move |meta| {
                let keep = meta
                    .as_ref()
                    .map_or(true, |meta| scope.admits(&meta.location));
                std::future::ready(keep)
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result
            .objects
            .retain(|meta| self.scope.admits(&meta.location));
        result
            .common_prefixes
            .retain(|common| self.scope.may_admit_below(common));
        Ok(result)
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(self.read_only())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(url: &str) -> ObjectLocation {
        ObjectLocation::parse(&Url::parse(url).expect("test URL parses"), None)
            .expect("an object-store URL")
    }

    #[test]
    fn coverage_is_by_store_bucket_and_whole_path_segments() {
        let exports = location("s3://analytics/exports/");
        assert!(exports.covers(&location("s3://analytics/exports/2026/11/part-0.parquet")));
        assert!(exports.covers(&location("s3://analytics/exports")));
        assert!(!exports.covers(&location("s3://analytics/exports-old/x.parquet")));
        assert!(!exports.covers(&location("s3://analytics/backups/db.parquet")));
        assert!(!exports.covers(&location("s3://other/exports/x.parquet")));
        assert!(!exports.covers(&location("gs://analytics/exports/x.parquet")));
        assert!(location("gcs://b/x/").covers(&location("gs://b/x/y.csv")));
    }

    #[test]
    fn dot_segments_percent_encoding_and_wildcards_are_normalized() {
        let exports = location("s3://analytics/exports/");
        assert!(!exports.covers(&location("s3://analytics/exports/../backups/db.parquet")));
        assert!(exports.covers(&location("s3://analytics/%65xports/a.parquet")));
        assert!(exports.covers(&location("s3://analytics/exports/2026/*.parquet")));
        assert!(!exports.covers(&location("s3://analytics/*/a.parquet")));
        assert_eq!(location("s3://b/a/./b//c/").depth(), 3);
    }

    #[test]
    fn azure_locations_compare_account_and_container() {
        let dataset = location("abfss://data@acct.dfs.core.windows.net/exports/");
        assert!(dataset.covers(&location(
            "abfs://data@acct.dfs.core.windows.net/exports/a.csv"
        )));
        let by_param = ObjectLocation::parse(
            &Url::parse("abfss://data/exports/a.csv").expect("valid URL"),
            Some("ACCT"),
        )
        .expect("an Azure URL");
        assert!(dataset.covers(&by_param));
        assert!(!dataset.covers(&location(
            "abfss://data@other.dfs.core.windows.net/exports/a.csv"
        )));
        assert!(!dataset.covers(&location(
            "abfss://logs@acct.dfs.core.windows.net/exports/a.csv"
        )));
    }

    /// The enforcement point: whatever path DataFusion lists or reads, the store answers only
    /// with the objects the dataset reads, and writes nothing.
    #[tokio::test]
    async fn a_scoped_store_serves_only_the_datasets_objects() {
        use futures::TryStreamExt;
        use object_store::{ObjectStoreExt, memory::InMemory};

        let inner = Arc::new(InMemory::new());
        for path in [
            "exports/a.parquet",
            "exports/2026/b.parquet",
            "exports/secrets.csv",
            "backups/db.parquet",
        ] {
            inner
                .put(&Path::from(path), PutPayload::from_static(b"x"))
                .await
                .expect("seeded");
        }
        let scope = DatasetScope::new("exports", "s3://analytics/exports/", ".parquet")
            .expect("valid scope");
        let store = ScopedStore::new(inner, scope);

        let mut listed: Vec<String> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .expect("listed");
        listed.sort();
        assert_eq!(listed, ["exports/2026/b.parquet", "exports/a.parquet"]);

        let top = store.list_with_delimiter(None).await.expect("listed");
        assert_eq!(
            top.common_prefixes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["exports"],
            "a prefix the dataset never reads is not even named"
        );

        store
            .get(&Path::from("exports/a.parquet"))
            .await
            .expect("an admitted object is read");
        for refused in ["exports/secrets.csv", "backups/db.parquet"] {
            let message = store
                .get(&Path::from(refused))
                .await
                .expect_err("a refused object is not read")
                .to_string();
            assert!(
                message.contains(
                    "dataset 'exports' covers this location but does not read this object \
                     (it reads '.parquet' objects under 's3://analytics/exports/')"
                ),
                "{refused}: {message}"
            );
        }
        let message = store
            .put(
                &Path::from("exports/new.parquet"),
                PutPayload::from_static(b"y"),
            )
            .await
            .expect_err("writes are refused")
            .to_string();
        assert!(
            message.contains("a URL table reads dataset 'exports' read-only"),
            "{message}"
        );
    }

    #[test]
    fn a_scope_admits_what_the_dataset_lists() {
        let scope = DatasetScope::new("exports", "s3://analytics/exports/", ".parquet")
            .expect("valid scope");
        assert!(scope.admits(&Path::from("exports/2026/part-0.parquet")));
        assert!(!scope.admits(&Path::from("exports/secrets.csv")));
        assert!(!scope.admits(&Path::from("exports-old/part-0.parquet")));
        assert!(!scope.admits(&Path::from("backups/part-0.parquet")));

        let any = DatasetScope::new("docs", "s3://analytics/docs/", "*").expect("valid scope");
        assert!(any.admits(&Path::from("docs/a.pdf")));
        assert!(!any.admits(&Path::from("other/a.pdf")));

        let single = DatasetScope::new("one", "s3://analytics/exports/a.parquet", ".parquet")
            .expect("valid scope");
        assert!(single.admits(&Path::from("exports/a.parquet")));
        assert!(!single.admits(&Path::from("exports/b.parquet")));
    }
}
