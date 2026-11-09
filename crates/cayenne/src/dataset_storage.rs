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

//! Each dataset's storage: its own object store, the object path its data lives
//! under, and, for a local directory, the directory itself.
//!
//! A table whose data path is an object-store URL (`s3://bucket/…`, or a local
//! directory addressed as `file:///…`) reads and writes every data file through
//! its [`DatasetStorage`]. `DataFusion` finds the store for a scan or a write by the
//! URL's `scheme://host`, so the dataset's store is registered in each `RuntimeEnv`
//! the table uses under a URL of its own, `cayenne://<table id>/`, and the table
//! addresses its files as `cayenne://<table id>/<object path>`. The object path is
//! the same as in the recorded table path, so the metastore keeps recording the
//! table path as before and only the scheme and host differ at run time.
//!
//! Nothing here is process-wide: two datasets in one bucket, a source that
//! registered a store for that bucket, and the session's shared `file://` store
//! each keep their own entry.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStoreExt as _;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

/// The URL scheme each dataset's store is registered under in a `RuntimeEnv`.
pub const DATASET_STORE_SCHEME: &str = "cayenne";

const STORE_NAME: &str = "cayenne";

/// Deletes are passed to the inner store in batches of at most this many paths.
pub(crate) const DELETE_BATCH: usize = 1_000;

const CAYENNE_DOCS: &str = "https://spiceai.org/docs/components/data-accelerators/cayenne";

/// Which of a dataset's files a [`DataFileError`] is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataFileKind {
    /// A Vortex data file a snapshot's committed file set names.
    Data,
    /// A deletion vector (`.arrow`) the dataset's committed deletes name.
    Deletion,
}

impl DataFileKind {
    /// The kind of the file at `location`, an object path of a dataset, when it is
    /// one its committed state names: a `.vortex` data file outside a staging
    /// directory, or a deletion vector under a `deletions` directory.
    #[must_use]
    pub fn of(location: &str) -> Option<Self> {
        let staged = location
            .split('/')
            .any(|segment| segment.starts_with("_staging"));
        if staged {
            return None;
        }
        // Cayenne names its files in lower case, so the extensions compare exactly.
        let extension = std::path::Path::new(location).extension();
        if extension.is_some_and(|extension| extension == "vortex") {
            Some(Self::Data)
        } else if extension.is_some_and(|extension| extension == "arrow")
            && location.split('/').any(|segment| segment == "deletions")
        {
            Some(Self::Deletion)
        } else {
            None
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Self::Data => "data file",
            Self::Deletion => "deletion file",
        }
    }

    /// What fails, and what the user is spared, when a file of this kind cannot be read.
    fn consequence(self) -> &'static str {
        match self {
            Self::Data => "the query fails rather than return partial results",
            Self::Deletion => "the query fails rather than return rows the dataset deleted",
        }
    }
}

/// The message for a file `dataset`'s committed state names that is missing from
/// its storage: `object` is its path under `location`, the dataset's data path.
#[must_use]
pub fn missing_file_message(
    dataset: &str,
    kind: DataFileKind,
    object: &str,
    location: &str,
) -> String {
    format!(
        "Failed to query dataset '{dataset}' (cayenne): {} '{object}' is missing from '{location}', so {}. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: {CAYENNE_DOCS}",
        kind.noun(),
        kind.consequence(),
    )
}

/// The message for a file `dataset`'s committed state names that exists but cannot
/// be read, for `cause`.
#[must_use]
pub fn unreadable_file_message(
    dataset: &str,
    kind: DataFileKind,
    object: &str,
    location: &str,
    cause: &str,
) -> String {
    format!(
        "Failed to query dataset '{dataset}' (cayenne): {} '{object}' in '{location}' is unreadable ({cause}), so {}. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: {CAYENNE_DOCS}",
        kind.noun(),
        kind.consequence(),
    )
}

/// A file a dataset's committed state names that its storage cannot serve. Never
/// answered with fewer rows: the read that needs it fails with this error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataFileError {
    /// The object is not in the store.
    Missing {
        /// The dataset whose committed state names the file.
        dataset: String,
        /// What the file holds.
        kind: DataFileKind,
        /// The file's path under `location`.
        object: String,
        /// The dataset's data path.
        location: String,
    },
    /// The object is in the store but cannot be read as committed.
    Unreadable {
        /// The dataset whose committed state names the file.
        dataset: String,
        /// What the file holds.
        kind: DataFileKind,
        /// The file's path under `location`.
        object: String,
        /// The dataset's data path.
        location: String,
        /// Why it cannot be read.
        cause: String,
    },
}

impl Display for DataFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing {
                dataset,
                kind,
                object,
                location,
            } => f.write_str(&missing_file_message(dataset, *kind, object, location)),
            Self::Unreadable {
                dataset,
                kind,
                object,
                location,
                cause,
            } => f.write_str(&unreadable_file_message(
                dataset, *kind, object, location, cause,
            )),
        }
    }
}

impl std::error::Error for DataFileError {}

/// A store for a dataset whose data is in a local directory addressed as
/// `file:///…`: object paths are absolute paths without the leading `/`, and
/// every write is flushed before it returns. Each dataset gets its own.
#[must_use]
pub fn local_directory_store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store_occ::DurableLocalFileSystem::new())
}

/// A dataset's storage, resolved once when its table opens.
#[derive(Debug)]
pub struct DatasetStorage {
    /// The dataset's store, which refuses any path outside `prefix`.
    store: Arc<dyn ObjectStore>,
    /// The object path the dataset's data lives under.
    prefix: Path,
    /// The local directory the data lives in, when it is local. Only host features
    /// that need a filesystem path use it (free-space probe, storage class,
    /// snapshot export, `file_create` safety checks, page-cache hints); no data
    /// file is read, written, copied, renamed, deleted or listed through it.
    local_root: Option<PathBuf>,
    /// The key the store is registered under in a `RuntimeEnv`: `cayenne://<id>/`.
    url: url::Url,
    /// `cayenne://<id>/<prefix>`: the table path the data path builds its URLs on.
    data_root: String,
    /// `scheme://host` of the recorded table path (`s3://bucket`, `file://`).
    recorded_authority: String,
    /// The dataset's name and its data path as recorded, for the errors that name
    /// a file it cannot read.
    dataset: Arc<str>,
    table_path: Arc<str>,
    /// Whether the dataset's data is in the object-native location
    /// ([`crate::metastore_layout::is_object_native_data_path`]).
    in_location: bool,
    /// The table's record of the files its writes have not committed yet, for a
    /// dataset in the location: its deletion-vector writes hold their files there
    /// until a commit names them (see [`crate::provider::uncommitted_files`]).
    writer_record: Option<Arc<crate::provider::uncommitted_files::UncommittedFiles>>,
}

impl DatasetStorage {
    /// The storage of dataset `dataset`'s table whose recorded data path is
    /// `table_path`, an object-store URL, reached through `store` (rooted where
    /// `table_path`'s object paths are), and registered under
    /// `cayenne://<dataset_id>/`.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_path` is not a URL with an object path, or
    /// `dataset_id` cannot be a URL host.
    pub fn new(
        dataset: &str,
        table_path: &str,
        dataset_id: &str,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, String> {
        let recorded = url::Url::parse(table_path)
            .map_err(|error| format!("data path '{table_path}' is not a URL: {error}"))?;
        let prefix = Path::from_url_path(recorded.path())
            .map_err(|error| format!("data path '{table_path}' has no object path: {error}"))?;
        let local_root = (recorded.scheme() == "file")
            .then(|| recorded.to_file_path().ok())
            .flatten();
        let url = url::Url::parse(&format!("{DATASET_STORE_SCHEME}://{dataset_id}/"))
            .map_err(|error| format!("dataset id '{dataset_id}' cannot name a store: {error}"))?;
        let recorded_authority = format!(
            "{}://{}",
            recorded.scheme(),
            recorded.host_str().unwrap_or_default()
        );
        let data_root = if prefix.as_ref().is_empty() {
            format!("{DATASET_STORE_SCHEME}://{dataset_id}")
        } else {
            format!("{DATASET_STORE_SCHEME}://{dataset_id}/{prefix}")
        };
        let dataset: Arc<str> = Arc::from(dataset);
        let table_path: Arc<str> = Arc::from(table_path.trim_end_matches('/'));
        let scoped: Arc<dyn ObjectStore> = Arc::new(DatasetScopedStore {
            inner: store,
            prefix: prefix.clone(),
            authority: recorded_authority.clone(),
            dataset: Arc::clone(&dataset),
            table_path: Arc::clone(&table_path),
        });
        Ok(Self {
            store: scoped,
            in_location: crate::metastore_layout::is_object_native_data_path(&table_path),
            prefix,
            local_root,
            url,
            data_root,
            recorded_authority,
            dataset,
            table_path,
            writer_record: None,
        })
    }

    /// Gives the dataset's deletion-vector writes the table's record of
    /// uncommitted files, for a dataset in the object-native location; other
    /// datasets keep none.
    #[must_use]
    pub(crate) fn with_writer_record(
        mut self,
        record: Arc<crate::provider::uncommitted_files::UncommittedFiles>,
    ) -> Self {
        if self.in_location {
            self.writer_record = Some(record);
        }
        self
    }

    /// The table's record of uncommitted files, for a dataset in the location.
    pub(crate) fn writer_record(
        &self,
    ) -> Option<&crate::provider::uncommitted_files::UncommittedFiles> {
        self.writer_record.as_deref()
    }

    /// Whether the dataset's data is in the object-native location: its committed
    /// file sets are the only record of its files, so nothing it names may be
    /// tolerated as missing.
    #[must_use]
    pub fn in_location(&self) -> bool {
        self.in_location
    }

    /// The error for a file of this dataset at object path `location` that is
    /// missing from the store.
    #[must_use]
    pub fn missing(&self, kind: DataFileKind, location: &Path) -> DataFileError {
        DataFileError::Missing {
            dataset: self.dataset.to_string(),
            kind,
            object: relative_object(&self.prefix, location),
            location: self.table_path.to_string(),
        }
    }

    /// The dataset's store. It refuses any path outside [`Self::prefix`].
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The object path the dataset's data lives under.
    #[must_use]
    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// The local directory the data lives in, when it is local; for host features only.
    #[must_use]
    pub fn local_root(&self) -> Option<&std::path::Path> {
        self.local_root.as_deref()
    }

    /// Whether the dataset's data lives in a local directory.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.local_root.is_some()
    }

    /// The URL the store is registered under: `cayenne://<id>/`.
    #[must_use]
    pub fn url(&self) -> &url::Url {
        &self.url
    }

    /// `scheme://host` of the recorded table path (`s3://bucket`, `file://`): which
    /// storage service the dataset's data lives in.
    #[must_use]
    pub fn recorded_authority(&self) -> &str {
        &self.recorded_authority
    }

    /// `cayenne://<id>/<prefix>`, the table path every data URL is built on.
    #[must_use]
    pub fn data_root(&self) -> &str {
        &self.data_root
    }

    /// Registers the dataset's store in `runtime_env` under [`Self::url`], unless it
    /// is already there. Nothing else's registration is touched.
    pub fn register(&self, runtime_env: &RuntimeEnv) {
        let registered = runtime_env
            .object_store_registry
            .get_store(&self.url)
            .is_ok_and(|existing| Arc::ptr_eq(&existing, &self.store));
        if !registered {
            runtime_env.register_object_store(&self.url, Arc::clone(&self.store));
        }
    }

    /// The object path of `location`, a file of this dataset addressed by its
    /// recorded URL (`s3://bucket/…`, `file:///…`) or by its run-time URL
    /// (`cayenne://<id>/…`); `Ok(None)` for a plain local path.
    ///
    /// # Errors
    ///
    /// Returns an error for a URL under another scheme or host: no file of this
    /// dataset lives there.
    pub fn resolve(&self, location: &str) -> Result<Option<Path>, ObjectStoreError> {
        let Ok(url) = url::Url::parse(location) else {
            return Ok(None);
        };
        let authority = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
        let own = format!(
            "{}://{}",
            self.url.scheme(),
            self.url.host_str().unwrap_or_default()
        );
        if authority != self.recorded_authority && authority != own {
            return Err(outside(&self.recorded_authority, location));
        }
        Path::from_url_path(url.path())
            .map(Some)
            .map_err(|error| ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(error),
            })
    }
}

/// `location` relative to the dataset's `prefix`, as a user finds it under the
/// dataset's data path.
fn relative_object(prefix: &Path, location: &Path) -> String {
    let location = location.as_ref();
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return location.to_string();
    }
    location
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(location)
        .to_string()
}

/// What one batched delete did to each path it was given.
#[derive(Debug, Default)]
pub(crate) struct BatchedDelete {
    /// Paths the store reports deleted by this call.
    pub deleted: std::collections::HashSet<Path>,
    /// Paths the store reports absent: deleted by an earlier pass, so this call
    /// reclaimed nothing for them.
    pub missing: std::collections::HashSet<Path>,
    /// Paths given but neither deleted nor absent, so they may still be stored.
    pub failed: usize,
    /// The first error for a failed path.
    pub first_error: Option<ObjectStoreError>,
}

/// Delete `paths` through `store`'s `delete_stream`, which an S3 store sends as
/// `DeleteObjects` requests of up to [`DELETE_BATCH`] keys each, instead of one
/// request per object. Every path is attempted; a failure of one does not stop
/// the others.
pub(crate) async fn delete_in_batches(
    store: &Arc<dyn ObjectStore>,
    paths: Vec<Path>,
) -> BatchedDelete {
    let mut outcome = BatchedDelete::default();
    if paths.is_empty() {
        return outcome;
    }
    let given: std::collections::HashSet<Path> = paths.iter().cloned().collect();
    let mut results = store.delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed());
    while let Some(result) = results.next().await {
        match result {
            Ok(path) => {
                outcome.deleted.insert(path);
            }
            Err(ObjectStoreError::NotFound { path, .. }) => {
                outcome.missing.insert(Path::from(path));
            }
            Err(error) => {
                if outcome.first_error.is_none() {
                    outcome.first_error = Some(error);
                }
            }
        }
    }
    outcome.failed = given
        .iter()
        .filter(|path| !outcome.deleted.contains(*path) && !outcome.missing.contains(*path))
        .count();
    outcome
}

fn outside(authority: &str, location: &str) -> ObjectStoreError {
    let shown = if location.contains("://") {
        location.to_string()
    } else {
        format!("{authority}/{location}")
    };
    ObjectStoreError::Generic {
        store: STORE_NAME,
        source: format!(
            "No Cayenne dataset's storage contains '{shown}', so it was not accessed. A dataset reads and writes only under its own data path."
        )
        .into(),
    }
}

/// A dataset's store: every request outside the dataset's prefix is refused.
#[derive(Debug)]
struct DatasetScopedStore {
    inner: Arc<dyn ObjectStore>,
    prefix: Path,
    /// `scheme://host` of the recorded table path, for error messages.
    authority: String,
    /// The dataset's name and recorded data path, for the error that names a
    /// committed file the store cannot serve.
    dataset: Arc<str>,
    table_path: Arc<str>,
}

impl DatasetScopedStore {
    /// A failed read of `location` as the dataset sees it: a committed file that is
    /// not in the store keeps its `NotFound` kind, so callers that probe for a file
    /// still recognize it, and carries [`DataFileError::Missing`] as its source; a
    /// committed file shorter than the read needs is [`DataFileError::Unreadable`].
    /// Anything else, including every other file, is returned as it was.
    async fn read_failed(
        &self,
        location: &Path,
        needed_until: Option<u64>,
        error: ObjectStoreError,
    ) -> ObjectStoreError {
        let Some(kind) = DataFileKind::of(location.as_ref()) else {
            return error;
        };
        let object = relative_object(&self.prefix, location);
        match error {
            ObjectStoreError::NotFound { path, .. } => ObjectStoreError::NotFound {
                path,
                source: Box::new(DataFileError::Missing {
                    dataset: self.dataset.to_string(),
                    kind,
                    object,
                    location: self.table_path.to_string(),
                }),
            },
            error => {
                // A read past the end of the object: it is shorter than the file
                // its commit recorded, so no retry can read it.
                let Some(needed) = needed_until else {
                    return error;
                };
                match self.inner.head(location).await {
                    Ok(meta) if meta.size < needed => self.truncated(location, meta.size, needed),
                    _ => error,
                }
            }
        }
    }

    /// The error for committed file `location`, of `size` bytes, read up to
    /// byte `needed`.
    fn truncated(&self, location: &Path, size: u64, needed: u64) -> ObjectStoreError {
        let kind = DataFileKind::of(location.as_ref()).unwrap_or(DataFileKind::Data);
        ObjectStoreError::Generic {
            store: STORE_NAME,
            source: Box::new(DataFileError::Unreadable {
                dataset: self.dataset.to_string(),
                kind,
                object: relative_object(&self.prefix, location),
                location: self.table_path.to_string(),
                cause: format!(
                    "it has {size} bytes, fewer than the {needed} the read needs, so it was truncated"
                ),
            }),
        }
    }

    fn under(prefix: &str, location: &str) -> bool {
        prefix.is_empty()
            || location == prefix
            || location
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    fn check(&self, location: &Path) -> Result<(), ObjectStoreError> {
        if Self::under(self.prefix.as_ref(), location.as_ref()) {
            Ok(())
        } else {
            Err(outside(&self.authority, location.as_ref()))
        }
    }

    fn check_prefix(&self, prefix: Option<&Path>) -> Result<(), ObjectStoreError> {
        let prefix = prefix.map_or("", Path::as_ref);
        if Self::under(self.prefix.as_ref(), prefix) {
            Ok(())
        } else {
            Err(outside(&self.authority, prefix))
        }
    }
}

impl Display for DatasetScopedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DatasetStore({}/{})", self.authority, self.prefix)
    }
}

#[async_trait]
impl ObjectStore for DatasetScopedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        self.check(location)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        self.check(location)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        self.check(location)?;
        let needed_until = match &options.range {
            Some(object_store::GetRange::Bounded(range)) => Some(range.end),
            Some(object_store::GetRange::Offset(start)) => Some(start.saturating_add(1)),
            _ => None,
        };
        let result = match self.inner.get_opts(location, options).await {
            Ok(result) => result,
            Err(error) => return Err(self.read_failed(location, needed_until, error).await),
        };
        // A committed file shorter than the range read from it: some stores
        // answer such a read with what the object has.
        if let Some(needed) = needed_until.filter(|needed| result.range.end < *needed)
            && DataFileKind::of(location.as_ref()).is_some()
        {
            return Err(self.truncated(location, result.meta.size, needed));
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, ObjectStoreError> {
        self.check(location)?;
        match self.inner.get_ranges(location, ranges).await {
            Ok(bytes)
                if DataFileKind::of(location.as_ref()).is_some()
                    && bytes
                        .iter()
                        .zip(ranges)
                        .any(|(got, range)| (got.len() as u64) < range.end - range.start) =>
            {
                let needed = ranges
                    .iter()
                    .map(|range| range.end)
                    .max()
                    .unwrap_or_default();
                let size = match self.inner.head(location).await {
                    Ok(meta) => meta.size,
                    Err(error) => {
                        return Err(self.read_failed(location, Some(needed), error).await);
                    }
                };
                Err(self.truncated(location, size, needed))
            }
            Ok(bytes) => Ok(bytes),
            Err(error) => {
                let needed_until = ranges.iter().map(|range| range.end).max();
                Err(self.read_failed(location, needed_until, error).await)
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, ObjectStoreError>>,
    ) -> BoxStream<'static, Result<Path, ObjectStoreError>> {
        let prefix = self.prefix.clone();
        let authority = self.authority.clone();
        let inner = Arc::clone(&self.inner);
        // Paths outside the dataset fail in place; each batch of the rest goes to
        // the inner store's own batched delete.
        locations
            .ready_chunks(DELETE_BATCH)
            .flat_map(move |batch| {
                let mut inside = Vec::with_capacity(batch.len());
                let mut failures = Vec::new();
                for item in batch {
                    match item {
                        Ok(path) if Self::under(prefix.as_ref(), path.as_ref()) => {
                            inside.push(path);
                        }
                        Ok(path) => failures.push(Err(outside(&authority, path.as_ref()))),
                        Err(error) => failures.push(Err(error)),
                    }
                }
                let deleted = if inside.is_empty() {
                    stream::empty().boxed()
                } else {
                    inner.delete_stream(stream::iter(inside.into_iter().map(Ok)).boxed())
                };
                stream::iter(failures).chain(deleted)
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        match self.check_prefix(prefix) {
            Ok(()) => self.inner.list(prefix),
            Err(error) => stream::once(async { Err(error) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        match self.check_prefix(prefix) {
            Ok(()) => self.inner.list_with_offset(prefix, offset),
            Err(error) => stream::once(async { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        self.check_prefix(prefix)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), ObjectStoreError> {
        self.check(from)?;
        self.check(to)?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        self.check(from)?;
        self.check(to)?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    fn storage(table_path: &str, store: &Arc<dyn ObjectStore>) -> DatasetStorage {
        DatasetStorage::new(
            "orders",
            table_path,
            "0193aaaa-bbbb-7ccc-8ddd-eeeeffff0000",
            Arc::clone(store),
        )
        .expect("storage")
    }

    #[test]
    fn the_missing_and_unreadable_texts_name_the_file_the_location_and_the_fix() {
        let location = "s3://bucket/state/cayenne/v1/datasets/orders-0190c2c4-0000-7000-8000-000000000000/data";
        assert_eq!(
            missing_file_message("orders", DataFileKind::Data, "t/s/a.vortex", location),
            "Failed to query dataset 'orders' (cayenne): data file 't/s/a.vortex' is missing from 's3://bucket/state/cayenne/v1/datasets/orders-0190c2c4-0000-7000-8000-000000000000/data', so the query fails rather than return partial results. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
        assert_eq!(
            unreadable_file_message(
                "orders",
                DataFileKind::Data,
                "t/s/a.vortex",
                location,
                "it has 10 bytes, fewer than the 20 the read needs, so it was truncated"
            ),
            "Failed to query dataset 'orders' (cayenne): data file 't/s/a.vortex' in 's3://bucket/state/cayenne/v1/datasets/orders-0190c2c4-0000-7000-8000-000000000000/data' is unreadable (it has 10 bytes, fewer than the 20 the read needs, so it was truncated), so the query fails rather than return partial results. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
        assert_eq!(
            missing_file_message(
                "orders",
                DataFileKind::Deletion,
                "t/s/deletions/delete_1.arrow",
                location
            ),
            "Failed to query dataset 'orders' (cayenne): deletion file 't/s/deletions/delete_1.arrow' is missing from 's3://bucket/state/cayenne/v1/datasets/orders-0190c2c4-0000-7000-8000-000000000000/data', so the query fails rather than return rows the dataset deleted. To reload the dataset from its source, start it once with `acceleration.mode: file_create`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
    }

    #[test]
    fn only_committed_files_are_named_in_a_read_error() {
        assert_eq!(DataFileKind::of("p/t/s/a.vortex"), Some(DataFileKind::Data));
        assert_eq!(
            DataFileKind::of("p/t/s/deletions/delete_1.arrow"),
            Some(DataFileKind::Deletion)
        );
        assert_eq!(DataFileKind::of("p/t/_staging_s/a.vortex"), None);
        assert_eq!(DataFileKind::of("p/t/_staging_s/_wal.json"), None);
        assert_eq!(DataFileKind::of("p/t/s/other.arrow"), None);
    }

    #[tokio::test]
    async fn a_missing_committed_file_keeps_not_found_and_names_itself() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = storage("s3://bucket/p/a", &inner);
        let error = storage
            .store()
            .get(&Path::from("p/a/t/s/x.vortex"))
            .await
            .expect_err("missing");
        let ObjectStoreError::NotFound { source, .. } = &error else {
            panic!("a missing data file is still NotFound: {error}");
        };
        assert_eq!(
            source.to_string(),
            missing_file_message(
                "orders",
                DataFileKind::Data,
                "t/s/x.vortex",
                "s3://bucket/p/a"
            )
        );
        let staged = storage
            .store()
            .get(&Path::from("p/a/t/_staging_s/_wal.json"))
            .await
            .expect_err("missing");
        assert!(
            !staged.to_string().contains("Failed to query dataset"),
            "a staging file keeps the store's own error: {staged}"
        );
    }

    #[tokio::test]
    async fn a_path_inside_the_dataset_reaches_the_store() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = storage("s3://bucket/base/orders", &inner);
        storage
            .store()
            .put(
                &Path::from("base/orders/t/1.vortex"),
                PutPayload::from_static(b"a"),
            )
            .await
            .expect("put under the dataset");
        let keys: Vec<String> = inner
            .list(None)
            .map(|meta| meta.expect("list").location.to_string())
            .collect()
            .await;
        assert_eq!(keys, vec!["base/orders/t/1.vortex".to_string()]);
        assert_eq!(
            storage.data_root(),
            "cayenne://0193aaaa-bbbb-7ccc-8ddd-eeeeffff0000/base/orders"
        );
        assert_eq!(
            storage.url().as_str(),
            "cayenne://0193aaaa-bbbb-7ccc-8ddd-eeeeffff0000/"
        );
        assert!(!storage.is_local());
    }

    #[tokio::test]
    async fn a_path_sharing_only_a_string_prefix_is_outside() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = storage("s3://bucket/base/orders", &inner);
        let error = storage
            .store()
            .get(&Path::from("base/orders_2/t/1.vortex"))
            .await
            .expect_err("a sibling dataset's path is refused");
        assert_eq!(
            error.to_string(),
            "Generic cayenne error: No Cayenne dataset's storage contains 's3://bucket/base/orders_2/t/1.vortex', so it was not accessed. A dataset reads and writes only under its own data path."
        );
        let listed: Vec<_> = storage.store().list(None).collect().await;
        assert!(
            matches!(listed.as_slice(), [Err(_)]),
            "listing the whole store is refused: {listed:?}"
        );
    }

    #[tokio::test]
    async fn deletes_outside_the_dataset_fail_in_place() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in ["p/a/1", "p/b/1"] {
            inner
                .put(&Path::from(key), PutPayload::from_static(b"x"))
                .await
                .expect("seed");
        }
        let storage = storage("s3://bucket/p/a", &inner);
        let results: Vec<_> = storage
            .store()
            .delete_stream(stream::iter(["p/a/1", "p/b/1"].map(|k| Ok(Path::from(k)))).boxed())
            .collect()
            .await;
        let deleted: Vec<String> = results
            .iter()
            .filter_map(|r| r.as_ref().ok().map(ToString::to_string))
            .collect();
        assert_eq!(deleted, vec!["p/a/1".to_string()], "{results:?}");
        assert_eq!(
            results.iter().filter(|r| r.is_err()).count(),
            1,
            "{results:?}"
        );
        assert!(
            inner.head(&Path::from("p/a/1")).await.is_err(),
            "deleted inside"
        );
        assert!(
            inner.head(&Path::from("p/b/1")).await.is_ok(),
            "kept outside"
        );
    }

    #[tokio::test]
    async fn copy_out_of_the_dataset_is_refused() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner
            .put(&Path::from("p/a/1"), PutPayload::from_static(b"x"))
            .await
            .expect("seed");
        let storage = storage("s3://bucket/p/a", &inner);
        storage
            .store()
            .copy(&Path::from("p/a/1"), &Path::from("p/b/1"))
            .await
            .expect_err("a copy out of the dataset is refused");
        assert!(
            inner.head(&Path::from("p/b/1")).await.is_err(),
            "nothing copied"
        );
    }

    #[test]
    fn resolve_accepts_the_recorded_and_the_run_time_url_and_nothing_else() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = storage("s3://bucket/p/a", &inner);
        assert_eq!(
            storage
                .resolve("s3://bucket/p/a/s/deletions/d.arrow")
                .expect("recorded"),
            Some(Path::from("p/a/s/deletions/d.arrow"))
        );
        assert_eq!(
            storage
                .resolve("cayenne://0193aaaa-bbbb-7ccc-8ddd-eeeeffff0000/p/a/s/x.vortex")
                .expect("run time"),
            Some(Path::from("p/a/s/x.vortex"))
        );
        assert_eq!(storage.resolve("/tmp/local/x").expect("plain path"), None);
        assert!(
            storage.resolve("s3://other/p/a/x").is_err(),
            "another bucket"
        );
    }

    #[test]
    fn a_file_url_is_local_and_keeps_its_directory() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = storage("file:///var/data/orders/", &inner);
        assert_eq!(
            storage.local_root(),
            Some(std::path::Path::new("/var/data/orders/"))
        );
        assert_eq!(storage.prefix().as_ref(), "var/data/orders");
    }
}
