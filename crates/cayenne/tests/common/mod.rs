/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Common test utilities for Cayenne with multiple metastore backends

pub mod lookup_index;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig};
use cayenne::metastore::object_native::{
    NativeCatalogConfig, ObjectNativeCatalog, new_dataset_folder,
};
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;

pub type FixtureCatalog = dyn MetadataCatalog;

/// In-memory object stores for `CAYENNE_TEST_CATALOG=native`, one per fixture, so a
/// test that reopens its catalog finds the same objects.
static NATIVE_STORES: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<String, Arc<object_store::memory::InMemory>>>,
> = std::sync::LazyLock::new(Default::default);

/// The location of each [`BackendType::ObjectNative`] fixture's catalog, by the
/// database path in its connection string, so [`fixture_catalog`] reopens it.
static OBJECT_NATIVE_LOCATIONS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<String, ObjectNativeLocation>>,
> = std::sync::LazyLock::new(Default::default);

/// The connection-string scheme of a [`BackendType::ObjectNative`] fixture.
const OBJECT_NATIVE_SCHEME: &str = "object-native://";

/// Why the harness refuses the SQLite metastore with data files in an object store.
pub const UNSUPPORTED_PAIRING: &str = "Cayenne pairs the SQLite metastore only with local data files \
    (`runtime.params.cayenne_vnext` off) and data files in an object store only with the \
    object-native metastore (`cayenne_vnext` on); the SQLite metastore with object-store data \
    files is not a supported configuration. Use `BackendType::ObjectNative` for a test whose \
    data files are in an object store";

/// The head-log tuning of every object-native catalog the harness opens: tiny
/// batches and a checkpoint at every sealed batch, so the suites seal,
/// checkpoint and reopen from checkpoint roots constantly.
fn harness_native_config() -> NativeCatalogConfig {
    NativeCatalogConfig {
        log: object_store_occ::headlog::HeadLogConfig {
            batch_max_records: 2,
            ..object_store_occ::headlog::HeadLogConfig::default()
        },
        checkpoint_after_batches: 1,
        ..NativeCatalogConfig::default()
    }
}

/// Select a catalog only inside the integration harness. Reopens use the same
/// object store; the native branches never construct a SQL catalog.
///
/// A [`BackendType::ObjectNative`] fixture's connection string reopens its own
/// catalog, in the object store beside its data files. Otherwise
/// `CAYENNE_TEST_CATALOG` chooses, with the data files staying local:
///
/// - unset or `sqlite`: the SQLite metastore.
/// - `native`: an object-native catalog in an in-memory object store, so suite
///   speed does not depend on fsync.
/// - `native-durable`: an fsync'd local directory, for durable-write coverage.
/// - `native-nosync`: the same directory without fsync, to measure what durability costs.
pub async fn fixture_catalog(
    connection_string: String,
) -> Result<Arc<FixtureCatalog>, Box<dyn std::error::Error>> {
    if let Some(database) = connection_string.strip_prefix(OBJECT_NATIVE_SCHEME) {
        let location = OBJECT_NATIVE_LOCATIONS
            .lock()
            .get(database)
            .cloned()
            .ok_or_else(|| format!("no object-native fixture catalog for '{database}'"))?;
        return Ok(Arc::new(location.open().await?));
    }
    match std::env::var("CAYENNE_TEST_CATALOG").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("sqlite") => {
            Ok(Arc::new(CayenneCatalog::new(connection_string)?))
        }
        Ok(selection @ ("native" | "native-durable" | "native-nosync")) => {
            use object_store::{ObjectStore, ObjectStoreExt, path::Path};
            let database = connection_string
                .strip_prefix("sqlite://")
                .ok_or("native harness requires a local SQLite fixture path")?;
            let directory = std::path::PathBuf::from(format!("{database}.native"));
            tokio::fs::create_dir_all(&directory).await?;
            let store: Arc<dyn ObjectStore> = match selection {
                "native" => Arc::clone(
                    NATIVE_STORES
                        .lock()
                        .entry(database.to_string())
                        .or_insert_with(|| Arc::new(object_store::memory::InMemory::new())),
                ) as Arc<dyn ObjectStore>,
                "native-nosync" => {
                    Arc::new(object_store_occ::LocalConditionalPut::new(&directory)?)
                }
                _ => Arc::new(object_store_occ::LocalConditionalPut::new_durable(
                    &directory,
                )?),
            };
            let prefix = Path::from("catalog");
            let config = harness_native_config();
            let catalog = match store.head(&prefix.clone().join("head")).await {
                Ok(_) => {
                    ObjectNativeCatalog::open_with_config(
                        Arc::clone(&store),
                        prefix.clone(),
                        config,
                    )
                    .await?
                }
                Err(object_store::Error::NotFound { .. }) => {
                    ObjectNativeCatalog::create_with_config(
                        Arc::clone(&store),
                        prefix.clone(),
                        config,
                    )
                    .await?
                }
                Err(error) => return Err(error.into()),
            };
            let head = store.head(&prefix.join("head")).await?;
            assert!(head.size > 0, "native catalog must persist a head object");
            eprintln!(
                "CAYENNE_TEST_CATALOG={selection}: head {} ({} bytes)",
                head.location, head.size
            );
            Ok(Arc::new(catalog))
        }
        value => Err(format!("invalid CAYENNE_TEST_CATALOG selection: {value:?}").into()),
    }
}

/// Where a [`BackendType::ObjectNative`] fixture keeps its catalog and data files:
/// one object store, laid out as the runtime lays out `runtime.state.location`
/// under `runtime.params.cayenne_vnext`. The catalog's head log is at
/// `<root>/state/cayenne/v1/catalog`, and each dataset's data files at
/// `<root>/state/cayenne/v1/datasets/<folder>/data`, the placement the runtime
/// gives a new dataset (`object_native_data_placement` in `accelerator-cayenne`).
#[derive(Clone)]
struct ObjectNativeLocation {
    store: Arc<dyn object_store::ObjectStore>,
    /// `s3://<bucket>[/<fixture prefix>]/state`, or `file:///<directory>`, the
    /// runtime's `runtime.state.location`.
    state_url: String,
    /// The same location as a key prefix in `store`.
    state_prefix: String,
    /// The URL `store`'s keys are relative to: `s3://<bucket>`, or `file://` for
    /// a local directory, whose store is keyed by absolute paths.
    root_url: String,
}

impl ObjectNativeLocation {
    /// A location in `store`'s bucket `bucket`, under `root` (empty for the bucket root).
    fn new(store: Arc<dyn object_store::ObjectStore>, bucket: &str, root: &str) -> Self {
        let state_prefix = if root.is_empty() {
            "state".to_string()
        } else {
            format!("{}/state", root.trim_matches('/'))
        };
        Self {
            store,
            state_url: format!("s3://{bucket}/{state_prefix}"),
            state_prefix,
            root_url: format!("s3://{bucket}"),
        }
    }

    /// A location in local directory `directory`, in the store the runtime builds
    /// for a `file://` `runtime.state.location`: durable, and keyed by absolute
    /// paths (`runtime_state_location::build_object_store`).
    fn local(directory: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(directory)?;
        let store =
            object_store_occ::LocalConditionalPut::new_durable_keyed_by_absolute_path(directory)?;
        let state_prefix = directory.to_string_lossy().trim_matches('/').to_string();
        Ok(Self {
            store: Arc::new(store),
            state_url: format!("file:///{state_prefix}"),
            state_prefix,
            root_url: "file://".to_string(),
        })
    }

    /// Open the catalog, creating it on first use, as the runtime opens it.
    async fn open(&self) -> Result<ObjectNativeCatalog, Box<dyn std::error::Error>> {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(&self.store),
            object_store::path::Path::from(format!("{}/cayenne/v1/catalog", self.state_prefix)),
            &format!("{}/cayenne/v1", self.state_url),
            harness_native_config(),
        )
        .await?;
        Ok(catalog)
    }

    /// The data store a placed dataset's table is given: the store, under the
    /// URL of the catalog's dataset folders, as the runtime gives it.
    fn data_store(
        &self,
        catalog: &ObjectNativeCatalog,
    ) -> Result<FixtureDataStore, url::ParseError> {
        let url = url::Url::parse(&format!("{}/{}", self.root_url, catalog.datasets_prefix()))?;
        Ok(FixtureDataStore {
            root: url.as_str().trim_end_matches('/').to_string(),
            config: Some(ObjectStoreConfig {
                url,
                store: Arc::clone(&self.store),
            }),
        })
    }
}

/// The metastore and data-store pair a fixture runs on: the two Cayenne
/// supports, one for each `runtime.params.cayenne_vnext` setting. The SQLite
/// metastore never pairs with data files in an object store:
/// [`TestFixture::with_data_store`] and [`table_builder_on_store`] refuse it
/// ([`UNSUPPORTED_PAIRING`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    /// `cayenne_vnext` off: the SQLite metastore, with data files in the
    /// fixture's local data directory. `CAYENNE_TEST_CATALOG` may swap the
    /// metastore for an object-native one; the data files stay local.
    Sqlite,
    /// `cayenne_vnext` on: the object-native metastore and the data files in one
    /// object store, each dataset placed in the location's dataset folders as the
    /// runtime places it, so its scans read the files its commits name. The store
    /// is the one `CAYENNE_TEST_DATA_STORE` names, or an in-memory one when that
    /// is `local`.
    ObjectNative,
}

impl BackendType {
    pub fn name(self) -> &'static str {
        match self {
            BackendType::Sqlite => "SQLite",
            BackendType::ObjectNative => "object-native",
        }
    }

    /// The backend of a test that follows `CAYENNE_TEST_DATA_STORE`: [`Self::Sqlite`]
    /// on the local store, and [`Self::ObjectNative`] on any other, because data
    /// files in an object store pair only with the object-native metastore.
    pub fn for_data_store() -> Self {
        match std::env::var("CAYENNE_TEST_DATA_STORE").as_deref() {
            Err(_) | Ok("" | "local") => Self::Sqlite,
            Ok(_) => Self::ObjectNative,
        }
    }
}

/// Test fixture that sets up a temporary directory and catalog
pub struct TestFixture {
    /// Shared with [`run_with_backend`], which keeps the directory until the
    /// test's future completes: an `async move` body that names only some of
    /// the fixture's fields captures only those, and would otherwise delete the
    /// directory as soon as the future is built.
    pub temp_dir: Arc<TempDir>,
    pub catalog: Arc<FixtureCatalog>,
    pub data_path: std::path::PathBuf,
    pub backend_type: BackendType,
    /// Where this fixture's tables keep their data files; see [`FixtureDataStore`].
    pub data_store: FixtureDataStore,
}

/// Where a fixture's tables keep their data files: the fixture's local data
/// directory for [`BackendType::Sqlite`], and for [`BackendType::ObjectNative`]
/// the dataset folders of its object-native location, in the store
/// `CAYENNE_TEST_DATA_STORE` chooses:
///
/// - unset, `local` or `memory`: an in-memory object store;
/// - `fault`: the same, wrapped in `object_store_occ`'s `FaultStore` with no faults
///   armed, so every operation goes through the fault store's wrapper;
/// - `local-location`: a local directory in the fixture's temporary directory,
///   in the durable store the runtime builds for a `file://`
///   `runtime.state.location`, keyed by absolute paths;
/// - `rustfs`: an S3-compatible server (`RustFS`) named by
///   `CAYENNE_TEST_RUSTFS_ENDPOINT`, `CAYENNE_TEST_RUSTFS_BUCKET`,
///   `CAYENNE_TEST_RUSTFS_ACCESS_KEY` and `CAYENNE_TEST_RUSTFS_SECRET_KEY`, under
///   a prefix of the fixture's own in the bucket. A fixture fails with a message
///   naming the unset variables rather than run on another store.
///
/// The catalog lives in the same store, so a placed table's files and the
/// commits that name them share the location as they do in the runtime.
/// [`FixtureDataStore::create`] and [`FixtureDataStore::open`] fail a table in
/// an object store that is not placed there.
#[derive(Clone)]
pub struct FixtureDataStore {
    /// The local data directory, or the URL of the location's dataset folders.
    root: String,
    /// The store to give `CayenneTableProviderBuilder::with_object_store`, when not local.
    pub config: Option<ObjectStoreConfig>,
}

impl FixtureDataStore {
    fn local(data_path: &std::path::Path) -> Self {
        Self {
            root: data_path.to_string_lossy().to_string(),
            config: None,
        }
    }

    /// The location of an object-native fixture, in the store
    /// `CAYENNE_TEST_DATA_STORE` names.
    fn location_from_env(
        directory: &std::path::Path,
    ) -> Result<ObjectNativeLocation, Box<dyn std::error::Error>> {
        let kind = std::env::var("CAYENNE_TEST_DATA_STORE").unwrap_or_default();
        Ok(match kind.as_str() {
            "local-location" => ObjectNativeLocation::local(&directory.join("state"))?,
            "" | "local" | "memory" => ObjectNativeLocation::new(
                Arc::new(object_store::memory::InMemory::new()),
                "cayenne-test",
                "",
            ),
            "fault" => ObjectNativeLocation::new(
                Arc::new(object_store_sim::fault::FaultStore::new(Arc::new(
                    object_store::memory::InMemory::new(),
                ))),
                "cayenne-test",
                "",
            ),
            "rustfs" => Self::rustfs()?,
            other => {
                return Err(format!(
                    "CAYENNE_TEST_DATA_STORE is '{other}'; use 'local', 'memory', 'fault', 'local-location' or 'rustfs'"
                )
                .into());
            }
        })
    }

    /// A location on the `RustFS` server the `CAYENNE_TEST_RUSTFS_*` variables
    /// name, under a prefix of this fixture's own in its bucket.
    fn rustfs() -> Result<ObjectNativeLocation, Box<dyn std::error::Error>> {
        const VARIABLES: [&str; 4] = [
            "CAYENNE_TEST_RUSTFS_ENDPOINT",
            "CAYENNE_TEST_RUSTFS_BUCKET",
            "CAYENNE_TEST_RUSTFS_ACCESS_KEY",
            "CAYENNE_TEST_RUSTFS_SECRET_KEY",
        ];
        let values: Vec<Option<String>> = VARIABLES
            .iter()
            .map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
            .collect();
        let unset: Vec<&str> = VARIABLES
            .iter()
            .zip(&values)
            .filter(|(_, value)| value.is_none())
            .map(|(name, _)| *name)
            .collect();
        let [
            Some(endpoint),
            Some(bucket),
            Some(access_key),
            Some(secret_key),
        ] = <[Option<String>; 4]>::try_from(values).map_err(|_| "four variables")?
        else {
            return Err(format!(
                "CAYENNE_TEST_DATA_STORE is 'rustfs', but {} {} not set. Start RustFS, create a bucket, and set them; \
                 .github/workflows/cayenne_object_native.yml shows how",
                unset.join(", "),
                if unset.len() == 1 { "is" } else { "are" }
            )
            .into());
        };
        let store = object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&endpoint)
            .with_bucket_name(&bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_region("us-east-1")
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(false)
            .build()?;
        let root = format!("cayenne-test/{}", uuid::Uuid::now_v7().simple());
        Ok(ObjectNativeLocation::new(Arc::new(store), &bucket, &root))
    }

    /// The `base_path` for `CreateTableOptions` of a new table `table_name`: the
    /// local data directory, or in an object store a new dataset folder in the
    /// location, `<datasets>/<folder>/data`, as the runtime places a new dataset.
    pub fn base_path(&self, table_name: &str) -> String {
        match self.config {
            None => self.root.clone(),
            Some(_) => format!(
                "{}/{}/data",
                self.root,
                cayenne::metastore::object_native::new_dataset_folder(table_name)
            ),
        }
    }

    /// Create a table over `catalog` that reads and writes through this data store.
    /// # Errors
    /// The table could not be created, or it is in an object store but not placed
    /// in the object-native location.
    pub async fn create(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
        options: CreateTableOptions,
    ) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
        self.create_with(catalog, runtime_env, options, |builder| builder)
            .await
    }

    /// [`Self::create`] with `configure` applied to the builder first, for a
    /// table that needs builder settings such as retention filters.
    /// # Errors
    /// As [`Self::create`].
    pub async fn create_with(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
        options: CreateTableOptions,
        configure: impl FnOnce(CayenneTableProviderBuilder) -> CayenneTableProviderBuilder,
    ) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
        let table = configure(self.builder(catalog, runtime_env))
            .create(options)
            .await?;
        self.check_placement(&table)?;
        Ok(table)
    }

    /// Open table `table_name` over `catalog` through this data store.
    /// # Errors
    /// As [`Self::create`].
    pub async fn open(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
        table_name: &str,
    ) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
        self.open_with(catalog, runtime_env, table_name, |builder| builder)
            .await
    }

    /// [`Self::open`] with `configure` applied to the builder first.
    /// # Errors
    /// As [`Self::create`].
    pub async fn open_with(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
        table_name: &str,
        configure: impl FnOnce(CayenneTableProviderBuilder) -> CayenneTableProviderBuilder,
    ) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
        let table = configure(self.builder(catalog, runtime_env))
            .open(table_name)
            .await?;
        self.check_placement(&table)?;
        Ok(table)
    }

    /// The bytes of a file the catalog names by `path`, such as a deletion
    /// vector's: a local path, or a URL in this data store.
    /// # Errors
    /// The file could not be read.
    pub async fn read(&self, path: &str) -> Result<bytes::Bytes, Box<dyn std::error::Error>> {
        use object_store::ObjectStoreExt;
        match &self.config {
            None => Ok(tokio::fs::read(path).await?.into()),
            Some(config) => {
                let url = url::Url::parse(path)?;
                let location = object_store::path::Path::from_url_path(url.path())?;
                Ok(config.store.get(&location).await?.bytes().await?)
            }
        }
    }

    /// Every file stored under `data_path`, a table's data path
    /// (`metadata().path`), sorted by relative path: a walk of the local
    /// directory, or a listing of the object store's prefix. The listing reads
    /// the store itself, never the files the table's commits name, so a test
    /// that counts or checks files does not take its expected answer from the
    /// code under test.
    /// # Errors
    /// The directory or the store could not be listed.
    pub async fn list_files(
        &self,
        data_path: &str,
    ) -> Result<Vec<StoredFile>, Box<dyn std::error::Error>> {
        use futures::TryStreamExt;
        let root = data_path.trim_end_matches('/');
        let mut files = Vec::new();
        match &self.config {
            None => {
                let mut directories = vec![std::path::PathBuf::from(root)];
                while let Some(directory) = directories.pop() {
                    let mut entries = match tokio::fs::read_dir(&directory).await {
                        Ok(entries) => entries,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e.into()),
                    };
                    while let Some(entry) = entries.next_entry().await? {
                        // Background cleanup can delete an entry between the
                        // directory read and this call; it is then not stored.
                        let metadata = match entry.metadata().await {
                            Ok(metadata) => metadata,
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                            Err(e) => return Err(e.into()),
                        };
                        let path = entry.path();
                        if metadata.is_dir() {
                            directories.push(path);
                            continue;
                        }
                        let relative = path
                            .strip_prefix(root)?
                            .components()
                            .map(|part| part.as_os_str().to_string_lossy().into_owned())
                            .collect::<Vec<_>>()
                            .join("/");
                        files.push(StoredFile {
                            path: path.to_string_lossy().into_owned(),
                            relative,
                            size: metadata.len(),
                        });
                    }
                }
            }
            Some(config) => {
                let url = url::Url::parse(root)?;
                let prefix = object_store::path::Path::from_url_path(url.path())?;
                let objects: Vec<object_store::ObjectMeta> =
                    config.store.list(Some(&prefix)).try_collect().await?;
                for object in objects {
                    let Some(parts) = object.location.prefix_match(&prefix) else {
                        continue;
                    };
                    let relative = parts
                        .map(|part| part.as_ref().to_string())
                        .collect::<Vec<_>>()
                        .join("/");
                    files.push(StoredFile {
                        path: format!("{root}/{relative}"),
                        relative,
                        size: object.size,
                    });
                }
            }
        }
        files.sort_by(|a, b| a.relative.cmp(&b.relative));
        Ok(files)
    }

    /// The stored data files of `snapshot_ids` of table `table_id` at
    /// `data_path`, sorted by relative path. A local table keeps each snapshot's
    /// files in a directory of its own, `<table_id>/<snapshot_id>/`, which is
    /// listed. A table in the object-native location keeps every snapshot's files
    /// in one directory, so a file is attributed to a snapshot by the snapshot's
    /// committed manifest, and only a file the store holds is returned: one the
    /// manifest names but the store lacks is missing from the result.
    /// # Errors
    /// The store could not be listed, or the catalog could not be read.
    pub async fn snapshot_data_files(
        &self,
        catalog: &FixtureCatalog,
        table_id: &str,
        data_path: &str,
        snapshot_ids: &[String],
    ) -> Result<Vec<StoredFile>, Box<dyn std::error::Error>> {
        let stored = self.list_files(data_path).await?;
        if self.config.is_none() {
            return Ok(stored
                .into_iter()
                .filter(|file| {
                    file.is_data_file()
                        && snapshot_ids.iter().any(|snapshot| {
                            file.relative
                                .starts_with(&format!("{table_id}/{snapshot}/"))
                        })
                })
                .collect());
        }
        let mut named = std::collections::HashSet::new();
        for snapshot_id in snapshot_ids {
            for file in catalog.get_snapshot_files(table_id, snapshot_id).await? {
                let name = file
                    .file_path
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_string();
                named.insert(name);
            }
        }
        Ok(stored
            .into_iter()
            .filter(|file| file.is_data_file() && named.contains(file.name()))
            .collect())
    }

    /// [`statistics_row_key`] for a table at `data_path` in this data store. A
    /// table in the object-native location keeps every snapshot's files in its
    /// table directory, so the key carries no snapshot directory.
    /// # Errors
    /// `data_path` is not a URL in an object store.
    pub fn statistics_row_key(
        &self,
        data_path: &str,
        table_id: &str,
        file: &cayenne::metadata::SnapshotFile,
    ) -> Result<String, Box<dyn std::error::Error>> {
        if self.config.is_none() {
            return Ok(statistics_row_key(
                std::path::Path::new(data_path),
                table_id,
                file,
            ));
        }
        let url = url::Url::parse(data_path)?;
        Ok(format!(
            "{}/{table_id}/{}",
            url.path().trim_matches('/'),
            file.file_path
        ))
    }

    /// Replace the stored file at `path`, as [`StoredFile::path`] names it, with
    /// `bytes`, to simulate a file damaged outside Cayenne.
    /// # Errors
    /// The file could not be written.
    pub async fn write(
        &self,
        path: &str,
        bytes: bytes::Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use object_store::ObjectStoreExt;
        match &self.config {
            None => Ok(tokio::fs::write(path, bytes).await?),
            Some(config) => {
                let url = url::Url::parse(path)?;
                let location = object_store::path::Path::from_url_path(url.path())?;
                config.store.put(&location, bytes.into()).await?;
                Ok(())
            }
        }
    }

    /// Delete the stored file at `path`, as [`StoredFile::path`] names it, to
    /// simulate a file lost outside Cayenne.
    /// # Errors
    /// The file could not be deleted.
    pub async fn delete(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        use object_store::ObjectStoreExt;
        match &self.config {
            None => Ok(tokio::fs::remove_file(path).await?),
            Some(config) => {
                let url = url::Url::parse(path)?;
                let location = object_store::path::Path::from_url_path(url.path())?;
                Ok(config.store.delete(&location).await?)
            }
        }
    }

    fn builder(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> CayenneTableProviderBuilder {
        match &self.config {
            Some(config) => table_builder_on_store(catalog, runtime_env, config.clone()),
            None => CayenneTableProviderBuilder::new(catalog, runtime_env),
        }
    }

    /// A table in an object store must be placed in the object-native location,
    /// where its scans read the files its commits name (the test the table itself
    /// makes in `data_in_object_native_location`), or the fixture would run the
    /// listing data path the runtime never gives such a table.
    fn check_placement(&self, table: &CayenneTableProvider) -> Result<(), String> {
        let path = &table.metadata().path;
        if self.config.is_none() {
            return Ok(());
        }
        if !cayenne::metastore_layout::is_object_native_data_path(path) {
            return Err(format!(
                "table '{}' keeps its data files at '{path}', which is not a dataset folder of the \
                 fixture's object-native location, so it would not read the files its commits name",
                table.metadata().table_name
            ));
        }
        eprintln!(
            "fixture table '{}' is placed in the object-native location: {path}",
            table.metadata().table_name
        );
        Ok(())
    }
}

/// A file [`FixtureDataStore::list_files`] found in a data store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    /// The path [`FixtureDataStore::read`] and [`FixtureDataStore::delete`] take:
    /// a local path, or a URL in the data store.
    pub path: String,
    /// The path relative to the listed data path, `/`-separated.
    pub relative: String,
    pub size: u64,
}

impl StoredFile {
    /// The file's name, its last path segment.
    #[must_use]
    pub fn name(&self) -> &str {
        self.relative.rsplit('/').next().unwrap_or_default()
    }

    /// Whether a writer's staging area holds the file, before any commit names it.
    #[must_use]
    pub fn is_staged(&self) -> bool {
        self.relative
            .split('/')
            .any(|segment| segment.starts_with("_staging"))
    }

    /// A Vortex data file outside a staging area.
    #[must_use]
    pub fn is_data_file(&self) -> bool {
        !self.is_staged()
            && !self.name().starts_with('.')
            && std::path::Path::new(self.name())
                .extension()
                .is_some_and(|extension| extension == "vortex")
    }

    /// A deletion vector: an Arrow IPC file under a `deletions` directory.
    #[must_use]
    pub fn is_deletion_vector(&self) -> bool {
        self.relative
            .split('/')
            .any(|segment| segment == "deletions")
            && std::path::Path::new(self.name())
                .extension()
                .is_some_and(|extension| extension == "arrow")
    }
}

/// Delete now the files a table in the object-native location keeps for the
/// data-file retention after it retires them, as if the retention had passed.
/// A local table deletes those files without a delay, so this does nothing
/// there. The orphan sweep is left alone, so a file this deletes is one the
/// table retired, not one a test helper found unnamed.
pub async fn reclaim_delayed_deletes(fixture: &TestFixture, table: &CayenneTableProvider) {
    if fixture.data_store.config.is_none() {
        return;
    }
    table.reclaim_retired_snapshots_now().await;
}

/// A table builder over `catalog` whose data files are in `config`'s store.
///
/// Every test that gives a table an object store builds it here, so the SQLite
/// metastore can never be paired with object-store data files: that pairing
/// panics with [`UNSUPPORTED_PAIRING`]. `fixture_pairing_test` holds the suite
/// to it.
/// The catalog prefix of an object-native location at `s3://bucket/state/cayenne/v1`,
/// so a dataset's data path in it has the `…/v1/datasets/<folder>/data` shape the
/// object-native metastore requires.
pub const LOCATION_CATALOG: &str = "state/cayenne/v1/catalog";

/// The data path of a new dataset `name` in `catalog`'s object-native location.
#[must_use]
pub fn location_data_url(catalog: &ObjectNativeCatalog, name: &str) -> String {
    format!(
        "s3://bucket/{}/{}/data",
        catalog.datasets_prefix(),
        new_dataset_folder(name)
    )
}

pub fn table_builder_on_store(
    catalog: Arc<dyn MetadataCatalog>,
    runtime_env: Arc<RuntimeEnv>,
    config: ObjectStoreConfig,
) -> CayenneTableProviderBuilder {
    assert!(
        !catalog.as_any().is::<CayenneCatalog>(),
        "{UNSUPPORTED_PAIRING} (data files at '{}')",
        config.url
    );
    CayenneTableProviderBuilder::new(catalog, runtime_env).with_object_store(config)
}

impl TestFixture {
    /// Create a new test fixture with the specified backend
    pub async fn new(backend: BackendType) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_data_store(backend, None).await
    }

    /// A fixture on `backend`; with `store`, an object-native one whose catalog and
    /// data files are in `store`. The SQLite backend takes no store
    /// ([`UNSUPPORTED_PAIRING`]).
    pub async fn with_data_store(
        backend: BackendType,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = Arc::new(TempDir::new()?);
        match (backend, store) {
            (BackendType::Sqlite, Some(_)) => Err(UNSUPPORTED_PAIRING.into()),
            (BackendType::Sqlite, None) => Self::build(temp_dir, backend, None).await,
            (BackendType::ObjectNative, store) => {
                let location = match store {
                    Some(store) => ObjectNativeLocation::new(store, "cayenne-test", ""),
                    None => FixtureDataStore::location_from_env(temp_dir.path())?,
                };
                Self::build(temp_dir, backend, Some(location)).await
            }
        }
    }

    /// An object-native fixture in the store `CAYENNE_TEST_DATA_STORE` names, with
    /// the catalog and every table going through `wrap(store)`. Returns the fixture
    /// and the unwrapped store, for reading and writing objects outside Cayenne.
    pub async fn object_native_through(
        wrap: impl FnOnce(Arc<dyn object_store::ObjectStore>) -> Arc<dyn object_store::ObjectStore>,
    ) -> Result<(Self, Arc<dyn object_store::ObjectStore>), Box<dyn std::error::Error>> {
        let temp_dir = Arc::new(TempDir::new()?);
        let mut location = FixtureDataStore::location_from_env(temp_dir.path())?;
        let raw = Arc::clone(&location.store);
        location.store = wrap(Arc::clone(&raw));
        let fixture = Self::build(temp_dir, BackendType::ObjectNative, Some(location)).await?;
        Ok((fixture, raw))
    }

    async fn build(
        temp_dir: Arc<TempDir>,
        backend: BackendType,
        location: Option<ObjectNativeLocation>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let data_path = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_path)?;
        let db_path = temp_dir.path().join("test.db");

        let (catalog, data_store): (Arc<FixtureCatalog>, FixtureDataStore) = match location {
            None => (
                fixture_catalog(format!("sqlite://{}", db_path.to_string_lossy())).await?,
                FixtureDataStore::local(&data_path),
            ),
            Some(location) => {
                let catalog = location.open().await?;
                let data_store = location.data_store(&catalog)?;
                OBJECT_NATIVE_LOCATIONS
                    .lock()
                    .insert(db_path.to_string_lossy().to_string(), location);
                (Arc::new(catalog) as Arc<FixtureCatalog>, data_store)
            }
        };
        catalog.init().await?;

        Ok(Self {
            temp_dir,
            catalog,
            data_path,
            backend_type: backend,
            data_store,
        })
    }

    /// Get the database path for SQLite-specific verification
    pub fn db_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("test.db")
    }

    /// Connection string for this fixture's catalog, which [`fixture_catalog`]
    /// reopens. Tests that simulate a restart (reopen the catalog) use this
    /// rather than building their own.
    pub fn connection_string(&self) -> String {
        let db_path = self.db_path();
        match self.backend_type {
            BackendType::Sqlite => format!("sqlite://{}", db_path.to_string_lossy()),
            BackendType::ObjectNative => {
                format!("{OBJECT_NATIVE_SCHEME}{}", db_path.to_string_lossy())
            }
        }
    }
}

/// Wrap one record batch in a [`SendableRecordBatchStream`].
pub fn single_batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

/// Stack size for the thread a backend-parameterized test body runs on.
///
/// The mutation and cold-tier workloads plan and unparse deeply enough to need
/// more than the 2 MiB std gives a thread (#12436). 16 MiB matches the headroom
/// `runtime`'s own deep-plan tests reserve
/// (`crates/runtime/tests/cayenne/transaction.rs`), and is address space
/// reserved rather than memory committed.
pub const TEST_STACK_SIZE: usize = 16 * 1024 * 1024;

/// Fails the calling test unless it runs in a process of its own.
///
/// nextest runs every test in its own process; plain `cargo test` runs a
/// binary's tests on threads of one process. A test that changes process-global
/// state calls this before changing it, so under `cargo test` it fails with
/// instructions instead of changing the state every neighbouring test runs under.
/// `why` names that state.
pub fn require_process_per_test(why: &str) {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "{why}, so it must run in a process of its own. Run it with \
         `cargo nextest run -p cayenne <test name>`: plain \
         `cargo test` runs every test in the binary on threads of one process"
    );
}

/// Run a test on the SQLite backend, with its data files local.
///
/// A test runs on the object-native backend when it registers with
/// [`test_on_data_store!`]. The tests below stay here on purpose, each checking
/// behavior that the object-native data path does not have, or failing there as
/// its row describes:
///
/// # Object-native exceptions
///
/// | Test | Reason |
/// |------|--------|
/// | `small_files_compaction_test::link_or_copy_snapshot_files_hardlinks_locally` | Asserts a local hard link (same inode); an object store copies the file. |
/// | `mutation_model_test::test_delete_file_replacement_same_path_impl` | Writes deletion vectors by hand at a local path. |
/// | `mutation_model_test::test_publication_without_reinsert_sequence_impl` | Writes deletion vectors by hand at a local path. |
/// | `mutation_property_test::prop_auto_crossing_observes_position_then_key_switch_impl` | `deletion_mode: auto` starts on position deletes only for a table on a local directory. |
/// | `mutation_property_test::prop_sequential_auto_crossing_impl` | As above. |
/// | `mutation_property_test::prop_sequential_cold_impl` | Cold tier, which object-native data does not support. |
/// | `mutation_property_test::prop_sequential_cold_upsert_replay_impl` | As above. |
/// | `scan_view_retention_test::scan_view_selection_during_retention` | Holds a scan's planning on retention's unlink of a data file; a table in the object-native location keeps a retired file for `DATA_FILE_RETENTION`, so the delete unlinks nothing. |
/// | `file_based_retention_delete_test` (all but `test_loader_errors_on_missing_needed_dv_impl`) | File-based retention (whole-file deletes) runs only for local data files; a table in an object store deletes the expired rows through deletion vectors, so the file counts, deleted-row counts and orphaned deletion vectors these tests assert differ. The two list-files cache tests also need a listing the table never makes. |
/// | `file_scan_statistics_source_test::concurrent_cold_scans_share_one_footer_read_and_upsert_per_file` | Counts footer reads through the session's `file://` store and upserts through SQLite triggers. |
/// | `file_scan_statistics_source_test::scan_file_statistics_counters_match_the_store_and_the_metastore` | As above. |
/// | `integration_test::test_cayenne_catalog_persistence_impl` | Reopens a SQLite catalog file. |
/// | `listing_cache_retention_race_test` (all) | Races a snapshot-directory listing through the list-files cache; a table in the object-native location never lists its files. |
/// | `mem_tier_overwrite_retention_test::a_waiting_publish_does_not_hold_back_the_other_partitions_impl` | Commits partitions' overwrites in one SQLite metastore transaction; the object-native metastore does not hold partitioned tables. |
/// | `orphan_dv_compaction_test::provider_open_replays_an_orphan_backlog_left_by_a_crash_impl` | Writes deletion vectors and their catalog rows by hand at a local path. |
/// | `orphan_dv_compaction_test::scheduled_cleanup_waits_for_the_orphan_threshold_impl` | As above. |
/// | `orphan_dv_compaction_test::scheduled_cleanup_keeps_a_deletion_vector_the_cold_tier_needs_impl` | Cold tier, which object-native data does not support. |
/// | `position_mode_reclaim_test::default_mode_small_table_reclaims_by_rewriting_itself` | `deletion_mode: auto` starts on position deletes only for a table on a local directory. |
/// | `position_mode_reclaim_test::empty_table_reclaim_clears_tombstones_and_survives_reopen` | As above. |
/// | `position_mode_reclaim_test::default_mode_table_past_its_bound_reclaims_through_the_bake` | As above: a table in an object store keeps key deletes from the start, so it never crosses the position-delete bound this test checks. |
/// | `staged_append_test` WAL and crash-leftover tests (`test_staged_append_self_healing_leftover_impl`, `test_wal_*`, `test_leftover_tmp_*`, `test_repeated_wal_writes_are_atomic_impl`, `test_prepared_lifecycle_matches_commit_impl`, `test_prepared_rollback_clears_staging_impl`, `test_corrupt_framed_wal_discarded_on_recovery_impl`, `test_cancelled_plain_append_does_not_wedge_after_compaction_impl`) | Check or plant the `_staging/` directory and its `_wal.json` on local disk; a table in an object store writes its files at their final keys and stages no WAL. |
#[macro_export]
macro_rules! test_with_backends {
    ($test_fn:ident) => {
        paste::paste! {
            #[test]
            fn [<$test_fn _sqlite>]() -> Result<(), String> {
                tracing::debug!("\n🔧 Running {} with SQLite backend", stringify!($test_fn));
                common::run_with_backend_blocking(common::BackendType::Sqlite, $test_fn)
            }

        }
    };
}

/// Run a test whose tables keep their data files where `fixture.data_store`
/// says on [`BackendType::for_data_store`]: on the SQLite backend with the local
/// store, and on the object-native backend with any other. The generated test
/// is named `<test_fn>_on_data_store`, since its backend follows the store.
#[macro_export]
macro_rules! test_on_data_store {
    ($test_fn:ident) => {
        paste::paste! {
            #[test]
            fn [<$test_fn _on_data_store>]() -> Result<(), String> {
                common::run_with_backend_blocking(common::BackendType::for_data_store(), $test_fn)
            }
        }
    };
}

/// Helper to run a test function with a specific backend
pub async fn run_with_backend<F, Fut>(
    backend: BackendType,
    test_fn: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(TestFixture) -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let fixture = TestFixture::new(backend).await?;
    let _directory = Arc::clone(&fixture.temp_dir);
    test_fn(fixture).await
}

/// [`run_with_backend`] on a thread with a [`TEST_STACK_SIZE`] stack, for
/// callers that are synchronous — which every `test_with_backends!` body is,
/// because the stack a body needs is a property of the thread it runs on and
/// `#[tokio::test]` builds its runtime on the thread libtest hands it.
///
/// The runtime built here matches what `#[tokio::test]` would have built (a
/// current-thread runtime with all drivers enabled); `thread_stack_size`
/// extends the same headroom to the blocking pool, whose threads the attribute
/// also leaves at the std default.
///
/// The body's `Box<dyn Error>` is not `Send`, so a returned error is flattened
/// to its message before crossing the join. A panic is resumed rather than
/// flattened, so an assertion failure still reaches libtest as the panic it
/// was, with its payload and location intact.
pub fn run_with_backend_blocking<F, Fut>(backend: BackendType, test_fn: F) -> Result<(), String>
where
    F: FnOnce(TestFixture) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + 'static,
{
    let outcome = std::thread::Builder::new()
        .stack_size(TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(TEST_STACK_SIZE)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build tokio runtime: {e}"))?
                .block_on(async move {
                    run_with_backend(backend, test_fn)
                        .await
                        .map_err(|e| e.to_string())
                })
        })
        .map_err(|e| format!("failed to spawn test thread: {e}"))?
        .join();

    match outcome {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

// ============================================================================
// Insert Helper Functions
// ============================================================================
//
// These helpers wrap the `CayenneTableProvider::insert_into()` API for tests.
// They ensure write logic goes through `CayenneDataSink::write_all()`.

/// Insert a single batch using `insert_into()` (append mode).
///
/// Creates a temporary `SessionContext` internally.
pub async fn insert_batch(provider: &CayenneTableProvider, batch: RecordBatch) -> DFResult<u64> {
    insert_batches(provider, vec![batch]).await
}

/// Insert record batches using `insert_into()` API (append mode).
///
/// Creates a temporary `SessionContext` internally.
pub async fn insert_batches(
    provider: &CayenneTableProvider,
    batches: Vec<RecordBatch>,
) -> DFResult<u64> {
    write_batches(provider, batches, InsertOp::Append).await
}

/// Write record batches through the `insert_into()` API with `op`, in one
/// write; returns the row count the write reports.
///
/// Creates a temporary `SessionContext` internally.
pub async fn write_batches(
    provider: &CayenneTableProvider,
    batches: Vec<RecordBatch>,
    op: InsertOp,
) -> DFResult<u64> {
    use datafusion::physical_plan::collect;

    if batches.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "Cannot insert empty batches".to_string(),
        ));
    }

    let ctx = SessionContext::new();
    let schema = Arc::clone(batches[0].schema_ref());
    let input_exec = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    let insert_plan = provider.insert_into(&ctx.state(), input_exec, op).await?;
    let results = collect(insert_plan, ctx.task_ctx()).await?;

    Ok(extract_row_count(&results))
}

/// Poll `catalog.get_inlined_data_count(table_id)` until it returns `0` or
/// the timeout elapses. The auto-checkpoint that drains the inline memtable
/// runs in a background `tokio::spawn` task scheduled from the write path,
/// so tests that assert "inline data was flushed" after an insert race
/// against that task. This helper gives the background task a bounded
/// window to complete before failing the test.
pub async fn poll_inlined_data_count_zero(
    catalog: &Arc<impl MetadataCatalog + ?Sized>,
    table_id: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    let started = std::time::Instant::now();
    loop {
        let count = catalog.get_inlined_data_count(table_id).await?;
        if count == 0 || started.elapsed() >= TIMEOUT {
            return Ok(count);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll `catalog.get_inlined_data_stats(table_id).tombstone_entry_count` until it
/// is at or below `limit`, or the timeout elapses; returns the last count seen.
///
/// The tombstone sibling of [`poll_inlined_data_count_zero`], and it races the
/// same background task: the reclamation that drains `cayenne_inlined_delete`
/// runs in a `tokio::spawn` scheduled from the write path.
pub async fn poll_inlined_delete_count_at_most(
    catalog: &Arc<impl MetadataCatalog + ?Sized>,
    table_id: &str,
    limit: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    let started = std::time::Instant::now();
    loop {
        let count = catalog
            .get_inlined_data_stats(table_id)
            .await?
            .tombstone_entry_count;
        if count <= limit || started.elapsed() >= TIMEOUT {
            return Ok(count);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Read `var` as a positive scale multiplier for fuzz/stress-test depth
/// (iteration counts, attempt counts, deadlines). Accepts fractions below 1
/// (e.g. `0.25` for a lighter per-PR pass); a missing, non-positive, or
/// unparseable value is treated as `1.0` (the default, full-depth run).
/// Mirrors `mutation_property_test`'s `env_scale`, shared here so every
/// hand-rolled race/stress test can dial CI depth the same way.
pub fn env_scale(var: &str) -> f64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&v| v > 0.0)
        .unwrap_or(1.0)
}

/// The key a per-file statistics row is stored under: the object-store location,
/// which is the store-relative path. The manifest carries only the bare filename.
pub fn statistics_row_key(
    data_path: &std::path::Path,
    table_id: &str,
    file: &cayenne::metadata::SnapshotFile,
) -> String {
    format!(
        "{}/{}/{}/{}",
        data_path
            .to_string_lossy()
            .trim_start_matches('/')
            .trim_end_matches('/'),
        table_id,
        file.snapshot_id,
        file.file_path
    )
}

/// Extract the row count from insert result batches.
fn extract_row_count(results: &[RecordBatch]) -> u64 {
    use arrow::datatypes::DataType;

    if results.is_empty() {
        return 0;
    }
    let batch = &results[0];
    if batch.num_columns() == 0 || batch.num_rows() == 0 {
        return 0;
    }
    let col = batch.column(0);
    match col.data_type() {
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .map_or(0, |a| a.value(0)),
        _ => 0,
    }
}

/// One data file of `snapshot`, so retiring the snapshot retires a file.
pub fn one_file(table_id: &str, snapshot: &str) -> [cayenne::metadata::SnapshotFile; 1] {
    [cayenne::metadata::SnapshotFile {
        table_id: table_id.to_string(),
        snapshot_id: snapshot.to_string(),
        file_path: "part-0.vortex".to_string(),
        row_count: 1,
        file_size_bytes: 1,
        min_sequence: 0,
        max_sequence: 0,
        digest: None,
    }]
}
