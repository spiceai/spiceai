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

//! The dataset registry and catalog routing.

use super::{Domain, NativeCatalogConfig, OpenSummary, ValueChange, invalid, position};
use crate::{
    catalog::{CatalogError, CatalogResult, MetadataCatalog},
    metadata::CreateTableOptions,
};
use cayenne_metastore_actions::proto;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// The registry's key for its entries: partition = dataset name, value = the
/// dataset's folder under `datasets/`. It has no `/`, so it names no dataset.
pub(super) const REGISTRY: &str = "cayenne:datasets";

/// The object in a dropped dataset's folder whose time is when it was dropped.
pub(super) const DROPPED: &str = "dropped";

/// The name registry and every dataset's log.
pub struct ObjectNativeCatalog {
    pub(super) store: Arc<dyn ObjectStore>,
    /// The location's store as the data files of datasets in the location use
    /// it: the same bucket, its requests measured apart from the logs'.
    pub(super) data_store: Option<Arc<dyn ObjectStore>>,
    pub(super) config: NativeCatalogConfig,
    /// The name registry's log, at the catalog's prefix. It also holds tables
    /// created before datasets had logs of their own, and positions whose key
    /// names no dataset.
    pub(super) registry: Arc<Domain>,
    /// The registry's prefix, and `datasets/` and `transactions/` beside it.
    pub(super) prefix: Path,
    pub(super) datasets: Path,
    pub(super) transactions: Path,
    routes: RwLock<Routes>,
    /// Serializes creating and dropping datasets, and deleting a dropped
    /// dataset's folder.
    pub(super) ddl: Arc<tokio::sync::Mutex<()>>,
    /// Folders of datasets dropped in this process whose deletion is pending.
    pub(super) dropping: Arc<parking_lot::Mutex<HashSet<String>>>,
    /// Restore-point and export holds on each table's deletions, by table id.
    pub(super) deletion_holds: parking_lot::Mutex<HashMap<String, Vec<TableDeletionHolds>>>,
    /// The URL of the folder `datasets/` is in, when the catalog was opened at a
    /// location URL: a dataset's data path under it is recorded relative to it
    /// and resolved against it on every read, so a copy of the location opened
    /// at another URL reads its own data.
    pub(super) anchor: Option<Arc<str>>,
}

/// A weak handle to a table's restore-point and export deletion hold.
#[derive(Clone, Debug)]
pub struct TableDeletionHolds {
    deletions: std::sync::Weak<tokio::sync::RwLock<()>>,
}

impl TableDeletionHolds {
    pub(crate) fn new(deletions: &Arc<tokio::sync::RwLock<()>>) -> Self {
        Self {
            deletions: Arc::downgrade(deletions),
        }
    }
    pub(super) fn held(&self) -> bool {
        self.deletions
            .upgrade()
            .is_some_and(|fence| fence.try_write().is_err())
    }
    pub(super) fn gone(&self) -> bool {
        self.deletions.strong_count() == 0
    }
}

/// Which log holds each table.
#[derive(Default)]
struct Routes {
    by_name: HashMap<String, Arc<Domain>>,
    by_table: HashMap<String, Arc<Domain>>,
    /// Tables whose partitions are child tables on the same log.
    partitioned: HashSet<String>,
    /// Each dataset's own log, by dataset name.
    datasets: HashMap<String, Arc<Domain>>,
}

impl Routes {
    /// Route every table of `domain` to it, replacing what was routed there.
    fn refresh(&mut self, domain: &Arc<Domain>, tables: Vec<(String, String, bool)>) {
        self.by_name
            .retain(|_, routed| !Arc::ptr_eq(routed, domain));
        self.by_table
            .retain(|_, routed| !Arc::ptr_eq(routed, domain));
        for (id, name, partitioned) in tables {
            if partitioned {
                self.partitioned.insert(name.clone());
            } else {
                self.partitioned.remove(&name);
            }
            self.by_table.insert(id, Arc::clone(domain));
            self.by_name.insert(name, Arc::clone(domain));
        }
    }
}

/// The URL of the folder holding the registry at `prefix` and `datasets/`, when
/// `location` is that folder's URL (`s3://bucket/state/cayenne/v1` for the
/// registry `state/cayenne/v1/catalog`).
fn location_anchor(location: &str, prefix: &Path) -> Option<Arc<str>> {
    let url = url::Url::parse(location).ok()?;
    if url.scheme() == "file" {
        return None;
    }
    let folder: Vec<_> = prefix.parts().collect();
    let folder = Path::from_iter(folder[..folder.len().checked_sub(1)?].iter().cloned());
    (url.path().trim_matches('/') == folder.as_ref())
        .then(|| Arc::from(location.trim_end_matches('/')))
}

/// `prefix`'s siblings: the folders beside the registry.
fn sibling(prefix: &Path, name: &str) -> Path {
    let mut parts: Vec<_> = prefix.parts().collect();
    parts.pop();
    Path::from_iter(parts).join(name)
}

/// Keep `domain`'s writer lease, under [`super::Access::Leased`], for as long
/// as the catalog holds the log: renew it, or take over once it lapses.
fn keep_lease_while_open(domain: &Arc<Domain>) {
    let Some(every) = domain.lease_interval() else {
        return;
    };
    let domain = Arc::downgrade(domain);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            let Some(domain) = domain.upgrade() else {
                break;
            };
            domain.keep_lease().await;
        }
    });
}

/// A new folder for dataset `name`: its slug and a fresh `UUIDv7`. A caller that
/// puts the dataset's data files in the location mints the folder first, so the
/// table path it creates the dataset with (`<datasets>/<folder>/data`) names it.
#[must_use]
pub fn new_dataset_folder(name: &str) -> String {
    format!("{}-{}", slug(name), uuid::Uuid::now_v7())
}

/// A folder name a person browsing the bucket can match to its dataset.
fn slug(name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(40)
        .collect();
    if slug.is_empty() {
        "dataset".to_string()
    } else {
        slug
    }
}

/// When the `UUIDv7` that ends `name` (a transaction id or a dataset folder) was
/// made, or `None` when `name` does not end in one.
pub(super) fn created_at(name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let id = name.get(name.len().checked_sub(36)?..)?;
    let (seconds, nanos) = uuid::Uuid::parse_str(id).ok()?.get_timestamp()?.to_unix();
    chrono::DateTime::from_timestamp(i64::try_from(seconds).ok()?, nanos)
}

/// Delete a dropped dataset's folder at `prefix`: its data through
/// `data_store`, its log through `store`, in batched requests, and its `dropped`
/// marker last. Returns the data files and bytes deleted.
async fn delete_dropped_folder(
    store: &Arc<dyn ObjectStore>,
    data_store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> CatalogResult<(u64, u64)> {
    let marker = prefix.clone().join(DROPPED);
    let data = prefix.clone().join("data");
    let listed: Vec<object_store::ObjectMeta> = store
        .list(Some(prefix))
        .try_collect()
        .await
        .map_err(invalid)?;
    let (data_objects, log_objects): (Vec<_>, Vec<_>) = listed
        .into_iter()
        .filter(|meta| meta.location != marker)
        .partition(|meta| meta.location.prefix_matches(&data));
    let sizes: HashMap<Path, u64> = data_objects
        .iter()
        .map(|meta| (meta.location.clone(), meta.size))
        .collect();
    let data_deleted = crate::dataset_storage::delete_in_batches(
        data_store,
        data_objects.into_iter().map(|meta| meta.location).collect(),
    )
    .await;
    let log_deleted = crate::dataset_storage::delete_in_batches(
        store,
        log_objects.into_iter().map(|meta| meta.location).collect(),
    )
    .await;
    for outcome in [&data_deleted, &log_deleted] {
        if let Some(error) = &outcome.first_error {
            return Err(invalid(error));
        }
        if outcome.failed > 0 {
            return Err(invalid(format!(
                "{} object(s) were neither deleted nor reported absent",
                outcome.failed
            )));
        }
    }
    let files = u64::try_from(data_deleted.deleted.len()).unwrap_or(u64::MAX);
    let bytes = data_deleted
        .deleted
        .iter()
        .map(|path| sizes.get(path).copied().unwrap_or(0))
        .sum();
    match store.delete(&marker).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok((files, bytes)),
        Err(error) => Err(invalid(error)),
    }
}

/// Delete every object under `prefix`.
pub(super) async fn delete_under(store: &Arc<dyn ObjectStore>, prefix: &Path) -> CatalogResult<()> {
    let locations = store
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .boxed();
    store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .map_err(invalid)?;
    Ok(())
}

/// The prefix of the log that holds `dataset` in the catalog at `prefix`, read
/// from storage alone, for tools and tests that read a dataset's log directly.
/// # Errors
/// Returns an error when the registry cannot be read.
pub async fn dataset_log_at(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    dataset: &str,
) -> CatalogResult<Option<Path>> {
    let (_, opened) = object_store_occ::headlog::Reader::open(store, prefix.clone())
        .await
        .map_err(invalid)?;
    let state = super::replay_state(opened)?.state;
    state
        .positions
        .get(&(REGISTRY.to_string(), dataset.to_string()))
        .map(|folder| {
            let folder = std::str::from_utf8(folder).map_err(invalid)?;
            Ok(sibling(prefix, "datasets").join(folder))
        })
        .transpose()
}

impl ObjectNativeCatalog {
    /// Create an empty catalog at a new prefix.
    /// # Errors
    /// Returns an error when conditional writes are unsupported or the prefix exists.
    pub async fn create(store: Arc<dyn ObjectStore>, prefix: Path) -> CatalogResult<Self> {
        Self::create_with_config(store, prefix, NativeCatalogConfig::default()).await
    }
    /// [`Self::create`] with explicit head-log tuning.
    /// # Errors
    /// Returns an error when conditional writes are unsupported or the prefix exists.
    pub async fn create_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
    ) -> CatalogResult<Self> {
        Self::create_at(store, prefix, config, None).await
    }
    pub(super) async fn create_at(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        anchor: Option<Arc<str>>,
    ) -> CatalogResult<Self> {
        let registry = Domain::create_with_config(
            Arc::clone(&store),
            prefix.clone(),
            config,
            true,
            anchor.clone(),
            "",
        )
        .await?;
        Self::assemble(store, prefix, config, registry, anchor).await
    }
    /// Open the catalog kept in a local directory, creating it on first use. The
    /// directory is an fsync'd object store with conditional writes.
    /// # Errors
    /// Returns an error when the directory cannot be used or the history is invalid.
    pub async fn open_or_create_local(directory: &std::path::Path) -> CatalogResult<Self> {
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(invalid)?;
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store_occ::LocalConditionalPut::new_durable(directory).map_err(invalid)?,
        );
        Self::open_or_create_at(
            store,
            Path::from("catalog"),
            &directory.display().to_string(),
        )
        .await
    }
    /// Open the catalog at `prefix` in `store`, creating it on first use.
    /// `location` names where it is, for the startup log. When `location` is the
    /// URL of the folder `prefix` is in (`s3://bucket/state/cayenne/v1` for
    /// `state/cayenne/v1/catalog`), a dataset's data path under it is recorded
    /// relative to it, so a copy of the location opened at another URL serves
    /// the same rows from its own objects.
    /// # Errors
    /// Returns an error when the store cannot be used or the history is invalid.
    pub async fn open_or_create_at(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        location: &str,
    ) -> CatalogResult<Self> {
        Self::open_or_create_at_with_config(store, prefix, location, NativeCatalogConfig::default())
            .await
    }
    /// [`Self::open_or_create_at`] with explicit head-log tuning.
    /// # Errors
    /// Returns an error when the store cannot be used or the history is invalid.
    pub async fn open_or_create_at_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        location: &str,
        config: NativeCatalogConfig,
    ) -> CatalogResult<Self> {
        let anchor = location_anchor(location, &prefix);
        let (catalog, verb) = match store.head(&prefix.clone().join("head")).await {
            Ok(_) => (
                Self::open_at(store, prefix, config, anchor).await?,
                "Opened",
            ),
            Err(object_store::Error::NotFound { .. }) => (
                Self::create_at(store, prefix, config, anchor).await?,
                "Created",
            ),
            Err(error) => return Err(invalid(error)),
        };
        let (datasets, tables, replayed) = catalog.summary();
        let OpenSummary { lease, .. } = catalog.registry.opened;
        tracing::info!(
            "{verb} the Cayenne object-native metastore at '{location}': {datasets} datasets with {tables} tables, plus {replayed} commits after their checkpoints, writing as lease {lease}"
        );
        Ok(catalog)
    }
    /// Replay committed records, then fence the previous writers with the next lease.
    /// # Errors
    /// Returns an error on invalid history, takeover conflict, or storage failure.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> CatalogResult<Self> {
        Self::open_with_config(store, prefix, NativeCatalogConfig::default()).await
    }
    /// [`Self::open`] with explicit head-log tuning.
    /// # Errors
    /// Returns an error on invalid history, takeover conflict, or storage failure.
    pub async fn open_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
    ) -> CatalogResult<Self> {
        Self::open_at(store, prefix, config, None).await
    }
    pub(super) async fn open_at(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        anchor: Option<Arc<str>>,
    ) -> CatalogResult<Self> {
        let registry = Domain::open_with_config(
            Arc::clone(&store),
            prefix.clone(),
            config,
            true,
            anchor.clone(),
            "",
        )
        .await?;
        Self::assemble(store, prefix, config, registry, anchor).await
    }
    /// Open the log of every dataset the registry names, route each table to its
    /// log, and resolve every transaction a crash left prepared.
    async fn assemble(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        registry: Domain,
        anchor: Option<Arc<str>>,
    ) -> CatalogResult<Self> {
        let datasets = sibling(&prefix, "datasets");
        let transactions = sibling(&prefix, "transactions");
        let registry = Arc::new(registry);
        keep_lease_while_open(&registry);
        let mut routes = Routes::default();
        routes.refresh(&registry, registry.tables().await);
        let entries = registry.positions_under(REGISTRY).await;
        let opened = futures::future::try_join_all(entries.into_iter().map(|(name, folder)| {
            let store = Arc::clone(&store);
            let datasets = datasets.clone();
            let anchor = anchor.clone();
            async move {
                let folder = String::from_utf8(folder).map_err(invalid)?;
                let prefix = datasets.join(folder.as_str());
                match store.head(&prefix.clone().join("head")).await {
                    Ok(_) => {}
                    // Registered, then the process stopped before its log was created.
                    Err(object_store::Error::NotFound { .. }) => return Ok((name, None)),
                    Err(error) => return Err(invalid(error)),
                }
                let mut domain = Domain::open_with_config(
                    store,
                    prefix,
                    config,
                    config.commit_each_change,
                    anchor,
                    &name,
                )
                .await?;
                domain.folder = folder;
                Ok::<_, CatalogError>((name, Some(domain)))
            }
        }))
        .await?;
        let mut stale = Vec::new();
        for (name, domain) in opened {
            let Some(domain) = domain else {
                stale.push(position(REGISTRY, &name, None));
                continue;
            };
            let domain = Arc::new(domain);
            keep_lease_while_open(&domain);
            routes.refresh(&domain, domain.tables().await);
            routes.datasets.insert(name, domain);
        }
        registry.commit_actions(stale).await?;
        let catalog = Self {
            store,
            data_store: None,
            config,
            registry,
            prefix,
            datasets,
            transactions,
            routes: RwLock::new(routes),
            ddl: Arc::new(tokio::sync::Mutex::new(())),
            dropping: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            deletion_holds: parking_lot::Mutex::new(HashMap::new()),
            anchor,
        };
        catalog.recover().await?;
        catalog.reclaim().await?;
        Ok(catalog)
    }
    /// The number of datasets with their own log, of tables, and of commits
    /// replayed after the checkpoints.
    fn summary(&self) -> (usize, usize, usize) {
        let routes = self.routes.read();
        let replayed = routes
            .datasets
            .values()
            .map(|domain| domain.opened.replayed)
            .sum::<usize>()
            + self.registry.opened.replayed;
        (routes.datasets.len(), routes.by_table.len(), replayed)
    }
    /// The store the catalog's logs are in.
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }
    /// Data files of datasets in the location go through `store`: the same
    /// location as [`Self::store`], measured as data rather than as the logs.
    #[must_use]
    pub fn with_data_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.data_store = Some(store);
        self
    }
    /// The store a dataset whose data files are in the location keeps them in,
    /// under [`Self::datasets_prefix`]: the one [`Self::with_data_store`] set,
    /// or else [`Self::store`].
    #[must_use]
    pub fn data_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(self.data_store.as_ref().unwrap_or(&self.store))
    }
    /// The prefix of the dataset folders (`…/cayenne/v1/datasets`) in [`Self::store`].
    #[must_use]
    pub fn datasets_prefix(&self) -> &Path {
        &self.datasets
    }
    /// The folder `base_path` names for a new dataset `name`, when it is a data path
    /// `…/<datasets>/<folder>/data` in this catalog's location whose folder is one
    /// [`new_dataset_folder`] makes for `name`.
    pub(super) fn folder_named_by(&self, name: &str, base_path: &str) -> Option<String> {
        let url = url::Url::parse(base_path).ok()?;
        let path = url.path().trim_matches('/');
        let rest = path
            .strip_prefix(self.datasets.as_ref())?
            .strip_prefix('/')?;
        let folder = rest.strip_suffix("/data")?;
        let uuid = folder.strip_prefix(&format!("{}-", slug(name)))?;
        (!folder.contains('/') && uuid::Uuid::parse_str(uuid).is_ok()).then(|| folder.to_string())
    }
    /// Commit, on `dataset`'s log, a change its state refuses: the path a bug in
    /// this catalog's own checks would take. For tests of what a refused change
    /// does to the dataset.
    /// # Errors
    /// Returns the refusal.
    #[doc(hidden)]
    pub async fn commit_change_the_state_refuses(&self, dataset: &str) -> CatalogResult<()> {
        let domain = self.for_name(dataset);
        let table_id = domain
            .tables()
            .await
            .into_iter()
            .find(|(_, name, _)| name == dataset)
            .map(|(id, _, _)| id)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: dataset.to_string(),
            })?;
        domain
            .commit_actions(vec![crate::metastore::actions::remove_groups(
                &table_id,
                proto::Select::Listed,
                &["not-a-registered-snapshot".to_string()],
                true,
            )])
            .await
    }
    /// Register the restore-point and export holds that delay folder deletion.
    pub fn register_deletion_holds(&self, table_id: &str, readers: TableDeletionHolds) {
        let mut registered = self.deletion_holds.lock();
        registered.retain(|_, tables| {
            tables.retain(|table| !table.gone());
            !tables.is_empty()
        });
        registered
            .entry(table_id.to_string())
            .or_default()
            .push(readers);
    }
    /// Whether `path`, a table's data path, is a dataset folder's data in this
    /// catalog's location: its files go with the dataset's folder when the dataset
    /// is dropped. `path` is either the absolute URL a read of the table returns
    /// (the state records it relative to the location and resolves it against the
    /// URL the catalog was opened at) or the relative form itself,
    /// `datasets/<folder>/data`.
    #[must_use]
    pub fn holds_data_at(&self, path: &str) -> bool {
        let in_datasets = match url::Url::parse(path) {
            Ok(url) => {
                // Opened at a location URL: the data must be under that URL, not
                // under the same key prefix of another bucket.
                if let Some(anchor) = self.anchor.as_deref()
                    && !path
                        .strip_prefix(anchor)
                        .is_some_and(|rest| rest.starts_with('/'))
                {
                    return false;
                }
                url.path()
                    .trim_matches('/')
                    .strip_prefix(self.datasets.as_ref())
                    .and_then(|rest| rest.strip_prefix('/'))
                    .map(str::to_string)
            }
            Err(_) => path
                .trim_matches('/')
                .strip_prefix("datasets/")
                .map(str::to_string),
        };
        in_datasets
            .as_deref()
            .and_then(|rest| rest.split_once('/'))
            .is_some_and(|(folder, rest)| {
                !folder.is_empty() && (rest == "data" || rest.starts_with("data/"))
            })
    }
    /// Delete a dropped dataset's folder after `dropped_grace` and once no
    /// restore point or export holds its deletions. The marker goes last so a
    /// later open can resume deletion after a process stops.
    pub(super) fn reclaim_dropped_folder(
        &self,
        name: &str,
        folder: &str,
        readers: Vec<TableDeletionHolds>,
    ) {
        let store = Arc::clone(&self.store);
        let data_store = self.data_store();
        let prefix = self.datasets.clone().join(folder);
        let grace = self.config.dropped_grace;
        let name = name.to_string();
        let folder = folder.to_string();
        let ddl = Arc::clone(&self.ddl);
        let dropping = Arc::clone(&self.dropping);
        dropping.lock().insert(folder.clone());
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let grace = grace.min(std::time::Duration::from_secs(1));
            while readers.iter().any(TableDeletionHolds::held) {
                tokio::time::sleep(grace.max(std::time::Duration::from_millis(100))).await;
            }
            // Under the DDL lock, so no create can take the folder while it goes.
            let _ddl = ddl.lock().await;
            if !dropping.lock().remove(&folder) {
                return;
            }
            match delete_dropped_folder(&store, &data_store, &prefix).await {
                Ok((files, bytes)) => {
                    crate::provider::maintenance_metrics::track_reclaimed(
                        &name,
                        crate::provider::maintenance_metrics::MaintenanceOp::RetiredDirSweep,
                        files,
                        bytes,
                        0,
                    );
                    tracing::debug!(
                        "Deleted the {files} object(s) of dataset '{name}''s dropped acceleration in '{prefix}'"
                    );
                }
                Err(error) => {
                    crate::provider::maintenance_metrics::track_maintenance(
                        &name,
                        crate::provider::maintenance_metrics::MaintenanceOp::RetiredDirSweep,
                        crate::provider::maintenance_metrics::MaintenanceOutcome::Failed,
                    );
                    tracing::warn!(
                        "Failed to delete the files of dataset '{name}''s dropped acceleration under '{prefix}', so they keep using storage until Spice deletes them at a start at least an hour after the drop; no data is lost. Cause: {error}"
                    );
                }
            }
        });
    }
    /// The prefix of the log that holds `dataset`, for tools and tests that read
    /// a dataset's log directly.
    #[must_use]
    pub fn dataset_log(&self, dataset: &str) -> Option<Path> {
        self.routes
            .read()
            .datasets
            .get(dataset)
            .map(|domain| self.datasets.clone().join(domain.folder.as_str()))
    }
    pub(super) fn for_table(&self, table_id: &str) -> Arc<Domain> {
        self.routes
            .read()
            .by_table
            .get(table_id)
            .map_or_else(|| Arc::clone(&self.registry), Arc::clone)
    }
    pub(super) fn for_name(&self, name: &str) -> Arc<Domain> {
        self.routes
            .read()
            .by_name
            .get(name)
            .map_or_else(|| Arc::clone(&self.registry), Arc::clone)
    }
    /// The log of the dataset a bookkeeping key names (`<store>/<dataset>`); the
    /// registry's when it names none.
    fn for_key(&self, key: &str) -> Arc<Domain> {
        key.split_once('/').map_or_else(
            || Arc::clone(&self.registry),
            |(_, dataset)| self.for_name(dataset),
        )
    }
    /// The registry and every dataset's log.
    pub(super) fn domains(&self) -> Vec<Arc<Domain>> {
        let routes = self.routes.read();
        std::iter::once(Arc::clone(&self.registry))
            .chain(routes.datasets.values().cloned())
            .collect()
    }
    /// Run `operation` on every log at once, and return the first error.
    pub(super) async fn on_every_log<F, Fut, T>(&self, operation: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(Arc<Domain>) -> Fut,
        Fut: std::future::Future<Output = CatalogResult<T>>,
    {
        futures::future::join_all(self.domains().into_iter().map(operation))
            .await
            .into_iter()
            .collect()
    }
    /// A new dataset gets a log of its own; a partition's child table goes on its
    /// parent's log, so a drop takes both in one record.
    pub(super) async fn create_dataset(
        &self,
        options: CreateTableOptions,
    ) -> CatalogResult<String> {
        let name = options.table_name.clone();
        let (existing, parent) = {
            let routes = self.routes.read();
            let parent = routes
                .partitioned
                .iter()
                .find(|parent| name.starts_with(&format!("{parent}_")))
                .and_then(|parent| routes.by_name.get(parent))
                .cloned();
            (routes.by_name.get(&name).cloned(), parent)
        };
        if let Some(domain) = existing.or(parent) {
            let id = domain.create_table(options).await?;
            let tables = domain.tables().await;
            self.routes.write().refresh(&domain, tables);
            return Ok(id);
        }
        // A dataset whose data files go in the location names its folder in its
        // table path (`<datasets>/<folder>/data`, see [`new_dataset_folder`]), so the
        // files and the log share one folder and a drop reclaims both.
        let folder = self
            .folder_named_by(&name, &options.base_path)
            .unwrap_or_else(|| new_dataset_folder(&name));
        let mut domain = Domain::create_with_config(
            Arc::clone(&self.store),
            self.datasets.clone().join(folder.as_str()),
            self.config,
            self.config.commit_each_change,
            self.anchor.clone(),
            &name,
        )
        .await?;
        domain.folder.clone_from(&folder);
        let domain = Arc::new(domain);
        keep_lease_while_open(&domain);
        let id = domain.create_table(options).await?;
        // The dataset's bookkeeping kept while it had no log moves to its log,
        // which commits before the registry names it: a crash between leaves a
        // log nothing names and the values where they were.
        let moved = self.registry.positions_of(&name).await;
        domain
            .commit_actions(
                moved
                    .iter()
                    .map(|((source, partition), value)| {
                        position(source, partition, Some(value.clone()))
                    })
                    .collect(),
            )
            .await?;
        domain.commit_pending().await?;
        let mut register = vec![position(REGISTRY, &name, Some(folder.into_bytes()))];
        register.extend(
            moved
                .iter()
                .map(|((source, partition), _)| position(source, partition, None)),
        );
        self.registry.commit_actions(register).await?;
        let tables = domain.tables().await;
        let mut routes = self.routes.write();
        routes.refresh(&domain, tables);
        routes.datasets.insert(name, domain);
        Ok(id)
    }
    /// Dropping a dataset removes its name from the registry, which ends its
    /// log; its bookkeeping values move to the registry, where they were kept
    /// before datasets had logs, so they outlive the table as they did then.
    pub(super) async fn drop_dataset(&self, name: &str) -> CatalogResult<bool> {
        let (domain, own) = {
            let routes = self.routes.read();
            (
                routes.by_name.get(name).cloned(),
                routes.datasets.get(name).cloned(),
            )
        };
        let Some(domain) = domain else {
            return self.registry.drop_table(name).await;
        };
        let table_ids: Vec<String> = domain
            .tables()
            .await
            .into_iter()
            .map(|(id, _, _)| id)
            .collect();
        let dropped = domain.drop_table(name).await?;
        if own.is_some_and(|own| Arc::ptr_eq(&own, &domain)) {
            // Written before the registry forgets the name, so a log no name
            // refers to either has this marker or was never named.
            self.store
                .put(
                    &self
                        .datasets
                        .clone()
                        .join(domain.folder.as_str())
                        .join(DROPPED),
                    PutPayload::new(),
                )
                .await
                .map_err(invalid)?;
            let mut unregister = vec![position(REGISTRY, name, None)];
            unregister.extend(
                domain
                    .positions_of(name)
                    .await
                    .into_iter()
                    .map(|((source, partition), value)| position(&source, &partition, Some(value))),
            );
            self.registry.commit_actions(unregister).await?;
            {
                let mut routes = self.routes.write();
                routes.refresh(&domain, Vec::new());
                routes.datasets.remove(name);
            }
            let readers: Vec<TableDeletionHolds> = {
                let mut registered = self.deletion_holds.lock();
                table_ids
                    .iter()
                    .filter_map(|id| registered.remove(id))
                    .flatten()
                    .collect()
            };
            self.reclaim_dropped_folder(name, &domain.folder, readers);
            return Ok(dropped);
        }
        let tables = domain.tables().await;
        self.routes.write().refresh(&domain, tables);
        Ok(dropped)
    }
    /// The runtime's bookkeeping value under `key` (a CDC position, a dataset
    /// checkpoint), kept in the log of the dataset it names.
    /// # Errors
    /// Returns an error when the catalog must be reopened.
    pub async fn sidecar_value(&self, key: &str) -> CatalogResult<Option<Vec<u8>>> {
        self.for_key(key).sidecar_value(key).await
    }
    /// The counters kept under `key` by [`Self::advance_sidecar_counters`], by name.
    /// # Errors
    /// Returns an error when the catalog must be reopened.
    pub async fn sidecar_counters(&self, key: &str) -> CatalogResult<Vec<(String, i64)>> {
        self.for_key(key).sidecar_counters(key).await
    }
    /// Move each named counter under `key` up to its given value, keeping the
    /// larger of the stored and given values, in one commit (or in the enclosing
    /// deferred record). Names must not be empty: the empty name is the value of
    /// [`Self::set_sidecar_value`].
    /// # Errors
    /// Returns an error when a name is empty or the commit is not accepted.
    pub async fn advance_sidecar_counters(
        &self,
        key: &str,
        counters: &[(String, i64)],
    ) -> CatalogResult<()> {
        self.for_key(key)
            .set_sidecar_value_and_counters(key, ValueChange::Keep, counters)
            .await
    }
    /// [`Self::set_sidecar_value`] (when `value` is `Some`) and
    /// [`Self::advance_sidecar_counters`] in the same commit.
    /// # Errors
    /// Returns an error when a counter name is empty or the commit is not accepted.
    pub async fn set_sidecar_value_and_counters(
        &self,
        key: &str,
        value: Option<Option<Vec<u8>>>,
        counters: &[(String, i64)],
    ) -> CatalogResult<()> {
        let value = match value {
            None => ValueChange::Keep,
            Some(Some(value)) => ValueChange::Set(value),
            Some(None) => ValueChange::Remove,
        };
        self.for_key(key)
            .set_sidecar_value_and_counters(key, value, counters)
            .await
    }
    /// Set (`Some`) or remove (`None`) the bookkeeping value under `key`. Setting
    /// the value it already holds commits nothing.
    /// # Errors
    /// Returns an error when the commit is not accepted.
    pub async fn set_sidecar_value(&self, key: &str, value: Option<Vec<u8>>) -> CatalogResult<()> {
        self.for_key(key).set_sidecar_value(key, value).await
    }
}
