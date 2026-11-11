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
    /// Serializes creating and dropping datasets.
    pub(super) ddl: tokio::sync::Mutex<()>,
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

/// `prefix`'s siblings: the folders beside the registry.
fn sibling(prefix: &Path, name: &str) -> Path {
    let mut parts: Vec<_> = prefix.parts().collect();
    parts.pop();
    Path::from_iter(parts).join(name)
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
        let registry =
            Domain::create_with_config(Arc::clone(&store), prefix.clone(), config, true).await?;
        Self::assemble(store, prefix, config, registry).await
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
    /// `location` names where it is, for the startup log.
    /// # Errors
    /// Returns an error when the store cannot be used or the history is invalid.
    pub async fn open_or_create_at(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        location: &str,
    ) -> CatalogResult<Self> {
        let (catalog, verb) = match store.head(&prefix.clone().join("head")).await {
            Ok(_) => (Self::open(store, prefix).await?, "Opened"),
            Err(object_store::Error::NotFound { .. }) => {
                (Self::create(store, prefix).await?, "Created")
            }
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
        let registry =
            Domain::open_with_config(Arc::clone(&store), prefix.clone(), config, true).await?;
        Self::assemble(store, prefix, config, registry).await
    }

    /// Open the log of every dataset the registry names, route each table to its
    /// log, and resolve every transaction a crash left prepared.
    async fn assemble(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        registry: Domain,
    ) -> CatalogResult<Self> {
        let datasets = sibling(&prefix, "datasets");
        let transactions = sibling(&prefix, "transactions");
        let registry = Arc::new(registry);
        let mut routes = Routes::default();
        routes.refresh(&registry, registry.tables().await);
        let entries = registry.positions_under(REGISTRY).await;
        let opened = futures::future::try_join_all(entries.into_iter().map(|(name, folder)| {
            let store = Arc::clone(&store);
            let datasets = datasets.clone();
            async move {
                let folder = String::from_utf8(folder).map_err(invalid)?;
                let prefix = datasets.join(folder.as_str());
                match store.head(&prefix.clone().join("head")).await {
                    Ok(_) => {}
                    // Registered, then the process stopped before its log was created.
                    Err(object_store::Error::NotFound { .. }) => return Ok((name, None)),
                    Err(error) => return Err(invalid(error)),
                }
                let mut domain = Domain::open_with_config(store, prefix, config, false).await?;
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
            routes.refresh(&domain, domain.tables().await);
            routes.datasets.insert(name, domain);
        }
        registry.commit_actions(stale).await?;
        let catalog = Self {
            store,
            config,
            registry,
            prefix,
            datasets,
            transactions,
            routes: RwLock::new(routes),
            ddl: tokio::sync::Mutex::new(()),
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
        let folder = format!("{}-{}", slug(&name), uuid::Uuid::now_v7());
        let mut domain = Domain::create_with_config(
            Arc::clone(&self.store),
            self.datasets.clone().join(folder.as_str()),
            self.config,
            false,
        )
        .await?;
        domain.folder.clone_from(&folder);
        let domain = Arc::new(domain);
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
            let mut routes = self.routes.write();
            routes.refresh(&domain, Vec::new());
            routes.datasets.remove(name);
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
