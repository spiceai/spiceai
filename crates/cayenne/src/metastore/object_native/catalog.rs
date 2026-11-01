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

//! The object-native catalog: a name registry and one head log per dataset,
//! so datasets commit independently. A transaction across datasets commits by
//! two-phase commit over their logs.

use super::{Domain, NativeCatalogConfig, OpenSummary, ValueChange, invalid, position};
use crate::{
    catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit},
    metadata::{
        ColdTierFile, CreateTableOptions, DeleteFile, IndexRunRecord, InlinedData,
        InlinedDataStats, InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics,
        TableMetadata, TableStatistics, TableStorageStats,
    },
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, path::Path};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// The registry's key for its entries: partition = dataset name, value = the
/// dataset's folder under `datasets/`. It has no `/`, so it names no dataset.
const REGISTRY: &str = "cayenne:datasets";

/// How long a transaction's coordinator has before anyone may abort it.
const TRANSACTION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// The object in a dropped dataset's folder whose time is when it was dropped.
const DROPPED: &str = "dropped";

/// The name registry and every dataset's log.
pub struct ObjectNativeCatalog {
    store: Arc<dyn ObjectStore>,
    config: NativeCatalogConfig,
    /// The name registry's log, at the catalog's prefix. It also holds tables
    /// created before datasets had logs of their own, and positions whose key
    /// names no dataset.
    registry: Arc<Domain>,
    /// `datasets/` and `transactions/`, beside the registry.
    datasets: Path,
    transactions: Path,
    routes: RwLock<Routes>,
    /// Serializes creating and dropping datasets.
    ddl: tokio::sync::Mutex<()>,
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
        self.by_name.retain(|_, routed| !Arc::ptr_eq(routed, domain));
        self.by_table.retain(|_, routed| !Arc::ptr_eq(routed, domain));
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
    if slug.is_empty() { "dataset".to_string() } else { slug }
}

/// When the UUIDv7 that ends `name` (a transaction id or a dataset folder) was
/// made, or `None` when `name` does not end in one.
fn created_at(name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let id = name.get(name.len().checked_sub(36)?..)?;
    let (seconds, nanos) = uuid::Uuid::parse_str(id).ok()?.get_timestamp()?.to_unix();
    chrono::DateTime::from_timestamp(i64::try_from(seconds).ok()?, nanos)
}

/// Delete every object under `prefix`.
async fn delete_under(store: &Arc<dyn ObjectStore>, prefix: &Path) -> CatalogResult<()> {
    let locations = store.list(Some(prefix)).map_ok(|meta| meta.location).boxed();
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

    fn for_table(&self, table_id: &str) -> Arc<Domain> {
        self.routes
            .read()
            .by_table
            .get(table_id)
            .map_or_else(|| Arc::clone(&self.registry), Arc::clone)
    }

    fn for_name(&self, name: &str) -> Arc<Domain> {
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
    fn domains(&self) -> Vec<Arc<Domain>> {
        let routes = self.routes.read();
        std::iter::once(Arc::clone(&self.registry))
            .chain(routes.datasets.values().cloned())
            .collect()
    }

    /// Run `operation` on every log at once, and return the first error.
    async fn on_every_log<F, Fut, T>(&self, operation: F) -> CatalogResult<Vec<T>>
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
    async fn create_dataset(&self, options: CreateTableOptions) -> CatalogResult<String> {
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
    async fn drop_dataset(&self, name: &str) -> CatalogResult<bool> {
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
                    &self.datasets.clone().join(domain.folder.as_str()).join(DROPPED),
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

    /// Commit publications on several datasets' logs as one transaction:
    /// write the intent, `Prepare` on every participant, decide by
    /// create-if-absent of the decision, then `Resolve` on every participant.
    /// Participants are reserved in folder order, so two transactions never
    /// wait on each other.
    async fn commit_across(
        &self,
        mut groups: Vec<(Arc<Domain>, Vec<crate::catalog::CatalogPublication>)>,
    ) -> CatalogResult<()> {
        groups.sort_by(|(a, _), (b, _)| a.folder.cmp(&b.folder));
        let mut reservations = Vec::with_capacity(groups.len());
        for (domain, _) in &groups {
            reservations.push(domain.inner.reserve().await);
        }
        let txn = uuid::Uuid::now_v7().to_string();
        let deadline = chrono::Utc::now()
            + chrono::Duration::from_std(TRANSACTION_DEADLINE).map_err(invalid)?;
        let intent = serde_json::json!({
            "participants": groups.iter().map(|(domain, _)| domain.folder.clone()).collect::<Vec<_>>(),
            "deadline": deadline.to_rfc3339(),
        });
        self.store
            .put_opts(
                &self.transaction_object(&txn, "intent.json"),
                PutPayload::from(intent.to_string()),
                PutMode::Create.into(),
            )
            .await
            .map_err(invalid)?;
        let prepared = futures::future::join_all(
            groups
                .iter()
                .map(|(domain, publications)| domain.prepare(&txn, publications)),
        )
        .await;
        let all_prepared = prepared.iter().all(Result::is_ok);
        let decision = self.decide(&txn, all_prepared).await;
        let commit = matches!(decision, Ok(true));
        let resolved = match &decision {
            // Undecided: each participant's recovery resolves it.
            Err(_) => Vec::new(),
            Ok(_) => {
                futures::future::join_all(groups.iter().zip(&prepared).filter_map(
                    |((domain, _), footprint)| {
                        footprint
                            .as_ref()
                            .ok()
                            .map(|footprint| domain.resolve(&txn, footprint.clone(), commit))
                    },
                ))
                .await
            }
        };
        drop(reservations);
        // Once every participant has resolved it, nothing reads the transaction's
        // records again. Any other outcome leaves them for recovery.
        if all_prepared && decision.is_ok() && resolved.iter().all(Result::is_ok) {
            self.forget(&txn).await;
        }
        for outcome in prepared {
            outcome?;
        }
        decision?;
        for outcome in resolved {
            outcome?;
        }
        if commit {
            Ok(())
        } else {
            Err(invalid(format!(
                "transaction {txn} across datasets was aborted before it committed"
            )))
        }
    }

    fn transaction_object(&self, txn: &str, name: &str) -> Path {
        self.transactions.clone().join(txn).join(name)
    }

    /// Remove a resolved transaction's records. One that fails is removed when
    /// a catalog next opens.
    async fn forget(&self, txn: &str) {
        if let Err(error) = delete_under(&self.store, &self.transactions.clone().join(txn)).await {
            tracing::debug!("Cayenne object-native metastore left the records of resolved transaction {txn} for the next open to remove: {error}");
        }
    }

    /// Remove what no log needs once recovery has resolved every transaction
    /// left prepared: transaction records, logs no name refers to (a dataset
    /// dropped, or a create stopped before it was named). Only what is older than
    /// `reclaim_after` goes, since a newer one may belong to a process that took
    /// the logs over after this one opened them.
    async fn reclaim(&self) -> CatalogResult<()> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(self.config.reclaim_after).map_err(invalid)?;
        let mut removed = (0, 0);
        let listed = self
            .store
            .list_with_delimiter(Some(&self.transactions))
            .await
            .map_err(invalid)?;
        for prefix in listed.common_prefixes {
            let Some(txn) = prefix.filename() else {
                continue;
            };
            if created_at(txn).is_some_and(|created| created < cutoff) {
                delete_under(&self.store, &prefix).await?;
                removed.0 += 1;
            }
        }
        let named: HashSet<String> = self
            .registry
            .positions_under(REGISTRY)
            .await
            .into_iter()
            .filter_map(|(_, folder)| String::from_utf8(folder).ok())
            .collect();
        let listed = self
            .store
            .list_with_delimiter(Some(&self.datasets))
            .await
            .map_err(invalid)?;
        for prefix in listed.common_prefixes {
            let Some(folder) = prefix.filename() else {
                continue;
            };
            if named.contains(folder) {
                continue;
            }
            let ended = match self.store.head(&prefix.clone().join(DROPPED)).await {
                Ok(marker) => Some(marker.last_modified),
                Err(object_store::Error::NotFound { .. }) => created_at(folder),
                Err(error) => return Err(invalid(error)),
            };
            if ended.is_some_and(|ended| ended < cutoff) {
                delete_under(&self.store, &prefix).await?;
                removed.1 += 1;
            }
        }
        if removed != (0, 0) {
            tracing::debug!(
                "Cayenne object-native metastore removed the records of {} resolved transactions and {} dataset logs no dataset uses",
                removed.0,
                removed.1
            );
        }
        Ok(())
    }

    /// Record `commit` as the outcome of `txn` unless one is recorded, and
    /// return the recorded outcome. Exactly one outcome is ever recorded.
    async fn decide(&self, txn: &str, commit: bool) -> CatalogResult<bool> {
        const ATTEMPTS: u32 = 8;
        let path = self.transaction_object(txn, "decision.json");
        let body = if commit { "commit" } else { "abort" };
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(50 << attempt.min(6))).await;
            }
            match self
                .store
                .put_opts(&path, PutPayload::from(body), PutMode::Create.into())
                .await
            {
                Ok(_) => return Ok(commit),
                Err(object_store::Error::AlreadyExists { .. }) => {}
                // Unknown whether it landed: read it back, or try again.
                Err(error) => {
                    last = Some(error);
                    continue;
                }
            }
            match self.store.get(&path).await {
                Ok(recorded) => {
                    let recorded = recorded.bytes().await.map_err(invalid)?;
                    return match recorded.as_ref() {
                        b"commit" => Ok(true),
                        b"abort" => Ok(false),
                        other => Err(invalid(format!(
                            "transaction {txn} has an unreadable decision: {}",
                            String::from_utf8_lossy(other)
                        ))),
                    };
                }
                Err(error) => last = Some(error),
            }
        }
        Err(invalid(format!(
            "could not record the decision of transaction {txn}: {}",
            last.map_or_else(String::new, |error| error.to_string())
        )))
    }

    /// Resolve every transaction a stopped process left prepared, from its
    /// decision. One with no decision yet is aborted: this process fenced the
    /// coordinator when it opened the logs, so it can no longer commit.
    async fn recover(&self) -> CatalogResult<()> {
        for domain in self.domains() {
            for (txn, footprint) in domain.unresolved().await {
                let reservation = domain.inner.reserve().await;
                let commit = self.decide(&txn, false).await?;
                domain.resolve(&txn, footprint, commit).await?;
                drop(reservation);
                tracing::info!(
                    "Cayenne object-native metastore resolved transaction {txn}, left prepared when spiced stopped: {}",
                    if commit { "committed" } else { "aborted" }
                );
            }
        }
        Ok(())
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

#[async_trait]
impl MetadataCatalog for ObjectNativeCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn commit_pending(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.commit_pending().await })
            .await
            .map(|_| ())
    }

    async fn commit_publications(
        &self,
        publications: &[crate::catalog::CatalogPublication],
    ) -> CatalogResult<()> {
        let mut groups: Vec<(Arc<Domain>, Vec<crate::catalog::CatalogPublication>)> = Vec::new();
        for publication in publications {
            let domain = self.for_table(&publication.table_id);
            match groups
                .iter_mut()
                .find(|(grouped, _)| Arc::ptr_eq(grouped, &domain))
            {
                Some((_, group)) => group.push(publication.clone()),
                None => groups.push((domain, vec![publication.clone()])),
            }
        }
        match groups.len() {
            0 => Ok(()),
            1 => {
                let (domain, publications) = groups.remove(0);
                domain.commit_publications(&publications).await
            }
            _ => self.commit_across(groups).await,
        }
    }

    async fn init(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.init().await })
            .await
            .map(|_| ())
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.shutdown().await })
            .await
            .map(|_| ())
    }

    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        let mut names: Vec<String> = self
            .on_every_log(|domain| async move { domain.list_table_names().await })
            .await?
            .into_iter()
            .flatten()
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String> {
        let _ddl = self.ddl.lock().await;
        self.create_dataset(options).await
    }

    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool> {
        let _ddl = self.ddl.lock().await;
        self.drop_dataset(table_name).await
    }

    async fn add_delete_files(&self, delete_files: Vec<DeleteFile>) -> CatalogResult<()> {
        let Some(first) = delete_files.first() else {
            return Ok(());
        };
        let domain = self.for_table(&first.table_id);
        if delete_files
            .iter()
            .any(|file| !Arc::ptr_eq(&self.for_table(&file.table_id), &domain))
        {
            return Err(invalid(
                "deletion vectors added together must belong to one dataset",
            ));
        }
        domain.add_delete_files(delete_files).await
    }

    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        Ok(())
    }

    async fn incremental_vacuum(&self) -> CatalogResult<u64> {
        Ok(0)
    }

    async fn export_dataset_slice(
        &self,
        dataset_name: &str,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<crate::metastore::snapshot::DatasetMetastoreSlice> {
        self.for_name(dataset_name)
            .export_dataset_slice(dataset_name, data_dir_anchor)
            .await
    }

    async fn import_dataset_slice(
        &self,
        slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        self.registry
            .import_dataset_slice(slice, data_dir_anchor)
            .await
    }

    async fn commit_pending_for(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).commit_pending_for(table_id).await
    }
    async fn list_pending_write_back(&self, table_id: &str, limit: usize, after: Option<&(i64, Vec<u8>)>) -> CatalogResult<Vec<(Vec<u8>, i64)>> {
        self.for_table(table_id).list_pending_write_back(table_id, limit, after).await
    }
    async fn clear_pending_write_back(&self, table_id: &str, keys: &[(Vec<u8>, i64)]) -> CatalogResult<()> {
        self.for_table(table_id).clear_pending_write_back(table_id, keys).await
    }
    async fn pending_write_back_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id).pending_write_back_count(table_id).await
    }
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        self.for_name(table_name).get_table(table_name).await
    }
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id).get_sequence_number(table_id).await
    }
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        self.for_table(table_id).get_table_delete_files(table_id).await
    }
    async fn get_orphan_eligible_delete_files(&self, table_id: &str, max_sequence: i64, limit: usize) -> CatalogResult<Vec<DeleteFile>> {
        self.for_table(table_id).get_orphan_eligible_delete_files(table_id, max_sequence, limit).await
    }
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>> {
        self.for_table(table_id).get_insert_records(table_id).await
    }
    async fn get_snapshot_sequence(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<Option<i64>> {
        self.for_table(table_id).get_snapshot_sequence(table_id, snapshot_id).await
    }
    async fn get_all_snapshot_sequences(&self, table_id: &str) -> CatalogResult<HashMap<String, i64>> {
        self.for_table(table_id).get_all_snapshot_sequences(table_id).await
    }
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        self.for_table(table_id).get_partitions(table_id).await
    }
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>> {
        self.for_table(table_id).get_table_statistics(table_id).await
    }
    async fn get_snapshot_file_statistics(&self, table_id: &str, snapshot_id: &str, file_path: &str) -> CatalogResult<Option<SnapshotFileStatistics>> {
        self.for_table(table_id).get_snapshot_file_statistics(table_id, snapshot_id, file_path).await
    }
    async fn get_all_snapshot_files(&self, table_id: &str) -> CatalogResult<Vec<SnapshotFile>> {
        self.for_table(table_id).get_all_snapshot_files(table_id).await
    }
    async fn get_snapshot_files(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<Vec<SnapshotFile>> {
        self.for_table(table_id).get_snapshot_files(table_id, snapshot_id).await
    }
    async fn list_cold_tier_files(&self, table_id: &str) -> CatalogResult<Vec<ColdTierFile>> {
        self.for_table(table_id).list_cold_tier_files(table_id).await
    }
    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>> {
        self.for_table(table_id).get_pk_index(table_id).await
    }
    async fn list_index_runs(&self, table_id: &str) -> CatalogResult<Vec<IndexRunRecord>> {
        self.for_table(table_id).list_index_runs(table_id).await
    }
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id).get_inlined_data(table_id).await
    }
    async fn get_inlined_data_for_partition(&self, table_id: &str, partition_key: &str) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id).get_inlined_data_for_partition(table_id, partition_key).await
    }
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id).get_inlined_data_count(table_id).await
    }
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        self.for_table(table_id).get_inlined_data_stats(table_id).await
    }
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.for_table(table_id).get_inlined_deletes(table_id).await
    }
    async fn get_published_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.for_table(table_id).get_published_inlined_deletes(table_id).await
    }
    async fn get_unpublished_inlined_delete_ids(&self, table_id: &str) -> CatalogResult<Vec<String>> {
        self.for_table(table_id).get_unpublished_inlined_delete_ids(table_id).await
    }
    async fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        self.for_table(table_id).table_storage_stats(table_id).await
    }
    async fn update_table_schema(&self, table_id: &str, schema: &SchemaRef) -> CatalogResult<()> {
        self.for_table(table_id).update_table_schema(table_id, schema).await
    }
    async fn update_table_schema_dropping_statistics(&self, table_id: &str, schema: &SchemaRef) -> CatalogResult<()> {
        self.for_table(table_id).update_table_schema_dropping_statistics(table_id, schema).await
    }
    async fn set_current_snapshot(&self, table_id: &str, replaced_snapshot_id: &str, new_snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).set_current_snapshot(table_id, replaced_snapshot_id, new_snapshot_id).await
    }
    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id).increment_sequence_number(table_id).await
    }
    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64> {
        self.for_table(table_id).reserve_sequence_numbers(table_id, count).await
    }
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String> {
        self.for_table(&delete_file.table_id).add_delete_file(delete_file).await
    }
    async fn commit_delete_files_with_inlined_rewrite(&self, delete_files: Vec<DeleteFile>, table_id: &str, updated_data: Vec<InlinedData>, deleted_inlined_ids: Vec<String>, replaced_delete_file_ids: Vec<String>) -> CatalogResult<()> {
        self.for_table(table_id).commit_delete_files_with_inlined_rewrite(delete_files, table_id, updated_data, deleted_inlined_ids, replaced_delete_file_ids).await
    }
    async fn remove_delete_files(&self, table_id: &str, delete_file_ids: &[String]) -> CatalogResult<()> {
        self.for_table(table_id).remove_delete_files(table_id, delete_file_ids).await
    }
    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_delete_files(table_id).await
    }
    async fn add_insert_record(&self, table_id: &str, pk_bytes: Vec<u8>, sequence_number: i64) -> CatalogResult<()> {
        self.for_table(table_id).add_insert_record(table_id, pk_bytes, sequence_number).await
    }
    async fn add_insert_records_batch(&self, table_id: &str, pk_bytes_list: Vec<Vec<u8>>, sequence_number: i64) -> CatalogResult<()> {
        self.for_table(table_id).add_insert_records_batch(table_id, pk_bytes_list, sequence_number).await
    }
    async fn commit_on_conflict_deletions(&self, delete_files: Vec<DeleteFile>, table_id: &str, insert_pk_bytes_list: Vec<Vec<u8>>, insert_sequence: i64, snapshot_sequence: Option<SnapshotSequenceCommit>) -> CatalogResult<()> {
        self.for_table(table_id).commit_on_conflict_deletions(delete_files, table_id, insert_pk_bytes_list, insert_sequence, snapshot_sequence).await
    }
    async fn commit_on_conflict_deletions_with_tombstone(&self, delete_files: Vec<DeleteFile>, table_id: &str, insert_pk_bytes_list: Vec<Vec<u8>>, insert_sequence: i64, snapshot_sequence: Option<SnapshotSequenceCommit>, inline_tombstone: Option<InlinedDelete>, pending_durable_flips: &[String]) -> CatalogResult<Option<String>> {
        self.for_table(table_id).commit_on_conflict_deletions_with_tombstone(delete_files, table_id, insert_pk_bytes_list, insert_sequence, snapshot_sequence, inline_tombstone, pending_durable_flips).await
    }
    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_insert_records(table_id).await
    }
    async fn set_snapshot_sequence(&self, table_id: &str, snapshot_id: &str, sequence_number: i64) -> CatalogResult<()> {
        self.for_table(table_id).set_snapshot_sequence(table_id, snapshot_id, sequence_number).await
    }
    async fn clear_snapshot_sequence(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_sequence(table_id, snapshot_id).await
    }
    async fn commit_compaction(&self, table_id: &str, replaced_snapshot_id: &str, new_snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).commit_compaction(table_id, replaced_snapshot_id, new_snapshot_id).await
    }
    async fn commit_compaction_fenced(&self, table_id: &str, replaced_snapshot_id: &str, new_snapshot_id: &str, cutoff: i64, protected_snapshot_ids_to_clear: &[String]) -> CatalogResult<()> {
        self.for_table(table_id).commit_compaction_fenced(table_id, replaced_snapshot_id, new_snapshot_id, cutoff, protected_snapshot_ids_to_clear).await
    }
    async fn swap_protected_snapshots(&self, table_id: &str, old_snapshot_ids: &[String], new_snapshot_id: &str, new_sequence_number: i64) -> CatalogResult<bool> {
        self.for_table(table_id).swap_protected_snapshots(table_id, old_snapshot_ids, new_snapshot_id, new_sequence_number).await
    }
    async fn commit_overwrite(&self, table_id: &str, new_snapshot_id: &str, inlined: Option<&InlinedData>, delete_files: &[crate::metadata::DeleteFile]) -> CatalogResult<()> {
        self.for_table(table_id).commit_overwrite(table_id, new_snapshot_id, inlined, delete_files).await
    }
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String> {
        self.for_table(&partition.table_id).add_partition(partition).await
    }
    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()> {
        self.for_table(&stats.table_id).upsert_table_statistics(stats).await
    }
    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_table_statistics(table_id).await
    }
    async fn upsert_snapshot_file_statistics(&self, stats: &SnapshotFileStatistics) -> CatalogResult<()> {
        self.for_table(&stats.table_id).upsert_snapshot_file_statistics(stats).await
    }
    async fn clear_snapshot_file_statistics_except(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_file_statistics_except(table_id, snapshot_id).await
    }
    async fn clear_snapshot_file_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_file_statistics(table_id).await
    }
    async fn upsert_snapshot_file(&self, file: &SnapshotFile) -> CatalogResult<()> {
        self.for_table(&file.table_id).upsert_snapshot_file(file).await
    }
    async fn replace_snapshot_files(&self, table_id: &str, snapshot_id: &str, files: &[SnapshotFile]) -> CatalogResult<()> {
        self.for_table(table_id).replace_snapshot_files(table_id, snapshot_id, files).await
    }
    async fn clear_snapshot_cached_metadata(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_cached_metadata(table_id, snapshot_id).await
    }
    async fn clear_snapshot_files_except(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_files_except(table_id, snapshot_id).await
    }
    async fn clear_snapshot_files(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_snapshot_files(table_id).await
    }
    async fn commit_overwrite_to_cold(&self, table_id: &str, new_snapshot_id: &str, cold_files: &[ColdTierFile]) -> CatalogResult<()> {
        self.for_table(table_id).commit_overwrite_to_cold(table_id, new_snapshot_id, cold_files).await
    }
    async fn upsert_pk_index(&self, table_id: &str, snapshot_id: &str, index_blob: &[u8]) -> CatalogResult<()> {
        self.for_table(table_id).upsert_pk_index(table_id, snapshot_id, index_blob).await
    }
    async fn clear_pk_index(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_pk_index(table_id).await
    }
    async fn register_index_run(&self, run: &IndexRunRecord) -> CatalogResult<()> {
        self.for_table(&run.table_id).register_index_run(run).await
    }
    async fn remove_index_run(&self, table_id: &str, index_key: &str, run_name: &str) -> CatalogResult<()> {
        self.for_table(table_id).remove_index_run(table_id, index_key, run_name).await
    }
    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String> {
        self.for_table(&data.table_id).add_inlined_data(data).await
    }
    async fn get_inlined_data_above_sequence(&self, table_id: &str, after_sequence: i64) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id).get_inlined_data_above_sequence(table_id, after_sequence).await
    }
    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_inlined_data(table_id).await
    }
    async fn clear_inlined_data_and_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_inlined_data_and_deletes(table_id).await
    }
    async fn commit_on_conflict_deletions_and_clear_inlined(&self, delete_files: Vec<DeleteFile>, table_id: &str, insert_pk_bytes_list: Vec<Vec<u8>>, insert_sequence: i64, snapshot_sequence: Option<SnapshotSequenceCommit>) -> CatalogResult<()> {
        self.for_table(table_id).commit_on_conflict_deletions_and_clear_inlined(delete_files, table_id, insert_pk_bytes_list, insert_sequence, snapshot_sequence).await
    }
    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String> {
        self.for_table(&delete.table_id).add_inlined_delete(delete).await
    }
    async fn mark_inlined_delete_published(&self, table_id: &str, inlined_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).mark_inlined_delete_published(table_id, inlined_id).await
    }
    async fn mark_inlined_delete_unpublished(&self, table_id: &str, inlined_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).mark_inlined_delete_unpublished(table_id, inlined_id).await
    }
    async fn publish_orphan_inlined_deletes(&self, table_id: &str) -> CatalogResult<u64> {
        self.for_table(table_id).publish_orphan_inlined_deletes(table_id).await
    }
    async fn commit_inlined_mutation(&self, table_id: &str, updated_data: Vec<InlinedData>, deleted_inlined_ids: Vec<String>, data: Vec<InlinedData>, assigned_sequence: i64) -> CatalogResult<Option<i64>> {
        self.for_table(table_id).commit_inlined_mutation(table_id, updated_data, deleted_inlined_ids, data, assigned_sequence).await
    }
    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_inlined_deletes(table_id).await
    }
}
