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

//! Implementation of the `DataFusion` Catalog/Schema providers for Iceberg.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::catalog_filter::TableSelector;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use futures::future::{join_all, try_join_all};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use tokio::sync::Semaphore;

use crate::RefreshableCatalogProvider;
use crate::iceberg::catalog::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A namespace's loaded tables, and the tables that failed to load.
struct NamespaceTables {
    tables: HashMap<String, Arc<dyn TableProvider>>,
    failed: Vec<(TableIdent, Error)>,
}

/// A hook that wraps each Iceberg table provider as it is loaded from the
/// catalog, given its `(schema_name, table_name)`. The runtime injects this so
/// catalog-sourced Iceberg scans can be serialized for distributed (Ballista)
/// execution — `data_components` cannot name the runtime's
/// `IcebergClusterTableProvider` directly, so the runtime supplies a closure
/// that applies it. When `None`, table providers are returned unwrapped (the
/// single-node behavior).
pub type CatalogTableWrapper =
    Arc<dyn Fn(&str, &str, Arc<dyn TableProvider>) -> Arc<dyn TableProvider> + Send + Sync>;

/// Provides an interface to manage and access multiple schemas
/// within an Iceberg [`Catalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
pub struct IcebergCatalogProvider {
    /// The underlying Iceberg catalog client.
    catalog: Arc<dyn Catalog>,
    /// Optional root namespace to scope namespace discovery.
    root_namespace: Option<NamespaceIdent>,
    /// Which discovered tables the catalog registers.
    selector: TableSelector,
    /// Optional hook to wrap each loaded table provider (see
    /// [`CatalogTableWrapper`]). Reapplied on every refresh.
    table_wrapper: Option<CatalogTableWrapper>,
    /// A `RwLock`-protected `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl std::fmt::Debug for IcebergCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCatalogProvider")
            .field("root_namespace", &self.root_namespace)
            .field("has_table_wrapper", &self.table_wrapper.is_some())
            .finish_non_exhaustive()
    }
}

impl IcebergCatalogProvider {
    /// Asynchronously tries to construct a new [`IcebergCatalogProvider`]
    /// using the given client to fetch and initialize schema providers for
    /// each namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves the list of namespace names
    /// attempts to create a schema provider for each namespace, and
    /// collects these providers into a `HashMap`.
    ///
    /// # Arguments
    /// * `client` - The Iceberg catalog client
    /// * `root_namespace` - Optional root namespace to start from
    /// * `selector` - Which discovered tables the catalog registers
    pub async fn try_new(
        client: Arc<dyn Catalog>,
        root_namespace: Option<NamespaceIdent>,
        selector: &TableSelector,
        table_wrapper: Option<CatalogTableWrapper>,
    ) -> Result<Self> {
        let schemas = Self::load_schemas(
            Arc::clone(&client),
            root_namespace.as_ref(),
            selector,
            table_wrapper.as_ref(),
            &HashMap::new(),
        )
        .await?;

        Ok(IcebergCatalogProvider {
            catalog: client,
            root_namespace,
            selector: selector.clone(),
            table_wrapper,
            schemas: RwLock::new(schemas),
        })
    }

    /// Returns a reference to the underlying Iceberg catalog client.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Returns the root namespace, if any.
    #[must_use]
    pub fn root_namespace(&self) -> Option<&NamespaceIdent> {
        self.root_namespace.as_ref()
    }

    /// Load all schemas (namespaces) from the Iceberg catalog.
    ///
    /// A table that fails to load is skipped with a warning, or keeps its
    /// provider from `previous` (the schemas being refreshed) if it had one,
    /// so one bad table does not take down the rest of the catalog. If no table
    /// loads at all, the first failure is returned.
    async fn load_schemas(
        client: Arc<dyn Catalog>,
        root_namespace: Option<&NamespaceIdent>,
        selector: &TableSelector,
        table_wrapper: Option<&CatalogTableWrapper>,
        previous: &HashMap<String, Arc<dyn SchemaProvider>>,
    ) -> Result<HashMap<String, Arc<dyn SchemaProvider>>> {
        // Limits concurrent table loads across all namespaces.
        let load_semaphore = Semaphore::new(10);

        // Keep each child namespace as a full `NamespaceIdent`. A REST catalog
        // returns children of `root_namespace` as multi-level identifiers (e.g.
        // `["spice", "public"]`); splitting them into single-level identifiers
        // would list tables under `/v1/namespaces/public` instead of
        // `/v1/namespaces/spice%1Fpublic`.
        let child_namespaces: Vec<NamespaceIdent> =
            match client.list_namespaces(root_namespace).await {
                Ok(namespaces) => namespaces,
                Err(e) => match e.kind() {
                    iceberg::ErrorKind::DataInvalid => {
                        // Unfortunately, there isn't a better way to handle this
                        let err_msg = e.to_string();

                        if let Some(namespace) = root_namespace
                            && (err_msg.contains("NoSuchNamespaceException")
                                || err_msg.contains("Namespace does not exist"))
                        {
                            return Err(Error::NamespaceDoesNotExist {
                                namespace: namespace.join("."),
                            });
                        }

                        return Err(handle_iceberg_error(e));
                    }
                    _ => return Err(handle_iceberg_error(e)),
                },
            };

        let mut loaded = try_join_all(child_namespaces.iter().map(|namespace| {
            IcebergSchemaProvider::load_tables(
                &client,
                namespace,
                &load_semaphore,
                selector,
                table_wrapper,
            )
        }))
        .await?;

        // When nothing loads, the cause is the catalog or its storage, not one table.
        if loaded.iter().all(|namespace| namespace.tables.is_empty())
            && let Some((_, e)) = loaded
                .iter_mut()
                .find_map(|namespace| namespace.failed.pop())
        {
            return Err(e);
        }

        let mut schemas = HashMap::new();
        for (namespace, NamespaceTables { mut tables, failed }) in
            child_namespaces.into_iter().zip(loaded)
        {
            let name = schema_name_for_namespace(&namespace);
            let previous = previous.get(&name);
            for (table, e) in failed {
                if let Some(schema) = previous
                    && let Ok(Some(provider)) = schema.table(table.name()).await
                {
                    tracing::warn!(
                        "Failed to reload Iceberg table '{table}'; it keeps its previously loaded definition. {e}"
                    );
                    tables.insert(table.name().to_string(), provider);
                } else {
                    tracing::warn!(
                        "Skipping Iceberg table '{table}': it failed to load and is unavailable until it does. The catalog's other tables are not affected. {e}"
                    );
                }
            }
            let provider = Arc::new(IcebergSchemaProvider {
                catalog: Arc::clone(&client),
                namespace,
                tables: RwLock::new(tables),
            }) as Arc<dyn SchemaProvider>;
            schemas.insert(name, provider);
        }

        Ok(schemas)
    }
}

/// The schema name a child namespace is registered under: its last level.
fn schema_name_for_namespace(namespace: &NamespaceIdent) -> String {
    namespace.as_ref().last().cloned().unwrap_or_default()
}

impl CatalogProvider for IcebergCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .read()
            .map(|schemas| schemas.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .read()
            .ok()
            .and_then(|schemas| schemas.get(name).cloned())
    }
}

#[async_trait]
impl RefreshableCatalogProvider for IcebergCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let previous = self
            .schemas
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let new_schemas = Self::load_schemas(
            Arc::clone(&self.catalog),
            self.root_namespace.as_ref(),
            &self.selector,
            self.table_wrapper.as_ref(),
            &previous,
        )
        .await?;

        match self.schemas.write() {
            Ok(mut schemas) => {
                *schemas = new_schemas;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = new_schemas;
            }
        }

        Ok(())
    }
}

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
#[derive(Debug)]
pub struct IcebergSchemaProvider {
    /// The underlying Iceberg catalog client.
    catalog: Arc<dyn Catalog>,
    /// The namespace this schema provider manages.
    namespace: NamespaceIdent,
    /// A `RwLock`-protected `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl IcebergSchemaProvider {
    /// Synchronously look up a cached table provider by name.
    ///
    /// Tables are loaded eagerly at construction (and on catalog refresh), so
    /// this is a lock-and-clone with no catalog I/O. The runtime relies on this
    /// to resolve catalog-sourced Iceberg providers on remote executors during
    /// distributed-plan deserialization, where no async context is available.
    #[must_use]
    pub fn table_sync(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // Recover a poisoned lock (as `refresh` does) rather than returning
        // `None`: an unrelated panic must not make a table appear to vanish,
        // which would surface as a confusing "table not registered" error during
        // distributed scan reconstruction on an executor.
        self.tables
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// Returns a reference to the underlying Iceberg catalog client.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Returns the namespace this schema provider manages.
    #[must_use]
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Load all tables from the Iceberg catalog for this namespace, returning
    /// the loaded tables and the tables that failed to load.
    async fn load_tables(
        client: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        load_semaphore: &Semaphore,
        selector: &TableSelector,
        table_wrapper: Option<&CatalogTableWrapper>,
    ) -> Result<NamespaceTables> {
        let table_names: Vec<_> = client
            .list_tables(namespace)
            .await
            .map_err(handle_iceberg_error)?
            .into_iter()
            // Iceberg matches against the fully qualified `TableIdent`, not
            // `"{schema}.{table}"` -- both halves of the selector see that name.
            .filter(|table| selector.selects(&table.to_string()))
            .collect();

        let table_results = join_all(
            table_names
                .iter()
                .map(|name| Self::load_table(client, name, load_semaphore, table_wrapper)),
        )
        .await;

        let mut tables = HashMap::new();
        let mut failed = Vec::new();
        for (name, result) in table_names.into_iter().zip(table_results) {
            match result {
                Ok(Some(provider)) => {
                    tables.insert(name.name().to_string(), provider);
                }
                Ok(None) => {}
                Err(e) => failed.push((name, e)),
            }
        }

        Ok(NamespaceTables { tables, failed })
    }

    async fn load_table(
        catalog: &Arc<dyn Catalog>,
        table_name: &TableIdent,
        semaphore: &Semaphore,
        table_wrapper: Option<&CatalogTableWrapper>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Acquire a permit from the semaphore to limit concurrent table loads
        let _permit = semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError { source: e })?;

        match catalog.load_table(table_name).await {
            Ok(_) => match IcebergTableProvider::try_new(
                Arc::clone(catalog),
                table_name.namespace().clone(),
                table_name.name().to_string(),
            )
            .await
            {
                Ok(provider) => {
                    // Wrap in IcebergDeletionProvider so that
                    // catalog tables support DELETE FROM via equality delete files.
                    // Access control is handled by the SQL validator, not here.
                    let inner: Arc<dyn TableProvider> = Arc::new(provider);
                    let deletion_provider = crate::iceberg::delete::IcebergDeletionProvider::new(
                        Arc::clone(catalog),
                        table_name.namespace().clone(),
                        table_name.name().to_string(),
                        Arc::clone(&inner),
                    );
                    let adapted: Arc<dyn TableProvider> =
                        spice_table::SpiceTable::over(Arc::new(deletion_provider), inner);

                    // Wrap so catalog-sourced Iceberg scans can cross Ballista
                    // node boundaries. The schema name is the (single-level)
                    // namespace under which this table is registered, so the
                    // recipe's `catalog.schema.table` reference resolves back to
                    // this provider on a remote executor. In a single-node
                    // session the wrapper is a transparent pass-through.
                    let adapted = match table_wrapper {
                        Some(wrap) => {
                            let schema_name = table_name.namespace().as_ref().join(".");
                            wrap(&schema_name, table_name.name(), adapted)
                        }
                        None => adapted,
                    };
                    Ok(Some(adapted))
                }
                Err(e) => Err(handle_iceberg_error(e)),
            },
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("NoSuchIcebergTableException") || err_msg.contains("code: 404")
                {
                    tracing::warn!(
                        "Failed to load '{}.{}' as an Iceberg table: table may not exist or is not in Iceberg format.",
                        table_name.namespace().join("."),
                        table_name.name()
                    );
                    Ok(None)
                } else {
                    Err(handle_iceberg_error(e))
                }
            }
        }
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .map(|tables| tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .read()
            .is_ok_and(|tables| tables.contains_key(name))
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .tables
            .read()
            .ok()
            .and_then(|tables| tables.get(name).cloned()))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.insert(name, table)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on tables".to_string(),
            )),
        }
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.remove(name)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on tables".to_string(),
            )),
        }
    }
}

fn handle_iceberg_error(e: iceberg::Error) -> Error {
    match e.kind() {
        iceberg::ErrorKind::DataInvalid => Error::DataInvalid { source: e },
        iceberg::ErrorKind::FeatureUnsupported => Error::FeatureUnsupported { source: e },
        iceberg::ErrorKind::Unexpected => {
            // This is also returned when we cannot connect to the Iceberg catalog, so check for that.
            // i.e. Unexpected => Failed to execute http request, source: error sending request for url (http://localhoster:8181/v1/config)
            let err_msg = e.to_string();
            let err_in_detail = format!("{e:?}");
            let err_in_detail_lc = err_in_detail.to_lowercase();
            if err_msg.contains("error sending request for url") {
                // Extract the URL from the error message
                let url = err_msg
                    .split("error sending request for url")
                    .nth(1)
                    .unwrap_or_default()
                    .trim();

                // Special case for detailed certificate errors
                if err_in_detail_lc.contains("certificate")
                    || err_in_detail_lc.contains("tls")
                    || err_in_detail_lc.contains("ssl")
                {
                    return Error::CertificateError {
                        url: url.to_string(),
                        detail: err_in_detail,
                        source: Box::new(e),
                    };
                }

                // Return a generic connection error for all other cases
                return Error::FailedToConnect {
                    url: url.to_string(),
                    source: Box::new(e),
                };
            }

            Error::Unknown { source: e }
        }
        _ => Error::Unknown { source: e },
    }
}

/// Guards the pinned snapshot read `spiceai/iceberg-rust` fork PR #45 adds to
/// [`IcebergTableProvider`].
///
/// `with_snapshot_id` is the only way to read an Iceberg table as of anything but
/// its current snapshot, and it is how the distributed path plans every task of one
/// query against the snapshot the scheduler chose (the Iceberg arm of the runtime's
/// physical extension codec).
///
/// The fork branch is re-cut per Iceberg and `DataFusion` version, and the loss this
/// guards is the quietest shape a dropped patch can take: a re-cut that keeps the
/// builder and drops the snapshot id it feeds into the table scan still compiles and
/// still scans — it just reads the table's *current* snapshot. Time travel and a
/// repeatable read then return live data, with no error and no difference in the plan
/// a reader would notice. `docs/dev/fork_patches.md` is the ledger this guard is
/// named in.
#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray as _, RecordBatch};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{CatalogBuilder, TableCreation};

    use super::*;

    /// A spec-compliant REST catalog lists the children of `spice` as full
    /// multi-level identifiers. Tables must be listed under the encoded full
    /// identifier (`spice%1Fpublic`), not under the last level alone, and the
    /// schema is registered under that last level.
    #[tokio::test]
    async fn load_schemas_keeps_nested_child_namespace() {
        use iceberg::io::LocalFsStorageFactory;
        use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
        use wiremock::matchers::{method, path, path_regex, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"defaults": {}, "overrides": {}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces"))
            .and(query_param("parent", "spice"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"namespaces": [["spice", "public"]]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(
                r"^/v1/namespaces/spice(%1F|%1f|\x1f)public/tables$",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"identifiers": []})),
            )
            .expect(1)
            .mount(&server)
            .await;
        // Anything else (e.g. `/v1/namespaces/public/tables`) falls through to
        // wiremock's default 404, which surfaces as an error.

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "rest",
                HashMap::from([(REST_CATALOG_PROP_URI.to_string(), server.uri())]),
            )
            .await
            .expect("load REST catalog");
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);

        let root = NamespaceIdent::new("spice".to_string());
        let schemas = IcebergCatalogProvider::load_schemas(
            catalog,
            Some(&root),
            &TableSelector::new(None, None),
            None,
            &HashMap::new(),
        )
        .await
        .expect("nested child namespace loads");

        let names: Vec<_> = schemas.keys().cloned().collect();
        assert_eq!(names, vec!["public".to_string()]);
    }

    const NAMESPACE: &str = "guard_ns";
    const TABLE: &str = "pinned";

    /// A catalog holding one empty single-column table, entirely in memory: the
    /// memory catalog's default storage keeps the metadata and the data files in a
    /// `HashMap`, so this needs no warehouse on disk and no credentials.
    async fn catalog_with_empty_table() -> (Arc<dyn Catalog>, TableIdent) {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:/warehouse".to_string(),
                )]),
            )
            .await
            .expect("memory catalog loads");

        let namespace = NamespaceIdent::new(NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace is created");

        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema builds");

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(TABLE.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table is created");

        (
            Arc::new(catalog),
            TableIdent::new(namespace, TABLE.to_string()),
        )
    }

    async fn provider_for(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> IcebergTableProvider {
        IcebergTableProvider::try_new(
            Arc::clone(catalog),
            ident.namespace().clone(),
            ident.name().to_string(),
        )
        .await
        .expect("provider is constructed")
    }

    /// Register `table` under `name` and return the `id` column of
    /// `SELECT id FROM <name> ORDER BY id`.
    async fn ids_visible_to(
        ctx: &SessionContext,
        name: &str,
        table: IcebergTableProvider,
    ) -> Vec<i64> {
        ctx.register_table(name, Arc::new(table))
            .expect("provider registers");
        let batches = ctx
            .sql(&format!("SELECT id FROM {name} ORDER BY id"))
            .await
            .expect("scan plans")
            .collect()
            .await
            .expect("scan executes");

        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|value| value.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The `id` column of a scan taken straight off the provider, so the rows counted
    /// are the ones the Iceberg scan itself emits rather than the ones a
    /// `GlobalLimitExec` above it would have trimmed anyway.
    async fn ids_from_scan(
        ctx: &SessionContext,
        provider: &IcebergTableProvider,
        limit: Option<usize>,
    ) -> Vec<i64> {
        let plan = provider
            .scan(&ctx.state(), None, &[], limit)
            .await
            .expect("scan plans");
        let batches = collect(plan, ctx.task_ctx()).await.expect("scan executes");
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|value| value.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Guards the limit push-down `spiceai/iceberg-rust` fork PR #19 adds to
    /// [`IcebergTableProvider`]: the scan carries the limit into file planning and
    /// truncates the stream it emits, instead of reading the table and leaving the
    /// trimming to the operator above.
    ///
    /// Asserted at the provider rather than through SQL because SQL cannot see it: a
    /// `GlobalLimitExec` sits above the scan and returns the right rows either way,
    /// so the only observable difference is how many rows the scan itself produced.
    /// A single partition is what makes that count exact — the limit the fork applies
    /// is per-partition, so several partitions would each be entitled to it.
    ///
    /// The distributed path cannot lose this quietly (the cluster codec refuses to
    /// serialise a scan whose limit it cannot carry); the single-node scan can, which
    /// is the half this covers.
    #[tokio::test]
    async fn a_scan_given_a_limit_reads_no_more_rows_than_it_asked_for() {
        let (catalog, ident) = catalog_with_empty_table().await;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        ctx.register_table("writable", Arc::new(provider_for(&catalog, &ident).await))
            .expect("provider registers");
        ctx.sql("INSERT INTO writable VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
            .await
            .expect("append plans")
            .collect()
            .await
            .expect("append commits");

        let provider = provider_for(&catalog, &ident).await;

        // The control: with no limit the scan emits the whole table, so the count
        // below is a limit being applied rather than a table that was already short.
        assert_eq!(
            ids_from_scan(&ctx, &provider, None).await.len(),
            10,
            "an unlimited scan reads the whole table"
        );

        assert_eq!(
            ids_from_scan(&ctx, &provider, Some(3)).await.len(),
            3,
            "a scan given a limit must stop at it, not read the table and let the operator \
             above trim the result"
        );
    }

    /// Guards the session batch size passed to the fork's `IcebergTableScan`.
    /// The size is neither the Parquet reader's 1024 nor `DataFusion`'s 8192
    /// default, so a scan that hard-codes either one fails.
    #[tokio::test]
    async fn a_scan_uses_the_session_batch_size() {
        let (catalog, ident) = catalog_with_empty_table().await;
        let batch_size = 5000;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(batch_size),
        );
        ctx.register_table("writable", Arc::new(provider_for(&catalog, &ident).await))
            .expect("provider registers");
        ctx.sql("INSERT INTO writable SELECT CAST(value AS BIGINT) AS id FROM generate_series(0, 19999)")
            .await.expect("append plans").collect().await.expect("append commits");
        let table = catalog.load_table(&ident).await.expect("table loads");
        let files: Vec<_> = futures::TryStreamExt::try_collect(
            table
                .scan()
                .build()
                .expect("file scan builds")
                .plan_files()
                .await
                .expect("files plan"),
        )
        .await
        .expect("files collect");
        assert_eq!(files.len(), 1, "fixture must contain one data file");
        let provider = provider_for(&catalog, &ident).await;
        let plan = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan plans");
        let batches = collect(plan, ctx.task_ctx()).await.expect("scan executes");
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            20_000
        );
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).max(),
            Some(batch_size)
        );
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|id| id.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..20_000).collect::<Vec<i64>>());
    }

    #[tokio::test]
    async fn a_scan_pinned_to_a_snapshot_reads_that_snapshot_not_the_current_one() {
        let (catalog, ident) = catalog_with_empty_table().await;
        let ctx = SessionContext::new();

        ctx.register_table("writable", Arc::new(provider_for(&catalog, &ident).await))
            .expect("provider registers");

        ctx.sql("INSERT INTO writable VALUES (1)")
            .await
            .expect("first append plans")
            .collect()
            .await
            .expect("first append commits");
        let pinned_snapshot = catalog
            .load_table(&ident)
            .await
            .expect("table loads")
            .metadata()
            .current_snapshot_id()
            .expect("the first append published a snapshot");

        ctx.sql("INSERT INTO writable VALUES (2)")
            .await
            .expect("second append plans")
            .collect()
            .await
            .expect("second append commits");

        // The control: an unpinned provider follows the table, so it sees the row
        // the second append added.
        assert_eq!(
            ids_visible_to(&ctx, "current", provider_for(&catalog, &ident).await).await,
            vec![1, 2],
            "an unpinned scan reads the current snapshot"
        );

        // The guard: pinned to the first snapshot, the same scan must not see it.
        assert_eq!(
            ids_visible_to(
                &ctx,
                "at_pin",
                provider_for(&catalog, &ident)
                    .await
                    .with_snapshot_id(Some(pinned_snapshot)),
            )
            .await,
            vec![1],
            "a scan pinned to a snapshot must read that snapshot, not the current one"
        );
    }

    /// Writes `ns.<table>` with one row under `warehouse`, laid out the way a
    /// Hadoop catalog reads it: `<warehouse>/ns/<table>/{metadata,data}`.
    async fn write_hadoop_table(warehouse: &std::path::Path, table: &str) {
        use iceberg::io::LocalFsStorageFactory;

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await
            .expect("memory catalog loads");
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace is created");
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema builds");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table is created");
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let ident = TableIdent::new(namespace, table.to_string());
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider_for(&catalog, &ident).await))
            .expect("provider registers");
        ctx.sql("INSERT INTO t VALUES (1)")
            .await
            .expect("append plans")
            .collect()
            .await
            .expect("append commits");
    }

    /// Points `ns.<table>`'s version hint at a metadata version that does not exist.
    fn break_hadoop_table(warehouse: &std::path::Path, table: &str) {
        std::fs::write(
            warehouse
                .join("ns")
                .join(table)
                .join("metadata")
                .join("version-hint.text"),
            "9",
        )
        .expect("version hint is written");
    }

    async fn hadoop_catalog(warehouse: &std::path::Path) -> Arc<dyn Catalog> {
        use crate::iceberg::catalog::hadoop::HadoopCatalogBuilder;

        let mut config = opendal::services::FsConfig::default();
        config.root = Some(warehouse.display().to_string());
        let operator = opendal::Operator::new(opendal::Configurator::into_builder(config))
            .expect("fs operator builds");
        let warehouse_url =
            url::Url::from_directory_path(warehouse).expect("warehouse is an absolute path");
        let catalog = HadoopCatalogBuilder::default()
            .with_warehouse_root(warehouse_url.to_string())
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
            .with_operator(operator)
            .build()
            .await
            .expect("hadoop catalog builds");
        Arc::new(catalog)
    }

    async fn load_hadoop_catalog(warehouse: &std::path::Path) -> Result<IcebergCatalogProvider> {
        IcebergCatalogProvider::try_new(
            hadoop_catalog(warehouse).await,
            None,
            &TableSelector::select_all(),
            None,
        )
        .await
    }

    fn table_names(provider: &IcebergCatalogProvider) -> Vec<String> {
        let mut names = provider
            .schema("ns")
            .expect("schema 'ns' is registered")
            .table_names();
        names.sort();
        names
    }

    /// The `id`s `SELECT id FROM wh.ns.<table>` returns with the catalog
    /// registered as `wh`, the way a query reads a catalog table.
    async fn ids_in(provider: &Arc<IcebergCatalogProvider>, table: &str) -> Vec<i64> {
        let ctx = SessionContext::new();
        ctx.register_catalog("wh", Arc::clone(provider) as Arc<dyn CatalogProvider>);
        let batches = ctx
            .sql(&format!("SELECT id FROM wh.ns.{table}"))
            .await
            .expect("query plans")
            .collect()
            .await
            .expect("query executes");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|id| id.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn a_table_that_fails_to_load_does_not_take_down_the_catalog() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        write_hadoop_table(warehouse.path(), "good").await;
        write_hadoop_table(warehouse.path(), "bad").await;
        break_hadoop_table(warehouse.path(), "bad");

        let provider = Arc::new(
            load_hadoop_catalog(warehouse.path())
                .await
                .expect("the catalog loads despite one bad table"),
        );

        assert_eq!(table_names(&provider), vec!["good".to_string()]);
        assert_eq!(ids_in(&provider, "good").await, vec![1]);
    }

    #[tokio::test]
    async fn a_table_that_fails_on_refresh_keeps_its_previous_definition() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        write_hadoop_table(warehouse.path(), "good").await;
        write_hadoop_table(warehouse.path(), "bad").await;

        let provider = Arc::new(
            load_hadoop_catalog(warehouse.path())
                .await
                .expect("the catalog loads"),
        );
        assert_eq!(table_names(&provider), vec!["bad", "good"]);

        break_hadoop_table(warehouse.path(), "bad");
        write_hadoop_table(warehouse.path(), "added").await;
        provider
            .refresh()
            .await
            .expect("the refresh succeeds despite one bad table");

        assert_eq!(table_names(&provider), vec!["added", "bad", "good"]);
        assert_eq!(ids_in(&provider, "good").await, vec![1]);
        assert_eq!(ids_in(&provider, "added").await, vec![1]);
    }

    #[tokio::test]
    async fn a_catalog_whose_every_table_fails_to_load_fails() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        write_hadoop_table(warehouse.path(), "bad").await;
        break_hadoop_table(warehouse.path(), "bad");

        let result = load_hadoop_catalog(warehouse.path()).await;

        assert!(
            matches!(result, Err(Error::DataInvalid { .. })),
            "{result:?}"
        );
    }
}
