/*
Copyright 2026 The Spice.ai OSS Authors

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

//! ADBC data connector for Spice.ai runtime.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds.

use adbc_core::options::AdbcVersion;
use adbc_core::{Driver as _, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
use runtime::component::dataset::Dataset;
use runtime::secrets::ParameterSpec;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use data_components::adbc_helpers::{
    build_db_options, build_join_context, dialect_for_driver, enrich_with_bigquery_metadata,
};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
};
use runtime::parameters::Parameters;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: adbc_driver"))]
    MissingAdbcDriver,

    #[snafu(display("Missing required parameter: adbc_uri"))]
    MissingAdbcUri,

    #[snafu(display(
        "Invalid value for parameter '{name}': expected a positive integer, got '{value}'"
    ))]
    InvalidPoolParameter { name: String, value: String },

    #[snafu(display("Invalid value for parameter 'adbc_{name}': expected a non-empty string"))]
    InvalidEmptyParameter { name: String },

    #[snafu(display("Failed to load ADBC driver '{driver_location}': {source}"))]
    UnableToLoadDriver {
        driver_location: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC database (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateDatabase {
        driver_location: String,
        uri: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC connection pool (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateConnectionPool {
        driver_location: String,
        uri: String,
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },

    #[snafu(display(
        "Invalid 'query_federation' value '{value}'. Expected 'enabled' or 'disabled'."
    ))]
    InvalidQueryFederation { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Adbc {
    /// Wrapped in `Option` so `Drop` can move the factory to a blocking thread
    /// for cleanup. ADBC drivers perform synchronous FFI calls during drop
    /// (e.g. closing network sessions) that must not run on the async runtime.
    factory: Option<AdbcTableFactory<adbc_driver_manager::ManagedDatabase>>,
    pool: Weak<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    driver_name: String,
}

impl std::fmt::Debug for Adbc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adbc").finish_non_exhaustive()
    }
}

impl Drop for Adbc {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            // Send to the dedicated cleanup thread so we don't stall the Tokio
            // runtime. If the bounded queue is full or the worker is gone,
            // offload to a dedicated overflow thread instead of dropping inline.
            match adbc_cleanup_sender().try_send(factory) {
                Ok(()) => {}
                Err(std::sync::mpsc::TrySendError::Full(factory)) => {
                    offload_adbc_factory_drop(
                        factory,
                        "ADBC cleanup queue full; offloading drop to overflow thread",
                    );
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(factory)) => {
                    offload_adbc_factory_drop(
                        factory,
                        "ADBC cleanup channel closed; offloading drop to overflow thread",
                    );
                }
            }
        }
    }
}

const ADBC_CLEANUP_QUEUE_CAPACITY: usize = 64;

fn offload_adbc_factory_drop(
    factory: AdbcTableFactory<adbc_driver_manager::ManagedDatabase>,
    reason: &str,
) {
    tracing::warn!("{reason}");
    if std::thread::Builder::new()
        .name("adbc-cleanup-overflow".to_string())
        .spawn(move || {
            drop(factory);
        })
        .is_err()
    {
        tracing::warn!("Failed to spawn overflow ADBC cleanup thread; dropping inline");
    }
}

/// Returns a sender for offloading ADBC factory cleanup to a dedicated
/// background thread. The worker thread is created once (on first use) and
/// processes drop work sequentially with bounded buffering, avoiding both
/// unbounded thread spawns and unbounded cleanup backlog growth.
fn adbc_cleanup_sender()
-> &'static std::sync::mpsc::SyncSender<AdbcTableFactory<adbc_driver_manager::ManagedDatabase>> {
    static SENDER: OnceLock<
        std::sync::mpsc::SyncSender<AdbcTableFactory<adbc_driver_manager::ManagedDatabase>>,
    > = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel::<
            AdbcTableFactory<adbc_driver_manager::ManagedDatabase>,
        >(ADBC_CLEANUP_QUEUE_CAPACITY);
        if std::thread::Builder::new()
            .name("adbc-cleanup".to_string())
            .spawn(move || {
                for factory in rx {
                    drop(factory);
                }
            })
            .is_err()
        {
            tracing::warn!(
                "Failed to spawn ADBC cleanup worker thread; subsequent cleanup will use the overflow thread path or be performed inline as a last resort"
            );
        }
        tx
    })
}

type ConnectorCache = parking_lot::Mutex<HashMap<String, Arc<ConnectorCacheEntry>>>;

#[derive(Default)]
enum ConnectorCacheState {
    #[default]
    Vacant,
    Initializing,
    Ready(Weak<dyn DataConnector>),
}

struct ConnectorCacheEntry {
    state: parking_lot::Mutex<ConnectorCacheState>,
    notify: tokio::sync::Notify,
}

struct ConnectorInitializationGuard {
    entry: Arc<ConnectorCacheEntry>,
    active: bool,
}

impl ConnectorCacheState {
    fn should_retain(&self) -> bool {
        match self {
            Self::Vacant => false,
            Self::Initializing => true,
            Self::Ready(connector) => connector.upgrade().is_some(),
        }
    }
}

impl ConnectorCacheEntry {
    fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(ConnectorCacheState::Vacant),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn should_retain(&self) -> bool {
        self.state.lock().should_retain()
    }
}

impl ConnectorInitializationGuard {
    fn new(entry: Arc<ConnectorCacheEntry>) -> Self {
        Self {
            entry,
            active: true,
        }
    }

    fn complete(&mut self, result: &runtime::dataconnector::NewDataConnectorResult) {
        {
            let mut state = self.entry.state.lock();
            *state = match result {
                Ok(connector) => ConnectorCacheState::Ready(Arc::downgrade(connector)),
                Err(_) => ConnectorCacheState::Vacant,
            };
        }
        self.active = false;
        self.entry.notify.notify_waiters();
    }
}

impl Drop for ConnectorInitializationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        {
            let mut state = self.entry.state.lock();
            if matches!(&*state, ConnectorCacheState::Initializing) {
                *state = ConnectorCacheState::Vacant;
            }
        }
        self.entry.notify.notify_waiters();
    }
}

pub struct AdbcFactory {
    /// Cache of per-config ADBC connector entries keyed by a deterministic
    /// representation of their configuration. Datasets with identical ADBC
    /// config share a single connector initialization and connection pool while
    /// they remain in use.
    ///
    /// Each entry tracks whether initialization is in-flight or a ready
    /// connector is currently alive. Ready entries store a `Weak` reference so
    /// unused connectors and their pools can be dropped after dataset removal,
    /// reload, or credential rotation. Stale and failed entries are pruned
    /// opportunistically on subsequent creates.
    cache: ConnectorCache,
}

impl std::fmt::Debug for AdbcFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcFactory").finish_non_exhaustive()
    }
}

impl AdbcFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self::new()) as Arc<dyn DataConnectorFactory>
    }
}

impl Default for AdbcFactory {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("driver")
        .description("The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres')")
        .required(),
    ParameterSpec::component("driver_path").description("Optional path to the ADBC driver library"),
    ParameterSpec::component("uri")
        .description("Database URI/connection string for the ADBC driver")
        .required(),
    ParameterSpec::component("username")
        .description("Username for database authentication")
        .secret(),
    ParameterSpec::component("password")
        .description("Password for database authentication")
        .secret(),
    ParameterSpec::component("driver_options").description(
        "Semicolon-delimited driver-specific database options (e.g., 'key1=value1;key2=value2')",
    ),
    ParameterSpec::component("catalog").description("The catalog for the connection"),
    ParameterSpec::component("schema").description("The schema for the connection"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections in the connection pool.")
        .default("5"),
    ParameterSpec::runtime("connection_pool_min_idle")
        .description("The minimum number of idle connections to keep open in the pool.")
        .default("1"),
    ParameterSpec::runtime("query_federation")
        .description("Enable or disable query federation for this connector. Valid values: 'enabled' (default), 'disabled'.")
        .default("enabled"),
];

impl AdbcFactory {
    /// Performs the actual ADBC driver initialization.
    async fn init_connector(
        params: ConnectorParams,
    ) -> runtime::dataconnector::NewDataConnectorResult {
        let driver_name = params
            .parameters
            .get("driver")
            .expose()
            .ok()
            .context(MissingAdbcDriverSnafu)
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "adbc".to_string(),
                connector_component: params.component.clone(),
                source: Box::new(e),
            })?;

        let driver_name_owned = driver_name.to_string();
        let driver_path = params.parameters.get("driver_path").expose().ok();
        let driver_location = driver_path.unwrap_or(driver_name).to_string();

        let uri = params
            .parameters
            .get("uri")
            .expose()
            .ok()
            .context(MissingAdbcUriSnafu)
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "adbc".to_string(),
                connector_component: params.component.clone(),
                source: Box::new(e),
            })?;

        let uri_str = uri.to_string();

        let username = params.parameters.get("username").expose().ok();
        let password = params.parameters.get("password").expose().ok();
        let driver_options = params.parameters.get("driver_options").expose().ok();
        let db_options = build_db_options(&uri_str, username, password, driver_options);

        let connection_namespace =
            resolve_connection_namespace(&driver_name_owned, &params.component, &params.parameters)
                .map_err(|e| DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                })?;

        let conn_options = build_conn_options(
            connection_namespace.catalog.as_deref(),
            connection_namespace.schema.as_deref(),
        );

        let join_context = build_join_context(
            &uri_str,
            username,
            connection_namespace.catalog.as_deref(),
            connection_namespace.schema.as_deref(),
        );

        let federation_enabled = is_query_federation_enabled(&params.parameters).map_err(|e| {
            DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "adbc".to_string(),
                connector_component: params.component.clone(),
                message: e.to_string(),
            }
        })?;

        let parse_pool_param = |name: &str| -> std::result::Result<Option<u32>, Error> {
            match params.parameters.get(name).expose().ok() {
                Some(v) => {
                    let parsed = v.parse::<u32>().map_err(|_| Error::InvalidPoolParameter {
                        name: name.to_string(),
                        value: v.to_string(),
                    })?;
                    if parsed == 0 {
                        return Err(Error::InvalidPoolParameter {
                            name: name.to_string(),
                            value: v.to_string(),
                        });
                    }
                    Ok(Some(parsed))
                }
                None => Ok(None),
            }
        };

        let pool_size = parse_pool_param("connection_pool_size").map_err(|e| {
            DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "adbc".to_string(),
                connector_component: params.component.clone(),
                source: Box::new(e),
            }
        })?;
        let pool_min_idle = parse_pool_param("connection_pool_min_idle").map_err(|e| {
            DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "adbc".to_string(),
                connector_component: params.component.clone(),
                source: Box::new(e),
            }
        })?;

        let component = params.component.clone();

        if uri_str == ":memory:" || uri_str.contains("mode=memory") {
            let err: Box<dyn std::error::Error + Send + Sync> =
                Box::new(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "adbc".to_string(),
                    connector_component: component,
                    message: "In-memory database URIs (e.g., ':memory:') are not supported because each pooled connection creates an isolated database, leading to data inconsistency".to_string(),
                });
            return Err(err);
        }

        // Driver loading, database creation, and pool creation are all
        // synchronous FFI/IO operations — offload to a blocking thread.
        //
        // Note: aborting a `spawn_blocking` task is best-effort and will not
        // reliably stop the underlying blocking FFI call. If the timeout below
        // fires, the driver initialization may continue running in the
        // background. The timeout bounds only the *await*, not the actual
        // blocking execution. Use driver-level/connect-level timeouts where
        // the driver supports them for stricter cancellation.
        let init_handle = tokio::task::spawn_blocking(move || -> Result<Arc<ADBCPool<_>>> {
            let mut driver = ManagedDriver::load_from_name(
                &driver_location,
                None,
                AdbcVersion::V110,
                LOAD_FLAG_DEFAULT,
                None,
            )
            .context(UnableToLoadDriverSnafu {
                driver_location: driver_location.clone(),
            })?;

            let db =
                driver
                    .new_database_with_opts(db_options)
                    .context(UnableToCreateDatabaseSnafu {
                        driver_location: driver_location.clone(),
                        uri: uri_str.clone(),
                    })?;

            let mut pool_builder = AdbcConnectionPoolBuilder::new(db)
                .with_max_size(pool_size)
                .with_min_idle(pool_min_idle)
                .with_join_push_down(JoinPushDown::AllowedFor(join_context));

            if let Some(conn_opts) = conn_options {
                pool_builder = pool_builder.with_conn_options(conn_opts);
            }

            let pool = pool_builder
                .build()
                .context(UnableToCreateConnectionPoolSnafu {
                    driver_location,
                    uri: uri_str,
                })?;

            Ok(Arc::new(pool))
        });
        let abort_handle = init_handle.abort_handle();

        let pool = tokio::time::timeout(std::time::Duration::from_secs(120), init_handle)
            .await
            .map_err(|_elapsed| {
                abort_handle.abort();
                DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: component.clone(),
                    source: "ADBC driver initialization timed out after 120 seconds".into(),
                }
            })?
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "adbc".to_string(),
                connector_component: component.clone(),
                source: Box::new(e),
            })?
            .map_err(|e| {
                let error_string = e.to_string();
                if is_auth_or_permission_error(&error_string) {
                    let hint = auth_permission_hint(&driver_name_owned);
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("adbc ({driver_name_owned})"),
                        connector_component: component,
                        source: format!("{error_string}. {hint}").into(),
                    }
                } else {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: "adbc".to_string(),
                        connector_component: component,
                        source: Box::new(e),
                    }
                }
            })?;

        let adbc_factory =
            AdbcTableFactory::new(Arc::clone(&pool)).with_federation_enabled(federation_enabled);

        Ok(Arc::new(Adbc {
            factory: Some(adbc_factory),
            pool: Arc::downgrade(&pool),
            driver_name: driver_name_owned,
        }) as Arc<dyn DataConnector>)
    }
}

async fn enrich_with_bigquery_metadata_from_weak_pool(
    driver_name: &str,
    pool: &Weak<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    table_reference: &TableReference,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    let Some(pool) = pool.upgrade() else {
        tracing::warn!(
            table = %table_reference,
            "Failed to query BigQuery schema metadata via ADBC because the connection pool is shutting down; registering without source metadata"
        );
        return provider;
    };

    enrich_with_bigquery_metadata(driver_name, &pool, table_reference, provider).await
}

impl DataConnectorFactory for AdbcFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        let cache_key = compute_adbc_cache_key(&params);

        let entry = {
            let mut cache = self.cache.lock();
            cache.retain(|_, entry| entry.should_retain());
            Arc::clone(
                cache
                    .entry(cache_key)
                    .or_insert_with(|| Arc::new(ConnectorCacheEntry::new())),
            )
        };

        Box::pin(async move {
            enum CacheAction {
                Return(Arc<dyn DataConnector>),
                Initialize,
                Wait,
            }

            loop {
                let notified = entry.notify.notified();
                let action = {
                    let mut state = entry.state.lock();
                    match &*state {
                        ConnectorCacheState::Ready(connector) => {
                            if let Some(connector) = connector.upgrade() {
                                CacheAction::Return(connector)
                            } else {
                                *state = ConnectorCacheState::Initializing;
                                CacheAction::Initialize
                            }
                        }
                        ConnectorCacheState::Vacant => {
                            *state = ConnectorCacheState::Initializing;
                            CacheAction::Initialize
                        }
                        ConnectorCacheState::Initializing => CacheAction::Wait,
                    }
                };

                match action {
                    CacheAction::Return(connector) => return Ok(connector),
                    CacheAction::Initialize => {
                        let mut init_guard = ConnectorInitializationGuard::new(Arc::clone(&entry));
                        let result = Self::init_connector(params.clone()).await;
                        init_guard.complete(&result);
                        return result;
                    }
                    CacheAction::Wait => notified.await,
                }
            }
        })
    }
    fn prefix(&self) -> &'static str {
        "adbc"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Computes a deterministic cache key from the ADBC connection parameters.
/// Datasets with identical ADBC configuration produce the same key and share
/// a single connector instance.  The key is a fixed-size BLAKE3 hex digest
/// over all identity-relevant parameters.
fn compute_adbc_cache_key(params: &ConnectorParams) -> String {
    // All ADBC configuration parameters that determine connection identity,
    // listed alphabetically for deterministic output.
    let keys = [
        "connection_pool_min_idle",
        "connection_pool_size",
        "driver",
        "driver_options",
        "driver_path",
        "password",
        "query_federation",
        "uri",
        "username",
    ];

    let mut hasher = blake3::Hasher::new();
    for k in &keys {
        let v = params.parameters.get(k).expose().ok().unwrap_or("");
        hasher.update(k.as_bytes());
        hasher.update(b"\0");
        hasher.update(v.as_bytes());
        hasher.update(b"\0");
    }

    let connection_namespace = connection_namespace_for_cache_key(params);
    for (key, value) in [
        ("catalog", connection_namespace.catalog.as_deref()),
        ("schema", connection_namespace.schema.as_deref()),
    ] {
        hasher.update(key.as_bytes());
        hasher.update(b"\0");
        match value {
            Some(value) => {
                hasher.update(b"1");
                hasher.update(b"\0");
                hasher.update(value.as_bytes());
            }
            None => {
                hasher.update(b"0");
            }
        }
        hasher.update(b"\0");
    }

    hasher.finalize().to_hex().to_string()
}

/// Builds the list of ADBC database options from connector parameters.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConnectionNamespace {
    catalog: Option<String>,
    schema: Option<String>,
}

fn optional_connection_name(params: &Parameters, name: &str) -> Result<Option<String>> {
    match params.get(name).expose().ok() {
        Some(value) if value.trim().is_empty() => InvalidEmptyParameterSnafu {
            name: name.to_string(),
        }
        .fail(),
        Some(value) => Ok(Some(value.to_string())),
        None => Ok(None),
    }
}

fn resolve_connection_namespace(
    driver_name: &str,
    component: &ConnectorComponent,
    params: &Parameters,
) -> Result<ConnectionNamespace> {
    let explicit = ConnectionNamespace {
        catalog: optional_connection_name(params, "catalog")?,
        schema: optional_connection_name(params, "schema")?,
    };

    if driver_name != "bigquery" {
        return Ok(explicit);
    }

    let inferred = infer_bigquery_namespace(component);
    Ok(ConnectionNamespace {
        catalog: explicit.catalog.or(inferred.catalog),
        schema: explicit.schema.or(inferred.schema),
    })
}

fn infer_bigquery_namespace(component: &ConnectorComponent) -> ConnectionNamespace {
    match component {
        ConnectorComponent::Dataset(dataset) => infer_bigquery_namespace_from_dataset(dataset),
        ConnectorComponent::Catalog(_) => ConnectionNamespace::default(),
    }
}

fn infer_bigquery_namespace_from_dataset(dataset: &Dataset) -> ConnectionNamespace {
    let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
    dataset.parse_path(true, Some(&dialect)).map_or_else(
        |_| infer_bigquery_namespace_from_path(dataset.path()),
        |table_reference| connection_namespace_from_table_reference(&table_reference),
    )
}

fn connection_namespace_from_table_reference(
    table_reference: &TableReference,
) -> ConnectionNamespace {
    match table_reference {
        TableReference::Full {
            catalog, schema, ..
        } => ConnectionNamespace {
            catalog: Some(catalog.to_string()),
            schema: Some(schema.to_string()),
        },
        TableReference::Partial { schema, .. } => ConnectionNamespace {
            catalog: None,
            schema: Some(schema.to_string()),
        },
        TableReference::Bare { .. } => ConnectionNamespace::default(),
    }
}

fn infer_bigquery_namespace_from_path(path: &str) -> ConnectionNamespace {
    if path
        .chars()
        .any(|ch| ch.is_whitespace() || ch == '`' || ch == '"')
    {
        return ConnectionNamespace::default();
    }

    let parts: Vec<&str> = path.split('.').map(str::trim).collect();
    match parts.as_slice() {
        [schema, table] if !schema.is_empty() && !table.is_empty() => ConnectionNamespace {
            catalog: None,
            schema: Some((*schema).to_string()),
        },
        [catalog, schema, table]
            if !catalog.is_empty() && !schema.is_empty() && !table.is_empty() =>
        {
            ConnectionNamespace {
                catalog: Some((*catalog).to_string()),
                schema: Some((*schema).to_string()),
            }
        }
        _ => ConnectionNamespace::default(),
    }
}

fn connection_namespace_for_cache_key(params: &ConnectorParams) -> ConnectionNamespace {
    let explicit = ConnectionNamespace {
        catalog: params
            .parameters
            .get("catalog")
            .expose()
            .ok()
            .map(String::from),
        schema: params
            .parameters
            .get("schema")
            .expose()
            .ok()
            .map(String::from),
    };

    if params.parameters.get("driver").expose().ok() != Some("bigquery") {
        return explicit;
    }

    let inferred = infer_bigquery_namespace(&params.component);
    ConnectionNamespace {
        catalog: explicit.catalog.or(inferred.catalog),
        schema: explicit.schema.or(inferred.schema),
    }
}

/// Builds connection-level options from connector parameters.
fn build_conn_options(
    catalog: Option<&str>,
    schema: Option<&str>,
) -> Option<HashMap<String, String>> {
    let mut opts = HashMap::new();

    if let Some(catalog) = catalog {
        opts.insert(
            adbc_core::options::OptionConnection::CurrentCatalog
                .as_ref()
                .to_string(),
            catalog.to_string(),
        );
    }

    if let Some(schema) = schema {
        opts.insert(
            adbc_core::options::OptionConnection::CurrentSchema
                .as_ref()
                .to_string(),
            schema.to_string(),
        );
    }

    if opts.is_empty() { None } else { Some(opts) }
}

/// Builds a hashed join-pushdown context identifier for ADBC connections.
///
/// ADBC URIs are driver-vendor-specific and can mix sensitive credentials
/// with critical identity information (e.g. `bigquery:///project?DatasetId=x`)
/// in ways that cannot be reliably parsed. We hash all identity-relevant
/// parts together (similar to the ODBC connector approach) so that:
///
/// - No secrets are ever exposed in `EXPLAIN` plans (`compute_context=...`)
/// - Two connections to the same database instance produce the same hash,
///   enabling federated join pushdown
/// - Different usernames, catalogs, or schemas produce different hashes,
///   preventing incorrect cross-credential pushdown

/// Checks if an error message indicates an authentication or authorization failure.
///
/// `BigQuery`'s ADBC driver often returns `Status::Unknown` with auth details in the
/// message body rather than using `Status::Unauthenticated`/`Status::Unauthorized`,
/// so we also inspect the error text.
fn is_auth_or_permission_error(error_message: &str) -> bool {
    let lower = error_message.to_lowercase();
    lower.contains("invalid_grant")
        || lower.contains("reauth related error")
        || lower.contains("unauthenticated")
        || lower.contains("unauthorized")
        || lower.contains("access denied")
        || lower.contains("permission denied")
        || lower.contains("forbidden")
        || lower.contains("credentials")
        || lower.contains("invalid credentials")
}

/// Returns a driver-specific hint for auth/permission errors.
fn auth_permission_hint(driver_name: &str) -> &'static str {
    if driver_name == "bigquery" {
        "Verify your BigQuery credentials are valid, not expired, and have the required permissions. If you use gcloud user auth, re-run `gcloud auth application-default login`. If you use a service account, confirm its key or workload identity is configured correctly and has BigQuery access. See: https://cloud.google.com/bigquery/docs/authentication"
    } else {
        "Verify the configured credentials are valid and have the required permissions."
    }
}

/// Classify a provider error into a more specific [`DataConnectorError`] for
/// `BigQuery` auth/permission failures, falling back to `fallback_variant`.
fn classify_adbc_error(
    error: Box<dyn std::error::Error + Send + Sync>,
    driver_name: &str,
    dataset: &Dataset,
    fallback_variant: fn(
        String,
        ConnectorComponent,
        Box<dyn std::error::Error + Send + Sync>,
    ) -> DataConnectorError,
) -> DataConnectorError {
    let error_string = error.to_string();
    if is_auth_or_permission_error(&error_string) {
        let hint = auth_permission_hint(driver_name);
        DataConnectorError::UnableToConnectInternal {
            dataconnector: format!("adbc ({driver_name})"),
            connector_component: ConnectorComponent::from(dataset),
            source: format!("{error_string}. {hint}").into(),
        }
    } else {
        fallback_variant("adbc".to_string(), ConnectorComponent::from(dataset), error)
    }
}

#[async_trait]
impl DataConnector for Adbc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
        let adbc_factory =
            self.factory
                .as_ref()
                .ok_or_else(|| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: "ADBC connector has been shut down".into(),
                })?;
        let table_reference: TableReference = dataset.path().into();
        let dialect = dialect_for_driver(&self.driver_name);
        let provider = adbc_factory
            .table_provider(table_reference.clone(), dialect)
            .await
            .map_err(|e| {
                classify_adbc_error(e, &self.driver_name, dataset, |dc, cc, src| {
                    DataConnectorError::UnableToGetReadProvider {
                        dataconnector: dc,
                        connector_component: cc,
                        source: src,
                    }
                })
            })?;

        Ok(enrich_with_bigquery_metadata_from_weak_pool(
            &self.driver_name,
            &self.pool,
            &table_reference,
            provider,
        )
        .await)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>>> {
        let adbc_factory =
            self.factory
                .as_ref()
                .ok_or_else(|| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: "ADBC connector has been shut down".into(),
                });
        let adbc_factory = match adbc_factory {
            Ok(f) => f,
            Err(e) => return Some(Err(e)),
        };
        let table_reference: TableReference = dataset.path().into();
        let dialect = dialect_for_driver(&self.driver_name);

        let result = match adbc_factory
            .read_write_table_provider(table_reference.clone(), dialect)
            .await
        {
            Ok(provider) => Ok(enrich_with_bigquery_metadata_from_weak_pool(
                &self.driver_name,
                &self.pool,
                &table_reference,
                provider,
            )
            .await),
            Err(e) => Err(classify_adbc_error(
                e,
                &self.driver_name,
                dataset,
                |dc, cc, src| DataConnectorError::UnableToGetReadWriteProvider {
                    dataconnector: dc,
                    connector_component: cc,
                    source: src,
                },
            )),
        };

        Some(result)
    }
}

/// Returns whether query federation is enabled based on the `query_federation` parameter.
/// Defaults to `true` (enabled) when the parameter is absent.
fn is_query_federation_enabled(params: &Parameters) -> Result<bool> {
    match params.get("query_federation").expose().ok() {
        None | Some("enabled") => Ok(true),
        Some("disabled") => Ok(false),
        Some(other) => InvalidQueryFederationSnafu {
            value: other.to_string(),
        }
        .fail(),
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "adbc";

/// Returns a new instance of the ADBC connector factory.
#[must_use]
pub fn factory() -> Arc<dyn runtime::dataconnector::DataConnectorFactory> {
    AdbcFactory::new_arc()
}
