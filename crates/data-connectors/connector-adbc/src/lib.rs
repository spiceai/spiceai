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

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Driver as _, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use async_trait::async_trait;
use data_components::{FieldMetadata, metadata_enriched_table_provider};
use data_connector_api::ConnectorContext;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
use futures::TryStreamExt;
use runtime_component::dataset::DatasetSpec;
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use data_connector_api::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime_parameters::{ParameterSpec, Parameters};

mod table_factory;
use table_factory::{AdbcTableFactoryWithPolicy, dialect_for_driver};

/// The factory this connector holds: the managed-driver ADBC factory, with the
/// federation policy attached by its type. Named because it appears in the drop
/// plumbing as well as the connector itself.
type ManagedAdbcTableFactory = AdbcTableFactoryWithPolicy<adbc_driver_manager::ManagedDatabase>;

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
    factory: Option<ManagedAdbcTableFactory>,
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

fn offload_adbc_factory_drop(factory: ManagedAdbcTableFactory, reason: &str) {
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
fn adbc_cleanup_sender() -> &'static std::sync::mpsc::SyncSender<ManagedAdbcTableFactory> {
    static SENDER: OnceLock<std::sync::mpsc::SyncSender<ManagedAdbcTableFactory>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) =
            std::sync::mpsc::sync_channel::<ManagedAdbcTableFactory>(ADBC_CLEANUP_QUEUE_CAPACITY);
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

    fn complete(&mut self, result: &NewDataConnectorResult) {
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
    async fn init_connector(params: ConnectorParams) -> NewDataConnectorResult {
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

        // Identity used to decide whether two ADBC-backed tables can be joined
        // in one federated pushdown: DataFusion's federation optimizer only
        // merges sub-plans whose `compute_context()` strings match, and
        // `SqlTable::compute_context()` derives that string from this pool's
        // `join_push_down()`.
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

        let pool = tokio::time::timeout(std::time::Duration::from_mins(2), init_handle)
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

        let adbc_factory = AdbcTableFactoryWithPolicy::new(
            Arc::clone(&pool),
            federation_enabled,
            &driver_name_owned,
        );

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

pub(crate) async fn enrich_with_bigquery_metadata(
    driver_name: &str,
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    table_reference: &TableReference,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    if !driver_name.eq_ignore_ascii_case("bigquery") {
        return provider;
    }

    match bigquery_schema_metadata(pool, table_reference).await {
        Ok((table_metadata, field_metadata)) => {
            if table_metadata.is_empty() && field_metadata.is_empty() {
                provider
            } else {
                metadata_enriched_table_provider(provider, table_metadata, field_metadata)
            }
        }
        Err(error) => {
            tracing::warn!(
                table = %table_reference,
                error = %error,
                "Failed to query BigQuery schema metadata via ADBC; registering without source metadata"
            );
            provider
        }
    }
}

async fn bigquery_schema_metadata(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    table_reference: &TableReference,
) -> std::result::Result<
    (HashMap<String, String>, FieldMetadata),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let table_name = bigquery_string_literal(table_reference.table());
    let table_options = bigquery_information_schema_table(table_reference, "TABLE_OPTIONS");
    let column_field_paths =
        bigquery_information_schema_table(table_reference, "COLUMN_FIELD_PATHS");
    let columns = bigquery_information_schema_table(table_reference, "COLUMNS");

    let table_sql = format!(
        "SELECT option_value FROM {table_options} WHERE table_name = {table_name} AND option_name = 'description' AND option_value IS NOT NULL AND option_value != ''"
    );
    let comment_sql = format!(
        "SELECT field_path, description FROM {column_field_paths} WHERE table_name = {table_name} AND description IS NOT NULL AND description != ''"
    );
    let column_sql = format!(
        "SELECT column_name, data_type, CASE WHEN is_partitioning_column = 'YES' THEN 'true' ELSE NULL END, CAST(clustering_ordinal_position AS STRING) FROM {columns} WHERE table_name = {table_name}"
    );

    let mut table_metadata = HashMap::new();
    if let Some(comment) = first_string_result(pool, table_sql).await? {
        table_metadata.insert(
            data_components::DESCRIPTION_METADATA_KEY.to_string(),
            comment,
        );
    }

    let mut field_metadata = FieldMetadata::new();
    for row in string_column_results(pool, column_sql, 4).await? {
        let [column_name, source_type, partition, clustering] = row.as_slice() else {
            continue;
        };
        let Some(column_name) = column_name else {
            continue;
        };
        let metadata = field_metadata.entry(column_name.clone()).or_default();
        if let Some(source_type) = source_type {
            metadata.insert(
                data_components::SOURCE_TYPE_METADATA_KEY.to_string(),
                source_type.clone(),
            );
        }
        if partition.as_deref() == Some("true") {
            metadata.insert(
                data_components::PARTITION_METADATA_KEY.to_string(),
                "true".to_string(),
            );
        }
        if let Some(clustering) = clustering {
            metadata.insert(
                data_components::CLUSTERING_METADATA_KEY.to_string(),
                clustering.clone(),
            );
        }
    }

    for row in string_column_results(pool, comment_sql, 2).await? {
        let [field_path, comment] = row.as_slice() else {
            continue;
        };
        let (Some(field_path), Some(comment)) = (field_path, comment) else {
            continue;
        };
        if field_path.contains('.') {
            continue;
        }
        field_metadata
            .entry(field_path.clone())
            .or_default()
            .insert(
                data_components::DESCRIPTION_METADATA_KEY.to_string(),
                comment.clone(),
            );
    }

    Ok((table_metadata, field_metadata))
}

async fn first_string_result(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    sql: String,
) -> std::result::Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Arc::clone(pool).connect().await?;
    let batches: Vec<_> = query_arrow(conn, sql, None).await?.try_collect().await?;
    for batch in &batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let values = batch.column(0);
        for row in 0..batch.num_rows() {
            if let Some(value) = string_value(values, row) {
                return Ok(Some(value.to_string()));
            }
        }
    }
    Ok(None)
}

async fn string_column_results(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    sql: String,
    column_count: usize,
) -> std::result::Result<Vec<Vec<Option<String>>>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Arc::clone(pool).connect().await?;
    let batches: Vec<_> = query_arrow(conn, sql, None).await?.try_collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        if batch.num_columns() < column_count {
            continue;
        }
        for row in 0..batch.num_rows() {
            values.push(
                (0..column_count)
                    .map(|column| string_value(batch.column(column), row).map(ToString::to_string))
                    .collect(),
            );
        }
    }
    Ok(values)
}

fn string_value(array: &ArrayRef, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }

    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row))
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|array| array.value(row))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bigquery_information_schema_table(table_reference: &TableReference, view: &str) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = table_reference.catalog() {
        parts.push(catalog.to_string());
    }
    if let Some(schema) = table_reference.schema() {
        parts.push(schema.to_string());
    }
    parts.push("INFORMATION_SCHEMA".to_string());
    parts.push(view.to_string());

    format!(
        "`{}`",
        parts
            .into_iter()
            .map(|part| part.replace('`', "\\`"))
            .collect::<Vec<_>>()
            .join(".")
    )
}

fn bigquery_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

impl DataConnectorFactory for AdbcFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
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
pub(crate) fn build_db_options(
    uri: &str,
    username: Option<&str>,
    password: Option<&str>,
    driver_options: Option<&str>,
) -> Vec<(OptionDatabase, adbc_core::options::OptionValue)> {
    let mut opts = vec![(OptionDatabase::Uri, uri.into())];
    if let Some(u) = username {
        opts.push((OptionDatabase::Username, u.into()));
    }
    if let Some(p) = password {
        opts.push((OptionDatabase::Password, p.into()));
    }
    if let Some(options_str) = driver_options {
        for pair in options_str.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((key, value)) = pair.split_once('=') {
                let key = key.trim();
                if key.is_empty() {
                    tracing::warn!("Ignoring ADBC driver option with empty key");
                    continue;
                }
                let key = if key.starts_with("adbc.") {
                    key.to_string()
                } else {
                    format!("adbc.{key}")
                };
                opts.push((OptionDatabase::Other(key), value.trim().into()));
            } else {
                tracing::warn!("Ignoring malformed ADBC driver option (expected 'key=value')");
            }
        }
    }
    opts
}

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

fn infer_bigquery_namespace_from_dataset(dataset: &DatasetSpec) -> ConnectionNamespace {
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
fn build_join_context(
    uri: &str,
    username: Option<&str>,
    catalog: Option<&str>,
    schema: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(uri.as_bytes());
    hasher.update(b"\0");
    if let Some(u) = username {
        hasher.update(u.as_bytes());
    }
    hasher.update(b"\0");
    if let Some(c) = catalog {
        hasher.update(c.as_bytes());
    }
    hasher.update(b"\0");
    if let Some(s) = schema {
        hasher.update(s.as_bytes());
    }
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        let _ = write!(hash, "{b:02x}");
        hash
    })
}

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
    dataset: &DatasetSpec,
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
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
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
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
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
pub fn factory() -> std::sync::Arc<dyn DataConnectorFactory> {
    AdbcFactory::new_arc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::component::dataset::Dataset;
    use secrecy::SecretString;

    #[test]
    fn test_factory_as_any() {
        let factory = AdbcFactory::new();
        assert!(factory.as_any().is::<AdbcFactory>());
    }

    #[test]
    fn test_factory_prefix() {
        let factory = AdbcFactory::new();
        assert_eq!(factory.prefix(), "adbc");
    }

    #[test]
    fn test_factory_parameters() {
        let factory = AdbcFactory::new();
        let params = factory.parameters();

        let param_names: Vec<&str> = params.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"driver"));
        assert!(param_names.contains(&"driver_path"));
        assert!(param_names.contains(&"uri"));
        assert!(param_names.contains(&"username"));
        assert!(param_names.contains(&"password"));
        assert!(param_names.contains(&"driver_options"));
        assert!(param_names.contains(&"catalog"));
        assert!(param_names.contains(&"schema"));
        assert!(param_names.contains(&"connection_pool_size"));
        assert!(param_names.contains(&"connection_pool_min_idle"));
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingAdbcDriver;
        assert_eq!(err.to_string(), "Missing required parameter: adbc_driver");

        let _boxed: Box<dyn std::error::Error> = Box::new(err);
    }

    #[test]
    fn test_factory_new_arc() {
        let factory = AdbcFactory::new_arc();
        assert_eq!(factory.prefix(), "adbc");
    }

    #[test]
    fn test_debug_impl() {
        let factory = AdbcFactory::new();
        let debug_str = format!("{factory:?}");
        assert!(debug_str.contains("AdbcFactory"));
    }

    #[test]
    fn test_build_db_options_uri_only() {
        let opts = build_db_options("file:test.db", None, None, None);
        assert_eq!(opts.len(), 1);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "file:test.db")
        );
    }

    #[test]
    fn test_build_db_options_with_username_password() {
        let opts = build_db_options("postgres://host/db", Some("admin"), Some("secret"), None);
        assert_eq!(opts.len(), 3);

        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "postgres://host/db")
        );

        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "admin"));

        assert_eq!(opts[2].0, OptionDatabase::Password);
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "secret"));
    }

    #[test]
    fn test_build_db_options_username_only() {
        let opts = build_db_options("sqlite:test.db", Some("user"), None, None);
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "user"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_unprefixed() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("snowflake.sql.db=MY_DB;snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "MY_DB"));
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "PUBLIC"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_prefixed() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("adbc.snowflake.sql.db=MY_DB;adbc.snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
    }

    #[test]
    fn test_build_db_options_driver_options_trailing_semicolon() {
        let opts = build_db_options("uri://db", None, None, Some("key=value;"));
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.key".to_string()));
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "value"));
    }

    #[test]
    fn test_build_db_options_driver_options_malformed_ignored() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("good=val;bad_no_equals;another=ok"),
        );
        assert_eq!(opts.len(), 3); // uri + good + another (bad_no_equals skipped)
    }

    #[test]
    fn test_build_db_options_driver_options_empty_key_ignored() {
        let opts = build_db_options("uri://db", None, None, Some("=value;good=ok"));
        assert_eq!(opts.len(), 2); // uri + good (empty key skipped)
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.good".to_string()));
    }

    #[test]
    fn test_build_conn_options_none_when_empty() {
        let opts = build_conn_options(None, None);
        assert!(opts.is_none());
    }

    #[test]
    fn test_build_conn_options_both() {
        let opts =
            build_conn_options(Some("my_catalog"), Some("my_schema")).expect("should have options");
        assert_eq!(opts.len(), 2);
        assert_eq!(
            opts.get("adbc.connection.catalog"),
            Some(&"my_catalog".to_string())
        );
        assert_eq!(
            opts.get("adbc.connection.db_schema"),
            Some(&"my_schema".to_string())
        );
    }

    #[test]
    fn test_build_conn_options_catalog_only() {
        let opts = build_conn_options(Some("cat"), None).expect("should have options");
        assert_eq!(opts.len(), 1);
        assert_eq!(
            opts.get("adbc.connection.catalog"),
            Some(&"cat".to_string())
        );
    }

    #[test]
    fn test_is_auth_or_permission_error_bigquery_invalid_grant() {
        assert!(is_auth_or_permission_error(
            r#"Unknown: [BigQuery] Get "https://bigquery.googleapis.com:443/bigquery/v2/projects/my-project/datasets/my_dataset/tables/my_table?alt=json&prettyPrint=false": auth: "invalid_grant" "reauth related error (invalid_rapt)" "https://support.google.com/a/answer/9368756""#
        ));
    }

    #[test]
    fn test_is_auth_or_permission_error_permission_denied() {
        assert!(is_auth_or_permission_error(
            "Permission denied on resource project my-project"
        ));
    }

    #[test]
    fn test_is_auth_or_permission_error_access_denied() {
        assert!(is_auth_or_permission_error("Access Denied"));
    }

    #[test]
    fn test_is_auth_or_permission_error_unauthenticated() {
        assert!(is_auth_or_permission_error("Request is unauthenticated"));
    }

    #[test]
    fn test_is_auth_or_permission_error_forbidden() {
        assert!(is_auth_or_permission_error("403 Forbidden"));
    }

    #[test]
    fn test_is_auth_or_permission_error_not_auth() {
        assert!(!is_auth_or_permission_error("Table not found"));
        assert!(!is_auth_or_permission_error("Connection reset by peer"));
        assert!(!is_auth_or_permission_error("timeout"));
    }

    async fn test_dataset(from: &str, name: &str) -> Dataset {
        use app::AppBuilder;
        use runtime::component::dataset::builder::DatasetBuilder;

        let app = AppBuilder::new("test_app").build();
        let rt = runtime::Runtime::builder().build().await;
        DatasetBuilder::try_new(from.to_string(), name)
            .expect("valid builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("valid dataset")
    }

    #[tokio::test]
    async fn test_classify_adbc_error_bigquery_auth() {
        let dataset = test_dataset("bigquery:my_project.my_dataset.my_table", "my_table").await;
        let error: Box<dyn std::error::Error + Send + Sync> =
            "invalid_grant: reauth related error".into();
        let result = classify_adbc_error(error, "bigquery", &dataset, |dc, cc, src| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: dc,
                connector_component: cc,
                source: src,
            }
        });
        let msg = result.to_string();
        assert!(
            msg.contains("BigQuery credentials"),
            "Expected BigQuery-specific hint, got: {msg}"
        );
        assert!(
            msg.contains("gcloud auth application-default login"),
            "Expected gcloud re-auth guidance, got: {msg}"
        );
        assert!(
            msg.contains("service account"),
            "Expected service-account guidance, got: {msg}"
        );
        assert!(
            msg.contains("invalid_grant"),
            "Expected original error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_classify_adbc_error_generic_auth() {
        let dataset = test_dataset("adbc:snowflake://host/db", "my_table").await;
        let error: Box<dyn std::error::Error + Send + Sync> = "403 Forbidden".into();
        let result = classify_adbc_error(error, "snowflake", &dataset, |dc, cc, src| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: dc,
                connector_component: cc,
                source: src,
            }
        });
        let msg = result.to_string();
        assert!(
            msg.contains("credentials are valid"),
            "Expected generic auth hint, got: {msg}"
        );
        assert!(
            !msg.contains("BigQuery"),
            "Should not mention BigQuery for non-BigQuery driver, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_classify_adbc_error_non_auth_uses_fallback() {
        let dataset = test_dataset("adbc:postgres://host/db", "my_table").await;
        let error: Box<dyn std::error::Error + Send + Sync> = "Connection refused".into();
        let result = classify_adbc_error(error, "postgres", &dataset, |dc, cc, src| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: dc,
                connector_component: cc,
                source: src,
            }
        });
        let msg = result.to_string();
        assert!(
            msg.contains("Connection refused"),
            "Expected original error in fallback, got: {msg}"
        );
        assert!(
            !msg.contains("credentials"),
            "Should not mention credentials for non-auth error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_cache_key_identical_configs_match() {
        let dataset_a = test_dataset("adbc:bigquery/my_project.dataset.table_a", "table_a").await;
        let dataset_b = test_dataset("adbc:bigquery/my_project.dataset.table_b", "table_b").await;

        let make_params = |dataset: &Dataset| {
            let parameters = Parameters::new(
                vec![
                    ("driver".to_string(), SecretString::from("bigquery")),
                    (
                        "uri".to_string(),
                        SecretString::from("grpc://bigquery.googleapis.com"),
                    ),
                    ("catalog".to_string(), SecretString::from("my_project")),
                ],
                "adbc",
                PARAMETERS,
            );

            ConnectorParams {
                parameters,
                unsupported_type_action: None,
                component: ConnectorComponent::from(dataset),
                io_runtime: tokio::runtime::Handle::current(),
            }
        };

        let params_a = make_params(&dataset_a);
        let params_b = make_params(&dataset_b);

        // Same config params → same cache key, despite different datasets
        assert_eq!(
            compute_adbc_cache_key(&params_a),
            compute_adbc_cache_key(&params_b)
        );
    }

    #[tokio::test]
    async fn test_cache_key_different_configs_differ() {
        let dataset = test_dataset("adbc:bigquery/my_project.dataset.table_a", "table_a").await;

        let make_params = |uri: &str| {
            let parameters = Parameters::new(
                vec![
                    ("driver".to_string(), SecretString::from("bigquery")),
                    ("uri".to_string(), SecretString::from(uri)),
                ],
                "adbc",
                PARAMETERS,
            );

            ConnectorParams {
                parameters,
                unsupported_type_action: None,
                component: ConnectorComponent::from(&dataset),
                io_runtime: tokio::runtime::Handle::current(),
            }
        };

        let params_a = make_params("grpc://bigquery.googleapis.com");
        let params_b = make_params("grpc://other-endpoint.example.com");

        // Different URIs → different cache keys
        assert_ne!(
            compute_adbc_cache_key(&params_a),
            compute_adbc_cache_key(&params_b)
        );
    }

    #[tokio::test]
    async fn test_resolve_connection_namespace_bigquery_infers_from_hyphenated_project_path() {
        let dataset = test_dataset("adbc:my-project.my_dataset.my_table", "my_table").await;

        let parameters = Parameters::new(
            vec![
                ("driver".to_string(), SecretString::from("bigquery")),
                (
                    "uri".to_string(),
                    SecretString::from("bigquery:///my-project"),
                ),
            ],
            "adbc",
            PARAMETERS,
        );

        let namespace = resolve_connection_namespace(
            "bigquery",
            &ConnectorComponent::from(&dataset),
            &parameters,
        )
        .expect("bigquery namespace should resolve");

        assert_eq!(namespace.catalog.as_deref(), Some("my-project"));
        assert_eq!(namespace.schema.as_deref(), Some("my_dataset"));
    }

    #[tokio::test]
    async fn test_resolve_connection_namespace_bigquery_preserves_explicit_values() {
        let dataset = test_dataset("adbc:my-project.path_dataset.my_table", "my_table").await;

        let parameters = Parameters::new(
            vec![
                ("driver".to_string(), SecretString::from("bigquery")),
                (
                    "uri".to_string(),
                    SecretString::from("bigquery:///my-project"),
                ),
                (
                    "catalog".to_string(),
                    SecretString::from("configured-project"),
                ),
                (
                    "schema".to_string(),
                    SecretString::from("configured_dataset"),
                ),
            ],
            "adbc",
            PARAMETERS,
        );

        let namespace = resolve_connection_namespace(
            "bigquery",
            &ConnectorComponent::from(&dataset),
            &parameters,
        )
        .expect("explicit namespace should be preserved");

        assert_eq!(namespace.catalog.as_deref(), Some("configured-project"));
        assert_eq!(namespace.schema.as_deref(), Some("configured_dataset"));
    }

    #[tokio::test]
    async fn test_resolve_connection_namespace_rejects_empty_schema() {
        let dataset = test_dataset("adbc:my_dataset.my_table", "my_table").await;

        let parameters = Parameters::new(
            vec![
                ("driver".to_string(), SecretString::from("bigquery")),
                (
                    "uri".to_string(),
                    SecretString::from("bigquery:///my-project"),
                ),
                ("schema".to_string(), SecretString::from("")),
            ],
            "adbc",
            PARAMETERS,
        );

        let err = resolve_connection_namespace(
            "bigquery",
            &ConnectorComponent::from(&dataset),
            &parameters,
        )
        .expect_err("empty schema should be rejected");

        assert_eq!(
            err.to_string(),
            "Invalid value for parameter 'adbc_schema': expected a non-empty string"
        );
    }

    #[tokio::test]
    async fn test_cache_key_bigquery_inferred_schema_differs() {
        let dataset_a = test_dataset("adbc:my_project.dataset_a.table_a", "table_a").await;
        let dataset_b = test_dataset("adbc:my_project.dataset_b.table_b", "table_b").await;

        let make_params = |dataset: &Dataset| {
            let parameters = Parameters::new(
                vec![
                    ("driver".to_string(), SecretString::from("bigquery")),
                    (
                        "uri".to_string(),
                        SecretString::from("bigquery:///my_project"),
                    ),
                ],
                "adbc",
                PARAMETERS,
            );

            ConnectorParams {
                parameters,
                unsupported_type_action: None,
                component: ConnectorComponent::from(dataset),
                io_runtime: tokio::runtime::Handle::current(),
            }
        };

        let params_a = make_params(&dataset_a);
        let params_b = make_params(&dataset_b);

        assert_ne!(
            compute_adbc_cache_key(&params_a),
            compute_adbc_cache_key(&params_b)
        );
    }

    #[test]
    fn test_bigquery_information_schema_table_uses_comment_source_path() {
        let table_reference = TableReference::full("project-a", "analytics", "customers");

        assert_eq!(
            bigquery_information_schema_table(&table_reference, "COLUMN_FIELD_PATHS"),
            "`project-a.analytics.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`"
        );
    }

    #[test]
    fn test_bigquery_string_literal_escapes_comment_query_value() {
        assert_eq!(
            bigquery_string_literal("customer's\\table"),
            "'customer\\'s\\\\table'"
        );
    }

    #[derive(Debug)]
    struct TestConnector;

    #[async_trait]
    impl DataConnector for TestConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn ConnectorContext,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            unreachable!("test connector is not used to read data")
        }
    }

    #[test]
    fn test_connector_cache_state_prunes_expired_ready_entries() {
        let connector: Arc<dyn DataConnector> = Arc::new(TestConnector);
        let state = ConnectorCacheState::Ready(Arc::downgrade(&connector));

        assert!(state.should_retain());

        drop(connector);

        assert!(!state.should_retain());
    }

    #[test]
    fn test_connector_cache_entry_retains_inflight_initialization() {
        let entry = ConnectorCacheEntry::new();
        *entry.state.lock() = ConnectorCacheState::Initializing;

        assert!(entry.should_retain());
    }

    #[tokio::test]
    async fn test_connector_initialization_guard_resets_state_on_drop() {
        let entry = Arc::new(ConnectorCacheEntry::new());
        *entry.state.lock() = ConnectorCacheState::Initializing;

        let notified = entry.notify.notified();
        let guard = ConnectorInitializationGuard::new(Arc::clone(&entry));
        drop(guard);

        notified.await;

        assert!(matches!(&*entry.state.lock(), ConnectorCacheState::Vacant));
    }

    fn make_params(pairs: Vec<(&str, &str)>) -> Parameters {
        Parameters::new(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
                .collect(),
            "adbc",
            PARAMETERS,
        )
    }

    #[test]
    fn test_query_federation_enabled() {
        let params = make_params(vec![("query_federation", "enabled")]);
        assert!(is_query_federation_enabled(&params).expect("to parse"));
    }

    #[test]
    fn test_query_federation_disabled() {
        let params = make_params(vec![("query_federation", "disabled")]);
        assert!(!is_query_federation_enabled(&params).expect("to parse"));
    }

    #[test]
    fn test_query_federation_missing_defaults_enabled() {
        let params = make_params(vec![]);
        assert!(is_query_federation_enabled(&params).expect("to parse"));
    }

    #[test]
    fn test_query_federation_invalid_value() {
        let params = make_params(vec![("query_federation", "invalid")]);
        is_query_federation_enabled(&params).expect_err("should error on invalid value");
    }

    #[test]
    fn test_build_join_context_no_secrets_in_output() {
        let ctx = build_join_context(
            "bigquery:///project?DatasetId=tpch_sf1&token=SECRET123",
            Some("admin"),
            Some("my_catalog"),
            Some("my_schema"),
        );
        // Hash output must not contain any raw URI or credential fragments
        assert!(
            !ctx.contains("SECRET123"),
            "context must not contain secrets from URI"
        );
        assert!(
            !ctx.contains("bigquery:///"),
            "context must not contain raw URI"
        );
        assert!(
            !ctx.contains("admin"),
            "context must not contain raw username"
        );
        assert!(
            !ctx.contains("my_catalog"),
            "context must not contain raw catalog"
        );
        // Must be a fixed-length hex string (SHA-256 = 64 hex chars)
        assert_eq!(
            ctx.len(),
            64,
            "context should be a 64-char SHA-256 hex digest"
        );
        assert!(
            ctx.chars().all(|c| c.is_ascii_hexdigit()),
            "context should be hex only"
        );
    }

    #[test]
    fn test_build_join_context_deterministic() {
        let ctx1 = build_join_context("postgresql://host:5432/db", Some("user"), None, None);
        let ctx2 = build_join_context("postgresql://host:5432/db", Some("user"), None, None);
        assert_eq!(ctx1, ctx2, "same inputs must produce the same hash");
    }

    #[test]
    fn test_build_join_context_differs_by_username() {
        let ctx_a = build_join_context("postgresql://host/db", Some("alice"), None, None);
        let ctx_b = build_join_context("postgresql://host/db", Some("bob"), None, None);
        assert_ne!(
            ctx_a, ctx_b,
            "different usernames must produce different hashes"
        );
    }

    #[test]
    fn test_build_join_context_differs_by_uri() {
        let ctx_a = build_join_context("bigquery:///project-a?DatasetId=ds1", None, None, None);
        let ctx_b = build_join_context("bigquery:///project-b?DatasetId=ds1", None, None, None);
        assert_ne!(ctx_a, ctx_b, "different URIs must produce different hashes");
    }
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_adbc as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_adbc_connector,
    ADBC_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    AdbcFactory
);

#[cfg(test)]
mod function_support_tests {
    //! Regression tests for the federation deny-list on the ADBC table
    //! factory: [`AdbcTableFactoryWithPolicy`] must install the Spice function
    //! deny-list and honor the `query_federation` setting. Without the
    //! deny-list, Spice-only UDFs like `json_get_str` are unparsed into the
    //! SQL sent to the remote database (e.g. `BigQuery`), which cannot
    //! evaluate them.

    use std::collections::HashSet;
    use std::sync::Arc;

    use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status};
    use adbc_core::options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    };
    use adbc_core::{Connection, Database, Optionable, PartitionedResult, Statement};
    use arrow::array::{RecordBatch, RecordBatchReader};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNode;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::{TableProvider, provider_as_source};
    use datafusion::logical_expr::{
        ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, TableSource, Volatility,
        builder::LogicalTableSource, create_udf, expr::ScalarFunction,
    };
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::prelude::{col, lit};
    use datafusion::sql::TableReference;
    use datafusion_federation::sql::federation_analyzer_rule;
    use datafusion_federation::{FederatedTableProviderAdaptor, FederationAnalyzerForLogicalPlan};
    use datafusion_table_providers::sql::db_connection_pool::adbcpool::ADBCPool;

    use super::{AdbcTableFactoryWithPolicy, dialect_for_driver};

    fn not_implemented(what: &str) -> AdbcError {
        AdbcError::with_message_and_status(
            format!("{what} is not implemented by the stub ADBC driver"),
            Status::NotImplemented,
        )
    }

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ])
    }

    /// Minimal in-process ADBC driver: enough for the connection pool to hand
    /// out connections and for `AdbcTableFactory::table_provider` to resolve
    /// the table schema. Everything else reports `NotImplemented`.
    struct StubDatabase;
    struct StubConnection;
    // Clonable because cancelling a running query needs a second handle to the
    // same statement, which the ADBC table factory requires of every driver.
    #[derive(Clone)]
    struct StubStatement;

    impl Optionable for StubDatabase {
        type Option = OptionDatabase;
        fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
            Err(not_implemented("set_option"))
        }
        fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
            Err(not_implemented("get_option_string"))
        }
        fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
            Err(not_implemented("get_option_bytes"))
        }
        fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
            Err(not_implemented("get_option_int"))
        }
        fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
            Err(not_implemented("get_option_double"))
        }
    }

    impl Database for StubDatabase {
        type ConnectionType = StubConnection;

        fn new_connection(&self) -> AdbcResult<StubConnection> {
            Ok(StubConnection)
        }

        fn new_connection_with_opts(
            &self,
            _opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
        ) -> AdbcResult<StubConnection> {
            Ok(StubConnection)
        }
    }

    impl Optionable for StubConnection {
        type Option = OptionConnection;
        fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
            Err(not_implemented("set_option"))
        }
        fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
            Err(not_implemented("get_option_string"))
        }
        fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
            Err(not_implemented("get_option_bytes"))
        }
        fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
            Err(not_implemented("get_option_int"))
        }
        fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
            Err(not_implemented("get_option_double"))
        }
    }

    impl Connection for StubConnection {
        type StatementType = StubStatement;

        fn new_statement(&mut self) -> AdbcResult<StubStatement> {
            Ok(StubStatement)
        }

        fn cancel(&mut self) -> AdbcResult<()> {
            Err(not_implemented("cancel"))
        }

        fn get_info(
            &self,
            _codes: Option<HashSet<InfoCode>>,
        ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("get_info"))
        }

        fn get_objects(
            &self,
            _depth: ObjectDepth,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: Option<&str>,
            _table_type: Option<Vec<&str>>,
            _column_name: Option<&str>,
        ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("get_objects"))
        }

        fn get_table_schema(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: &str,
        ) -> AdbcResult<Schema> {
            Ok(table_schema())
        }

        fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("get_table_types"))
        }

        fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("get_statistic_names"))
        }

        fn get_statistics(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: Option<&str>,
            _approximate: bool,
        ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("get_statistics"))
        }

        fn commit(&mut self) -> AdbcResult<()> {
            Err(not_implemented("commit"))
        }

        fn rollback(&mut self) -> AdbcResult<()> {
            Err(not_implemented("rollback"))
        }

        fn read_partition(
            &self,
            _partition: impl AsRef<[u8]>,
        ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("read_partition"))
        }
    }

    impl Optionable for StubStatement {
        type Option = OptionStatement;
        fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
            Err(not_implemented("set_option"))
        }
        fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
            Err(not_implemented("get_option_string"))
        }
        fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
            Err(not_implemented("get_option_bytes"))
        }
        fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
            Err(not_implemented("get_option_int"))
        }
        fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
            Err(not_implemented("get_option_double"))
        }
    }

    // The stub never runs a query, so a handle to it cancels nothing; the trait
    // is implemented because the ADBC table factory now requires statements to
    // say how a cancellation handle is obtained.
    impl datafusion_table_providers::sql::db_connection_pool::dbconnection::adbcconn::StatementCancelHandle
        for StubStatement
    {
        fn cancel(&mut self) -> AdbcResult<()> {
            Err(not_implemented("cancel"))
        }
    }

    impl datafusion_table_providers::sql::db_connection_pool::dbconnection::adbcconn::CancellableStatement
        for StubStatement
    {
        type CancelHandle = Self;

        fn cancel_handle(&self) -> Self::CancelHandle {
            self.clone()
        }
    }

    impl Statement for StubStatement {
        fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
            Err(not_implemented("bind"))
        }

        fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
            Err(not_implemented("bind_stream"))
        }

        fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
            Err(not_implemented("execute"))
        }

        fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
            Err(not_implemented("execute_update"))
        }

        fn execute_schema(&mut self) -> AdbcResult<Schema> {
            Err(not_implemented("execute_schema"))
        }

        fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
            Err(not_implemented("execute_partitions"))
        }

        fn get_parameter_schema(&self) -> AdbcResult<Schema> {
            Err(not_implemented("get_parameter_schema"))
        }

        fn prepare(&mut self) -> AdbcResult<()> {
            Err(not_implemented("prepare"))
        }

        fn set_sql_query(&mut self, _query: impl AsRef<str>) -> AdbcResult<()> {
            Err(not_implemented("set_sql_query"))
        }

        fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> AdbcResult<()> {
            Err(not_implemented("set_substrait_plan"))
        }

        fn cancel(&mut self) -> AdbcResult<()> {
            Err(not_implemented("cancel"))
        }
    }

    /// A projection over a scan of the stub table, so a plan can carry an
    /// arbitrary expression through the federation `can_execute_plan` check.
    fn scan_project(expr: Expr) -> LogicalPlan {
        let source =
            Arc::new(LogicalTableSource::new(Arc::new(table_schema()))) as Arc<dyn TableSource>;
        LogicalPlanBuilder::scan("t", source, None)
            .expect("scan the stub table")
            .project(vec![expr])
            .expect("project the expression")
            .build()
            .expect("build the plan")
    }

    fn udf_expr(name: &str) -> Expr {
        let udf = Arc::new(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ));
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![col("val")]))
    }

    /// A provider over the stub driver, built the way the connector builds one
    /// for `driver_name` — same policy, same dialect.
    async fn stub_table_provider(
        federation_enabled: bool,
        driver_name: &str,
    ) -> Arc<dyn TableProvider> {
        let pool = Arc::new(ADBCPool::new(StubDatabase, None).expect("build the stub ADBC pool"));
        AdbcTableFactoryWithPolicy::new(pool, federation_enabled, driver_name)
            .table_provider(TableReference::bare("t"), dialect_for_driver(driver_name))
            .await
            .expect("build the ADBC table provider")
    }

    #[tokio::test]
    async fn table_factory_denies_spice_functions_from_federation() {
        let provider = stub_table_provider(true, "sqlite").await;
        let adaptor = (provider.as_ref() as &dyn std::any::Any)
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .expect("a federation-enabled factory must produce a federated provider");
        let federation = adaptor.source.federation_provider();

        let denied = scan_project(udf_expr("json_get_str"));
        assert!(
            matches!(
                federation.analyzer(&denied),
                Some(FederationAnalyzerForLogicalPlan::Unable)
            ),
            "a plan using a Spice-only UDF must not federate to the remote ADBC database"
        );

        let allowed = scan_project(col("id"));
        assert!(
            matches!(
                federation.analyzer(&allowed),
                Some(FederationAnalyzerForLogicalPlan::With(_))
            ),
            "a plan without Spice-only UDFs must still federate"
        );
    }

    /// A `json_get_*` call over the stub table's `val` column.
    fn json_call(name: &str, path: Vec<Expr>) -> Expr {
        let mut args = vec![col("val")];
        args.extend(path);
        let udf = Arc::new(create_udf(
            name,
            vec![DataType::Utf8; args.len()],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ));
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
    }

    /// Whether a plan carrying `expr` federates to a provider built for
    /// `driver_name`.
    async fn federates(driver_name: &str, expr: Expr) -> bool {
        let provider = stub_table_provider(true, driver_name).await;
        let adaptor = (provider.as_ref() as &dyn std::any::Any)
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .expect("a federation-enabled factory must produce a federated provider");
        matches!(
            adaptor
                .source
                .federation_provider()
                .analyzer(&scan_project(expr)),
            Some(FederationAnalyzerForLogicalPlan::With(_))
        )
    }

    #[tokio::test]
    async fn bigquery_federates_the_json_calls_its_dialect_can_translate() {
        assert!(
            federates("bigquery", json_call("json_get_int", vec![lit("a")])).await,
            "the BigQuery dialect rewrites this into JSON_VALUE, so it must push down"
        );
        assert!(
            federates("bigquery", json_call("json_get_str", vec![lit("a")])).await,
            "the BigQuery dialect guards JSON_VALUE with JSON_QUERY for the string \
             form, so it must push down too"
        );
        assert!(
            federates("bigquery", json_call("json_get_bool", vec![lit("a")])).await,
            "the BigQuery dialect compares JSON_VALUE's rendering for the bool form"
        );
        assert!(
            federates("bigquery", json_call("json_get_float", vec![lit("a")])).await,
            "BigQuery's SAFE_CAST saturates exactly as Rust's f64::FromStr does, so the \
             float form is translatable too"
        );
        assert!(
            federates("bigquery", json_call("json_length", vec![lit("a")])).await,
            "an array's and an object's counts each have a BigQuery equivalent"
        );
        assert!(
            federates("bigquery", json_call("json_object_keys", vec![lit("a")])).await,
            "JSON_KEYS at depth 1 is the object's own keys, which is what this returns"
        );
        assert!(
            federates(
                "bigquery",
                json_call("json_get_int", vec![lit("a"), lit(0_i64)])
            )
            .await,
            "a literal path of any length is translatable"
        );
    }

    #[tokio::test]
    async fn bigquery_refuses_the_json_call_shapes_its_dialect_cannot_translate() {
        // BigQuery's JSON path argument must be a constant, so a per-row path
        // has no translation. Federating it would unparse `json_get_int`
        // verbatim into BigQuery SQL — issue #10703 all over again — so the
        // plan must stay local instead.
        assert!(
            !federates("bigquery", json_call("json_get_int", vec![col("val")])).await,
            "a per-row path has no BigQuery translation, so the plan must not federate"
        );
        assert!(
            !federates(
                "bigquery",
                json_call("json_get_int", vec![lit("a"), col("val")])
            )
            .await,
            "one non-literal element is enough to make the whole path untranslatable"
        );
        assert!(
            !federates("bigquery", json_call("json_get_int", vec![lit(r#"a"b"#)])).await,
            "a key the SQL-literal and JSON-path layers quote differently is left local \
             rather than escaped across both and silently turned into another path"
        );
        // The check is per call, not per function: every translated name owes
        // the same refusals, or the newest handler is the one that federates a
        // shape it cannot render.
        assert!(
            !federates("bigquery", json_call("json_get_str", vec![col("val")])).await,
            "a per-row path has no BigQuery translation for the string form either"
        );
        assert!(
            !federates("bigquery", json_call("json_get_str", vec![lit(r#"a"b"#)])).await,
            "an unquotable key is left local for the string form too"
        );
    }

    #[tokio::test]
    async fn bigquery_still_denies_the_json_functions_it_has_no_translation_for() {
        for name in [
            // Returns a union of every JSON scalar type, which has no SQL type
            // to unparse into.
            "json_get",
            // Returns the matched node's own bytes; JSON_QUERY re-renders it.
            "json_as_text",
            // A JSON `null` and a missing key are indistinguishable in
            // BigQuery, and json_contains tells them apart.
            "json_contains",
        ] {
            assert!(
                !federates("bigquery", json_call(name, vec![lit("a")])).await,
                "{name} has no BigQuery translation and must stay denied"
            );
        }
    }

    #[tokio::test]
    async fn a_driver_without_a_spice_dialect_denies_every_json_function() {
        // The carve-out is BigQuery's, not ADBC's: another driver gets the
        // plain deny-list and evaluates these locally.
        for name in [
            "json_get_int",
            "json_get_float",
            "json_get_str",
            "json_get_bool",
        ] {
            assert!(
                !federates("sqlite", json_call(name, vec![lit("a")])).await,
                "{name} must not push down to a driver whose dialect cannot rewrite it"
            );
        }
    }

    /// `SELECT id FROM t WHERE <predicate>` over the stub table, run through
    /// the federation analyzer. The projection keeps the root off the leaf,
    /// where the analyzer deliberately leaves a bare scan for the provider to
    /// handle itself.
    async fn federated_plan(predicate: Expr) -> LogicalPlan {
        let provider = stub_table_provider(true, "bigquery").await;
        let plan = LogicalPlanBuilder::scan("t", provider_as_source(provider), None)
            .expect("scan the stub table")
            .filter(predicate)
            .expect("filter on the JSON value")
            .project(vec![col("id")])
            .expect("project")
            .build()
            .expect("build the plan");

        federation_analyzer_rule()
            .analyze(plan, &ConfigOptions::default())
            .expect("federate what can be federated")
    }

    #[tokio::test]
    async fn a_translatable_predicate_federates_as_one_node_with_the_predicate_inside_it() {
        let analyzed =
            federated_plan(json_call("json_get_int", vec![lit("a")]).eq(lit(1_i64))).await;

        assert!(
            is_federated_node(&analyzed),
            "the predicate translates, so the whole plan is one remote statement: {analyzed}"
        );
        assert_eq!(
            federated_node_count(&analyzed),
            1,
            "one remote statement, not one per side: {analyzed}"
        );
    }

    #[tokio::test]
    async fn an_untranslatable_predicate_is_left_above_the_federated_scan() {
        let analyzed =
            federated_plan(json_call("json_get_int", vec![col("val")]).eq(lit(1_i64))).await;

        assert!(
            !is_federated_node(&analyzed),
            "a per-row path has no translation, so the plan cannot federate whole: {analyzed}"
        );
        assert!(
            analyzed
                .apply(|node| {
                    Ok(if matches!(node, LogicalPlan::Filter(_)) {
                        datafusion::common::tree_node::TreeNodeRecursion::Stop
                    } else {
                        datafusion::common::tree_node::TreeNodeRecursion::Continue
                    })
                })
                .map(|recursion| {
                    recursion == datafusion::common::tree_node::TreeNodeRecursion::Stop
                })
                .expect("walk the plan"),
            "the predicate must be left for the local engine to evaluate: {analyzed}"
        );
    }

    fn is_federated_node(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Extension(extension) if extension.node.name() == "Federated")
    }

    fn federated_node_count(plan: &LogicalPlan) -> usize {
        let mut count = 0;
        plan.apply(|node| {
            if is_federated_node(node) {
                count += 1;
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .expect("walk the plan");
        count
    }

    #[tokio::test]
    async fn table_factory_disables_federation_when_configured() {
        let provider = stub_table_provider(false, "sqlite").await;
        assert!(
            (provider.as_ref() as &dyn std::any::Any)
                .downcast_ref::<FederatedTableProviderAdaptor>()
                .is_none(),
            "`query_federation: disabled` must produce a provider that never federates"
        );
    }
}
