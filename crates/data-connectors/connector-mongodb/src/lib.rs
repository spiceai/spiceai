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

//! `MongoDB` data connector for Spice.ai runtime.
//!
//! This crate provides the `MongoDB` connector implementation, allowing
//! Spice.ai to connect to `MongoDB` databases as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

mod changes;

use async_trait::async_trait;
use data_components::inferred_schema::{InferredIndex, InferredSchema, InferredSortColumn};
use datafusion::datasource::TableProvider;
use datafusion_table_providers::mongodb::{
    Error as MongoDBError, MongoDBTableFactory, connection_pool::MongoDBConnectionPool,
};
use mongodb::bson::{Bson, Document, doc};
use runtime::component::dataset::Dataset;
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use runtime::federated_table::FederatedTable;
use runtime::parameters::{ParameterSpec, Parameters};
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// `MongoDB` data connector.
pub struct MongoDB {
    mongodb_factory: MongoDBTableFactory,
    pool: Arc<MongoDBConnectionPool>,
    params: Parameters,
}

impl std::fmt::Debug for MongoDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDB").finish_non_exhaustive()
    }
}

/// Factory for creating `MongoDB` connector instances.
#[derive(Default, Copy, Clone)]
pub struct MongoDBFactory {}

impl MongoDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const DEFAULT_CONNECTION_POOL_MIN: usize = 1;
const DEFAULT_CONNECTION_POOL_MIN_STR: &str = "1";
const DEFAULT_CONNECTION_POOL_MAX: usize = 5;
const DEFAULT_CONNECTION_POOL_MAX_STR: &str = "5";

/// Time bound for best-effort `MongoDB` catalog enrichment (secondary indexes,
/// sort order, sizing). The constant `_id` primary key is inferred regardless;
/// this only caps the optional extras so a slow or unavailable catalog can never
/// block dataset load/readiness (which would otherwise stall `refresh_mode: changes`).
const MONGODB_CATALOG_TIMEOUT: Duration = Duration::from_secs(10);

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description("Full MongoDB connection URI in standard format (e.g., mongodb://user:pass@host:port/dbname). If provided, this overrides individual host, port, user, pass, and db parameters. See: https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-formats")
        .secret(),
    ParameterSpec::component("user")
        .description("Username for MongoDB authentication. Must be used together with 'pass' unless 'connection_string' is provided.")
        .secret(),
    ParameterSpec::component("pass")
        .description("Password for MongoDB authentication. Must be used together with 'user' unless 'connection_string' is provided.")
        .secret(),
    ParameterSpec::component("host")
        .description("Hostname or IP address of the MongoDB server. Defaults to 'localhost' if not specified."),
    ParameterSpec::component("port")
        .description("Port number the MongoDB server is listening on. Defaults to '27017'."),
    ParameterSpec::component("db")
        .description("Database name to connect to. Defaults to 'default' if not specified."),
    ParameterSpec::component("sslmode")
        .description("TLS/SSL mode for the connection. Supported values: 'disabled', 'required', 'preferred'. Defaults to 'required'. 'preferred' allows invalid certificates/hostnames.")
        .one_of(&["disabled", "required", "preferred"]),
    ParameterSpec::component("sslrootcert")
        .description("Path to a CA root certificate file to use for TLS verification. Optional; if not provided, system defaults are used."),
    ParameterSpec::component("auth_source")
        .description("Authentication source database. Overrides the default auth source in the connection string."),
    ParameterSpec::component("direct_connection")
        .description("Whether to connect directly to a single MongoDB host instead of discovering the topology. Accepts 'true' or 'false'.")
        .is_boolean(),
    ParameterSpec::component("srv")
        .description("Use mongodb+srv:// connection scheme for DNS SRV record discovery. Auto-detected for .mongodb.net hosts. Accepts 'true' or 'false'. Defaults to 'false'.")
        .is_boolean(),
    ParameterSpec::component("time_zone")
        .description("Time zone to use for interpreting and returning timestamp values (e.g., 'UTC', 'America/Los_Angeles')."),
    ParameterSpec::component("unnest_depth")
        .description("Maximum nesting depth for unnesting embedded documents into a flattened structure. Higher values expand deeper nested fields."),
    ParameterSpec::component("num_docs_to_infer_schema")
        .description("Number of documents to use to infer the schema. Defaults to 400."),
    ParameterSpec::component("pool_min")
        .description("The minimum number of connections to keep open in the pool, lazily created when requested.")
        .default(DEFAULT_CONNECTION_POOL_MIN_STR),
    ParameterSpec::component("pool_max")
        .description("The maximum number of connections created in the connection pool.")
        .default(DEFAULT_CONNECTION_POOL_MAX_STR),
    ParameterSpec::runtime("change_stream_batch_max_size")
        .description("Maximum number of MongoDB Change Stream events to batch together before processing.")
        .default("1000"),
    ParameterSpec::runtime("change_stream_batch_max_duration")
        .description("Maximum time to wait for a MongoDB Change Stream batch to fill before processing.")
        .default("1s"),
    ParameterSpec::runtime("change_stream_max_await_time")
        .description("Maximum time MongoDB should wait for new Change Stream events before returning an empty batch.")
        .default("1s"),
    ParameterSpec::runtime("change_stream_batch_size")
        .description("Number of Change Stream events MongoDB should request from the server per batch.")
        .default("1000"),
    ParameterSpec::runtime("mongodb_resume_token_invalid_behavior")
        .description("Behavior when a persisted Change Stream resume token cannot be honored by the server (e.g. past the oplog retention window). 'error' surfaces a clear error so the operator can decide (recommended default; re-snapshotting a large collection should be opt-in). 'rebootstrap' drops the persisted token and re-snapshots the collection.")
        .default("error")
        .one_of(&["error", "rebootstrap"]),
];

const IGNORED_IF_URI: &[&str] = &[
    "host",
    "port",
    "db",
    "user",
    "pass",
    "auth_source",
    "direct_connection",
    "srv",
];

/// Returns `true` if the host looks like a `MongoDB` SRV endpoint
/// (e.g. `cluster0.abc123.mongodb.net`).
fn is_srv_host(host: &str) -> bool {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    normalized.ends_with(".mongodb.net")
}

impl DataConnectorFactory for MongoDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            // If a full connection_string is provided, warn about ignored connection details.
            if params.parameters.get("connection_string").ok().is_some() {
                let ignored: Vec<&str> = IGNORED_IF_URI
                    .iter()
                    .copied()
                    .filter(|k| params.parameters.get(k).ok().is_some())
                    .collect();

                if !ignored.is_empty() {
                    tracing::warn!(
                        "Both 'connection_string' and individual connection parameters ({parameters}) were provided for the {component}. The 'connection_string' will be used and the listed parameters will be ignored.",
                        parameters = ignored.join(", "),
                        component = params.component
                    );
                }
            } else {
                // Auto-detect SRV from the host parameter unless the user already set it.
                let host = params
                    .parameters
                    .get("host")
                    .ok()
                    .map(|s| s.expose_secret().to_string());
                let srv_provided = params.parameters.get("srv").ok().is_some();

                if let Some(ref h) = host
                    && is_srv_host(h)
                {
                    if !srv_provided {
                        params
                            .parameters
                            .insert("srv".to_string(), "true".to_string().into());
                    }

                    if params.parameters.get("port").ok().is_some() {
                        tracing::warn!(
                            "The 'port' parameter is ignored for SRV host '{h}' on the {component}. mongodb+srv:// uses DNS SRV records for host/port discovery.",
                            component = params.component
                        );
                    }
                }
            }

            let mut pool_min = params
                .parameters
                .get("pool_min")
                .ok()
                .and_then(|s| {
                    let pool_min_str = s.expose_secret();
                    let parsed_pool_min = pool_min_str.parse::<usize>();
                    if parsed_pool_min.is_err() {
                        tracing::warn!(
                            "Invalid pool_min value: {pool_min_str}, using default of {DEFAULT_CONNECTION_POOL_MIN_STR}"
                        );
                    }
                    parsed_pool_min.ok()
                })
                .unwrap_or(DEFAULT_CONNECTION_POOL_MIN);
            let mut pool_max = params
                .parameters
                .get("pool_max")
                .ok()
                .and_then(|s| {
                    let pool_max_str = s.expose_secret();
                    let parsed_pool_max = pool_max_str.parse::<usize>();
                    if parsed_pool_max.is_err() {
                        tracing::warn!(
                            "Invalid pool_max value: {pool_max_str}, using default of {DEFAULT_CONNECTION_POOL_MAX_STR}"
                        );
                    }
                    parsed_pool_max.ok()
                })
                .unwrap_or(DEFAULT_CONNECTION_POOL_MAX);

            if pool_min > pool_max {
                tracing::warn!(
                    "pool_min value: {pool_min} is greater than pool_max value: {pool_max}, using default values of {DEFAULT_CONNECTION_POOL_MIN_STR} and {DEFAULT_CONNECTION_POOL_MAX_STR}"
                );
                pool_min = DEFAULT_CONNECTION_POOL_MIN;
                pool_max = DEFAULT_CONNECTION_POOL_MAX;

                params
                    .parameters
                    .insert("pool_min".to_string(), pool_min.to_string().into());
                params
                    .parameters
                    .insert("pool_max".to_string(), pool_max.to_string().into());
            }

            let pool = match MongoDBConnectionPool::new(params.parameters.to_secret_map()).await {
                Ok(pool) => Arc::new(pool),
                Err(error) => match error {
                    MongoDBError::InvalidUsernameOrPassword => {
                        return Err(
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "mongodb".to_string(),
                                connector_component: params.component.clone(),
                            }
                            .into(),
                        );
                    }

                    _ => {
                        return Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "mongodb".to_string(),
                            connector_component: params.component.clone(),
                            source: Box::new(error),
                        }
                        .into());
                    }
                },
            };

            let mongodb_factory = MongoDBTableFactory::new(Arc::clone(&pool));

            Ok(Arc::new(MongoDB {
                mongodb_factory,
                pool,
                params: params.parameters,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mongodb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "mongodb";

/// Returns a new instance of the `MongoDB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    MongoDBFactory::new_arc()
}

#[derive(Debug, Snafu)]
enum ReadProviderError {
    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: &'static str,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ReadProviderError> for DataConnectorError {
    fn from(err: ReadProviderError) -> Self {
        match err {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector,
                connector_component,
                source,
            } => DataConnectorError::UnableToGetReadProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
    }
}

/// Maps a `MongoDB` index-key value to a sort direction: `1` ascending, `-1`
/// descending. Returns `None` for non-b-tree key types (`text`, `2dsphere`,
/// `hashed`, ...), which cannot be expressed as accelerator sort/index columns.
fn index_column_direction(value: &Bson) -> Option<bool> {
    let n = match value {
        Bson::Int32(i) => i64::from(*i),
        Bson::Int64(i) => *i,
        Bson::Double(f) if (*f - 1.0).abs() < f64::EPSILON => 1,
        Bson::Double(f) if (*f + 1.0).abs() < f64::EPSILON => -1,
        _ => return None,
    };
    match n {
        1 => Some(false),
        -1 => Some(true),
        _ => None,
    }
}

/// If the collection is a clustered collection (`MongoDB` 5.3+), return its cluster
/// key columns with direction — the physical-order analog of a Postgres clustered
/// index. Returns `None` (best-effort) when not clustered or on any parse miss.
async fn mongodb_clustered_sort(
    db: &mongodb::Database,
    collection_name: &str,
) -> Option<Vec<InferredSortColumn>> {
    let response = db
        .run_command(doc! { "listCollections": 1, "filter": { "name": collection_name } })
        .await
        .ok()?;
    clustered_sort_from_response(&response)
}

/// Parse the clustered-collection sort order out of a `listCollections` response.
/// Pure (no I/O) so it is unit-tested against synthetic responses. Returns `None`
/// when the collection is not clustered or the response cannot be parsed.
fn clustered_sort_from_response(response: &Document) -> Option<Vec<InferredSortColumn>> {
    let key_doc = response
        .get_document("cursor")
        .ok()?
        .get_array("firstBatch")
        .ok()?
        .first()?
        .as_document()?
        .get_document("options")
        .ok()?
        .get_document("clusteredIndex")
        .ok()?
        .get_document("key")
        .ok()?;

    let mut sort_columns = Vec::new();
    for (field, value) in key_doc {
        let desc = index_column_direction(value)?;
        sort_columns.push(InferredSortColumn {
            column: field.clone(),
            desc,
        });
    }
    (!sort_columns.is_empty()).then_some(sort_columns)
}

/// Parse one `listIndexes` entry into an [`InferredIndex`], or `None` if it should
/// be skipped: a partial index (not a table-wide guarantee), an index with no key
/// columns, or any non-b-tree key type (`text`, `2dsphere`, `hashed`, ...). Pure so
/// it is unit-tested against synthetic index documents.
fn parse_mongo_index(index_doc: &Document) -> Option<InferredIndex> {
    if index_doc.contains_key("partialFilterExpression") {
        return None;
    }
    let key_doc = index_doc.get_document("key").ok()?;
    let mut columns: Vec<String> = Vec::new();
    for (field, value) in key_doc {
        // A non-ascending/descending key type makes the whole index unusable.
        index_column_direction(value)?;
        columns.push(field.clone());
    }
    if columns.is_empty() {
        return None;
    }
    let unique = index_doc.get_bool("unique").unwrap_or(false);
    Some(InferredIndex { columns, unique })
}

/// Convert a `collStats` numeric field to a `u64` (counts and sizes are
/// non-negative integers). Returns `None` for missing, non-integer, or negative
/// values. Pure, so it is unit-tested.
fn bson_to_u64(value: Option<&Bson>) -> Option<u64> {
    match value? {
        Bson::Int32(i) => u64::try_from(*i).ok(),
        Bson::Int64(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

/// Rough collection sizing via the `collStats` command: estimated document count
/// and total data byte size. Best-effort — returns `(None, None)` on any failure.
async fn mongo_collection_size(
    db: &mongodb::Database,
    collection_name: &str,
) -> (Option<u64>, Option<u64>) {
    let Ok(stats) = db.run_command(doc! { "collStats": collection_name }).await else {
        return (None, None);
    };
    (
        bson_to_u64(stats.get("count")),
        bson_to_u64(stats.get("size")),
    )
}

/// Primary-key-ascending sort, used when the collection has no clustered key or
/// when catalog enrichment is skipped.
fn default_sort_from_primary_key(primary_key: &[String]) -> Vec<InferredSortColumn> {
    primary_key
        .iter()
        .map(|column| InferredSortColumn {
            column: column.clone(),
            desc: false,
        })
        .collect()
}

/// Best-effort catalog details for a `MongoDB` collection — everything beyond the
/// constant `_id` primary key: secondary indexes, sort/clustering order, and rough
/// sizing.
struct MongoCatalogDetails {
    indexes: Vec<InferredIndex>,
    sort_columns: Vec<InferredSortColumn>,
    row_count: Option<u64>,
    table_bytes: Option<u64>,
}

impl MongoCatalogDetails {
    /// Details when only the primary key is known (catalog unavailable or skipped):
    /// no secondary indexes, primary-key-ascending sort, no sizing.
    fn primary_key_only(primary_key: &[String]) -> Self {
        Self {
            indexes: Vec::new(),
            sort_columns: default_sort_from_primary_key(primary_key),
            row_count: None,
            table_bytes: None,
        }
    }
}

/// Best-effort `MongoDB` catalog details — secondary indexes (`listIndexes`),
/// sort/clustering order (`listCollections`), and rough sizing (`collStats`).
/// Kept separate from the constant `_id` primary key so a slow or unavailable
/// catalog can degrade to "primary key only" without blocking dataset readiness.
async fn mongodb_catalog_details(
    pool: &Arc<MongoDBConnectionPool>,
    collection_name: &str,
    primary_key: &[String],
) -> Result<MongoCatalogDetails, Box<dyn std::error::Error + Send + Sync>> {
    // Propagate the original error (`?` boxes the connection-pool error, preserving
    // its type and source chain); the call site logs it with `%error` and context.
    let connection = pool.connect().await?;
    let db = connection.client.database(&connection.db_name);

    // Secondary indexes via the `listIndexes` command. A collection has at most 64
    // indexes (a MongoDB hard limit) — far under the cursor's first-batch size — so
    // `firstBatch` always holds them all and the cursor never needs `getMore`.
    let mut indexes: Vec<InferredIndex> = Vec::new();
    let index_response = db
        .run_command(doc! { "listIndexes": collection_name })
        .await?;
    if let Ok(cursor) = index_response.get_document("cursor")
        && let Ok(first_batch) = cursor.get_array("firstBatch")
    {
        for entry in first_batch {
            let Some(index) = entry.as_document().and_then(parse_mongo_index) else {
                continue;
            };
            if index.columns == primary_key {
                continue; // the `_id_` index — already captured as the primary key
            }
            indexes.push(index);
        }
    }

    // Sort heuristic: clustered collection key (with direction), else the primary
    // key ascending — mirrors the Postgres "clustered, else primary key" rule.
    let sort_columns = match mongodb_clustered_sort(&db, collection_name).await {
        Some(sort) => sort,
        None => default_sort_from_primary_key(primary_key),
    };

    let (row_count, table_bytes) = mongo_collection_size(&db, collection_name).await;

    Ok(MongoCatalogDetails {
        indexes,
        sort_columns,
        row_count,
        table_bytes,
    })
}

/// Infer the collection's primary key, plus best-effort secondary indexes, sort
/// order, and sizing.
///
/// `MongoDB`'s document key is always `_id`, so the primary key is the constant
/// `["_id"]` and needs no catalog round-trip — this is what makes
/// `refresh_mode: changes` (`MongoDB` Streams) work without manual configuration,
/// since the change-stream path requires `primary_key: _id` plus a matching
/// `on_conflict` upsert.
///
/// The remaining details require catalog commands, bounded by
/// [`MONGODB_CATALOG_TIMEOUT`]: a slow or unavailable catalog degrades to "primary
/// key only" rather than delaying dataset load. The `MongoDB` driver is async, so
/// the timeout preempts the request at its I/O await points; the constant `_id`
/// primary key is emitted regardless.
async fn mongodb_inferred_schema_metadata(
    pool: &Arc<MongoDBConnectionPool>,
    collection_name: &str,
) -> InferredSchema {
    let primary_key = vec!["_id".to_string()];

    let details = match tokio::time::timeout(
        MONGODB_CATALOG_TIMEOUT,
        mongodb_catalog_details(pool, collection_name, &primary_key),
    )
    .await
    {
        Ok(Ok(details)) => details,
        Ok(Err(error)) => {
            tracing::debug!(
                collection = collection_name,
                %error,
                "MongoDB catalog enrichment failed; inferring the `_id` primary key only"
            );
            MongoCatalogDetails::primary_key_only(&primary_key)
        }
        Err(_elapsed) => {
            tracing::debug!(
                collection = collection_name,
                timeout_secs = MONGODB_CATALOG_TIMEOUT.as_secs(),
                "MongoDB catalog enrichment timed out; inferring the `_id` primary key only"
            );
            MongoCatalogDetails::primary_key_only(&primary_key)
        }
    };

    InferredSchema {
        primary_key,
        indexes: details.indexes,
        sort_columns: details.sort_columns,
        row_count: details.row_count,
        table_bytes: details.table_bytes,
    }
}

/// Enrich the provider's schema with inferred primary key / indexes / sort columns
/// when the dataset opts into `schema_inference: extended`.
async fn enrich_with_mongodb_metadata(
    pool: &Arc<MongoDBConnectionPool>,
    dataset: &Dataset,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    if !dataset.schema_inference.is_extended() {
        return provider;
    }

    tracing::debug!(
        dataset = %dataset.name,
        collection = %dataset.path(),
        "Applying MongoDB extended schema inference (inferring `_id` primary key; catalog enrichment is best-effort)"
    );
    let inferred = mongodb_inferred_schema_metadata(pool, dataset.path()).await;
    if inferred.is_empty() {
        return provider;
    }
    tracing::debug!(
        dataset = %dataset.name,
        collection = %dataset.path(),
        primary_key = ?inferred.primary_key,
        indexes = inferred.indexes.len(),
        sort_columns = inferred.sort_columns.len(),
        row_count = ?inferred.row_count,
        table_bytes = ?inferred.table_bytes,
        "Inferred extended schema metadata from MongoDB catalog"
    );
    data_components::metadata_enriched_table_provider(
        provider,
        inferred.to_metadata(),
        data_components::FieldMetadata::new(),
    )
}

#[async_trait]
impl DataConnector for MongoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let provider = self
            .mongodb_factory
            .table_provider(dataset.path().into())
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "mongodb",
                connector_component: ConnectorComponent::from(dataset),
            })?;
        Ok(enrich_with_mongodb_metadata(&self.pool, dataset, provider).await)
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<tokio::sync::Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<data_components::cdc::ChangesStream> {
        Some(changes::build_changes_stream(
            Arc::clone(&self.pool),
            self.params.clone(),
            dataset.clone(),
            federated_table,
        ))
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_srv_host_basic() {
        assert!(is_srv_host("cluster0.abc123.mongodb.net"));
        assert!(is_srv_host("my-cluster.xyz.mongodb.net"));
    }

    #[test]
    fn test_is_srv_host_case_insensitive() {
        assert!(is_srv_host("Cluster0.ABC123.MongoDB.Net"));
        assert!(is_srv_host("CLUSTER0.ABC123.MONGODB.NET"));
        assert!(is_srv_host("cluster0.abc123.MongoDB.NET"));
    }

    #[test]
    fn test_is_srv_host_trailing_dot() {
        assert!(is_srv_host("cluster0.abc123.mongodb.net."));
    }

    #[test]
    fn test_is_srv_host_non_srv() {
        assert!(!is_srv_host("localhost"));
        assert!(!is_srv_host("192.168.1.1"));
        assert!(!is_srv_host("mongo.example.com"));
        assert!(!is_srv_host("mongodb.net")); // bare domain, no subdomain
    }
}

#[cfg(test)]
mod inferred_schema_tests {
    use super::{
        InferredIndex, InferredSortColumn, MongoCatalogDetails, bson_to_u64,
        clustered_sort_from_response, default_sort_from_primary_key, index_column_direction,
        parse_mongo_index,
    };
    use mongodb::bson::{Bson, doc};

    #[test]
    fn coll_stats_numbers_convert_to_u64() {
        assert_eq!(bson_to_u64(Some(&Bson::Int32(42))), Some(42));
        assert_eq!(
            bson_to_u64(Some(&Bson::Int64(9_000_000_000))),
            Some(9_000_000_000)
        );
        assert_eq!(bson_to_u64(Some(&Bson::Int32(-1))), None);
        assert_eq!(bson_to_u64(Some(&Bson::String("x".to_string()))), None);
        assert_eq!(bson_to_u64(None), None);
    }

    #[test]
    fn direction_from_index_key_values() {
        assert_eq!(index_column_direction(&Bson::Int32(1)), Some(false));
        assert_eq!(index_column_direction(&Bson::Int32(-1)), Some(true));
        assert_eq!(index_column_direction(&Bson::Int64(1)), Some(false));
        assert_eq!(index_column_direction(&Bson::Double(-1.0)), Some(true));
        // Non-b-tree key types are not sort directions.
        assert_eq!(
            index_column_direction(&Bson::String("text".to_string())),
            None
        );
        assert_eq!(index_column_direction(&Bson::Int32(2)), None);
    }

    #[test]
    fn parses_simple_unique_index() {
        let index =
            parse_mongo_index(&doc! { "key": { "email": 1 }, "name": "uq_email", "unique": true })
                .expect("usable index");
        assert_eq!(
            index,
            InferredIndex {
                columns: vec!["email".to_string()],
                unique: true
            }
        );
    }

    #[test]
    fn parses_compound_non_unique_index() {
        let index = parse_mongo_index(&doc! { "key": { "a": 1, "b": -1 }, "name": "idx_ab" })
            .expect("usable index");
        assert_eq!(
            index,
            InferredIndex {
                columns: vec!["a".to_string(), "b".to_string()],
                unique: false
            }
        );
    }

    #[test]
    fn skips_partial_index() {
        assert!(
            parse_mongo_index(&doc! {
                "key": { "sku": 1 },
                "unique": true,
                "partialFilterExpression": { "active": true }
            })
            .is_none()
        );
    }

    #[test]
    fn skips_non_btree_indexes() {
        assert!(parse_mongo_index(&doc! { "key": { "body": "text" }, "name": "txt" }).is_none());
        assert!(parse_mongo_index(&doc! { "key": { "loc": "2dsphere" }, "name": "geo" }).is_none());
    }

    #[test]
    fn _id_index_parses_as_primary_key_columns() {
        // The implicit `_id_` index; the caller drops it as the primary key.
        let index =
            parse_mongo_index(&doc! { "key": { "_id": 1 }, "name": "_id_" }).expect("usable index");
        assert_eq!(index.columns, vec!["_id".to_string()]);
    }

    #[test]
    fn parses_clustered_collection_sort() {
        let response = doc! {
            "cursor": {
                "firstBatch": [
                    { "name": "events", "options": { "clusteredIndex": { "key": { "_id": 1 } } } }
                ]
            }
        };
        assert_eq!(
            clustered_sort_from_response(&response),
            Some(vec![InferredSortColumn {
                column: "_id".to_string(),
                desc: false
            }])
        );
    }

    #[test]
    fn non_clustered_collection_has_no_sort() {
        let response = doc! { "cursor": { "firstBatch": [ { "name": "events", "options": {} } ] } };
        assert_eq!(clustered_sort_from_response(&response), None);
    }

    #[test]
    fn default_sort_is_primary_key_ascending() {
        assert_eq!(
            default_sort_from_primary_key(&["a".to_string(), "b".to_string()]),
            vec![
                InferredSortColumn {
                    column: "a".to_string(),
                    desc: false
                },
                InferredSortColumn {
                    column: "b".to_string(),
                    desc: false
                },
            ]
        );
    }

    #[test]
    fn primary_key_only_details_skip_indexes_and_sizing() {
        // When catalog enrichment is unavailable (timeout/error), the fallback still
        // yields a primary-key-ascending sort and nothing else; the constant `_id`
        // primary key the caller adds is enough for change streams.
        let details = MongoCatalogDetails::primary_key_only(&["_id".to_string()]);
        assert!(details.indexes.is_empty());
        assert_eq!(
            details.sort_columns,
            vec![InferredSortColumn {
                column: "_id".to_string(),
                desc: false
            }]
        );
        assert_eq!(details.row_count, None);
        assert_eq!(details.table_bytes, None);
    }
}
