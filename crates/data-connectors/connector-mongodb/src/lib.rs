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
use mongodb::bson::{Bson, doc};
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

/// Maps a MongoDB index-key value to a sort direction: `1` ascending, `-1`
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

/// If the collection is a clustered collection (MongoDB 5.3+), return its cluster
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

/// Infer the collection's primary key, secondary indexes, and sort order from
/// MongoDB catalog commands (`listIndexes`, `listCollections`).
///
/// MongoDB's document key is always `_id`, so the primary key is `["_id"]` — this
/// is what makes `refresh_mode: changes` (MongoDB Streams) work without manual
/// configuration, since the change-stream path requires `primary_key: _id` plus a
/// matching `on_conflict` upsert.
async fn mongodb_inferred_schema_metadata(
    pool: &Arc<MongoDBConnectionPool>,
    collection_name: &str,
) -> Result<InferredSchema, Box<dyn std::error::Error + Send + Sync>> {
    let connection = pool
        .connect()
        .await
        .map_err(|e| format!("failed to connect to MongoDB: {e}"))?;
    let db = connection.client.database(&connection.db_name);

    // MongoDB's document key is always `_id`.
    let primary_key = vec!["_id".to_string()];

    // Secondary indexes via the `listIndexes` command (indexes are few and fit in
    // the first cursor batch, so there is no need to exhaust the cursor).
    let mut indexes: Vec<InferredIndex> = Vec::new();
    let index_response = db
        .run_command(doc! { "listIndexes": collection_name })
        .await?;
    if let Ok(cursor) = index_response.get_document("cursor")
        && let Ok(first_batch) = cursor.get_array("firstBatch")
    {
        for entry in first_batch {
            let Some(index_doc) = entry.as_document() else {
                continue;
            };
            // Partial indexes are not a table-wide guarantee — skip.
            if index_doc.contains_key("partialFilterExpression") {
                continue;
            }
            let Ok(key_doc) = index_doc.get_document("key") else {
                continue;
            };
            // Collect plain b-tree columns; drop the whole index if any key part is
            // a non-ascending/descending key type (text, 2dsphere, hashed, ...).
            let mut columns: Vec<String> = Vec::new();
            let mut usable = true;
            for (field, value) in key_doc {
                if index_column_direction(value).is_some() {
                    columns.push(field.clone());
                } else {
                    usable = false;
                    break;
                }
            }
            if !usable || columns.is_empty() || columns == primary_key {
                continue; // unusable, empty, or the `_id_` index (the primary key)
            }
            let unique = index_doc.get_bool("unique").unwrap_or(false);
            indexes.push(InferredIndex { columns, unique });
        }
    }

    // Sort heuristic: clustered collection key (with direction), else the primary
    // key ascending — mirrors the Postgres "clustered, else primary key" rule.
    let sort_columns = match mongodb_clustered_sort(&db, collection_name).await {
        Some(sort) => sort,
        None => primary_key
            .iter()
            .map(|column| InferredSortColumn {
                column: column.clone(),
                desc: false,
            })
            .collect(),
    };

    Ok(InferredSchema {
        primary_key,
        indexes,
        sort_columns,
    })
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

    match mongodb_inferred_schema_metadata(pool, dataset.path()).await {
        Ok(inferred) => {
            if inferred.is_empty() {
                return provider;
            }
            tracing::debug!(
                dataset = %dataset.name,
                collection = %dataset.path(),
                primary_key = ?inferred.primary_key,
                indexes = inferred.indexes.len(),
                sort_columns = inferred.sort_columns.len(),
                "Inferred extended schema metadata from MongoDB catalog"
            );
            data_components::metadata_enriched_table_provider(
                provider,
                inferred.to_metadata(),
                data_components::FieldMetadata::new(),
            )
        }
        Err(error) => {
            tracing::warn!(
                dataset = %dataset.name,
                collection = %dataset.path(),
                error = %error,
                "Failed to infer extended schema from MongoDB catalog; registering without inferred metadata"
            );
            provider
        }
    }
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
            .table_provider(dataset.path().into(), dataset.schema.clone())
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
