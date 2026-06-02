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

//! Azure Cosmos DB (`NoSQL` / Core SQL API) data connector.
//!
//! Read-only scan with schema inferred from a sample of documents, backed by
//! RC-level connection resilience (concurrency limiting, retry with backoff,
//! permanent-error detection) and an `inflight_operations` metric gauge.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use async_trait::async_trait;
use data_components::cosmosdb::{
    BackoffMethod, CosmosDBCredential, CosmosDBTableProvider, CosmosResilienceConfig,
    DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_RETRIES, DEFAULT_QUERY,
    DEFAULT_SCHEMA_INFER_MAX_RECORDS, build_container_client,
    provider::CosmosDBTableProviderConfig,
};
use datafusion::datasource::TableProvider;
use datafusion_table_providers::UnsupportedTypeAction as DFUnsupportedTypeAction;
use opentelemetry::KeyValue;
use tokio::sync::Semaphore;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};
use crate::component::ComponentType;
use crate::component::dataset::Dataset;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};

const CONNECTOR_NAME: &str = "cosmosdb";

/// Semaphore paired with the numeric limit it was constructed with, so
/// mismatches across datasets targeting the same Cosmos account can be
/// detected and surfaced as a warning.
type SemaphoreEntry = (Arc<Semaphore>, usize);

/// Per-account-endpoint concurrency semaphores. Datasets that hit the same
/// Cosmos account share a single concurrency budget, matching the per-account
/// rate-limit model of Cosmos DB.
///
/// Entries are never evicted during the runtime's lifetime: each slot holds an
/// `Arc<Semaphore>` + `usize` (~40 bytes on 64-bit platforms), and typical
/// deployments configure a bounded set of accounts. Workloads that
/// dynamically materialize many distinct Cosmos accounts should treat this as
/// a known upper bound on memory use.
static COSMOS_CONCURRENCY_LIMITS: LazyLock<Mutex<HashMap<String, SemaphoreEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Per-account-endpoint disabled-state flags. A permanent error (401/403/404)
/// observed by one dataset latches the connector for every dataset pointing
/// at the same account. Same memory footprint and eviction trade-off as
/// `COSMOS_CONCURRENCY_LIMITS` above.
static COSMOS_DISABLED_FLAGS: LazyLock<Mutex<HashMap<String, Arc<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn shared_semaphore(endpoint: &str, max_concurrent: usize) -> Arc<Semaphore> {
    let mut guard = COSMOS_CONCURRENCY_LIMITS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((semaphore, existing_max)) = guard.get(endpoint) {
        if *existing_max != max_concurrent {
            tracing::warn!(
                endpoint = %endpoint,
                existing_max,
                requested_max = max_concurrent,
                "Multiple datasets target the same Cosmos DB account with different max_concurrent_requests values. Keeping the first-seen limit ({existing_max})."
            );
        }
        Arc::<Semaphore>::clone(semaphore)
    } else {
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        guard.insert(
            endpoint.to_string(),
            (Arc::<Semaphore>::clone(&semaphore), max_concurrent),
        );
        semaphore
    }
}

fn shared_disabled_flag(endpoint: &str) -> Arc<AtomicBool> {
    let mut guard = COSMOS_DISABLED_FLAGS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::<AtomicBool>::clone(
        guard
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(AtomicBool::new(false))),
    )
}

const COSMOSDB_METRICS: &[MetricSpec] =
    &[
        MetricSpec::new("inflight_operations", MetricType::ObservableGaugeU64)
            .description("Azure Cosmos DB operations currently holding a concurrency permit — incremented once per operation and held across retry backoff sleeps (not a pure in-flight-HTTP counter)")
            .auto_register(),
    ];

#[derive(Debug, Clone)]
struct CosmosDBMetricsProvider {
    inflight_operations: Arc<AtomicU64>,
}

impl MetricsProvider for CosmosDBMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        COSMOSDB_METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        match metric.name {
            "inflight_operations" => {
                let counter = Arc::<AtomicU64>::clone(&self.inflight_operations);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(counter.load(Ordering::Relaxed), &attributes);
                })))
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct CosmosDB {
    params: Parameters,
    /// Drives the `inflight_operations` metric gauge. Instantiated per
    /// connector (one per dataset), so the exported value reflects in-flight
    /// operations for that dataset rather than a shared per-account budget —
    /// the shared concurrency budget itself is enforced via the endpoint-keyed
    /// `COSMOS_CONCURRENCY_LIMITS` map, not this counter.
    inflight_operations: Arc<AtomicU64>,
    unsupported_type_action: Option<DFUnsupportedTypeAction>,
}

#[derive(Default, Debug, Copy, Clone)]
pub struct CosmosDBFactory {}

impl CosmosDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("account_endpoint")
        .description("The Azure Cosmos DB account endpoint URL, e.g. 'https://my-account.documents.azure.com:443/'.")
        .secret(),
    ParameterSpec::component("account_key")
        .description("The Azure Cosmos DB account primary or secondary key.")
        .secret(),
    ParameterSpec::component("connection_string")
        .description("An Azure Cosmos DB connection string (AccountEndpoint=...;AccountKey=...). Takes precedence over account_endpoint/account_key if set.")
        .secret(),
    ParameterSpec::component("database")
        .description("The Cosmos DB database name. Defaults to the first segment of the dataset `from:` path ('database.container')."),
    ParameterSpec::runtime("query")
        .description("Cosmos SQL query used to scan the container. Defaults to 'SELECT * FROM c'.")
        .default(DEFAULT_QUERY),
    ParameterSpec::runtime("schema_infer_max_records")
        .description("Number of documents sampled during schema inference. Larger samples produce a more precise schema at the cost of additional RU consumption on dataset registration.")
        .default("100"),

    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent Azure Cosmos DB requests per account endpoint, shared across all datasets pointing at the same account.")
        .default("4"),
    ParameterSpec::runtime("http_max_retries")
        .description("Maximum number of retries for transient errors (429, 5xx, network) during the schema-inference sampling pass at dataset registration. Retries use the configured backoff strategy and honor Retry-After headers. Mid-stream pager errors during scan execution are not retried.")
        .default("3"),
    ParameterSpec::runtime("backoff_method")
        .description("Backoff strategy between schema-inference sampling retries on transient errors. 'exponential' doubles the delay each attempt; 'fibonacci' follows the Fibonacci sequence.")
        .one_of(&["exponential", "fibonacci"])
        .default("exponential"),
    ParameterSpec::runtime("disable_on_permanent_error")
        .description("When true, a permanent error (401/403/404) from Azure Cosmos DB latches the connector into a disabled state and short-circuits subsequent requests until Spice is restarted.")
        .default("true")
        .is_boolean(),
];

impl DataConnectorFactory for CosmosDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let unsupported_type_action = params.unsupported_type_action;
        Box::pin(async move {
            let conn = CosmosDB {
                params: params.parameters,
                inflight_operations: Arc::new(AtomicU64::new(0)),
                unsupported_type_action,
            };
            Ok(Arc::new(conn) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }
}

impl CosmosDB {
    fn build_credential(
        &self,
        dataset: &Dataset,
    ) -> Result<CosmosDBCredential, DataConnectorError> {
        if let Some(conn_str) = self.params.get("connection_string").expose().ok() {
            return Ok(CosmosDBCredential::ConnectionString(conn_str.to_string()));
        }

        let endpoint = self.params.get("account_endpoint").expose().ok();
        let key = self.params.get("account_key").expose().ok();

        match (endpoint, key) {
            (Some(endpoint), Some(key)) => Ok(CosmosDBCredential::Key {
                endpoint: endpoint.to_string(),
                key: key.to_string(),
            }),
            _ => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: "Azure Cosmos DB requires either 'cosmosdb_connection_string' or both 'cosmosdb_account_endpoint' and 'cosmosdb_account_key'.".to_string(),
            }),
        }
    }

    /// Materialize a resilience config from validated parameters. Per-endpoint
    /// semaphore and disabled flag are shared across datasets that target the
    /// same Cosmos account.
    fn build_resilience(&self, endpoint: &str) -> CosmosResilienceConfig {
        let max_concurrent_requests = self
            .params
            .get("max_concurrent_requests")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
            .max(1);

        let max_retries = self
            .params
            .get("http_max_retries")
            .expose()
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_MAX_RETRIES);

        let backoff_value = self
            .params
            .get("backoff_method")
            .expose()
            .ok()
            .unwrap_or("exponential");
        let backoff = BackoffMethod::parse(backoff_value).unwrap_or_else(|message| {
            tracing::warn!("{message}; falling back to 'exponential'.");
            BackoffMethod::Exponential
        });

        let disable_on_permanent_error = self
            .params
            .get("disable_on_permanent_error")
            .expose()
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        let semaphore = shared_semaphore(endpoint, max_concurrent_requests);
        let disabled = shared_disabled_flag(endpoint);

        CosmosResilienceConfig {
            max_retries,
            backoff,
            semaphore: Some(semaphore),
            disable_on_permanent_error,
            inflight: Arc::<AtomicU64>::clone(&self.inflight_operations),
            disabled,
        }
    }
}

/// Pure parsing helper for [`resolve_database_and_container`]. Split out so
/// it can be exercised in unit tests without constructing a full [`Dataset`].
fn parse_database_and_container(
    path: &str,
    database_param: Option<&str>,
) -> Result<(String, String), String> {
    // Accept either `database.container` or `database/container`, or just the
    // container when `database` is explicitly set.
    let (db_from_path, container) = if let Some((db, container)) = path.split_once('.') {
        (Some(db.to_string()), container.to_string())
    } else if let Some((db, container)) = path.split_once('/') {
        (Some(db.to_string()), container.to_string())
    } else {
        (None, path.to_string())
    };

    let database = match (database_param, db_from_path) {
        (Some(d), _) => d.to_string(),
        (None, Some(d)) => d,
        (None, None) => {
            return Err(format!(
                "Could not determine Cosmos DB database from dataset path '{path}'. Expected 'database.container' or set the 'cosmosdb_database' parameter."
            ));
        }
    };

    if database.is_empty() {
        return Err(format!(
            "Could not determine Cosmos DB database from dataset path '{path}'. Expected 'database.container' or set the 'cosmosdb_database' parameter."
        ));
    }

    if container.is_empty() {
        return Err(format!(
            "Could not determine Cosmos DB container from dataset path '{path}'."
        ));
    }

    Ok((database, container))
}

/// Parse `database.container` / `database/container` from the dataset path.
/// If the configured `database` parameter is set, it overrides the database
/// segment and the path is treated as just the container name.
fn resolve_database_and_container(
    dataset: &Dataset,
    database_param: Option<&str>,
) -> Result<(String, String), DataConnectorError> {
    parse_database_and_container(dataset.path(), database_param).map_err(|message| {
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: CONNECTOR_NAME.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message,
        }
    })
}

#[async_trait]
impl DataConnector for CosmosDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        let credential = self.build_credential(dataset)?;

        let database_param = self.params.get("database").expose().ok();
        let (database, container) = resolve_database_and_container(dataset, database_param)?;

        let (container_client, endpoint) =
            build_container_client(credential, &database, &container).map_err(|e| {
                DataConnectorError::UnableToGetReadProvider {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?;

        let query = self
            .params
            .get("query")
            .expose()
            .ok()
            .unwrap_or(DEFAULT_QUERY)
            .to_string();

        let schema_infer_max_records = match self
            .params
            .get("schema_infer_max_records")
            .expose()
            .ok()
        {
            Some(value) => match value.parse::<usize>() {
                Ok(0) => {
                    tracing::warn!(
                        "Ignoring invalid schema_infer_max_records value '0' for dataset {}; using default value {}.",
                        dataset.name,
                        DEFAULT_SCHEMA_INFER_MAX_RECORDS
                    );
                    DEFAULT_SCHEMA_INFER_MAX_RECORDS
                }
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!(
                        "Ignoring invalid schema_infer_max_records value '{}' for dataset {}; expected a positive integer, using default value {}.",
                        value,
                        dataset.name,
                        DEFAULT_SCHEMA_INFER_MAX_RECORDS
                    );
                    DEFAULT_SCHEMA_INFER_MAX_RECORDS
                }
            },
            None => DEFAULT_SCHEMA_INFER_MAX_RECORDS,
        };

        let resilience = self.build_resilience(&endpoint);

        let mut config = CosmosDBTableProviderConfig::new(database, container, query)
            .with_schema_infer_max_records(schema_infer_max_records)
            .with_resilience(resilience);

        if let Some(action) = self.unsupported_type_action {
            config = config.with_unsupported_type_action(action);
        }

        let provider = CosmosDBTableProvider::try_new(container_client, endpoint, config)
            .await
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        Ok(Arc::new(provider))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(CosmosDBMetricsProvider {
            inflight_operations: Arc::<AtomicU64>::clone(&self.inflight_operations),
        }))
    }
}

register_data_connector!("cosmosdb", CosmosDBFactory);

#[cfg(test)]
mod tests {
    use super::{parse_database_and_container, shared_disabled_flag, shared_semaphore};
    use std::sync::atomic::Ordering;

    #[test]
    fn parses_dot_delimited_path() {
        let (db, container) = parse_database_and_container("mydb.mycontainer", None)
            .expect("dot-delimited path should parse");
        assert_eq!(db, "mydb");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn parses_slash_delimited_path() {
        let (db, container) = parse_database_and_container("mydb/mycontainer", None)
            .expect("slash-delimited path should parse");
        assert_eq!(db, "mydb");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn uses_database_param_when_path_is_container_only() {
        let (db, container) = parse_database_and_container("mycontainer", Some("explicit_db"))
            .expect("container-only path with explicit db should parse");
        assert_eq!(db, "explicit_db");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn database_param_overrides_path_segment() {
        let (db, container) =
            parse_database_and_container("path_db.mycontainer", Some("override_db"))
                .expect("db param should override path segment");
        assert_eq!(db, "override_db");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn errors_when_no_database_can_be_determined() {
        let err = parse_database_and_container("just_container", None)
            .expect_err("missing db should be an error");
        assert!(err.contains("Could not determine Cosmos DB database"));
    }

    #[test]
    fn errors_on_empty_container_segment() {
        let err = parse_database_and_container("mydb.", None)
            .expect_err("empty container segment should be an error");
        assert!(err.contains("Could not determine Cosmos DB container"));

        let err = parse_database_and_container("mydb/", None)
            .expect_err("empty container segment should be an error");
        assert!(err.contains("Could not determine Cosmos DB container"));
    }

    #[test]
    fn errors_on_empty_database_segment() {
        let err = parse_database_and_container(".mycontainer", None)
            .expect_err("empty database segment should be an error");
        assert!(err.contains("Could not determine Cosmos DB database"));

        let err = parse_database_and_container("/mycontainer", None)
            .expect_err("empty database segment should be an error");
        assert!(err.contains("Could not determine Cosmos DB database"));
    }

    #[test]
    fn dot_takes_precedence_over_slash() {
        // Documents current behavior: the first `.` wins even when a `/` is
        // also present. Cosmos DB names do not legally contain `.`, so this
        // mainly matters for malformed input.
        let (db, container) =
            parse_database_and_container("a/b.c", None).expect("dot takes precedence over slash");
        assert_eq!(db, "a/b");
        assert_eq!(container, "c");
    }

    #[test]
    fn multiple_dots_split_at_first() {
        let (db, container) =
            parse_database_and_container("a.b.c", None).expect("multiple dots split at first");
        assert_eq!(db, "a");
        assert_eq!(container, "b.c");
    }

    #[test]
    fn shared_semaphore_returns_same_instance_for_same_endpoint() {
        // Use a unique endpoint per test to avoid cross-test interference
        // through the process-wide `COSMOS_CONCURRENCY_LIMITS` map.
        let endpoint = "https://shared-semaphore-same-endpoint.documents.azure.com:443/";
        let sem_a = shared_semaphore(endpoint, 4);
        let sem_b = shared_semaphore(endpoint, 4);
        assert!(std::sync::Arc::ptr_eq(&sem_a, &sem_b));
    }

    #[test]
    fn shared_semaphore_keeps_first_seen_limit_on_mismatch() {
        let endpoint = "https://shared-semaphore-mismatch.documents.azure.com:443/";
        let sem_a = shared_semaphore(endpoint, 4);
        // A conflicting request should be resolved in favor of the first-seen
        // limit rather than silently bumping or panicking.
        let sem_b = shared_semaphore(endpoint, 16);
        assert!(std::sync::Arc::ptr_eq(&sem_a, &sem_b));
        assert_eq!(sem_a.available_permits(), 4);
    }

    #[test]
    fn shared_disabled_flag_shares_state_across_lookups() {
        let endpoint = "https://shared-disabled-flag.documents.azure.com:443/";
        let flag_a = shared_disabled_flag(endpoint);
        let flag_b = shared_disabled_flag(endpoint);
        assert!(std::sync::Arc::ptr_eq(&flag_a, &flag_b));
        flag_a.store(true, Ordering::SeqCst);
        assert!(flag_b.load(Ordering::SeqCst));
    }
}
