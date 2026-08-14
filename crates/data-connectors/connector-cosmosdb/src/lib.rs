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
#![allow(clippy::missing_errors_doc)]

pub mod cosmosdb;

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crate::cosmosdb::{
    BackoffMethod, CosmosDBCredential, CosmosDBTableProvider, CosmosResilienceConfig,
    DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_RETRIES, DEFAULT_QUERY,
    DEFAULT_SCHEMA_INFER_MAX_RECORDS, build_container_client,
    provider::CosmosDBTableProviderConfig,
};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::UnsupportedTypeAction as DFUnsupportedTypeAction;
use opentelemetry::KeyValue;
use tokio::sync::Semaphore;

use runtime::component::dataset::Dataset;
use runtime_component::dataset::DatasetSpec;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
};
use runtime_api_types::v1::ComponentType;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ParameterSpec, Parameters};

type SemaphoreEntry = (Arc<Semaphore>, usize);

static COSMOS_CONCURRENCY_LIMITS: LazyLock<Mutex<HashMap<String, SemaphoreEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
            .description("Azure Cosmos DB operations currently holding a concurrency permit")
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
        .description("Number of documents sampled during schema inference.")
        .default("100"),
    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent Azure Cosmos DB requests per account endpoint.")
        .default("4"),
    ParameterSpec::runtime("http_max_retries")
        .description("Maximum number of retries for transient errors.")
        .default("3"),
    ParameterSpec::runtime("backoff_method")
        .description("Backoff strategy between schema-inference sampling retries.")
        .one_of(&["exponential", "fibonacci"])
        .default("exponential"),
    ParameterSpec::runtime("disable_on_permanent_error")
        .description("When true, a permanent error latches the connector into a disabled state.")
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
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
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
        dataset: &DatasetSpec,
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

fn parse_database_and_container(
    path: &str,
    database_param: Option<&str>,
) -> Result<(String, String), String> {
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

fn resolve_database_and_container(
    dataset: &DatasetSpec,
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
        dataset: &DatasetSpec,
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

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "cosmosdb";

/// Returns a new instance of the `CosmosDB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    CosmosDBFactory::new_arc()
}

// Self-register into runtime's linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_cosmosdb as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
runtime::register_data_connector!(
    register_cosmosdb_connector,
    COSMOSDB_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    CosmosDBFactory
);
