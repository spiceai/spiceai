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

//! `PostgreSQL` data connector for Spice.ai runtime.
//!
//! This crate provides the `PostgreSQL` connector implementation, allowing
//! Spice.ai to connect to `PostgreSQL` databases as data sources. It also
//! exposes a direct WAL-based `ChangesStream`, so users can set
//! `acceleration.refresh_mode: changes` on a Postgres dataset and get
//! change-by-change replication into the local accelerator without Debezium.

use async_trait::async_trait;
use data_components::federation::create_spice_federated_table_provider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion_table_providers::postgres::{DynPostgresConnectionPool, PostgresTableFactory};
use datafusion_table_providers::sql::db_connection_pool::dbconnection;
use datafusion_table_providers::sql::db_connection_pool::{
    Error as DbConnectionPoolError,
    postgrespool::{self, PostgresConnectionPool},
};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::MetricsProvider;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::datafusion::udf::deny_spice_specific_functions;
use runtime::parameters::ParameterSpec;
use secrecy::SecretBox;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

mod replication;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: DbConnectionPoolError },
}

/// `PostgreSQL` data connector.
pub struct Postgres {
    factory: PostgresTableFactory,
    pool: Arc<PostgresConnectionPool>,
    params: runtime::parameters::Parameters,
    replication_metrics:
        std::sync::Arc<data_components::postgres_replication::ReplicationMetricsCollector>,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres").finish_non_exhaustive()
    }
}

/// Factory for creating `PostgreSQL` connector instances.
#[derive(Default, Copy, Clone)]
pub struct PostgresFactory {}

impl PostgresFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const POSTGRES_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/postgres";

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description(
            "Full libpq-style connection string. Overrides other connection params if set.",
        )
        .examples(&["host=db.example.com port=5432 dbname=app user=ro sslmode=require"])
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("user")
        .description("PostgreSQL username.")
        .examples(&["postgres", "spice_reader"])
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("pass")
        .description("PostgreSQL password.")
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("host")
        .description("PostgreSQL server hostname or IP.")
        .examples(&["db.internal", "10.0.0.5"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("port")
        .description("PostgreSQL TCP port.")
        .examples(&["5432"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("db")
        .description("Database name.")
        .examples(&["app", "analytics"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("sslmode")
        .description("libpq SSL mode: disable, allow, prefer, require, verify-ca, verify-full.")
        .one_of(&[
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("sslrootcert")
        .description(
            "Path to, or inline PEM content for, a CA certificate used when sslmode is verify-ca/verify-full.",
        )
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("connection_pool_min_idle")
        .description("The minimum number of idle connections to keep open in the pool.")
        .default("1")
        .help_link(POSTGRES_DOCS),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections created in the connection pool.")
        .default("5")
        .help_link(POSTGRES_DOCS),
    // --- Logical replication (WAL streaming) ---
    ParameterSpec::component("replication_slot").description(
        "Name of the Postgres replication slot to create/reuse for this dataset. \
         Defaults to `spice_<dataset>_<dataset-hash>_<instance-hash>`. Each Spice replica \
         MUST have its own unique slot.",
    ),
    ParameterSpec::component("publication").description(
        "Name of the Postgres publication to create/reuse for this dataset. \
         Defaults to `spice_<dataset>_<dataset-hash>_pub`. Shared across replicas for the \
         same dataset.",
    ),
    ParameterSpec::component("replication_initial_snapshot")
        .description(
            "Whether to take an initial snapshot of the table's existing rows on first \
             connection, before streaming WAL changes. Default: true.",
        )
        .default("true"),
    ParameterSpec::component("replication_temporary_slot")
        .description(
            "If true, create a temporary replication slot that is dropped when the \
             Spice process disconnects. Default: false (durable slot).",
        )
        .default("false"),
    ParameterSpec::component("replication_status_interval")
        .description(
            "How often to send StandbyStatusUpdate to Postgres (e.g. '10s'). \
             Default: 10s.",
        )
        .default("10s"),
    ParameterSpec::component("replication_bootstrap_batch_size")
        .description(
            "Rows per emitted batch during the initial replication snapshot. \
             Default: 8192. Maximum: 1048576.",
        )
        .default("8192"),
];

impl DataConnectorFactory for PostgresFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mut param_map = params.parameters.to_secret_map();

            param_map.insert(
                "application_name".to_string(),
                SecretBox::from(format!("Spice.ai {}", env!("CARGO_PKG_VERSION"))),
            );

            let params_for_replication = params.parameters.clone();

            match PostgresConnectionPool::new(param_map).await {
                Ok(pool) => {
                    let unsupported_type_action = params
                        .unsupported_type_action
                        .unwrap_or(datafusion_table_providers::UnsupportedTypeAction::String);
                    let pool = pool.with_unsupported_type_action(unsupported_type_action);

                    let pool = Arc::new(pool);
                    let factory = PostgresTableFactory::new(Arc::clone(&pool));
                    Ok(Arc::new(Postgres {
                        factory,
                        pool,
                        params: params_for_replication,
                        replication_metrics:
                            data_components::postgres_replication::ReplicationMetricsCollector::new(
                            ),
                    }) as Arc<dyn DataConnector>)
                }
                Err(e) => match e {
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "postgres".to_string(),
                            connector_component: params.component.clone(),
                        }
                        .into(),
                    ),

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        host,
                        port: format!("{port}"),
                    }
                    .into()),

                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

async fn postgres_comment_metadata(
    pool: &Arc<PostgresConnectionPool>,
    table_path: &str,
) -> std::result::Result<
    (HashMap<String, String>, data_components::FieldMetadata),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let conn = pool.connect_direct().await?;
    let rows = conn
        .conn
        .query(
            "SELECT \
                 obj_description(c.oid, 'pg_class') AS table_comment, \
                 a.attname AS column_name, \
                 col_description(c.oid, a.attnum) AS column_comment, \
                 format_type(a.atttypid, a.atttypmod) AS column_source_type \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = c.oid \
                 AND a.attnum > 0 \
                 AND NOT a.attisdropped \
             WHERE c.oid = to_regclass($1) \
             ORDER BY a.attnum",
            &[&table_path],
        )
        .await?;
    let rows = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)));

    Ok(data_components::postgres::provider::postgres_metadata_from_rows(rows))
}

/// Build a federated `PostgreSQL` read provider with the Spice function deny-list
/// installed, so Spice-only UDFs (`json_get_str`, etc.) are evaluated locally
/// instead of being unparsed into the SQL sent to `PostgreSQL`, which would
/// reject them. This mirrors the upstream `PostgresTableFactory::table_provider`
/// internals but routes pushdown decisions through
/// [`create_spice_federated_table_provider`] (the upstream `SqlTable`'s
/// `can_execute_plan` defaults to always-federate and ignores the deny-list). See
/// issue #10703.
async fn federated_postgres_table_provider(
    pool: Arc<PostgresConnectionPool>,
    table_reference: TableReference,
) -> std::result::Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>>
{
    let dyn_pool: Arc<DynPostgresConnectionPool> = pool;
    let sql_table = Arc::new(
        SqlTable::new("postgres", &dyn_pool, table_reference.clone())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .with_dialect(Arc::new(PostgreSqlDialect {})),
    );

    let schema = sql_table.schema();
    Ok(Arc::new(create_spice_federated_table_provider(
        sql_table,
        schema,
        table_reference,
        Some(deny_spice_specific_functions().as_ref().clone()),
    )))
}

async fn enrich_with_postgres_comments(
    pool: &Arc<PostgresConnectionPool>,
    dataset: &Dataset,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    match postgres_comment_metadata(pool, dataset.path()).await {
        Ok((table_metadata, field_metadata)) => {
            if table_metadata.is_empty() && field_metadata.is_empty() {
                provider
            } else {
                data_components::metadata_enriched_table_provider(
                    provider,
                    table_metadata,
                    field_metadata,
                )
            }
        }
        Err(error) => {
            tracing::warn!(
                dataset = %dataset.name,
                source = %dataset.path(),
                error = %error,
                "Failed to query PostgreSQL comments; registering without comment metadata"
            );
            provider
        }
    }
}

#[async_trait]
impl DataConnector for Postgres {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self
            .factory
            .read_write_table_provider(dataset.path().into())
            .await
        {
            Ok(provider) => Some(Ok(enrich_with_postgres_comments(
                &self.pool, dataset, provider,
            )
            .await)),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Some(Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            }));
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Some(Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            }));
                        }
                        _ => {}
                    }
                }

                Some(Err(DataConnectorError::UnableToGetReadWriteProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                }))
            }
        }
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match federated_postgres_table_provider(Arc::clone(&self.pool), dataset.path().into()).await
        {
            Ok(provider) => Ok(enrich_with_postgres_comments(&self.pool, dataset, provider).await),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            });
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            });
                        }
                        _ => {}
                    }
                }

                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })
            }
        }
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<runtime::federated_table::FederatedTable>,
        dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<tokio::sync::Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<data_components::cdc::ChangesStream> {
        Some(replication::build_changes_stream(
            &self.params,
            dataset,
            federated_table,
            Arc::clone(&self.replication_metrics),
        ))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(replication::PostgresMetricsProvider::new(
            data_components::postgres_replication::ReplicationMetrics::new(Arc::clone(
                &self.replication_metrics,
            )),
        )))
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "postgres";

/// Returns a new instance of the `PostgreSQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    PostgresFactory::new_arc()
}
