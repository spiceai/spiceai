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

//! `ScyllaDB` data connector for Spice.ai runtime.
//!
//! This crate provides the `ScyllaDB` connector implementation, allowing
//! Spice.ai to connect to `ScyllaDB` clusters as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use data_components::Read;
use data_components::scylladb::ScyllaDbTableFactory;
use datafusion::datasource::TableProvider;
use db_connection_pool::scylladbpool::ScyllaDbConnectionPool;
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use runtime_parameters::Parameters;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to ScyllaDB: {source}"))]
    UnableToCreateSession {
        source: scylla::errors::NewSessionError,
    },

    #[snafu(display(
        "Missing required parameter: '{parameter_name}'. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/scylladb#configuration"
    ))]
    MissingRequiredParameter { parameter_name: String },

    #[snafu(display(
        "Unable to connect to ScyllaDB on {host}:{port}. Ensure that the host and port are correctly configured, and that the host is reachable."
    ))]
    InvalidHostOrPortError {
        source: Box<dyn std::error::Error + Sync + Send>,
        host: String,
        port: String,
    },

    #[snafu(display("Invalid port value: {source}"))]
    InvalidPortValue { source: std::num::ParseIntError },

    #[snafu(display("Invalid connection timeout value: {source}"))]
    InvalidConnectionTimeoutValue { source: std::num::ParseIntError },

    #[snafu(display(
        "Authentication failed. Ensure that the username and password are correctly configured."
    ))]
    AuthenticationError,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `ScyllaDB` data connector.
#[derive(Debug)]
pub struct ScyllaDb {
    scylladb_factory: ScyllaDbTableFactory,
}

/// Factory for creating `ScyllaDB` connector instances.
#[derive(Default, Copy, Clone)]
pub struct ScyllaDbFactory {}

impl ScyllaDbFactory {
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
    // scylladb_host or scylladb_hosts
    ParameterSpec::component("host").description(
        "The hostname of the ScyllaDB node to connect to. Can be a comma-separated list of hosts.",
    ),
    ParameterSpec::component("hosts").description(
        "A comma-separated list of ScyllaDB node hostnames to connect to (alternative to host).",
    ),
    // scylladb_port
    ParameterSpec::component("port")
        .description("The port of the ScyllaDB server. Defaults to 9042."),
    // scylladb_keyspace
    ParameterSpec::component("keyspace")
        .description("The keyspace to use on the ScyllaDB cluster."),
    // scylladb_user
    ParameterSpec::component("user")
        .description("The username to use to authenticate with ScyllaDB."),
    // scylladb_pass
    ParameterSpec::component("pass")
        .secret()
        .description("The password to use to authenticate with ScyllaDB."),
    // scylladb_datacenter
    ParameterSpec::component("datacenter")
        .description("The datacenter to use for local connection preferences."),
    // scylladb_ssl
    ParameterSpec::component("ssl")
        .description("Whether to use SSL/TLS for the connection. Defaults to false."),
    // connection_timeout
    ParameterSpec::runtime("connection_timeout")
        .description("The connection timeout in milliseconds."),
];

// CQL reserved keywords that may need quoting
const RESERVED_KEYWORDS: &[&str] = &[
    "ADD",
    "ALLOW",
    "ALTER",
    "AND",
    "ANY",
    "APPLY",
    "ASC",
    "AUTHORIZE",
    "BATCH",
    "BEGIN",
    "BY",
    "COLUMNFAMILY",
    "CREATE",
    "DELETE",
    "DESC",
    "DROP",
    "EACH_QUORUM",
    "FROM",
    "GRANT",
    "IF",
    "IN",
    "INDEX",
    "INET",
    "INFINITY",
    "INSERT",
    "INTO",
    "KEYSPACE",
    "KEYSPACES",
    "LIMIT",
    "LOCAL_ONE",
    "LOCAL_QUORUM",
    "MODIFY",
    "NAN",
    "NORECURSIVE",
    "NOT",
    "OF",
    "ON",
    "ONE",
    "ORDER",
    "PASSWORD",
    "PRIMARY",
    "QUORUM",
    "RENAME",
    "REVOKE",
    "SCHEMA",
    "SELECT",
    "SET",
    "TABLE",
    "THREE",
    "TOKEN",
    "TRUNCATE",
    "TTL",
    "TWO",
    "UNLOGGED",
    "UPDATE",
    "USE",
    "USING",
    "WHERE",
    "WITH",
    "WRITETIME",
];

impl DataConnectorFactory for ScyllaDbFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match create_scylladb_connector(params.parameters).await {
                Ok((session, keyspace, compute_context)) => {
                    let pool = ScyllaDbConnectionPool::new(
                        Arc::clone(&session),
                        Arc::clone(&keyspace),
                        compute_context,
                    );
                    let scylladb_factory =
                        ScyllaDbTableFactory::new(Arc::new(pool), session, keyspace);
                    Ok(Arc::new(ScyllaDb { scylladb_factory }) as Arc<dyn DataConnector>)
                }
                Err(e) => {
                    let error = match &e {
                        Error::AuthenticationError => {
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "scylladb".to_string(),
                                connector_component: params.component,
                            }
                        }
                        Error::InvalidHostOrPortError {
                            host,
                            port,
                            source: _,
                        } => DataConnectorError::UnableToConnectInvalidHostOrPort {
                            dataconnector: "scylladb".to_string(),
                            connector_component: params.component,
                            host: host.clone(),
                            port: port.clone(),
                        },
                        _ => DataConnectorError::UnableToConnectInternal {
                            dataconnector: "scylladb".to_string(),
                            connector_component: params.component,
                            source: Box::new(e),
                        },
                    };
                    Err(error.into())
                }
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "scylladb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn reserved_keywords(&self) -> &'static [&'static str] {
        RESERVED_KEYWORDS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "scylladb";

/// Returns a new instance of the `ScyllaDB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    ScyllaDbFactory::new_arc()
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

#[async_trait]
impl DataConnector for ScyllaDb {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.scylladb_factory, dataset.path().into())
                .await
                .context(UnableToGetReadProviderSnafu {
                    dataconnector: "scylladb",
                    connector_component: ConnectorComponent::from(dataset),
                })?,
        )
    }
}

const DEFAULT_PORT: u16 = 9042;
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

async fn create_scylladb_connector(params: Parameters) -> Result<(Arc<Session>, Arc<str>, String)> {
    // Get hosts - can be from 'host' or 'hosts' parameter
    let hosts_str = params
        .get("hosts")
        .expose()
        .ok()
        .or_else(|| params.get("host").expose().ok())
        .ok_or_else(|| Error::MissingRequiredParameter {
            parameter_name: "scylladb_host".to_string(),
        })?;

    let port_str = params.get("port").expose().ok().unwrap_or("9042");

    let port: u16 = port_str.parse().context(InvalidPortValueSnafu)?;

    let keyspace =
        params
            .get("keyspace")
            .expose()
            .ok_or_else(|p| Error::MissingRequiredParameter {
                parameter_name: p.0,
            })?;

    // Parse hosts - can be comma-separated
    let hosts: Vec<&str> = hosts_str.split(',').map(str::trim).collect();

    // Verify connectivity to at least one host
    let mut last_error = None;
    for host in &hosts {
        match verify_ns_lookup_and_tcp_connect(host, port).await {
            Ok(()) => {
                last_error = None;
                break;
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
    }

    if let Some(e) = last_error {
        return Err(Error::InvalidHostOrPortError {
            source: e.into(),
            host: hosts_str.to_string(),
            port: port_str.to_string(),
        });
    }

    // Build session
    let mut builder = SessionBuilder::new();

    // Add known nodes
    for host in &hosts {
        let node_addr = if port == DEFAULT_PORT {
            (*host).to_string()
        } else {
            format!("{host}:{port}")
        };
        builder = builder.known_node(&node_addr);
    }

    // Set keyspace
    builder = builder.use_keyspace(keyspace, false);

    // Set connection timeout
    let connection_timeout =
        if let Some(timeout_str) = params.get("connection_timeout").expose().ok() {
            let timeout_ms: u64 = timeout_str
                .parse()
                .context(InvalidConnectionTimeoutValueSnafu)?;
            Duration::from_millis(timeout_ms)
        } else {
            DEFAULT_CONNECTION_TIMEOUT
        };
    builder = builder.connection_timeout(connection_timeout);

    // Set authentication if provided
    if let Some(user) = params.get("user").expose().ok() {
        let pass = params
            .get("pass")
            .expose()
            .ok()
            .map(ToString::to_string)
            .unwrap_or_default();
        builder = builder.user(user, &pass);
    }

    // Set datacenter if provided
    if let Some(datacenter) = params.get("datacenter").expose().ok() {
        builder = builder.default_execution_profile_handle(
            scylla::client::execution_profile::ExecutionProfile::builder()
                .load_balancing_policy(
                    scylla::policies::load_balancing::DefaultPolicy::builder()
                        .prefer_datacenter(datacenter.to_string())
                        .build(),
                )
                .build()
                .into_handle(),
        );
    }

    // Build the session
    let session = builder.build().await.map_err(|e| {
        // Check if this is an authentication error
        let error_str = e.to_string().to_lowercase();
        if error_str.contains("authentication") || error_str.contains("auth") {
            Error::AuthenticationError
        } else {
            Error::UnableToCreateSession { source: e }
        }
    })?;

    // Create compute context for federation
    let compute_context = format!(
        "scylladb://{}:{}@{}:{}/{}",
        params.get("user").expose().ok().unwrap_or(""),
        "", // Don't include password in compute context
        hosts.first().unwrap_or(&"localhost"),
        port,
        keyspace
    );

    Ok((Arc::new(session), keyspace.into(), compute_context))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_prefix() {
        let factory = ScyllaDbFactory::new();
        assert_eq!(factory.prefix(), "scylladb");
    }

    #[test]
    fn test_factory_parameters() {
        let factory = ScyllaDbFactory::new();
        let params = factory.parameters();

        // Check required parameters are present
        let param_names: Vec<&str> = params.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"host"));
        assert!(param_names.contains(&"keyspace"));
        assert!(param_names.contains(&"user"));
        assert!(param_names.contains(&"pass"));
        assert!(param_names.contains(&"port"));
    }

    #[test]
    fn test_reserved_keywords() {
        let factory = ScyllaDbFactory::new();
        let keywords = factory.reserved_keywords();

        // Check some common CQL keywords
        assert!(keywords.contains(&"SELECT"));
        assert!(keywords.contains(&"FROM"));
        assert!(keywords.contains(&"WHERE"));
        assert!(keywords.contains(&"KEYSPACE"));
    }

    #[test]
    fn test_factory_new_arc() {
        let factory_arc = ScyllaDbFactory::new_arc();
        assert_eq!(factory_arc.prefix(), "scylladb");
    }

    #[test]
    fn test_factory_as_any() {
        let factory = ScyllaDbFactory::new();
        let any_ref = factory.as_any();
        assert!(any_ref.downcast_ref::<ScyllaDbFactory>().is_some());
    }

    #[test]
    fn test_reserved_keywords_comprehensive() {
        let factory = ScyllaDbFactory::new();
        let keywords = factory.reserved_keywords();

        // Verify essential CQL keywords are present
        let essential_keywords = vec![
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "KEYSPACE",
            "TABLE",
            "INDEX",
            "PRIMARY",
            "AND",
            "NOT",
            "IN",
            "ORDER",
            "BY",
            "LIMIT",
            "USING",
            "TTL",
            "WRITETIME",
            "IF",
        ];

        for kw in essential_keywords {
            assert!(keywords.contains(&kw), "Missing essential keyword: {kw}");
        }
    }

    #[test]
    fn test_reserved_keywords_count() {
        let factory = ScyllaDbFactory::new();
        let keywords = factory.reserved_keywords();

        // Ensure we have a reasonable number of keywords
        assert!(
            keywords.len() >= 30,
            "Should have at least 30 reserved keywords"
        );
    }

    #[test]
    fn test_parameter_specs_secret_marking() {
        let factory = ScyllaDbFactory::new();
        let params = factory.parameters();

        // Find the password parameter and verify it's marked as secret
        let pass_param = params.iter().find(|p| p.name == "pass");
        assert!(pass_param.is_some(), "pass parameter should exist");

        let pass_param = pass_param.expect("pass param exists");
        assert!(
            pass_param.secret,
            "pass parameter should be marked as secret"
        );
    }

    #[test]
    fn test_parameter_specs_descriptions() {
        let factory = ScyllaDbFactory::new();
        let params = factory.parameters();

        // All parameters should have descriptions
        for param in params {
            assert!(
                !param.description.is_empty(),
                "Parameter '{}' should have a description",
                param.name
            );
        }
    }

    #[test]
    fn test_parameter_count() {
        let factory = ScyllaDbFactory::new();
        let params = factory.parameters();

        // We should have all expected parameters
        assert!(
            params.len() >= 8,
            "Should have at least 8 parameters (host, hosts, port, keyspace, user, pass, datacenter, ssl, connection_timeout)"
        );
    }

    #[test]
    fn test_alternative_host_parameters() {
        let factory = ScyllaDbFactory::new();
        let params = factory.parameters();

        // Both 'host' and 'hosts' should be valid parameters
        let param_names: Vec<&str> = params.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"host"));
        assert!(param_names.contains(&"hosts"));
    }

    #[test]
    fn test_error_display_messages() {
        // Test MissingRequiredParameter error
        let err = Error::MissingRequiredParameter {
            parameter_name: "scylladb_host".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("scylladb_host"));
        assert!(msg.contains("Missing required parameter"));
        assert!(msg.contains("https://spiceai.org/docs"));

        // Test InvalidPortValue error
        let parse_err = "invalid".parse::<u16>().expect_err("should fail");
        let err = Error::InvalidPortValue { source: parse_err };
        let msg = err.to_string();
        assert!(msg.contains("Invalid port value"));

        // Test AuthenticationError
        let err = Error::AuthenticationError;
        let msg = err.to_string();
        assert!(msg.contains("Authentication failed"));
        assert!(msg.contains("username") || msg.contains("password"));
    }

    #[test]
    fn test_default_port_constant() {
        assert_eq!(DEFAULT_PORT, 9042);
    }

    #[test]
    fn test_default_connection_timeout_constant() {
        assert_eq!(DEFAULT_CONNECTION_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn test_reserved_keywords_case_sensitivity() {
        let factory = ScyllaDbFactory::new();
        let keywords = factory.reserved_keywords();

        // Keywords should be uppercase as that's the CQL convention
        for kw in keywords {
            assert_eq!(kw.to_uppercase(), *kw, "Keyword '{kw}' should be uppercase");
        }
    }

    #[test]
    fn test_factory_clone() {
        let factory1 = ScyllaDbFactory::new();
        let factory2 = factory1;

        // Both should work independently after copy
        assert_eq!(factory1.prefix(), factory2.prefix());
        assert_eq!(factory1.parameters().len(), factory2.parameters().len());
    }

    #[test]
    fn test_factory_default() {
        let factory = ScyllaDbFactory::default();
        assert_eq!(factory.prefix(), "scylladb");
    }
}
