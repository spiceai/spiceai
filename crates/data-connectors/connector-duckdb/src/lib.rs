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

//! `DuckDB` data connector for Spice.ai runtime.
//!
//! This crate provides the `DuckDB` connector implementation, allowing
//! Spice.ai to connect to `DuckDB` databases as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use data_components::adbc_helpers::{
    DUCKDB_DRIVER, await_adbc_initialization, driver_entrypoint, load_adbc_database,
    managed_duckdb_options, offload_adbc_drop,
};
use data_connector_api::ConnectorContext;
use data_connector_api::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::sql::sqlparser::{ast::TableFactor, dialect::DuckDbDialect, parser::Parser};
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
use runtime_component::dataset::DatasetSpec;
use runtime_datafusion::dialect::new_duckdb_dialect;
use runtime_datafusion::function_support::deny_spice_functions_for_duckdb_table_providers;
use runtime_parameters::ParameterSpec;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter `duckdb_open`. Set it to the DuckDB database file to read, for example `duckdb_open: ./data.duckdb`. For details, visit: https://spiceai.org/docs/components/data-connectors/duckdb"
    ))]
    MissingDuckDBFile,

    #[snafu(display(
        "Failed to load the installed DuckDB library. Install libduckdb on the system library path or register the 'duckdb' ADBC driver manifest. For details, visit: https://spiceai.org/docs/components/data-connectors/duckdb"
    ))]
    MissingLibrary {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to open DuckDB: {source}"))]
    Initialize {
        source: data_components::adbc_helpers::AdbcInitError,
    },

    #[snafu(display("Failed to initialize DuckDB for {component}: {source}"))]
    Setup {
        component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The DuckDB connection does not support synchronous setup"))]
    MissingSyncConnection,

    #[snafu(display("The DuckDB connector has already been closed"))]
    Closed,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `DuckDB` data connector.
pub struct DuckDB {
    duckdb_factory: Option<AdbcTableFactory<adbc_driver_manager::ManagedDatabase>>,
    function_view: Option<String>,
}

impl std::fmt::Debug for DuckDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDB").finish_non_exhaustive()
    }
}

impl Drop for DuckDB {
    fn drop(&mut self) {
        if let Some(factory) = self.duckdb_factory.take() {
            offload_adbc_drop(factory);
        }
    }
}

/// Recognizes the same bare table-function syntax as the bundled provider.
fn is_table_function(reference: &TableReference) -> bool {
    let TableReference::Bare { table } = reference else {
        return false;
    };
    let dialect = DuckDbDialect {};
    let Ok(mut parser) = Parser::new(&dialect).try_with_sql(table) else {
        return false;
    };
    matches!(
        parser.parse_table_factor(),
        Ok(TableFactor::Table { args: Some(_), .. })
    )
}

/// Encodes every UTF-8 byte, so distinct function text cannot share a view name.
/// The identifier is quoted when used in SQL; no function text enters its name.
fn table_function_view_name(function: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut name = String::from("spice_duckdb_");
    for byte in function.bytes() {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 15)]));
    }
    name.push_str("__view");
    name
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

impl DuckDB {
    async fn create(params: ConnectorParams) -> data_connector_api::NewDataConnectorResult {
        // `unsupported_type_action` is accepted and has nothing to act on. It
        // guarded the bundled DuckDB path, which could not convert nested lists
        // of non-primitive types (and structs holding them) to Arrow. DuckDB's
        // own Arrow export, which ADBC returns, converts every DuckDB type, so
        // no column is unsupported and none is dropped.
        let path = params
            .parameters
            .get("open")
            .expose()
            .ok()
            .map(str::to_owned);
        let function = match &params.component {
            ConnectorComponent::Dataset(dataset) => {
                let reference = TableReference::from(dataset.path());
                is_table_function(&reference).then(|| reference.table().to_owned())
            }
            ConnectorComponent::Catalog(_) => None,
        };
        let function_view = function.as_deref().map(table_function_view_name);
        let view = function_view.clone();
        let component = params.component.clone();
        let init = tokio::task::spawn_blocking(
            move || -> Result<Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>> {
                // One database owns every pooled connection, including memory views
                // and attachments. File sources open their database read-only.
                let database_path = if function.is_some() {
                    ":memory:"
                } else {
                    path.as_deref().unwrap_or(":memory:")
                };
                let db = load_adbc_database(
                    DUCKDB_DRIVER,
                    driver_entrypoint(DUCKDB_DRIVER),
                    managed_duckdb_options(database_path, database_path != ":memory:"),
                )
                .map_err(|source| match source {
                    source @ data_components::adbc_helpers::AdbcInitError::LoadDriver { .. } => {
                        Error::MissingLibrary {
                            source: Box::new(source),
                        }
                    }
                    source => Error::Initialize { source },
                })?;
                data_components::adbc_helpers::check_duckdb_version(&db, "duckdb").map_err(
                    |source| Error::Setup {
                        component: component.clone(),
                        source: Box::new(source),
                    },
                )?;
                let joins = if database_path == ":memory:" {
                    JoinPushDown::Disallow
                } else {
                    JoinPushDown::AllowedFor(format!("duckdb:file:{database_path}"))
                };
                let setup =
                    || -> std::result::Result<_, Box<dyn std::error::Error + Send + Sync>> {
                        // Only file databases with the same path share a SQL namespace.
                        // Memory views and attachments belong to this database alone.
                        let pool = Arc::new(
                            AdbcConnectionPoolBuilder::new(db)
                                .with_join_push_down(joins)
                                .build()?,
                        );
                        if let (Some(function), Some(view)) = (function, view) {
                            let connection = Arc::clone(&pool).connect_sync()?;
                            let connection =
                                connection.as_sync().context(MissingSyncConnectionSnafu)?;
                            if let Some(path) = path.filter(|path| path != ":memory:") {
                                let alias = std::path::Path::new(&path)
                                    .file_stem()
                                    .and_then(std::ffi::OsStr::to_str)
                                    .unwrap_or("source");
                                connection.execute(
                                    &format!(
                                        "ATTACH '{}' AS {} (READ_ONLY)",
                                        path.replace('\'', "''"),
                                        quote_identifier(alias)
                                    ),
                                    &[],
                                )?;
                            }
                            connection.execute(
                                &format!(
                                    "CREATE VIEW memory.main.{} AS SELECT * FROM {function}",
                                    quote_identifier(&view)
                                ),
                                &[],
                            )?;
                        }
                        Ok(pool)
                    };
                setup().map_err(|source| Error::Setup { component, source })
            },
        );
        let pool = await_adbc_initialization(init).await.map_err(|source| {
            DataConnectorError::UnableToConnectInternal {
                dataconnector: "duckdb".into(),
                connector_component: params.component,
                source: Box::new(source),
            }
        })?;
        #[expect(
            clippy::disallowed_methods,
            reason = "The DuckDB function policy is installed immediately on this factory"
        )]
        let factory = AdbcTableFactory::new(pool)
            .with_function_support(deny_spice_functions_for_duckdb_table_providers());
        Ok(Arc::new(Self {
            duckdb_factory: Some(factory),
            function_view,
        }) as Arc<dyn DataConnector>)
    }
}

/// Factory for creating `DuckDB` connector instances.
#[derive(Default, Copy, Clone)]
pub struct DuckDBFactory {}

impl DuckDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const DUCKDB_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/duckdb";

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::component("open")
    .description(
        "Path to the DuckDB database file to open. Use ':memory:' for an in-memory database.",
    )
    .examples(&["/var/lib/spice/analytics.db", ":memory:"])
    .help_link(DUCKDB_DOCS)];

impl DataConnectorFactory for DuckDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = data_connector_api::NewDataConnectorResult> + Send + 'a>> {
        Box::pin(DuckDB::create(params))
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "duckdb";

/// Returns a new instance of the `DuckDB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    DuckDBFactory::new_arc()
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
impl DataConnector for DuckDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path: TableReference = dataset.path().into();

        if !(is_table_function(&path) || dataset.params.contains_key("duckdb_open")) {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "duckdb".to_string(),
                source: Box::new(Error::MissingDuckDBFile {}),
                connector_component: ConnectorComponent::from(dataset),
            });
        }

        let path = self
            .function_view
            .as_ref()
            .map_or(path, |view| TableReference::bare(view.clone()));
        let factory = self
            .duckdb_factory
            .as_ref()
            .context(ClosedSnafu)
            .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "duckdb".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(source),
            })?;
        Ok(factory
            .table_provider(path, Some(new_duckdb_dialect()))
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "duckdb",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_duckdb as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_duckdb_connector,
    DUCKDB_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    DuckDBFactory
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_function_names_preserve_every_byte() {
        // DTP's flattening convention ends in __view but drops punctuation.
        // Encoding keeps even paths differing only by punctuation distinct.
        assert_eq!(
            table_function_view_name("f('a-b')"),
            "spice_duckdb_662827612d622729__view"
        );
        assert_eq!(
            table_function_view_name("f('ab')"),
            "spice_duckdb_66282761622729__view"
        );
        assert_eq!(
            table_function_view_name("f('雪')"),
            "spice_duckdb_662827e99baa2729__view"
        );
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn recognizes_bare_table_functions() {
        for (name, expected) in [
            ("t", false),
            ("read_parquet('a.b.parquet')", true),
            ("read_csv_auto('x.csv', header=true)", true),
        ] {
            assert_eq!(
                is_table_function(&TableReference::from(name)),
                expected,
                "{name}"
            );
        }
        assert!(!is_table_function(&TableReference::partial("main", "f()")));
    }

    #[test]
    fn missing_library_error_names_the_installation_fix() {
        assert_eq!(
            Error::MissingLibrary {
                source: "driver not found".into(),
            }
            .to_string(),
            "Failed to load the installed DuckDB library. Install libduckdb on the system library path or register the 'duckdb' ADBC driver manifest. For details, visit: https://spiceai.org/docs/components/data-connectors/duckdb"
        );
    }

    #[test]
    fn missing_file_error_names_the_spicepod_key() {
        assert_eq!(
            Error::MissingDuckDBFile.to_string(),
            "Missing required parameter `duckdb_open`. Set it to the DuckDB database file to read, for example `duckdb_open: ./data.duckdb`. For details, visit: https://spiceai.org/docs/components/data-connectors/duckdb"
        );
    }
}
