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

//! `DuckLake` catalog connector.
//!
//! Connects to a `DuckLake` catalog using `DuckDB` with the `ducklake` extension
//! and provides schema/table discovery.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime,
    component::catalog::{Catalog, table_selector},
    dataconnector::parameters::ConnectorParams,
    parameters::ExposedParamLookup,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::ducklake::provider::{DuckLakeCatalogProvider, DuckLakeFederation};
use data_components::ducklake::{
    DuckLakeS3Params, build_ducklake_attach_sql, configure_duckdb_httpfs,
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use runtime_datafusion::dialect::new_duckdb_dialect;
use runtime_datafusion::function_support::deny_spice_functions_for_duckdb_table_providers;
use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "ducklake";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/catalogs/ducklake"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display("Failed to initialize DuckLake: {source}"))]
    UnableToInitializeDuckLake { source: duckdb::Error },
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description("The DuckLake connection string (e.g., 's3://bucket/path/metadata.ducklake'). If omitted, the catalog id from `from: ducklake:<connection_string>` is used."),
    ParameterSpec::component("name")
        .description("The name to attach the DuckLake catalog as in DuckDB. Defaults to 'ducklake'."),
    ParameterSpec::component("open")
        .description("Optional path to an existing `DuckDB` file. If not provided, an in-memory `DuckDB` is used."),
    ParameterSpec::component("aws_region")
        .description("The AWS region for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_session_token")
        .description(
            "The AWS session token for S3 storage. Required with temporary (STS) credentials.",
        )
        .secret(),
    ParameterSpec::component("aws_endpoint")
        .description("Custom S3-compatible endpoint URL (e.g. for MinIO).")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3."),
    ParameterSpec::component("automatic_migration").description(
        "Automatically migrate an older DuckLake catalog schema to the version required by the ducklake extension on attach. Defaults to false; migration rewrites catalog metadata and cannot be undone.",
    ),
];

/// A catalog connector for `DuckLake`, providing access to schemas and tables via `DuckDB`.
#[derive(Clone)]
pub struct DuckLakeCatalog {
    params: ConnectorParams,
}

impl DuckLakeCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let connection_string: String =
            match self.params.parameters.get("connection_string").expose() {
                ExposedParamLookup::Present(value) => value.to_string(),
                ExposedParamLookup::Absent(parameter) => {
                    if let Some(catalog_id) = catalog.catalog_id.as_ref() {
                        if catalog_id.is_empty() {
                            let e = Error::MissingParameter {
                                parameter: parameter.to_string(),
                            };
                            return Err(super::Error::InvalidConfigurationNoSource {
                                connector: PREFIX.to_string(),
                                connector_component,
                                message: e.to_string(),
                            });
                        }
                        catalog_id.clone()
                    } else {
                        let e = Error::MissingParameter {
                            parameter: parameter.to_string(),
                        };
                        return Err(super::Error::InvalidConfigurationNoSource {
                            connector: PREFIX.to_string(),
                            connector_component,
                            message: e.to_string(),
                        });
                    }
                }
            };

        let catalog_name = self
            .params
            .parameters
            .get("name")
            .expose()
            .ok()
            .map_or_else(|| "ducklake".to_string(), ToString::to_string);

        let open_path = self
            .params
            .parameters
            .get("open")
            .expose()
            .ok()
            .map(ToString::to_string);

        let automatic_migration = self
            .params
            .parameters
            .get("automatic_migration")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Get the catalog's access mode to determine writable/ddl_enabled flags
        let writable = catalog.access.allows_write();
        let ddl_enabled = catalog.access.allows_ddl();

        // Use the appropriate `DuckDB` access mode based on catalog permissions
        let duckdb_access_mode = if writable {
            AccessMode::ReadWrite
        } else {
            AccessMode::ReadOnly
        };

        let s3_params = DuckLakeS3Params {
            region: self
                .params
                .parameters
                .get("aws_region")
                .expose()
                .ok()
                .map(ToString::to_string),
            access_key_id: self
                .params
                .parameters
                .get("aws_access_key_id")
                .expose()
                .ok()
                .map(ToString::to_string),
            secret_access_key: self
                .params
                .parameters
                .get("aws_secret_access_key")
                .expose()
                .ok()
                .map(ToString::to_string),
            session_token: self
                .params
                .parameters
                .get("aws_session_token")
                .expose()
                .ok()
                .map(ToString::to_string),
            endpoint: self
                .params
                .parameters
                .get("aws_endpoint")
                .expose()
                .ok()
                .map(ToString::to_string),
            allow_http: self
                .params
                .parameters
                .get("aws_allow_http")
                .expose()
                .ok()
                .is_some_and(|v| v == "true"),
        };

        let connection_string_for_pool = connection_string;
        let catalog_name_for_pool = catalog_name.clone();
        let connector_component_for_pool = connector_component.clone();

        // Blocking DuckDB setup is isolated from the async runtime thread.
        let pool =
            tokio::task::spawn_blocking(move || -> super::Result<Arc<DuckDbConnectionPool>> {
                let pool = if let Some(path) = open_path.as_deref() {
                    Arc::new(
                        DuckDbConnectionPool::new_file(path, &duckdb_access_mode).map_err(|e| {
                            super::Error::UnableToGetCatalogProvider {
                                connector: PREFIX.to_string(),
                                connector_component: connector_component_for_pool.clone(),
                                source: e,
                            }
                        })?,
                    )
                } else {
                    Arc::new(DuckDbConnectionPool::new_memory().map_err(|e| {
                        super::Error::UnableToGetCatalogProvider {
                            connector: PREFIX.to_string(),
                            connector_component: connector_component_for_pool.clone(),
                            source: e,
                        }
                    })?)
                };

                let conn = Arc::clone(&pool).connect_sync().map_err(|e| {
                    super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: e,
                    }
                })?;

                let duckdb_wrapper = conn
                    .as_any()
                    .downcast_ref::<DuckDbConnection>()
                    .ok_or_else(|| super::Error::InvalidConfigurationNoSource {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        message: "Failed to get underlying DuckDB connection".to_string(),
                    })?;

                duckdb_wrapper
                    .conn
                    .execute("INSTALL ducklake", [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: Box::new(e),
                    })?;

                duckdb_wrapper
                    .conn
                    .execute("LOAD ducklake", [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: Box::new(e),
                    })?;

                configure_duckdb_httpfs(&duckdb_wrapper.conn, &s3_params)
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: Box::new(e),
                    })?;

                let attach_sql = build_ducklake_attach_sql(
                    &connection_string_for_pool,
                    &catalog_name_for_pool,
                    automatic_migration,
                );
                duckdb_wrapper
                    .conn
                    .execute(&attach_sql, [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool,
                        source: Box::new(e),
                    })?;

                Ok(pool)
            })
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })??;

        // Create the catalog provider with the pool (which has ducklake extension and catalog attached)
        let catalog_provider = Arc::new(DuckLakeCatalogProvider::new(
            pool,
            catalog_name,
            writable,
            ddl_enabled,
            table_selector(catalog),
            ducklake_federation(),
        ));

        // Initial refresh to populate schemas and tables
        catalog_provider
            .refresh()
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component,
                source: e,
            })?;

        Ok(catalog_provider as Arc<dyn RefreshableCatalogProvider>)
    }
}

/// The dialect and deny-list a `DuckLake` catalog federates with.
///
/// `DuckLake` is `DuckDB`, so there is one correct answer here and it is built in
/// one place: the deny-list carves out exactly the functions
/// [`new_duckdb_dialect`] rewrites into native `DuckDB` SQL, and both are derived
/// from the same override list. See #13664.
fn ducklake_federation() -> DuckLakeFederation {
    DuckLakeFederation {
        dialect: new_duckdb_dialect(),
        function_support: deny_spice_functions_for_duckdb_table_providers(),
    }
}

#[cfg(test)]
mod tests {
    use super::PARAMETERS;
    use crate::parameters::Parameters;
    use crate::secrets::Secrets;
    use secrecy::{ExposeSecret, SecretString};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// The S3 secret built for `DuckDB` only carries `SESSION_TOKEN` when the parameter
    /// survives `Parameters::try_new` — otherwise temporary (STS) credentials fail with
    /// `InvalidAccessKeyId`.
    #[tokio::test]
    async fn ducklake_aws_session_token_is_accepted() {
        let parameters = Parameters::try_new(
            "catalog ducklake",
            vec![(
                "ducklake_aws_session_token".to_string(),
                SecretString::from("FwoSessionToken"),
            )],
            "ducklake",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("session token should be accepted for ducklake");

        assert_eq!(
            parameters
                .to_secret_map()
                .get("aws_session_token")
                .map(ExposeSecret::expose_secret),
            Some("FwoSessionToken")
        );
    }
}

#[cfg(test)]
mod federation_tests {
    use super::*;
    use crate::catalogconnector::stub_udf;
    use datafusion::prelude::col;
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::Dialect as _;
    use runtime_datafusion::dialect::duckdb_native_function_names;

    /// A `DuckLake` catalog must deny the Spice-only UDFs `DuckDB` cannot run, so
    /// `DataFusion` evaluates them locally instead of unparsing them into the
    /// statement sent to `DuckDB`. See issue #13664.
    #[test]
    fn the_catalog_denies_the_spice_functions_duckdb_cannot_run() {
        let support = ducklake_federation().function_support;
        assert!(
            !support.supports(&stub_udf("json_get_str", 2)),
            "json_get_str must be denied so federation falls back to local DataFusion"
        );
        assert!(
            support.supports(&stub_udf("upper", 1)),
            "a non-Spice function like upper() must still federate"
        );
    }

    /// The half of the pairing a deny-list alone cannot express: every name the
    /// carve-out *allows* through must be one the dialect this catalog installs
    /// actually has a handler for. Pair the `DuckDB`-flavored deny-list with the
    /// stock dialect and `cosine_distance` federates and is then emitted
    /// verbatim -- the unknown-function failure the deny-list exists to prevent,
    /// reached through the functions it deliberately allowed.
    ///
    /// `scalar_function_to_sql_overrides` answers `Ok(None)` exactly when the
    /// dialect has no handler for the name, which is what the stock dialect
    /// returns for all of these. `Err` still means a handler ran, so it counts as
    /// installed here; whether a handler may refuse a call shape at all is #13665.
    #[test]
    fn every_carved_out_function_is_one_the_installed_dialect_handles() {
        let federation = ducklake_federation();
        let unparser = Unparser::new(federation.dialect.as_ref());

        let carved_out = duckdb_native_function_names();
        assert!(
            !carved_out.is_empty(),
            "the carve-out list must not be empty, or this test proves nothing"
        );

        for name in carved_out {
            let expr = stub_udf(name, 2);
            assert!(
                federation.function_support.supports(&expr),
                "{name} is rewritten by the dialect, so the deny-list must carve it out"
            );
            let args = [col("c0"), col("c1")];
            let handled = !matches!(
                federation
                    .dialect
                    .scalar_function_to_sql_overrides(&unparser, name, &args),
                Ok(None)
            );
            assert!(
                handled,
                "the deny-list lets {name} federate, so the installed dialect must have a \
                 handler for it -- the stock DuckDB dialect does not, and would emit it verbatim"
            );
        }
    }
}
