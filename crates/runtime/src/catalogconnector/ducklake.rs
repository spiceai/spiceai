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
    Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams,
    parameters::ExposedParamLookup,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::ducklake::DuckLakeS3Params;
use data_components::ducklake::provider::DuckLakeCatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
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
    ParameterSpec::component("aws_endpoint")
        .description("Custom S3-compatible endpoint URL (e.g. for MinIO).")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3."),
];

fn configure_duckdb_httpfs(
    conn: &duckdb::Connection,
    s3: &DuckLakeS3Params,
) -> Result<(), duckdb::Error> {
    conn.execute("INSTALL httpfs", [])?;
    conn.execute("LOAD httpfs", [])?;

    let has_explicit_creds =
        s3.access_key_id.is_some() || s3.endpoint.is_some() || s3.region.is_some();
    if !has_explicit_creds {
        return Ok(());
    }

    let region = s3.region.as_deref().unwrap_or("us-east-1");
    let use_ssl = !s3.allow_http;

    let mut secret_parts = vec![
        "TYPE s3".to_string(),
        format!("REGION '{}'", region.replace('\'', "''")),
        format!("USE_SSL {use_ssl}"),
    ];

    if let Some(key_id) = &s3.access_key_id {
        secret_parts.push("PROVIDER config".to_string());
        secret_parts.push(format!("KEY_ID '{}'", key_id.replace('\'', "''")));
        if let Some(secret) = &s3.secret_access_key {
            secret_parts.push(format!("SECRET '{}'", secret.replace('\'', "''")));
        } else {
            tracing::warn!(
                "DuckLake: 'aws_access_key_id' provided without 'aws_secret_access_key'. Both must be set for S3 authentication."
            );
        }
    } else {
        secret_parts.push("PROVIDER credential_chain".to_string());
    }

    if let Some(endpoint) = &s3.endpoint {
        let endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        secret_parts.push(format!("ENDPOINT '{}'", endpoint.replace('\'', "''")));
        secret_parts.push("URL_STYLE 'path'".to_string());
    }

    let secret_sql = format!(
        "CREATE OR REPLACE SECRET __ducklake_s3 ({})",
        secret_parts.join(", ")
    );
    conn.execute(&secret_sql, [])?;

    Ok(())
}

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

                let escaped_connection_string = connection_string_for_pool.replace('\'', "''");
                let escaped_catalog_name = catalog_name_for_pool.replace('"', "\"\"");
                let attach_sql = format!(
                    "ATTACH 'ducklake:{escaped_connection_string}' AS \"{escaped_catalog_name}\""
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
            catalog.include.clone(),
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
