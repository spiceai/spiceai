/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{fmt::Write, sync::Arc};

use datafusion::sql::TableReference;
use serde::Deserialize;
use snafu::prelude::*;
use url::Url;

use token_provider::TokenProvider;
use tracing::Instrument;

use crate::resilient_http::{configure_client_builder, send_request_with_retry};

pub mod provider;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/catalogs/unity-catalog#configuration"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "Failed to connect to the Unity Catalog API. Check the Unity Catalog API endpoint is valid and accessible. The following connection error occurred: {source}"
    ))]
    ConnectionError { source: reqwest::Error },

    #[snafu(display(
        "Failed to connect to the Unity Catalog API. Check the Unity Catalog API endpoint is valid and accessible. The following HTTP status code was received when connecting: {status}"
    ))]
    UnexpectedStatusCode { status: reqwest::StatusCode },

    #[snafu(display(
        "Expected a valid URL, but '{url}' was provided. For details, visit: https://spiceai.org/docs/components/catalogs/unity-catalog#configuration"
    ))]
    URLParseError {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "An invalid catalog URL was provided: '{url}'. Expected a catalog URL in the format of: 'https://<host>/api/2.1/unity-catalog/catalogs/<catalog_id>'",
    ))]
    InvalidCatalogURL { url: String },

    #[snafu(display(
        "Failed to find the catalog with ID '{catalog_id}'. Verify the catalog exists, and try again."
    ))]
    CatalogDoesntExist { catalog_id: String },

    #[snafu(display(
        "Failed to find the schema '{schema}' in the catalog '{catalog_id}'. Verify the schema and catalog exist, and try again."
    ))]
    SchemaDoesntExist { schema: String, catalog_id: String },

    #[snafu(display("Failed to get token. {source}"))]
    UnableToGetToken { source: token_provider::Error },

    #[snafu(display("Failed to create HTTP client for Unity Catalog: {source}"))]
    UnableToCreateHttpClient { source: reqwest::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An ergonomic wrapper around calling Unity Catalog APIs.
///
/// Could be replaced once <https://crates.io/crates/unitycatalog-client> is available.
#[derive(Debug)]
pub struct UnityCatalog {
    endpoint: String,
    token_provider: Option<Arc<dyn TokenProvider>>,
    client: reqwest::Client,
    user_agent: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Endpoint(pub String);

#[derive(Debug, Clone)]
pub struct CatalogId(pub String);

impl UnityCatalog {
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(endpoint: Endpoint, token_provider: Option<Arc<dyn TokenProvider>>) -> Result<Self> {
        let mut endpoint_str = endpoint.0.trim_end_matches('/').to_string();
        if !endpoint_str.starts_with("http") {
            endpoint_str = format!("https://{endpoint_str}");
        }

        let user_agent: Option<String>;
        #[cfg(not(feature = "databricks"))]
        {
            user_agent = None;
        }
        #[cfg(feature = "databricks")]
        // Include user_agent, if connects to Databricks instance
        {
            user_agent = if endpoint.0.contains("databricks") {
                Some(crate::databricks::user_agent().to_string())
            } else {
                None
            };
        }

        let client = configure_client_builder(reqwest::Client::builder())
            .user_agent(util::spiceai_user_agent())
            .build()
            .context(UnableToCreateHttpClientSnafu)?;

        Ok(Self {
            endpoint: endpoint_str,
            token_provider,
            client,
            user_agent,
        })
    }

    /// Parses a catalog url into the endpoint and catalog id.
    ///
    /// Example:
    ///
    /// `https://dbc-f34ee0b7-90f2.cloud.databricks.com/api/2.1/unity-catalog/catalogs/spiceai_sandbox`
    ///
    /// Returns `("https://dbc-f34ee0b7-90f2.cloud.databricks.com", "spiceai_sandbox")`
    pub fn parse_catalog_url(url: &str) -> Result<(Endpoint, CatalogId)> {
        let url = url.trim_end_matches('/');
        let parsed_url = url.parse::<Url>().context(URLParseSnafu {
            url: url.to_string(),
        })?;

        // Extract the endpoint
        let mut endpoint = format!(
            "{}://{}",
            parsed_url.scheme(),
            parsed_url
                .host_str()
                .map(|s| s.trim_end_matches('/'))
                .context(InvalidCatalogURLSnafu {
                    url: url.to_string()
                })?
        );

        if let Some(port) = parsed_url.port() {
            let _ = write!(endpoint, ":{port}");
        }

        tracing::debug!("parse_catalog_url: endpoint: {}", endpoint);

        // Extract the catalog id from the path segments
        let mut path_segments = parsed_url.path_segments().context(InvalidCatalogURLSnafu {
            url: url.to_string(),
        })?;

        let mut parse_expected_segment = |expected_segment: &str| {
            ensure!(
                path_segments.next() == Some(expected_segment),
                InvalidCatalogURLSnafu {
                    url: url.to_string()
                }
            );
            Ok(())
        };

        parse_expected_segment("api")?;
        parse_expected_segment("2.1")?;
        parse_expected_segment("unity-catalog")?;
        parse_expected_segment("catalogs")?;

        // The catalog ID is the last segment in the path
        let catalog_id = path_segments.next().context(InvalidCatalogURLSnafu {
            url: url.to_string(),
        })?;

        Ok((Endpoint(endpoint), CatalogId(catalog_id.to_string())))
    }

    pub async fn get_table(&self, table_reference: &TableReference) -> Result<Option<UCTable>> {
        let table_name = table_reference.to_string();
        let path = format!("/api/2.1/unity-catalog/tables/{table_name}");
        async {
            let response = self.send_get_with_retry("get table", &path).await?;

            if response.status().is_success() {
                let api_response: UCTable = response.json().await.context(ConnectionSnafu)?;
                Ok(Some(api_response))
            } else if response.status().as_u16() == 404 {
                Ok(None)
            } else {
                UnexpectedStatusCodeSnafu {
                    status: response.status(),
                }
                .fail()
            }
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "uc_get_table",
            input = %table_name,
        ))
        .await
    }

    pub async fn get_catalog(&self, catalog_id: &str) -> Result<Option<UCCatalog>> {
        let path = format!("/api/2.1/unity-catalog/catalogs/{catalog_id}");
        async {
            let response = self.send_get_with_retry("get catalog", &path).await?;

            tracing::debug!("get_catalog: Response status: {}", response.status());

            if response.status().is_success() {
                let api_response: UCCatalog = response.json().await.context(ConnectionSnafu)?;
                Ok(Some(api_response))
            } else if response.status().as_u16() == 404 {
                Ok(None)
            } else {
                UnexpectedStatusCodeSnafu {
                    status: response.status(),
                }
                .fail()
            }
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "uc_get_catalog",
            input = catalog_id,
        ))
        .await
    }

    pub async fn list_schemas(&self, catalog_id: &str) -> Result<Option<Vec<UCSchema>>> {
        let path = format!("/api/2.1/unity-catalog/schemas?catalog_name={catalog_id}");
        async {
            let response = self.send_get_with_retry("list schemas", &path).await?;

            tracing::debug!("list_schemas: Response status: {}", response.status());

            if response.status().is_success() {
                let api_response: UCSchemaEnvelope =
                    response.json().await.context(ConnectionSnafu)?;
                Ok(Some(api_response.schemas))
            } else if response.status().as_u16() == 404 {
                Ok(None)
            } else {
                UnexpectedStatusCodeSnafu {
                    status: response.status(),
                }
                .fail()
            }
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "uc_list_schemas",
            input = catalog_id,
        ))
        .await
    }

    pub async fn list_tables(
        &self,
        catalog_id: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<UCTable>>> {
        let path = format!(
            "/api/2.1/unity-catalog/tables?catalog_name={catalog_id}&schema_name={schema_name}"
        );
        async {
            let response = self.send_get_with_retry("list tables", &path).await?;

            tracing::debug!("list_tables: Response status: {}", response.status());

            if response.status().is_success() {
                let api_response: UCTableEnvelope =
                    response.json().await.context(ConnectionSnafu)?;
                Ok(Some(api_response.tables))
            } else if response.status().as_u16() == 404 {
                Ok(None)
            } else {
                UnexpectedStatusCodeSnafu {
                    status: response.status(),
                }
                .fail()
            }
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "uc_list_tables",
            input = %format!("{catalog_id}.{schema_name}"),
        ))
        .await
    }

    /// Fetches the effective permissions for a table from the UC API.
    ///
    /// Returns `Ok(None)` if the table is not found (404).
    /// The `full_name` should be in `catalog.schema.table` format.
    pub async fn get_effective_permissions(
        &self,
        full_name: &str,
    ) -> Result<Option<UCPermissionsEnvelope>> {
        let path = format!("/api/2.1/unity-catalog/effective-permissions/table/{full_name}");
        async {
            let response = self
                .send_get_with_retry("get effective permissions", &path)
                .await?;

            if response.status().is_success() {
                let envelope: UCPermissionsEnvelope =
                    response.json().await.context(ConnectionSnafu)?;
                Ok(Some(envelope))
            } else if response.status().as_u16() == 404 {
                Ok(None)
            } else {
                UnexpectedStatusCodeSnafu {
                    status: response.status(),
                }
                .fail()
            }
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "uc_get_effective_permissions",
            input = full_name,
        ))
        .await
    }

    fn get_req(&self, path: &str) -> reqwest::RequestBuilder {
        let full_url = format!("{}{path}", self.endpoint);
        tracing::debug!("Sending request to {full_url}");
        let mut builder = self.client.get(full_url);

        if let Some(token_provider) = &self.token_provider {
            tracing::debug!("Adding bearer token to request");
            builder = builder.bearer_auth(token_provider.get_token());
        }
        if let Some(user_agent) = &self.user_agent {
            builder = builder.header("User-Agent", user_agent);
        }

        builder
    }

    async fn send_get_with_retry(&self, operation: &str, path: &str) -> Result<reqwest::Response> {
        send_request_with_retry("Unity Catalog", operation, || self.get_req(path))
            .await
            .context(ConnectionSnafu)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UCTableEnvelope {
    #[serde(default)]
    pub tables: Vec<UCTable>,
}

/// Response from `/api/2.1/unity-catalog/tables/{table_name}`
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct UCTable {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(default)]
    pub table_type: String,
    #[serde(default)]
    pub data_source_format: String,
    #[serde(default)]
    pub columns: Vec<UCColumn>,
    #[serde(default)]
    pub storage_location: Option<String>,
}

impl UCTable {
    /// Returns the fully qualified name of the table: `catalog.schema.table`.
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog_name, self.schema_name, self.name)
    }

    /// Returns the parsed [`UCTableType`] for this table.
    #[must_use]
    pub fn parsed_table_type(&self) -> UCTableType {
        UCTableType::from(self.table_type.as_str())
    }

    /// Returns `true` if the table type is supported for direct querying
    /// through the SQL Warehouse or Spark Connect connectors.
    ///
    /// `VIEW` and `STREAMING_TABLE` types are not supported.
    #[must_use]
    pub fn is_queryable(&self) -> bool {
        self.parsed_table_type().is_queryable()
    }
}

/// Databricks Unity Catalog table types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UCTableType {
    Managed,
    External,
    View,
    MaterializedView,
    StreamingTable,
    /// An unrecognised value from the API.
    Unknown,
}

impl UCTableType {
    /// Returns `true` if tables of this type can be queried directly.
    #[must_use]
    pub const fn is_queryable(self) -> bool {
        matches!(
            self,
            Self::Managed | Self::External | Self::MaterializedView
        )
    }
}

impl From<&str> for UCTableType {
    fn from(s: &str) -> Self {
        match s {
            "MANAGED" => Self::Managed,
            "EXTERNAL" => Self::External,
            "VIEW" => Self::View,
            "MATERIALIZED_VIEW" => Self::MaterializedView,
            "STREAMING_TABLE" => Self::StreamingTable,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct UCColumn {
    pub name: String,
    pub type_text: String,
    pub type_name: String,
    pub position: i64,
    pub type_precision: i64,
    pub type_scale: i64,
    pub type_json: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UCCatalog {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UCSchemaEnvelope {
    #[serde(default)]
    pub schemas: Vec<UCSchema>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UCSchema {
    pub name: String,
    pub catalog_name: String,
}

// ============================================================================
// Permissions
// ============================================================================

/// Response from `/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`.
#[derive(Debug, Clone, Deserialize)]
pub struct UCPermissionsEnvelope {
    #[serde(default)]
    pub privilege_assignments: Vec<UCPrivilegeAssignment>,
}

/// A single privilege assignment returned by the UC permissions endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct UCPrivilegeAssignment {
    pub principal: String,
    #[serde(default)]
    pub privileges: Vec<UCPrivilege>,
}

/// A single privilege entry.
#[derive(Debug, Clone, Deserialize)]
pub struct UCPrivilege {
    pub privilege: String,
}

/// The subset of UC privileges relevant to read operations.
const READ_PRIVILEGES: &[&str] = &["SELECT", "ALL_PRIVILEGES", "ALL PRIVILEGES"];

impl UCPermissionsEnvelope {
    /// Returns `true` if any principal in this response has a read-compatible
    /// privilege (`SELECT` or `ALL_PRIVILEGES`).
    #[must_use]
    pub fn has_read_permission(&self) -> bool {
        self.privilege_assignments.iter().any(|pa| {
            pa.privileges
                .iter()
                .any(|p| READ_PRIVILEGES.contains(&p.privilege.as_str()))
        })
    }
}
