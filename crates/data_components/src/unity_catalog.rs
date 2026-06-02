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
use runtime_rate_control::RateController;
use serde::Deserialize;
use snafu::prelude::*;
use tokio::sync::Semaphore;
use url::Url;

use token_provider::TokenProvider;
use tracing::Instrument;

use crate::resilient_http::{
    RetryConfig, configure_client_builder, send_request_with_retry_and_concurrency_limit,
};

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

    #[snafu(display("Failed to acquire Unity Catalog rate-control permit: {source}"))]
    RateControl { source: runtime_rate_control::Error },
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
    request_semaphore: Option<Arc<Semaphore>>,
    rate_controller: Option<Arc<RateController>>,
}

#[derive(Debug, Clone)]
pub struct Endpoint(pub String);

#[derive(Debug, Clone)]
pub struct CatalogId(pub String);

impl UnityCatalog {
    pub fn new(
        endpoint: Endpoint,
        token_provider: Option<Arc<dyn TokenProvider>>,
        request_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Self> {
        Self::new_with_rate_controller(endpoint, token_provider, request_semaphore, None)
    }

    pub fn new_with_rate_controller(
        endpoint: Endpoint,
        token_provider: Option<Arc<dyn TokenProvider>>,
        request_semaphore: Option<Arc<Semaphore>>,
        rate_controller: Option<Arc<RateController>>,
    ) -> Result<Self> {
        let Endpoint(endpoint) = endpoint;
        let mut endpoint_str = endpoint.trim_end_matches('/').to_string();
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
            user_agent = if endpoint.contains("databricks") {
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
            request_semaphore,
            rate_controller,
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
        let encoded = Self::encode_uc_name(&table_name);
        let path = format!("/api/2.1/unity-catalog/tables/{encoded}");
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
        let encoded_catalog =
            percent_encoding::utf8_percent_encode(catalog_id, percent_encoding::NON_ALPHANUMERIC);
        let path = format!("/api/2.1/unity-catalog/schemas?catalog_name={encoded_catalog}");
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
        let encoded_catalog =
            percent_encoding::utf8_percent_encode(catalog_id, percent_encoding::NON_ALPHANUMERIC);
        let encoded_schema =
            percent_encoding::utf8_percent_encode(schema_name, percent_encoding::NON_ALPHANUMERIC);
        let path = format!(
            "/api/2.1/unity-catalog/tables?catalog_name={encoded_catalog}&schema_name={encoded_schema}"
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
        let encoded = Self::encode_uc_name(full_name);
        let path = format!("/api/2.1/unity-catalog/effective-permissions/table/{encoded}");
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
        let rate_controller_permit = self.acquire_rate_controller_permit().await?;
        let response = send_request_with_retry_and_concurrency_limit(
            "Unity Catalog",
            operation,
            || self.get_req(path),
            &RetryConfig {
                concurrency_limit: self.request_semaphore.as_deref(),
                ..RetryConfig::default()
            },
        )
        .await
        .context(ConnectionSnafu)?;
        drop(rate_controller_permit);
        Ok(response)
    }

    async fn acquire_rate_controller_permit(&self) -> Result<Option<runtime_rate_control::Permit>> {
        let Some(rate_controller) = &self.rate_controller else {
            return Ok(None);
        };

        rate_controller
            .acquire()
            .await
            .context(RateControlSnafu)
            .map(Some)
    }

    /// Percent-encodes each dot-separated segment of a UC name individually.
    ///
    /// UC table names are `catalog.schema.table` where dots are separators.
    /// Each segment is encoded but dots are preserved so the API receives
    /// the correct path structure.
    fn encode_uc_name(name: &str) -> String {
        name.split('.')
            .map(|segment| {
                percent_encoding::utf8_percent_encode(segment, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Runs an advisory permission check and logs the result.
    ///
    /// This never blocks initialization or filters tables — it only produces
    /// diagnostic log output so operators can identify likely access issues
    /// before queries hit Databricks at runtime.
    pub async fn log_advisory_permission_check(&self, table_name: &str, context: &str) {
        match self.get_effective_permissions(table_name).await {
            Ok(Some(perms)) => {
                if perms.has_read_permission() {
                    tracing::debug!(
                        table = %table_name,
                        principals = ?perms.principals(),
                        "Unity Catalog permission check passed"
                    );
                } else {
                    tracing::warn!(
                        table = %table_name,
                        "Unity Catalog effective-permissions did not report a read-compatible privilege during {context}; proceeding and deferring to Databricks query-time validation"
                    );
                    tracing::debug!(
                        table = %table_name,
                        principals = ?perms.principals(),
                        privileges = ?perms.all_privileges(),
                        "Permission denial details"
                    );
                }
            }
            Ok(None) => {
                tracing::debug!(
                    table = %table_name,
                    "Table not found when checking permissions; proceeding"
                );
            }
            Err(e) => {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "Failed to check Unity Catalog permissions; proceeding without validation"
                );
            }
        }
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

    /// Returns `true` when the table should be rejected up front if UC does
    /// not report a read-compatible privilege.
    #[must_use]
    pub fn requires_read_permission_validation(&self) -> bool {
        self.parsed_table_type()
            .requires_read_permission_validation()
    }
}

/// Databricks Unity Catalog table types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UCTableType {
    Managed,
    External,
    Foreign,
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
            Self::Managed | Self::External | Self::Foreign | Self::MaterializedView
        )
    }

    /// Returns `true` when UC effective-permissions is authoritative enough to
    /// reject access up front.
    #[must_use]
    pub const fn requires_read_permission_validation(self) -> bool {
        !matches!(self, Self::Foreign)
    }
}

impl From<&str> for UCTableType {
    fn from(s: &str) -> Self {
        match s {
            "MANAGED" => Self::Managed,
            "EXTERNAL" => Self::External,
            "FOREIGN" => Self::Foreign,
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
    #[serde(default)]
    pub position: Option<i64>,
    #[serde(default)]
    pub type_precision: Option<i64>,
    #[serde(default)]
    pub type_scale: Option<i64>,
    #[serde(default)]
    pub type_json: Option<String>,
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

/// Response from `/api/2.1/unity-catalog/effective-permissions/table/{full_name}`.
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
const READ_PRIVILEGES: &[&str] = &[
    "SELECT",
    "ALL_PRIVILEGES",
    "ALL PRIVILEGES",
    "OWNER",
    "OWNERSHIP",
];

impl UCPermissionsEnvelope {
    /// Returns `true` if the current caller has a read-compatible privilege
    /// (`SELECT` or `ALL_PRIVILEGES`).
    ///
    /// This checks the response from the UC **effective-permissions** endpoint,
    /// which already scopes results to the authenticated principal.
    #[must_use]
    pub fn has_read_permission(&self) -> bool {
        self.privilege_assignments.iter().any(|pa| {
            pa.privileges
                .iter()
                .any(|p| READ_PRIVILEGES.contains(&p.privilege.as_str()))
        })
    }

    /// Returns the principal identifiers from the privilege assignments.
    #[must_use]
    pub fn principals(&self) -> Vec<&str> {
        self.privilege_assignments
            .iter()
            .map(|pa| pa.principal.as_str())
            .collect()
    }

    /// Returns all privilege names across all principal assignments.
    #[must_use]
    pub fn all_privileges(&self) -> Vec<&str> {
        self.privilege_assignments
            .iter()
            .flat_map(|pa| pa.privileges.iter().map(|p| p.privilege.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        UCColumn, UCPermissionsEnvelope, UCPrivilege, UCPrivilegeAssignment, UCTable, UCTableType,
    };

    fn make_table(table_type: &str) -> UCTable {
        UCTable {
            name: "table".to_string(),
            catalog_name: "catalog".to_string(),
            schema_name: "schema".to_string(),
            table_type: table_type.to_string(),
            data_source_format: "DELTA".to_string(),
            columns: Vec::<UCColumn>::new(),
            storage_location: None,
        }
    }

    fn make_permissions(privileges: &[&str]) -> UCPermissionsEnvelope {
        UCPermissionsEnvelope {
            privilege_assignments: vec![UCPrivilegeAssignment {
                principal: "test_user".to_string(),
                privileges: privileges
                    .iter()
                    .map(|p| UCPrivilege {
                        privilege: (*p).to_string(),
                    })
                    .collect(),
            }],
        }
    }

    // ----------------------------------------------------------------
    // UCTableType parsing
    // ----------------------------------------------------------------

    #[test]
    fn test_uc_table_type_from_string() {
        assert_eq!(UCTableType::from("MANAGED"), UCTableType::Managed);
        assert_eq!(UCTableType::from("EXTERNAL"), UCTableType::External);
        assert_eq!(UCTableType::from("FOREIGN"), UCTableType::Foreign);
        assert_eq!(UCTableType::from("VIEW"), UCTableType::View);
        assert_eq!(
            UCTableType::from("MATERIALIZED_VIEW"),
            UCTableType::MaterializedView
        );
        assert_eq!(
            UCTableType::from("STREAMING_TABLE"),
            UCTableType::StreamingTable
        );
        assert_eq!(UCTableType::from("SOMETHING_NEW"), UCTableType::Unknown);
        assert_eq!(UCTableType::from(""), UCTableType::Unknown);
    }

    // ----------------------------------------------------------------
    // is_queryable
    // ----------------------------------------------------------------

    #[test]
    fn test_queryable_table_types() {
        assert!(make_table("MANAGED").is_queryable());
        assert!(make_table("EXTERNAL").is_queryable());
        assert!(make_table("FOREIGN").is_queryable());
        assert!(make_table("MATERIALIZED_VIEW").is_queryable());
    }

    #[test]
    fn test_non_queryable_table_types() {
        assert!(!make_table("VIEW").is_queryable());
        assert!(!make_table("STREAMING_TABLE").is_queryable());
        assert!(!make_table("UNKNOWN_TYPE").is_queryable());
    }

    // ----------------------------------------------------------------
    // requires_read_permission_validation
    // ----------------------------------------------------------------

    #[test]
    fn test_foreign_tables_skip_strict_permission_validation() {
        let table = make_table("FOREIGN");

        assert!(table.is_queryable());
        assert!(!table.requires_read_permission_validation());
    }

    #[test]
    fn test_managed_tables_keep_permission_validation() {
        let table = make_table("MANAGED");

        assert!(table.is_queryable());
        assert!(table.requires_read_permission_validation());
    }

    #[test]
    fn test_external_tables_keep_permission_validation() {
        assert!(make_table("EXTERNAL").requires_read_permission_validation());
    }

    #[test]
    fn test_materialized_view_keeps_permission_validation() {
        assert!(make_table("MATERIALIZED_VIEW").requires_read_permission_validation());
    }

    #[test]
    fn test_non_queryable_types_still_require_permission_validation() {
        assert!(make_table("VIEW").requires_read_permission_validation());
        assert!(make_table("STREAMING_TABLE").requires_read_permission_validation());
    }

    // ----------------------------------------------------------------
    // UCPermissionsEnvelope::has_read_permission
    // ----------------------------------------------------------------

    #[test]
    fn test_has_read_permission_with_select() {
        assert!(make_permissions(&["SELECT"]).has_read_permission());
    }

    #[test]
    fn test_has_read_permission_with_all_privileges() {
        assert!(make_permissions(&["ALL_PRIVILEGES"]).has_read_permission());
    }

    #[test]
    fn test_has_read_permission_with_all_privileges_space() {
        assert!(make_permissions(&["ALL PRIVILEGES"]).has_read_permission());
    }

    #[test]
    fn test_has_read_permission_with_owner() {
        assert!(make_permissions(&["OWNER"]).has_read_permission());
    }

    #[test]
    fn test_has_read_permission_with_ownership() {
        assert!(make_permissions(&["OWNERSHIP"]).has_read_permission());
    }

    #[test]
    fn test_no_read_permission_with_only_modify() {
        assert!(!make_permissions(&["MODIFY"]).has_read_permission());
    }

    #[test]
    fn test_no_read_permission_with_only_create() {
        assert!(!make_permissions(&["CREATE"]).has_read_permission());
    }

    #[test]
    fn test_no_read_permission_empty_assignments() {
        let perms = UCPermissionsEnvelope {
            privilege_assignments: vec![],
        };
        assert!(!perms.has_read_permission());
    }

    #[test]
    fn test_no_read_permission_empty_privileges() {
        let perms = UCPermissionsEnvelope {
            privilege_assignments: vec![UCPrivilegeAssignment {
                principal: "test_user".to_string(),
                privileges: vec![],
            }],
        };
        assert!(!perms.has_read_permission());
    }

    #[test]
    fn test_has_read_permission_mixed_privileges() {
        assert!(make_permissions(&["MODIFY", "CREATE", "SELECT"]).has_read_permission());
    }

    #[test]
    fn test_has_read_permission_multiple_principals() {
        let perms = UCPermissionsEnvelope {
            privilege_assignments: vec![
                UCPrivilegeAssignment {
                    principal: "user_no_access".to_string(),
                    privileges: vec![UCPrivilege {
                        privilege: "MODIFY".to_string(),
                    }],
                },
                UCPrivilegeAssignment {
                    principal: "user_with_access".to_string(),
                    privileges: vec![UCPrivilege {
                        privilege: "SELECT".to_string(),
                    }],
                },
            ],
        };
        assert!(perms.has_read_permission());
    }

    // ----------------------------------------------------------------
    // UCTable::full_name
    // ----------------------------------------------------------------

    #[test]
    fn test_full_name() {
        let table = make_table("MANAGED");
        assert_eq!(table.full_name(), "catalog.schema.table");
    }

    // ----------------------------------------------------------------
    // UCPermissionsEnvelope helpers
    // ----------------------------------------------------------------

    #[test]
    fn test_principals_returns_all_principal_names() {
        let perms = UCPermissionsEnvelope {
            privilege_assignments: vec![
                UCPrivilegeAssignment {
                    principal: "alice".to_string(),
                    privileges: vec![],
                },
                UCPrivilegeAssignment {
                    principal: "bob".to_string(),
                    privileges: vec![],
                },
            ],
        };
        assert_eq!(perms.principals(), vec!["alice", "bob"]);
    }

    #[test]
    fn test_all_privileges_returns_flattened_privileges() {
        let perms = UCPermissionsEnvelope {
            privilege_assignments: vec![
                UCPrivilegeAssignment {
                    principal: "alice".to_string(),
                    privileges: vec![
                        UCPrivilege {
                            privilege: "SELECT".to_string(),
                        },
                        UCPrivilege {
                            privilege: "MODIFY".to_string(),
                        },
                    ],
                },
                UCPrivilegeAssignment {
                    principal: "bob".to_string(),
                    privileges: vec![UCPrivilege {
                        privilege: "CREATE".to_string(),
                    }],
                },
            ],
        };
        assert_eq!(perms.all_privileges(), vec!["SELECT", "MODIFY", "CREATE"]);
    }
}
