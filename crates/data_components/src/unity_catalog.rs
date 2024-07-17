/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::sql::TableReference;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use std::collections::HashMap;
use url::Url;

pub mod provider;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("Unity Catalog API request failed: {source}"))]
    ConnectionError { source: reqwest::Error },

    #[snafu(display("Unity Catalog API request failed: {status}"))]
    UnexpectedStatusCode { status: reqwest::StatusCode },

    #[snafu(display("Could not parse {url} into a URL: {source}"))]
    URLParseError {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Invalid catalog URL structure {}, expected format: https://<host>/api/2.1/unity-catalog/catalogs/<catalog_id>",
        url,
    ))]
    InvalidCatalogURL { url: String },

    #[snafu(display("The catalog {catalog_id} doesn't exist."))]
    CatalogDoesntExist { catalog_id: String },

    #[snafu(display("The schema {schema} doesn't exist in {catalog_id}."))]
    SchemaDoesntExist { schema: String, catalog_id: String },

    #[snafu(display("Couldn't get table provider for {table_reference}: {source}"))]
    UnableToGetTableProvider {
        table_reference: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An ergonomic wrapper around calling Unity Catalog APIs.
///
/// Could be replaced once <https://crates.io/crates/unitycatalog-client> is available.
pub struct UnityCatalog {
    endpoint: String,
    token: Option<SecretString>,
    client: reqwest::Client,
}

pub struct Endpoint(pub String);
pub struct CatalogId(pub String);

impl UnityCatalog {
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(endpoint: Endpoint, token: Option<SecretString>) -> Self {
        let mut endpoint_str = endpoint.0.trim_end_matches('/').to_string();
        if !endpoint_str.starts_with("http") {
            endpoint_str = format!("https://{endpoint_str}");
        }

        Self {
            endpoint: endpoint_str,
            token,
            client: reqwest::Client::new(),
        }
    }

    pub fn from_params(params: &HashMap<String, SecretString>) -> Result<Self, Error> {
        let endpoint = params
            .get("endpoint")
            .context(MissingParameterSnafu {
                parameter: "endpoint".to_string(),
            })?
            .expose_secret();

        let token = params.get("token").cloned();

        Ok(Self::new(Endpoint(endpoint.to_string()), token))
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
        let endpoint = format!(
            "{}://{}",
            parsed_url.scheme(),
            parsed_url
                .host_str()
                .map(|s| s.trim_end_matches('/'))
                .context(InvalidCatalogURLSnafu {
                    url: url.to_string()
                })?
        );

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
        let response = self.get_req(&path).send().await.context(ConnectionSnafu)?;

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

    pub async fn get_catalog(&self, catalog_id: &str) -> Result<Option<UCCatalog>> {
        let path = format!("/api/2.1/unity-catalog/catalogs/{catalog_id}");
        let response = self.get_req(&path).send().await.context(ConnectionSnafu)?;

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

    pub async fn list_schemas(&self, catalog_id: &str) -> Result<Option<Vec<UCSchema>>> {
        let path = format!("/api/2.1/unity-catalog/schemas?catalog_name={catalog_id}");
        let response = self.get_req(&path).send().await.context(ConnectionSnafu)?;

        tracing::debug!("list_schemas: Response status: {}", response.status());

        if response.status().is_success() {
            let api_response: UCSchemaEnvelope = response.json().await.context(ConnectionSnafu)?;
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

    pub async fn list_tables(
        &self,
        catalog_id: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<UCTable>>> {
        let path = format!(
            "/api/2.1/unity-catalog/tables?catalog_name={catalog_id}&schema_name={schema_name}"
        );
        let response = self.get_req(&path).send().await.context(ConnectionSnafu)?;

        tracing::debug!("list_tables: Response status: {}", response.status());

        if response.status().is_success() {
            let api_response: UCTableEnvelope = response.json().await.context(ConnectionSnafu)?;
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

    fn get_req(&self, path: &str) -> reqwest::RequestBuilder {
        let full_url = format!("{}{path}", self.endpoint);
        tracing::debug!("Sending request to {full_url}");
        let mut builder = self.client.get(full_url);
        if let Some(token) = &self.token {
            tracing::debug!("Adding bearer token to request");
            builder = builder.bearer_auth(token.expose_secret());
        }
        builder
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UCTableEnvelope {
    #[serde(default)]
    pub tables: Vec<UCTable>,
}

/// Response from `/api/2.1/unity-catalog/tables/{table_name}`
#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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
