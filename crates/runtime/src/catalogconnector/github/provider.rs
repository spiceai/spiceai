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

use super::GitHubResourceType;
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::{
        DataConnector, DataConnectorFactory, github::GithubFactory, parameters::ConnectorParams,
    },
    token_providers::github_app_token::GitHubAppTokenProvider,
};
use app::App;
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{any::Any, fmt, time::Duration};
use token_provider::{StaticTokenProvider, TokenProvider};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid GitHub catalog path. Expected format: 'github.com/<org>/<resource_type>' where resource_type is 'pulls', 'issues', or 'projects'. Got: {path}"
    ))]
    InvalidPath { path: String },

    #[snafu(display(
        "Invalid GitHub resource type '{resource_type}'. Expected 'pulls', 'issues', or 'projects'."
    ))]
    InvalidResourceType { resource_type: String },

    #[snafu(display("Failed to list GitHub repositories for organization '{org}': {source}"))]
    ListRepositories {
        org: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create table provider for '{repo}/{resource_type}': {source}"))]
    CreateTableProvider {
        repo: String,
        resource_type: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create dataset for '{dataset}': {source}"))]
    CreatingDataset {
        dataset: String,
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Failed to create GitHub data connector: {source}"))]
    CreateDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create GitHub App token provider: {source}"))]
    CreateTokenProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog provider for GitHub organizations, providing tables for issues, PRs, and projects.
pub struct GitHubCatalogProvider {
    org: String,
    resource_type: GitHubResourceType,
    token: Option<Arc<dyn TokenProvider>>,
    endpoint: String,
    runtime: Arc<Runtime>,
    app: Arc<App>,
    parameters: ConnectorParams,
    /// Repos are stored as schemas; each repo has one table matching the resource type
    repos: RwLock<HashMap<RepoName, Arc<dyn SchemaProvider>>>,
}

type RepoName = String;

impl fmt::Debug for GitHubCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubCatalogProvider")
            .field("org", &self.org)
            .field("resource_type", &self.resource_type)
            .finish_non_exhaustive()
    }
}

/// A schema provider for a specific GitHub repository
#[derive(Debug)]
pub struct GitHubRepoSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl GitHubCatalogProvider {
    pub async fn new(
        parameters: ConnectorParams,
        catalog: &Catalog,
        runtime: Arc<Runtime>,
        app: Arc<App>,
    ) -> Result<Self> {
        let catalog_id = catalog.catalog_id.as_deref().unwrap_or("");
        let (org, resource_type) = parse_github_catalog_path(catalog_id)?;

        let endpoint = parameters
            .parameters
            .get("endpoint")
            .expose()
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "https://api.github.com".to_string());

        let token_provider = Self::create_token_provider(&parameters).await?;

        Ok(Self {
            org,
            resource_type,
            token: token_provider,
            endpoint,
            runtime,
            app,
            parameters,
            repos: RwLock::new(HashMap::new()),
        })
    }

    async fn create_token_provider(
        parameters: &ConnectorParams,
    ) -> Result<Option<Arc<dyn TokenProvider>>> {
        let token = parameters.parameters.get("token").ok().cloned();
        let client_id = parameters
            .parameters
            .get("client_id")
            .expose()
            .ok()
            .map(ToString::to_string);
        let private_key = parameters
            .parameters
            .get("private_key")
            .expose()
            .ok()
            .map(ToString::to_string);
        let installation_id = parameters
            .parameters
            .get("installation_id")
            .expose()
            .ok()
            .map(ToString::to_string);

        match (token, client_id, private_key, installation_id) {
            (Some(token), _, _, _) => Ok(Some(Arc::new(StaticTokenProvider::new(token.clone())))),
            (None, Some(client_id), Some(private_key), Some(installation_id)) => {
                let provider = GitHubAppTokenProvider::try_new(
                    client_id.into(),
                    private_key.into(),
                    installation_id.into(),
                )
                .await
                .map_err(|e| Error::CreateTokenProvider {
                    source: Box::new(e),
                })?;
                Ok(Some(Arc::new(provider)))
            }
            _ => Ok(None),
        }
    }

    /// List all repositories in the organization using the GraphQL API
    async fn list_organization_repositories(&self) -> Result<Vec<String>> {
        let token = self.token.as_ref().map(Arc::clone);

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| Error::CreateDataConnector {
                source: Box::new(e),
            })?;

        let graphql_endpoint = format!("{}/graphql", self.endpoint);

        let mut all_repos = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let after_clause = cursor
                .as_ref()
                .map_or(String::new(), |c| format!(r#", after: "{c}""#));

            let query = format!(
                r#"{{
                    organization(login: "{org}") {{
                        repositories(first: 100{after_clause}) {{
                            nodes {{
                                name
                            }}
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                        }}
                    }}
                }}"#,
                org = self.org
            );

            let mut request = client
                .post(&graphql_endpoint)
                .header("Content-Type", "application/json")
                .header("User-Agent", "spice");

            if let Some(ref token) = token {
                request = request.header("Authorization", format!("Bearer {}", token.get_token()));
            }

            let response = request
                .json(&serde_json::json!({ "query": query }))
                .send()
                .await
                .map_err(|e| Error::ListRepositories {
                    org: self.org.clone(),
                    source: Box::new(e),
                })?;

            let response_text = response.text().await.map_err(|e| Error::ListRepositories {
                org: self.org.clone(),
                source: Box::new(e),
            })?;

            let response: GraphQLResponse =
                serde_json::from_str(&response_text).map_err(|e| Error::ListRepositories {
                    org: self.org.clone(),
                    source: format!(
                        "Failed to parse GraphQL response: {e}. Response: {response_text}"
                    )
                    .into(),
                })?;

            if let Some(errors) = response.errors {
                if !errors.is_empty() {
                    let error_messages: Vec<String> =
                        errors.iter().map(|e| e.message.clone()).collect();
                    return Err(Error::ListRepositories {
                        org: self.org.clone(),
                        source: error_messages.join(", ").into(),
                    });
                }
            }

            if let Some(data) = response.data {
                if let Some(org) = data.organization {
                    for repo in org.repositories.nodes {
                        all_repos.push(repo.name);
                    }

                    if org.repositories.page_info.has_next_page {
                        cursor = org.repositories.page_info.end_cursor;
                    } else {
                        break;
                    }
                } else {
                    return Err(Error::ListRepositories {
                        org: self.org.clone(),
                        source: "Organization not found or no access".into(),
                    });
                }
            } else {
                return Err(Error::ListRepositories {
                    org: self.org.clone(),
                    source: "Empty response from GitHub API".into(),
                });
            }
        }

        Ok(all_repos)
    }

    /// Create a schema provider for a specific repository
    async fn create_schema_provider(&self, repo: &str) -> Result<Arc<dyn SchemaProvider>> {
        let table_name = self.resource_type.to_string();
        let table_provider = self.create_table_provider(repo).await?;

        let mut tables = HashMap::new();
        tables.insert(table_name, table_provider);

        Ok(Arc::new(GitHubRepoSchemaProvider {
            tables: RwLock::new(tables),
        }))
    }

    /// Create a table provider for a specific repository and resource type using the GitHub data connector
    async fn create_table_provider(&self, repo: &str) -> Result<Arc<dyn TableProvider>> {
        // Create the GitHub data connector
        let github_factory = GithubFactory::new();
        let connector = github_factory
            .create(self.parameters.clone())
            .await
            .map_err(|e| Error::CreateDataConnector { source: e })?;

        // Build the dataset 'from' path in the expected format: github.com/owner/repo/resource_type
        let from = format!("github.com/{}/{}/{}", self.org, repo, self.resource_type);

        // Build a minimal dataset to pass to the data connector
        let dataset = DatasetBuilder::try_new(
            from.clone(),
            &table_name_for_repo(repo, &self.resource_type),
        )
        .boxed()
        .context(CreatingDatasetSnafu {
            dataset: from.clone(),
        })?
        .with_app(Arc::clone(&self.app))
        .with_runtime(Arc::clone(&self.runtime))
        .build()
        .boxed()
        .context(CreatingDatasetSnafu {
            dataset: from.clone(),
        })?;

        // Use the data connector to create the table provider
        connector
            .read_provider(&dataset)
            .await
            .boxed()
            .context(CreateTableProviderSnafu {
                repo: repo.to_string(),
                resource_type: self.resource_type.to_string(),
            })
    }
}

/// Generate table name for a repository and resource type
fn table_name_for_repo(repo: &str, resource_type: &GitHubResourceType) -> String {
    format!("{repo}_{resource_type}")
}

impl CatalogProvider for GitHubCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let repos = match self.repos.read() {
            Ok(r) => r,
            Err(poisoned) => poisoned.into_inner(),
        };

        repos.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let repos = match self.repos.read() {
            Ok(r) => r,
            Err(poisoned) => poisoned.into_inner(),
        };

        repos.get(name).cloned()
    }
}

#[async_trait]
impl RefreshableCatalogProvider for GitHubCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let repo_names = self.list_organization_repositories().await?;

        let mut repos = HashMap::new();

        for repo_name in repo_names {
            match self.create_schema_provider(&repo_name).await {
                Ok(schema_provider) => {
                    repos.insert(repo_name, schema_provider);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to create schema provider for repository '{repo_name}': {e}"
                    );
                }
            }
        }

        let mut repos_lock = match self.repos.write() {
            Ok(r) => r,
            Err(poisoned) => poisoned.into_inner(),
        };

        *repos_lock = repos;

        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for GitHubRepoSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        Ok(tables.get(name).cloned())
    }
}

/// Parse the GitHub catalog path to extract org and resource type
/// Expected format: `github.com/<org>/<resource_type>` where resource_type is 'pulls', 'issues', or 'projects'
fn parse_github_catalog_path(path: &str) -> Result<(String, GitHubResourceType)> {
    let path_without_prefix = path.strip_prefix("github.com/").unwrap_or(path);
    let segments: Vec<&str> = path_without_prefix.split('/').collect();

    match segments.as_slice() {
        [org, resource_type_str] => {
            let resource_type = resource_type_str
                .parse::<GitHubResourceType>()
                .map_err(|_| Error::InvalidResourceType {
                    resource_type: (*resource_type_str).to_string(),
                })?;
            Ok(((*org).to_string(), resource_type))
        }
        _ => Err(Error::InvalidPath {
            path: path.to_string(),
        }),
    }
}

// GraphQL response structures for deserializing repository list
#[derive(Debug, Deserialize)]
struct GraphQLResponse {
    data: Option<GraphQLData>,
    errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQLData {
    organization: Option<OrganizationData>,
}

#[derive(Debug, Deserialize)]
struct OrganizationData {
    repositories: RepositoriesData,
}

#[derive(Debug, Deserialize)]
struct RepositoriesData {
    nodes: Vec<RepositoryNode>,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Debug, Deserialize)]
struct RepositoryNode {
    name: String,
}

#[derive(Debug, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GraphQLError {
    message: String,
}
