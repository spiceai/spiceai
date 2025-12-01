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

use crate::dataconnector::github::pull_requests::PullRequestCommentType;
use crate::token_providers::github_app_token::GitHubAppTokenProvider;
use crate::{component::dataset::Dataset, dataconnector::github::members::MembersTableArgs};
use arrow::array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chrono::{SecondsFormat, TimeZone, Utc, offset::LocalResult};
use commits::CommitsTableArgs;
use data_components::graphql::client::UnnestBehavior;
use data_components::{
    github::{self, GithubFilesTableProvider, GithubRestClient},
    graphql::{
        self, FilterPushdownResult, GraphQLContext,
        builder::GraphQLClientBuilder,
        client::{GraphQLClient, GraphQLQuery, PaginationParameters},
        provider::GraphQLTableProviderBuilder,
    },
    rate_limit::RateLimiter,
};
use datafusion::{
    common::Column,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{Operator, TableProviderFilterPushDown},
    prelude::Expr,
    scalar::ScalarValue,
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use graphql_parser::query::{
    Definition, InlineFragment, OperationDefinition, Query, Selection, SelectionSet,
};
use issues::IssuesTableArgs;
use projects::ProjectsTableArgs;
use pull_requests::PullRequestTableArgs;
use rate_limit::GitHubRateLimiter;
use secrecy::ExposeSecret;
use snafu::ResultExt;
use stargazers::StargazersTableArgs;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::{any::Any, future::Future, pin::Pin, str::FromStr, sync::Arc, time::Duration};
use token_provider::{StaticTokenProvider, TokenProvider};
use tokio::sync::{Mutex, Semaphore};
use url::Url;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters, graphql::default_spice_client,
};

mod commits;
mod issues;
mod members;
mod projects;
mod pull_requests;
mod rate_limit;
mod stargazers;

static GITHUB_CONCURRENCY_LIMITS: LazyLock<Mutex<HashMap<String, Arc<Semaphore>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const GITHUB_DEFAULT_MAX_CONCURRENT_CONNECTIONS: usize = 10;

pub struct Github {
    params: Parameters,
    token: Option<Arc<dyn TokenProvider>>,
    rate_limiter: Arc<GitHubRateLimiter>,
    semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for Github {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Github")
            .field("params", &self.params)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("rate_limiter", &self.rate_limiter)
            .field("semaphore", &"<Semaphore>")
            .finish()
    }
}

pub struct GitHubTableGraphQLParams {
    /// The GraphQL query string
    query: Arc<str>,

    /// The JSON pointer to the data in the response. If not provided, it will be inferred from the query.
    json_pointer: Option<&'static str>,
    /// The behavior to use for unnesting the response data
    unnest_behavior: UnnestBehavior,
    /// The GraphQL schema of the response data, if available
    schema: Option<SchemaRef>,
}

impl GitHubTableGraphQLParams {
    #[must_use]
    pub fn new(
        query: Arc<str>,
        json_pointer: Option<&'static str>,
        unnest_behavior: UnnestBehavior,
        schema: Option<SchemaRef>,
    ) -> Self {
        Self {
            query,
            json_pointer,
            unnest_behavior,
            schema,
        }
    }
}

pub trait GitHubTableArgs: Send + Sync {
    fn get_graphql_values(&self) -> GitHubTableGraphQLParams;
    fn get_component(&self) -> ConnectorComponent;
}

impl Github {
    /// Common error handling for validation responses
    async fn handle_validation_response(
        response: Result<reqwest::Response, reqwest::Error>,
        target: &str,
        resource_type: &str,
        installation_id: &str,
    ) -> Result<(), String> {
        match response {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!(
                    "GitHub App installation ID '{installation_id}' has access to '{target}'"
                );
                Ok(())
            }
            Ok(resp)
                if resp.status().as_u16() == 401
                    || resp.status().as_u16() == 403
                    || resp.status().as_u16() == 410 =>
            {
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response body".to_string());
                tracing::error!(
                    "GitHub App installation does not have access to '{target}' (HTTP {status}). Response: {body}"
                );
                Err(format!(
                    "GitHub App installation ID '{installation_id}' does not have permission to access '{resource_type}' for '{target}' (HTTP {status}). Verify the GitHub App has the required permissions and is correctly installed into {target}."
                ))
            }
            Ok(resp) if resp.status().as_u16() == 404 => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response body".to_string());
                tracing::error!(
                    "Target '{target}' not found or GitHub App installation does not have access (HTTP 404). Response: {body}"
                );
                Err(format!(
                    "Resource '{target}' not found or GitHub App installation ID '{installation_id}' does not have access to it."
                ))
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response body".to_string());
                tracing::error!(
                    "GitHub App installation validation failed for '{target}' (HTTP {status}). Response: {body}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation ID '{installation_id}' access to '{target}' (HTTP {status})."
                ))
            }
            Err(e) => {
                tracing::error!(
                    "GitHub App installation validation request failed for '{target}': {e}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation ID '{installation_id}' access to '{target}': {e}"
                ))
            }
        }
    }

    /// Validates that the GitHub App installation has access to the specified resource type.
    async fn validate_installation_access(
        &self,
        owner: &str,
        repo: Option<&str>,
        resource_type: &str,
    ) -> Result<(), String> {
        // Check if we're using a GitHub App token provider with an installation ID
        let installation_id = self.params.get("installation_id").expose().ok();

        // If no installation ID is provided, validation passes
        let Some(installation_id) = installation_id else {
            tracing::debug!("No GitHub App installation ID provided, skipping validation");
            return Ok(());
        };

        let target = if let Some(repo) = repo {
            format!("{owner}/{repo}/{resource_type}")
        } else {
            format!("{owner}/{resource_type}")
        };

        tracing::debug!(
            "Validating GitHub App installation ID '{installation_id}' has access to '{target}'"
        );

        // If there's an installation ID, we need to validate it by checking if we can get a token
        // The token provider should already be initialized at this point
        if let Some(token_provider) = &self.token {
            // Try to get a token - this will fail if the installation ID is invalid
            let token = token_provider.get_token();
            if token.is_empty() {
                return Err(format!(
                    "Failed to authenticate with GitHub App installation ID '{installation_id}'. The installation ID may be invalid or the app may not be installed."
                ));
            }

            // Validate that the installation has access to the target repository or organization
            let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
                return Ok(()); // If no endpoint, skip this validation
            };

            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|err| {
                    format!(
                        "Failed to create GitHub HTTP client while validating installation access: {err}"
                    )
                })?;

            // Check if the installation has access to this specific resource type
            let validation_url = if let Some(repo) = repo {
                // For repository resources, try to access a specific resource endpoint
                match resource_type {
                    "issues" => format!("{endpoint}/repos/{owner}/{repo}/issues?per_page=1"),
                    "pulls" => format!("{endpoint}/repos/{owner}/{repo}/pulls?per_page=1"),
                    "commits" => format!("{endpoint}/repos/{owner}/{repo}/commits?per_page=1"),
                    "stargazers" => {
                        format!("{endpoint}/repos/{owner}/{repo}/stargazers?per_page=1")
                    }
                    "files" => format!("{endpoint}/repos/{owner}/{repo}/git/trees/HEAD"),
                    // Projects validation is handled during query execution via error_checker
                    // since classic projects API is deprecated and returns HTTP 410
                    "projects" => return Ok(()),
                    _ => format!("{endpoint}/repos/{owner}/{repo}"),
                }
            } else {
                // For organization resources
                match resource_type {
                    "members" => format!("{endpoint}/orgs/{owner}/members?per_page=1"),
                    // Projects validation is handled during query execution via error_checker
                    // since classic projects API is deprecated and returns HTTP 410
                    "projects" => return Ok(()),
                    _ => format!("{endpoint}/orgs/{owner}"),
                }
            };

            let response = client
                .get(&validation_url)
                .header("Accept", "application/vnd.github+json")
                .header("Authorization", format!("Bearer {token}"))
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("User-Agent", "spice")
                .send()
                .await;

            Self::handle_validation_response(response, &target, resource_type, installation_id)
                .await
        } else {
            // No token provider but installation_id was provided - this is a configuration error
            Err(format!(
                "GitHub App installation ID '{installation_id}' provided but no token could be generated. Verify 'client_id' and 'private_key' are configured."
            ))
        }
    }

    pub(crate) fn create_graphql_client(
        &self,
        tbl: &Arc<dyn GitHubTableArgs>,
    ) -> std::result::Result<GraphQLClient, Box<dyn std::error::Error + Send + Sync>> {
        let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
            return Err("Github 'endpoint' not provided".into());
        };

        let token = self
            .token
            .as_ref()
            .map(|token| Arc::clone(token) as Arc<dyn TokenProvider>);

        let client = default_spice_client("application/json").boxed()?;

        let gql_client_params = tbl.get_graphql_values();

        GraphQLClientBuilder::new(
            Url::parse(&format!("{endpoint}/graphql")).boxed()?,
            gql_client_params.unnest_behavior,
        )
        .with_token_provider(token)
        .with_json_pointer(gql_client_params.json_pointer)
        .with_schema(gql_client_params.schema)
        .with_rate_limiter(Some(Arc::clone(&self.rate_limiter) as Arc<dyn RateLimiter>))
        .with_semaphore(Some(Arc::clone(&self.semaphore)))
        .build(client)
        .boxed()
    }

    fn get_health_check_for_owner_and_repo(owner: &str, repo: &str) -> String {
        format!(
            r#"{{
            githubHealthCheck: repository(owner: "{owner}", name: "{repo}") {{
                id
                nameWithOwner
            }}
        }}"#
        )
    }

    fn get_health_check_for_org(org: &str) -> String {
        format!(
            r#"{{
            githubHealthCheck: organization(login: "{org}") {{
                id
                name
            }}
        }}"#
        )
    }

    async fn create_gql_table_provider(
        &self,
        table_args: Arc<dyn GitHubTableArgs>,
        context: Option<Arc<dyn GraphQLContext>>,
        health_check_query_string: String,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = self.create_graphql_client(&table_args).context(
            super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
                connector_component: table_args.get_component(),
            },
        )?;

        let provider_builder = GraphQLTableProviderBuilder::new(client)
            .with_schema_transform(github_gql_raw_schema_cast);

        let provider_builder = if let Some(context) = context {
            provider_builder.with_context(context)
        } else {
            provider_builder
        };

        let query_arc = Arc::from(health_check_query_string);
        let health_check_query = GraphQLQuery::try_from(query_arc)
            .map_err(|e| DataConnectorError::InternalWithSource {
                dataconnector: "github".to_string(),
                connector_component: table_args.get_component(),
                source: e.into(),
            })?
            .with_json_pointer(Arc::from("/data/githubHealthCheck"));

        Ok(Arc::new(
            provider_builder
                .with_health_check_query(health_check_query)
                .build(table_args.get_graphql_values().query.as_ref())
                .await
                .map_err(|e| {
                    if matches!(e, graphql::Error::RateLimited { .. }) {
                        DataConnectorError::RateLimited {
                            dataconnector: "github".to_string(),
                            connector_component: table_args.get_component(),
                            source: e.into(),
                        }
                    } else {
                        DataConnectorError::UnableToGetReadProvider {
                            dataconnector: "github".to_string(),
                            connector_component: table_args.get_component(),
                            source: e.into(),
                        }
                    }
                })?,
        ))
    }

    pub(crate) fn create_rest_client(
        &self,
    ) -> std::result::Result<GithubRestClient, Box<dyn std::error::Error + Send + Sync>> {
        let token = self
            .token
            .as_ref()
            .map(|token| Arc::clone(token) as Arc<dyn TokenProvider>);

        match token {
            Some(token) => GithubRestClient::new(
                token,
                Arc::clone(&self.rate_limiter) as Arc<dyn RateLimiter>,
            )
            .map_err(Into::into),
            None => Err("Github token not provided".into()),
        }
    }

    async fn create_files_table_provider(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: Option<&str>,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(tree_sha) = tree_sha.filter(|s| !s.is_empty()) else {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: format!("The branch or tag name is required in the dataset 'from' and must be in the format 'github.com/{owner}/{repo}/files/<BRANCH_NAME>'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#querying-github-files").into(),
                connector_component: ConnectorComponent::from(dataset),
            });
        };

        let client = self
            .create_rest_client()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
                connector_component: ConnectorComponent::from(dataset),
            })?;

        let include = match self.params.get("include").expose().ok() {
            Some(pattern) => Some(parse_globs(&ConnectorComponent::from(dataset), pattern)?),
            None => None,
        };

        let include_commits = dataset
            .params
            .get("github_include_commits")
            .is_some_and(|value| value.as_str() == "true");

        Ok(Arc::new(
            GithubFilesTableProvider::new(
                client,
                owner,
                repo,
                tree_sha,
                include,
                dataset.is_accelerated(),
                include_commits,
            )
            .await
            .map_err(|e| {
                if matches!(e, github::Error::RateLimited { .. }) {
                    DataConnectorError::RateLimited {
                        dataconnector: "github".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: e.into(),
                    }
                } else {
                    DataConnectorError::UnableToGetReadProvider {
                        dataconnector: "github".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: e.into(),
                    }
                }
            })?,
        ))
    }
}

fn github_gql_raw_schema_cast(
    record_batch: &RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for (idx, field) in record_batch.schema().fields().iter().enumerate() {
        let column = record_batch.column(idx);

        // Handle lists with single-field structs
        if let DataType::List(inner_field) = field.data_type()
            && let DataType::Struct(struct_fields) = inner_field.data_type()
            && struct_fields.len() == 1
        {
            let (new_column, new_field) =
                arrow_tools::record_batch::to_primitive_type_list(column, field)?;
            fields.push(new_field);
            columns.push(new_column);
            continue;
        }

        // Handle top-level structs with a single field (e.g., creator: { creator: "value" })
        // Extract the inner field value and flatten it if the inner and outer fields are the same
        if let DataType::Struct(struct_fields) = field.data_type()
            && struct_fields.len() == 1
        {
            let inner_field = &struct_fields[0];

            // Only flatten if the inner field name matches the outer field name
            if inner_field.name() == field.name() {
                let struct_array = column
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .ok_or_else(|| {
                        format!(
                            "Expected StructArray for field {}, but got different type",
                            field.name()
                        )
                    })?;

                // Get the single inner column
                let inner_column = struct_array.column(0);

                // Create a new field with the outer name but inner type
                let new_field = Arc::new(Field::new(
                    field.name(),
                    inner_field.data_type().clone(),
                    field.is_nullable(),
                ));

                fields.push(new_field);
                columns.push(Arc::clone(inner_column));
                continue;
            }
        }

        fields.push(Arc::clone(field));
        columns.push(Arc::clone(column));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(std::convert::Into::into)
}

#[derive(Default, Debug, Copy, Clone)]
pub struct GithubFactory {}

impl GithubFactory {
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
    ParameterSpec::component("token")
        .description("A Github token.")
        .secret(),
    ParameterSpec::component("client_id")
        .description("The Github App Client ID.")
        .secret(),
    ParameterSpec::component("private_key")
        .description("The Github App private key.")
        .secret(),
    ParameterSpec::component("installation_id")
        .description("The Github App installation ID.")
        .secret(),
    ParameterSpec::component("query_mode")
        .description(
            "Specify what search mode (REST, GraphQL, Search API) to use when retrieving results.",
        )
        .default("auto"),
    ParameterSpec::component("endpoint")
        .description("The Github API endpoint.")
        .default("https://api.github.com"),
    ParameterSpec::component("include_comments")
        .description(
            "Specifies the types of comments to fetch: 'all', 'review', 'discussion', or 'none'.",
        )
        .default("none"),
    ParameterSpec::component("max_comments_fetched")
        .description("Maximum number of comments to fetch per discussion or review thread.")
        .default("100"),
    ParameterSpec::component("include_commits")
        .description("Whether to fetch commit information (created_at, updated_at) for files. Set to 'true' to enable.")
        .default("false"),
    ParameterSpec::runtime("include")
        .description("Include only files matching the pattern.")
        .examples(&["*.json", "**/*.yaml;src/**/*.json"]),
];

impl DataConnectorFactory for GithubFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let token = params.parameters.get("token").ok().cloned();
        let client_id = params
            .parameters
            .get("client_id")
            .expose()
            .ok()
            .map(ToString::to_string);
        let private_key = params
            .parameters
            .get("private_key")
            .expose()
            .ok()
            .map(ToString::to_string);
        let installation_id = params
            .parameters
            .get("installation_id")
            .expose()
            .ok()
            .map(ToString::to_string);

        let max_concurrent_connections = params
            .app
            .and_then(|app| {
                app.runtime
                    .params
                    .get("github_max_concurrent_connections")
                    .cloned()
            })
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(GITHUB_DEFAULT_MAX_CONCURRENT_CONNECTIONS);

        Box::pin(async move {
            let (token_provider, semaphore_key): (Option<Arc<dyn TokenProvider>>, Option<String>) =
                match (token, client_id, private_key, installation_id) {
                    (Some(token), _, _, _) => {
                        let key = token.expose_secret().to_string();
                        (Some(Arc::new(StaticTokenProvider::new(token))), Some(key))
                    }

                    (None, Some(client_id), Some(private_key), Some(installation_id)) => {
                        // GitHub rate limits are per installation, so use the installation ID as the key
                        let key = installation_id.clone();
                        let provider = Arc::new(
                            GitHubAppTokenProvider::try_new(
                                client_id.into(),
                                private_key.into(),
                                installation_id.into(),
                            )
                            .await?,
                        );
                        (Some(provider), Some(key))
                    }

                    _ => (None, None),
                };

            let semaphore = if let Some(key) = semaphore_key {
                let mut limits = GITHUB_CONCURRENCY_LIMITS.lock().await;
                Arc::clone(
                    limits
                        .entry(key)
                        .or_insert_with(|| Arc::new(Semaphore::new(max_concurrent_connections))),
                )
            } else {
                Arc::new(Semaphore::new(max_concurrent_connections))
            };

            Ok(Arc::new(Github {
                params: params.parameters,
                token: token_provider,
                rate_limiter: Arc::new(GitHubRateLimiter::new()),
                semaphore,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "github"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum GitHubQueryMode {
    Auto,
    Search,
}

impl std::str::FromStr for GitHubQueryMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "search" => Ok(Self::Search),
            s => Err(s.to_string()),
        }
    }
}

fn warn_if_provided(
    parameters: Vec<(&str, bool)>,
    table_type: &str,
    connector_component: &ConnectorComponent,
) {
    for (param, present) in parameters {
        if present {
            tracing::warn!(
                "The parameter '{param}' is not supported for the {connector_component}, as a '{table_type}' table. For details, visit: https://spiceai.org/docs/components/data-connectors/github"
            );
        }
    }
}

const MAX_COMMENTS_FETCHED: u32 = 100;

// Organization-level resources (2 segments: owner/resource_type)
const ORG_LEVEL_RESOURCES: &[&str] = &["members", "projects"];

// Repository-level resources (3+ segments: owner/repo/resource_type[/...])
const REPO_LEVEL_RESOURCES: &[&str] = &[
    "pulls",
    "issues",
    "commits",
    "stargazers",
    "projects",
    "files",
];

/// Parsed GitHub path components
#[derive(Debug)]
struct GitHubPathComponents<'a> {
    owner: &'a str,
    repo: Option<&'a str>,
    resource_type: &'a str,
    remaining: Option<String>,
}

/// Parse owner, repo, and resource type from the GitHub path
fn parse_github_path(path: &str) -> Option<GitHubPathComponents<'_>> {
    // Strip prefix and split into segments
    let path_without_prefix = path.strip_prefix("github.com/")?;
    let segments: Vec<&str> = path_without_prefix.split('/').collect();

    match segments.as_slice() {
        // Organization-level: github.com/owner/resource_type
        [owner, resource_type] if ORG_LEVEL_RESOURCES.contains(resource_type) => {
            Some(GitHubPathComponents {
                owner,
                repo: None,
                resource_type,
                remaining: None,
            })
        }
        // Repository-level: github.com/owner/repo/resource_type or github.com/owner/repo/resource_type/...
        [owner, repo, resource_type, remaining @ ..]
            if REPO_LEVEL_RESOURCES.contains(resource_type) =>
        {
            // Filter out empty segments (from trailing slashes) before joining
            let remaining_filtered: Vec<&str> = remaining
                .iter()
                .filter(|s| !s.is_empty())
                .copied()
                .collect();

            Some(GitHubPathComponents {
                owner,
                repo: Some(repo),
                resource_type,
                remaining: if remaining_filtered.is_empty() {
                    None
                } else {
                    Some(remaining_filtered.join("/"))
                },
            })
        }
        _ => None,
    }
}

#[async_trait]
#[expect(clippy::too_many_lines)]
impl DataConnector for Github {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path().to_string();

        // Parse owner, repo, and resource type from the path for validation
        if let Some(parsed) = parse_github_path(&path) {
            self.validate_installation_access(parsed.owner, parsed.repo, parsed.resource_type)
                .await
                .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e.into(),
                })?;
        }

        let query_mode = dataset
            .params
            .get("github_query_mode")
            .map_or("auto", |v| v);

        let query_mode = GitHubQueryMode::from_str(query_mode).map_err(|e| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: format!("Invalid query mode: {e}.\nEnsure a valid query mode is used, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-parameters").into(),
            }
        })?;

        let include_comments = dataset
            .params
            .get("github_include_comments")
            .map(|value| {
                PullRequestCommentType::try_from(value.as_str()).map_err(|e| {
                    DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "github".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: e,
                    }
                })
            })
            .transpose()?;

        let max_comments_fetched = dataset
            .params
            .get("github_max_comments_fetched")
            .map(|value| {
                value
                    .parse::<u32>()
                    .map_err(|e| DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "github".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: format!("Failed to parse integer from string '{value}': {e}"),
                    })
            })
            .transpose()?;

        let pull_request_specific_params = vec![
            ("github_include_comments", include_comments.is_some()),
            (
                "github_max_comments_fetched",
                max_comments_fetched.is_some(),
            ),
        ];

        let component = ConnectorComponent::from(dataset);

        // Parse the path and handle based on the resource type
        let Some(parsed) = parse_github_path(&path) else {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                connector_component: component,
                source: "Invalid GitHub path provided in the dataset 'from'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration".into(),
            });
        };

        match (parsed.resource_type, parsed.repo) {
            ("pulls", Some(repo)) => {
                let max_comments_fetched = match max_comments_fetched.unwrap_or(MAX_COMMENTS_FETCHED) {
                    value if value > MAX_COMMENTS_FETCHED => {
                        tracing::warn!(
                            "Due to GitHub API rate limits, the number of comments fetched for {component} per pull request is limited to {MAX_COMMENTS_FETCHED}."
                        );
                        MAX_COMMENTS_FETCHED
                    }
                    value => value,
                };

                let table_args = Arc::new(PullRequestTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: repo.to_string(),
                    query_mode,
                    component,
                    include_comments: include_comments.unwrap_or(PullRequestCommentType::None),
                    max_comments_fetched,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_owner_and_repo(parsed.owner, repo)
                )
                .await
            }
            ("commits", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "commits", &component);

                let table_args = Arc::new(CommitsTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: repo.to_string(),
                    component,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_owner_and_repo(parsed.owner, repo)
                )
                .await
            }
            ("issues", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "issues", &component);

                let table_args = Arc::new(IssuesTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: repo.to_string(),
                    query_mode,
                    component,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_owner_and_repo(parsed.owner, repo)
                )
                .await
            }
            ("stargazers", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "stargazers", &component);

                let table_args = Arc::new(StargazersTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: repo.to_string(),
                    component,
                });
                self.create_gql_table_provider(table_args, None, Github::get_health_check_for_owner_and_repo(parsed.owner, repo)).await
            }
            ("files", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "files", &component);
                self.create_files_table_provider(
                    parsed.owner,
                    repo,
                    parsed.remaining.as_deref(),
                    dataset,
                )
                .await
            }
            ("projects", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "projects", &component);
                let table_args = Arc::new(ProjectsTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: Some(repo.to_string()),
                    component,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_owner_and_repo(parsed.owner, repo)
                )
                .await
            }
            ("projects", None) => {
                warn_if_provided(pull_request_specific_params, "projects", &component);
                let table_args = Arc::new(ProjectsTableArgs {
                    owner: parsed.owner.to_string(),
                    repo: None,
                    component,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_org(parsed.owner)
                )
                .await
            }
            ("members", None) => {
                warn_if_provided(pull_request_specific_params, "members", &component);
                let table_args = Arc::new(MembersTableArgs {
                    org: parsed.owner.to_string(),
                    component,
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    None,
                    Github::get_health_check_for_org(parsed.owner)
                )
                .await
            }
            (resource_type, _) => {
                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    source: format!("Invalid GitHub table type: {resource_type}.\nEnsure a valid table type is used, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration").into(),
                    connector_component: component,
                })
            }
        }
    }
}

pub fn parse_globs(
    component: &ConnectorComponent,
    input: &str,
) -> super::DataConnectorResult<Arc<GlobSet>> {
    let patterns: Vec<&str> = input.split(&[',', ';'][..]).collect();
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        let trimmed_pattern = pattern.trim();
        if !trimmed_pattern.is_empty() {
            builder.add(
                Glob::new(trimmed_pattern).context(super::InvalidGlobPatternSnafu {
                    pattern,
                    dataconnector: "github".to_string(),
                    connector_component: component.clone(),
                })?,
            );
        }
    }

    let glob_set = builder.build().context(super::InvalidGlobPatternSnafu {
        pattern: input,
        dataconnector: "github".to_string(),
        connector_component: component.clone(),
    })?;
    Ok(Arc::new(glob_set))
}

enum GitHubFilterRemap {
    Column(&'static str),
    Operator((Operator, &'static str)),
}

struct GitHubPushdownSupport {
    // which operators are permitted to be pushed down
    ops: Vec<Operator>,
    // if the column name needs to be changed for the query, include a remap
    // remaps can be operator dependent. For example, the "since" and "until" operators for "committed_date"
    remaps: Option<Vec<GitHubFilterRemap>>,
    // Whether this query parameter permits the use of modifiers like <, >, -, etc
    uses_modifiers: bool,
}

// TODO: add support for IN filters, to support columns like assignees, labels, etc.
// Table currently doesn't support IN at all though, with or without pushdown, so that needs to be fixed first
static GITHUB_FILTER_PUSHDOWNS_SUPPORTED: LazyLock<HashMap<&'static str, GitHubPushdownSupport>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert(
            "author",
            GitHubPushdownSupport {
                ops: vec![Operator::Eq, Operator::NotEq],
                remaps: None,
                uses_modifiers: true,
            },
        );

        m.insert(
            "title",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::LikeMatch,
                    Operator::ILikeMatch,
                    Operator::NotLikeMatch,
                    Operator::NotILikeMatch,
                ],
                remaps: None,
                uses_modifiers: false,
            },
        );

        m.insert(
            "state",
            GitHubPushdownSupport {
                ops: vec![Operator::Eq, Operator::NotEq],
                remaps: None,
                uses_modifiers: true,
            },
        );

        m.insert(
            "body",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::LikeMatch,
                    Operator::ILikeMatch,
                    Operator::NotLikeMatch,
                    Operator::NotILikeMatch,
                ],
                remaps: None,
                uses_modifiers: false,
            },
        );

        m.insert(
            "created_at",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
                remaps: Some(vec![GitHubFilterRemap::Column("created")]),
                uses_modifiers: true,
            },
        );

        m.insert(
            "updated_at",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
                remaps: Some(vec![GitHubFilterRemap::Column("updated")]),
                uses_modifiers: true,
            },
        );

        m.insert(
            "closed_at",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
                remaps: Some(vec![GitHubFilterRemap::Column("closed")]),
                uses_modifiers: true,
            },
        );

        m.insert(
            "merged_at",
            GitHubPushdownSupport {
                ops: vec![
                    Operator::Eq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
                remaps: Some(vec![GitHubFilterRemap::Column("merged")]),
                uses_modifiers: true,
            },
        );

        m.insert(
            "committed_date",
            GitHubPushdownSupport {
                // e.g. committed_date > '2024-09-14'
                ops: vec![Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq],
                remaps: Some(vec![
                    GitHubFilterRemap::Operator((Operator::Gt, "since")),
                    GitHubFilterRemap::Operator((Operator::GtEq, "since")),
                    GitHubFilterRemap::Operator((Operator::Lt, "until")),
                    GitHubFilterRemap::Operator((Operator::LtEq, "until")),
                ]),
                uses_modifiers: false,
            },
        );

        m.insert(
            "labels",
            GitHubPushdownSupport {
                ops: vec![Operator::LikeMatch],
                remaps: Some(vec![GitHubFilterRemap::Column("label")]),
                uses_modifiers: false,
            },
        );

        m
    });

fn expr_to_match(expr: &Expr) -> Option<(Column, ScalarValue, Operator)> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            match (*binary_expr.left.clone(), *binary_expr.right.clone()) {
                (Expr::Column(column), Expr::Literal(value, _))
                | (Expr::Literal(value, _), Expr::Column(column)) => {
                    Some((column, value, binary_expr.op))
                }
                _ => None,
            }
        }
        Expr::Like(like_expr) => match (*like_expr.expr.clone(), *like_expr.pattern.clone()) {
            (Expr::Column(column), Expr::Literal(value, _))
            | (Expr::Literal(value, _), Expr::Column(column)) => {
                let op = match (like_expr.negated, like_expr.case_insensitive) {
                    (false, false) => Operator::LikeMatch,
                    (true, false) => Operator::NotLikeMatch,
                    (false, true) => Operator::ILikeMatch,
                    (true, true) => Operator::NotILikeMatch,
                };

                Some((column, value, op))
            }
            _ => None,
        },
        Expr::ScalarFunction(func) => {
            if func.args.len() != 2 || !func.func.aliases().contains(&"list_contains".to_string()) {
                None
            } else {
                match (func.args[0].clone(), func.args[1].clone()) {
                    (Expr::Column(column), Expr::Literal(value, _))
                    | (Expr::Literal(value, _), Expr::Column(column)) => {
                        Some((column, value, Operator::LikeMatch))
                    }
                    _ => None,
                }
            }
        }
        _ => None,
    }
}

pub(crate) fn filter_pushdown(expr: &Expr) -> FilterPushdownResult {
    let column_matches = expr_to_match(expr);

    if let Some((column, value, op)) = column_matches
        && let Some(column_support) = GITHUB_FILTER_PUSHDOWNS_SUPPORTED.get(column.name.as_str())
    {
        if !column_support.ops.contains(&op) {
            tracing::debug!("Unsupported operator {op} for column {}", column.name);

            return FilterPushdownResult {
                filter_pushdown: TableProviderFilterPushDown::Unsupported,
                expr: expr.clone(),
                context: None,
            };
        }

        let column_name = if let Some(remaps) = &column_support.remaps {
            let mut column_name: Option<&str> = None;
            for remap in remaps {
                match remap {
                    GitHubFilterRemap::Column(remap_column) => {
                        column_name = Some(remap_column);
                    }
                    GitHubFilterRemap::Operator((remap_op, remap_column)) => {
                        if *remap_op == op {
                            column_name = Some(remap_column);
                        }
                    }
                }
            }

            column_name.unwrap_or(column.name.as_str())
        } else {
            column.name.as_str()
        };

        let value = match value {
            ScalarValue::Utf8(Some(v)) => {
                if column.name == "state" {
                    v.to_lowercase()
                } else {
                    v
                }
            }
            ScalarValue::TimestampMillisecond(Some(millis), _) => {
                let dt = Utc.timestamp_millis_opt(millis);
                match dt {
                    LocalResult::Single(dt) => match column_name {
                        "updated" | "created" | "closed" | "merged" => dt.to_rfc3339(),
                        "since" | "until" => dt.to_rfc3339_opts(SecondsFormat::Secs, true),
                        _ => {
                            return FilterPushdownResult {
                                filter_pushdown: TableProviderFilterPushDown::Unsupported,
                                expr: expr.clone(),
                                context: None,
                            };
                        }
                    },
                    _ => {
                        return FilterPushdownResult {
                            filter_pushdown: TableProviderFilterPushDown::Unsupported,
                            expr: expr.clone(),
                            context: None,
                        };
                    }
                }
            }
            _ => value.to_string(),
        };

        let neq = match op {
            Operator::NotEq => "-",
            _ => "",
        };

        let modifier = match (column_support.uses_modifiers, op) {
            (true, Operator::LtEq) => "<=",
            (true, Operator::Lt) => "<",
            (true, Operator::GtEq) => ">=",
            (true, Operator::Gt) => ">",
            _ => "",
        };

        let parameter = match column_name {
            "title" => format!("{value} in:title"),
            "body" => format!("{value} in:body"),
            "state" => format!("is:{value}"), // is:merged, is:closed, is:open provides more granular results than state:closed
            // state:closed returns both closed and merged PRs, but is:merged returns only merged PRs
            // is:closed still returns both closed and merged PRs
            _ => format!("{neq}{column_name}:{modifier}{value}"),
        };

        return FilterPushdownResult {
            filter_pushdown: TableProviderFilterPushDown::Inexact,
            expr: expr.clone(),
            context: Some(parameter),
        };
    }

    FilterPushdownResult {
        filter_pushdown: TableProviderFilterPushDown::Unsupported,
        expr: expr.clone(),
        context: None,
    }
}

pub(crate) fn search_inject_parameters(
    field: &mut graphql_parser::query::Field<'_, String>,
    filters: &[&FilterPushdownResult],
) -> Result<(), datafusion::error::DataFusionError> {
    // get the query: argument from the search() field
    let query_arg = field.arguments.iter_mut().find_map(|arg| {
            if arg.0 == "query" {
                Some(arg)
            } else {
                None
            }
        }).ok_or_else(|| DataFusionError::Execution("GitHub GraphQL query did not contain a 'query' argument in the 'search()' statement, when one was expected".to_string()))?;

    let arg_additions = filters
        .iter()
        .map(|filter| {
            if let Some(context) = &filter.context {
                format!(" {context}")
            } else {
                String::new()
            }
        })
        .collect::<Vec<String>>()
        .join(" ");

    let query_value = match &query_arg.1 {
        graphql_parser::query::Value::String(v) => {
            let v = v.replace('"', "");
            Ok(format!("{v} {arg_additions}"))
        }
        _ => Err(DataFusionError::Execution(
            "GitHub GraphQL query 'query' argument was not a string".to_string(),
        )),
    }?;

    // now replace the argument in search()
    *query_arg = (
        query_arg.0.clone(),
        graphql_parser::query::Value::String(query_value),
    );

    Ok(())
}

pub(crate) fn commits_inject_parameters(
    field: &mut graphql_parser::query::Field<'_, String>,
    filters: &[&FilterPushdownResult],
) -> Result<(), datafusion::error::DataFusionError> {
    for filter in filters {
        if let Some(context) = &filter.context {
            let Some((column, value)) = context.split_once(':') else {
                return Err(DataFusionError::Execution(
                    "GitHub GraphQL query argument was not in the expected format of '<column>:<value>'".to_string(),
                ));
            };

            field.arguments.push((
                column.to_string(),
                graphql_parser::query::Value::String::<String>(value.to_string()),
            ));
        }
    }

    Ok(())
}

pub(crate) fn inject_parameters<F>(
    target_field_name: &str,
    field_modifier: F,
    filters: &[FilterPushdownResult],
    query: &mut GraphQLQuery,
) -> Result<(), datafusion::error::DataFusionError>
where
    F: Fn(
        &mut graphql_parser::query::Field<'_, String>,
        &[&FilterPushdownResult],
    ) -> Result<(), datafusion::error::DataFusionError>,
{
    if filters.is_empty() {
        return Ok(());
    }

    // only inject filters that aren't unsupported
    let filters: Vec<&FilterPushdownResult> = filters
        .iter()
        .filter(|f| f.filter_pushdown != TableProviderFilterPushDown::Unsupported)
        .collect();

    // find the history() field leaf in the AST
    let mut all_selections: Vec<&mut Selection<'_, String>> = Vec::new();
    for def in &mut query.ast_mut().definitions {
        let selections = match def {
            Definition::Operation(OperationDefinition::Query(Query { selection_set, .. })) => {
                &mut selection_set.items
            }
            Definition::Operation(OperationDefinition::SelectionSet(SelectionSet {
                items,
                ..
            })) => items,
            _ => continue,
        };

        all_selections.extend(selections.iter_mut());
    }

    let mut target_field = None;
    // loop over inner selection sets to find the target field if it's deep in a nest
    loop {
        let Some(selection) = all_selections.pop() else {
            break;
        };

        match selection {
            graphql_parser::query::Selection::InlineFragment(InlineFragment {
                selection_set,
                ..
            }) => {
                selection_set
                    .items
                    .iter_mut()
                    .for_each(|item| all_selections.push(item));
            }
            graphql_parser::query::Selection::Field(field) => {
                if field.name == target_field_name {
                    target_field = Some(field);
                    break;
                }

                field
                    .selection_set
                    .items
                    .iter_mut()
                    .for_each(|item| all_selections.push(item));
            }
            graphql_parser::query::Selection::FragmentSpread(_) => {}
        }
    }

    let target_field = target_field.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "GitHub GraphQL query did not contain a '{target_field_name}()' statement, when one was expected"
        ))
    })?;

    field_modifier(target_field, &filters)?;

    // update any change in JSON pointer and pagination parameters
    let (pagination_parameters, json_pointer) = PaginationParameters::parse(query.ast());
    query.pagination_parameters = pagination_parameters;
    query.json_pointer = json_pointer.map(Arc::from);

    Ok(())
}
