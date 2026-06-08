/*
Copyright 2025 The Spice.ai OSS Authors

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
use commits::{CommitsTableArgs, CommitsTableProvider};
use data_components::graphql::client::UnnestBehavior;
use data_components::{
    github::{self, GithubFilesTableProvider, GithubRestClient},
    graphql::{
        self, FilterPushdownResult, GraphQLContext,
        builder::GraphQLClientBuilder,
        client::{GraphQLClient, GraphQLQuery, PaginationParameters},
        provider::{GraphQLTableProvider, GraphQLTableProviderBuilder},
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
use governor::Quota;
use graphql_parser::query::{
    Definition, InlineFragment, OperationDefinition, Query, Selection, SelectionSet,
};
use issues::IssuesTableArgs;
use projects::ProjectsTableArgs;
use pull_requests::PullRequestTableArgs;
use rate_limit::GitHubRateLimiter;
use runtime_rate_control::{JitterConfig, RateController, RateControllerBuilder};
use secrecy::ExposeSecret;
use snafu::ResultExt;
use stargazers::StargazersTableArgs;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::{any::Any, future::Future, pin::Pin, str::FromStr, sync::Arc, time::Duration};
use token_provider::{StaticTokenProvider, TokenProvider};
use tokio::sync::{Mutex, RwLock, Semaphore};
use url::Url;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};

mod commits;
mod issues;
mod members;
mod projects;
mod pull_requests;
mod rate_limit;
mod stargazers;
mod workflow_runs;
mod workflows;

type GitHubConcurrencyLimits = HashMap<String, (usize, Arc<Semaphore>)>;

static GITHUB_CONCURRENCY_LIMITS: LazyLock<Mutex<GitHubConcurrencyLimits>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static GITHUB_AUTH_CONTEXT_RATE_CONTROLLERS: LazyLock<
    RwLock<HashMap<String, Arc<RateController>>>,
> = LazyLock::new(|| RwLock::new(HashMap::new()));
static UNAUTHENTICATED_AUTH_CONTEXT: &str = "unauthenticated";
const GITHUB_CONNECTOR_DOCS_URL: &str =
    "https://spiceai.org/docs/components/data-connectors/github";
const RUNTIME_SOURCE_GITHUB_CONCURRENT_CONNECTIONS_LIMIT: &str =
    "runtime.source_rate_control.github_concurrent_connections_limit";
const LEGACY_RUNTIME_GITHUB_MAX_CONCURRENT_CONNECTIONS: &str =
    "runtime.params.github_max_concurrent_connections";

fn sanitize_github_validation_body(body: &str) -> String {
    body.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn get_github_auth_context_rate_controller(auth_context: String) -> Arc<RateController> {
    let rate_controllers = GITHUB_AUTH_CONTEXT_RATE_CONTROLLERS.read().await;
    if let Some(controller) = rate_controllers.get(&auth_context) {
        return Arc::clone(controller);
    }

    drop(rate_controllers);
    let mut rate_controllers = GITHUB_AUTH_CONTEXT_RATE_CONTROLLERS.write().await;

    // GitHub secondary rate limit for GraphQL is 2000 points per minute
    let Some(secondary_quota_per_minute) = NonZeroU32::new(2000) else {
        unreachable!("2000 is non-zero");
    };

    // GitHub secondary rate limit for requests per minute cannot exceed 90 CPU time per 60 seconds wall time
    let Some(cpu_time_limit) = NonZeroU32::new(90) else {
        unreachable!("90 is non-zero");
    };

    let rate_controller = RateControllerBuilder::new()
        .with_weighted_quota(Quota::per_minute(secondary_quota_per_minute))
        .add_quota(Quota::per_minute(cpu_time_limit))
        .with_jitter(JitterConfig::new(
            Duration::from_millis(5),
            Duration::from_millis(10),
        ));

    let controller = rate_controller.build();
    rate_controllers.insert(auth_context.clone(), Arc::clone(&controller));

    controller
}

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
                tracing::debug!("GitHub App installation {installation_id} has access to {target}");
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
                let body = sanitize_github_validation_body(&body);
                tracing::error!(
                    "GitHub App installation validation failed for {target} (resource: {resource_type}, installation_id: {installation_id}, status: {status}): {body}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation {installation_id} for {target}: permission denied (HTTP {status}). Verify the app is installed on the target and has access to {resource_type}."
                ))
            }
            Ok(resp) if resp.status().as_u16() == 404 => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response body".to_string());
                let body = sanitize_github_validation_body(&body);
                tracing::error!(
                    "GitHub App installation validation could not find {target} (installation_id: {installation_id}): {body}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation {installation_id} for {target}: the resource was not found or is not accessible (HTTP 404)."
                ))
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response body".to_string());
                let body = sanitize_github_validation_body(&body);
                tracing::error!(
                    "GitHub App installation validation failed for {target} (installation_id: {installation_id}, status: {status}): {body}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation {installation_id} for {target} (HTTP {status})."
                ))
            }
            Err(e) => {
                tracing::error!(
                    "GitHub App installation validation request failed for {target} (installation_id: {installation_id}): {e}"
                );
                Err(format!(
                    "Failed to validate GitHub App installation {installation_id} for {target}: {e}"
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
        // Skip validation when token-based auth is used (token takes precedence over app auth).
        // The `installation_id` parameter may be present due to secret autoloading from .env,
        // even when the dataset was explicitly configured with a token.
        if self.params.get("token").ok().is_some() {
            tracing::debug!(
                "Skipping GitHub App access validation because token-based auth is active"
            );
            return Ok(());
        }

        // Check if we're using a GitHub App token provider with an installation ID
        let installation_id = self.params.get("installation_id").expose().ok();

        // If no installation ID is provided, validation passes
        let Some(installation_id) = installation_id else {
            tracing::debug!(
                "Skipping GitHub App access validation because no installation_id was configured"
            );
            return Ok(());
        };

        let target = if let Some(repo) = repo {
            format!("{owner}/{repo}/{resource_type}")
        } else {
            format!("{owner}/{resource_type}")
        };

        tracing::debug!("Validating GitHub App installation {installation_id} for {target}");

        // If there's an installation ID, we need to validate it by checking if we can get a token
        // The token provider should already be initialized at this point
        if let Some(token_provider) = &self.token {
            // Try to get a token - this will fail if the installation ID is invalid
            let token = token_provider.get_token();
            if token.is_empty() {
                return Err(format!(
                    "Failed to authenticate with GitHub App installation {installation_id}. The installation ID may be invalid or the app may not be installed."
                ));
            }

            // Validate that the installation has access to the target repository or organization
            let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
                return Ok(()); // If no endpoint, skip this validation
            };

            let client = reqwest::Client::builder()
                .user_agent(util::spiceai_user_agent())
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

    pub(crate) async fn create_graphql_client(
        &self,
        tbl: &Arc<dyn GitHubTableArgs>,
    ) -> std::result::Result<GraphQLClient, Box<dyn std::error::Error + Send + Sync>> {
        let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
            return Err(
                "GitHub endpoint not provided. Set github_endpoint or use the default https://api.github.com."
                    .into(),
            );
        };

        let token = self
            .token
            .as_ref()
            .map(|token| Arc::clone(token) as Arc<dyn TokenProvider>);

        let auth_context = token.as_ref().map_or_else(
            || UNAUTHENTICATED_AUTH_CONTEXT.to_string(),
            |t| t.dyn_hash(),
        );

        let rate_controller = get_github_auth_context_rate_controller(auth_context).await;

        let client = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_mins(2))
            .gzip(true)
            .brotli(true)
            .zstd(true)
            .deflate(true)
            .default_headers({
                use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
                let mut headers = HeaderMap::new();
                headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                headers
            })
            .build()
            .boxed()?;

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
        .with_rate_controller(Some(rate_controller))
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
        self.build_gql_table_provider(table_args, context, health_check_query_string)
            .await
            .map(|provider| Arc::new(provider) as Arc<dyn TableProvider>)
    }

    async fn build_gql_table_provider(
        &self,
        table_args: Arc<dyn GitHubTableArgs>,
        context: Option<Arc<dyn GraphQLContext>>,
        health_check_query_string: String,
    ) -> super::DataConnectorResult<GraphQLTableProvider> {
        let connector_component_name = format!("{}", table_args.get_component());
        let graphql_values = table_args.get_graphql_values();
        let client = self.create_graphql_client(&table_args).await.context(
            super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
                connector_component: table_args.get_component(),
            },
        )?;

        let provider_builder = GraphQLTableProviderBuilder::new(client)
            .with_schema_transform(github_gql_raw_schema_cast);

        let provider_builder = if let Some(context) = context.as_ref() {
            provider_builder.with_context(Arc::clone(context))
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

        let initial_build_error = match provider_builder
            .with_health_check_query(health_check_query)
            .build(graphql_values.query.as_ref())
            .await
        {
            Ok(provider) => return Ok(provider),
            Err(e) => e,
        };

        if graphql_values.schema.is_some()
            && (matches!(initial_build_error, graphql::Error::RateLimited { .. })
                || graphql::is_retriable_error(&initial_build_error))
        {
            tracing::warn!(
                "GitHub GraphQL preflight validation failed for {connector_component_name}; continuing with the configured schema so the dataset can recover on a later refresh: {initial_build_error}"
            );

            let fallback_client = self.create_graphql_client(&table_args).await.context(
                super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                    connector_component: table_args.get_component(),
                },
            )?;

            let fallback_builder = GraphQLTableProviderBuilder::new(fallback_client)
                .with_schema_transform(github_gql_raw_schema_cast);
            let fallback_builder = if let Some(context) = context {
                fallback_builder.with_context(context)
            } else {
                fallback_builder
            };

            return fallback_builder
                .build_without_validation(graphql_values.query.as_ref())
                .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    connector_component: table_args.get_component(),
                    source: e.into(),
                });
        }

        Err(
            if matches!(initial_build_error, graphql::Error::RateLimited { .. }) {
                DataConnectorError::RateLimited {
                    dataconnector: "github".to_string(),
                    connector_component: table_args.get_component(),
                    source: initial_build_error.into(),
                }
            } else {
                DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    connector_component: table_args.get_component(),
                    source: initial_build_error.into(),
                }
            },
        )
    }

    pub(crate) fn create_rest_client(
        &self,
    ) -> std::result::Result<GithubRestClient, Box<dyn std::error::Error + Send + Sync>> {
        let token = self
            .token
            .as_ref()
            .map(|token| Arc::clone(token) as Arc<dyn TokenProvider>);

        if token.is_none() {
            tracing::debug!(
                "No GitHub token configured; using unauthenticated GitHub REST API access with public rate limits"
            );
        }

        GithubRestClient::new(
            token,
            Arc::clone(&self.rate_limiter) as Arc<dyn RateLimiter>,
        )
        .map_err(Into::into)
    }

    async fn create_files_table_provider(
        &self,
        owner: &str,
        repo: &str,
        requested_ref: Option<&str>,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
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
                requested_ref,
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

    async fn create_commits_table_provider(
        &self,
        owner: &str,
        repo: &str,
        requested_ref: Option<&str>,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_args = Arc::new(CommitsTableArgs {
            owner: owner.to_string(),
            repo: repo.to_string(),
            requested_ref: requested_ref.map(ToString::to_string),
            component: ConnectorComponent::from(dataset),
        });

        let gql_table_args = Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>;
        let gql_context = Arc::clone(&table_args) as Arc<dyn GraphQLContext>;

        let delegate_provider = self
            .build_gql_table_provider(
                Arc::clone(&gql_table_args),
                Some(gql_context),
                Github::get_health_check_for_owner_and_repo(owner, repo),
            )
            .await?;
        let client = delegate_provider.client();
        let delegate = Arc::new(delegate_provider) as Arc<dyn TableProvider>;

        let rest_client =
            self.create_rest_client()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                })?;

        Ok(Arc::new(CommitsTableProvider::new(
            delegate,
            client,
            rest_client,
            table_args,
        )) as Arc<dyn TableProvider>)
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
    ParameterSpec::component("workflow_logs")
        .description("Whether to download and include workflow run logs. Set to 'enabled' to download logs for each workflow run. Defaults to 'disabled'.")
        .default("disabled"),
    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent GitHub HTTP requests for this authentication context. If unset, falls back to runtime.source_rate_control.github_concurrent_connections_limit, deprecated runtime.params.github_max_concurrent_connections, or the connector default."),
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

        let connector_component = params.component.clone();

        let dataset_max_concurrent_requests = match params
            .parameters
            .get("max_concurrent_requests")
            .expose()
            .ok()
            .map(str::trim)
        {
            Some("") | None => None,
            Some(value) => match value.parse::<usize>() {
                Ok(0) => {
                    let error = DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "github".to_string(),
                        connector_component,
                        message: format!(
                            "The '{}' parameter must be greater than 0.",
                            params.parameters.user_param("max_concurrent_requests")
                        ),
                    };
                    return Box::pin(async move { Err(Box::new(error) as _) });
                }
                Ok(value) => Some(value),
                Err(source) => {
                    let error = DataConnectorError::InvalidConfiguration {
                        dataconnector: "github".to_string(),
                        message: format!(
                            "The '{}' parameter must be a positive integer.",
                            params.parameters.user_param("max_concurrent_requests")
                        ),
                        connector_component,
                        source: source.into(),
                    };
                    return Box::pin(async move { Err(Box::new(error) as _) });
                }
            },
        };

        let app_max_concurrent_connections = if dataset_max_concurrent_requests.is_some() {
            None
        } else {
            match resolve_runtime_github_concurrent_connections_limit(
                params.app.as_deref(),
                &connector_component,
            ) {
                Ok(value) => value,
                Err(error) => return Box::pin(async move { Err(Box::new(error) as _) }),
            }
        };

        let max_concurrent_connections = dataset_max_concurrent_requests
            .or(app_max_concurrent_connections)
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
                match limits.get(&key) {
                    Some((existing_limit, semaphore))
                        if *existing_limit == max_concurrent_connections =>
                    {
                        Arc::clone(semaphore)
                    }
                    Some((existing_limit, _)) => {
                        return Err(Box::new(DataConnectorError::InvalidConfigurationNoSource {
                            dataconnector: "github".to_string(),
                            connector_component,
                            message: format!(
                                "Multiple GitHub datasets share the same authentication context with different concurrency limits ({existing_limit} and {max_concurrent_connections}). Use the same max_concurrent_requests value for datasets sharing a GitHub token or installation."
                            ),
                        }) as _);
                    }
                    None => {
                        let semaphore = Arc::new(Semaphore::new(max_concurrent_connections));
                        limits.insert(key, (max_concurrent_connections, Arc::clone(&semaphore)));
                        semaphore
                    }
                }
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

register_data_connector!("github", GithubFactory);

fn resolve_runtime_github_concurrent_connections_limit(
    app: Option<&app::App>,
    connector_component: &ConnectorComponent,
) -> Result<Option<usize>, DataConnectorError> {
    let Some(app) = app else {
        return Ok(None);
    };

    let source_limit = app
        .runtime
        .source_rate_control
        .as_ref()
        .and_then(|config| config.github_concurrent_connections_limit);
    let legacy_limit = app
        .runtime
        .params
        .get("github_max_concurrent_connections")
        .map(String::as_str);

    if legacy_limit.is_some() {
        tracing::warn!(
            "`{LEGACY_RUNTIME_GITHUB_MAX_CONCURRENT_CONNECTIONS}` is deprecated; use `{RUNTIME_SOURCE_GITHUB_CONCURRENT_CONNECTIONS_LIMIT}` instead."
        );
    }

    if let Some(source_limit) = source_limit {
        if source_limit == 0 {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "github".to_string(),
                connector_component: connector_component.clone(),
                message: format!(
                    "`{RUNTIME_SOURCE_GITHUB_CONCURRENT_CONNECTIONS_LIMIT}` must be greater than 0."
                ),
            });
        }

        return Ok(Some(source_limit));
    }

    Ok(legacy_limit
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0))
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

/// Default number of comments fetched per pull request when the user does
/// not override `github_max_comments_fetched`.
///
/// Lowered from the previous hard cap of 75 to 25 to keep the GitHub GraphQL
/// node count well under the 500,000 node hard limit for queries that enable
/// `include_comments` (each PR page multiplies this by up to 20 review threads).
///
/// See: <https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#node-limit>
const DEFAULT_MAX_COMMENTS_FETCHED: u32 = 25;

/// Hard upper bound on `github_max_comments_fetched`. Values above this cap
/// are clamped to protect against GitHub secondary rate limits and the 500,000
/// node hard limit on a single GraphQL query.
const MAX_COMMENTS_FETCHED: u32 = 75;

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
    "workflows",
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
                source: format!(
                    "Invalid GitHub query mode '{e}'. Use one of: auto, search. See {GITHUB_CONNECTOR_DOCS_URL}#common-parameters."
                )
                .into(),
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
                source: format!(
                    "Invalid GitHub dataset path '{path}'. Use github:github.com/<owner>/<repo>/<resource> or github:github.com/<owner>/<resource>. See {GITHUB_CONNECTOR_DOCS_URL}#common-configuration."
                )
                .into(),
            });
        };

        match (parsed.resource_type, parsed.repo) {
            ("pulls", Some(repo)) => {
                let max_comments_fetched = match max_comments_fetched.unwrap_or(DEFAULT_MAX_COMMENTS_FETCHED) {
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
                    component: component.clone(),
                    include_comments: include_comments.unwrap_or(PullRequestCommentType::None),
                    max_comments_fetched,
                });

                // Validate that the computed query stays under GitHub's 500K
                // node hard limit before we bother opening a connection.
                table_args.check_node_limit().map_err(|message| {
                    DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "github".to_string(),
                        connector_component: component.clone(),
                        message,
                    }
                })?;

                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                    Github::get_health_check_for_owner_and_repo(parsed.owner, repo)
                )
                .await
            }
            ("commits", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "commits", &component);
                self.create_commits_table_provider(
                    parsed.owner,
                    repo,
                    parsed.remaining.as_deref(),
                    dataset,
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
                self.create_gql_table_provider(Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>, Some(table_args), Github::get_health_check_for_owner_and_repo(parsed.owner, repo)).await
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
            ("workflows", Some(repo)) => {
                warn_if_provided(pull_request_specific_params, "workflows", &component);

                let client = self.create_rest_client().context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                    connector_component: component.clone(),
                })?;

                // Check if there's a remaining path (workflow_id/runs)
                match parsed.remaining.as_deref() {
                    None | Some("") => {
                        // No workflow ID specified - list all workflows
                        // Warn if github_workflow_logs is set since it's not applicable
                        if dataset
                            .params
                            .get("github_workflow_logs")
                            .is_some_and(|value| value.as_str() == "enabled")
                        {
                            tracing::warn!(
                                "The 'github_workflow_logs' parameter is only supported when retrieving workflow runs (e.g., github.com/{}/{}/workflows/workflow.yml/runs), not when listing workflows. It will be ignored for {component}.",
                                parsed.owner,
                                repo
                            );
                        }

                        Ok(Arc::new(
                            workflows::WorkflowsTableProvider::new(
                                client,
                                parsed.owner,
                                repo,
                                dataset,
                            )
                            .await?,
                        ) as Arc<dyn TableProvider>)
                    }
                    Some(remaining) => {
                        // Workflow ID specified - parse workflow_id/runs
                        let parts: Vec<&str> = remaining.split('/').collect();
                        if parts.len() != 2 || parts[1] != "runs" {
                            return Err(DataConnectorError::UnableToGetReadProvider {
                                dataconnector: "github".to_string(),
                                source: format!(
                                    "Invalid GitHub workflow path '{path}'. Expected format: github.com/<owner>/<repo>/workflows/<workflow_file.yml>/runs. See {GITHUB_CONNECTOR_DOCS_URL}."
                                )
                                .into(),
                                connector_component: component,
                            });
                        }

                        let workflow_id = parts[0];

                        let fetch_logs = dataset
                            .params
                            .get("github_workflow_logs")
                            .is_some_and(|value| value.as_str() == "enabled");

                        Ok(Arc::new(
                            workflow_runs::WorkflowRunsTableProvider::new(
                                client,
                                parsed.owner,
                                repo,
                                workflow_id,
                                fetch_logs,
                                dataset,
                            )
                            .await?,
                        ) as Arc<dyn TableProvider>)
                    }
                }
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
                    Some(table_args),
                    Github::get_health_check_for_org(parsed.owner)
                )
                .await
            }
            (resource_type, _) => {
                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    source: format!(
                        "Invalid GitHub table type '{resource_type}'. See {GITHUB_CONNECTOR_DOCS_URL}#common-configuration for supported resources."
                    )
                    .into(),
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

pub(crate) fn scalar_utf8_value(scalar: &ScalarValue) -> Option<&str> {
    match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(v.as_str()),
        _ => None,
    }
}

pub(crate) fn expr_to_match(expr: &Expr) -> Option<(Column, ScalarValue, Operator)> {
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

        let value = match &value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => {
                if column.name == "state" {
                    v.to_lowercase()
                } else {
                    v.clone()
                }
            }
            ScalarValue::TimestampMillisecond(Some(millis), _) => {
                let dt = Utc.timestamp_millis_opt(*millis);
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

#[cfg(test)]
mod tests {
    use super::{
        Github, GithubFactory, PARAMETERS, parse_github_path, sanitize_github_validation_body,
    };
    use crate::Runtime;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::{
        ConnectorComponent, ConnectorParams, DataConnectorError, DataConnectorFactory,
    };
    use crate::parameters::Parameters;
    use runtime_secrets::Secrets;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn github_connector_params(
        dataset_name: &str,
        token: &str,
        extra: &[(&str, &str)],
    ) -> ConnectorParams {
        github_connector_params_with_runtime(
            dataset_name,
            token,
            extra,
            spicepod::component::runtime::Runtime::default(),
        )
        .await
    }

    async fn github_connector_params_with_runtime(
        dataset_name: &str,
        token: &str,
        extra: &[(&str, &str)],
        app_runtime: spicepod::component::runtime::Runtime,
    ) -> ConnectorParams {
        let mut params = vec![("github_token".to_string(), token.to_string().into())];
        params.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string().into())),
        );

        let parameters = Parameters::try_new(
            "connector github",
            params,
            "github",
            Arc::new(RwLock::new(Secrets::default())),
            PARAMETERS,
        )
        .await
        .expect("test GitHub parameters should be valid");

        let app = app::AppBuilder::new(dataset_name.to_string())
            .with_runtime(app_runtime)
            .build();
        let runtime = Arc::new(Runtime::builder().with_app(app.clone()).build().await);
        let app = Arc::new(app);
        let dataset = DatasetBuilder::try_new(
            "github:github.com/spiceai/spiceai/issues".to_string(),
            dataset_name,
        )
        .expect("test GitHub dataset should be valid")
        .with_app(Arc::clone(&app))
        .with_runtime(Arc::clone(&runtime))
        .build()
        .expect("test GitHub dataset should build");

        ConnectorParams {
            parameters,
            unsupported_type_action: None,
            component: ConnectorComponent::from(&dataset),
            app: Some(app),
            runtime: Some(runtime),
            io_runtime: tokio::runtime::Handle::current(),
        }
    }

    fn github_available_permits(connector: &Arc<dyn crate::dataconnector::DataConnector>) -> usize {
        connector
            .as_any()
            .downcast_ref::<Github>()
            .expect("connector should be GitHub")
            .semaphore
            .available_permits()
    }

    fn expect_invalid_configuration_message(
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> String {
        let error = error
            .downcast::<DataConnectorError>()
            .expect("error should be a DataConnectorError");

        match *error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. }
            | DataConnectorError::InvalidConfiguration { message, .. } => message,
            other => panic!("expected GitHub invalid configuration error, got: {other}"),
        }
    }

    #[test]
    fn test_sanitize_github_validation_body_normalizes_crlf() {
        assert_eq!(
            sanitize_github_validation_body("first line\r\nsecond\nthird\tfourth"),
            "first line second third fourth"
        );
    }

    #[test]
    fn test_parse_github_path_preserves_commits_ref_suffix() {
        let parsed = parse_github_path("github.com/spiceai/spiceai/commits/feature/ref-test")
            .expect("path should parse");

        assert_eq!(parsed.owner, "spiceai");
        assert_eq!(parsed.repo, Some("spiceai"));
        assert_eq!(parsed.resource_type, "commits");
        assert_eq!(parsed.remaining.as_deref(), Some("feature/ref-test"));
    }

    #[test]
    fn test_parse_github_path_allows_files_without_explicit_ref() {
        let parsed =
            parse_github_path("github.com/spiceai/spiceai/files").expect("path should parse");

        assert_eq!(parsed.owner, "spiceai");
        assert_eq!(parsed.repo, Some("spiceai"));
        assert_eq!(parsed.resource_type, "files");
        assert!(parsed.remaining.is_none());
    }

    #[tokio::test]
    async fn test_github_rejects_invalid_max_concurrent_requests() {
        let params = github_connector_params(
            "github_invalid_concurrency",
            "github-invalid-concurrency-token",
            &[("max_concurrent_requests", "0")],
        )
        .await;

        let Err(error) = GithubFactory::new().create(params).await else {
            panic!("zero GitHub max_concurrent_requests should be rejected");
        };
        let message = expect_invalid_configuration_message(error);

        assert!(
            message.contains("must be greater than 0"),
            "expected zero-limit validation error, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_github_uses_source_rate_control_concurrency_limit() {
        let factory = GithubFactory::new();
        let app_runtime = spicepod::component::runtime::Runtime {
            source_rate_control: Some(spicepod::component::runtime::SourceRateControl {
                github_concurrent_connections_limit: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };

        let params = github_connector_params_with_runtime(
            "github_source_rate_control_concurrency",
            "github-source-rate-control-token",
            &[],
            app_runtime,
        )
        .await;

        let connector = factory
            .create(params)
            .await
            .expect("GitHub connector should be created");

        assert_eq!(github_available_permits(&connector), 2);
    }

    #[tokio::test]
    async fn test_github_source_rate_control_overrides_legacy_runtime_param() {
        let factory = GithubFactory::new();
        let mut app_runtime = spicepod::component::runtime::Runtime {
            source_rate_control: Some(spicepod::component::runtime::SourceRateControl {
                github_concurrent_connections_limit: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        app_runtime.params.insert(
            "github_max_concurrent_connections".to_string(),
            "3".to_string(),
        );

        let params = github_connector_params_with_runtime(
            "github_source_rate_control_overrides_legacy",
            "github-source-rate-control-overrides-legacy-token",
            &[],
            app_runtime,
        )
        .await;

        let connector = factory
            .create(params)
            .await
            .expect("GitHub connector should be created");

        assert_eq!(github_available_permits(&connector), 2);
    }

    #[tokio::test]
    async fn test_github_keeps_legacy_runtime_concurrency_param() {
        let factory = GithubFactory::new();
        let mut app_runtime = spicepod::component::runtime::Runtime::default();
        app_runtime.params.insert(
            "github_max_concurrent_connections".to_string(),
            "3".to_string(),
        );

        let params = github_connector_params_with_runtime(
            "github_legacy_runtime_concurrency",
            "github-legacy-runtime-concurrency-token",
            &[],
            app_runtime,
        )
        .await;

        let connector = factory
            .create(params)
            .await
            .expect("GitHub connector should be created");

        assert_eq!(github_available_permits(&connector), 3);
    }

    #[tokio::test]
    async fn test_github_rejects_zero_source_rate_control_concurrency_limit() {
        let app_runtime = spicepod::component::runtime::Runtime {
            source_rate_control: Some(spicepod::component::runtime::SourceRateControl {
                github_concurrent_connections_limit: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };

        let params = github_connector_params_with_runtime(
            "github_zero_source_rate_control_concurrency",
            "github-zero-source-rate-control-token",
            &[],
            app_runtime,
        )
        .await;

        let Err(error) = GithubFactory::new().create(params).await else {
            panic!("zero source_rate_control GitHub limit should be rejected");
        };
        let message = expect_invalid_configuration_message(error);

        assert!(
            message.contains("must be greater than 0"),
            "expected zero-limit validation error, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_github_rejects_conflicting_shared_auth_concurrency_limits() {
        let factory = GithubFactory::new();
        let token = "github-conflicting-concurrency-token";

        let first = github_connector_params(
            "github_conflicting_concurrency_first",
            token,
            &[("max_concurrent_requests", "2")],
        )
        .await;
        factory
            .create(first)
            .await
            .expect("first GitHub connector should be created");

        let second = github_connector_params(
            "github_conflicting_concurrency_second",
            token,
            &[("max_concurrent_requests", "3")],
        )
        .await;
        let Err(error) = factory.create(second).await else {
            panic!("conflicting GitHub concurrency limits should be rejected");
        };
        let message = expect_invalid_configuration_message(error);

        assert!(
            message.contains("different concurrency limits"),
            "expected shared auth concurrency conflict, got: {message}"
        );
    }
}
