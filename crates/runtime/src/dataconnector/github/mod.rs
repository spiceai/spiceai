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

use crate::component::dataset::Dataset;
use arrow::array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chrono::{offset::LocalResult, SecondsFormat, TimeZone, Utc};
use commits::CommitsTableArgs;
use data_components::{
    github::{self, GithubFilesTableProvider, GithubRestClient},
    graphql::{
        self,
        builder::GraphQLClientBuilder,
        client::{GraphQLClient, GraphQLQuery, PaginationParameters},
        provider::GraphQLTableProviderBuilder,
        FilterPushdownResult, GraphQLContext,
    },
    rate_limit::RateLimiter,
    token_provider::{StaticTokenProvider, TokenProvider},
};
use datafusion::{
    common::Column,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{Operator, TableProviderFilterPushDown},
    prelude::Expr,
    scalar::ScalarValue,
};
use github_app_token_provider::GitHubAppTokenProvider;
use globset::{Glob, GlobSet, GlobSetBuilder};
use graphql_parser::query::{
    Definition, InlineFragment, OperationDefinition, Query, Selection, SelectionSet,
};
use issues::IssuesTableArgs;
use pull_requests::PullRequestTableArgs;
use rate_limit::GitHubRateLimiter;
use snafu::ResultExt;
use stargazers::StargazersTableArgs;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::{any::Any, future::Future, pin::Pin, str::FromStr, sync::Arc};
use url::Url;

use super::{
    graphql::default_spice_client, ConnectorComponent, ConnectorParams, DataConnector,
    DataConnectorError, DataConnectorFactory, ParameterSpec, Parameters,
};

mod commits;
mod github_app_token_provider;
mod issues;
mod pull_requests;
mod rate_limit;
mod stargazers;

pub struct Github {
    params: Parameters,
    token: Option<Arc<dyn TokenProvider>>,
    rate_limiter: Arc<GitHubRateLimiter>,
}

pub struct GitHubTableGraphQLParams {
    /// The GraphQL query string
    query: Arc<str>,

    /// The JSON pointer to the data in the response. If not provided, it will be inferred from the query.
    json_pointer: Option<&'static str>,
    /// The depth to unnest the data
    unnest_depth: usize,
    /// The GraphQL schema of the response data, if available
    schema: Option<SchemaRef>,
}

impl GitHubTableGraphQLParams {
    #[must_use]
    pub fn new(
        query: Arc<str>,
        json_pointer: Option<&'static str>,
        unnest_depth: usize,
        schema: Option<SchemaRef>,
    ) -> Self {
        Self {
            query,
            json_pointer,
            unnest_depth,
            schema,
        }
    }
}

pub trait GitHubTableArgs: Send + Sync {
    fn get_graphql_values(&self) -> GitHubTableGraphQLParams;
    fn get_component(&self) -> ConnectorComponent;
}

impl Github {
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
            gql_client_params.unnest_depth,
        )
        .with_token_provider(token)
        .with_json_pointer(gql_client_params.json_pointer)
        .with_schema(gql_client_params.schema)
        .with_rate_limiter(Some(Arc::clone(&self.rate_limiter) as Arc<dyn RateLimiter>))
        .build(client)
        .boxed()
    }

    async fn create_gql_table_provider(
        &self,
        table_args: Arc<dyn GitHubTableArgs>,
        context: Option<Arc<dyn GraphQLContext>>,
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

        Ok(Arc::new(
            provider_builder
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
            Some(token) => Ok(GithubRestClient::new(
                token,
                Arc::clone(&self.rate_limiter) as Arc<dyn RateLimiter>,
            )),
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

        Ok(Arc::new(
            GithubFilesTableProvider::new(
                client,
                owner,
                repo,
                tree_sha,
                include,
                dataset.is_accelerated(),
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
        if let DataType::List(inner_field) = field.data_type() {
            if let DataType::Struct(struct_fields) = inner_field.data_type() {
                if struct_fields.len() == 1 {
                    let (new_column, new_field) =
                        arrow_tools::record_batch::to_primitive_type_list(column, field)?;
                    fields.push(new_field);
                    columns.push(new_column);
                    continue;
                }
            }
        }

        fields.push(Arc::clone(field));
        columns.push(Arc::clone(column));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(std::convert::Into::into)
}

#[derive(Default, Copy, Clone)]
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
    ParameterSpec::connector("token")
        .description("A Github token.")
        .secret(),
    ParameterSpec::connector("client_id")
        .description("The Github App Client ID.")
        .secret(),
    ParameterSpec::connector("private_key")
        .description("The Github App private key.")
        .secret(),
    ParameterSpec::connector("installation_id")
        .description("The Github App installation ID.")
        .secret(),
    ParameterSpec::connector("query_mode")
        .description(
            "Specify what search mode (REST, GraphQL, Search API) to use when retrieving results.",
        )
        .default("auto"),
    ParameterSpec::connector("endpoint")
        .description("The Github API endpoint.")
        .default("https://api.github.com"),
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
        let token = params.parameters.get("token").expose().ok();
        let client_id = params.parameters.get("client_id").expose().ok();
        let private_key = params.parameters.get("private_key").expose().ok();
        let installation_id = params.parameters.get("installation_id").expose().ok();

        let token_provider: Option<Arc<dyn TokenProvider>> =
            match (token, client_id, private_key, installation_id) {
                (Some(token), _, _, _) => Some(Arc::new(StaticTokenProvider::new(token.into()))),

                (None, Some(client_id), Some(private_key), Some(installation_id)) => {
                    Some(Arc::new(GitHubAppTokenProvider::new(
                        Arc::from(client_id),
                        Arc::from(private_key),
                        Arc::from(installation_id),
                    )))
                }

                _ => None,
            };

        Box::pin(async move {
            Ok(Arc::new(Github {
                params: params.parameters,
                token: token_provider,
                rate_limiter: Arc::new(GitHubRateLimiter::new()),
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
        let mut parts = path.split('/');

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

        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("github.com"), Some(owner), Some(repo), Some("pulls")) => {
                let table_args = Arc::new(PullRequestTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    query_mode,
                    component: ConnectorComponent::from(dataset),
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                )
                .await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("commits")) => {
                let table_args = Arc::new(CommitsTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    component: ConnectorComponent::from(dataset),
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                )
                .await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("issues")) => {
                let table_args = Arc::new(IssuesTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    query_mode,
                    component: ConnectorComponent::from(dataset),
                });
                self.create_gql_table_provider(
                    Arc::clone(&table_args) as Arc<dyn GitHubTableArgs>,
                    Some(table_args),
                )
                .await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("stargazers")) => {
                let table_args = Arc::new(StargazersTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    component: ConnectorComponent::from(dataset),
                });
                self.create_gql_table_provider(table_args, None).await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("files")) => {
                self.create_files_table_provider(owner, repo, parts.next(), dataset)
                    .await
            }
            (Some("github.com"), Some(_), Some(_), Some(invalid_table)) => {
                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    source: format!("Invalid GitHub table type: {invalid_table}.\nEnsure a valid table type is used, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration").into(),
                    connector_component: ConnectorComponent::from(dataset),
                })
            }
            (_, Some(owner), Some(repo), _) => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: format!("The dataset `from` must start with 'github.com/{owner}/{repo}'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration").into(),
            }),
            _ => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: "Invalid GitHub path provided in the dataset 'from'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration".into(),
            }),
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
                (Expr::Column(column), Expr::Literal(value))
                | (Expr::Literal(value), Expr::Column(column)) => {
                    Some((column, value, binary_expr.op))
                }
                _ => None,
            }
        }
        Expr::Like(like_expr) => match (*like_expr.expr.clone(), *like_expr.pattern.clone()) {
            (Expr::Column(column), Expr::Literal(value))
            | (Expr::Literal(value), Expr::Column(column)) => {
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
                    (Expr::Column(column), Expr::Literal(value))
                    | (Expr::Literal(value), Expr::Column(column)) => {
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

    if let Some((column, value, op)) = column_matches {
        if let Some(column_support) = GITHUB_FILTER_PUSHDOWNS_SUPPORTED.get(column.name.as_str()) {
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
                                }
                            }
                        },
                        _ => {
                            return FilterPushdownResult {
                                filter_pushdown: TableProviderFilterPushDown::Unsupported,
                                expr: expr.clone(),
                                context: None,
                            }
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
            graphql_parser::query::Selection::FragmentSpread(_) => continue,
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
