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

use crate::component::dataset::Dataset;
use arrow::array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chrono::{offset::LocalResult, SecondsFormat, TimeZone, Utc};
use commits::CommitsTableArgs;
use data_components::{
    github::{GithubFilesTableProvider, GithubRestClient},
    graphql::{
        client::{GraphQLClient, GraphQLQuery, PaginationParameters},
        provider::GraphQLTableProviderBuilder,
        FilterPushdownResult, GraphQLOptimizer,
    },
};
use datafusion::{
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
use lazy_static::lazy_static;
use pull_requests::PullRequestTableArgs;
use snafu::ResultExt;
use stargazers::StargazersTableArgs;
use std::collections::HashMap;
use std::{any::Any, future::Future, pin::Pin, str::FromStr, sync::Arc};
use url::Url;

use super::{
    graphql::default_spice_client, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};

mod commits;
mod issues;
mod pull_requests;
mod stargazers;

pub struct Github {
    params: Parameters,
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
}

impl Github {
    pub(crate) fn create_graphql_client(
        &self,
        tbl: &Arc<dyn GitHubTableArgs>,
    ) -> std::result::Result<GraphQLClient, Box<dyn std::error::Error + Send + Sync>> {
        let access_token = self.params.get("token").expose().ok();

        let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
            return Err("Github 'endpoint' not provided".into());
        };

        let client = default_spice_client("application/json").boxed()?;

        let gql_client_params = tbl.get_graphql_values();

        GraphQLClient::new(
            client,
            Url::parse(&format!("{endpoint}/graphql")).boxed()?,
            gql_client_params.json_pointer,
            access_token,
            None,
            None,
            gql_client_params.unnest_depth,
            gql_client_params.schema,
        )
        .boxed()
    }

    async fn create_gql_table_provider(
        &self,
        table_args: Arc<dyn GitHubTableArgs>,
        optimizer: Option<Arc<dyn GraphQLOptimizer>>,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = self.create_graphql_client(&table_args).context(
            super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            },
        )?;

        let provider_builder = GraphQLTableProviderBuilder::new(client)
            .with_schema_transform(github_gql_raw_schema_cast);

        let provider_builder = if let Some(optimizer) = optimizer {
            provider_builder.with_optimizer(optimizer)
        } else {
            provider_builder
        };

        Ok(Arc::new(
            provider_builder
                .build(table_args.get_graphql_values().query.as_ref())
                .await
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                })?,
        ))
    }

    pub(crate) fn create_rest_client(
        &self,
    ) -> std::result::Result<GithubRestClient, Box<dyn std::error::Error + Send + Sync>> {
        let Some(access_token) = self.params.get("token").expose().ok() else {
            return Err("Github token not provided".into());
        };

        Ok(GithubRestClient::new(access_token))
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
                source: format!("Branch or tag name is required in dataset definition; must be 'github.com/{owner}/{repo}/files/BRANCH_NAME'").into(),
            });
        };

        let client = self
            .create_rest_client()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            })?;

        let include = match self.params.get("include").expose().ok() {
            Some(pattern) => Some(parse_globs(pattern)?),
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
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
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
    fn create(
        &self,
        params: Parameters,
        _metadata: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Github { params }) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "github"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[derive(PartialEq, Eq)]
pub(crate) enum GitHubQueryMode {
    Auto,
    Search,
}

impl std::str::FromStr for GitHubQueryMode {
    type Err = DataConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "search" => Ok(Self::Search),
            s => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: format!("Invalid value for 'github_query_mode' parameter: {s}").into(),
            }),
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
        let path = dataset.path().clone();
        let mut parts = path.split('/');

        let query_mode = dataset
            .params
            .get("github_query_mode")
            .map_or("auto", |v| v);

        let query_mode = GitHubQueryMode::from_str(query_mode)?;

        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("github.com"), Some(owner), Some(repo), Some("pulls")) => {
                let table_args = Arc::new(PullRequestTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                    query_mode,
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
                    source: format!("Invalid Github table type: {invalid_table}").into(),
                })
            }
            (_, Some(owner), Some(repo), _) => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: format!("`from` must start with 'github.com/{owner}/{repo}'").into(),
            }),
            _ => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: "Invalid Github dataset path".into(),
            }),
        }
    }
}

pub fn parse_globs(input: &str) -> super::DataConnectorResult<Arc<GlobSet>> {
    let patterns: Vec<&str> = input.split(&[',', ';'][..]).collect();
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        let trimmed_pattern = pattern.trim();
        if !trimmed_pattern.is_empty() {
            builder.add(
                Glob::new(trimmed_pattern).context(super::InvalidGlobPatternSnafu { pattern })?,
            );
        }
    }

    let glob_set = builder
        .build()
        .context(super::InvalidGlobPatternSnafu { pattern: input })?;
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
lazy_static! {
    static ref GITHUB_FILTER_PUSHDOWNS_SUPPORTED: HashMap<&'static str, GitHubPushdownSupport> = {
        let mut m = HashMap::new();
        m.insert(
            "login",
            GitHubPushdownSupport {
                ops: vec![Operator::Eq, Operator::NotEq],
                remaps: Some(vec![GitHubFilterRemap::Column("author")]),
                uses_modifiers: true
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
                uses_modifiers: false
            },
        );

        m.insert(
            "state",
            GitHubPushdownSupport {
                ops: vec![Operator::Eq, Operator::NotEq],
                remaps: None,
                uses_modifiers: true
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
                uses_modifiers: false
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
                uses_modifiers: true
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
                uses_modifiers: true
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
                uses_modifiers: true
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
                uses_modifiers: true
            },
        );

        m.insert(
            "committed_date",
            GitHubPushdownSupport {
                // e.g. committed_date > '2024-09-14'
                ops: vec![
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
                remaps: Some(vec![
                    GitHubFilterRemap::Operator((Operator::Gt, "since")),
                    GitHubFilterRemap::Operator((Operator::GtEq, "since")),
                    GitHubFilterRemap::Operator((Operator::Lt, "until")),
                    GitHubFilterRemap::Operator((Operator::LtEq, "until"))]),
                uses_modifiers: false
            },
        );

        m
    };
}

#[allow(clippy::too_many_lines)]
pub(crate) fn filter_pushdown(expr: &Expr) -> FilterPushdownResult {
    let column_matches = match expr {
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
        _ => None,
    };

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
                        // "state" in GitHub search is odd
                        // it returns values for CLOSED, MERGED and OPEN
                        // but you can only search with either closed or open (in lowercase as well)
                        if v.to_lowercase() == "merged" {
                            "closed".to_string() // so merged gets remapped to closed
                                                 // and because the filter is Inexact, we expect the Memtable to do the final filter to find only MERGED items
                                                 // not the best, but its better filtering than nothing
                        } else {
                            v.to_lowercase()
                        }
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
            Ok(format!(r#"{v} {arg_additions}"#))
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
    query: &mut GraphQLQuery<'_>,
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
    for def in &mut query.ast.definitions {
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
    let (pagination_parameters, json_pointer) = PaginationParameters::parse(&query.ast);
    query.pagination_parameters = pagination_parameters;
    query.json_pointer = json_pointer.map(Arc::from);

    Ok(())
}
