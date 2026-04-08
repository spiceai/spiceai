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

use crate::{dataconnector::ConnectorComponent, datafusion::error::find_datafusion_root};
use async_trait::async_trait;

use super::{
    GitHubTableArgs, GitHubTableGraphQLParams, commits_inject_parameters, expr_to_match,
    filter_pushdown, inject_parameters, scalar_utf8_value,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::{
    github::{GithubRef, GithubRestClient, error_checker},
    graphql::{
        ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
        client::{
            DuplicateBehavior, GraphQLClient, GraphQLQuery, UnnestBehavior,
            unnest_json_object_to_depth,
        },
    },
};
use datafusion::{
    catalog::Session,
    datasource::{MemTable, TableProvider, TableType},
    error::DataFusionError,
    logical_expr::Operator,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use futures::TryStreamExt;
use graphql_parser::query::{Definition, InlineFragment, OperationDefinition, Query, Selection};
use serde_json::{Map, Value};
use std::sync::Arc;

const COMMITS_JSON_POINTER: &str = "/data/repository";

// https://docs.github.com/en/graphql/reference/objects#commit
#[derive(Debug, Clone)]
pub struct CommitsTableArgs {
    pub owner: String,
    pub repo: String,
    pub requested_ref: Option<String>,
    pub component: ConnectorComponent,
}

impl GraphQLContext for CommitsTableArgs {
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        Ok(commits_filter_pushdown(expr))
    }

    fn inject_parameters(
        &self,
        filters: &[FilterPushdownResult],
        query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        let requested_ref = self.requested_ref.clone().or_else(|| {
            match ref_fetch_mode_from_filter_results(filters) {
                RefFetchMode::Exact(ref_name) => Some(ref_name),
                RefFetchMode::None | RefFetchMode::Dynamic => None,
            }
        });

        if let Some(ref_name) = requested_ref.as_deref() {
            inject_commit_ref_parameter(query, ref_name)?;
        }

        let history_filters = filters
            .iter()
            .filter(|filter| {
                filter
                    .context
                    .as_deref()
                    .is_none_or(|context| !context.starts_with("ref:"))
            })
            .cloned()
            .collect::<Vec<_>>();

        inject_parameters(
            "history",
            commits_inject_parameters,
            &history_filters,
            query,
        )
        .map_err(find_datafusion_root)
    }

    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(5)
    }
}

impl GitHubTableArgs for CommitsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let selected_ref_query = format!(
            r"selected_ref: defaultBranchRef {{
                        {selected_ref_fields}
                    }}",
            selected_ref_fields = selected_ref_fields(),
        );

        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    default_ref: defaultBranchRef {{
                        ref: name
                    }}
                    {selected_ref_query}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            selected_ref_query = selected_ref_query,
        );
        GitHubTableGraphQLParams::new(
            query.into(),
            Some(COMMITS_JSON_POINTER),
            UnnestBehavior::Custom(Box::new(custom_unnestter)),
            Some(gql_schema()),
        )
    }
}

pub struct CommitsTableProvider {
    delegate: Arc<dyn TableProvider>,
    client: Arc<GraphQLClient>,
    rest_client: GithubRestClient,
    table_args: Arc<CommitsTableArgs>,
    schema: SchemaRef,
}

impl std::fmt::Debug for CommitsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitsTableProvider")
            .field("table_args", &self.table_args)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl CommitsTableProvider {
    pub fn new(
        delegate: Arc<dyn TableProvider>,
        client: GraphQLClient,
        rest_client: GithubRestClient,
        table_args: Arc<CommitsTableArgs>,
    ) -> Self {
        Self {
            schema: delegate.schema(),
            delegate,
            client: Arc::new(client),
            rest_client,
            table_args,
        }
    }

    async fn fetch_commits_for_ref(
        &self,
        pushdown_filters: &[FilterPushdownResult],
        ref_name: &str,
        limit: Option<usize>,
    ) -> std::result::Result<Vec<arrow::array::RecordBatch>, Box<dyn std::error::Error + Send + Sync>>
    {
        let mut ref_args = self.table_args.as_ref().clone();
        ref_args.requested_ref = Some(ref_name.to_string());

        let graphql_values = ref_args.get_graphql_values();
        let mut query = GraphQLQuery::try_from(Arc::clone(&graphql_values.query))?;
        ref_args.inject_parameters(pushdown_filters, &mut query)?;

        Arc::clone(&self.client)
            .execute_paginated(
                query,
                Arc::clone(&self.schema),
                Arc::clone(&self.schema),
                limit,
                ref_args.error_checker(),
                ref_args.query_cost(),
            )
            .try_collect()
            .await
            .map_err(Into::into)
    }

    async fn fetch_commits_for_requested_ref(
        &self,
        pushdown_filters: &[FilterPushdownResult],
        requested_ref: &str,
        limit: Option<usize>,
    ) -> std::result::Result<Vec<arrow::array::RecordBatch>, Box<dyn std::error::Error + Send + Sync>>
    {
        let candidate_refs = self.resolve_requested_ref_names(requested_ref).await?;
        let mut last_not_found_error = None;

        for candidate_ref in candidate_refs {
            match self
                .fetch_commits_for_ref(pushdown_filters, &candidate_ref, limit)
                .await
            {
                Ok(batches) => return Ok(batches),
                Err(err) if is_resource_not_found_error(err.as_ref()) => {
                    last_not_found_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }

        Err(last_not_found_error.unwrap_or_else(|| {
            data_components::graphql::Error::ResourceNotFound {
                message: format!(
                    "GitHub commits ref {requested_ref:?} was not found or is not accessible. Verify the requested ref exists and is readable."
                ),
            }
            .into()
        }))
    }

    async fn fetch_repository_refs(
        &self,
    ) -> std::result::Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        self.rest_client
            .fetch_qualified_refs(&self.table_args.owner, &self.table_args.repo)
            .await
    }

    async fn resolve_requested_ref_names(
        &self,
        requested_ref: &str,
    ) -> std::result::Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        if requested_ref.starts_with("refs/") {
            return Ok(vec![requested_ref.to_string()]);
        }

        let refs = self.fetch_repository_refs().await?;
        let mut candidate_refs = resolve_requested_ref_candidates(requested_ref, &refs);

        if candidate_refs.is_empty() {
            candidate_refs.push(requested_ref.to_string());
        }

        Ok(candidate_refs)
    }

    async fn resolve_dynamic_refs(
        &self,
        filters: &[Expr],
    ) -> std::result::Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        let mut refs = self.fetch_repository_refs().await?;

        if filters
            .iter()
            .any(|expr| expr_references_ref(expr) && !expr_is_ref_only(expr))
        {
            return Ok(refs);
        }

        let ref_only_filters = filters
            .iter()
            .filter(|expr| expr_is_ref_only(expr))
            .collect::<Vec<_>>();

        if ref_only_filters.is_empty()
            || !ref_only_filters
                .iter()
                .all(|expr| can_evaluate_ref_expr(expr))
        {
            return Ok(refs);
        }

        refs.retain(|git_ref| {
            ref_only_filters
                .iter()
                .all(|expr| evaluate_ref_expr(expr, &git_ref.name).unwrap_or(true))
        });

        Ok(refs)
    }
}

#[async_trait]
impl TableProvider for CommitsTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<
        Vec<datafusion::logical_expr::TableProviderFilterPushDown>,
        DataFusionError,
    > {
        self.delegate.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let pushdown_filters = filters
            .iter()
            .map(|filter| self.table_args.filter_pushdown(filter))
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let requested_ref = self.table_args.requested_ref.clone().or_else(|| {
            match ref_fetch_mode_from_exprs(filters) {
                RefFetchMode::Exact(ref_name) => Some(ref_name),
                RefFetchMode::None | RefFetchMode::Dynamic => None,
            }
        });

        if let Some(requested_ref) = requested_ref.as_deref() {
            let batches = self
                .fetch_commits_for_requested_ref(&pushdown_filters, requested_ref, limit)
                .await
                .map_err(DataFusionError::External)?;
            let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
            return table.scan(state, projection, filters, limit).await;
        }

        if !matches!(ref_fetch_mode_from_exprs(filters), RefFetchMode::Dynamic) {
            return self.delegate.scan(state, projection, filters, limit).await;
        }

        let refs = self
            .resolve_dynamic_refs(filters)
            .await
            .map_err(DataFusionError::External)?;

        let stop_early = limit.is_some() && filters.iter().all(|expr| expr_is_ref_only(expr));
        let mut remaining = limit;
        let mut batches = Vec::new();

        for git_ref in refs {
            let ref_limit = if stop_early { remaining } else { None };
            let ref_batches = self
                .fetch_commits_for_ref(&pushdown_filters, &git_ref.qualified_name, ref_limit)
                .await
                .map_err(DataFusionError::External)?;

            let fetched_rows = ref_batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>();

            if fetched_rows == 0 {
                continue;
            }

            batches.extend(ref_batches);

            if stop_early && let Some(remaining_rows) = remaining.as_mut() {
                *remaining_rows = remaining_rows.saturating_sub(fetched_rows);
                if *remaining_rows == 0 {
                    break;
                }
            }
        }

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        table.scan(state, projection, filters, limit).await
    }
}

fn selected_ref_fields() -> String {
    format!(
        r"
            ref: name
            target {{
                ... on Commit {{
                    {history_query}
                }}
                ... on Tag {{
                    target {{
                        ... on Commit {{
                            {history_query}
                        }}
                    }}
                }}
            }}
        ",
        history_query = history_query(),
    )
}

fn history_query() -> &'static str {
    r"
        history(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                message
                message_head_line: messageHeadline
                message_body: messageBody
                sha: oid
                additions
                deletions
                changed_files: changedFilesIfAvailable
                id
                committed_date: committedDate
                authorName: author {
                    author_name: name
                }
                authorEmail: author {
                    author_email: email
                }
                committerName: committer {
                    committer_name: name
                }
                committerEmail: committer {
                    committer_email: email
                }
                committerDate: committer {
                    committer_date: date
                }
                associated_pull_request_number: associatedPullRequests(first: 1) {
                    nodes {
                        number
                    }
                }
                status: statusCheckRollup {
                    status: state
                }
            }
        }
    "
}

fn commits_filter_pushdown(expr: &Expr) -> FilterPushdownResult {
    if let Some((column, value, op)) = expr_to_match(expr)
        && column.name == "ref"
    {
        let ref_value = scalar_utf8_value(&value).filter(|v| !v.is_empty());
        return match (op, ref_value) {
            (Operator::Eq, Some(ref_value)) => FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Exact,
                expr: expr.clone(),
                context: Some(format!("ref:{ref_value}")),
            },
            _ => FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
                expr: expr.clone(),
                context: None,
            },
        };
    }

    filter_pushdown(expr)
}

fn expr_references_ref(expr: &Expr) -> bool {
    expr.column_refs().iter().any(|column| column.name == "ref")
}

fn expr_is_ref_only(expr: &Expr) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty() && columns.iter().all(|column| column.name == "ref")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefFetchMode {
    None,
    Exact(String),
    Dynamic,
}

fn merge_ref_fetch_modes(current: RefFetchMode, next: RefFetchMode) -> RefFetchMode {
    match (current, next) {
        (RefFetchMode::Dynamic, _) | (_, RefFetchMode::Dynamic) => RefFetchMode::Dynamic,
        (RefFetchMode::Exact(current), RefFetchMode::Exact(next)) if current == next => {
            RefFetchMode::Exact(current)
        }
        (RefFetchMode::Exact(_), RefFetchMode::Exact(_)) => RefFetchMode::Dynamic,
        (RefFetchMode::Exact(current), RefFetchMode::None)
        | (RefFetchMode::None, RefFetchMode::Exact(current)) => RefFetchMode::Exact(current),
        (RefFetchMode::None, RefFetchMode::None) => RefFetchMode::None,
    }
}

fn resolve_requested_ref_candidates(requested_ref: &str, refs: &[GithubRef]) -> Vec<String> {
    let mut candidate_refs = refs
        .iter()
        .filter(|git_ref| git_ref.name == requested_ref)
        .map(|git_ref| git_ref.qualified_name.clone())
        .collect::<Vec<_>>();

    candidate_refs.sort_by_key(|qualified_name| {
        if qualified_name.starts_with("refs/heads/") {
            0
        } else if qualified_name.starts_with("refs/tags/") {
            1
        } else {
            2
        }
    });
    candidate_refs.dedup();
    candidate_refs
}

fn is_resource_not_found_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<data_components::graphql::Error>()
        .is_some_and(|graphql_error| {
            matches!(
                graphql_error,
                data_components::graphql::Error::ResourceNotFound { .. }
            )
        })
}

fn ref_fetch_mode_from_expr(expr: &Expr) -> RefFetchMode {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => merge_ref_fetch_modes(
            ref_fetch_mode_from_expr(binary_expr.left.as_ref()),
            ref_fetch_mode_from_expr(binary_expr.right.as_ref()),
        ),
        _ => {
            if let Some((column, value, op)) = expr_to_match(expr)
                && column.name == "ref"
                && op == Operator::Eq
                && let Some(ref_value) = scalar_utf8_value(&value).filter(|v| !v.is_empty())
            {
                return RefFetchMode::Exact(ref_value.to_string());
            }

            if expr_references_ref(expr) {
                RefFetchMode::Dynamic
            } else {
                RefFetchMode::None
            }
        }
    }
}

fn ref_fetch_mode_from_filter_results(filters: &[FilterPushdownResult]) -> RefFetchMode {
    filters.iter().fold(RefFetchMode::None, |current, filter| {
        merge_ref_fetch_modes(current, ref_fetch_mode_from_expr(&filter.expr))
    })
}

fn ref_fetch_mode_from_exprs(filters: &[Expr]) -> RefFetchMode {
    filters.iter().fold(RefFetchMode::None, |current, filter| {
        merge_ref_fetch_modes(current, ref_fetch_mode_from_expr(filter))
    })
}

fn can_evaluate_ref_expr(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And | Operator::Or) => {
            can_evaluate_ref_expr(binary_expr.left.as_ref())
                && can_evaluate_ref_expr(binary_expr.right.as_ref())
        }
        _ => {
            if let Some((column, value, op)) = expr_to_match(expr) {
                column.name == "ref"
                    && matches!(op, Operator::Eq | Operator::NotEq)
                    && scalar_utf8_value(&value)
                        .filter(|value| !value.is_empty())
                        .is_some()
            } else {
                false
            }
        }
    }
}

fn evaluate_ref_expr(expr: &Expr, ref_name: &str) -> Option<bool> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => Some(
            evaluate_ref_expr(binary_expr.left.as_ref(), ref_name)?
                && evaluate_ref_expr(binary_expr.right.as_ref(), ref_name)?,
        ),
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => Some(
            evaluate_ref_expr(binary_expr.left.as_ref(), ref_name)?
                || evaluate_ref_expr(binary_expr.right.as_ref(), ref_name)?,
        ),
        _ => {
            let (column, value, op) = expr_to_match(expr)?;
            let expected = scalar_utf8_value(&value)?;
            if column.name != "ref" || expected.is_empty() {
                return None;
            }

            match op {
                Operator::Eq => Some(ref_name == expected),
                Operator::NotEq => Some(ref_name != expected),
                _ => None,
            }
        }
    }
}

fn inject_commit_ref_parameter(
    query: &mut GraphQLQuery,
    ref_name: &str,
) -> Result<(), datafusion::error::DataFusionError> {
    let mut all_selections: Vec<&mut Selection<'_, String>> = Vec::new();
    for def in &mut query.ast_mut().definitions {
        let selections = match def {
            Definition::Operation(
                OperationDefinition::Query(Query { selection_set, .. })
                | OperationDefinition::SelectionSet(selection_set),
            ) => &mut selection_set.items,
            _ => continue,
        };

        all_selections.extend(selections.iter_mut());
    }

    while let Some(selection) = all_selections.pop() {
        match selection {
            Selection::InlineFragment(InlineFragment { selection_set, .. }) => {
                selection_set
                    .items
                    .iter_mut()
                    .for_each(|item| all_selections.push(item));
            }
            Selection::Field(field) => {
                if field.alias.as_deref() == Some("selected_ref") {
                    field.name = "ref".to_string();
                    field.arguments = vec![(
                        "qualifiedName".to_string(),
                        graphql_parser::query::Value::String(ref_name.to_string()),
                    )];
                    return Ok(());
                }

                field
                    .selection_set
                    .items
                    .iter_mut()
                    .for_each(|item| all_selections.push(item));
            }
            Selection::FragmentSpread(_) => {}
        }
    }

    Err(datafusion::error::DataFusionError::Execution(
        "GitHub commits query did not contain the expected selected_ref field".to_string(),
    ))
}

fn custom_unnestter(object: &Value) -> Result<Vec<Value>> {
    let Value::Object(repository) = object else {
        return Ok(Vec::new());
    };

    let Some(selected_ref) = selected_ref_from_repository(repository)? else {
        return Ok(Vec::new());
    };

    let ref_value = selected_ref
        .get("ref")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    let history = selected_ref
        .get("target")
        .and_then(commit_history_from_target);

    let Some(Value::Object(history)) = history else {
        return Ok(Vec::new());
    };

    let Some(Value::Array(nodes)) = history.get("nodes") else {
        return Ok(Vec::new());
    };

    let mut commits = Vec::with_capacity(nodes.len());
    for node in nodes {
        let Value::Object(mut commit) = node.clone() else {
            continue;
        };

        if let Some(ref_value) = &ref_value {
            commit.insert("ref".to_string(), Value::String(ref_value.clone()));
        }

        if let Some(pr_value) = commit.remove("associated_pull_request_number") {
            commit.insert(
                "associated_pull_request_number".to_string(),
                extract_associated_pull_request_number(&pr_value),
            );
        }

        let flattened =
            unnest_json_object_to_depth(&Value::Object(commit), 1, &DuplicateBehavior::Error)?;
        commits.extend(flattened);
    }

    Ok(commits)
}

fn selected_ref_from_repository(
    repository: &Map<String, Value>,
) -> Result<Option<&Map<String, Value>>> {
    match repository.get("selected_ref") {
        Some(Value::Object(selected_ref)) => Ok(Some(selected_ref)),
        Some(Value::Null) if repository.get("default_ref").is_some_and(Value::is_object) => {
            Err(data_components::graphql::Error::ResourceNotFound {
                message: "GitHub commits ref was not found or is not accessible. Verify the requested ref exists and is readable.".to_string(),
            })
        }
        _ => Ok(None),
    }
}

fn commit_history_from_target(target: &Value) -> Option<&Value> {
    target.get("history").or_else(|| {
        target
            .get("target")
            .and_then(|tag_target| tag_target.get("history"))
    })
}

fn extract_associated_pull_request_number(value: &Value) -> Value {
    value
        .get("nodes")
        .and_then(Value::as_array)
        .and_then(|nodes| nodes.first())
        .and_then(|node| node.get("number"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sha", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, true),
        Field::new("author_name", DataType::Utf8, true),
        Field::new("author_email", DataType::Utf8, true),
        Field::new("committer_name", DataType::Utf8, true),
        Field::new("committer_email", DataType::Utf8, true),
        Field::new(
            "committed_date",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "committer_date",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("message", DataType::Utf8, true),
        Field::new("message_body", DataType::Utf8, true),
        Field::new("message_head_line", DataType::Utf8, true),
        Field::new("additions", DataType::Int64, true),
        Field::new("deletions", DataType::Int64, true),
        Field::new("changed_files", DataType::Int64, true),
        Field::new("associated_pull_request_number", DataType::Int64, true),
        Field::new("status", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RuntimeBuilder;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use serde_json::json;

    fn create_mock_component(name: &str) -> ConnectorComponent {
        let app = AppBuilder::new("test").build();
        let runtime = tokio::runtime::Runtime::new().expect("to create tokio runtime");
        let spice_runtime = runtime.block_on(async { RuntimeBuilder::new().build().await });

        let dataset = DatasetBuilder::try_new("github".to_string(), name)
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(spice_runtime))
            .build()
            .expect("to create dataset");
        ConnectorComponent::from(&dataset)
    }

    #[test]
    fn test_commits_schema_includes_ref_and_new_metadata() {
        let schema = gql_schema();

        assert_eq!(schema.fields().len(), 17);
        assert_eq!(schema.field(2).name(), "ref");
        assert_eq!(schema.field(5).name(), "committer_name");
        assert_eq!(schema.field(6).name(), "committer_email");
        assert_eq!(schema.field(8).name(), "committer_date");
        assert_eq!(schema.field(14).name(), "changed_files");
        assert_eq!(schema.field(15).name(), "associated_pull_request_number");
        assert_eq!(schema.field(16).name(), "status");
    }

    #[test]
    fn test_commits_query_uses_default_branch_by_default() {
        let args = CommitsTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            requested_ref: None,
            component: create_mock_component("github.com/spiceai/spiceai/commits"),
        };

        let graphql_params = args.get_graphql_values();
        let query = graphql_params.query.as_ref();

        assert!(query.contains("default_ref: defaultBranchRef"));
        assert!(query.contains("selected_ref: defaultBranchRef"));
        assert!(query.contains("changed_files: changedFilesIfAvailable"));
        assert!(query.contains("associated_pull_request_number: associatedPullRequests(first: 1)"));
        assert!(query.contains("status: statusCheckRollup"));
        assert_eq!(graphql_params.json_pointer, Some(COMMITS_JSON_POINTER));
        assert!(matches!(
            graphql_params.unnest_behavior,
            UnnestBehavior::Custom(_)
        ));
    }

    #[test]
    fn test_commits_query_uses_requested_ref() {
        let args = CommitsTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            requested_ref: Some("trunk".to_string()),
            component: create_mock_component("github.com/spiceai/spiceai/commits/trunk"),
        };

        let graphql_params = args.get_graphql_values();
        let query = graphql_params.query.as_ref();
        let mut query_ast =
            GraphQLQuery::try_from(Arc::clone(&graphql_params.query)).expect("query should parse");

        args.inject_parameters(&[], &mut query_ast)
            .expect("requested ref should be injected into the query AST");
        let injected_query = query_ast
            .to_string(None, None)
            .expect("query string should serialize");

        assert!(query.contains("selected_ref: defaultBranchRef"));
        assert!(!query.contains("qualifiedName: \"trunk\""));
        assert!(injected_query.contains("selected_ref: ref(qualifiedName: \"trunk\")"));
    }

    #[test]
    fn test_custom_unnester_inserts_ref_and_flattens_pr_number() {
        let rows = custom_unnestter(&json!({
            "default_ref": {
                "ref": "main"
            },
            "selected_ref": {
                "ref": "trunk",
                "target": {
                    "history": {
                        "nodes": [
                            {
                                "sha": "abc123",
                                "status": {
                                    "status": "SUCCESS"
                                },
                                "authorName": {
                                    "author_name": "Alice"
                                },
                                "associated_pull_request_number": {
                                    "nodes": [
                                        { "number": 42 }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }))
        .expect("custom unnest should succeed");

        assert_eq!(rows.len(), 1);
        let row = rows[0].as_object().expect("row should be an object");
        assert_eq!(row.get("ref"), Some(&Value::String("trunk".to_string())));
        assert_eq!(
            row.get("author_name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            row.get("status"),
            Some(&Value::String("SUCCESS".to_string()))
        );
        assert_eq!(row.get("associated_pull_request_number"), Some(&json!(42)));
    }

    #[test]
    fn test_custom_unnester_rejects_missing_requested_ref() {
        let err = custom_unnestter(&json!({
            "default_ref": {
                "ref": "main"
            },
            "selected_ref": null
        }))
        .expect_err("missing requested ref should fail");

        assert!(
            err.to_string()
                .contains("GitHub commits ref was not found or is not accessible")
        );
    }

    #[test]
    fn test_ref_filter_pushdown_is_supported() {
        let expr = datafusion::prelude::col("ref").eq(datafusion::prelude::lit("trunk"));
        let result = commits_filter_pushdown(&expr);

        assert_eq!(
            result.filter_pushdown,
            datafusion::logical_expr::TableProviderFilterPushDown::Exact
        );
        assert_eq!(result.context.as_deref(), Some("ref:trunk"));
    }

    #[test]
    fn test_ref_filter_pushdown_supports_utf8view() {
        use datafusion::logical_expr::Expr;
        use datafusion::scalar::ScalarValue;
        let expr = datafusion::prelude::col("ref").eq(Expr::Literal(
            ScalarValue::Utf8View(Some("trunk".to_string())),
            None,
        ));
        let result = commits_filter_pushdown(&expr);

        assert_eq!(
            result.filter_pushdown,
            datafusion::logical_expr::TableProviderFilterPushDown::Exact
        );
        assert_eq!(result.context.as_deref(), Some("ref:trunk"));
    }

    #[test]
    fn test_ref_fetch_mode_from_filters_supports_conjunctive_filter() {
        let filters = vec![FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            expr: datafusion::prelude::col("ref")
                .eq(datafusion::prelude::lit("trunk"))
                .and(datafusion::prelude::col("sha").eq(datafusion::prelude::lit("abc123"))),
            context: None,
        }];

        assert_eq!(
            ref_fetch_mode_from_filter_results(&filters),
            RefFetchMode::Exact("trunk".to_string())
        );
    }

    #[test]
    fn test_ref_fetch_mode_from_filters_uses_dynamic_mode_for_multiple_ref_values() {
        let filters = vec![
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Exact,
                expr: datafusion::prelude::col("ref").eq(datafusion::prelude::lit("trunk")),
                context: Some("ref:trunk".to_string()),
            },
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Exact,
                expr: datafusion::prelude::col("ref").eq(datafusion::prelude::lit("main")),
                context: Some("ref:main".to_string()),
            },
        ];

        assert_eq!(
            ref_fetch_mode_from_filter_results(&filters),
            RefFetchMode::Dynamic
        );
    }

    #[test]
    fn test_ref_fetch_mode_from_filters_uses_dynamic_mode_for_ref_or_predicate() {
        let filters = vec![FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            expr: datafusion::prelude::col("ref")
                .eq(datafusion::prelude::lit("trunk"))
                .or(datafusion::prelude::col("ref").eq(datafusion::prelude::lit("main"))),
            context: None,
        }];

        assert_eq!(
            ref_fetch_mode_from_filter_results(&filters),
            RefFetchMode::Dynamic
        );
    }

    #[test]
    fn test_evaluate_ref_expr_supports_not_eq() {
        let expr = datafusion::prelude::col("ref").not_eq(datafusion::prelude::lit("main"));

        assert!(can_evaluate_ref_expr(&expr));
        assert_eq!(evaluate_ref_expr(&expr, "main"), Some(false));
        assert_eq!(evaluate_ref_expr(&expr, "trunk"), Some(true));
    }

    #[test]
    fn test_resolve_requested_ref_candidates_prefers_qualified_branch_names() {
        let refs = vec![
            GithubRef {
                name: "ashtom/fullsessionids".to_string(),
                qualified_name: "refs/heads/ashtom/fullsessionids".to_string(),
            },
            GithubRef {
                name: "v1.0.0".to_string(),
                qualified_name: "refs/tags/v1.0.0".to_string(),
            },
        ];

        assert_eq!(
            resolve_requested_ref_candidates("ashtom/fullsessionids", &refs),
            vec!["refs/heads/ashtom/fullsessionids".to_string()]
        );
    }

    #[test]
    fn test_resolve_requested_ref_candidates_prefers_branches_before_tags() {
        let refs = vec![
            GithubRef {
                name: "release".to_string(),
                qualified_name: "refs/tags/release".to_string(),
            },
            GithubRef {
                name: "release".to_string(),
                qualified_name: "refs/heads/release".to_string(),
            },
        ];

        assert_eq!(
            resolve_requested_ref_candidates("release", &refs),
            vec![
                "refs/heads/release".to_string(),
                "refs/tags/release".to_string()
            ]
        );
    }
}
