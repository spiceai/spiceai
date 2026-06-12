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
        provider::GraphQLTableProviderExec,
    },
};
use datafusion::{
    catalog::Session,
    datasource::{MemTable, TableProvider, TableType},
    error::DataFusionError,
    logical_expr::Operator,
    physical_expr::PhysicalExpr,
    physical_plan::{
        ExecutionPlan, empty::EmptyExec, expressions::Column, projection::ProjectionExec,
    },
    prelude::Expr,
};
use futures::TryStreamExt;
use graphql_parser::query::{Definition, InlineFragment, OperationDefinition, Query, Selection};
use serde_json::{Map, Value};
use std::sync::Arc;

const COMMITS_JSON_POINTER: &str = "/data/repository";
const MAX_DYNAMIC_REF_SCAN_REFS: usize = 5;

/// Per-ref commit limit during dynamic scans to avoid overwhelming the GitHub
/// GraphQL API with unbounded pagination across many refs. Matches the
/// `history(first: 100)` page size so each ref costs at most one API call.
const DYNAMIC_SCAN_PER_REF_COMMIT_LIMIT: usize = 100;

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
        let requested_ref = match merge_requested_ref_mode(
            self.requested_ref.as_deref(),
            ref_fetch_mode_from_filter_results(filters),
        ) {
            RefFetchMode::Exact(ref_name) => Some(ref_name),
            RefFetchMode::None | RefFetchMode::Dynamic => None,
            RefFetchMode::Unsatisfiable => {
                return Err(DataFusionError::Execution(
                    "GitHub commits ref predicates are contradictory and cannot match any rows."
                        .to_string(),
                ));
            }
        };

        if let Some(ref_name) = requested_ref.as_deref() {
            inject_commit_ref_parameter(query, ref_name)?;
        }

        let history_filters = filters
            .iter()
            .filter(|filter| {
                filter
                    .context
                    .as_deref()
                    .is_none_or(|context| context != "ref-dynamic" && !context.starts_with("ref:"))
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
        client: Arc<GraphQLClient>,
        rest_client: GithubRestClient,
        table_args: Arc<CommitsTableArgs>,
    ) -> Self {
        Self {
            schema: delegate.schema(),
            delegate,
            client,
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
        // Clear the auto-inferred json_pointer so the client's pointer
        // ("/data/repository") is used instead. The inferred pointer points
        // directly to the nodes array, but the custom_unnestter expects the
        // full repository object to extract selected_ref → target → history.
        query.json_pointer = None;
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

    async fn resolve_requested_ref_name(
        &self,
        requested_ref: &str,
    ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
        for candidate_ref in requested_ref_candidates(requested_ref) {
            if !candidate_ref.starts_with("refs/") {
                continue;
            }

            if let Some(git_ref) = self
                .rest_client
                .fetch_qualified_ref(
                    &self.table_args.owner,
                    &self.table_args.repo,
                    &candidate_ref,
                )
                .await?
            {
                return Ok(git_ref.qualified_name);
            }
        }

        Ok(requested_ref.to_string())
    }

    async fn fetch_repository_refs_bounded(
        &self,
        max_refs: usize,
    ) -> std::result::Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        self.rest_client
            .fetch_qualified_refs_bounded(&self.table_args.owner, &self.table_args.repo, max_refs)
            .await
    }

    async fn resolve_dynamic_refs(
        &self,
        filters: &[Expr],
    ) -> std::result::Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        let mut refs = self
            .fetch_repository_refs_bounded(MAX_DYNAMIC_REF_SCAN_REFS)
            .await?;

        refs.retain(|git_ref| {
            filters
                .iter()
                .all(|expr| evaluate_ref_expr(expr, &git_ref.name).unwrap_or(true))
        });

        Ok(refs)
    }

    async fn scan_for_requested_ref(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        requested_ref: &str,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let resolved_ref = self
            .resolve_requested_ref_name(requested_ref)
            .await
            .map_err(DataFusionError::External)?;

        let mut ref_args = self.table_args.as_ref().clone();
        ref_args.requested_ref = Some(resolved_ref);

        let graphql_values = ref_args.get_graphql_values();
        let mut query = GraphQLQuery::try_from(Arc::clone(&graphql_values.query))
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

        if let Some(json_pointer) = graphql_values.json_pointer {
            query = query.with_json_pointer(Arc::from(json_pointer));
        }

        let pushdown_filters = filters
            .iter()
            .map(|filter| ref_args.filter_pushdown(filter))
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        ref_args.inject_parameters(&pushdown_filters, &mut query)?;

        let graphql_exec = Arc::new(
            GraphQLTableProviderExec::new(
                Arc::clone(&self.client),
                query,
                Arc::clone(&self.schema),
                Arc::clone(&self.schema),
            )
            .with_limit(limit)
            .with_error_checker(ref_args.error_checker())
            .with_query_cost(ref_args.query_cost()),
        );

        if let Some(projection) = projection {
            let mut projection_expr = Vec::with_capacity(projection.len());
            for idx in projection {
                let col_name = self.schema.field(*idx).name();
                projection_expr.push((
                    Arc::new(Column::new(col_name, *idx)) as Arc<dyn PhysicalExpr>,
                    col_name.clone(),
                ));
            }

            let projection_exec = ProjectionExec::try_new(projection_expr, graphql_exec)?;
            return Ok(Arc::new(projection_exec) as Arc<dyn ExecutionPlan>);
        }

        Ok(graphql_exec as Arc<dyn ExecutionPlan>)
    }

    fn empty_scan_plan(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = if let Some(projection) = projection {
            Arc::new(self.schema.project(projection)?)
        } else {
            Arc::clone(&self.schema)
        };

        Ok(Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>)
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
        if limit == Some(0) {
            return self.empty_scan_plan(projection);
        }

        let pushdown_filters = filters
            .iter()
            .map(|filter| self.table_args.filter_pushdown(filter))
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let ref_fetch_mode = merge_requested_ref_mode(
            self.table_args.requested_ref.as_deref(),
            ref_fetch_mode_from_exprs(filters),
        );

        match ref_fetch_mode {
            RefFetchMode::Unsatisfiable => return self.empty_scan_plan(projection),
            RefFetchMode::Exact(requested_ref) => {
                return self
                    .scan_for_requested_ref(projection, filters, limit, &requested_ref)
                    .await;
            }
            RefFetchMode::None => {
                return self.delegate.scan(state, projection, filters, limit).await;
            }
            RefFetchMode::Dynamic => {}
        }

        validate_dynamic_ref_scan(filters)?;

        let refs = self
            .resolve_dynamic_refs(filters)
            .await
            .map_err(DataFusionError::External)?;

        let mut remaining = limit;
        let mut batches = Vec::new();

        for git_ref in refs {
            let per_ref_limit = remaining.map_or(DYNAMIC_SCAN_PER_REF_COMMIT_LIMIT, |r| {
                r.min(DYNAMIC_SCAN_PER_REF_COMMIT_LIMIT)
            });
            let ref_batches = self
                .fetch_commits_for_ref(
                    &pushdown_filters,
                    &git_ref.qualified_name,
                    Some(per_ref_limit),
                )
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

            if let Some(remaining_rows) = remaining.as_mut() {
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
        && op == Operator::Eq
        && let Some(ref_value) = scalar_utf8_value(&value).filter(|v| !v.is_empty())
    {
        // Qualified refs (e.g. "refs/heads/trunk") are resolved by scan() but the
        // ref column projects the short name ("trunk").  Mark as Inexact so
        // DataFusion re-applies the filter — this correctly returns 0 rows when the
        // user filters on a qualified name that differs from the projected short name.
        let pushdown = if ref_value.starts_with("refs/") {
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact
        } else {
            datafusion::logical_expr::TableProviderFilterPushDown::Exact
        };
        return FilterPushdownResult {
            filter_pushdown: pushdown,
            expr: expr.clone(),
            context: Some(format!("ref:{ref_value}")),
        };
    }

    if expr_references_ref(expr) && expr_is_ref_only(expr) {
        return FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            expr: expr.clone(),
            context: Some("ref-dynamic".to_string()),
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
    Unsatisfiable,
}

fn merge_ref_fetch_modes(current: RefFetchMode, next: RefFetchMode) -> RefFetchMode {
    match (current, next) {
        (RefFetchMode::Unsatisfiable, _) | (_, RefFetchMode::Unsatisfiable) => {
            RefFetchMode::Unsatisfiable
        }
        (RefFetchMode::Exact(current), RefFetchMode::Exact(next)) => {
            merge_exact_refs(current, next)
        }
        (RefFetchMode::Exact(current), _) | (_, RefFetchMode::Exact(current)) => {
            RefFetchMode::Exact(current)
        }
        (RefFetchMode::Dynamic, _) | (_, RefFetchMode::Dynamic) => RefFetchMode::Dynamic,
        (RefFetchMode::None, RefFetchMode::None) => RefFetchMode::None,
    }
}

fn merge_exact_refs(current: String, next: String) -> RefFetchMode {
    if !exact_refs_are_compatible(&current, &next) {
        return RefFetchMode::Unsatisfiable;
    }

    if !current.starts_with("refs/") && next.starts_with("refs/") {
        RefFetchMode::Exact(next)
    } else {
        RefFetchMode::Exact(current)
    }
}

fn exact_refs_are_compatible(current: &str, next: &str) -> bool {
    let current_candidates = requested_ref_candidates(current);
    let next_candidates = requested_ref_candidates(next);

    current_candidates
        .iter()
        .any(|candidate| next_candidates.iter().any(|other| other == candidate))
}

fn merge_requested_ref_mode(
    requested_ref: Option<&str>,
    filter_mode: RefFetchMode,
) -> RefFetchMode {
    match requested_ref {
        Some(requested_ref) => {
            merge_ref_fetch_modes(RefFetchMode::Exact(requested_ref.to_string()), filter_mode)
        }
        None => filter_mode,
    }
}

fn requested_ref_candidates(requested_ref: &str) -> Vec<String> {
    if requested_ref.starts_with("refs/") {
        return vec![requested_ref.to_string()];
    }

    let mut candidate_refs = vec![
        format!("refs/heads/{requested_ref}"),
        format!("refs/tags/{requested_ref}"),
        requested_ref.to_string(),
    ];
    candidate_refs.dedup();
    candidate_refs
}

fn validate_dynamic_ref_scan(filters: &[Expr]) -> datafusion::error::Result<()> {
    if !filters.iter().all(expr_is_ref_only) {
        return Err(DataFusionError::Execution(
            "GitHub commits dynamic ref scans only support predicates on ref. Add an exact ref = '<value>' predicate or remove additional filters.".to_string(),
        ));
    }

    if !filters.iter().all(can_evaluate_ref_expr) {
        return Err(DataFusionError::Execution(
            "GitHub commits dynamic ref scans only support ref predicates built from = and != with AND/OR. Use an exact ref = '<value>' predicate for other expressions.".to_string(),
        ));
    }

    Ok(())
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
                        .as_ref()
                        .is_some_and(|value| !value.is_empty())
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
    fn test_ref_filter_pushdown_uses_inexact_for_dynamic_ref_predicate() {
        let expr = datafusion::prelude::col("ref").not_eq(datafusion::prelude::lit("trunk"));
        let result = commits_filter_pushdown(&expr);

        assert_eq!(
            result.filter_pushdown,
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact
        );
        assert_eq!(result.context.as_deref(), Some("ref-dynamic"));
    }

    #[test]
    fn test_ref_filter_pushdown_qualified_ref_uses_inexact() {
        let expr =
            datafusion::prelude::col("ref").eq(datafusion::prelude::lit("refs/heads/release/1.11"));
        let result = commits_filter_pushdown(&expr);

        assert_eq!(
            result.filter_pushdown,
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            "qualified refs must use Inexact since the ref column projects the short name"
        );
        assert_eq!(
            result.context.as_deref(),
            Some("ref:refs/heads/release/1.11")
        );
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
    fn test_ref_fetch_mode_from_filters_returns_unsatisfiable_for_conflicting_ref_values() {
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
            RefFetchMode::Unsatisfiable
        );
    }

    #[test]
    fn test_ref_fetch_mode_from_filters_prefers_qualified_exact_ref_when_compatible() {
        let filters = vec![
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Exact,
                expr: datafusion::prelude::col("ref").eq(datafusion::prelude::lit("release")),
                context: Some("ref:release".to_string()),
            },
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Exact,
                expr: datafusion::prelude::col("ref")
                    .eq(datafusion::prelude::lit("refs/heads/release")),
                context: Some("ref:refs/heads/release".to_string()),
            },
        ];

        assert_eq!(
            ref_fetch_mode_from_filter_results(&filters),
            RefFetchMode::Exact("refs/heads/release".to_string())
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
    fn test_ref_fetch_mode_from_filters_prefers_exact_ref_for_conjunctive_predicate() {
        let filters = vec![FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            expr: datafusion::prelude::col("ref")
                .eq(datafusion::prelude::lit("release/1.11"))
                .and(datafusion::prelude::col("ref").not_eq(datafusion::prelude::lit("trunk"))),
            context: None,
        }];

        assert_eq!(
            ref_fetch_mode_from_filter_results(&filters),
            RefFetchMode::Exact("release/1.11".to_string())
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
    fn test_requested_ref_candidates_try_common_qualified_refs_first() {
        assert_eq!(
            requested_ref_candidates("release/1.11"),
            vec![
                "refs/heads/release/1.11".to_string(),
                "refs/tags/release/1.11".to_string(),
                "release/1.11".to_string()
            ]
        );
    }

    #[test]
    fn test_requested_ref_candidates_preserve_qualified_refs() {
        assert_eq!(
            requested_ref_candidates("refs/heads/release/1.11"),
            vec!["refs/heads/release/1.11".to_string()]
        );
    }

    #[test]
    fn test_merge_requested_ref_mode_detects_conflicting_requested_ref_and_filter() {
        assert_eq!(
            merge_requested_ref_mode(Some("trunk"), RefFetchMode::Exact("main".to_string())),
            RefFetchMode::Unsatisfiable
        );
    }

    #[test]
    fn test_validate_dynamic_ref_scan_requires_ref_only_filters() {
        let filters = vec![
            datafusion::prelude::col("ref")
                .not_eq(datafusion::prelude::lit("trunk"))
                .and(datafusion::prelude::col("sha").eq(datafusion::prelude::lit("abc123"))),
        ];

        let err = validate_dynamic_ref_scan(&filters)
            .expect_err("dynamic ref scans with non-ref filters should fail");

        assert!(err.to_string().contains("only support predicates on ref"));
    }

    #[test]
    fn test_validate_dynamic_ref_scan_requires_evaluable_ref_filters() {
        let filters = vec![datafusion::prelude::col("ref").is_not_null()];

        let err = validate_dynamic_ref_scan(&filters)
            .expect_err("dynamic ref scans with unevaluable ref filters should fail");

        assert!(
            err.to_string()
                .contains("only support ref predicates built from = and !=")
        );
    }
}
