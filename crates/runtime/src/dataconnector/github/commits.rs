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

use super::{
    GitHubTableArgs, GitHubTableGraphQLParams, commits_inject_parameters, expr_to_match,
    filter_pushdown, inject_parameters,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::{
    github::error_checker,
    graphql::{
        ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
        client::{DuplicateBehavior, GraphQLQuery, UnnestBehavior, unnest_json_object_to_depth},
    },
};
use datafusion::{logical_expr::Operator, prelude::Expr, scalar::ScalarValue};
use graphql_parser::query::{Definition, InlineFragment, OperationDefinition, Query, Selection};
use serde_json::{Map, Value};
use std::sync::Arc;

const COMMITS_JSON_POINTER: &str = "/data/repository";

// https://docs.github.com/en/graphql/reference/objects#commit
#[derive(Debug)]
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
        let requested_ref =
            requested_ref_from_filters(filters)?.or_else(|| self.requested_ref.clone());

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
        return match (op, value) {
            (Operator::Eq, ScalarValue::Utf8(Some(value))) if !value.is_empty() => {
                FilterPushdownResult {
                    filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
                    expr: expr.clone(),
                    context: Some(format!("ref:{value}")),
                }
            }
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

fn unsupported_ref_filter_error() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "GitHub commits only support a single non-empty ref = '<value>' predicate. Queries using ref with OR, IN, inequality, or multiple values are not supported because they can return incorrect results.".to_string(),
    )
}

fn merge_requested_refs(
    current: Option<String>,
    next: Option<String>,
) -> Result<Option<String>, datafusion::error::DataFusionError> {
    match (current, next) {
        (Some(current), Some(next)) if current != next => Err(unsupported_ref_filter_error()),
        (Some(current), _) => Ok(Some(current)),
        (None, Some(next)) => Ok(Some(next)),
        (None, None) => Ok(None),
    }
}

fn requested_ref_from_filter(
    expr: &Expr,
) -> Result<Option<String>, datafusion::error::DataFusionError> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => merge_requested_refs(
            requested_ref_from_filter(binary_expr.left.as_ref())?,
            requested_ref_from_filter(binary_expr.right.as_ref())?,
        ),
        _ => {
            if let Some((column, value, op)) = expr_to_match(expr)
                && column.name == "ref"
            {
                return match (op, value) {
                    (Operator::Eq, ScalarValue::Utf8(Some(value))) if !value.is_empty() => {
                        Ok(Some(value))
                    }
                    _ => Err(unsupported_ref_filter_error()),
                };
            }

            if expr_references_ref(expr) {
                return Err(unsupported_ref_filter_error());
            }

            Ok(None)
        }
    }
}

fn requested_ref_from_filters(
    filters: &[FilterPushdownResult],
) -> Result<Option<String>, datafusion::error::DataFusionError> {
    filters.iter().try_fold(None, |current, filter| {
        merge_requested_refs(current, requested_ref_from_filter(&filter.expr)?)
    })
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
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact
        );
        assert_eq!(result.context.as_deref(), Some("ref:trunk"));
    }

    #[test]
    fn test_requested_ref_from_filters_supports_conjunctive_filter() {
        let filters = vec![FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            expr: datafusion::prelude::col("ref")
                .eq(datafusion::prelude::lit("trunk"))
                .and(datafusion::prelude::col("sha").eq(datafusion::prelude::lit("abc123"))),
            context: None,
        }];

        assert_eq!(
            requested_ref_from_filters(&filters)
                .expect("conjunctive ref filter should be supported")
                .as_deref(),
            Some("trunk")
        );
    }

    #[test]
    fn test_requested_ref_from_filters_rejects_multiple_ref_values() {
        let filters = vec![
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
                expr: datafusion::prelude::col("ref").eq(datafusion::prelude::lit("trunk")),
                context: Some("ref:trunk".to_string()),
            },
            FilterPushdownResult {
                filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
                expr: datafusion::prelude::col("ref").eq(datafusion::prelude::lit("main")),
                context: Some("ref:main".to_string()),
            },
        ];

        let _ = requested_ref_from_filters(&filters)
            .expect_err("multiple ref values should be rejected");
    }

    #[test]
    fn test_requested_ref_from_filters_rejects_unsupported_ref_or_predicate() {
        let filters = vec![FilterPushdownResult {
            filter_pushdown: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            expr: datafusion::prelude::col("ref")
                .eq(datafusion::prelude::lit("trunk"))
                .or(datafusion::prelude::col("ref").eq(datafusion::prelude::lit("main"))),
            context: None,
        }];

        let _ = requested_ref_from_filters(&filters)
            .expect_err("unsupported ref OR predicates should be rejected");
    }
}
