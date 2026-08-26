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

use data_connector_api::ConnectorComponent;
use runtime_datafusion::error::find_datafusion_root;

use super::{
    GitHubQueryMode, GitHubTableArgs, GitHubTableGraphQLParams, filter_pushdown, inject_parameters,
    search_inject_parameters,
};
use crate::github::error_checker;
use crate::identity::{insert_identity, push_identity_fields};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use connector_graphql::graphql::{
    ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
    client::{DuplicateBehavior, GraphQLQuery, UnnestBehavior, unnest_json_object_to_depth},
};
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Response key holding the timeline lookup the closing actor is read from.
/// The connection is removed before unnesting, so it never reaches a row.
const CLOSED_BY_SOURCE_KEY: &str = "closed_by_source";

/// Upper bound on the number of issues fetched per page.
const ISSUES_PAGE_SIZE: u32 = 100;

// https://docs.github.com/en/graphql/reference/objects#repository
#[derive(Debug)]
pub struct IssuesTableArgs {
    pub owner: String,
    pub repo: String,
    pub query_mode: GitHubQueryMode,
    pub component: ConnectorComponent,
}

impl GraphQLContext for IssuesTableArgs {
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        if self.query_mode == GitHubQueryMode::Auto {
            return Ok(FilterPushdownResult {
                filter_pushdown: TableProviderFilterPushDown::Unsupported,
                expr: expr.clone(),
                context: None,
            });
        }

        Ok(filter_pushdown(expr))
    }

    fn inject_parameters(
        &self,
        filters: &[FilterPushdownResult],
        query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        if self.query_mode == GitHubQueryMode::Auto {
            return Ok(());
        }

        inject_parameters("search", search_inject_parameters, filters, query)
            .map_err(find_datafusion_root)
    }

    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // Each connection in the query charges its page size, and 1 for the issue
        // connection itself:
        // 1 + 100 (labels) + 25 (comments) + 100 (assignees) + 1 (timelineItems)
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(227)
    }
}

impl IssuesTableArgs {
    /// The issue fields both query modes select. Keeping one list keeps the two
    /// shapes from drifting apart.
    fn requested_nodes() -> String {
        format!(
            r"
            id
            number
            title
            url
            body
            state
            state_reason: stateReason
            created_at: createdAt
            updated_at: updatedAt
            closed_at: closedAt
            author: author {{ author: login }}
            milestone_id: milestone {{ milestone_id: id }}
            milestone_title: milestone {{ milestone_title: title }}
            reactions_wrapper: reactions {{ reactions_count: totalCount }}
            labels(first: 100) {{ labels: nodes {{ name }} }}
            comments(first: 25) {{ comments_count: totalCount, comments: nodes {{ body, author {{ login }} }} }}
            assignees(first: 100) {{ assignees: nodes {{ login }} }}
            type: issueType {{ type: name, type_color: color }}
            {CLOSED_BY_SOURCE_KEY}: timelineItems(last: 1, itemTypes: [CLOSED_EVENT]) {{
                nodes {{ ... on ClosedEvent {{ actor {{ login }} }} }}
            }}
        "
        )
    }
}

impl GitHubTableArgs for IssuesTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let nodes = Self::requested_nodes();
        let query = match self.query_mode {
            GitHubQueryMode::Search => format!(
                r#"{{
                search(query:"repo:{owner}/{name} type:issue", first:{page_size}, type:ISSUE) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on Issue {{
                            {nodes}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo,
                page_size = ISSUES_PAGE_SIZE,
            ),
            // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
            // predicate on the sort key, so ordering by a mutable field (e.g. UPDATED_AT)
            // lets an issue touched on the source mid-scan jump ahead of the cursor, where
            // no remaining page will return it — silently dropping the row from the scan.
            // CREATED_AT ASC is GitHub's own default order for this connection.
            GitHubQueryMode::Auto => format!(
                r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    issues(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            {nodes}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo,
                page_size = ISSUES_PAGE_SIZE,
            ),
        };

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                custom_unnestter(object, &owner, &repo)
            })),
            Some(gql_schema()),
        )
    }
}

fn custom_unnestter(object: &Value, owner: &str, repo: &str) -> Result<Vec<Value>> {
    // The timeline lookup hangs its payload under a `nodes` key that no column
    // claims, so flatten it away before unnesting hoists it into the row.
    let mut object = object.clone();
    if let Value::Object(issue) = &mut object {
        flatten_closed_by(issue);
    }

    unnest_json_object_to_depth(object, 2, &DuplicateBehavior::Error).map(|mut rows| {
        for row in &mut rows {
            if let Value::Object(row) = row {
                insert_identity(row, owner, Some(repo));
            }
        }

        rows
    })
}

/// Replaces the closing-event timeline lookup with a `closed_by` login.
fn flatten_closed_by(issue: &mut Map<String, Value>) {
    let closing_actor = issue
        .remove(CLOSED_BY_SOURCE_KEY)
        .and_then(|timeline| timeline.pointer("/nodes/0/actor/login").cloned())
        .unwrap_or(Value::Null);

    // GitHub keeps a `ClosedEvent` on the timeline of an issue that was closed
    // and then reopened, so the actor only describes the issue's current state
    // while it is actually closed.
    let is_closed = issue
        .get("closed_at")
        .is_some_and(|closed_at| !closed_at.is_null());

    issue.insert(
        "closed_by".to_string(),
        if is_closed {
            closing_actor
        } else {
            Value::Null
        },
    );
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "closed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("closed_by", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("state_reason", DataType::Utf8, true),
        Field::new("reactions_count", DataType::Int64, true),
        // The query has always selected `issueType`; without these two the
        // Arrow JSON reader dropped it on the way into the record batch.
        Field::new("type", DataType::Utf8, true),
        Field::new("type_color", DataType::Utf8, true),
        Field::new("milestone_id", DataType::Utf8, true),
        Field::new("milestone_title", DataType::Utf8, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("comments_count", DataType::Int64, true),
        Field::new(
            "comments",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new(
                            "author",
                            DataType::Struct(
                                vec![Field::new("login", DataType::Utf8, true)].into(),
                            ),
                            true,
                        ),
                        Field::new("body", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
        Field::new(
            "assignees",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("login", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::shared_component;
    use serde_json::json;

    fn auto_args() -> IssuesTableArgs {
        IssuesTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            query_mode: GitHubQueryMode::Auto,
            component: shared_component("test.issues"),
        }
    }

    fn search_args() -> IssuesTableArgs {
        IssuesTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            query_mode: GitHubQueryMode::Search,
            component: shared_component("test.issues_search"),
        }
    }

    /// An issue node shaped the way GitHub returns it, before unnesting.
    fn issue_node(closed_at: &Value) -> Value {
        json!({
            "id": "I_1",
            "number": 13423,
            "title": "Arrow relabel",
            "url": "https://github.com/spiceai/spiceai/issues/13423",
            "body": "",
            "state": "CLOSED",
            "state_reason": "COMPLETED",
            "created_at": "2026-08-01T00:00:00Z",
            "updated_at": "2026-08-24T00:00:00Z",
            "closed_at": closed_at,
            "author": {"author": "lukekim"},
            "milestone_id": {"milestone_id": "MI_1"},
            "milestone_title": {"milestone_title": "v2.3.0"},
            "reactions_wrapper": {"reactions_count": 2},
            "labels": {"labels": [{"name": "kind/bug"}]},
            "comments": {"comments_count": 3, "comments": [{"body": "hi", "author": {"login": "a"}}]},
            "assignees": {"assignees": [{"login": "lukekim"}]},
            "type": {"type": "Bug", "type_color": "RED"},
            "closed_by_source": {"nodes": [{"actor": {"login": "claudespice"}}]}
        })
    }

    fn unnest_one(args: &IssuesTableArgs, node: &Value) -> Value {
        let params = args.get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("issues must use a custom unnest");
        };
        let rows = unnest(node).expect("unnest to succeed");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().unwrap_or(Value::Null)
    }

    #[test]
    fn auto_mode_query_orders_by_created_at_asc() {
        // Deterministic ordering on an immutable key: see
        // `auto_mode_query_never_paginates_on_a_mutable_sort_key`.
        let params = auto_args().get_graphql_values();
        let query = params.query.as_ref();
        assert!(
            query.contains("issues(first: 100, orderBy: {field: CREATED_AT, direction: ASC})"),
            "auto-mode issues query must order by CREATED_AT ASC, got:\n{query}"
        );
    }

    /// Regression test for #12067. A GitHub `after:` cursor is a value predicate on the
    /// sort key, so a connection paginated on a mutable field silently drops any row
    /// that is touched on the source mid-scan: the row's key moves ahead of the cursor
    /// and no remaining page returns it. Only immutable sort keys are safe here.
    #[test]
    fn auto_mode_query_never_paginates_on_a_mutable_sort_key() {
        let params = auto_args().get_graphql_values();
        let query = params.query.as_ref();
        // The GraphQL enum is upper-case (`UPDATED_AT`), so this cannot collide with the
        // `updated_at: updatedAt` field alias the query also selects.
        assert!(
            !query.contains("UPDATED_AT"),
            "auto-mode issues query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn both_query_modes_select_the_same_fields() {
        // The two shapes were separate copies of the field list and had already
        // drifted; a column added to one but not the other is null in half the
        // configurations and impossible to notice from SQL.
        let nodes = IssuesTableArgs::requested_nodes();

        for query in [
            auto_args().get_graphql_values().query.to_string(),
            search_args().get_graphql_values().query.to_string(),
        ] {
            assert!(
                query.contains(nodes.trim_end()),
                "issues query mode is missing the shared field list, got:\n{query}"
            );
        }
    }

    #[test]
    fn unnest_flattens_the_new_columns_and_stamps_identity() {
        let row = unnest_one(&auto_args(), &issue_node(&json!("2026-08-24T00:00:00Z")));

        assert_eq!(row["state_reason"], json!("COMPLETED"));
        assert_eq!(row["closed_by"], json!("claudespice"));
        assert_eq!(row["reactions_count"], json!(2));
        assert_eq!(row["type"], json!("Bug"));
        assert_eq!(row["type_color"], json!("RED"));
        assert_eq!(row["owner"], json!("spiceai"));
        assert_eq!(row["repo"], json!("spiceai"));
    }

    #[test]
    fn closed_by_is_null_while_the_issue_is_open() {
        // GitHub keeps a `ClosedEvent` on the timeline of an issue that was closed
        // and then reopened, so reading the timeline alone would attribute a stale
        // actor to an open issue.
        let row = unnest_one(&auto_args(), &issue_node(&Value::Null));

        assert_eq!(row["closed_by"], Value::Null);
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        // A key the schema does not declare is silently dropped by the Arrow JSON
        // reader, so the two must be kept in step.
        let row = unnest_one(&auto_args(), &issue_node(&json!("2026-08-24T00:00:00Z")));
        let schema = gql_schema();

        for key in row.as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "issues emits '{key}' but the schema does not declare it"
            );
        }
    }
}
