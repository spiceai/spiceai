/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `github.com/{owner}/{repo}/review_threads` — one row per resolvable review
//! thread.
//!
//! `pulls.review_comments` is a flat list of comment bodies with no thread
//! grouping and no resolution state, so "which pull requests still have
//! unresolved review comments" — the reviewer-queue question — has no SQL
//! answer. This table carries thread identity, `is_resolved`, and the file and
//! line the thread hangs off.

use crate::identity::push_identity_fields;
use crate::nested_connection::{NestedConnection, fan_out, flatten_login, flatten_member};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use connector_graphql::graphql::{
    ErrorChecker, GraphQLContext, Result,
    client::{NestedConnectionPager, UnnestBehavior},
};
use data_connector_api::ConnectorComponent;
use serde_json::Value;
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;

/// Response key holding the pull request's node id on every thread row.
const PULL_REQUEST_ID_KEY: &str = "pull_request_id";

/// Response key holding the pull request's number on every thread row.
const PULL_REQUEST_NUMBER_KEY: &str = "pull_request_number";

/// Fans each pull request's `reviewThreads` connection out into one row per
/// thread.
const REVIEW_THREADS_CONNECTION: NestedConnection<'static> = NestedConnection {
    connection_key: "reviewThreads",
    parent_keys: &[PULL_REQUEST_ID_KEY, PULL_REQUEST_NUMBER_KEY],
    parent_id_key: PULL_REQUEST_NUMBER_KEY,
    parent_label: "pull request",
    child_label: "review threads",
};

/// Pull requests fetched per page. Threads carry no bodies, so this can be
/// GitHub's full page size.
const PULL_REQUESTS_PAGE_SIZE: u32 = 50;

/// Review threads fetched per pull request. 100 is GitHub's per-connection
/// maximum. Remaining threads are loaded via `node(id:)` follow-up pages.
const THREADS_PER_PULL_REQUEST: u32 = 100;

const REVIEW_THREAD_NODE_SELECTION: &str = r"
    id
    path
    line
    is_resolved: isResolved
    is_outdated: isOutdated
    is_collapsed: isCollapsed
    start_line: startLine
    diff_side: diffSide
    resolved_by: resolvedBy { login }
    comments { totalCount }
";

// https://docs.github.com/en/graphql/reference/objects#pullrequestreviewthread
#[derive(Debug)]
pub struct ReviewThreadsTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ReviewThreadsTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn supports_limit_pushdown(&self) -> bool {
        // One pull request fans out into many thread rows, so a row limit cannot
        // bound the number of pull requests to fetch.
        false
    }

    fn query_cost(&self) -> Option<u32> {
        Some(crate::rate_limit::graphql_secondary_query_cost())
    }
}

impl GitHubTableArgs for ReviewThreadsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
        // predicate on the sort key, so ordering by a mutable field lets a pull request
        // touched on the source mid-scan jump ahead of the cursor, where no remaining
        // page will return it — silently dropping every thread on it from the scan.
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    pullRequests(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            {pull_request_id}: id
                            {pull_request_number}: number
                            reviewThreads(first: {threads_per_pull_request}) {{
                                totalCount
                                pageInfo {{
                                    hasNextPage
                                    endCursor
                                }}
                                nodes {{
                                    {thread_node_selection}
                                }}
                            }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            page_size = PULL_REQUESTS_PAGE_SIZE,
            threads_per_pull_request = THREADS_PER_PULL_REQUEST,
            pull_request_id = PULL_REQUEST_ID_KEY,
            pull_request_number = PULL_REQUEST_NUMBER_KEY,
            thread_node_selection = REVIEW_THREAD_NODE_SELECTION,
        );

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                fan_out(
                    object,
                    &REVIEW_THREADS_CONNECTION,
                    &owner,
                    &repo,
                    |thread| {
                        flatten_login(thread, "resolved_by");
                        flatten_member(thread, "comments", "totalCount", "comments_count");
                    },
                )
            })),
            Some(gql_schema()),
        )
        .with_nested_pager(NestedConnectionPager {
            connection_key: "reviewThreads".to_string(),
            parent_id_key: PULL_REQUEST_ID_KEY.to_string(),
            type_condition: "PullRequest".to_string(),
            node_selection: REVIEW_THREAD_NODE_SELECTION.to_string(),
            page_size: THREADS_PER_PULL_REQUEST,
        })
    }
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(PULL_REQUEST_ID_KEY, DataType::Utf8, true),
        Field::new(PULL_REQUEST_NUMBER_KEY, DataType::Int64, true),
        Field::new("is_resolved", DataType::Boolean, true),
        Field::new("is_outdated", DataType::Boolean, true),
        Field::new("is_collapsed", DataType::Boolean, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("line", DataType::Int64, true),
        Field::new("start_line", DataType::Int64, true),
        Field::new("diff_side", DataType::Utf8, true),
        Field::new("resolved_by", DataType::Utf8, true),
        Field::new("comments_count", DataType::Int64, true),
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::{ReviewThreadsTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::GraphQLContext;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Value, json};

    fn args() -> ReviewThreadsTableArgs {
        ReviewThreadsTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            component: shared_component("test.review_threads"),
        }
    }

    fn thread_page() -> Value {
        json!({
            "pull_request_id": "PR_1",
            "pull_request_number": 13435,
            "reviewThreads": {
                "totalCount": 2,
                "nodes": [
                    {
                        "id": "PRRT_1",
                        "path": "crates/arrow_tools/src/type_rewrite.rs",
                        "line": 572,
                        "is_resolved": true,
                        "is_outdated": true,
                        "is_collapsed": true,
                        "start_line": 500,
                        "diff_side": "RIGHT",
                        "resolved_by": {"login": "lukekim"},
                        "comments": {"totalCount": 2}
                    },
                    {
                        "id": "PRRT_2",
                        "path": "crates/runtime/src/lib.rs",
                        "line": null,
                        "is_resolved": false,
                        "is_outdated": false,
                        "is_collapsed": false,
                        "start_line": null,
                        "diff_side": "RIGHT",
                        "resolved_by": null,
                        "comments": {"totalCount": 1}
                    }
                ]
            }
        })
    }

    #[test]
    fn query_requests_page_info_so_overflow_threads_can_be_paginated() {
        let params = args().get_graphql_values();
        let query = params.query.to_string();
        let threads = query
            .split("reviewThreads(first:")
            .nth(1)
            .expect("reviewThreads connection");

        assert!(
            threads.contains("hasNextPage") && threads.contains("endCursor"),
            "reviewThreads connection must request pageInfo, got:\n{query}"
        );
        assert!(
            params.nested_pager.is_some(),
            "review_threads must page overflow via node(id:)"
        );
    }

    #[test]
    fn query_paginates_pull_requests_on_an_immutable_sort_key() {
        let query = args().get_graphql_values().query.to_string();

        assert!(
            query.contains("orderBy: {field: CREATED_AT, direction: ASC}"),
            "review_threads query must order pull requests by CREATED_AT ASC, got:\n{query}"
        );
        assert!(
            !query.contains("UPDATED_AT"),
            "review_threads query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn unnest_emits_one_row_per_thread_with_resolution_state() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("review_threads must fan out its rows with a custom unnest");
        };

        let rows = unnest(&thread_page()).expect("unnest to succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["is_resolved"], json!(true));
        assert_eq!(rows[0]["resolved_by"], json!("lukekim"));
        assert_eq!(rows[0]["comments_count"], json!(2));
        assert_eq!(
            rows[0]["path"],
            json!("crates/arrow_tools/src/type_rewrite.rs")
        );
        assert_eq!(rows[0]["owner"], json!("spiceai"));
        assert_eq!(rows[0]["repo"], json!("spiceai"));

        // An unresolved thread keeps a null resolver rather than an empty string.
        assert_eq!(rows[1]["is_resolved"], json!(false));
        assert_eq!(rows[1]["resolved_by"], Value::Null);
        // An outdated thread has no line, which must stay null rather than 0.
        assert_eq!(rows[1]["line"], Value::Null);
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("review_threads must fan out its rows with a custom unnest");
        };

        let rows = unnest(&thread_page()).expect("unnest to succeed");
        let schema = gql_schema();

        for row in &rows {
            for key in row.as_object().expect("row object").keys() {
                assert!(
                    schema.field_with_name(key).is_ok(),
                    "review_threads emits '{key}' but the schema does not declare it"
                );
            }
        }
    }

    #[test]
    fn query_cost_stays_within_the_github_secondary_rate_limit_burst() {
        let cost = args()
            .query_cost()
            .expect("review_threads to declare a query cost");
        assert_eq!(
            cost,
            crate::rate_limit::GITHUB_GRAPHQL_SECONDARY_QUERY_POINTS
        );
    }
}
