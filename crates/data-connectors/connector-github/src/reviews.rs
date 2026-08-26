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

//! `github.com/{owner}/{repo}/reviews` — one row per pull request review.
//!
//! `pulls.reviews_count` is a bare integer and `pulls.review_comments` records
//! only inline comments, so an approval left without an inline comment is
//! invisible in SQL. This table carries the review state, the reviewer and the
//! submission time, which is what review latency, approval counts and reviewer
//! load are computed from.

use crate::identity::push_identity_fields;
use crate::nested_connection::{NestedConnection, fan_out, flatten_login, flatten_member};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connector_graphql::graphql::{ErrorChecker, GraphQLContext, Result, client::UnnestBehavior};
use data_connector_api::ConnectorComponent;
use serde_json::Value;
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;

/// Response key holding the pull request's node id on every review row.
const PULL_REQUEST_ID_KEY: &str = "pull_request_id";

/// Response key holding the pull request's number on every review row.
const PULL_REQUEST_NUMBER_KEY: &str = "pull_request_number";

/// Fans each pull request's `reviews` connection out into one row per review.
const REVIEWS_CONNECTION: NestedConnection<'static> = NestedConnection {
    connection_key: "reviews",
    parent_keys: &[PULL_REQUEST_ID_KEY, PULL_REQUEST_NUMBER_KEY],
    parent_id_key: PULL_REQUEST_NUMBER_KEY,
    parent_label: "pull request",
    child_label: "reviews",
};

/// Pull requests fetched per page. Each one expands to up to
/// [`REVIEWS_PER_PULL_REQUEST`] reviews whose bodies can be long, so this stays
/// well below the 100 GitHub allows to keep a single response a sane size.
const PULL_REQUESTS_PAGE_SIZE: u32 = 25;

/// Reviews fetched per pull request. 100 is GitHub's per-connection maximum and
/// a nested connection cannot be paginated, so a pull request with more reviews
/// than this is truncated — the fan-out warns by name when that happens.
const REVIEWS_PER_PULL_REQUEST: u32 = 100;

// https://docs.github.com/en/graphql/reference/objects#pullrequestreview
#[derive(Debug)]
pub struct ReviewsTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ReviewsTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn supports_limit_pushdown(&self) -> bool {
        // One pull request fans out into many review rows, so a row limit cannot
        // bound the number of pull requests to fetch.
        false
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (pullRequests) + 100 (reviews per pull request)
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(1 + REVIEWS_PER_PULL_REQUEST)
    }
}

impl GitHubTableArgs for ReviewsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
        // predicate on the sort key, so ordering by a mutable field lets a pull request
        // touched on the source mid-scan jump ahead of the cursor, where no remaining
        // page will return it — silently dropping every review on it from the scan.
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
                            reviews(first: {reviews_per_pull_request}) {{
                                totalCount
                                nodes {{
                                    id
                                    state
                                    body
                                    url
                                    submitted_at: submittedAt
                                    author_association: authorAssociation
                                    author {{ login }}
                                    commit {{ oid }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            page_size = PULL_REQUESTS_PAGE_SIZE,
            reviews_per_pull_request = REVIEWS_PER_PULL_REQUEST,
            pull_request_id = PULL_REQUEST_ID_KEY,
            pull_request_number = PULL_REQUEST_NUMBER_KEY,
        );

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                fan_out(object, &REVIEWS_CONNECTION, &owner, &repo, |review| {
                    flatten_login(review, "author");
                    flatten_member(review, "commit", "oid", "commit_sha");
                })
            })),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(PULL_REQUEST_ID_KEY, DataType::Utf8, true),
        Field::new(PULL_REQUEST_NUMBER_KEY, DataType::Int64, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new(
            "submitted_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("commit_sha", DataType::Utf8, true),
        Field::new("author_association", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::{PULL_REQUESTS_PAGE_SIZE, REVIEWS_PER_PULL_REQUEST, ReviewsTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::GraphQLContext;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::json;

    fn args() -> ReviewsTableArgs {
        ReviewsTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            component: shared_component("test.reviews"),
        }
    }

    #[test]
    fn query_paginates_pull_requests_on_an_immutable_sort_key() {
        // Regression guard matching `pulls` and `issues`: a GitHub `after:` cursor is a
        // value predicate on the sort key, so paginating on a mutable field silently
        // drops rows touched mid-scan.
        let query = args().get_graphql_values().query.to_string();

        assert!(
            query.contains("orderBy: {field: CREATED_AT, direction: ASC}"),
            "reviews query must order pull requests by CREATED_AT ASC, got:\n{query}"
        );
        assert!(
            !query.contains("UPDATED_AT"),
            "reviews query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn query_requests_total_count_so_truncation_is_detectable() {
        let query = args().get_graphql_values().query.to_string();

        assert!(
            query.contains("totalCount"),
            "reviews query must request totalCount to detect a truncated page, got:\n{query}"
        );
        assert!(query.contains(&format!("reviews(first: {REVIEWS_PER_PULL_REQUEST})")));
        assert!(query.contains(&format!("pullRequests(first: {PULL_REQUESTS_PAGE_SIZE}")));
    }

    #[test]
    fn unnest_flattens_a_pull_request_into_one_row_per_review() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("reviews must fan out its rows with a custom unnest");
        };

        let rows = unnest(&json!({
            "pull_request_id": "PR_1",
            "pull_request_number": 13435,
            "reviews": {
                "totalCount": 2,
                "nodes": [
                    {
                        "id": "PRR_1",
                        "state": "APPROVED",
                        "body": "",
                        "url": "https://github.com/spiceai/spiceai/pull/13435",
                        "submitted_at": "2026-08-24T18:07:21Z",
                        "author_association": "MEMBER",
                        "author": {"login": "lukekim"},
                        "commit": {"oid": "abc123"}
                    },
                    {
                        "id": "PRR_2",
                        "state": "CHANGES_REQUESTED",
                        "body": "needs work",
                        "url": "https://github.com/spiceai/spiceai/pull/13435",
                        "submitted_at": "2026-08-24T19:00:00Z",
                        "author_association": "CONTRIBUTOR",
                        "author": null,
                        "commit": null
                    }
                ]
            }
        }))
        .expect("unnest to succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["state"], json!("APPROVED"));
        assert_eq!(rows[0]["author"], json!("lukekim"));
        assert_eq!(rows[0]["commit_sha"], json!("abc123"));
        assert_eq!(rows[0]["pull_request_number"], json!(13435));
        assert_eq!(rows[0]["owner"], json!("spiceai"));
        assert_eq!(rows[0]["repo"], json!("spiceai"));
        assert_eq!(rows[1]["author"], serde_json::Value::Null);
        assert_eq!(rows[1]["commit_sha"], serde_json::Value::Null);
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        // A key the schema does not declare is silently dropped by the Arrow JSON
        // reader, so the two must be kept in step.
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("reviews must fan out its rows with a custom unnest");
        };

        let rows = unnest(&json!({
            "pull_request_id": "PR_1",
            "pull_request_number": 1,
            "reviews": {"totalCount": 1, "nodes": [{
                "id": "PRR_1", "state": "APPROVED", "body": "", "url": "u",
                "submitted_at": "2026-08-24T18:07:21Z", "author_association": "MEMBER",
                "author": {"login": "lukekim"}, "commit": {"oid": "abc123"}
            }]}
        }))
        .expect("unnest to succeed");

        let schema = gql_schema();
        for key in rows[0].as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "reviews emits '{key}' but the schema does not declare it"
            );
        }
    }

    #[test]
    fn query_cost_stays_within_the_github_secondary_rate_limit_burst() {
        // The rate controller's weighted quota is 2000 points per minute; a cost
        // above the burst capacity fails the acquire outright instead of waiting.
        let cost = args()
            .query_cost()
            .expect("reviews to declare a query cost");
        assert!(cost <= 2000, "reviews query cost {cost} exceeds the burst");
    }
}
