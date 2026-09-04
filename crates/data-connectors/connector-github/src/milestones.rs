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

//! `github.com/{owner}/{repo}/milestones` — one row per milestone.
//!
//! `issues` and `pulls` both carry `milestone_id` and `milestone_title`, but
//! there was no milestone table to join them to, so a due date or a completion
//! percentage had to come from a direct API call.

use crate::identity::{identity_unnest, push_identity_fields};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connector_graphql::graphql::{ErrorChecker, GraphQLContext};
use data_connector_api::ConnectorComponent;
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;

/// Milestones fetched per page. Milestones are small rows, so this is GitHub's
/// full page size.
const MILESTONES_PAGE_SIZE: u32 = 100;

// https://docs.github.com/en/graphql/reference/objects#milestone
#[derive(Debug)]
pub struct MilestonesTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for MilestonesTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (milestones) + 2 issue-count connections per milestone
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(3)
    }
}

impl GitHubTableArgs for MilestonesTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
        // predicate on the sort key, so ordering by a mutable field lets a milestone
        // touched on the source mid-scan jump ahead of the cursor, where no remaining
        // page will return it — silently dropping the row from the scan.
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    milestones(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            id
                            number
                            title
                            description
                            state
                            url
                            due_on: dueOn
                            created_at: createdAt
                            updated_at: updatedAt
                            closed_at: closedAt
                            progress_percentage: progressPercentage
                            creator: creator {{ creator: login }}
                            open_issues_count: issues(states: OPEN) {{ open_issues_count: totalCount }}
                            closed_issues_count: issues(states: CLOSED) {{ closed_issues_count: totalCount }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            page_size = MILESTONES_PAGE_SIZE,
        );

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            identity_unnest(1, self.owner.clone(), Some(self.repo.clone())),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("creator", DataType::Utf8, true),
        Field::new(
            "due_on",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "closed_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("open_issues_count", DataType::Int64, true),
        Field::new("closed_issues_count", DataType::Int64, true),
        Field::new("progress_percentage", DataType::Float64, true),
        Field::new("url", DataType::Utf8, true),
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::{MilestonesTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::json;

    fn args() -> MilestonesTableArgs {
        MilestonesTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            component: shared_component("test.milestones"),
        }
    }

    #[test]
    fn query_paginates_on_an_immutable_sort_key() {
        let query = args().get_graphql_values().query.to_string();

        assert!(
            query.contains("orderBy: {field: CREATED_AT, direction: ASC}"),
            "milestones query must order by CREATED_AT ASC, got:\n{query}"
        );
        assert!(
            !query.contains("UPDATED_AT"),
            "milestones query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn unnest_flattens_the_issue_counts_and_stamps_identity() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("milestones must stamp identity with a custom unnest");
        };

        let rows = unnest(&json!({
            "id": "MI_1",
            "number": 100,
            "title": "v2.3.0",
            "description": null,
            "state": "OPEN",
            "url": "https://github.com/spiceai/spiceai/milestone/100",
            "due_on": "2026-09-08T00:00:00Z",
            "created_at": "2026-08-01T00:00:00Z",
            "updated_at": "2026-08-20T00:00:00Z",
            "closed_at": null,
            "progress_percentage": 42.5,
            "creator": {"creator": "lukekim"},
            "open_issues_count": {"open_issues_count": 7},
            "closed_issues_count": {"closed_issues_count": 13}
        }))
        .expect("unnest to succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["open_issues_count"], json!(7));
        assert_eq!(rows[0]["closed_issues_count"], json!(13));
        assert_eq!(rows[0]["creator"], json!("lukekim"));
        assert_eq!(rows[0]["progress_percentage"], json!(42.5));
        assert_eq!(rows[0]["owner"], json!("spiceai"));
        assert_eq!(rows[0]["repo"], json!("spiceai"));

        let schema = gql_schema();
        for key in rows[0].as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "milestones emits '{key}' but the schema does not declare it"
            );
        }
    }
}
