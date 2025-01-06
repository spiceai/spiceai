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

use crate::{dataconnector::ConnectorComponent, datafusion::error::find_datafusion_root};

use super::{
    filter_pushdown, inject_parameters, search_inject_parameters, GitHubQueryMode, GitHubTableArgs,
    GitHubTableGraphQLParams,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::{
    github::error_checker,
    graphql::{client::GraphQLQuery, ErrorChecker, FilterPushdownResult, GraphQLContext, Result},
};
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#repository
#[derive(Debug)]
pub struct PullRequestTableArgs {
    pub owner: String,
    pub repo: String,
    pub query_mode: GitHubQueryMode,
    pub component: ConnectorComponent,
}

impl GraphQLContext for PullRequestTableArgs {
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
}

impl GitHubTableArgs for PullRequestTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = match self.query_mode {
            GitHubQueryMode::Search => {
                format!(
                    r#"{{
                search(query:"repo:{owner}/{name} type:pr", first:100, type:ISSUE) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on PullRequest {{
                            title
                            number
                            id
                            url
                            body
                            state
                            created_at: createdAt
                            updated_at: updatedAt
                            merged_at: mergedAt
                            closed_at: closedAt
                            number
                            reviews {{reviews_count: totalCount}}
                            author: author {{ author: login }}
                            additions
                            deletions
                            changed_files: changedFiles
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            comments(first: 100) {{comments_count: totalCount}}
                            commits(first: 100) {{commits_count: totalCount, hashes: nodes{{ id }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                        }}
                    }}
                }}
            }}"#,
                    owner = self.owner,
                    name = self.repo,
                )
            }
            GitHubQueryMode::Auto => {
                format!(
                    r#"
            {{
                repository(owner: "{owner}", name: "{name}") {{
                    pullRequests(first: 100) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            title
                            number
                            id
                            url
                            body
                            state
                            created_at: createdAt
                            updated_at: updatedAt
                            merged_at: mergedAt
                            closed_at: closedAt
                            number
                            reviews {{reviews_count: totalCount}}
                            author: author {{ author: login }}
                            additions
                            deletions
                            changed_files: changedFiles
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            comments(first: 100) {{comments_count: totalCount}}
                            commits(first: 100) {{commits_count: totalCount, hashes: nodes{{ id }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                        }}
                    }}
                }}
            }}
            "#,
                    owner = self.owner,
                    name = self.repo,
                )
            }
        };

        GitHubTableGraphQLParams::new(query.into(), None, 1, Some(gql_schema()))
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("additions", DataType::Int64, true),
        Field::new(
            "assignees",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("login", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("author", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("changed_files", DataType::Int64, true),
        Field::new(
            "closed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("comments_count", DataType::Int64, true),
        Field::new("commits_count", DataType::Int64, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("deletions", DataType::Int64, true),
        Field::new(
            "hashes",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("id", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new(
            "merged_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("number", DataType::Int64, true),
        Field::new("reviews_count", DataType::Int64, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("url", DataType::Utf8, true),
    ]))
}
