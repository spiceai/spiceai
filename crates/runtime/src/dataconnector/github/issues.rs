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
}

impl GitHubTableArgs for IssuesTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = match self.query_mode {
            GitHubQueryMode::Search => format!(
                r#"{{
                search(query:"repo:{owner}/{name} type:issue", first:100, type:ISSUE) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on Issue {{
                            id
                            number
                            title
                            url
                            author: author {{ author: login }}
                            body
                            created_at: createdAt
                            updated_at: updatedAt
                            closed_at: closedAt
                            state
                            milestone_id: milestone {{ milestone_id: id}}
                            milestone_title: milestone {{ milestone_title: title }}
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            milestone_title: milestone {{ milestone_title: title }}
                            comments(first: 100) {{ comments_count: totalCount, comments: nodes {{ body, author {{ login }} }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo
            ),
            GitHubQueryMode::Auto => format!(
                r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    issues(first: 100) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            id
                            number
                            title
                            url
                            author: author {{ author: login }}
                            body
                            created_at: createdAt
                            updated_at: updatedAt
                            closed_at: closedAt
                            state
                            milestone_id: milestone {{ milestone_id: id}}
                            milestone_title: milestone {{ milestone_title: title }}
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            milestone_title: milestone {{ milestone_title: title }}
                            comments(first: 100) {{ comments_count: totalCount, comments: nodes {{ body, author {{ login }} }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo
            ),
        };

        GitHubTableGraphQLParams::new(query.into(), None, 2, Some(gql_schema()))
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new("state", DataType::Utf8, true),
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
    ]))
}
