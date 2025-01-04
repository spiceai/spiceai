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
    commits_inject_parameters, filter_pushdown, inject_parameters, GitHubTableArgs,
    GitHubTableGraphQLParams,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::{
    github::error_checker,
    graphql::{client::GraphQLQuery, ErrorChecker, FilterPushdownResult, GraphQLContext},
};
use datafusion::prelude::Expr;
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#commit
#[derive(Debug)]
pub struct CommitsTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for CommitsTableArgs {
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        Ok(filter_pushdown(expr))
    }

    fn inject_parameters(
        &self,
        filters: &[FilterPushdownResult],
        query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        inject_parameters("history", commits_inject_parameters, filters, query)
            .map_err(find_datafusion_root)
    }

    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }
}

impl GitHubTableArgs for CommitsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    defaultBranchRef {{
                        target {{
                            ... on Commit {{
                                history(first: 100) {{
                                    pageInfo {{
                                        hasNextPage
                                        endCursor
                                    }}
                                    nodes {{
                                        message
                                        message_head_line: messageHeadline
                                        message_body: messageBody
                                        sha: oid
                                        additions
                                        deletions
                                        id
                                        committed_date: committedDate
                                        authorName: author {{
                                            author_name: name
                                        }}
                                        authorEmail: author {{
                                            author_email: email
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo
        );
        GitHubTableGraphQLParams::new(query.into(), None, 1, Some(gql_schema()))
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sha", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("author_name", DataType::Utf8, true),
        Field::new("author_email", DataType::Utf8, true),
        Field::new(
            "committed_date",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("message", DataType::Utf8, true),
        Field::new("message_body", DataType::Utf8, true),
        Field::new("message_head_line", DataType::Utf8, true),
        Field::new("additions", DataType::Int64, true),
        Field::new("deletions", DataType::Int64, true),
    ]))
}
