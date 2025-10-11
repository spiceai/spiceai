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

use crate::dataconnector::ConnectorComponent;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::graphql::client::UnnestBehavior;
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#projectv2
pub struct ProjectsTableArgs {
    pub owner: String,
    pub repo: Option<String>,
    pub component: ConnectorComponent,
}

impl GitHubTableArgs for ProjectsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = if let Some(repo) = &self.repo {
            // Fetch projects for a specific repository
            format!(
                r#"{{
                    repository(owner: "{owner}", name: "{repo}") {{
                        projectsV2(first: 100) {{
                            edges {{
                                node {{
                                    id
                                    number
                                    title
                                    short_description: shortDescription
                                    readme
                                    public
                                    closed
                                    url
                                    created_at: createdAt
                                    updated_at: updatedAt
                                    closed_at: closedAt
                                    creator {{
                                        login
                                    }}
                                }}
                            }}
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                        }}
                    }}
                }}"#,
                owner = self.owner,
                repo = repo
            )
        } else {
            // Fetch projects for an organization or user
            format!(
                r#"{{
                    repositoryOwner(login: "{owner}") {{
                        ... on Organization {{
                            projectsV2(first: 100) {{
                                edges {{
                                    node {{
                                        id
                                        number
                                        title
                                        short_description: shortDescription
                                        readme
                                        public
                                        closed
                                        url
                                        created_at: createdAt
                                        updated_at: updatedAt
                                        closed_at: closedAt
                                        creator {{
                                            login
                                        }}
                                    }}
                                }}
                                pageInfo {{
                                    hasNextPage
                                    endCursor
                                }}
                            }}
                        }}
                        ... on User {{
                            projectsV2(first: 100) {{
                                edges {{
                                    node {{
                                        id
                                        number
                                        title
                                        short_description: shortDescription
                                        readme
                                        public
                                        closed
                                        url
                                        created_at: createdAt
                                        updated_at: updatedAt
                                        closed_at: closedAt
                                        creator {{
                                            login
                                        }}
                                    }}
                                }}
                                pageInfo {{
                                    hasNextPage
                                    endCursor
                                }}
                            }}
                        }}
                    }}
                }}"#,
                owner = self.owner
            )
        };

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Depth(1),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("short_description", DataType::Utf8, true),
        Field::new("readme", DataType::Utf8, true),
        Field::new("public", DataType::Boolean, true),
        Field::new("closed", DataType::Boolean, true),
        Field::new("url", DataType::Utf8, true),
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
        Field::new("creator", DataType::Utf8, true),
    ]))
}
