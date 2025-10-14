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
                                    creator: creator {{
                                        creator: login
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
                                        creator: creator {{
                                            creator: login
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
                                        creator: creator {{
                                            creator: login
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
            UnnestBehavior::Depth(2),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RuntimeBuilder;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;

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
    fn test_projects_schema() {
        let schema = gql_schema();

        // Verify all expected fields are present with correct types
        assert_eq!(schema.fields().len(), 12);

        // Check critical fields
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

        assert_eq!(schema.field(1).name(), "number");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);

        assert_eq!(schema.field(2).name(), "title");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);

        assert_eq!(schema.field(3).name(), "short_description");
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);

        // Check timestamp fields use underscore naming (indices 8, 9, 10 based on schema order)
        assert_eq!(schema.field(8).name(), "created_at");
        assert_eq!(
            schema.field(8).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        );

        assert_eq!(schema.field(9).name(), "updated_at");
        assert_eq!(
            schema.field(9).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        );

        assert_eq!(schema.field(10).name(), "closed_at");
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        );

        // Check creator field (unnested from creator.login)
        assert_eq!(schema.field(11).name(), "creator");
        assert_eq!(schema.field(11).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_projects_query_repo_specific() {
        let args = ProjectsTableArgs {
            owner: "spiceai".to_string(),
            repo: Some("spiceai".to_string()),
            component: create_mock_component("github.com/spiceai/spiceai/projects"),
        };

        let graphql_params = args.get_graphql_values();
        let query = graphql_params.query.as_ref();

        // Verify the query contains repository-specific structure
        assert!(query.contains("repository(owner:"));
        assert!(query.contains("projectsV2(first: 100)"));
        assert!(query.contains("created_at: createdAt"));
        assert!(query.contains("updated_at: updatedAt"));
        assert!(query.contains("closed_at: closedAt"));
        assert!(query.contains("short_description: shortDescription"));

        // Should NOT contain repositoryOwner or fragments
        assert!(!query.contains("repositoryOwner"));
        assert!(!query.contains("... on Organization"));
        assert!(!query.contains("... on User"));
    }

    #[test]
    fn test_projects_query_owner_level() {
        let args = ProjectsTableArgs {
            owner: "spiceai".to_string(),
            repo: None,
            component: create_mock_component("github.com/spiceai/projects"),
        };

        let graphql_params = args.get_graphql_values();
        let query = graphql_params.query.as_ref();

        // Verify the query contains owner-level structure with fragments
        assert!(query.contains("repositoryOwner(login:"));
        assert!(query.contains("... on Organization"));
        assert!(query.contains("... on User"));
        assert!(query.contains("projectsV2(first: 100)"));
        assert!(query.contains("created_at: createdAt"));
        assert!(query.contains("updated_at: updatedAt"));
        assert!(query.contains("closed_at: closedAt"));
        assert!(query.contains("short_description: shortDescription"));

        // Should NOT contain repository-specific structure
        assert!(!query.contains("repository(owner:"));
    }

    #[test]
    fn test_projects_graphql_params() {
        let args = ProjectsTableArgs {
            owner: "spiceai".to_string(),
            repo: Some("spiceai".to_string()),
            component: create_mock_component("github.com/spiceai/spiceai/projects"),
        };

        let graphql_params = args.get_graphql_values();

        // Verify GraphQL parameters are set correctly
        assert!(graphql_params.json_pointer.is_none());
        assert!(matches!(
            graphql_params.unnest_behavior,
            UnnestBehavior::Depth(2)
        ));
        assert!(graphql_params.schema.is_some());

        // Verify the schema matches what we expect
        let schema = graphql_params.schema.expect("schema should be present");
        assert_eq!(schema.fields().len(), 12);
    }
}
