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

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::identity::{identity_unnest, push_identity_fields};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use connector_graphql::graphql::{ErrorChecker, GraphQLContext};
use http::{HeaderMap, HeaderValue};
use serde_json::Value;
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#projectv2
#[derive(Debug)]
pub struct ProjectsTableArgs {
    pub owner: String,
    pub repo: Option<String>,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ProjectsTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        let owner = self.owner.clone();
        let repo = self.repo.clone();

        Some(Arc::new(
            move |headers: &HeaderMap<HeaderValue>, response: &Value| {
                let target = repo
                    .as_ref()
                    .map_or_else(|| owner.clone(), |repo| format!("{owner}/{repo}"));
                let target_kind = if repo.is_some() {
                    "repository projects"
                } else {
                    "organization projects"
                };

                // Trace the response for debugging
                tracing::trace!(
                    "GitHub projects GraphQL response for {target}: {}",
                    serde_json::to_string(response)
                        .unwrap_or_else(|_| "Unable to serialize response".to_string())
                );

                // First check standard GitHub errors (rate limits, etc.)
                crate::github::error_checker(headers, response)?;

                // GitHub bug: When the app doesn't have access to Projects v2, GitHub sometimes
                // returns "Something went wrong while executing your query" instead of a proper
                // permission error. This appears to be a GitHub API bug where lack of permissions
                // triggers an internal error rather than returning a proper authorization error.
                if let Some(errors) = response.get("errors") {
                    tracing::debug!(
                        "GitHub projects query for {target} returned errors: {:?}",
                        errors
                    );
                    if let Some(errors_array) = errors.as_array() {
                        for error in errors_array {
                            if let Some(message) = error.get("message").and_then(|m| m.as_str())
                                && message
                                    .contains("Something went wrong while executing your query")
                            {
                                tracing::error!(
                                    "GitHub returned a misleading projects error for {target}; treating it as a permissions failure"
                                );
                                return Err(connector_graphql::graphql::Error::InvalidCredentialsOrPermissions {
                                message: format!("Failed to access {target_kind} for {target}: GitHub reported an internal query error, which usually means the GitHub App lacks project read permissions. Verify the app has the required project access."),
                            });
                            }
                        }
                    }
                }

                Ok(())
            },
        ))
    }

    fn query_cost(&self) -> Option<u32> {
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(1)
    }
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
            identity_unnest(2, self.owner.clone(), self.repo.clone()),
            Some(gql_schema(self.repo.is_some())),
        )
    }
}

fn gql_schema(repo_scoped: bool) -> SchemaRef {
    let mut fields = vec![
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
    ];

    // A repository-scoped projects dataset carries `repo`; an organization-scoped
    // one carries only `owner`.
    push_identity_fields(&mut fields, repo_scoped);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use connector_graphql::graphql::client::UnnestBehavior;
    use runtime::builder::RuntimeBuilder;
    use runtime::component::dataset::builder::DatasetBuilder;
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
    fn test_projects_schema() {
        let schema = gql_schema(true);

        // Verify all expected fields are present with correct types, plus the
        // `owner` / `repo` identity columns.
        assert_eq!(schema.fields().len(), 14);
        assert_eq!(schema.field(12).name(), "owner");
        assert_eq!(schema.field(13).name(), "repo");

        // An organization-scoped projects dataset has no repository to name.
        let org_schema = gql_schema(false);
        assert_eq!(org_schema.fields().len(), 13);
        assert_eq!(org_schema.field(12).name(), "owner");

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
        assert!(query.contains("creator: creator"));
        assert!(query.contains("creator: login"));

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
        assert!(query.contains("creator: creator"));
        assert!(query.contains("creator: login"));

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
        assert!(graphql_params.schema.is_some());

        // Verify the schema matches what we expect, including the `owner` /
        // `repo` identity columns the custom unnest stamps onto each row.
        let schema = graphql_params.schema.expect("schema should be present");
        assert_eq!(schema.fields().len(), 14);

        let UnnestBehavior::Custom(unnest) = &graphql_params.unnest_behavior else {
            panic!("projects must stamp identity with a custom unnest");
        };
        let rows = unnest(&json!({
            "id": "PVT_1",
            "title": "Roadmap",
            "creator": {"creator": "lukekim"}
        }))
        .expect("unnest to succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["creator"], json!("lukekim"));
        assert_eq!(rows[0]["owner"], json!("spiceai"));
        assert_eq!(rows[0]["repo"], json!("spiceai"));
    }

    #[test]
    fn test_projects_org_scope_carries_only_owner() {
        let args = ProjectsTableArgs {
            owner: "spiceai".to_string(),
            repo: None,
            component: create_mock_component("github.com/spiceai/projects"),
        };

        let graphql_params = args.get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &graphql_params.unnest_behavior else {
            panic!("projects must stamp identity with a custom unnest");
        };
        let rows = unnest(&json!({"id": "PVT_1", "title": "Roadmap"})).expect("unnest to succeed");

        assert_eq!(rows[0]["owner"], json!("spiceai"));
        assert!(rows[0].get("repo").is_none());
    }
}
