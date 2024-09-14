/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::component::dataset::Dataset;
use arrow::array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::{
    github::{GithubFilesTableProvider, GithubRestClient},
    graphql::{client::GraphQLClient, provider::GraphQLTableProviderBuilder},
};
use datafusion::datasource::TableProvider;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use url::Url;

use super::{
    graphql::default_spice_client, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};

pub struct Github {
    params: Parameters,
}

pub type GraphQLQuery = Arc<str>;
pub type JSONPointer = Arc<str>;
pub type UnnestDepth = usize;

pub trait GithubTableArgs: Send + Sync {
    /// Converts the arguments for a Github table into a tuple of:
    ///   1. The GraphQL query string
    ///   2. The JSON pointer to the data in the response
    ///   3. The depth to unnest the data
    ///   4. The GraphQL schema of the response data
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth, Option<SchemaRef>);
}

// TODO: implement PR filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for pullRequests`.
pub struct PullRequestTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for PullRequestTableArgs {
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth, Option<SchemaRef>) {
        let query = format!(
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

                            author {{
                                login
                            }}
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
        );

        (
            query.into(),
            "/data/repository/pullRequests/nodes".into(),
            1,
            None,
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PullRequestState {
    Open,
    Closed,
    Merged,
}

// TODO: implement filters from https://docs.github.com/en/graphql/reference/objects#commit `Arguments for history`.
pub struct CommitTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for CommitTableArgs {
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth, Option<SchemaRef>) {
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
        (
            query.into(),
            "/data/repository/defaultBranchRef/target/history/nodes".into(),
            1,
            None,
        )
    }
}

// TODO: implement PR filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for issues`
pub struct IssueTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for IssueTableArgs {
    #[allow(clippy::too_many_lines)]
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth, Option<SchemaRef>) {
        let query = format!(
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
        );

        let gql_schema = Arc::new(Schema::new(vec![
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
        ]));

        (
            query.into(),
            "/data/repository/issues/nodes".into(),
            2,
            Some(gql_schema),
        )
    }
}

// TODO: implement filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for stargazers`
pub struct StargazersTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for StargazersTableArgs {
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth, Option<SchemaRef>) {
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    stargazers(first: 100) {{
                        edges {{
                            starred_at: starredAt
                            node {{
                                login
                                name
                                avatar_url: avatarUrl
                                bio
                                location
                                company
                                email
                                x_username: twitterUsername
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
            name = self.repo
        );

        (
            query.into(),
            "/data/repository/stargazers/edges".into(),
            1,
            None,
        )
    }
}

impl Github {
    pub(crate) fn create_graphql_client(
        &self,
        tbl: &Arc<dyn GithubTableArgs>,
    ) -> std::result::Result<GraphQLClient, Box<dyn std::error::Error + Send + Sync>> {
        let access_token = self.params.get("token").expose().ok();

        let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
            return Err("Github 'endpoint' not provided".into());
        };

        let client = default_spice_client("application/json").boxed()?;

        let (query, json_pointer, unnest_depth, schema) = tbl.get_graphql_values();

        Ok(GraphQLClient::new(
            client,
            Url::parse(&format!("{endpoint}/graphql")).boxed()?,
            query,
            json_pointer,
            access_token,
            None,
            None,
            unnest_depth,
            schema,
        ))
    }

    async fn create_gql_table_provider(
        &self,
        table_args: Arc<dyn GithubTableArgs>,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = self.create_graphql_client(&table_args).context(
            super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            },
        )?;

        Ok(Arc::new(
            GraphQLTableProviderBuilder::new(client)
                .with_schema_transform(github_gql_raw_schema_cast)
                .build()
                .await
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                })?,
        ))
    }

    pub(crate) fn create_rest_client(
        &self,
    ) -> std::result::Result<GithubRestClient, Box<dyn std::error::Error + Send + Sync>> {
        let Some(access_token) = self.params.get("token").expose().ok() else {
            return Err("Github token not provided".into());
        };

        Ok(GithubRestClient::new(access_token))
    }

    async fn create_files_table_provider(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: Option<&str>,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(tree_sha) = tree_sha.filter(|s| !s.is_empty()) else {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: format!("Branch or tag name is required in dataset definition; must be 'github.com/{owner}/{repo}/files/BRANCH_NAME'").into(),
            });
        };

        let client = self
            .create_rest_client()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            })?;

        let include = match self.params.get("include").expose().ok() {
            Some(pattern) => Some(parse_globs(pattern)?),
            None => None,
        };

        Ok(Arc::new(
            GithubFilesTableProvider::new(
                client,
                owner,
                repo,
                tree_sha,
                include,
                dataset.is_accelerated(),
            )
            .await
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            })?,
        ))
    }
}

fn github_gql_raw_schema_cast(
    record_batch: &RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for (idx, field) in record_batch.schema().fields().iter().enumerate() {
        let column = record_batch.column(idx);
        if let DataType::List(inner_field) = field.data_type() {
            if let DataType::Struct(struct_fields) = inner_field.data_type() {
                if struct_fields.len() == 1 {
                    let (new_column, new_field) =
                        arrow_tools::record_batch::to_primitive_type_list(column, field)?;
                    fields.push(new_field);
                    columns.push(new_column);
                    continue;
                }
            }
        }

        fields.push(Arc::clone(field));
        columns.push(Arc::clone(column));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(std::convert::Into::into)
}

#[derive(Default, Copy, Clone)]
pub struct GithubFactory {}

impl GithubFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("token")
        .description("A Github token.")
        .secret(),
    ParameterSpec::connector("endpoint")
        .description("The Github API endpoint.")
        .default("https://api.github.com"),
    ParameterSpec::runtime("include")
        .description("Include only files matching the pattern.")
        .examples(&["*.json", "**/*.yaml;src/**/*.json"]),
];

impl DataConnectorFactory for GithubFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Github { params }) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "github"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Github {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path().clone();
        let mut parts = path.split('/');

        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("github.com"), Some(owner), Some(repo), Some("pulls")) => {
                let table_args = Arc::new(PullRequestTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                });
                self.create_gql_table_provider(table_args).await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("commits")) => {
                let table_args = Arc::new(CommitTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                });
                self.create_gql_table_provider(table_args).await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("issues")) => {
                let table_args = Arc::new(IssueTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                });
                self.create_gql_table_provider(table_args).await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("stargazers")) => {
                let table_args = Arc::new(StargazersTableArgs {
                    owner: owner.to_string(),
                    repo: repo.to_string(),
                });
                self.create_gql_table_provider(table_args).await
            }
            (Some("github.com"), Some(owner), Some(repo), Some("files")) => {
                self.create_files_table_provider(owner, repo, parts.next(), dataset)
                    .await
            }
            (Some("github.com"), Some(_), Some(_), Some(invalid_table)) => {
                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "github".to_string(),
                    source: format!("Invalid Github table type: {invalid_table}").into(),
                })
            }
            (_, Some(owner), Some(repo), _) => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: format!("`from` must start with 'github.com/{owner}/{repo}'").into(),
            }),
            _ => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                source: "Invalid Github dataset path".into(),
            }),
        }
    }
}

pub fn parse_globs(input: &str) -> super::DataConnectorResult<Arc<GlobSet>> {
    let patterns: Vec<&str> = input.split(&[',', ';'][..]).collect();
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        let trimmed_pattern = pattern.trim();
        if !trimmed_pattern.is_empty() {
            builder.add(
                Glob::new(trimmed_pattern).context(super::InvalidGlobPatternSnafu { pattern })?,
            );
        }
    }

    let glob_set = builder
        .build()
        .context(super::InvalidGlobPatternSnafu { pattern: input })?;
    Ok(Arc::new(glob_set))
}
