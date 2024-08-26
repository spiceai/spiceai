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
use async_trait::async_trait;
use data_components::graphql::{client::GraphQLClient, provider::GraphQLTableProvider};
use datafusion::datasource::TableProvider;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{any::Any, error::Error, future::Future, pin::Pin, sync::Arc};
use url::Url;

use super::{
    graphql::default_spice_client, DataConnector, DataConnectorFactory, ParameterSpec, Parameters,
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
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth);
}

// TODO: implement PR filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for pullRequests`.
pub struct PullRequestTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for PullRequestTableArgs {
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth) {
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
                            url
                            body
                            state
                            createdAt
                            mergedAt
                            closedAt
                            number
                            num_of_reviews: reviews {{totalCount}}
                            ref: headRef {{ id }}
                            
                            author {{
                                login
                            }}
                            additions
                            deletions
                            changedFiles
                            comments(first: 100) {{num_of_comments: totalCount}}
                            commits(first: 100) {{num_of_commits: totalCount, hashes: nodes{{ id }} }}
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
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth) {
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
                                        messageHeadline
                                        messageBody
                                        oid
                                        additions
                                        deletions
                                        id
                                        committedDate
                                        authorName: author {{
                                            name
                                        }}
                                        authorEmail: author {{
                                            email
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
        )
    }
}

// TODO: implement PR filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for issues`
pub struct IssueTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GithubTableArgs for IssueTableArgs {
    fn get_graphql_values(&self) -> (GraphQLQuery, JSONPointer, UnnestDepth) {
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
                            title
                            url
                            author: author {{ login }}
                            body
                            number
                            createdAt
                            updatedAt
                            closedAt
                            state
                            milestone {{ milestone_id: id, milestone_title: title }}
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            comments(first:100) {{ num_of_comments: totalCount, comments: nodes {{ body, author {{ login }} }} }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo
        );

        (query.into(), "/data/repository/issues/nodes".into(), 1)
    }
}

pub fn args_from_ds(
    ds: &Dataset,
) -> Result<Arc<dyn GithubTableArgs>, Box<dyn Error + Send + Sync>> {
    let path = ds.path().clone();
    let mut parts = path.split('/');

    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("github.com"), Some(owner), Some(repo), Some("pulls")) => {
            Ok(Arc::new(PullRequestTableArgs {
                owner: owner.to_string(),
                repo: repo.to_string(),
            }))
        }
        (Some("github.com"), Some(owner), Some(repo), Some("commits")) => {
            Ok(Arc::new(CommitTableArgs {
                owner: owner.to_string(),
                repo: repo.to_string(),
            }))
        }
        (Some("github.com"), Some(owner), Some(repo), Some("issues")) => {
            Ok(Arc::new(IssueTableArgs {
                owner: owner.to_string(),
                repo: repo.to_string(),
            }))
        }
        (Some("github.com"), Some(_), Some(_), Some(invalid_table)) => {
            Err(format!("Invalid Github table type: {invalid_table}").into())
        }
        (_, Some(owner), Some(repo), _) => {
            Err(format!("`from` must start with 'github.com/{owner}/{repo}'").into())
        }
        _ => Err("Invalid Github dataset path".into()),
    }
}

impl Github {
    pub(crate) fn create_graphql_client(
        &self,
        tbl: &Arc<dyn GithubTableArgs>,
    ) -> std::result::Result<GraphQLClient, Box<dyn std::error::Error + Send + Sync>> {
        let access_token = self.params.get("access_token").expose().ok();

        let Some(endpoint) = self.params.get("endpoint").expose().ok() else {
            return Err("Github 'endpoint' not provided".into());
        };

        let client = default_spice_client("application/json").boxed()?;

        let (query, json_pointer, unnest_depth) = tbl.get_graphql_values();

        Ok(GraphQLClient::new(
            client,
            Url::parse(endpoint).boxed()?,
            query,
            json_pointer,
            access_token,
            None,
            None,
            unnest_depth,
        ))
    }
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
    ParameterSpec::connector("access_token")
        .description("A Github access token.")
        .secret(),
    ParameterSpec::connector("endpoint")
        .description("The Github API endpoint.")
        .default("https://api.github.com/graphql"),
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
        let table_args = args_from_ds(dataset).context(super::UnableToGetReadProviderSnafu {
            dataconnector: "github".to_string(),
        })?;
        let client = self.create_graphql_client(&table_args).context(
            super::UnableToGetReadProviderSnafu {
                dataconnector: "github".to_string(),
            },
        )?;

        Ok(Arc::new(
            GraphQLTableProvider::new(client).await.boxed().context(
                super::UnableToGetReadProviderSnafu {
                    dataconnector: "github".to_string(),
                },
            )?,
        ))
    }
}
