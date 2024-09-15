use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use super::{GitHubTableArgs, GitHubTableGraphQLParams, GraphQLQuery, JSONPointer, UnnestDepth};

// TODO: implement PR filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for pullRequests`.
pub struct PullRequestTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GitHubTableArgs for PullRequestTableArgs {
    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
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

        GitHubTableGraphQLParams::new(
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