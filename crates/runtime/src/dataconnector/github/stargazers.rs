use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::{GitHubTableArgs, GitHubTableGraphQLParams, GraphQLQuery, JSONPointer, UnnestDepth};

// TODO: implement filters from https://docs.github.com/en/graphql/reference/objects#repository `Arguments for stargazers`
pub struct StargazersTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GitHubTableArgs for StargazersTableArgs {
    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
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

        GitHubTableGraphQLParams::new(
            query.into(),
            "/data/repository/stargazers/edges".into(),
            1,
            None,
        )
    }
}