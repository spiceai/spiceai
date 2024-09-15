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

use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use super::{GitHubTableArgs, GitHubTableGraphQLParams};

// https://docs.github.com/en/graphql/reference/objects#repository
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
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "starred_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("login", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("company", DataType::Utf8, true),
        Field::new("x_username", DataType::Utf8, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("avatar_url", DataType::Utf8, true),
        Field::new("bio", DataType::Utf8, true),
    ]))
}
