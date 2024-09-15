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

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

// TODO: implement filters from https://docs.github.com/en/graphql/reference/objects#commit `Arguments for history`.
pub struct CommitsTableArgs {
    pub owner: String,
    pub repo: String,
}

impl GitHubTableArgs for CommitsTableArgs {
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
        GitHubTableGraphQLParams::new(
            query.into(),
            "/data/repository/defaultBranchRef/target/history/nodes".into(),
            1,
            None,
        )
    }
}