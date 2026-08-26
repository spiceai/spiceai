/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `github.com/{login}/user` — the public profile of one GitHub user.
//!
//! `members` covers organization members only, so a login outside the
//! organization — a contributor, a stargazer, an issue reporter — could not be
//! resolved at all. The table holds a single row, the profile of the login the
//! dataset names.
//!
//! `email` is deliberately absent: GitHub gates it behind the `read:user` or
//! `user:email` token scope and fails the whole query with `INSUFFICIENT_SCOPES`
//! when the token lacks it, which would make the table unusable for the
//! `repo`-scoped tokens the rest of the connector runs on.

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connector_graphql::graphql::{ErrorChecker, GraphQLContext, client::UnnestBehavior};
use data_connector_api::ConnectorComponent;
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;

/// The response has no `pageInfo` to infer a data path from, so the pointer to
/// the single user object is given explicitly.
const USER_JSON_POINTER: &str = "/data/user";

// https://docs.github.com/en/graphql/reference/objects#user
#[derive(Debug)]
pub struct UsersTableArgs {
    pub login: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for UsersTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (user) + 4 count-only connections
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(5)
    }
}

impl GitHubTableArgs for UsersTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = format!(
            r#"{{
                user(login: "{login}") {{
                    id
                    login
                    name
                    company
                    bio
                    location
                    url
                    blog: websiteUrl
                    twitter_username: twitterUsername
                    avatar_url: avatarUrl
                    created_at: createdAt
                    updated_at: updatedAt
                    is_hireable: isHireable
                    followers: followers {{ followers: totalCount }}
                    following: following {{ following: totalCount }}
                    public_repos: repositories(privacy: PUBLIC) {{ public_repos: totalCount }}
                    public_gists: gists(privacy: PUBLIC) {{ public_gists: totalCount }}
                }}
            }}"#,
            login = self.login,
        );

        GitHubTableGraphQLParams::new(
            query.into(),
            Some(USER_JSON_POINTER),
            UnnestBehavior::Depth(1),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("login", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("company", DataType::Utf8, true),
        Field::new("bio", DataType::Utf8, true),
        Field::new("blog", DataType::Utf8, true),
        Field::new("twitter_username", DataType::Utf8, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("avatar_url", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("is_hireable", DataType::Boolean, true),
        Field::new("followers", DataType::Int64, true),
        Field::new("following", DataType::Int64, true),
        Field::new("public_repos", DataType::Int64, true),
        Field::new("public_gists", DataType::Int64, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::{USER_JSON_POINTER, UsersTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::client::{
        DuplicateBehavior, UnnestBehavior, unnest_json_object_to_depth,
    };
    use serde_json::json;

    fn args() -> UsersTableArgs {
        UsersTableArgs {
            login: "lukekim".to_string(),
            component: shared_component("test.user"),
        }
    }

    #[test]
    fn query_supplies_an_explicit_json_pointer_because_there_is_no_page_info() {
        let params = args().get_graphql_values();

        assert_eq!(params.json_pointer, Some(USER_JSON_POINTER));
        assert!(!params.query.contains("pageInfo"));
    }

    #[test]
    fn query_never_requests_the_scope_gated_email_field() {
        // Requesting `email` fails the whole query with INSUFFICIENT_SCOPES on a
        // token without `read:user`, which is the token most datasets run on.
        let query = args().get_graphql_values().query.to_string();

        assert!(
            !query.contains("email"),
            "users query must not request the scope-gated email field, got:\n{query}"
        );
    }

    #[test]
    fn unnest_flattens_every_count_connection_into_a_scalar() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Depth(depth) = params.unnest_behavior else {
            panic!("users must use a depth unnest");
        };

        let rows = unnest_json_object_to_depth(
            json!({
                "id": "MDQ6VXNlcjgwMTc0",
                "login": "lukekim",
                "name": "Luke Kim",
                "company": "@SpiceAI",
                "bio": "Founder",
                "location": "Seattle, Washington",
                "url": "https://github.com/lukekim",
                "blog": "spice.ai",
                "twitter_username": "lukekim",
                "avatar_url": "https://avatars.githubusercontent.com/u/80174",
                "created_at": "2009-05-02T09:21:56Z",
                "updated_at": "2026-08-25T16:58:22Z",
                "is_hireable": false,
                "followers": {"followers": 113},
                "following": {"following": 42},
                "public_repos": {"public_repos": 44},
                "public_gists": {"public_gists": 6}
            }),
            depth,
            &DuplicateBehavior::Error,
        )
        .expect("unnest to succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["followers"], json!(113));
        assert_eq!(rows[0]["following"], json!(42));
        assert_eq!(rows[0]["public_repos"], json!(44));
        assert_eq!(rows[0]["public_gists"], json!(6));

        let schema = gql_schema();
        for key in rows[0].as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "users emits '{key}' but the schema does not declare it"
            );
        }
    }
}
