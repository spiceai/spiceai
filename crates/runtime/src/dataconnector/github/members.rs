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

use crate::dataconnector::ConnectorComponent;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::graphql::client::UnnestBehavior;
use std::sync::Arc;

pub struct MembersTableArgs {
    pub org: String,
    pub component: ConnectorComponent,
}

impl GitHubTableArgs for MembersTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = format!(
            r#"{{
                organization(login: "{org}") {{
                    membersWithRole(first: 100) {{
                        nodes {{
                            username: login
                            name
                            avatar_url: avatarUrl
                            url
                            email
                            location
                            company
                            created_at: createdAt
                            bio
                        }}
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                    }}
                }}
            }}"#,
            org = self.org
        );

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Depth(0),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("username", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("avatar_url", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("company", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("bio", DataType::Utf8, true),
    ]))
}
