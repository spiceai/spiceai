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
