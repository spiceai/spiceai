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

//! `github.com/{owner}/{repo}/release_assets` — one row per release asset.
//!
//! Per-asset download counts are the headline OSS adoption metric. They live on
//! their own table rather than as a nested list on `releases` because every
//! column here is a scalar, which is what lets the table survive a `DuckDB` or
//! `SQLite` acceleration refresh — a `List(Struct)` column does not.
//!
//! Join back to `releases` on `release_id`, or read `release_tag_name`
//! directly.

use crate::identity::push_identity_fields;
use crate::nested_connection::{NestedConnection, fan_out};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connector_graphql::graphql::{ErrorChecker, GraphQLContext, Result, client::UnnestBehavior};
use data_connector_api::ConnectorComponent;
use serde_json::Value;
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;
use crate::releases::{ASSETS_PER_RELEASE, RELEASES_PAGE_SIZE};

/// Response key holding the release's node id on every asset row.
const RELEASE_ID_KEY: &str = "release_id";

/// Response key holding the release's tag on every asset row.
const RELEASE_TAG_NAME_KEY: &str = "release_tag_name";

/// Fans each release's `releaseAssets` connection out into one row per asset.
const RELEASE_ASSETS_CONNECTION: NestedConnection<'static> = NestedConnection {
    connection_key: "releaseAssets",
    parent_keys: &[RELEASE_ID_KEY, RELEASE_TAG_NAME_KEY],
    parent_id_key: RELEASE_TAG_NAME_KEY,
    parent_label: "release",
    child_label: "assets",
};

// https://docs.github.com/en/graphql/reference/objects#releaseasset
#[derive(Debug)]
pub struct ReleaseAssetsTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ReleaseAssetsTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (releases) + 100 (releaseAssets per release)
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(1 + ASSETS_PER_RELEASE)
    }
}

impl GitHubTableArgs for ReleaseAssetsTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
        // predicate on the sort key, so ordering by a mutable field lets a release
        // touched on the source mid-scan jump ahead of the cursor, where no remaining
        // page will return it — silently dropping every asset on it from the scan.
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    releases(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            {release_id}: id
                            {release_tag_name}: tagName
                            releaseAssets(first: {assets_per_release}) {{
                                totalCount
                                nodes {{
                                    id
                                    name
                                    size
                                    url: downloadUrl
                                    download_count: downloadCount
                                    content_type: contentType
                                    created_at: createdAt
                                    updated_at: updatedAt
                                }}
                            }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            page_size = RELEASES_PAGE_SIZE,
            assets_per_release = ASSETS_PER_RELEASE,
            release_id = RELEASE_ID_KEY,
            release_tag_name = RELEASE_TAG_NAME_KEY,
        );

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                Ok(fan_out(
                    object,
                    &RELEASE_ASSETS_CONNECTION,
                    &owner,
                    &repo,
                    |_| {},
                ))
            })),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(RELEASE_ID_KEY, DataType::Utf8, true),
        Field::new(RELEASE_TAG_NAME_KEY, DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("download_count", DataType::Int64, true),
        Field::new("size", DataType::Int64, true),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
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
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::{ReleaseAssetsTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Value, json};

    fn args() -> ReleaseAssetsTableArgs {
        ReleaseAssetsTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            component: shared_component("test.release_assets"),
        }
    }

    fn release_node() -> Value {
        json!({
            "release_id": "RE_1",
            "release_tag_name": "v2.2.0",
            "releaseAssets": {
                "totalCount": 2,
                "nodes": [
                    {
                        "id": "RA_1",
                        "name": "spice_darwin_aarch64.tar.gz",
                        "size": 24_121_699,
                        "url": "https://github.com/spiceai/spiceai/releases/download/v2.2.0/a.tar.gz",
                        "download_count": 29,
                        "content_type": "application/octet-stream",
                        "created_at": "2026-08-24T19:31:35Z",
                        "updated_at": "2026-08-24T19:40:00Z"
                    },
                    {
                        "id": "RA_2",
                        "name": "spice_linux_x86_64.tar.gz",
                        "size": 29_704_101,
                        "url": "https://github.com/spiceai/spiceai/releases/download/v2.2.0/b.tar.gz",
                        "download_count": 1294,
                        "content_type": "application/octet-stream",
                        "created_at": "2026-08-24T19:31:35Z",
                        "updated_at": "2026-08-24T19:40:00Z"
                    }
                ]
            }
        })
    }

    #[test]
    fn unnest_emits_one_row_per_asset_carrying_its_release() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("release_assets must fan out its rows with a custom unnest");
        };

        let rows = unnest(&release_node()).expect("unnest to succeed");

        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row["release_id"], json!("RE_1"));
            assert_eq!(row["release_tag_name"], json!("v2.2.0"));
            assert_eq!(row["owner"], json!("spiceai"));
            assert_eq!(row["repo"], json!("spiceai"));
        }
        assert_eq!(rows[0]["download_count"], json!(29));
        assert_eq!(rows[1]["download_count"], json!(1294));
    }

    #[test]
    fn every_column_is_a_scalar_so_the_table_can_be_accelerated() {
        // A `List(Struct)` column cannot survive a DuckDB or SQLite refresh, which
        // is the reason assets are their own table rather than a nested list on
        // `releases`.
        for field in gql_schema().fields() {
            assert!(
                field.data_type().is_primitive()
                    || matches!(
                        field.data_type(),
                        arrow_schema::DataType::Utf8 | arrow_schema::DataType::Boolean
                    ),
                "'{}' is {:?}, which is not a scalar",
                field.name(),
                field.data_type()
            );
        }
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("release_assets must fan out its rows with a custom unnest");
        };

        let rows = unnest(&release_node()).expect("unnest to succeed");
        let schema = gql_schema();

        for key in rows[0].as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "release_assets emits '{key}' but the schema does not declare it"
            );
        }
    }
}
