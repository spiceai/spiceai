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

//! `github.com/{owner}/{repo}/releases` — one row per release.
//!
//! Release asset download counts are a headline adoption metric with no SQL
//! path today, and release cadence needs `published_at` over a full history.
//!
//! Every column is a scalar so the table survives a `DuckDB` or `SQLite`
//! acceleration refresh: `total_download_count` answers the headline question
//! directly, and per-asset detail lives in `release_assets`, which joins back
//! on the release `id`.

use crate::identity::{insert_identity, push_identity_fields};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connector_graphql::graphql::{
    ErrorChecker, GraphQLContext, Result,
    client::{DuplicateBehavior, UnnestBehavior, unnest_json_object_to_depth},
};
use data_connector_api::ConnectorComponent;
use serde_json::{Map, Value};
use std::sync::Arc;

use super::{GitHubTableArgs, GitHubTableGraphQLParams};
use crate::github::error_checker;

/// Releases fetched per page. Release notes are long — a Spice release body
/// runs to tens of kilobytes — so this stays well below the 100 GitHub allows
/// to keep a single response a sane size. Shared with `release_assets`, which
/// pages over the same connection.
pub(crate) const RELEASES_PAGE_SIZE: u32 = 25;

/// Assets fetched per release. 100 is GitHub's per-connection maximum and a
/// nested connection cannot be paginated, so `assets_count` records GitHub's own
/// total and makes a short `total_download_count` detectable in SQL.
pub(crate) const ASSETS_PER_RELEASE: u32 = 100;

// https://docs.github.com/en/graphql/reference/objects#release
#[derive(Debug)]
pub struct ReleasesTableArgs {
    pub owner: String,
    pub repo: String,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ReleasesTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (releases) + 100 (releaseAssets per release)
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(1 + ASSETS_PER_RELEASE)
    }
}

impl GitHubTableArgs for ReleasesTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
        // predicate on the sort key, so ordering by a mutable field lets a release
        // touched on the source mid-scan jump ahead of the cursor, where no remaining
        // page will return it — silently dropping the row from the scan.
        let query = format!(
            r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    releases(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            id
                            name
                            url
                            tag_name: tagName
                            body: description
                            is_draft: isDraft
                            is_prerelease: isPrerelease
                            is_latest: isLatest
                            created_at: createdAt
                            published_at: publishedAt
                            updated_at: updatedAt
                            author: author {{ author: login }}
                            tag_sha: tagCommit {{ tag_sha: oid }}
                            assets_wrapper: releaseAssets(first: {assets_per_release}) {{
                                assets_count: totalCount
                                assets: nodes {{ download_count: downloadCount }}
                            }}
                        }}
                    }}
                }}
            }}"#,
            owner = self.owner,
            name = self.repo,
            page_size = RELEASES_PAGE_SIZE,
            assets_per_release = ASSETS_PER_RELEASE,
        );

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                let mut rows =
                    unnest_json_object_to_depth(object.clone(), 1, &DuplicateBehavior::Error)?;

                for row in &mut rows {
                    if let Value::Object(row) = row {
                        insert_identity(row, &owner, Some(&repo));
                        add_total_download_count(row, &owner, &repo);
                    }
                }

                Ok(rows)
            })),
            Some(gql_schema()),
        )
    }
}

/// Sums the release's asset download counts into `total_download_count`, so the
/// headline adoption metric is a scalar rather than something a caller has to
/// join `release_assets` to compute.
///
/// GitHub caps the nested asset connection at one page. When a release has more
/// assets than one page, the sum can only cover the assets that arrived, so the
/// total is left NULL rather than published as a number that is knowably too
/// small — a null reads as "unknown", a short total reads as fact.
/// `assets_count` still records what GitHub said the true total was.
fn add_total_download_count(release: &mut Map<String, Value>, owner: &str, repo: &str) {
    let assets = release.remove("assets");
    let assets = assets.as_ref().and_then(Value::as_array);

    // Counts the assets that actually reached the sum, not the entries that
    // arrived. A connection node GitHub returns as null — or one missing
    // `download_count` — contributes nothing, so counting it as returned would
    // let the truncation check below pass and publish a total that is knowably
    // short.
    let (total, returned) = assets.map_or((0_i64, 0_usize), |assets| {
        assets
            .iter()
            .filter_map(|asset| asset.get("download_count").and_then(Value::as_i64))
            .fold((0_i64, 0_usize), |(total, counted), downloads| {
                (total.saturating_add(downloads), counted + 1)
            })
    });

    let assets_count = release.get("assets_count").and_then(Value::as_i64);
    let truncated =
        assets_count.is_some_and(|count| count > i64::try_from(returned).unwrap_or(i64::MAX));

    if truncated {
        let tag = release
            .get("tag_name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let assets_count = assets_count.unwrap_or_default();
        tracing::warn!(
            "GitHub returned only {returned} of {assets_count} assets for release '{tag}' of '{owner}/{repo}', so `total_download_count` is null for it rather than a total that is knowably too small. GitHub caps a nested connection at one page and cannot paginate it, which is a limit of GitHub's API rather than of the dataset's configuration; follow https://github.com/spiceai/spiceai/issues/13458 for nested pagination. See: https://spiceai.org/docs/components/data-connectors/github"
        );
    }

    release.insert(
        "total_download_count".to_string(),
        if truncated {
            Value::Null
        } else {
            Value::from(total)
        },
    );
}

fn gql_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("tag_name", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("tag_sha", DataType::Utf8, true),
        Field::new("is_draft", DataType::Boolean, true),
        Field::new("is_prerelease", DataType::Boolean, true),
        Field::new("is_latest", DataType::Boolean, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "published_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("url", DataType::Utf8, true),
        Field::new("assets_count", DataType::Int64, true),
        Field::new("total_download_count", DataType::Int64, true),
    ];

    push_identity_fields(&mut fields, true);

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::{ASSETS_PER_RELEASE, ReleasesTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::GraphQLContext;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Value, json};

    fn args() -> ReleasesTableArgs {
        ReleasesTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            component: shared_component("test.releases"),
        }
    }

    fn release(assets_count: i64, downloads: &[i64]) -> Value {
        let assets: Vec<Value> = downloads
            .iter()
            .map(|count| json!({"download_count": count}))
            .collect();

        json!({
            "id": "RE_1",
            "name": "Spice v2.2.0",
            "url": "https://github.com/spiceai/spiceai/releases/tag/v2.2.0",
            "tag_name": "v2.2.0",
            "body": "notes",
            "is_draft": false,
            "is_prerelease": false,
            "is_latest": true,
            "created_at": "2026-08-24T19:31:35Z",
            "published_at": "2026-08-24T20:23:46Z",
            "updated_at": "2026-08-26T00:08:31Z",
            "author": {"author": "sgrebnov"},
            "tag_sha": {"tag_sha": "c9148ab"},
            "assets_wrapper": {"assets_count": assets_count, "assets": assets}
        })
    }

    fn unnest_one(value: &Value) -> Value {
        let params = args().get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("releases must use a custom unnest");
        };
        let rows = unnest(value).expect("unnest to succeed");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().unwrap_or(Value::Null)
    }

    #[test]
    fn query_paginates_on_an_immutable_sort_key() {
        let query = args().get_graphql_values().query.to_string();

        assert!(
            query.contains("orderBy: {field: CREATED_AT, direction: ASC}"),
            "releases query must order by CREATED_AT ASC, got:\n{query}"
        );
        assert!(
            !query.contains("UPDATED_AT"),
            "releases query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn total_download_count_sums_every_asset() {
        let row = unnest_one(&release(3, &[4, 29, 5]));

        assert_eq!(row["total_download_count"], json!(38));
        assert_eq!(row["assets_count"], json!(3));
        assert_eq!(row["author"], json!("sgrebnov"));
        assert_eq!(row["tag_sha"], json!("c9148ab"));
        assert_eq!(row["owner"], json!("spiceai"));
        assert_eq!(row["repo"], json!("spiceai"));
    }

    #[test]
    fn total_download_count_is_zero_for_a_release_with_no_assets() {
        let row = unnest_one(&release(0, &[]));

        assert_eq!(row["total_download_count"], json!(0));
        assert_eq!(row["assets_count"], json!(0));
    }

    #[test]
    fn a_truncated_asset_page_nulls_the_total_rather_than_under_reporting_it() {
        // A sum over only the assets that arrived is knowably too small, and a
        // number reads as fact where a null reads as unknown.
        let row = unnest_one(&release(150, &[1, 2]));

        assert_eq!(row["assets_count"], json!(150));
        assert_eq!(row["total_download_count"], Value::Null);
    }

    /// A connection node GitHub returns as null contributes nothing to the sum,
    /// so counting it as an asset that arrived would make `assets_count` match
    /// and publish a total missing that asset's downloads as if it were the
    /// whole figure.
    #[test]
    fn a_null_asset_node_nulls_the_total_rather_than_passing_the_count_check() {
        let mut node = release(3, &[4, 29]);
        node["assets_wrapper"]["assets"]
            .as_array_mut()
            .expect("assets to be an array")
            .push(Value::Null);

        let row = unnest_one(&node);

        assert_eq!(row["assets_count"], json!(3));
        assert_eq!(row["total_download_count"], Value::Null);
    }

    #[test]
    fn the_raw_asset_list_never_reaches_a_row() {
        // A `List(Struct)` column cannot survive a DuckDB or SQLite refresh, so
        // per-asset detail lives on `release_assets` instead.
        let row = unnest_one(&release(1, &[7]));

        assert!(row.as_object().expect("row object").get("assets").is_none());
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        let row = unnest_one(&release(1, &[7]));
        let schema = gql_schema();

        for key in row.as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "releases emits '{key}' but the schema does not declare it"
            );
        }
    }

    #[test]
    fn query_cost_stays_within_the_github_secondary_rate_limit_burst() {
        let cost = args()
            .query_cost()
            .expect("releases to declare a query cost");
        assert_eq!(cost, 1 + ASSETS_PER_RELEASE);
        assert!(cost <= 2000, "releases query cost {cost} exceeds the burst");
    }
}
