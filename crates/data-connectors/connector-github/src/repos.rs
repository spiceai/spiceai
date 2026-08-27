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

//! Repository metadata, in two shapes:
//!
//! - `github.com/{owner}/repos` — every repository an owner has, one row each.
//! - `github.com/{owner}/{repo}/repo` — a single repository.
//!
//! The connector had no repository-metadata table at all, so something as basic
//! as excluding archived repositories needed a direct API call. The owner-level
//! shape is also the table the `owner` / `repo` columns on every other GitHub
//! table join back to.

use crate::identity::{OWNER_COLUMN, REPO_COLUMN};
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

/// Repositories fetched per page.
const REPOS_PAGE_SIZE: u32 = 50;

/// Topics fetched per repository. `topics_count` is exposed alongside `topics`
/// so a repository with more topics than one page is detectable in SQL.
const TOPICS_PER_REPO: u32 = 50;

/// The single-repository response has no `pageInfo` to infer a data path from,
/// so the pointer to the repository object is given explicitly.
const REPOSITORY_JSON_POINTER: &str = "/data/repository";

// https://docs.github.com/en/graphql/reference/objects#repository
#[derive(Debug)]
pub struct ReposTableArgs {
    pub owner: String,
    /// `Some` for `github.com/{owner}/{repo}/repo`, which returns exactly one
    /// row; `None` for `github.com/{owner}/repos`, which pages over every
    /// repository the owner has.
    pub repo: Option<String>,
    pub component: ConnectorComponent,
}

impl GraphQLContext for ReposTableArgs {
    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // 1 (repositories) + 50 (repositoryTopics) + 3 count-only connections
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(4 + TOPICS_PER_REPO)
    }
}

impl ReposTableArgs {
    fn requested_nodes() -> String {
        format!(
            r"
            id
            name
            name_with_owner: nameWithOwner
            description
            url
            homepage_url: homepageUrl
            is_archived: isArchived
            is_private: isPrivate
            is_fork: isFork
            is_template: isTemplate
            is_disabled: isDisabled
            is_locked: isLocked
            created_at: createdAt
            updated_at: updatedAt
            pushed_at: pushedAt
            disk_usage: diskUsage
            stargazers_count: stargazerCount
            forks_count: forkCount
            default_branch: defaultBranchRef {{ default_branch: name }}
            license: licenseInfo {{ license: key }}
            primary_language: primaryLanguage {{ primary_language: name }}
            watchers_count: watchers {{ watchers_count: totalCount }}
            open_issues_count: issues(states: OPEN) {{ open_issues_count: totalCount }}
            open_pull_requests_count: pullRequests(states: OPEN) {{ open_pull_requests_count: totalCount }}
            topics_wrapper: repositoryTopics(first: {TOPICS_PER_REPO}) {{
                topics_count: totalCount
                topics: nodes {{ topic {{ name }} }}
            }}
        "
        )
    }
}

impl GitHubTableArgs for ReposTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let nodes = Self::requested_nodes();
        let owner = self.owner.clone();
        let repo = self.repo.clone();

        let (query, json_pointer) = match self.repo.as_deref() {
            Some(repo) => (
                format!(
                    r#"{{
                repository(owner: "{owner}", name: "{repo}") {{
                    {nodes}
                }}
            }}"#,
                    owner = self.owner,
                ),
                Some(REPOSITORY_JSON_POINTER),
            ),
            // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
            // predicate on the sort key, so ordering by a mutable field (e.g. PUSHED_AT)
            // lets a repository touched on the source mid-scan jump ahead of the cursor,
            // where no remaining page will return it — silently dropping it from the scan.
            //
            // `repositoryOwner` resolves both an organization and a user, so one shape
            // serves `github.com/{org}/repos` and `github.com/{user}/repos`.
            //
            // `ownerAffiliations: OWNER` because the connection otherwise defaults to
            // [OWNER, COLLABORATOR] and returns repositories owned by other accounts —
            // for `lukekim`, 86 rows rather than 68. `stamp_identity` labels every row
            // with the login the dataset names, so those extra rows would carry an
            // `owner` that does not own them, and any join on it would be wrong.
            None => (
                format!(
                    r#"{{
                repositoryOwner(login: "{owner}") {{
                    repositories(first: {page_size}, ownerAffiliations: OWNER, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            {nodes}
                        }}
                    }}
                }}
            }}"#,
                    owner = self.owner,
                    page_size = REPOS_PAGE_SIZE,
                ),
                None,
            ),
        };

        GitHubTableGraphQLParams::new(
            query.into(),
            json_pointer,
            UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
                let mut rows =
                    unnest_json_object_to_depth(object.clone(), 1, &DuplicateBehavior::Error)?;

                for row in &mut rows {
                    if let Value::Object(row) = row {
                        flatten_topics(row);
                        stamp_identity(row, &owner, repo.as_deref());
                    }
                }

                Ok(rows)
            })),
            Some(gql_schema()),
        )
    }
}

/// Stamps the identity columns from the dataset path rather than the response.
///
/// GitHub treats owner and repository names as case-insensitive and answers in
/// its own canonical casing, but SQL string equality is not case-insensitive.
/// Taking `owner` from the response would make `github.com/SpiceAI/spiceai/repo`
/// carry a different `owner` than every path-stamped table, and a join between
/// them would silently match nothing. `name_with_owner` still carries GitHub's
/// canonical spelling for anyone who wants it.
///
/// The owner-level shape has one row per repository, so only `repo` comes from
/// the response there — as `name`, which is the row's own identity rather than
/// the dataset's.
fn stamp_identity(row: &mut Map<String, Value>, owner: &str, repo: Option<&str>) {
    row.insert(OWNER_COLUMN.to_string(), Value::String(owner.to_string()));

    let repo = match repo {
        Some(repo) => Value::String(repo.to_string()),
        None => row.get("name").cloned().unwrap_or(Value::Null),
    };
    row.insert(REPO_COLUMN.to_string(), repo);
    row.remove("name");
}

/// Rewrites `topics`, which GitHub returns as `[{"topic": {"name": "sql"}}]`,
/// to the plain `["sql"]` the schema declares.
fn flatten_topics(repo: &mut Map<String, Value>) {
    let topics = repo
        .get("topics")
        .and_then(Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .filter_map(|entry| entry.get("topic")?.get("name")?.as_str())
                .map(|name| Value::String(name.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    repo.insert("topics".to_string(), Value::Array(topics));
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(OWNER_COLUMN, DataType::Utf8, false),
        Field::new(REPO_COLUMN, DataType::Utf8, true),
        Field::new("name_with_owner", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("homepage_url", DataType::Utf8, true),
        Field::new("default_branch", DataType::Utf8, true),
        Field::new("license", DataType::Utf8, true),
        Field::new("primary_language", DataType::Utf8, true),
        Field::new("is_archived", DataType::Boolean, true),
        Field::new("is_private", DataType::Boolean, true),
        Field::new("is_fork", DataType::Boolean, true),
        Field::new("is_template", DataType::Boolean, true),
        Field::new("is_disabled", DataType::Boolean, true),
        Field::new("is_locked", DataType::Boolean, true),
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
        Field::new(
            "pushed_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("disk_usage", DataType::Int64, true),
        Field::new("stargazers_count", DataType::Int64, true),
        Field::new("forks_count", DataType::Int64, true),
        Field::new("watchers_count", DataType::Int64, true),
        Field::new("open_issues_count", DataType::Int64, true),
        Field::new("open_pull_requests_count", DataType::Int64, true),
        Field::new("topics_count", DataType::Int64, true),
        Field::new(
            "topics",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::{REPOSITORY_JSON_POINTER, ReposTableArgs, gql_schema};
    use crate::GitHubTableArgs;
    use crate::test_util::shared_component;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Value, json};

    fn owner_level_args() -> ReposTableArgs {
        ReposTableArgs {
            owner: "spiceai".to_string(),
            repo: None,
            component: shared_component("test.repos"),
        }
    }

    fn repo_level_args() -> ReposTableArgs {
        ReposTableArgs {
            owner: "spiceai".to_string(),
            repo: Some("spiceai".to_string()),
            component: shared_component("test.repo"),
        }
    }

    fn repository_node() -> Value {
        json!({
            "id": "MDEwOlJlcG9zaXRvcnkz",
            "name": "spicetrade",
            "name_with_owner": "spiceai/spicetrade",
            "description": "Example",
            "url": "https://github.com/spiceai/spicetrade",
            "homepage_url": "",
            "is_archived": true,
            "is_private": true,
            "is_fork": false,
            "is_template": false,
            "is_disabled": false,
            "is_locked": false,
            "created_at": "2020-12-14T00:09:55Z",
            "updated_at": "2024-11-06T21:55:45Z",
            "pushed_at": "2023-07-25T17:08:09Z",
            "disk_usage": 4355,
            "stargazers_count": 1,
            "forks_count": 0,
            "default_branch": {"default_branch": "trunk"},
            "license": null,
            "primary_language": {"primary_language": "Python"},
            "watchers_count": {"watchers_count": 1},
            "open_issues_count": {"open_issues_count": 10},
            "open_pull_requests_count": {"open_pull_requests_count": 23},
            "topics_wrapper": {
                "topics_count": 2,
                "topics": [{"topic": {"name": "sql"}}, {"topic": {"name": "ai"}}]
            }
        })
    }

    fn unnest_one(args: &ReposTableArgs, value: &Value) -> Value {
        let params = args.get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("repos must use a custom unnest");
        };
        let rows = unnest(value).expect("unnest to succeed");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().unwrap_or(Value::Null)
    }

    #[test]
    fn owner_level_query_pages_over_every_repository_on_an_immutable_sort_key() {
        let params = owner_level_args().get_graphql_values();
        let query = params.query.to_string();

        assert!(query.contains("repositoryOwner(login: \"spiceai\")"));
        assert!(query.contains("orderBy: {field: CREATED_AT, direction: ASC}"));
        assert!(
            !query.contains("PUSHED_AT") && !query.contains("UPDATED_AT"),
            "repos query must not paginate on a mutable sort key, got:\n{query}"
        );
        assert!(
            params.json_pointer.is_none(),
            "the paginated shape infers its data path from pageInfo"
        );
    }

    #[test]
    fn repo_level_query_returns_one_row_and_names_its_data_path() {
        let params = repo_level_args().get_graphql_values();
        let query = params.query.to_string();

        assert!(query.contains("repository(owner: \"spiceai\", name: \"spiceai\")"));
        assert!(!query.contains("pageInfo"));
        assert_eq!(params.json_pointer, Some(REPOSITORY_JSON_POINTER));
    }

    #[test]
    fn unnest_flattens_owner_repo_and_topics() {
        let row = unnest_one(&owner_level_args(), &repository_node());

        // `owner` comes from the dataset path so it matches every other table
        // exactly; `repo` is the row's own name, since one row is one repository.
        assert_eq!(row["owner"], json!("spiceai"));
        assert_eq!(row["repo"], json!("spicetrade"));
        assert!(
            row.as_object().expect("row object").get("name").is_none(),
            "`name` is replaced by `repo`, not carried alongside it"
        );
        assert_eq!(row["default_branch"], json!("trunk"));
        assert_eq!(row["primary_language"], json!("Python"));
        assert_eq!(row["watchers_count"], json!(1));
        assert_eq!(row["open_issues_count"], json!(10));
        assert_eq!(row["open_pull_requests_count"], json!(23));
        assert_eq!(row["topics"], json!(["sql", "ai"]));
        assert_eq!(row["topics_count"], json!(2));
        // A repository with no license keeps a null rather than an empty string.
        assert_eq!(row["license"], Value::Null);
    }

    /// GitHub answers in its own canonical casing, so reading identity out of the
    /// response would make a join against a path-stamped table miss on casing.
    #[test]
    fn the_repo_level_shape_stamps_identity_from_the_path_not_the_response() {
        let args = ReposTableArgs {
            owner: "SpiceAI".to_string(),
            repo: Some("SpiceAI".to_string()),
            component: shared_component("test.repo_casing"),
        };

        let row = unnest_one(&args, &repository_node());

        assert_eq!(row["owner"], json!("SpiceAI"));
        assert_eq!(row["repo"], json!("SpiceAI"));
        // GitHub's canonical spelling is still available.
        assert_eq!(row["name_with_owner"], json!("spiceai/spicetrade"));
    }

    #[test]
    fn unnest_emits_an_empty_topic_list_rather_than_a_null() {
        let mut node = repository_node();
        node["topics_wrapper"] = json!({"topics_count": 0, "topics": []});

        let row = unnest_one(&owner_level_args(), &node);
        assert_eq!(row["topics"], json!([]));
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        let row = unnest_one(&owner_level_args(), &repository_node());
        let schema = gql_schema();

        for key in row.as_object().expect("row object").keys() {
            assert!(
                schema.field_with_name(key).is_ok(),
                "repos emits '{key}' but the schema does not declare it"
            );
        }
    }
}
