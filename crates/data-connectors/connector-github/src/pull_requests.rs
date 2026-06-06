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

use runtime::dataconnector::ConnectorComponent;
use runtime_datafusion::error::find_datafusion_root;

use crate::{
    GitHubQueryMode, GitHubTableArgs, GitHubTableGraphQLParams, filter_pushdown, inject_parameters,
    search_inject_parameters,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::{
    github::error_checker,
    graphql::{
        ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
        client::{DuplicateBehavior, GraphQLQuery, UnnestBehavior, unnest_json_object_to_depth},
    },
};
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use serde_json::Value;
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#repository
#[derive(Debug)]
pub struct PullRequestTableArgs {
    pub owner: String,
    pub repo: String,
    pub query_mode: GitHubQueryMode,
    pub component: ConnectorComponent,
    pub include_comments: PullRequestCommentType,
    pub max_comments_fetched: u32,
}

#[derive(Debug)]
pub enum PullRequestCommentType {
    All,
    Review,
    Discussion,
    None,
}

impl TryFrom<&str> for PullRequestCommentType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "all" => Ok(PullRequestCommentType::All),
            "review" => Ok(PullRequestCommentType::Review),
            "discussion" => Ok(PullRequestCommentType::Discussion),
            "none" => Ok(PullRequestCommentType::None),
            _ => Err(format!(
                "Invalid comment type: {value}. Supported values are 'all', 'review', 'discussion', 'none'.",
            )),
        }
    }
}

impl GraphQLContext for PullRequestTableArgs {
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        if self.query_mode == GitHubQueryMode::Auto {
            return Ok(FilterPushdownResult {
                filter_pushdown: TableProviderFilterPushDown::Unsupported,
                expr: expr.clone(),
                context: None,
            });
        }

        Ok(filter_pushdown(expr))
    }

    fn inject_parameters(
        &self,
        filters: &[FilterPushdownResult],
        query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        if self.query_mode == GitHubQueryMode::Auto {
            return Ok(());
        }

        inject_parameters("search", search_inject_parameters, filters, query)
            .map_err(find_datafusion_root)
    }

    fn error_checker(&self) -> Option<ErrorChecker> {
        Some(Arc::new(error_checker))
    }

    fn query_cost(&self) -> Option<u32> {
        // first 100 pull requests could retrieve up to 100 PRs
        // each query returns labels, commits and assignees which are each additional requests
        // if review threads are enabled, 1 PR retrieves 20 review threads, which could each have comments that are also retrieved
        // if discussion comments are enabled, each PR also retrieves discussion comments
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        match self.include_comments {
            PullRequestCommentType::None => Some(226), // 1 + 100 (labels) + 25 (commits) + 100 (assignees) = 226
            PullRequestCommentType::Review => Some(226 + (20 * self.max_comments_fetched)), // 226 + (20 review threads * comments_to_fetch)
            PullRequestCommentType::Discussion => Some(226 + self.max_comments_fetched), // 226 + comments_to_fetch (discussion comments)
            PullRequestCommentType::All => {
                Some(226 + (20 * self.max_comments_fetched) + self.max_comments_fetched)
            }
        }
    }
}

impl PullRequestTableArgs {
    fn base_requested_nodes() -> &'static str {
        r"
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
            reviews { reviews_count: totalCount }
            author: author { author: login }
            additions
            deletions
            changed_files: changedFiles
            labels(first: 100) { labels: nodes { name } }
            commits(first: 25) { commits_count: totalCount, hashes: nodes { id } }
            assignees(first: 100) { assignees: nodes { login } }
            comments_count_wrapper: comments { comments_count: totalCount }
        "
    }

    fn review_thread_nodes(&self) -> String {
        format!(
            r"
            reviewThreads(first: 20) {{
                thread_comments: nodes {{
                    comments(first: {comments_to_fetch}) {{
                        review_comments: nodes {{
                            body
                            created_at: createdAt
                            author {{
                                author: login
                            }}
                        }}
                    }}
                }}
            }}
        ",
            comments_to_fetch = self.max_comments_fetched
        )
    }

    fn discussion_nodes(&self) -> String {
        format!(
            r"
            comments_info: comments(first: {comments_to_fetch}) {{
                discussion: nodes {{
                    body
                    created_at: createdAt
                    author {{
                        author: login
                    }}
                }}
            }}
        ",
            comments_to_fetch = self.max_comments_fetched
        )
    }

    fn get_requested_nodes(&self) -> String {
        match self.include_comments {
            PullRequestCommentType::All => format!(
                "{}\n{}\n{}",
                Self::base_requested_nodes(),
                self.review_thread_nodes(),
                self.discussion_nodes()
            ),
            PullRequestCommentType::Review => format!(
                "{}\n{}",
                Self::base_requested_nodes(),
                self.review_thread_nodes()
            ),
            PullRequestCommentType::Discussion => format!(
                "{}\n{}",
                Self::base_requested_nodes(),
                self.discussion_nodes()
            ),
            PullRequestCommentType::None => Self::base_requested_nodes().to_string(),
        }
    }
}

impl PullRequestTableArgs {
    /// GitHub's hard limit on the number of nodes a single GraphQL query may
    /// request. Queries that exceed this limit are rejected with
    /// `MAX_NODE_LIMIT_EXCEEDED`.
    ///
    /// See: <https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#node-limit>
    const GITHUB_NODE_LIMIT: u32 = 500_000;

    /// Default outer `first:` value for the pull request connection when
    /// `include_comments` is not enabled.
    const DEFAULT_PAGE_SIZE: u32 = 100;

    /// Reduced outer `first:` value when `include_comments` is enabled.
    ///
    /// With comments enabled, each PR can expand to up to `20 * max_comments_fetched`
    /// review-thread comments plus `max_comments_fetched` discussion comments.
    /// Keeping the outer page at 25 keeps the total node count safely under
    /// GitHub's 500,000 node hard limit and within secondary rate limits.
    ///
    /// See: <https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#node-limit>
    const COMMENTS_PAGE_SIZE: u32 = 25;

    /// Upper bound on the number of review threads fetched per PR.
    const REVIEW_THREADS_PER_PR: u32 = 20;

    /// Conservative upper bound on the number of nodes contributed by a
    /// single PR's non-comment fields. Kept as a constant so
    /// `estimated_node_count` and the base query cannot drift out of sync.
    ///
    /// Includes:
    /// - 1 root PR node
    /// - 100 `labels` nodes
    /// - 25 `commits` nodes (hashes)
    /// - 100 `assignees` nodes
    /// - 3 wrapper/object nodes the GraphQL node counter charges for:
    ///   `reviews`, `comments_count_wrapper`, `author`
    ///
    /// When review threads are enabled this count does NOT include the
    /// `reviewThreads` nodes themselves — those are added in
    /// `estimated_node_count` along with their nested comments.
    const BASE_INNER_NODE_COUNT: u32 = 1 /* root PR */
        + 100 /* labels */
        + 25 /* commits */
        + 100 /* assignees */
        + 3 /* reviews + comments_count_wrapper + author wrapper objects */;

    /// Returns the outer `first:` page size for the pull request connection.
    ///
    /// When comments are included (review, discussion, or both), the page size
    /// is reduced to keep total node count well under GitHub's 500,000 node
    /// hard limit on a single GraphQL query.
    fn outer_page_size(&self) -> u32 {
        match self.include_comments {
            PullRequestCommentType::None => Self::DEFAULT_PAGE_SIZE,
            PullRequestCommentType::Review
            | PullRequestCommentType::Discussion
            | PullRequestCommentType::All => Self::COMMENTS_PAGE_SIZE,
        }
    }

    /// Conservative upper bound on the number of nodes a single page of this
    /// query will request from GitHub. Used to validate that a caller-supplied
    /// configuration (most notably a high `max_comments_fetched`) cannot push
    /// the query over GitHub's 500,000 node hard limit.
    ///
    /// The estimate intentionally over-counts rather than under-counts:
    /// when review threads are enabled it charges for `REVIEW_THREADS_PER_PR`
    /// thread nodes *plus* `REVIEW_THREADS_PER_PR × max_comments_fetched`
    /// comment nodes, and always includes the full `BASE_INNER_NODE_COUNT`.
    ///
    /// Returns `outer_page_size × (base_inner + per_PR_comment_expansion)`.
    fn estimated_node_count(&self) -> u32 {
        let per_pr_comment_nodes: u32 = match self.include_comments {
            PullRequestCommentType::None => 0,
            PullRequestCommentType::Review => Self::REVIEW_THREADS_PER_PR.saturating_add(
                Self::REVIEW_THREADS_PER_PR.saturating_mul(self.max_comments_fetched),
            ),
            PullRequestCommentType::Discussion => self.max_comments_fetched,
            PullRequestCommentType::All => Self::REVIEW_THREADS_PER_PR
                .saturating_add(
                    Self::REVIEW_THREADS_PER_PR.saturating_mul(self.max_comments_fetched),
                )
                .saturating_add(self.max_comments_fetched),
        };

        self.outer_page_size()
            .saturating_mul(Self::BASE_INNER_NODE_COUNT.saturating_add(per_pr_comment_nodes))
    }

    /// Returns `Ok(())` if the query's estimated node count fits within
    /// GitHub's 500,000 node hard limit; otherwise returns an error describing
    /// how to reduce the request.
    pub(crate) fn check_node_limit(&self) -> std::result::Result<(), String> {
        let estimated = self.estimated_node_count();
        if estimated > Self::GITHUB_NODE_LIMIT {
            return Err(format!(
                "GitHub pull request query for {owner}/{repo} is estimated at {estimated} nodes, which exceeds GitHub's {limit} node hard limit. \
                 Reduce 'github_max_comments_fetched' (currently {max_comments}) or disable 'github_include_comments'. \
                 See: https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#node-limit",
                owner = self.owner,
                repo = self.repo,
                limit = Self::GITHUB_NODE_LIMIT,
                max_comments = self.max_comments_fetched,
            ));
        }
        Ok(())
    }
}

impl GitHubTableArgs for PullRequestTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let page_size = self.outer_page_size();
        let query = match self.query_mode {
            GitHubQueryMode::Search => {
                format!(
                    r#"{{
                search(query:"repo:{owner}/{name} type:pr", first:{page_size}, type:ISSUE) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on PullRequest {{
                            {nodes}
                        }}
                    }}
                }}
            }}"#,
                    owner = self.owner,
                    name = self.repo,
                    nodes = self.get_requested_nodes()
                )
            }
            GitHubQueryMode::Auto => {
                format!(
                    r#"
            {{
                repository(owner: "{owner}", name: "{name}") {{
                    pullRequests(first: {page_size}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            {nodes}
                        }}
                    }}
                }}
            }}
            "#,
                    owner = self.owner,
                    name = self.repo,
                    nodes = self.get_requested_nodes()
                )
            }
        };

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(custom_unnestter)),
            Some(gql_schema(&self.include_comments)),
        )
    }
}

fn flatten_author_field(comment: &mut Value) {
    if let Value::Object(comment_obj) = comment
        && let Some(Value::Object(author_obj)) = comment_obj.get("author")
        && let Some(Value::String(author_name)) = author_obj.get("author")
    {
        comment_obj.insert("author".to_string(), Value::String(author_name.clone()));
    }
}

fn custom_unnestter(object: &Value) -> Result<Vec<Value>> {
    // Unnest normally, then handle the `thread_comments` and `discussion` fields
    unnest_json_object_to_depth(object, 1, &DuplicateBehavior::Error).map(|mut values| {
        for value in &mut values {
            if let Value::Object(obj) = value {
                if let Some(thread_comments) = obj.remove("thread_comments") {
                    let review_comments = extract_review_comments(thread_comments);
                    obj.insert("review_comments".to_string(), Value::Array(review_comments));
                }

                if let Some(Value::Array(discussion_array)) = obj.get_mut("discussion") {
                    discussion_array.iter_mut().for_each(flatten_author_field);
                }
            }
        }

        values
    })
}

// Flattens the `thread_comments` field match the schema expected by the table
fn extract_review_comments(thread_comments: Value) -> Vec<Value> {
    match thread_comments {
        Value::Array(thread_array) => thread_array
            .into_iter()
            .filter_map(|thread| {
                if let Value::Object(thread_obj) = thread {
                    thread_obj
                        .get("comments")
                        .and_then(|comments| comments.as_object())
                        .and_then(|comments_obj| comments_obj.get("review_comments"))
                        .and_then(|reviews| reviews.as_array())
                        .cloned()
                } else {
                    None
                }
            })
            .flatten()
            .map(|mut review| {
                flatten_author_field(&mut review);
                review
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn add_fields_based_on_comment_type(
    field_vector: &mut Vec<Field>,
    comments_type: &PullRequestCommentType,
) {
    let comment_data_type = DataType::Struct(
        vec![
            Arc::new(Field::new("body", DataType::Utf8, true)),
            Arc::new(Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            )),
            Arc::new(Field::new("author", DataType::Utf8, true)),
        ]
        .into(),
    );

    match comments_type {
        PullRequestCommentType::All => {
            field_vector.push(Field::new(
                "discussion",
                DataType::List(Arc::new(Field::new(
                    "item",
                    comment_data_type.clone(),
                    true,
                ))),
                true,
            ));
            field_vector.push(Field::new(
                "review_comments",
                DataType::List(Arc::new(Field::new("item", comment_data_type, true))),
                true,
            ));
        }
        PullRequestCommentType::Review => {
            field_vector.push(Field::new(
                "review_comments",
                DataType::List(Arc::new(Field::new("item", comment_data_type, true))),
                true,
            ));
        }
        PullRequestCommentType::Discussion => {
            field_vector.push(Field::new(
                "discussion",
                DataType::List(Arc::new(Field::new("item", comment_data_type, true))),
                true,
            ));
        }
        PullRequestCommentType::None => {}
    }
}

fn gql_schema(comments_type: &PullRequestCommentType) -> SchemaRef {
    let mut field_vector = vec![
        Field::new("additions", DataType::Int64, true),
        Field::new(
            "assignees",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("login", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("author", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("changed_files", DataType::Int64, true),
        Field::new(
            "closed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("comments_count", DataType::Int64, true),
        Field::new("commits_count", DataType::Int64, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("deletions", DataType::Int64, true),
        Field::new(
            "hashes",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("id", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new(
            "merged_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("number", DataType::Int64, true),
        Field::new("reviews_count", DataType::Int64, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("url", DataType::Utf8, true),
    ];

    add_fields_based_on_comment_type(&mut field_vector, comments_type);

    Arc::new(Schema::new(field_vector))
}

#[cfg(test)]
mod tests {
    use crate::GitHubQueryMode;
    use crate::builder::RuntimeBuilder;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use runtime::dataconnector::ConnectorComponent;
    use runtime::dataconnector::{PullRequestCommentType, PullRequestTableArgs};
    use std::sync::{Arc, OnceLock};

    /// Building a `ConnectorComponent` requires a full runtime + app
    /// construction. Cache a single shared instance so the unit tests don't
    /// spin up a tokio runtime per invocation.
    fn shared_component() -> ConnectorComponent {
        static COMPONENT: OnceLock<ConnectorComponent> = OnceLock::new();
        COMPONENT
            .get_or_init(|| {
                let app = AppBuilder::new("test").build();
                let runtime = tokio::runtime::Runtime::new().expect("to create tokio runtime");
                let spice_runtime = runtime.block_on(async { RuntimeBuilder::new().build().await });
                let dataset = DatasetBuilder::try_new("github".to_string(), "test.pulls")
                    .expect("to create dataset builder")
                    .with_app(Arc::new(app))
                    .with_runtime(Arc::new(spice_runtime))
                    .build()
                    .expect("to create dataset");
                ConnectorComponent::from(&dataset)
            })
            .clone()
    }

    fn args(include: PullRequestCommentType, max_comments: u32) -> PullRequestTableArgs {
        PullRequestTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            query_mode: GitHubQueryMode::Auto,
            component: shared_component(),
            include_comments: include,
            max_comments_fetched: max_comments,
        }
    }

    #[test]
    fn outer_page_size_uses_default_when_no_comments() {
        let a = args(PullRequestCommentType::None, 25);
        assert_eq!(a.outer_page_size(), 100);
    }

    #[test]
    fn outer_page_size_shrinks_when_comments_enabled() {
        for include in [
            PullRequestCommentType::Review,
            PullRequestCommentType::Discussion,
            PullRequestCommentType::All,
        ] {
            let a = args(include, 25);
            assert_eq!(a.outer_page_size(), 25);
        }
    }

    #[test]
    fn node_limit_passes_for_sane_defaults() {
        let a = args(PullRequestCommentType::All, 25);
        let estimated = a.estimated_node_count();
        // outer=25, base_inner=229 (1+100+25+100+3), per-PR comments for All:
        // 20 thread nodes + 20×25 review comments + 25 discussion = 545
        // total = 25 × (229 + 545) = 25 × 774 = 19_350
        assert_eq!(estimated, 19_350);
        a.check_node_limit().expect("defaults must fit under 500K");
    }

    #[test]
    fn node_limit_passes_at_max_comments_cap() {
        // With the current MAX_COMMENTS_FETCHED=75 cap, worst-case still fits.
        let a = args(PullRequestCommentType::All, 75);
        a.check_node_limit()
            .expect("75 comments must fit under 500K");
    }

    #[test]
    fn node_limit_rejects_abusively_large_configurations() {
        // Construct a synthetically oversized config that bypasses the mod.rs
        // clamp to verify the guard itself rejects it.
        let mut a = args(PullRequestCommentType::All, 1_000);
        a.max_comments_fetched = 2_000;
        assert!(a.check_node_limit().is_err());
    }
}
