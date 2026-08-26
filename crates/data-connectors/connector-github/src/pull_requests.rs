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

use data_connector_api::ConnectorComponent;
use runtime_datafusion::error::find_datafusion_root;

use super::{
    GitHubQueryMode, GitHubTableArgs, GitHubTableGraphQLParams, filter_pushdown, inject_parameters,
    search_inject_parameters,
};
use crate::github::error_checker;
use crate::identity::{insert_identity, push_identity_fields};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use connector_graphql::graphql::{
    ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
    client::{DuplicateBehavior, GraphQLQuery, UnnestBehavior, unnest_json_object_to_depth},
};
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use serde_json::{Map, Value};
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
        // Each connection in the query charges its page size, and 1 for the pull
        // request connection itself. If review threads are enabled, 1 PR retrieves 20
        // review threads, which could each have comments that are also retrieved. If
        // discussion comments are enabled, each PR also retrieves discussion comments.
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        let base = Self::BASE_QUERY_COST;
        match self.include_comments {
            PullRequestCommentType::None => Some(base),
            PullRequestCommentType::Review => {
                Some(base + (Self::REVIEW_THREADS_PER_PR * self.max_comments_fetched))
            } // base + (20 review threads * comments_to_fetch)
            PullRequestCommentType::Discussion => Some(base + self.max_comments_fetched), // base + comments_to_fetch (discussion comments)
            PullRequestCommentType::All => Some(
                base + (Self::REVIEW_THREADS_PER_PR * self.max_comments_fetched)
                    + self.max_comments_fetched,
            ),
        }
    }
}

impl PullRequestTableArgs {
    fn base_requested_nodes() -> String {
        format!(
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
            is_draft: isDraft
            mergeable
            merge_state_status: mergeStateStatus
            review_decision: reviewDecision
            base_ref: baseRefName
            head_ref: headRefName
            head_sha: headRefOid
            additions
            deletions
            changed_files: changedFiles
            reviews {{ reviews_count: totalCount }}
            author: author {{ author: login }}
            merged_by: mergedBy {{ merged_by: login }}
            milestone_id: milestone {{ milestone_id: id }}
            milestone_title: milestone {{ milestone_title: title }}
            merge_queue_state: mergeQueueEntry {{ merge_queue_state: state }}
            merge_queue_position: mergeQueueEntry {{ merge_queue_position: position }}
            reactions_wrapper: reactions {{ reactions_count: totalCount }}
            labels(first: 100) {{ labels: nodes {{ name }} }}
            commits(first: 25) {{ commits_count: totalCount, hashes: nodes {{ id }} }}
            assignees(first: 100) {{ assignees: nodes {{ login }} }}
            comments_count_wrapper: comments {{ comments_count: totalCount }}
            {CLOSING_ISSUES_WRAPPER_KEY}: closingIssuesReferences(first: {closing_issues}) {{
                closing_issues_count: totalCount
                closing_issues_references: nodes {{ number }}
            }}
            {rollup_source}: commits(last: 1) {{
                nodes {{ commit {{ statusCheckRollup {{ state }} }} }}
            }}
            {closed_by_source}: timelineItems(last: 1, itemTypes: [CLOSED_EVENT]) {{
                nodes {{ ... on ClosedEvent {{ actor {{ login }} }} }}
            }}
        ",
            closing_issues = Self::CLOSING_ISSUES_PER_PR,
            rollup_source = STATUS_CHECK_ROLLUP_SOURCE_KEY,
            closed_by_source = CLOSED_BY_SOURCE_KEY,
        )
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
            PullRequestCommentType::None => Self::base_requested_nodes(),
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

    /// Upper bound on the number of issues a single PR reports as closing.
    /// `closing_issues_count` records GitHub's own total, so a PR that closes
    /// more issues than this is detectable rather than silently short.
    const CLOSING_ISSUES_PER_PR: u32 = 20;

    /// Point cost of one page of the query with no comments requested: 1 for
    /// the pull request connection, plus the page size of every connection
    /// underneath it.
    const BASE_QUERY_COST: u32 = 1 /* pullRequests */
        + 100 /* labels */
        + 25 /* commits */
        + 100 /* assignees */
        + Self::CLOSING_ISSUES_PER_PR
        + 1 /* commits(last: 1) for the check rollup */
        + 1 /* timelineItems(last: 1) for closed_by */;

    /// Conservative upper bound on the number of nodes contributed by a
    /// single PR's non-comment fields. Kept as a constant so
    /// `estimated_node_count` and the base query cannot drift out of sync.
    ///
    /// When review threads are enabled this count does NOT include the
    /// `reviewThreads` nodes themselves — those are added in
    /// `estimated_node_count` along with their nested comments.
    const BASE_INNER_NODE_COUNT: u32 = 1 /* root PR */
        + 100 /* labels */
        + 25 /* commits */
        + 100 /* assignees */
        + 3 /* reviews + comments_count_wrapper + author wrapper objects */
        + 1 /* mergedBy */
        + 2 /* milestone, aliased twice */
        + 2 /* mergeQueueEntry, aliased twice */
        + 1 /* reactions */
        + 1 + Self::CLOSING_ISSUES_PER_PR /* closingIssuesReferences + its nodes */
        + 4 /* commits(last: 1) -> node -> commit -> statusCheckRollup */
        + 3 /* timelineItems(last: 1) -> node -> actor */;

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
            // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
            // predicate on the sort key, so ordering by a mutable field (e.g. UPDATED_AT)
            // lets a pull request touched on the source mid-scan jump ahead of the cursor,
            // where no remaining page will return it — silently dropping the row from the
            // scan. CREATED_AT ASC is GitHub's own default order for this connection.
            GitHubQueryMode::Auto => {
                format!(
                    r#"
            {{
                repository(owner: "{owner}", name: "{name}") {{
                    pullRequests(first: {page_size}, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
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

        let owner = self.owner.clone();
        let repo = self.repo.clone();

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Custom(Box::new(move |object: &Value| {
                custom_unnestter(object, &owner, &repo)
            })),
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

/// Response key holding the single-commit connection the check rollup is read
/// from. The connection is removed before unnesting, so it never reaches a row.
const STATUS_CHECK_ROLLUP_SOURCE_KEY: &str = "status_check_rollup_source";

/// Response key holding the timeline lookup the closing actor is read from.
/// The connection is removed before unnesting, so it never reaches a row.
const CLOSED_BY_SOURCE_KEY: &str = "closed_by_source";

/// Response key wrapping the linked-issue connection. The unnest hoists its two
/// members — `closing_issues_count` and `closing_issues_references` — to the row.
const CLOSING_ISSUES_WRAPPER_KEY: &str = "closing_issues_wrapper";

fn custom_unnestter(object: &Value, owner: &str, repo: &str) -> Result<Vec<Value>> {
    // Flatten the fields nested deeper than the unnest reaches first: both hang
    // their payload under a `nodes` key, which would collide once hoisted.
    let mut object = object.clone();
    if let Value::Object(pull_request) = &mut object {
        flatten_deeply_nested_fields(pull_request);
    }

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

                insert_identity(obj, owner, Some(repo));
            }
        }

        values
    })
}

/// Flattens the two pull request fields GitHub nests deeper than the unnest
/// reaches: the head commit's check rollup, and the actor who closed the pull
/// request.
fn flatten_deeply_nested_fields(pull_request: &mut Map<String, Value>) {
    null_truncated_closing_issues(pull_request);

    let rollup = pull_request
        .remove(STATUS_CHECK_ROLLUP_SOURCE_KEY)
        .and_then(|commits| {
            commits
                .pointer("/nodes/0/commit/statusCheckRollup/state")
                .cloned()
        })
        .unwrap_or(Value::Null);
    pull_request.insert("status_check_rollup".to_string(), rollup);

    let closing_actor = pull_request
        .remove(CLOSED_BY_SOURCE_KEY)
        .and_then(|timeline| timeline.pointer("/nodes/0/actor/login").cloned())
        .unwrap_or(Value::Null);

    // GitHub keeps a `ClosedEvent` on the timeline of a pull request that was
    // closed and then reopened, so the actor only describes the pull request's
    // current state while it is actually closed.
    let is_closed = pull_request
        .get("closed_at")
        .is_some_and(|closed_at| !closed_at.is_null());
    let closed_by = if is_closed {
        closing_actor
    } else {
        Value::Null
    };

    pull_request.insert("closed_by".to_string(), closed_by);
}

/// Nulls `closing_issues_references` when GitHub reported more linked issues
/// than the one page the query can ask for.
///
/// A short list is indistinguishable from a complete one, so a query that
/// unnests it would quietly miss valid linked issues. A null says the set is
/// unknown; `closing_issues_count` still records how many there really are.
fn null_truncated_closing_issues(pull_request: &mut Map<String, Value>) {
    let Some(wrapper) = pull_request
        .get_mut(CLOSING_ISSUES_WRAPPER_KEY)
        .and_then(Value::as_object_mut)
    else {
        return;
    };

    let returned = wrapper
        .get("closing_issues_references")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let total = wrapper
        .get("closing_issues_count")
        .and_then(Value::as_i64)
        .unwrap_or_default();

    if total > i64::try_from(returned).unwrap_or(i64::MAX) {
        wrapper.insert("closing_issues_references".to_string(), Value::Null);
    }
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
        Field::new("base_ref", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("changed_files", DataType::Int64, true),
        Field::new(
            "closed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("closed_by", DataType::Utf8, true),
        Field::new("closing_issues_count", DataType::Int64, true),
        Field::new(
            "closing_issues_references",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("number", DataType::Int64, true)].into()),
                true,
            ))),
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
        Field::new("head_ref", DataType::Utf8, true),
        Field::new("head_sha", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("is_draft", DataType::Boolean, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("merge_queue_position", DataType::Int64, true),
        Field::new("merge_queue_state", DataType::Utf8, true),
        Field::new("merge_state_status", DataType::Utf8, true),
        Field::new("mergeable", DataType::Utf8, true),
        Field::new(
            "merged_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("merged_by", DataType::Utf8, true),
        Field::new("milestone_id", DataType::Utf8, true),
        Field::new("milestone_title", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("reactions_count", DataType::Int64, true),
        Field::new("review_decision", DataType::Utf8, true),
        Field::new("reviews_count", DataType::Int64, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("status_check_rollup", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("url", DataType::Utf8, true),
    ];

    add_fields_based_on_comment_type(&mut field_vector, comments_type);
    push_identity_fields(&mut field_vector, true);

    Arc::new(Schema::new(field_vector))
}

#[cfg(test)]
mod tests {
    use super::{PullRequestCommentType, PullRequestTableArgs, gql_schema};
    use crate::test_util::shared_component;
    use crate::{GitHubQueryMode, GitHubTableArgs};
    use connector_graphql::graphql::GraphQLContext;
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Value, json};

    fn args(include: PullRequestCommentType, max_comments: u32) -> PullRequestTableArgs {
        PullRequestTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            query_mode: GitHubQueryMode::Auto,
            component: shared_component("test.pulls"),
            include_comments: include,
            max_comments_fetched: max_comments,
        }
    }

    /// A pull request node shaped the way GitHub returns it, before unnesting.
    fn pull_request_node(closed_at: &Value) -> Value {
        json!({
            "title": "fix(arrow): refuse a relabel",
            "number": 13435,
            "id": "PR_1",
            "url": "https://github.com/spiceai/spiceai/pull/13435",
            "body": "",
            "state": "MERGED",
            "created_at": "2026-08-24T18:00:00Z",
            "updated_at": "2026-08-25T18:00:00Z",
            "merged_at": "2026-08-25T00:00:00Z",
            "closed_at": closed_at,
            "is_draft": false,
            "mergeable": "MERGEABLE",
            "merge_state_status": "BEHIND",
            "review_decision": "REVIEW_REQUIRED",
            "base_ref": "trunk",
            "head_ref": "fix/13423",
            "head_sha": "e3e46c6",
            "additions": 10,
            "deletions": 2,
            "changed_files": 1,
            "reviews": {"reviews_count": 14},
            "author": {"author": "claudespice"},
            "merged_by": {"merged_by": "lukekim"},
            "milestone_id": {"milestone_id": "MI_1"},
            "milestone_title": {"milestone_title": "v2.3.0"},
            "merge_queue_state": null,
            "merge_queue_position": null,
            "reactions_wrapper": {"reactions_count": 3},
            "labels": {"labels": [{"name": "kind/bug"}]},
            "commits": {"commits_count": 2, "hashes": [{"id": "C_1"}]},
            "assignees": {"assignees": [{"login": "lukekim"}]},
            "comments_count_wrapper": {"comments_count": 4},
            "closing_issues_wrapper": {
                "closing_issues_count": 1,
                "closing_issues_references": [{"number": 13423}]
            },
            "status_check_rollup_source": {
                "nodes": [{"commit": {"statusCheckRollup": {"state": "SUCCESS"}}}]
            },
            "closed_by_source": {"nodes": [{"actor": {"login": "claudespice"}}]}
        })
    }

    fn unnest_one(args: &PullRequestTableArgs, node: &Value) -> Value {
        let params = args.get_graphql_values();
        let UnnestBehavior::Custom(unnest) = &params.unnest_behavior else {
            panic!("pulls must use a custom unnest");
        };
        let rows = unnest(node).expect("unnest to succeed");
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().unwrap_or(Value::Null)
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
        // outer=25, base_inner=263, per-PR comments for All:
        // 20 thread nodes + 20×25 review comments + 25 discussion = 545
        // total = 25 × (263 + 545) = 25 × 808 = 20_200
        assert_eq!(estimated, 20_200);
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

    #[test]
    fn auto_mode_query_orders_by_created_at_asc() {
        // Deterministic ordering on an immutable key: see
        // `auto_mode_query_never_paginates_on_a_mutable_sort_key`.
        let a = args(PullRequestCommentType::None, 25);
        let params = a.get_graphql_values();
        let query = params.query.as_ref();
        assert!(
            query.contains("orderBy: {field: CREATED_AT, direction: ASC}"),
            "auto-mode pull_requests query must order by CREATED_AT ASC, got:\n{query}"
        );
    }

    /// Regression test for #12067. A GitHub `after:` cursor is a value predicate on the
    /// sort key, so a connection paginated on a mutable field silently drops any row
    /// that is touched on the source mid-scan: the row's key moves ahead of the cursor
    /// and no remaining page returns it. Only immutable sort keys are safe here.
    #[test]
    fn auto_mode_query_never_paginates_on_a_mutable_sort_key() {
        let a = args(PullRequestCommentType::All, 25);
        let params = a.get_graphql_values();
        let query = params.query.as_ref();
        // The GraphQL enum is upper-case (`UPDATED_AT`), so this cannot collide with the
        // `updated_at: updatedAt` field alias the query also selects.
        assert!(
            !query.contains("UPDATED_AT"),
            "auto-mode pull_requests query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }

    #[test]
    fn unnest_flattens_the_live_review_and_merge_state_columns() {
        let row = unnest_one(
            &args(PullRequestCommentType::None, 25),
            &pull_request_node(&json!("2026-08-25T00:00:00Z")),
        );

        assert_eq!(row["is_draft"], json!(false));
        assert_eq!(row["mergeable"], json!("MERGEABLE"));
        assert_eq!(row["merge_state_status"], json!("BEHIND"));
        assert_eq!(row["review_decision"], json!("REVIEW_REQUIRED"));
        assert_eq!(row["status_check_rollup"], json!("SUCCESS"));
        assert_eq!(row["base_ref"], json!("trunk"));
        assert_eq!(row["head_ref"], json!("fix/13423"));
        assert_eq!(row["head_sha"], json!("e3e46c6"));
        assert_eq!(row["merged_by"], json!("lukekim"));
        assert_eq!(row["milestone_title"], json!("v2.3.0"));
        assert_eq!(row["reactions_count"], json!(3));
        assert_eq!(row["closing_issues_count"], json!(1));
        assert_eq!(row["closing_issues_references"], json!([{"number": 13423}]));
        assert_eq!(row["owner"], json!("spiceai"));
        assert_eq!(row["repo"], json!("spiceai"));
        // A pull request not in a merge queue keeps nulls, not zeroes.
        assert_eq!(row["merge_queue_state"], Value::Null);
        assert_eq!(row["merge_queue_position"], Value::Null);
    }

    #[test]
    fn a_truncated_closing_issues_list_is_nulled_rather_than_left_short() {
        // A short list is indistinguishable from a complete one, so a query that
        // unnests it would quietly miss valid linked issues.
        let mut node = pull_request_node(&json!("2026-08-25T00:00:00Z"));
        node["closing_issues_wrapper"] = json!({
            "closing_issues_count": 40,
            "closing_issues_references": [{"number": 1}, {"number": 2}]
        });

        let row = unnest_one(&args(PullRequestCommentType::None, 25), &node);

        assert_eq!(row["closing_issues_count"], json!(40));
        assert_eq!(row["closing_issues_references"], Value::Null);
    }

    #[test]
    fn closed_by_is_null_while_the_pull_request_is_open() {
        // GitHub keeps a `ClosedEvent` on the timeline of a pull request that was
        // closed and then reopened, so reading the timeline alone would attribute a
        // stale actor to an open pull request.
        let row = unnest_one(
            &args(PullRequestCommentType::None, 25),
            &pull_request_node(&Value::Null),
        );

        assert_eq!(row["closed_by"], Value::Null);
    }

    #[test]
    fn closed_by_names_the_actor_once_the_pull_request_is_closed() {
        let row = unnest_one(
            &args(PullRequestCommentType::None, 25),
            &pull_request_node(&json!("2026-08-25T00:00:00Z")),
        );

        assert_eq!(row["closed_by"], json!("claudespice"));
    }

    #[test]
    fn the_deeply_nested_source_keys_never_reach_a_row() {
        // Both source connections hang their payload under a `nodes` key. Left in
        // place they collide on the way up and fail the whole unnest.
        let row = unnest_one(
            &args(PullRequestCommentType::None, 25),
            &pull_request_node(&json!("2026-08-25T00:00:00Z")),
        );
        let row = row.as_object().expect("row object");

        assert!(row.get("status_check_rollup_source").is_none());
        assert!(row.get("closed_by_source").is_none());
        assert!(row.get("nodes").is_none());
    }

    #[test]
    fn every_emitted_key_is_declared_in_the_schema() {
        // A key the schema does not declare is silently dropped by the Arrow JSON
        // reader, so the two must be kept in step.
        for include in [
            PullRequestCommentType::None,
            PullRequestCommentType::Review,
            PullRequestCommentType::Discussion,
            PullRequestCommentType::All,
        ] {
            let schema = gql_schema(&include);
            let row = unnest_one(&args(include, 25), &pull_request_node(&Value::Null));

            for key in row.as_object().expect("row object").keys() {
                assert!(
                    schema.field_with_name(key).is_ok(),
                    "pulls emits '{key}' but the schema does not declare it"
                );
            }
        }
    }

    #[test]
    fn query_cost_stays_within_the_github_secondary_rate_limit_burst() {
        // The rate controller's weighted quota is 2000 points per minute; a cost
        // above the burst capacity fails the acquire outright instead of waiting.
        // The worst case is every comment type at the `MAX_COMMENTS_FETCHED` cap.
        let cost = args(PullRequestCommentType::All, crate::MAX_COMMENTS_FETCHED)
            .query_cost()
            .expect("pulls to declare a query cost");

        assert!(
            cost <= 2000,
            "pulls query cost {cost} exceeds the 2000-point burst, so every scan would fail"
        );
    }
}
