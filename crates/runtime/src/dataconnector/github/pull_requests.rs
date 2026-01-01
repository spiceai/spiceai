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

use crate::{dataconnector::ConnectorComponent, datafusion::error::find_datafusion_root};

use super::{
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
            PullRequestCommentType::None => Some(301),
            PullRequestCommentType::Review => Some(301 + (20 * self.max_comments_fetched)), // 1 + 100 (labels) + 100 (commits) + 100 (assignees) + (20 review threads * comments_to_fetch) = n
            PullRequestCommentType::Discussion => Some(301 + self.max_comments_fetched), // 1 + 100 (labels) + 100 (commits) + 100 (assignees) + comments_to_fetch (discussion comments) = n
            PullRequestCommentType::All => {
                Some(301 + (20 * self.max_comments_fetched) + self.max_comments_fetched)
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
            number
            reviews { reviews_count: totalCount }
            author: author { author: login }
            additions
            deletions
            changed_files: changedFiles
            labels(first: 100) { labels: nodes { name } }
            commits(first: 100) { commits_count: totalCount, hashes: nodes { id } }
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

impl GitHubTableArgs for PullRequestTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = match self.query_mode {
            GitHubQueryMode::Search => {
                format!(
                    r#"{{
                search(query:"repo:{owner}/{name} type:pr", first:100, type:ISSUE) {{
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
                    pullRequests(first: 100) {{
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
