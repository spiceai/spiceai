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
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use connector_graphql::graphql::{
    ErrorChecker, FilterPushdownResult, GraphQLContext, Result,
    client::{GraphQLQuery, UnnestBehavior},
};
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use std::sync::Arc;

// https://docs.github.com/en/graphql/reference/objects#repository
#[derive(Debug)]
pub struct IssuesTableArgs {
    pub owner: String,
    pub repo: String,
    pub query_mode: GitHubQueryMode,
    pub component: ConnectorComponent,
}

impl GraphQLContext for IssuesTableArgs {
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
        // issues(first: 100) could retrieve up to 100 issues
        // each query returns labels, comments and assignees which are each additional requests
        // 1 + 100 (labels) + 25 (comments) + 100 (assignees) = 226 points
        // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits
        Some(226)
    }
}

impl GitHubTableArgs for IssuesTableArgs {
    fn get_component(&self) -> ConnectorComponent {
        self.component.clone()
    }

    fn get_graphql_values(&self) -> GitHubTableGraphQLParams {
        let query = match self.query_mode {
            GitHubQueryMode::Search => format!(
                r#"{{
                search(query:"repo:{owner}/{name} type:issue", first:100, type:ISSUE) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on Issue {{
                            id
                            number
                            title
                            url
                            author: author {{ author: login }}
                            body
                            created_at: createdAt
                            updated_at: updatedAt
                            closed_at: closedAt
                            state
                            milestone_id: milestone {{ milestone_id: id}}
                            milestone_title: milestone {{ milestone_title: title }}
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            milestone_title: milestone {{ milestone_title: title }}
                            comments(first: 25) {{ comments_count: totalCount, comments: nodes {{ body, author {{ login }} }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                            type: issueType {{ type: name, type_color: color }}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo
            ),
            // `orderBy` must name an immutable field. A GitHub `after:` cursor is a value
            // predicate on the sort key, so ordering by a mutable field (e.g. UPDATED_AT)
            // lets an issue touched on the source mid-scan jump ahead of the cursor, where
            // no remaining page will return it — silently dropping the row from the scan.
            // CREATED_AT ASC is GitHub's own default order for this connection.
            GitHubQueryMode::Auto => format!(
                r#"{{
                repository(owner: "{owner}", name: "{name}") {{
                    issues(first: 100, orderBy: {{field: CREATED_AT, direction: ASC}}) {{
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                        nodes {{
                            id
                            number
                            title
                            url
                            author: author {{ author: login }}
                            body
                            created_at: createdAt
                            updated_at: updatedAt
                            closed_at: closedAt
                            state
                            milestone_id: milestone {{ milestone_id: id}}
                            milestone_title: milestone {{ milestone_title: title }}
                            labels(first: 100) {{ labels: nodes {{ name }} }}
                            milestone_title: milestone {{ milestone_title: title }}
                            comments(first: 25) {{ comments_count: totalCount, comments: nodes {{ body, author {{ login }} }} }}
                            assignees(first: 100) {{ assignees: nodes {{ login }} }}
                            type: issueType {{ type: name, type_color: color }}
                        }}
                    }}
                }}
            }}"#,
                owner = self.owner,
                name = self.repo
            ),
        };

        GitHubTableGraphQLParams::new(
            query.into(),
            None,
            UnnestBehavior::Depth(2),
            Some(gql_schema()),
        )
    }
}

fn gql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "closed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("state", DataType::Utf8, true),
        Field::new("milestone_id", DataType::Utf8, true),
        Field::new("milestone_title", DataType::Utf8, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new("comments_count", DataType::Int64, true),
        Field::new(
            "comments",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new(
                            "author",
                            DataType::Struct(
                                vec![Field::new("login", DataType::Utf8, true)].into(),
                            ),
                            true,
                        ),
                        Field::new("body", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
        Field::new(
            "assignees",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("login", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use runtime::builder::RuntimeBuilder;
    use runtime::component::dataset::builder::DatasetBuilder;
    use std::sync::OnceLock;

    /// Building a `ConnectorComponent` requires a full runtime + app
    /// construction. Cache a single shared instance so the unit tests don't
    /// spin up a tokio runtime per invocation.
    fn shared_component() -> ConnectorComponent {
        // The tokio runtime is cached alongside the component and never dropped:
        // `RuntimeBuilder::build` defaults `io_runtime` to `Handle::current()`, so
        // dropping the runtime that built it would leave the constructed `Runtime`
        // holding handles to a dead tokio runtime.
        static COMPONENT: OnceLock<(tokio::runtime::Runtime, ConnectorComponent)> = OnceLock::new();
        COMPONENT
            .get_or_init(|| {
                let app = AppBuilder::new("test").build();
                let runtime = tokio::runtime::Runtime::new().expect("to create tokio runtime");
                let spice_runtime = runtime.block_on(async { RuntimeBuilder::new().build().await });
                let dataset = DatasetBuilder::try_new("github".to_string(), "test.issues")
                    .expect("to create dataset builder")
                    .with_app(Arc::new(app))
                    .with_runtime(Arc::new(spice_runtime))
                    .build()
                    .expect("to create dataset");
                (runtime, ConnectorComponent::from(&dataset))
            })
            .1
            .clone()
    }

    fn auto_args() -> IssuesTableArgs {
        IssuesTableArgs {
            owner: "spiceai".to_string(),
            repo: "spiceai".to_string(),
            query_mode: GitHubQueryMode::Auto,
            component: shared_component(),
        }
    }

    #[test]
    fn auto_mode_query_orders_by_created_at_asc() {
        // Deterministic ordering on an immutable key: see
        // `auto_mode_query_never_paginates_on_a_mutable_sort_key`.
        let params = auto_args().get_graphql_values();
        let query = params.query.as_ref();
        assert!(
            query.contains("issues(first: 100, orderBy: {field: CREATED_AT, direction: ASC})"),
            "auto-mode issues query must order by CREATED_AT ASC, got:\n{query}"
        );
    }

    /// Regression test for #12067. A GitHub `after:` cursor is a value predicate on the
    /// sort key, so a connection paginated on a mutable field silently drops any row
    /// that is touched on the source mid-scan: the row's key moves ahead of the cursor
    /// and no remaining page returns it. Only immutable sort keys are safe here.
    #[test]
    fn auto_mode_query_never_paginates_on_a_mutable_sort_key() {
        let params = auto_args().get_graphql_values();
        let query = params.query.as_ref();
        // The GraphQL enum is upper-case (`UPDATED_AT`), so this cannot collide with the
        // `updated_at: updatedAt` field alias the query also selects.
        assert!(
            !query.contains("UPDATED_AT"),
            "auto-mode issues query must not order by the mutable UPDATED_AT, got:\n{query}"
        );
    }
}
