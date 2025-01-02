/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use app::AppBuilder;

use async_graphql::{EmptyMutation, EmptySubscription, SimpleObject};
use async_graphql::{Object, Schema};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{routing::post, Extension, Router};
use runtime::{status, Runtime};
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use tokio::net::TcpListener;

use crate::utils::test_request_context;
use crate::{get_test_datafusion, init_tracing, run_query_and_check_results, ValidateFn};

type ServiceSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

#[derive(SimpleObject, Clone, Debug)]
struct Post {
    id: String,
    title: String,
    content: String,
}

#[derive(SimpleObject, Clone, Debug)]
struct User {
    id: String,
    name: String,
    posts: Vec<Post>,
}

#[derive(SimpleObject, Clone, Debug)]
struct PageInfo {
    has_next_page: bool,
    end_cursor: String,
}

#[derive(SimpleObject, Clone)]
struct UsersPaginated {
    users: Vec<User>,
    page_info: PageInfo,
}

struct UserService {
    users: Vec<User>,
}

impl UserService {
    fn new() -> Self {
        let users = vec![
            User {
                id: "1".to_string(),
                name: "John Doe".to_string(),
                posts: vec![
                    Post {
                        id: "1".to_string(),
                        title: "Hello world".to_string(),
                        content: "Hello world".to_string(),
                    },
                    Post {
                        id: "2".to_string(),
                        title: "First post".to_string(),
                        content: "First post content".to_string(),
                    },
                ],
            },
            User {
                id: "2".to_string(),
                name: "Jane Doe".to_string(),
                posts: vec![
                    Post {
                        id: "3".to_string(),
                        title: "First post".to_string(),
                        content: "First post content".to_string(),
                    },
                    Post {
                        id: "4".to_string(),
                        title: "Second post".to_string(),
                        content: "Second post content".to_string(),
                    },
                ],
            },
            User {
                id: "3".to_string(),
                name: "Alice".to_string(),
                posts: vec![
                    Post {
                        id: "5".to_string(),
                        title: "First post".to_string(),
                        content: "First post content".to_string(),
                    },
                    Post {
                        id: "6".to_string(),
                        title: "Second post".to_string(),
                        content: "Second post content".to_string(),
                    },
                ],
            },
            User {
                id: "4".to_string(),
                name: "Bob".to_string(),
                posts: vec![
                    Post {
                        id: "7".to_string(),
                        title: "First post".to_string(),
                        content: "First post content".to_string(),
                    },
                    Post {
                        id: "8".to_string(),
                        title: "Second post".to_string(),
                        content: "Second post content".to_string(),
                    },
                ],
            },
        ];

        Self { users }
    }

    fn users(&self) -> Vec<User> {
        self.users.clone()
    }

    fn paginated_users(&self, first: usize, after: Option<String>) -> Vec<User> {
        match after {
            Some(after_id) => self
                .users()
                .into_iter()
                .skip_while(|user| user.id <= after_id)
                .take(first)
                .collect(),
            None => self.users().into_iter().take(first).collect(),
        }
    }
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn users(&self) -> Vec<User> {
        UserService::new().users()
    }

    async fn paginated_users(&self, first: usize, after: Option<String>) -> UsersPaginated {
        let users_service = UserService::new();
        let users = users_service.paginated_users(first, after);
        let last_id = unsafe { users_service.users().last().unwrap_unchecked().id.clone() };
        let last_fetched = unsafe { users.last().unwrap_unchecked().id.clone() };

        let page_info = PageInfo {
            has_next_page: last_fetched < last_id,
            end_cursor: last_fetched,
        };
        UsersPaginated { users, page_info }
    }
}

async fn graphql_handler(schema: Extension<ServiceSchema>, req: GraphQLRequest) -> GraphQLResponse {
    let response = schema.execute(req.into_inner()).await;

    response.into()
}

async fn start_server() -> Result<(tokio::sync::oneshot::Sender<()>, SocketAddr), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .layer(Extension(schema));

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        tracing::error!("Failed to bind to address: {e}");
        e.to_string()
    })?;
    let addr = tcp_listener.local_addr().map_err(|e| {
        tracing::error!("Failed to get local address: {e}");
        e.to_string()
    })?;

    tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });

    Ok((tx, addr))
}

fn make_graphql_dataset(
    path: &str,
    name: &str,
    query: &str,
    json_pointer: &str,
    unnest_depth: Option<u32>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("graphql:{path}"), name.to_string());
    let mut params = HashMap::from([
        ("json_pointer".to_string(), json_pointer.to_string()),
        ("graphql_query".to_string(), query.to_string()),
    ]);

    if let Some(unnest_depth) = unnest_depth {
        params.insert("unnest_depth".to_string(), unnest_depth.to_string());
    };

    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[tokio::test]
async fn test_graphql() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (tx, addr) = start_server().await?;
            tracing::debug!("Server started at {}", addr);
            let app = AppBuilder::new("graphql_integration_test")
                .with_dataset(make_graphql_dataset(
                    &format!("http://{addr}/graphql"),
                    "test_graphql",
                    "query { users { id name posts { id title content } } }",
                    "/data/users",
                    None,
                ))
                .build();
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));
            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let queries: QueryTests = vec![
                (
                    "SELECT * FROM test_graphql",
                    "select_all",
                    Some(Box::new(|result_batches| {
                        for batch in result_batches {
                            assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                            assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                        }
                    })),
                ),
                (
                    "SELECT posts[1]['title'] from test_graphql",
                    "select_posts_title",
                    Some(Box::new(|result_batches| {
                        for batch in result_batches {
                            assert_eq!(batch.num_columns(), 1, "num_cols: {}", batch.num_columns());
                            assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                        }
                    })),
                ),
            ];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("test_graphql_{snapshot_suffix}"),
                    query,
                    true,
                    validate_result,
                )
                .await?;
            }

            tx.send(()).map_err(|()| {
                tracing::error!("Failed to send shutdown signal");
                "Failed to send shutdown signal".to_string()
            })?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_graphql_pagination() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context().scope(async {
        let (tx, addr) = start_server().await?;
        tracing::debug!("Server started at {}", addr);
        let app = AppBuilder::new("graphql_integration_test")
            .with_dataset(make_graphql_dataset(
                &format!("http://{addr}/graphql"),
                "test_graphql",
                "query { paginatedUsers(first: 2) { users { id name posts { id title content } } pageInfo { hasNextPage endCursor } } }",
                "/data/paginatedUsers/users",
                None
            ))
            .build();
        let status = status::RuntimeStatus::new();
        let df = get_test_datafusion(Arc::clone(&status));
        let mut rt = Runtime::builder()
            .with_app(app)
            .with_datafusion(df)
            .with_runtime_status(status)
            .build()
            .await;

        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err("Timed out waiting for datasets to load".to_string());
            }
            () = rt.load_components() => {}
        }

        let queries: QueryTests = vec![
            (
                "SELECT * FROM test_graphql",
                "select_all",
                Some(Box::new(|result_batches| {
                    let mut total = 0;
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                        total += batch.num_rows();
                    }
                    assert_eq!(total, 4);
                })),
            ),
            (
                "SELECT * FROM test_graphql where id = '4' limit 1",
                "select_limit_1_id_4",
                Some(Box::new(|result_batches| {
                    let mut total = 0;
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
                        total += batch.num_rows();
                    }
                    assert_eq!(total, 1);
                })),
            ),
        ];

        for (query, snapshot_suffix, validate_result) in queries {
            run_query_and_check_results(
                &mut rt,
                &format!("test_graphql_pagination_{snapshot_suffix}"),
                query,
                true,
                validate_result,
            )
            .await?;
        }

        tx.send(()).map_err(|()| {
            tracing::error!("Failed to send shutdown signal");
            "Failed to send shutdown signal".to_string()
        })?;

        Ok(())
    })
    .await
}
