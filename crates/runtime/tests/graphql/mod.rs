/*
Copyright 2024 The Spice.ai OSS Authors

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
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use tokio::net::TcpListener;
use tracing::debug;
use runtime::Runtime;

use crate::init_tracing;

type ServiceSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

#[derive(SimpleObject)]
struct User {
    id: String,
    name: String,
    email: String,
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn users(&self) -> Vec<User> {
        vec![User { id: "1".to_string(), name: "name".to_string(), email: "email".to_string()}]
    }
}

async fn graphql_handler(schema: Extension<ServiceSchema>, req: GraphQLRequest) -> GraphQLResponse {
    let response = schema.execute(req.into_inner()).await;

    response.into()
}

async fn start_server() -> (tokio::sync::oneshot::Sender<()>, SocketAddr) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .layer(Extension(schema));
    debug!("Starting server");

    let tcp_listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    debug!("{}", addr);
    tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    (tx, addr)
}

fn make_graphql_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("graphql:{path}"), name.to_string());
    let params = HashMap::from([
        ("json_path".to_string(), "data.users".to_string()),
        ("query".to_string(), "query { users { id } }".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[tokio::test]
async fn test_graphql() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let (tx, addr) = start_server().await;
    tracing::debug!("Server started at {}", addr);
    let app = AppBuilder::new("graphql_integration_test").with_dataset(make_graphql_dataset(
        &format!("http://{}/graphql", addr.to_string()),
        "test_graphql",
    )).build();
    let rt = Runtime::new(Some(app), Arc::new(vec!{})).await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_datasets() => {}
    }

    let res = rt.df.ctx.sql("SELECT * FROM test_graphql").await.unwrap();
    tracing::debug!("{:?}", res);

    tx.send(()).unwrap();

    Ok(())
}
