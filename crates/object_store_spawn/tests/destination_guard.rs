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

//! Redirects through a [`SpawnedReqwestConnector`] with and without a [`DestinationGuard`],
//! between two local HTTP servers: an origin that answers every request with a redirect and a
//! target that serves the object and counts its requests.

#![expect(clippy::expect_used, reason = "integration-test helpers")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    Router,
    http::{StatusCode, header::LOCATION},
    response::IntoResponse,
};
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, http::HttpBuilder, path::Path};
use object_store_spawn::{DestinationGuard, SpawnedReqwestConnector};
use tokio::{net::TcpListener, runtime::Handle};

const BODY: &str = "id\n1\n";

/// Serves `router` on a loopback port and returns the port.
async fn serve(router: Router) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local address").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    port
}

/// A target that serves [`BODY`] and counts the requests it receives.
async fn target() -> (u16, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let port = serve(Router::new().fallback(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        async { BODY }
    }))
    .await;
    (port, hits)
}

/// An origin that redirects every request to `location` and counts the requests it receives.
async fn redirecting_origin(location: String) -> (u16, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let port = serve(Router::new().fallback(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        let location = location.clone();
        async move { (StatusCode::FOUND, [(LOCATION, location)]).into_response() }
    }))
    .await;
    (port, hits)
}

fn store(origin_port: u16, connector: SpawnedReqwestConnector) -> Arc<dyn ObjectStore> {
    store_on("127.0.0.1", origin_port, connector)
}

fn store_on(host: &str, port: u16, connector: SpawnedReqwestConnector) -> Arc<dyn ObjectStore> {
    Arc::new(
        HttpBuilder::new()
            .with_url(format!("http://{host}:{port}/"))
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_http_connector(connector)
            .build()
            .expect("the HTTP store builds"),
    )
}

async fn get(store: &Arc<dyn ObjectStore>) -> object_store::Result<String> {
    let bytes = store.get(&Path::from("data.csv")).await?.bytes().await?;
    Ok(String::from_utf8(bytes.to_vec()).expect("the body is UTF-8"))
}

/// A guard that blocks redirects to `blocked_port` and no address.
fn port_guard(blocked_port: u16) -> DestinationGuard {
    DestinationGuard::new(
        |_| false,
        move |url| url.port() == Some(blocked_port),
        "the port is blocked",
    )
}

#[tokio::test]
async fn unguarded_connector_follows_a_redirect() {
    let (target_port, target_hits) = target().await;
    let (origin_port, origin_hits) =
        redirecting_origin(format!("http://127.0.0.1:{target_port}/data.csv")).await;

    let store = store(origin_port, SpawnedReqwestConnector::new(Handle::current()));
    let body = get(&store).await.expect("the redirect is followed");

    assert_eq!(body, BODY);
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
    assert_eq!(target_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn guard_refuses_a_redirect_to_a_blocked_url() {
    let (target_port, target_hits) = target().await;
    let location = format!("http://127.0.0.1:{target_port}/data.csv");
    let (origin_port, origin_hits) = redirecting_origin(location.clone()).await;

    let connector = SpawnedReqwestConnector::new(Handle::current())
        .with_destination_guard(port_guard(target_port));
    let error = get(&store(origin_port, connector))
        .await
        .expect_err("the redirect is refused");

    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "Refused to connect to {location}: the port is blocked"
        )),
        "unexpected error: {message}"
    );
    assert_eq!(
        target_hits.load(Ordering::SeqCst),
        0,
        "the target is never reached"
    );
    assert_eq!(
        origin_hits.load(Ordering::SeqCst),
        1,
        "a refusal is not retried"
    );
}

#[tokio::test]
async fn guard_follows_a_redirect_to_an_allowed_url() {
    let (target_port, target_hits) = target().await;
    let (origin_port, origin_hits) =
        redirecting_origin(format!("http://127.0.0.1:{target_port}/data.csv")).await;

    let connector = SpawnedReqwestConnector::new(Handle::current())
        .with_destination_guard(port_guard(target_port.wrapping_add(1)));
    let body = get(&store(origin_port, connector))
        .await
        .expect("an allowed redirect is followed");

    assert_eq!(body, BODY);
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
    assert_eq!(target_hits.load(Ordering::SeqCst), 1);
}

/// The redirect URL names a host rather than an address, so only the resolver can tell that
/// it reaches a blocked address.
#[tokio::test]
async fn guard_refuses_a_host_that_resolves_to_a_blocked_address() {
    let (target_port, target_hits) = target().await;
    let (origin_port, origin_hits) =
        redirecting_origin(format!("http://localhost:{target_port}/data.csv")).await;

    let guard = DestinationGuard::new(
        |address| address.is_loopback(),
        |_| false,
        "loopback is blocked",
    );
    let connector = SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(guard);
    let error = get(&store(origin_port, connector))
        .await
        .expect_err("the resolved address is refused");

    let message = error.to_string();
    assert!(
        message.contains("Refused to connect to localhost: loopback is blocked"),
        "unexpected error: {message}"
    );
    assert_eq!(
        target_hits.load(Ordering::SeqCst),
        0,
        "the target is never reached"
    );
    assert_eq!(
        origin_hits.load(Ordering::SeqCst),
        1,
        "a refusal is not retried"
    );
}

/// A guard that blocks every redirect and every loopback address, checked only for hosts
/// other than the request's own.
fn loopback_redirects_only_guard() -> DestinationGuard {
    DestinationGuard::new(
        |address| address.is_loopback(),
        |_| true,
        "loopback is blocked",
    )
    .redirects_only()
}

/// An origin on `localhost` that serves [`BODY`] at `/moved.csv` and redirects every other
/// path there, so a read stays on the store's own host.
async fn same_host_redirecting_origin() -> u16 {
    serve(
        Router::new()
            .route("/moved.csv", axum::routing::get(|| async { BODY }))
            .fallback(|| async { (StatusCode::FOUND, [(LOCATION, "/moved.csv")]).into_response() }),
    )
    .await
}

#[tokio::test]
async fn redirects_only_guard_allows_the_requests_own_host() {
    let port = same_host_redirecting_origin().await;

    let connector = SpawnedReqwestConnector::new(Handle::current())
        .with_destination_guard(loopback_redirects_only_guard());
    let body = get(&store_on("localhost", port, connector))
        .await
        .expect("the own host and a redirect within it are allowed");
    assert_eq!(body, BODY);

    let connector = SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(
        DestinationGuard::new(
            |address| address.is_loopback(),
            |_| false,
            "loopback is blocked",
        ),
    );
    let message = get(&store_on("localhost", port, connector))
        .await
        .expect_err("a guard for every host refuses the own host")
        .to_string();
    assert!(
        message.contains("Refused to connect to localhost: loopback is blocked"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn redirects_only_guard_refuses_a_redirect_to_another_host() {
    let (target_port, target_hits) = target().await;
    let location = format!("http://127.0.0.1:{target_port}/data.csv");
    let (origin_port, origin_hits) = redirecting_origin(location.clone()).await;

    let connector = SpawnedReqwestConnector::new(Handle::current())
        .with_destination_guard(loopback_redirects_only_guard());
    let message = get(&store_on("localhost", origin_port, connector))
        .await
        .expect_err("the redirect is refused")
        .to_string();

    assert!(
        message.contains(&format!(
            "Refused to connect to {location}: loopback is blocked"
        )),
        "unexpected error: {message}"
    );
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
}

/// The redirect names another host that resolves to a blocked address, which only the
/// resolver can see.
#[tokio::test]
async fn redirects_only_guard_refuses_another_host_that_resolves_to_a_blocked_address() {
    let (target_port, target_hits) = target().await;
    let (origin_port, origin_hits) =
        redirecting_origin(format!("http://localhost:{target_port}/data.csv")).await;

    let guard = DestinationGuard::new(
        |address| address.is_loopback(),
        |_| false,
        "loopback is blocked",
    )
    .redirects_only();
    let connector = SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(guard);
    let message = get(&store(origin_port, connector))
        .await
        .expect_err("the resolved address is refused")
        .to_string();

    assert!(
        message.contains("Refused to connect to localhost: loopback is blocked"),
        "unexpected error: {message}"
    );
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
}

/// The guarded client does not apply proxy options, so it refuses to build rather than ignore
/// one.
#[tokio::test]
async fn guard_rejects_an_option_it_does_not_apply() {
    let connector =
        SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(port_guard(0));
    let error = HttpBuilder::new()
        .with_url("http://127.0.0.1:1/")
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_proxy_url("http://proxy.example:8080"),
        )
        .with_http_connector(connector)
        .build()
        .expect_err("the client is not built");

    let message = error.to_string();
    assert!(
        message.contains(
            "the guarded HTTP client does not apply `proxy_url`; it only applies `allow_http`, \
             `timeout`, `connect_timeout`, `http1_only`, `user_agent`, and default headers"
        ),
        "unexpected error: {message}"
    );
}
