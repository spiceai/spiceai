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

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    http::{StatusCode, header::LOCATION},
    response::IntoResponse,
};
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, http::HttpBuilder, path::Path};
use object_store_spawn::{DestinationGuard, SpawnedReqwestConnector};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
};

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
    store_with(
        origin_port,
        connector,
        ClientOptions::new().with_allow_http(true),
    )
}

fn store_with(
    origin_port: u16,
    connector: SpawnedReqwestConnector,
    options: ClientOptions,
) -> Arc<dyn ObjectStore> {
    Arc::new(
        HttpBuilder::new()
            .with_url(format!("http://127.0.0.1:{origin_port}/"))
            .with_client_options(options)
            .with_http_connector(connector)
            .build()
            .expect("the HTTP store builds"),
    )
}

fn loopback_guard() -> DestinationGuard {
    DestinationGuard::new(
        |address| address.is_loopback(),
        |_| false,
        "loopback is blocked",
    )
}

fn direct_options(proxy_url: &str, excludes: &str) -> ClientOptions {
    ClientOptions::new()
        .with_allow_http(true)
        .with_proxy_url(proxy_url)
        .with_proxy_excludes(excludes)
        .with_connect_timeout(Duration::from_secs(2))
        .with_timeout(Duration::from_secs(5))
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

/// `NO_PROXY` makes this a direct connection. The proxy's hostname must not skip the address
/// check, or a redirect to that name on another port reaches the blocked address.
#[tokio::test]
async fn guard_checks_a_direct_connection_to_an_excluded_proxy_host() {
    let (target_port, target_hits) = target().await;
    let location = format!("http://localhost:{target_port}/data.csv");
    let (origin_port, origin_hits) = redirecting_origin(location).await;

    let options = direct_options("http://localhost:9", "localhost,127.0.0.1");
    let connector =
        SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(loopback_guard());
    let error = get(&store_with(origin_port, connector, options))
        .await
        .expect_err("a direct connection to the proxy hostname is refused");

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
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
}

/// `HTTPS_PROXY` applies to HTTPS destinations only. An HTTP redirect to that proxy's hostname
/// is a direct connection and is still checked.
#[tokio::test]
async fn guard_checks_a_direct_http_connection_when_only_an_https_proxy_is_set() {
    let _env = ProxyEnv::set("HTTPS_PROXY", "http://localhost:9");
    let (target_port, target_hits) = target().await;
    let location = format!("http://localhost:{target_port}/data.csv");
    let (origin_port, origin_hits) = redirecting_origin(location).await;

    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_connect_timeout(Duration::from_secs(2))
        .with_timeout(Duration::from_secs(5));
    let connector =
        SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(loopback_guard());
    let error = get(&store_with(origin_port, connector, options))
        .await
        .expect_err("an HTTP connection does not inherit an HTTPS proxy exemption");

    let message = error.to_string();
    assert!(
        message.contains("Refused to connect to localhost: loopback is blocked"),
        "unexpected error: {message}"
    );
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    assert_eq!(origin_hits.load(Ordering::SeqCst), 1);
}

/// An HTTP proxy configured by hostname still gets a connection when its address is blocked:
/// the proxy hop is pinned to that address, and the destination check is not how the proxy
/// itself is reached.
#[tokio::test]
async fn guard_reaches_a_loopback_proxy_configured_by_name() {
    let (proxy_port, proxy_hits) = forwarding_proxy().await;
    let (target_port, target_hits) = target().await;
    let (origin_port, origin_hits) =
        redirecting_origin(format!("http://127.0.0.1:{target_port}/data.csv")).await;

    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_proxy_url(format!("http://localhost:{proxy_port}"))
        .with_connect_timeout(Duration::from_secs(2))
        .with_timeout(Duration::from_secs(5));
    let connector =
        SpawnedReqwestConnector::new(Handle::current()).with_destination_guard(loopback_guard());
    let body = get(&store_with(origin_port, connector, options))
        .await
        .expect("the proxy hop is allowed");

    assert_eq!(body, BODY);
    assert!(
        proxy_hits.load(Ordering::SeqCst) >= 1,
        "the request went through the proxy"
    );
    assert!(target_hits.load(Ordering::SeqCst) >= 1);
    assert!(origin_hits.load(Ordering::SeqCst) >= 1);
}

/// Puts one proxy variable back when the test ends, including on a panic.
struct ProxyEnv {
    key: &'static str,
    previous: Option<String>,
}

impl ProxyEnv {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        // The test process owns this variable for the duration of the test, and `Drop` puts the
        // previous value back before the test returns, including after a panic.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ProxyEnv {
    fn drop(&mut self) {
        unsafe {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// Serves one absolute-form HTTP request per connection and counts accepted connections.
async fn forwarding_proxy() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("[::1]:0").await.expect("the proxy binds");
    let port = listener.local_addr().expect("proxy address").port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Err(error) = forward_one(inbound).await {
                    eprintln!("forwarding proxy: {error}");
                }
            });
        }
    });
    (port, hits)
}

async fn forward_one(mut inbound: TcpStream) -> Result<(), String> {
    let head = read_headers(&mut inbound).await?;
    let (request_line, headers) = head.split_once("\r\n").ok_or("no request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("method")?;
    let target = parts.next().ok_or("target")?;
    let version = parts.next().unwrap_or("HTTP/1.1");
    let target = target
        .strip_prefix("http://")
        .ok_or_else(|| format!("not an absolute-form target: {target}"))?;
    let (authority, path) = target.split_once('/').unwrap_or((target, ""));
    let mut upstream = TcpStream::connect(authority)
        .await
        .map_err(|error| error.to_string())?;
    let request = format!(
        "{method} /{path} {version}\r\n{}",
        rewrite_proxy_headers(headers)
    );
    upstream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let _ = upstream.flush().await;
    let response = tokio::time::timeout(Duration::from_secs(2), read_http_message(&mut upstream))
        .await
        .map_err(|_| "upstream response timed out".to_string())?
        .map_err(|error| error.to_string())?;
    inbound
        .write_all(&response)
        .await
        .map_err(|error| error.to_string())?;
    let _ = inbound.flush().await;
    Ok(())
}

/// Reads one HTTP/1 response: headers, then `Content-Length` bytes, or no body when the length
/// is absent.
async fn read_http_message(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 2048];
    let header_end = loop {
        let n = socket.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof before response headers",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut length = 0_usize;
    for line in head.split("\r\n") {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    while buf.len() < header_end + length {
        let n = socket.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    buf.truncate(header_end + length);
    Ok(buf)
}

fn rewrite_proxy_headers(headers: &str) -> String {
    let mut out = String::from("Connection: close\r\nContent-Length: 0\r\n");
    for line in headers.split("\r\n") {
        if line.is_empty() {
            continue;
        }
        let name = line.split(':').next().unwrap_or("").trim();
        if name.eq_ignore_ascii_case("connection") || name.eq_ignore_ascii_case("proxy-connection")
        {
            continue;
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    out.push_str("\r\n");
    out
}

async fn read_headers(socket: &mut TcpStream) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 2048];
    loop {
        let n = socket
            .read(&mut tmp)
            .await
            .map_err(|error| error.to_string())?;
        if n == 0 {
            return Err("eof before headers".to_string());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            return String::from_utf8(buf).map_err(|error| error.to_string());
        }
        if buf.len() > 65_536 {
            return Err("headers too large".to_string());
        }
    }
}
