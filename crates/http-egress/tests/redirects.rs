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

//! Redirects through real `reqwest` clients built with the egress policy, between local HTTP
//! servers that record every request they receive. Two servers on one host and different ports
//! are two origins.

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Bytes,
    http::{HeaderMap, Method, StatusCode, Uri, header::LOCATION},
    response::{IntoResponse, Response},
};
use http_egress::{DestinationGuard, Egress, OnOriginChange, configure, redirect_policy, send};
use tokio::net::TcpListener;

const API_KEY: &str = "x-api-key";
const SECRET: &str = "s3cr3t";

#[derive(Clone, Debug)]
struct Seen {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

/// A local server that records every request and answers it with `answer`.
struct Server {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    async fn start(answer: impl Fn(&Seen) -> Response + Clone + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!(
            "http://127.0.0.1:{}",
            listener.local_addr().expect("local address").port()
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let router = Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let answer = answer.clone();
                let log = Arc::clone(&log);
                async move {
                    let request = Seen {
                        method,
                        path: uri.path().to_string(),
                        headers,
                        body,
                    };
                    let response = answer(&request);
                    log.lock().expect("request log").push(request);
                    response
                }
            },
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self { url, seen }
    }

    /// A server that answers every request with `body`.
    async fn ok(body: &'static str) -> Self {
        Self::start(move |_| body.into_response()).await
    }

    /// A server that answers every request with `status` and `Location: location`.
    async fn redirecting(status: StatusCode, location: String) -> Self {
        Self::start(move |_| (status, [(LOCATION, location.clone())]).into_response()).await
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("request log").clone()
    }
}

fn client(egress: &Egress) -> reqwest::Client {
    configure(reqwest::Client::builder(), egress)
        .build()
        .expect("the client builds")
}

fn get_with_credentials(client: &reqwest::Client, url: &str) -> reqwest::Request {
    client
        .get(url)
        .header(API_KEY, SECRET)
        .header("authorization", format!("Bearer {SECRET}"))
        .header("accept", "application/json")
        .build()
        .expect("the request builds")
}

/// The finding this guards: a custom credential header followed a redirect to another origin,
/// because `reqwest` strips only the standard ones. The redirect is still followed, but the
/// other origin sees none of the request's own headers, not even an `Accept`.
#[tokio::test]
async fn a_redirect_to_another_origin_carries_no_credential() {
    let elsewhere = Server::ok("[]").await;
    let origin = Server::redirecting(StatusCode::FOUND, format!("{}/landing", elsewhere.url)).await;
    let egress = Egress::default();
    let client = client(&egress);

    let response = send(
        &client,
        get_with_credentials(&client, &format!("{}/data", origin.url)),
        &egress,
        OnOriginChange::Follow,
    )
    .await
    .expect("the redirect is followed");
    assert_eq!(response.status(), StatusCode::OK);

    let landed = elsewhere.seen();
    assert_eq!(landed.len(), 1);
    assert_eq!(landed[0].path, "/landing");
    assert_eq!(landed[0].headers.get(API_KEY), None);
    assert_eq!(landed[0].headers.get("authorization"), None);
    assert_eq!(
        landed[0]
            .headers
            .get("accept")
            .and_then(|value| value.to_str().ok()),
        Some("*/*"),
        "any header name can carry a credential, so not even the request's own `Accept` is \
         forwarded; `reqwest` adds its default"
    );
}

/// A redirect within the origin is the one place the credential belongs, so it is kept.
#[tokio::test]
async fn a_redirect_within_the_origin_keeps_the_credential() {
    let origin = Server::start(|request| {
        if request.path == "/moved" {
            "[]".into_response()
        } else {
            (StatusCode::FOUND, [(LOCATION, "/moved")]).into_response()
        }
    })
    .await;
    let egress = Egress::default();
    let client = client(&egress);

    send(
        &client,
        get_with_credentials(&client, &format!("{}/data", origin.url)),
        &egress,
        OnOriginChange::Follow,
    )
    .await
    .expect("the redirect is followed");

    let seen = origin.seen();
    assert_eq!(
        seen.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/data", "/moved"]
    );
    for request in &seen {
        assert_eq!(
            request
                .headers
                .get(API_KEY)
                .and_then(|value| value.to_str().ok()),
            Some(SECRET)
        );
    }
}

/// A request whose credential only its own origin may receive is sent refusing an origin
/// change: a redirect within the origin keeps the credential, and one that leaves it fails the
/// request before the other origin is contacted, even as a `302` `send` would otherwise follow
/// without the credential.
#[tokio::test]
async fn sending_refusing_an_origin_change_stays_on_the_request_origin() {
    let elsewhere = Server::ok("[]").await;
    let away = format!("{}/landing", elsewhere.url);
    let origin = Server::start(move |request| match request.path.as_str() {
        "/data" => (StatusCode::FOUND, [(LOCATION, "/moved".to_string())]).into_response(),
        "/moved" => "[]".into_response(),
        _ => (StatusCode::FOUND, [(LOCATION, away.clone())]).into_response(),
    })
    .await;
    let egress = Egress::default();
    let client = client(&egress);

    let response = send(
        &client,
        get_with_credentials(&client, &format!("{}/data", origin.url)),
        &egress,
        OnOriginChange::Refuse,
    )
    .await
    .expect("the redirect within the origin is followed");
    assert_eq!(response.status(), StatusCode::OK);
    let seen = origin.seen();
    assert_eq!(
        seen.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/data", "/moved"]
    );
    assert_eq!(
        seen[1]
            .headers
            .get(API_KEY)
            .and_then(|value| value.to_str().ok()),
        Some(SECRET)
    );

    let message = send(
        &client,
        get_with_credentials(&client, &format!("{}/away", origin.url)),
        &egress,
        OnOriginChange::Refuse,
    )
    .await
    .expect_err("the redirect off the origin is refused")
    .to_string();
    assert_eq!(
        message,
        format!(
            "the server redirected to another origin, {}, and this request carries a credential \
             that is sent only to {}",
            elsewhere.url, origin.url
        )
    );
    assert!(
        elsewhere.seen().is_empty(),
        "the other origin is never contacted"
    );
}

/// A `307` keeps the method and the body. Sending that body to another origin would replay
/// whatever it carries there, so the redirect is refused before the other origin is contacted.
#[tokio::test]
async fn a_307_never_resends_a_body_to_another_origin() {
    let elsewhere = Server::ok("{}").await;
    let origin = Server::redirecting(
        StatusCode::TEMPORARY_REDIRECT,
        format!("{}/collect", elsewhere.url),
    )
    .await;
    let egress = Egress::default();
    let client = client(&egress);
    let request = client
        .post(format!("{}/token", origin.url))
        .body("client_secret=s3cr3t")
        .build()
        .expect("the request builds");

    let message = send(&client, request, &egress, OnOriginChange::Follow)
        .await
        .expect_err("the redirect is refused")
        .to_string();
    assert_eq!(
        message,
        format!(
            "the server answered 307 to resend the request body to another origin, {}, and a \
             request body is never sent to an origin other than the one it was meant for",
            elsewhere.url
        )
    );
    assert!(
        elsewhere.seen().is_empty(),
        "the other origin is never contacted"
    );
}

/// A `303` answering a `POST` is followed as a `GET` without the body, as a browser does.
#[tokio::test]
async fn a_303_is_followed_as_a_get_without_the_body() {
    let origin = Server::start(|request| {
        if request.path == "/result" {
            "done".into_response()
        } else {
            (StatusCode::SEE_OTHER, [(LOCATION, "/result")]).into_response()
        }
    })
    .await;
    let egress = Egress::default();
    let client = client(&egress);
    let request = client
        .post(format!("{}/submit", origin.url))
        .header("content-type", "application/json")
        .body("{\"a\":1}")
        .build()
        .expect("the request builds");

    let response = send(&client, request, &egress, OnOriginChange::Follow)
        .await
        .expect("followed");
    assert_eq!(response.text().await.expect("body"), "done");
    let seen = origin.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].method, Method::GET);
    assert!(seen[1].body.is_empty());
    assert_eq!(seen[1].headers.get("content-type"), None);
}

/// The headers that describe a body, as each request on a chain within one origin carried them.
fn body_headers(seen: &[Seen]) -> Vec<(String, Method, Option<String>, Option<String>)> {
    seen.iter()
        .map(|request| {
            let header = |name: &str| {
                request
                    .headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            };
            (
                request.path.clone(),
                request.method.clone(),
                header("content-type"),
                header("content-encoding"),
            )
        })
        .collect()
}

/// Follows `request` through `send` and, on a second server with the same routes, through
/// `reqwest`'s own redirect handling, and returns what each server saw.
async fn sent_and_followed_by_reqwest(
    routes: fn(&Seen) -> Response,
    request: impl Fn(&reqwest::Client, &str) -> reqwest::Request,
) -> (Vec<Seen>, Vec<Seen>) {
    let through_send = Server::start(routes).await;
    let egress = Egress::default();
    let client = client(&egress);
    send(
        &client,
        request(&client, &through_send.url),
        &egress,
        OnOriginChange::Follow,
    )
    .await
    .expect("send follows the redirects");

    let through_reqwest = Server::start(routes).await;
    let following = reqwest::Client::new();
    following
        .execute(request(&following, &through_reqwest.url))
        .await
        .expect("reqwest follows the redirects");

    (through_send.seen(), through_reqwest.seen())
}

/// A `307`/`308` within the origin resends the request as it was sent: a request without a
/// body keeps the `Content-Type` and `Content-Encoding` it set, as `reqwest` keeps them when it
/// follows the redirect itself.
#[tokio::test]
async fn a_307_or_308_within_the_origin_keeps_a_bodyless_requests_content_headers() {
    fn temporary(request: &Seen) -> Response {
        match request.path.as_str() {
            "/data" => (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, "/moved")]).into_response(),
            _ => "[]".into_response(),
        }
    }
    fn permanent(request: &Seen) -> Response {
        match request.path.as_str() {
            "/data" => (StatusCode::PERMANENT_REDIRECT, [(LOCATION, "/moved")]).into_response(),
            _ => "[]".into_response(),
        }
    }
    let get = |client: &reqwest::Client, url: &str| {
        client
            .get(format!("{url}/data"))
            .header("content-type", "application/vnd.api+json")
            .header("content-encoding", "identity")
            .build()
            .expect("the request builds")
    };
    let kept = (
        Some("application/vnd.api+json".to_string()),
        Some("identity".to_string()),
    );

    for (status, routes) in [(307, temporary as fn(&Seen) -> Response), (308, permanent)] {
        let (sent, followed) = sent_and_followed_by_reqwest(routes, get).await;
        assert_eq!(
            body_headers(&sent),
            [
                (
                    "/data".to_string(),
                    Method::GET,
                    kept.0.clone(),
                    kept.1.clone()
                ),
                (
                    "/moved".to_string(),
                    Method::GET,
                    kept.0.clone(),
                    kept.1.clone()
                ),
            ],
            "{status}"
        );
        assert_eq!(body_headers(&sent), body_headers(&followed), "{status}");
    }
}

/// Once a redirect drops the body, the headers that described it stay dropped: a `303` that
/// turns a `POST` into a `GET`, then a `307` within the origin, reaches the last hop without the
/// `POST`'s `Content-Type`, as `reqwest` reaches it.
#[tokio::test]
async fn body_headers_a_redirect_drops_stay_dropped_on_later_hops() {
    fn routes(request: &Seen) -> Response {
        match request.path.as_str() {
            "/submit" => (StatusCode::SEE_OTHER, [(LOCATION, "/step")]).into_response(),
            "/step" => (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, "/result")]).into_response(),
            _ => "done".into_response(),
        }
    }
    let post = |client: &reqwest::Client, url: &str| {
        client
            .post(format!("{url}/submit"))
            .header("content-type", "application/json")
            .body("{\"a\":1}")
            .build()
            .expect("the request builds")
    };

    let (sent, followed) = sent_and_followed_by_reqwest(routes, post).await;
    assert_eq!(
        body_headers(&sent),
        [
            (
                "/submit".to_string(),
                Method::POST,
                Some("application/json".to_string()),
                None
            ),
            ("/step".to_string(), Method::GET, None, None),
            ("/result".to_string(), Method::GET, None, None),
        ]
    );
    assert_eq!(body_headers(&sent), body_headers(&followed));
}

/// A server that redirects `/a` to its own `/b` and answers `/b`: the rest of a chain that
/// continues on the host it moved to.
async fn chain_target() -> Server {
    Server::start(|request: &Seen| match request.path.as_str() {
        "/a" => (StatusCode::FOUND, [(LOCATION, "/b")]).into_response(),
        _ => "[]".into_response(),
    })
    .await
}

fn paths(server: &Server) -> Vec<String> {
    server
        .seen()
        .into_iter()
        .map(|request| request.path)
        .collect()
}

/// `reqwest` rebuilds every hop from the request's own headers and strips `Authorization` only
/// when that hop changes the host or port, so it sends the credential again when a chain that
/// left the origin continues on the host it moved to. A request with credentials refuses that
/// hop, whether `send` or `reqwest` follows the chain, and the hop is never requested.
#[tokio::test]
async fn a_chain_that_continues_on_the_host_it_moved_to_is_refused_with_credentials() {
    let target = chain_target().await;
    let origin = Server::redirecting(StatusCode::FOUND, format!("{}/a", target.url)).await;
    let egress = Egress::default();
    let client = client(&egress);
    let error = send(
        &client,
        get_with_credentials(&client, &format!("{}/data", origin.url)),
        &egress,
        OnOriginChange::FollowWithCredentials,
    )
    .await
    .expect_err("send refuses the chain");
    let refused = format!(
        "the redirect chain left {} and continued on {}, and a request that carries credentials \
         does not follow a redirect chain that continues on another host",
        origin.url, target.url
    );
    assert_eq!(error.to_string(), refused);
    assert_eq!(paths(&target), ["/a"]);

    let target = chain_target().await;
    let origin = Server::redirecting(StatusCode::FOUND, format!("{}/a", target.url)).await;
    let following = reqwest::Client::builder()
        .redirect(redirect_policy(
            Egress::default(),
            OnOriginChange::FollowWithCredentials,
        ))
        .build()
        .expect("the client builds");
    let error = following
        .execute(get_with_credentials(
            &following,
            &format!("{}/data", origin.url),
        ))
        .await
        .expect_err("reqwest refuses the chain");
    let refused = format!(
        "the redirect chain left {} and continued on {}, and a request that carries credentials \
         does not follow a redirect chain that continues on another host",
        origin.url, target.url
    );
    assert_eq!(
        http_egress::refused_redirect(&error).map(ToString::to_string),
        Some(refused)
    );
    assert_eq!(paths(&target), ["/a"]);
}

/// Without credentials the same chain is followed to its end, and `send` carries none of the
/// request's headers onto either hop off its origin.
#[tokio::test]
async fn a_chain_that_continues_on_the_host_it_moved_to_is_followed_without_credentials() {
    let target = chain_target().await;
    let origin = Server::redirecting(StatusCode::FOUND, format!("{}/a", target.url)).await;
    let egress = Egress::default();
    let client = client(&egress);

    let response = send(
        &client,
        get_with_credentials(&client, &format!("{}/data", origin.url)),
        &egress,
        OnOriginChange::Follow,
    )
    .await
    .expect("the chain is followed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(paths(&target), ["/a", "/b"]);
    for request in target.seen() {
        assert_eq!(
            request.headers.get("authorization"),
            None,
            "{}",
            request.path
        );
        assert_eq!(request.headers.get(API_KEY), None, "{}", request.path);
    }
}

/// A token request carries its secret in the body; `OnOriginChange::Refuse` keeps any redirect
/// from taking it to another origin, even one `reqwest` would follow with a `307`.
#[tokio::test]
async fn a_token_client_refuses_to_leave_its_origin() {
    let elsewhere = Server::ok("{}").await;
    let origin = Server::redirecting(
        StatusCode::TEMPORARY_REDIRECT,
        format!("{}/collect", elsewhere.url),
    )
    .await;
    let client = reqwest::Client::builder()
        .redirect(redirect_policy(Egress::default(), OnOriginChange::Refuse))
        .build()
        .expect("the client builds");

    let error = client
        .post(format!("{}/token", origin.url))
        .body("grant_type=client_credentials&client_secret=s3cr3t")
        .send()
        .await
        .expect_err("the redirect is refused");
    let source = std::error::Error::source(&error)
        .map(ToString::to_string)
        .unwrap_or_default();
    assert_eq!(
        source,
        format!(
            "the server redirected to another origin, {}, and this request carries a credential \
             that is sent only to {}",
            elsewhere.url, origin.url
        )
    );
    assert!(elsewhere.seen().is_empty());
}

/// The configured host is trusted, but a redirect that changes the port is a new destination:
/// it is refused when that destination is internal, and a redirect within the configured origin
/// is still followed.
#[tokio::test]
async fn another_port_on_an_internal_configured_host_is_a_new_destination() {
    let target = Server::ok("leaked").await;
    let target_port = target
        .url
        .rsplit(':')
        .next()
        .expect("the target URL has a port")
        .to_string();
    // Redirects to the target's port on whichever host name the request used.
    let origin = Server::start(move |request| {
        if request.path == "/same" {
            "ok".into_response()
        } else if request.path == "/hop" {
            (StatusCode::FOUND, [(LOCATION, "/same".to_string())]).into_response()
        } else {
            let host = request
                .headers
                .get("host")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.rsplit_once(':'))
                .map_or("127.0.0.1", |(host, _)| host)
                .to_string();
            let location = format!("http://{host}:{target_port}/internal");
            (StatusCode::FOUND, [(LOCATION, location)]).into_response()
        }
    })
    .await;
    let guard = DestinationGuard::new(
        |address| address.is_loopback(),
        |url| {
            url.host_str()
                .is_some_and(|host| host == "127.0.0.1" || host == "localhost")
        },
        "loopback is blocked",
    )
    .redirects_only();
    let egress = Egress::new(Some(guard), 5);
    let client = client(&egress);

    for host in ["127.0.0.1", "localhost"] {
        let origin_url = origin.url.replace("127.0.0.1", host);
        let request = client
            .get(format!("{origin_url}/hop"))
            .build()
            .expect("the request builds");
        let response = send(&client, request, &egress, OnOriginChange::Follow)
            .await
            .unwrap_or_else(|e| panic!("{host}: a redirect within the origin is followed: {e}"));
        assert_eq!(response.text().await.expect("body"), "ok");

        let request = client
            .get(format!("{origin_url}/data"))
            .build()
            .expect("the request builds");
        let message = send(&client, request, &egress, OnOriginChange::Follow)
            .await
            .expect_err("another port is refused")
            .to_string();
        assert_eq!(
            message,
            format!(
                "Refused to connect to {}/internal: loopback is blocked",
                target.url.replace("127.0.0.1", host)
            ),
            "{host}"
        );
    }
    assert!(target.seen().is_empty(), "the other port is never reached");
}
