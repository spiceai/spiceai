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

//! Where a file read follows the Hub's `resolve` redirect: never off the endpoint's host to a
//! private, loopback, or link-local address.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Response, StatusCode, header};
use bytes::Bytes;
use futures::TryStreamExt;
use huggingface_hub::settings::{Token, TokenScope, TokenSource};
use huggingface_hub::{FileRead, Hub, HubConfig, RepoId};
use parking_lot::Mutex;
use secrecy::SecretString;

use crate::mock_hub::C1;

const MARKER: &[u8] = b"served by the redirect target";

/// A server that answers every request with [`MARKER`] and records each one's
/// `Authorization` header.
#[derive(Clone, Default)]
struct Target(Arc<Mutex<Vec<Option<String>>>>);

impl Target {
    async fn start() -> (Self, SocketAddr) {
        let target = Self::default();
        let seen = target.clone();
        let app = axum::Router::new().fallback(move |headers: HeaderMap| {
            let seen = seen.clone();
            async move {
                seen.0.lock().push(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string),
                );
                Bytes::from_static(MARKER)
            }
        });
        (target, serve(app).await)
    }

    fn requests(&self) -> Vec<Option<String>> {
        self.0.lock().clone()
    }
}

/// A Hub whose `resolve` answers every file read with a redirect to `location`.
async fn redirecting_hub(location: String) -> SocketAddr {
    let app = axum::Router::new().fallback(move || {
        let location = location.clone();
        async move {
            Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, location)
                .body(Body::empty())
                .expect("a redirect response")
        }
    });
    serve(app).await
}

async fn serve(app: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let address = listener.local_addr().expect("a local address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    address
}

fn hub(endpoint: &str) -> Arc<Hub> {
    Arc::new(
        Hub::new(
            HubConfig {
                endpoint: url::Url::parse(endpoint).expect("a URL"),
                token: Some(Token::new(
                    SecretString::from("hf_secret".to_string()),
                    TokenSource::Param,
                )),
                token_scope: TokenScope::Any,
            },
            tokio::runtime::Handle::current(),
        )
        .expect("a client"),
    )
}

fn repo() -> RepoId {
    RepoId::dataset("o", "d").expect("a valid repository")
}

async fn read(hub: &Arc<Hub>) -> huggingface_hub::Result<FileRead> {
    hub.read(&repo(), C1, "data.parquet", None).await
}

async fn read_bytes(hub: &Arc<Hub>) -> Vec<u8> {
    let file = read(hub).await.expect("the file is read");
    let chunks: Vec<Bytes> = file.stream.try_collect().await.expect("the body is read");
    chunks.concat()
}

fn refusal(origin: &str) -> String {
    format!(
        "The Hugging Face Hub returned an unexpected response for dataset 'o/d': the download of 'data.parquet' redirected to {origin}, a private, loopback, or link-local address, which was not followed. If the Hub serves files from an internal host on purpose, set SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true"
    )
}

#[tokio::test]
async fn a_download_is_not_redirected_to_an_internal_address_off_the_hubs_host() {
    // The Hub and the target listen on 127.0.0.1, but the Hub is reached by another host name
    // than the redirect names, so the redirect leaves the endpoint's host. `localhost.` is not
    // an internal name, so only the address it resolves to refuses it.
    for (hub_host, target_host) in [
        ("localhost", "127.0.0.1"),
        ("127.0.0.1", "localhost"),
        ("127.0.0.1", "localhost."),
    ] {
        let (target, target_address) = Target::start().await;
        let location = format!("http://{target_host}:{}/blob", target_address.port());
        let hub_address = redirecting_hub(location).await;
        let hub = hub(&format!("http://{hub_host}:{}", hub_address.port()));

        let Err(error) = read(&hub).await else {
            panic!("the redirect from {hub_host} to {target_host} is followed");
        };
        assert_eq!(
            error.to_string(),
            refusal(&format!("http://{target_host}:{}", target_address.port())),
            "{hub_host} -> {target_host}"
        );
        assert_eq!(
            target.requests(),
            Vec::<Option<String>>::new(),
            "{target_host}"
        );
    }
}

#[tokio::test]
async fn a_download_follows_a_redirect_to_another_port_on_the_hubs_host() {
    let (target, target_address) = Target::start().await;
    let hub_address = redirecting_hub(format!("http://{target_address}/blob")).await;
    let hub = hub(&format!("http://{hub_address}"));

    assert_eq!(read_bytes(&hub).await, MARKER);
    // Another port is another origin, which never receives the token.
    assert_eq!(target.requests(), vec![None]);
}

#[cfg(unix)]
#[tokio::test]
async fn allowing_private_hosts_lets_a_download_follow_a_redirect_to_an_internal_address() {
    crate::require_process_per_test("it sets SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS");
    let (target, target_address) = Target::start().await;
    let hub_address = redirecting_hub(format!("http://{target_address}/blob")).await;
    let endpoint = format!("http://localhost:{}", hub_address.port());

    read(&hub(&endpoint))
        .await
        .err()
        .expect("the redirect is refused before private hosts are allowed");
    // SAFETY: `require_process_per_test` ensures no other thread of this process reads the
    // environment.
    unsafe { std::env::set_var("SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS", "true") };
    assert_eq!(read_bytes(&hub(&endpoint)).await, MARKER);
    assert_eq!(target.requests(), vec![None]);
}
