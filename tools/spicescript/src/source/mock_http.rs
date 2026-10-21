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

//! An HTTP server whose responses a script sets, for connectors and features
//! that call out over HTTP (the `http` connector, OAuth, caching, model
//! providers). Each instance gets its own path prefix, so its routes and the
//! requests it caused are its own.
//!
//! `source-exec` adds a route, as YAML:
//!
//! ```yaml
//! path: /users.json        # under the instance's prefix
//! method: GET              # any when unset
//! query: page=2            # the exact query string, when set
//! headers-match: { authorization: Bearer t0k }   # all must be present
//! status: 200
//! headers: { content-type: application/json }
//! body: '[{"id": 1}]'
//! delay-ms: 0
//! times: 1                 # answers this many requests, then the next route
//! ```
//!
//! Later routes for a path come first, so a script changes an answer by adding
//! a route; `reset` removes every route. `source-query` reads the requests:
//! `count <path>`, `log <path>` (method, path and query, body) or
//! `header <path> <name>`.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;

use super::{Source, TextResult};
use crate::{docker::Container, generate::Dialect};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct Route {
    path: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    headers_match: BTreeMap<String, String>,
    #[serde(default = "ok")]
    status: u16,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    times: Option<u64>,
}

fn ok() -> u16 {
    200
}

#[derive(Debug, Clone)]
struct Request {
    method: String,
    path: String,
    query: Option<String>,
    headers: HeaderMap,
    body: String,
}

#[derive(Default)]
struct Mock {
    /// Routes by full path (prefix included), newest last.
    routes: Vec<(Route, u64)>,
    requests: Vec<Request>,
}

pub struct MockHttp {
    port: u16,
    state: Arc<Mutex<Mock>>,
    server: tokio::task::JoinHandle<()>,
}

impl MockHttp {
    pub async fn start() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("Failed to bind the mock HTTP server")?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(Mutex::new(Mock::default()));
        let app = axum::Router::new()
            .fallback(answer)
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            port,
            state,
            server,
        })
    }

    fn prefix(namespace: &str) -> String {
        format!("/{namespace}")
    }
}

impl Drop for MockHttp {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn answer(
    State(state): State<Arc<Mutex<Mock>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = Request {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    let chosen = {
        let mut state = state.lock();
        state.requests.push(request.clone());
        let found = state.routes.iter_mut().rev().find(|(route, served)| {
            route.path == request.path
                && route
                    .method
                    .as_deref()
                    .is_none_or(|m| m.eq_ignore_ascii_case(&request.method))
                && route
                    .query
                    .as_deref()
                    .is_none_or(|q| Some(q) == request.query.as_deref())
                && route.headers_match.iter().all(|(name, value)| {
                    request
                        .headers
                        .get(name.as_str())
                        .and_then(|v| v.to_str().ok())
                        == Some(value.as_str())
                })
                && route.times.is_none_or(|times| *served < times)
        });
        found.map(|(route, served)| {
            *served += 1;
            route.clone()
        })
    };
    let Some(route) = chosen else {
        return (
            StatusCode::NOT_FOUND,
            format!("no mock route for {} {uri}", request.method),
        )
            .into_response();
    };
    if route.delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(route.delay_ms)).await;
    }
    let mut response = (
        StatusCode::from_u16(route.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        route.body,
    )
        .into_response();
    for (name, value) in &route.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::try_from(value.as_str()),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

#[async_trait]
impl Source for MockHttp {
    fn container(&self) -> Option<&Container> {
        None
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        let mut extra = serde_json::Map::new();
        extra.insert(
            "url".into(),
            json!(format!(
                "http://127.0.0.1:{}{}",
                self.port,
                Self::prefix(namespace)
            )),
        );
        Ok(super::Namespace {
            params: json!({}),
            extra,
        })
    }

    async fn exec(&self, namespace: &str, text: &str) -> Result<()> {
        let prefix = Self::prefix(namespace);
        if text.trim() == "reset" {
            self.state
                .lock()
                .routes
                .retain(|(r, _)| !r.path.starts_with(&format!("{prefix}/")));
            return Ok(());
        }
        let mut route: Route =
            yaml::from_str(text).with_context(|| format!("not a mock route: {text}"))?;
        if !route.path.starts_with('/') {
            bail!("a mock route path starts with '/': {}", route.path);
        }
        route.path = format!("{prefix}{}", route.path);
        self.state.lock().routes.push((route, 0));
        Ok(())
    }

    async fn query(&self, namespace: &str, text: &str) -> Result<TextResult> {
        let prefix = Self::prefix(namespace);
        let words: Vec<&str> = text.split_whitespace().collect();
        let state = self.state.lock();
        let matching = |path: &str| -> Vec<&Request> {
            let full = format!("{prefix}{path}");
            state.requests.iter().filter(|r| r.path == full).collect()
        };
        match words.as_slice() {
            ["count", path] => Ok(TextResult {
                columns: vec!["count".into()],
                rows: vec![vec![Some(matching(path).len().to_string())]],
            }),
            ["log", path] => Ok(TextResult {
                columns: vec!["method".into(), "uri".into(), "body".into()],
                rows: matching(path)
                    .into_iter()
                    .map(|r| {
                        let uri = match &r.query {
                            Some(q) => format!("{}?{q}", &r.path[prefix.len()..]),
                            None => r.path[prefix.len()..].to_string(),
                        };
                        vec![Some(r.method.clone()), Some(uri), Some(r.body.clone())]
                    })
                    .collect(),
            }),
            ["header", path, name] => Ok(TextResult {
                columns: vec![(*name).to_string()],
                rows: matching(path)
                    .into_iter()
                    .map(|r| {
                        vec![
                            r.headers
                                .get(*name)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string),
                        ]
                    })
                    .collect(),
            }),
            _ => bail!(
                "a mock HTTP source answers `count <path>`, `log <path>` or `header <path> <name>`, not `{text}`"
            ),
        }
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    async fn load(
        &self,
        _namespace: &str,
        _table: &str,
        _columns: &[String],
        _rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        bail!("a mock HTTP source serves routes; add them with source-exec")
    }
}
