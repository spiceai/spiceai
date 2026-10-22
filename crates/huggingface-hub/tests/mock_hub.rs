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

//! A Hugging Face Hub served on loopback, for driving downloads over real HTTP.
//!
//! It answers the endpoints the client uses the way the Hub does — revision lookup, a paginated
//! tree, `resolve` redirecting LFS files to a CDN on a *different origin* and small files to
//! the Hub's own `resolve-cache` — with the content ids the Hub reports: the git blob SHA-1 of
//! every file and the SHA-256 of an LFS file. It records every request, so a test asserts what
//! was sent (ranges, tokens) as well as what came back, and injects faults per test.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use bytes::Bytes;
use parking_lot::Mutex;
use sha1::Sha1;
use sha2::{Digest, Sha256};

pub const C1: &str = "1111111111111111111111111111111111111111";
pub const C2: &str = "2222222222222222222222222222222222222222";

#[derive(Clone)]
pub struct MockFile {
    pub bytes: Bytes,
    /// Stored in LFS: `resolve` redirects to the CDN.
    pub lfs: bool,
}

impl MockFile {
    pub fn lfs(bytes: impl Into<Bytes>) -> Self {
        Self {
            bytes: bytes.into(),
            lfs: true,
        }
    }

    pub fn text(content: &str) -> Self {
        Self {
            bytes: Bytes::from(content.to_string()),
            lfs: false,
        }
    }

    /// The git blob id: SHA-1 of `blob <size>\0` and the content.
    pub fn git_oid(&self) -> String {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", self.bytes.len()).as_bytes());
        hasher.update(&self.bytes);
        hex::encode(hasher.finalize())
    }

    pub fn sha256(&self) -> String {
        hex::encode(Sha256::digest(&self.bytes))
    }

    /// The blob name `huggingface_hub` gives the file in its cache.
    pub fn etag(&self) -> String {
        if self.lfs {
            self.sha256()
        } else {
            self.git_oid()
        }
    }
}

#[derive(Default)]
struct Repo {
    branches: HashMap<String, String>,
    commits: HashMap<String, BTreeMap<String, MockFile>>,
    gated: bool,
}

/// A request the mock received.
#[derive(Debug, Clone)]
pub struct Seen {
    pub cdn: bool,
    pub path: String,
    pub authorization: Option<String>,
    pub range: Option<String>,
}

#[derive(Default)]
pub struct Faults {
    /// Requests answered `429` with `Retry-After: 1` before one succeeds.
    pub rate_limited: u32,
    /// API requests answered `503` before one succeeds.
    pub unavailable: u32,
    /// Every Hub request (API and `resolve`) is answered `503`.
    pub hub_down: bool,
    /// CDN responses that end half way through their body, as a reset connection does.
    pub truncated: u32,
    /// CDN responses whose bytes are altered (same length).
    pub corrupted: u32,
    /// The CDN answers a ranged request with the whole file.
    pub ignore_range: bool,
    /// CDN responses held back this long before they start.
    pub delay: Option<Duration>,
}

#[derive(Default)]
struct MockState {
    repos: HashMap<String, Repo>,
    /// When set, the Hub answers 401 to requests without this bearer token.
    token: Option<String>,
    faults: Faults,
    seen: Vec<Seen>,
    hub_base: String,
    cdn_base: String,
    /// CDN blobs by SHA-256.
    blobs: HashMap<String, Bytes>,
    /// Renamed repositories: the old `<plural>/<owner>/<name>` and the new.
    renamed: HashMap<String, String>,
}

#[derive(Clone)]
pub struct MockHub(Arc<Mutex<MockState>>);

impl MockHub {
    pub async fn start() -> Self {
        let mock = MockHub(Arc::new(Mutex::new(MockState::default())));
        let hub = serve(
            axum::Router::new()
                .fallback(hub_handler)
                .with_state(mock.clone()),
        )
        .await;
        let cdn = serve(
            axum::Router::new()
                .fallback(cdn_handler)
                .with_state(mock.clone()),
        )
        .await;
        {
            let mut state = mock.0.lock();
            state.hub_base = format!("http://{hub}");
            // A different origin: `localhost` rather than `127.0.0.1`.
            state.cdn_base = format!("http://localhost:{}", cdn.port());
        }
        mock
    }

    pub fn endpoint(&self) -> String {
        self.0.lock().hub_base.clone()
    }

    /// Adds `commit` to the repository `<plural>/<owner>/<name>` (`models/o/m`), with `files`.
    pub fn commit(&self, repo: &str, commit: &str, files: Vec<(&str, MockFile)>) {
        let mut state = self.0.lock();
        let files: BTreeMap<String, MockFile> = files
            .into_iter()
            .map(|(path, file)| (path.to_string(), file))
            .collect();
        for file in files.values().filter(|file| file.lfs) {
            state.blobs.insert(file.sha256(), file.bytes.clone());
        }
        state
            .repos
            .entry(repo.to_string())
            .or_default()
            .commits
            .insert(commit.to_string(), files);
    }

    pub fn branch(&self, repo: &str, branch: &str, commit: &str) {
        self.0
            .lock()
            .repos
            .entry(repo.to_string())
            .or_default()
            .branches
            .insert(branch.to_string(), commit.to_string());
    }

    /// Renames the repository `new` (`models/o/m`) from `old`, as the Hub does: a request that
    /// names `old` is redirected to the same path under `new`.
    pub fn rename(&self, old: &str, new: &str) {
        self.0
            .lock()
            .renamed
            .insert(old.to_string(), new.to_string());
    }

    pub fn gate(&self, repo: &str) {
        self.0
            .lock()
            .repos
            .entry(repo.to_string())
            .or_default()
            .gated = true;
    }

    pub fn require_token(&self, token: &str) {
        self.0.lock().token = Some(token.to_string());
    }

    pub fn faults(&self, set: impl FnOnce(&mut Faults)) {
        set(&mut self.0.lock().faults);
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.0.lock().seen.clone()
    }

    pub fn clear_seen(&self) {
        self.0.lock().seen.clear();
    }

    /// The requests that downloaded file content, from the Hub or its CDN.
    pub fn downloads(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.cdn || seen.path.contains("/resolve"))
            .collect()
    }
}

async fn serve(app: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the mock binds a loopback port");
    let address = listener.local_addr().expect("the mock has an address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    address
}

fn record(state: &mut MockState, cdn: bool, request: &Request<Body>) {
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string)
    };
    state.seen.push(Seen {
        cdn,
        path: request
            .uri()
            .path_and_query()
            .map(ToString::to_string)
            .unwrap_or_default(),
        authorization: header("authorization"),
        range: header("range"),
    });
}

fn decode(segment: &str) -> String {
    percent_encoding::percent_decode_str(segment)
        .decode_utf8_lossy()
        .into_owned()
}

fn response(
    status: StatusCode,
    headers: &[(&str, String)],
    body: impl Into<Body>,
) -> Response<Body> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    builder.body(body.into()).expect("a well-formed response")
}

fn hub_error(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    response(
        status,
        &[
            ("x-error-code", code.to_string()),
            ("x-error-message", message.to_string()),
        ],
        format!("{{\"error\":\"{message}\"}}"),
    )
}

/// Where the Hub redirects a request for a renamed repository: the same path, relative, under
/// the new name, for the API (`/api/models/o/old/…`) and for files (`/o/old/resolve/…`).
fn renamed_location(renamed: &HashMap<String, String>, path: &str, query: &str) -> Option<String> {
    // A model's file URLs carry no `models/` prefix.
    let in_files = |repo: &str| repo.strip_prefix("models/").unwrap_or(repo).to_string();
    renamed.iter().find_map(|(old, new)| {
        [
            (format!("/api/{old}/"), format!("/api/{new}/")),
            (
                format!("/{}/", in_files(old)),
                format!("/{}/", in_files(new)),
            ),
        ]
        .into_iter()
        .find_map(|(from, to)| {
            let rest = path.strip_prefix(&from)?;
            let query = if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            };
            Some(format!("{to}{rest}{query}"))
        })
    })
}

/// The byte range a `Range` header asks of a file of `size` bytes, inclusive.
fn requested_range(range: Option<&str>, size: u64) -> Option<(u64, u64)> {
    let range = range?.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    match (start, end) {
        ("", suffix) => {
            let suffix: u64 = suffix.parse().ok()?;
            Some((size.saturating_sub(suffix), size - 1))
        }
        (start, "") => Some((start.parse().ok()?, size - 1)),
        (start, end) => Some((start.parse().ok()?, end.parse::<u64>().ok()?.min(size - 1))),
    }
}

/// Serves `range` of `bytes` as the Hub's CDN does, with the faults that apply.
fn ranged(state: &mut MockState, bytes: &Bytes, range: Option<&str>) -> Response<Body> {
    let size = bytes.len() as u64;
    let (status, start, end, mut body, headers) =
        match requested_range(range, size).filter(|_| !state.faults.ignore_range) {
            Some((start, end)) => {
                let body = bytes.slice(
                    usize::try_from(start).expect("a small file")
                        ..=usize::try_from(end).expect("a small file"),
                );
                (
                    StatusCode::PARTIAL_CONTENT,
                    start,
                    end,
                    body,
                    vec![("content-range", format!("bytes {start}-{end}/{size}"))],
                )
            }
            None => (
                StatusCode::OK,
                0,
                size.saturating_sub(1),
                bytes.clone(),
                Vec::new(),
            ),
        };
    let _ = (start, end);
    if state.faults.corrupted > 0 && !body.is_empty() {
        state.faults.corrupted -= 1;
        let mut altered = body.to_vec();
        for byte in &mut altered {
            *byte = !*byte;
        }
        body = Bytes::from(altered);
    }
    let length = body.len();
    let mut headers = headers;
    headers.push(("content-length", length.to_string()));
    if state.faults.truncated > 0 && length > 1 {
        state.faults.truncated -= 1;
        // Promise the whole length, send half, then end the stream with an error: the client
        // sees a connection that broke off. The pause lets the headers and the first half
        // reach the client first, so the break comes mid-body rather than before the response.
        let half = body.slice(..length / 2);
        let stream = futures::stream::unfold(Some(half), |half| async move {
            if let Some(half) = half {
                return Some((Ok::<Bytes, std::io::Error>(half), None));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            Some((
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "the mock reset the connection",
                )),
                None,
            ))
        });
        return response(status, &headers, Body::from_stream(stream));
    }
    response(status, &headers, body)
}

async fn hub_handler(State(mock): State<MockHub>, request: Request<Body>) -> Response<Body> {
    let mut state = mock.0.lock();
    record(&mut state, false, &request);
    if state.faults.hub_down {
        return hub_error(StatusCode::SERVICE_UNAVAILABLE, "", "The Hub is down.");
    }
    if let Some(token) = &state.token {
        let presented = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if presented != Some(token.as_str()) {
            return hub_error(
                StatusCode::UNAUTHORIZED,
                "",
                "Invalid username or password.",
            );
        }
    }
    if state.faults.rate_limited > 0 {
        state.faults.rate_limited -= 1;
        return response(
            StatusCode::TOO_MANY_REQUESTS,
            &[("retry-after", "1".to_string())],
            "rate limited",
        );
    }
    let path = request.uri().path().to_string();
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let is_api = segments.first() == Some(&"api");
    if is_api && state.faults.unavailable > 0 {
        state.faults.unavailable -= 1;
        return hub_error(StatusCode::SERVICE_UNAVAILABLE, "", "Try again later.");
    }
    let query = request.uri().query().unwrap_or_default().to_string();
    if let Some(location) = renamed_location(&state.renamed, &path, &query) {
        return response(
            StatusCode::TEMPORARY_REDIRECT,
            &[("location", location)],
            Body::empty(),
        );
    }
    let range = request
        .headers()
        .get("range")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);

    match segments.as_slice() {
        ["api", plural, owner, name, "revision", revision] => {
            let Some(repo) = state.repos.get(&format!("{plural}/{owner}/{name}")) else {
                return hub_error(
                    StatusCode::NOT_FOUND,
                    "RepoNotFound",
                    "Repository not found",
                );
            };
            let sha = if repo.commits.contains_key(*revision) {
                (*revision).to_string()
            } else if let Some(sha) = repo.branches.get(*revision) {
                sha.clone()
            } else {
                return hub_error(
                    StatusCode::NOT_FOUND,
                    "RevisionNotFound",
                    &format!("Invalid rev id: {revision}"),
                );
            };
            response(
                StatusCode::OK,
                &[],
                format!(
                    "{{\"id\":\"{owner}/{name}\",\"sha\":\"{sha}\",\"lastModified\":\"2026-01-02T03:04:05.000Z\"}}"
                ),
            )
        }
        ["api", plural, owner, name, "tree", commit, rest @ ..] => {
            let Some(repo) = state.repos.get(&format!("{plural}/{owner}/{name}")) else {
                return hub_error(
                    StatusCode::NOT_FOUND,
                    "RepoNotFound",
                    "Repository not found",
                );
            };
            let Some(files) = repo.commits.get(*commit) else {
                return hub_error(StatusCode::NOT_FOUND, "RevisionNotFound", "Invalid rev id");
            };
            let folder = rest.join("/");
            let recursive = query.contains("recursive=true");
            // The Hub masks the content ids of a gated repository's LFS files from an account
            // without access.
            let masked = repo.gated;
            let mut entries = Vec::new();
            for (path, file) in files {
                let Some(relative) = (if folder.is_empty() {
                    Some(path.as_str())
                } else {
                    path.strip_prefix(&format!("{folder}/"))
                }) else {
                    continue;
                };
                if !recursive && relative.contains('/') {
                    continue;
                }
                let mut entry = serde_json::json!({
                    "type": "file",
                    "oid": file.git_oid(),
                    "size": file.bytes.len(),
                    "path": path,
                });
                if file.lfs {
                    let oid = if masked {
                        "*".repeat(64)
                    } else {
                        file.sha256()
                    };
                    entry["lfs"] = serde_json::json!({"oid": oid, "size": file.bytes.len(), "pointerSize": 133});
                }
                entries.push(entry);
            }
            response(
                StatusCode::OK,
                &[],
                serde_json::to_string(&entries).expect("JSON"),
            )
        }
        [first, ..] if *first != "api" => {
            // `[<plural>/]<owner>/<name>/resolve/<commit>/<path>`: a model has no prefix.
            let (plural, rest) = match segments.as_slice() {
                [plural @ ("datasets" | "spaces"), rest @ ..] => (*plural, rest),
                rest => ("models", rest),
            };
            let [owner, name, "resolve", commit, file_path @ ..] = rest else {
                return hub_error(StatusCode::NOT_FOUND, "", "Not found");
            };
            let Some(repo) = state.repos.get(&format!("{plural}/{owner}/{name}")) else {
                return hub_error(
                    StatusCode::NOT_FOUND,
                    "RepoNotFound",
                    "Repository not found",
                );
            };
            if repo.gated {
                return hub_error(
                    StatusCode::UNAUTHORIZED,
                    "GatedRepo",
                    "Access to this repository is restricted.",
                );
            }
            let file_path = file_path.join("/");
            let Some(file) = repo
                .commits
                .get(*commit)
                .and_then(|files| files.get(&file_path))
                .cloned()
            else {
                return hub_error(StatusCode::NOT_FOUND, "EntryNotFound", "Entry not found");
            };
            if file.lfs {
                let location = format!("{}/blob/{}?Expires=1", state.cdn_base, file.sha256());
                response(
                    StatusCode::FOUND,
                    &[
                        ("x-linked-etag", format!("\"{}\"", file.sha256())),
                        ("x-linked-size", file.bytes.len().to_string()),
                        ("location", location),
                    ],
                    "",
                )
            } else {
                ranged(&mut state, &file.bytes, range.as_deref())
            }
        }
        _ => hub_error(StatusCode::NOT_FOUND, "", "Not found"),
    }
}

async fn cdn_handler(State(mock): State<MockHub>, request: Request<Body>) -> Response<Body> {
    let delay = {
        let mut state = mock.0.lock();
        record(&mut state, true, &request);
        state.faults.delay
    };
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await;
    }
    let mut state = mock.0.lock();
    let id = request
        .uri()
        .path()
        .trim_start_matches("/blob/")
        .to_string();
    let range = request
        .headers()
        .get("range")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let Some(bytes) = state.blobs.get(&id).cloned() else {
        return response(StatusCode::NOT_FOUND, &[], "no such blob");
    };
    ranged(&mut state, &bytes, range.as_deref())
}
