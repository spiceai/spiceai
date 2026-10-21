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

//! The connector against a Hub served on loopback: real HTTP, real Parquet, real `DataFusion`.
//!
//! The mock implements the endpoints the connector uses the way the Hub does — a paginated
//! tree, `paths-info`, `resolve` redirecting LFS files to a CDN on a *different origin* and
//! small files to the Hub's own `resolve-cache` — and records every request, so a test
//! asserts what was sent (ranges, tokens, commits) as well as what came back. Faults (a
//! truncated download, an API response cut off part way, an expired CDN URL, rate limiting, a
//! server that ignores `Range`) are injected per test.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use bytes::Bytes;
use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parking_lot::Mutex;
use runtime_component::dataset::DatasetSpec;
use runtime_parameters::Parameters;
use secrecy::SecretString;

use crate::hub::{Hub, HubConfig};
use crate::location::RepoId;
use crate::store::{HuggingFaceStore, store_url};
use crate::{HuggingFace, PARAMETERS};

const C1: &str = "1111111111111111111111111111111111111111";
const C2: &str = "2222222222222222222222222222222222222222";
const C3: &str = "3333333333333333333333333333333333333333";
const TOKEN: &str = "hf_test_token";

#[derive(Clone)]
struct File {
    bytes: Bytes,
    /// Stored in LFS: `resolve` redirects to the CDN.
    lfs: bool,
}

#[derive(Default)]
struct Repo {
    branches: HashMap<String, String>,
    commits: HashMap<String, BTreeMap<String, File>>,
    gated: bool,
}

#[derive(Debug, Clone)]
struct Seen {
    cdn: bool,
    path: String,
    authorization: Option<String>,
    range: Option<String>,
}

#[derive(Default)]
struct Faults {
    /// Revision lookups answered 429 before one succeeds.
    rate_limited: u32,
    /// CDN responses cut off half way through their body.
    truncated: u32,
    /// CDN requests refused as if their signature had expired.
    expired: u32,
    /// The CDN answers a ranged request with the whole file.
    ignore_range: bool,
    /// The CDN streams a whole file without `Content-Length`, as a chunked proxy does.
    chunked: bool,
    /// Tree pages after the first answer 404, as a listing cut short would.
    missing_later_pages: bool,
    /// API responses cut off half way through their body, by endpoint (`revision`, `tree`,
    /// `paths-info`).
    api_truncated: HashMap<&'static str, u32>,
}

#[derive(Default)]
struct MockState {
    repos: HashMap<String, Repo>,
    /// Entries per tree page.
    page_size: usize,
    /// When set, the Hub answers 401 to requests without this bearer token.
    token: Option<String>,
    /// More bearer tokens the Hub accepts.
    tokens: Vec<String>,
    faults: Faults,
    seen: Vec<Seen>,
    hub_base: String,
    cdn_base: String,
    /// CDN blobs by id.
    blobs: HashMap<String, Bytes>,
}

#[derive(Clone)]
struct Mock(Arc<Mutex<MockState>>);

impl Mock {
    async fn start() -> Self {
        let mock = Mock(Arc::new(Mutex::new(MockState {
            page_size: 1000,
            ..MockState::default()
        })));
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

    fn endpoint(&self) -> String {
        self.0.lock().hub_base.clone()
    }

    fn commit(&self, repo: &str, sha: &str, files: Vec<(&str, File)>) {
        let mut state = self.0.lock();
        let files: BTreeMap<String, File> = files
            .into_iter()
            .map(|(path, file)| (path.to_string(), file))
            .collect();
        for file in files.values().filter(|file| file.lfs) {
            state.blobs.insert(blob_id(&file.bytes), file.bytes.clone());
        }
        state
            .repos
            .entry(repo.to_string())
            .or_default()
            .commits
            .insert(sha.to_string(), files);
    }

    fn branch(&self, repo: &str, branch: &str, sha: &str) {
        self.0
            .lock()
            .repos
            .entry(repo.to_string())
            .or_default()
            .branches
            .insert(branch.to_string(), sha.to_string());
    }

    fn seen(&self) -> Vec<Seen> {
        self.0.lock().seen.clone()
    }

    fn clear_seen(&self) {
        self.0.lock().seen.clear();
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

fn blob_id(bytes: &Bytes) -> String {
    blake3::hash(bytes).to_hex().to_string()
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

/// Serves `range` of `bytes` as the Hub and its CDN do: 206 with `Content-Range`.
fn ranged(bytes: &Bytes, range: Option<&str>, extra: &[(&str, String)]) -> Response<Body> {
    let size = bytes.len() as u64;
    let Some(range) = range.and_then(|range| range.strip_prefix("bytes=")) else {
        let mut headers = extra.to_vec();
        headers.push(("content-length", size.to_string()));
        return response(StatusCode::OK, &headers, bytes.clone());
    };
    let (start, end) = match range.split_once('-') {
        Some(("", suffix)) => {
            let suffix: u64 = suffix.parse().expect("a suffix length");
            (size.saturating_sub(suffix), size - 1)
        }
        Some((start, "")) => (start.parse().expect("a start offset"), size - 1),
        Some((start, end)) => (
            start.parse().expect("a start offset"),
            end.parse::<u64>().expect("an end offset").min(size - 1),
        ),
        None => panic!("an invalid range {range}"),
    };
    let mut headers = extra.to_vec();
    headers.push(("content-range", format!("bytes {start}-{end}/{size}")));
    let start = usize::try_from(start).expect("a small file");
    let end = usize::try_from(end).expect("a small file");
    response(
        StatusCode::PARTIAL_CONTENT,
        &headers,
        bytes.slice(start..=end),
    )
}

async fn hub_handler(State(mock): State<Mock>, request: Request<Body>) -> Response<Body> {
    let mut state = mock.0.lock();
    record(&mut state, false, &request);
    let accepted: Vec<String> = state.token.iter().chain(&state.tokens).cloned().collect();
    if !accepted.is_empty() {
        let presented = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !presented.is_some_and(|token| accepted.iter().any(|t| t == token)) {
            return hub_error(
                StatusCode::UNAUTHORIZED,
                "",
                "Invalid username or password.",
            );
        }
    }
    let path = request.uri().path().to_string();
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let range = request
        .headers()
        .get("range")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);

    let api_endpoint = match segments.as_slice() {
        ["api", "datasets", _, _, "revision", ..] => Some("revision"),
        ["api", "datasets", _, _, "tree", ..] => Some("tree"),
        ["api", "datasets", _, _, "paths-info", ..] => Some("paths-info"),
        _ => None,
    };
    let response = match segments.as_slice() {
        ["api", "datasets", owner, name, "revision", revision] => {
            if state.faults.rate_limited > 0 {
                state.faults.rate_limited -= 1;
                return response(
                    StatusCode::TOO_MANY_REQUESTS,
                    &[("ratelimit", "\"api\";r=0;t=0".to_string())],
                    "rate limited",
                );
            }
            let Some(repo) = state.repos.get(&format!("{owner}/{name}")) else {
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
        ["api", "datasets", owner, name, "tree", commit, rest @ ..] => {
            let Some(repo) = state.repos.get(&format!("{owner}/{name}")) else {
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
            let query = request.uri().query().unwrap_or_default().to_string();
            if state.faults.missing_later_pages && query.contains("cursor=") {
                return hub_error(StatusCode::NOT_FOUND, "EntryNotFound", "does not exist");
            }
            let recursive = query.contains("recursive=true");
            let cursor: usize = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("cursor="))
                .map_or(0, |cursor| cursor.parse().expect("a numeric cursor"));
            let mut entries = Vec::new();
            let mut folders = std::collections::BTreeSet::new();
            for (path, file) in files {
                let Some(relative) = (if folder.is_empty() {
                    Some(path.as_str())
                } else {
                    path.strip_prefix(&format!("{folder}/"))
                }) else {
                    continue;
                };
                let depth = relative.matches('/').count();
                // Every folder between `folder` and the file.
                let parts: Vec<&str> = relative.split('/').collect();
                for level in 1..parts.len() {
                    if recursive || level == 1 {
                        let prefix = parts[..level].join("/");
                        let full = if folder.is_empty() {
                            prefix
                        } else {
                            format!("{folder}/{prefix}")
                        };
                        folders.insert(full);
                    }
                }
                if recursive || depth == 0 {
                    let mut entry = serde_json::json!({
                        "type": "file",
                        "oid": blob_id(&file.bytes)[..40].to_string(),
                        "size": file.bytes.len(),
                        "path": path,
                    });
                    if file.lfs {
                        entry["lfs"] = serde_json::json!({"oid": blob_id(&file.bytes), "size": file.bytes.len(), "pointerSize": 133});
                    }
                    entries.push(entry);
                }
            }
            if entries.is_empty() && folders.is_empty() {
                return hub_error(StatusCode::NOT_FOUND, "EntryNotFound", "does not exist");
            }
            let mut all: Vec<serde_json::Value> = folders
                .into_iter()
                .map(|path| serde_json::json!({"type": "directory", "oid": "0", "size": 0, "path": path}))
                .collect();
            all.extend(entries);
            let page: Vec<_> = all
                .iter()
                .skip(cursor)
                .take(state.page_size)
                .cloned()
                .collect();
            let mut headers = Vec::new();
            if cursor + state.page_size < all.len() {
                headers.push((
                    "link",
                    format!(
                        "<{}{}?recursive={recursive}&expand=false&cursor={}>; rel=\"next\"",
                        state.hub_base,
                        request.uri().path(),
                        cursor + state.page_size
                    ),
                ));
            }
            response(
                StatusCode::OK,
                &headers,
                serde_json::to_string(&page).expect("JSON"),
            )
        }
        ["api", "datasets", owner, name, "paths-info", commit] => {
            // As on the Hub, a gated dataset's paths are described only to an account with
            // access, while its revisions and tree are public.
            if state
                .repos
                .get(&format!("{owner}/{name}"))
                .is_some_and(|repo| repo.gated)
            {
                return hub_error(
                    StatusCode::UNAUTHORIZED,
                    "GatedRepo",
                    "Access to dataset is restricted.",
                );
            }
            let Some(files) = state
                .repos
                .get(&format!("{owner}/{name}"))
                .and_then(|repo| repo.commits.get(*commit))
            else {
                return hub_error(StatusCode::NOT_FOUND, "RevisionNotFound", "Invalid rev id");
            };
            let body =
                futures::executor::block_on(axum::body::to_bytes(request.into_body(), usize::MAX))
                    .expect("a form body");
            let wanted: Vec<String> = url::form_urlencoded::parse(&body)
                .filter(|(key, _)| key == "paths")
                .map(|(_, value)| value.into_owned())
                .collect();
            let entries: Vec<_> = wanted
                .iter()
                .filter_map(|path| files.get(path).map(|file| (path, file)))
                .map(|(path, file)| serde_json::json!({"type": "file", "oid": blob_id(&file.bytes)[..40].to_string(), "size": file.bytes.len(), "path": path}))
                .collect();
            response(
                StatusCode::OK,
                &[],
                serde_json::to_string(&entries).expect("JSON"),
            )
        }
        ["datasets", owner, name, "resolve", commit, rest @ ..] => {
            let Some(repo) = state.repos.get(&format!("{owner}/{name}")) else {
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
                    "Access to dataset is restricted.",
                );
            }
            let path = rest.join("/");
            let Some(file) = repo.commits.get(*commit).and_then(|files| files.get(&path)) else {
                return hub_error(StatusCode::NOT_FOUND, "EntryNotFound", "Entry not found");
            };
            let linked = [("x-linked-etag", format!("\"{}\"", blob_id(&file.bytes)))];
            if file.lfs {
                let location =
                    format!("{}/blob/{}?Expires=1", state.cdn_base, blob_id(&file.bytes));
                let mut headers = linked.to_vec();
                headers.push(("location", location));
                response(StatusCode::FOUND, &headers, "")
            } else {
                let mut headers = linked.to_vec();
                headers.push((
                    "location",
                    format!("/api/resolve-cache/datasets/{owner}/{name}/{commit}/{path}"),
                ));
                response(StatusCode::TEMPORARY_REDIRECT, &headers, "")
            }
        }
        [
            "api",
            "resolve-cache",
            "datasets",
            owner,
            name,
            commit,
            rest @ ..,
        ] => {
            let path = rest.join("/");
            let Some(file) = state
                .repos
                .get(&format!("{owner}/{name}"))
                .and_then(|repo| repo.commits.get(*commit))
                .and_then(|files| files.get(&path))
            else {
                return hub_error(StatusCode::NOT_FOUND, "EntryNotFound", "Entry not found");
            };
            ranged(&file.bytes, range.as_deref(), &[])
        }
        _ => response(StatusCode::NOT_FOUND, &[], "no such endpoint"),
    };
    let cut_off_left = api_endpoint
        .filter(|_| response.status() == StatusCode::OK)
        .and_then(|endpoint| state.faults.api_truncated.get_mut(endpoint))
        .filter(|left| **left > 0);
    match cut_off_left {
        Some(left) => {
            *left -= 1;
            cut_off(response)
        }
        None => response,
    }
}

/// `response` with its body cut off half way: the first half is flushed before the
/// connection fails, as when a connection is reset part way through a response.
fn cut_off(response: Response<Body>) -> Response<Body> {
    let (parts, body) = response.into_parts();
    let whole = futures::executor::block_on(axum::body::to_bytes(body, usize::MAX))
        .expect("the response body");
    let half = whole.slice(..whole.len() / 2);
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(half) }).chain(
        futures::stream::once(async {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Err(std::io::Error::other("connection reset"))
        }),
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

async fn cdn_handler(State(mock): State<Mock>, request: Request<Body>) -> Response<Body> {
    let mut state = mock.0.lock();
    record(&mut state, true, &request);
    let id = request
        .uri()
        .path()
        .strip_prefix("/blob/")
        .unwrap_or_default()
        .to_string();
    let Some(bytes) = state.blobs.get(&id).cloned() else {
        return response(StatusCode::NOT_FOUND, &[], "no such blob");
    };
    if state.faults.expired > 0 {
        state.faults.expired -= 1;
        return response(StatusCode::FORBIDDEN, &[], "Request has expired");
    }
    let range = request
        .headers()
        .get("range")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    if state.faults.ignore_range {
        return ranged(&bytes, None, &[]);
    }
    if state.faults.chunked && range.is_none() {
        let half = bytes.len() / 2;
        let parts = vec![
            Ok::<_, std::io::Error>(bytes.slice(..half)),
            Ok(bytes.slice(half..)),
        ];
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from_stream(futures::stream::iter(parts)))
            .expect("a well-formed response");
    }
    let full = ranged(&bytes, range.as_deref(), &[]);
    if state.faults.truncated > 0 {
        state.faults.truncated -= 1;
        return cut_off(full);
    }
    full
}

fn rows(range: std::ops::Range<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let ids: Vec<i64> = range.clone().collect();
    let names: Vec<Option<String>> = range
        .clone()
        .map(|id| (id % 7 != 0).then(|| format!("row-{id}")))
        .collect();
    #[expect(clippy::cast_precision_loss)]
    let scores: Vec<Option<f64>> = range
        .map(|id| (id % 5 != 0).then_some(id as f64 / 4.0))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .expect("a valid batch")
}

/// A Parquet file of `batch`, with small row groups so a scan reads it in several ranges.
fn parquet(batch: &RecordBatch) -> File {
    let mut bytes = Vec::new();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(64))
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))
            .expect("a Parquet writer");
    writer.write(batch).expect("the batch is written");
    writer.close().expect("the file is finished");
    File {
        bytes: bytes.into(),
        lfs: true,
    }
}

fn text(content: &str) -> File {
    File {
        bytes: Bytes::from(content.to_string()),
        lfs: false,
    }
}

static COMPONENTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn connector(mock: &Mock, token: Option<&str>, params: &[(&str, &str)]) -> HuggingFace {
    let endpoint = url::Url::parse(&mock.endpoint()).expect("the mock endpoint");
    let hub = Arc::new(
        Hub::new(
            HubConfig {
                endpoint,
                token: token.map(|token| SecretString::from(token.to_string())),
            },
            tokio::runtime::Handle::current(),
        )
        .expect("a Hub client"),
    );
    let params = params
        .iter()
        .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
        .collect();
    // Every test connector stands for a dataset of its own.
    let component = format!(
        "dataset_{}",
        COMPONENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    HuggingFace {
        params: Parameters::new(params, "hf", &PARAMETERS),
        store: Arc::new(HuggingFaceStore::new(hub, &component)),
        io_runtime: tokio::runtime::Handle::current(),
    }
}

/// A session configured as Spice configures its own: among other things, listing a folder
/// includes the files in its subfolders.
fn session() -> SessionContext {
    SessionContext::new_with_config(runtime_datafusion::session_config::get_df_default_config())
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} plans: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql} runs: {e}"))
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    arrow::compute::concat_batches(&schema, batches).expect("batches share a schema")
}

/// Asserts that a query returned exactly the rows of `expected`, in order. Spice reads
/// Parquet strings as `LargeUtf8`, so `expected` is cast to the result's types first: the
/// comparison is of values, and of column names and nullability.
fn assert_rows(batches: &[RecordBatch], expected: &RecordBatch) {
    let actual = concat(batches);
    let columns = expected
        .columns()
        .iter()
        .zip(actual.schema().fields())
        .map(|(column, field)| {
            arrow::compute::cast(column, field.data_type()).expect("castable to the result type")
        })
        .collect();
    let expected = RecordBatch::try_new(actual.schema(), columns).expect("the same shape");
    assert_eq!(actual, expected);
}

/// A dataset over a folder of Parquet files, without `file_format`: the connector infers it,
/// pins each scan to one commit, and follows the branch to a new commit. The rows are diffed
/// against the batches the files were written from.
#[tokio::test(flavor = "multi_thread")]
async fn scans_read_one_commit_and_follow_the_branch() {
    let mock = Mock::start().await;
    mock.commit(
        "o/branch",
        C1,
        vec![
            ("README.md", text("# A dataset\n")),
            ("data/part-0.parquet", parquet(&rows(0..300))),
            ("data/part-1.parquet", parquet(&rows(300..600))),
        ],
    );
    mock.branch("o/branch", "main", C1);

    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new("hf://datasets/o/branch/data/", TableReference::bare("t"));
    mock.clear_seen();
    let table = connector
        .table(&dataset)
        .await
        .expect("the dataset registers");
    let registered = table.schema();

    // A session that has never seen the store: the scan registers it.
    let ctx = session();
    ctx.register_table("t", Arc::clone(&table))
        .expect("the table registers");
    let batches = query(&ctx, "SELECT id, name, score FROM t ORDER BY id").await;
    assert_rows(&batches, &rows(0..600));

    // Every read named C1; across registration (which read a footer to infer the schema) and
    // the scan, each file was resolved once, and its bytes came from the CDN in ranges.
    let seen = mock.seen();
    let resolves: Vec<_> = seen
        .iter()
        .filter(|s| s.path.contains("/resolve/"))
        .collect();
    assert_eq!(resolves.len(), 2, "one resolve per file: {resolves:#?}");
    assert!(
        resolves.iter().all(|s| s.path.contains(C1)),
        "{resolves:#?}"
    );
    let cdn: Vec<_> = seen.iter().filter(|s| s.cdn).collect();
    assert!(cdn.len() > 2, "the scan read ranges from the CDN: {cdn:#?}");
    assert!(cdn.iter().all(|s| s.range.is_some()), "{cdn:#?}");

    // The branch moves: a third file appears and a file is rewritten.
    mock.commit(
        "o/branch",
        C2,
        vec![
            ("README.md", text("# A dataset\n")),
            ("data/part-0.parquet", parquet(&rows(0..300))),
            ("data/part-1.parquet", parquet(&rows(300..450))),
            ("data/part-2.parquet", parquet(&rows(1000..1100))),
        ],
    );
    mock.branch("o/branch", "main", C2);
    connector.store.hub().forget_revisions();
    mock.clear_seen();
    let batches = query(&ctx, "SELECT id, name, score FROM t ORDER BY id").await;
    let expected = concat(&[rows(0..450), rows(1000..1100)]);
    assert_rows(&batches, &expected);
    let seen = mock.seen();
    let reads: Vec<_> = seen
        .iter()
        .filter(|s| s.path.contains("/resolve/") || s.path.contains("/tree/"))
        .collect();
    assert!(!reads.is_empty());
    assert!(
        reads.iter().all(|s| s.path.contains(C2)),
        "after the branch moved, every listing and read names the new commit: {reads:#?}"
    );
    assert_eq!(
        table.schema(),
        registered,
        "the schema is the one the dataset registered with"
    );
}

/// An API response that breaks off part way, as when the connection is reset, is requested
/// again instead of failing the query. A scan of a branch looks the branch up again once its
/// lookup expires and lists a commit it has not listed before, so both requests happen while a
/// query runs, not only while the dataset registers.
#[tokio::test(flavor = "multi_thread")]
async fn api_responses_cut_off_part_way_are_requested_again() {
    let mock = Mock::start().await;
    mock.commit(
        "o/flaky",
        C1,
        vec![("data/part-0.parquet", parquet(&rows(0..300)))],
    );
    mock.branch("o/flaky", "main", C1);
    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new("hf://datasets/o/flaky/data/", TableReference::bare("t"));
    let table = connector
        .table(&dataset)
        .await
        .expect("the dataset registers");
    let ctx = session();
    ctx.register_table("t", table).expect("the table registers");
    assert_rows(
        &query(&ctx, "SELECT id, name, score FROM t ORDER BY id").await,
        &rows(0..300),
    );

    // The branch moves, so the next scan resolves it and lists the new commit. The first
    // response to each request breaks off half way.
    mock.commit(
        "o/flaky",
        C2,
        vec![
            ("data/part-0.parquet", parquet(&rows(0..300))),
            ("data/part-1.parquet", parquet(&rows(300..450))),
        ],
    );
    mock.branch("o/flaky", "main", C2);
    connector.store.hub().forget_revisions();
    {
        let mut state = mock.0.lock();
        state.faults.api_truncated.insert("revision", 1);
        state.faults.api_truncated.insert("tree", 1);
    }
    mock.clear_seen();
    assert_rows(
        &query(&ctx, "SELECT id, name, score FROM t ORDER BY id").await,
        &rows(0..450),
    );

    let seen = mock.seen();
    let requests = |endpoint: &str| {
        seen.iter()
            .filter(|s| s.path.contains(endpoint))
            .map(|s| s.path.clone())
            .collect::<Vec<_>>()
    };
    let lookups = requests("/revision/main");
    assert_eq!(
        lookups.len(),
        2,
        "the cut-off lookup and its retry: {lookups:#?}"
    );
    let listings = requests(&format!("/tree/{C2}/data"));
    assert_eq!(
        listings.len(),
        2,
        "the cut-off listing and its retry: {listings:#?}"
    );
    assert!(
        mock.0
            .lock()
            .faults
            .api_truncated
            .values()
            .all(|left| *left == 0),
        "both responses were cut off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_file_and_a_glob_select_their_files() {
    let mock = Mock::start().await;
    mock.commit(
        "o/files",
        C1,
        vec![
            ("data/train-0.parquet", parquet(&rows(0..10))),
            ("data/train-1.parquet", parquet(&rows(10..20))),
            ("data/test-0.parquet", parquet(&rows(100..105))),
        ],
    );
    mock.branch("o/files", "main", C1);
    let connector = connector(&mock, None, &[]);
    let ctx = session();

    let file = DatasetSpec::new(
        "hf://datasets/o/files/data/test-0.parquet",
        TableReference::bare("test"),
    );
    ctx.register_table(
        "test",
        connector.table(&file).await.expect("a file registers"),
    )
    .expect("registered");
    let glob = DatasetSpec::new(
        "hf://datasets/o/files/data/train-*.parquet",
        TableReference::bare("train"),
    );
    ctx.register_table(
        "train",
        connector.table(&glob).await.expect("a glob registers"),
    )
    .expect("registered");

    assert_rows(
        &query(&ctx, "SELECT * FROM test ORDER BY id").await,
        &rows(100..105),
    );
    assert_rows(
        &query(&ctx, "SELECT * FROM train ORDER BY id").await,
        &rows(0..20),
    );
}

/// A glob infers its schema from a file it selects, not from any file in its folder.
#[tokio::test(flavor = "multi_thread")]
async fn a_glob_infers_its_schema_from_the_files_it_selects() {
    let mock = Mock::start().await;
    mock.commit(
        "o/globschema",
        C1,
        vec![
            ("data/train-0.csv", text("id,name\n1,one\n2,two\n")),
            ("data/train-1.csv", text("id,name\n3,three\n")),
            // Every file of a commit has the commit's date, and a folder-wide inference keeps
            // the first of equally new files: this one, listed first.
            ("data/aa-other.csv", text("x,y,z\n7,8,9\n")),
        ],
    );
    mock.branch("o/globschema", "main", C1);
    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new(
        "hf://datasets/o/globschema/data/train-*.csv",
        TableReference::bare("t"),
    );
    let table = connector.table(&dataset).await.expect("registers");
    let names: Vec<_> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["id", "name"]);
    let ctx = session();
    ctx.register_table("t", table).expect("registered");
    let batches = query(&ctx, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("printable")
            .to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 1  | one   |\n| 2  | two   |\n| 3  | three |\n+----+-------+"
    );
}

/// CSV is read by column position, so a commit whose files reorder the columns must fail the
/// scan rather than put values under the wrong names; a commit with the same columns reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_csv_commit_with_reordered_columns_fails_instead_of_misreading() {
    let mock = Mock::start().await;
    mock.commit(
        "o/csvmove",
        C1,
        vec![("data/a.csv", text("id,name,city\n1,ann,oslo\n"))],
    );
    mock.branch("o/csvmove", "main", C1);
    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new("hf://datasets/o/csvmove/data/", TableReference::bare("t"));
    let ctx = session();
    ctx.register_table("t", connector.table(&dataset).await.expect("registers"))
        .expect("registered");
    let sql = "SELECT id, name, city FROM t ORDER BY id";
    let printed = |batches: &[RecordBatch]| {
        arrow::util::pretty::pretty_format_batches(batches)
            .expect("printable")
            .to_string()
    };
    assert_eq!(
        printed(&query(&ctx, sql).await),
        "+----+------+------+\n| id | name | city |\n+----+------+------+\n| 1  | ann  | oslo |\n+----+------+------+"
    );

    // The branch moves to files whose columns are reordered.
    mock.commit(
        "o/csvmove",
        C2,
        vec![("data/a.csv", text("id,city,name\n2,rome,bob\n"))],
    );
    mock.branch("o/csvmove", "main", C2);
    connector.store.hub().forget_revisions();
    let error = ctx
        .sql(sql)
        .await
        .expect("plans")
        .collect()
        .await
        .expect_err("reordered columns");
    assert!(
        error.to_string().contains(
            "File 'data/a.csv' of Hugging Face dataset 'o/csvmove' at commit 2222222222222222222222222222222222222222 has columns (id, city, name), but the dataset's columns are (id, name, city). CSV and TSV files are read by column position"
        ),
        "{error}"
    );

    // A later commit with the registered columns reads again.
    mock.commit(
        "o/csvmove",
        C3,
        vec![("data/a.csv", text("id,name,city\n3,cy,lima\n"))],
    );
    mock.branch("o/csvmove", "main", C3);
    connector.store.hub().forget_revisions();
    assert_eq!(
        printed(&query(&ctx, sql).await),
        "+----+------+------+\n| id | name | city |\n+----+------+------+\n| 3  | cy   | lima |\n+----+------+------+"
    );
}

/// Every CSV file a location selects must have the dataset's columns in order, not just the
/// one the schema is inferred from: they are all read by position.
#[tokio::test(flavor = "multi_thread")]
async fn csv_files_whose_columns_disagree_fail_registration() {
    let mock = Mock::start().await;
    mock.commit(
        "o/csvmixed",
        C1,
        vec![
            ("data/a.csv", text("id,name,city\n1,bob,rome\n")),
            // Neither the file the schema is inferred from nor the last one.
            ("data/m.csv", text("id,city,name\n3,lima,cy\n")),
            ("data/z.csv", text("id,name,city\n2,ann,oslo\n")),
        ],
    );
    mock.branch("o/csvmixed", "main", C1);
    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new("hf://datasets/o/csvmixed/data/", TableReference::bare("t"));
    let message = connector
        .table(&dataset)
        .await
        .expect_err("a file's columns differ")
        .to_string();
    assert!(
        message.starts_with(
            "Cannot setup the dataset t (hf) with an invalid configuration. File 'data/m.csv' of Hugging Face dataset 'o/csvmixed' at commit 1111111111111111111111111111111111111111 has columns (id, city, name), but the dataset's columns are (id, name, city)."
        ),
        "{message}"
    );
    assert!(
        message.contains(
            "CSV and TSV files are read by column position, so the file cannot be read with the dataset's columns."
        ),
        "{message}"
    );

    // Files with the same columns register and read.
    mock.commit(
        "o/csvmixed",
        C2,
        vec![
            ("data/a.csv", text("id,name,city\n1,bob,rome\n")),
            ("data/z.csv", text("id,name,city\n2,ann,oslo\n")),
        ],
    );
    mock.branch("o/csvmixed", "main", C2);
    connector.store.hub().forget_revisions();
    let ctx = session();
    ctx.register_table("t", connector.table(&dataset).await.expect("registers"))
        .expect("registered");
    let batches = query(&ctx, "SELECT id, name, city FROM t ORDER BY id").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("printable")
            .to_string(),
        "+----+------+------+\n| id | name | city |\n+----+------+------+\n| 1  | bob  | rome |\n| 2  | ann  | oslo |\n+----+------+------+"
    );
}

/// A quoted column name may span lines: the header check reads the whole first record.
#[tokio::test(flavor = "multi_thread")]
async fn a_quoted_multiline_column_name_is_one_column() {
    let mock = Mock::start().await;
    let csv = "id,\"first\nname\"\n1,ann\n";
    mock.commit(
        "o/csvquoted",
        C1,
        vec![("data/a.csv", text(csv)), ("data/b.csv", text(csv))],
    );
    mock.branch("o/csvquoted", "main", C1);
    let connector = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new("hf://datasets/o/csvquoted/data/", TableReference::bare("t"));
    let table = connector
        .table(&dataset)
        .await
        .expect("the header check passes");
    let names: Vec<_> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["id", "first\nname"]);
}

/// Datasets read with different tokens or endpoints never share a client: each configuration
/// has its own store, under its own URL.
#[tokio::test(flavor = "multi_thread")]
async fn each_token_reads_through_its_own_store() {
    let public = HubConfig {
        endpoint: url::Url::parse("https://huggingface.co").expect("a URL"),
        token: None,
    };
    assert_eq!(store_url(&public, "a").as_str(), "hf://datasets/");
    assert_eq!(store_url(&public, "b").as_str(), "hf://datasets/");
    let with_token = |token: &str| HubConfig {
        token: Some(SecretString::from(token.to_string())),
        ..public.clone()
    };
    let one = store_url(&with_token("hf_one"), "one");
    assert!(one.as_str().starts_with("hf://datasets."), "{one}");
    assert_ne!(one, store_url(&with_token("hf_two"), "two"));
    // Derived from the endpoint and the dataset, never the token: the same on every executor
    // and after a token is rotated, and nothing a URL shows can be traced to the token.
    assert_eq!(one, store_url(&with_token("hf_rotated"), "one"));
    let mirror = HubConfig {
        endpoint: url::Url::parse("https://hf-mirror.example").expect("a URL"),
        token: None,
    };
    assert_ne!(store_url(&mirror, "a"), store_url(&public, "a"));
    assert_ne!(store_url(&mirror, "a"), store_url(&mirror, "b"));

    // Two datasets on one private repository, each with its own valid token: each reads with
    // its own, whichever registered last.
    let mock = Mock::start().await;
    mock.commit("o/shared", C1, vec![("a.parquet", parquet(&rows(0..20)))]);
    mock.branch("o/shared", "main", C1);
    mock.0.lock().tokens = vec!["hf_one".to_string(), "hf_two".to_string()];
    let one = connector(&mock, Some("hf_one"), &[]);
    let two = connector(&mock, Some("hf_two"), &[]);
    assert_ne!(one.store.url(), two.store.url());
    let dataset = DatasetSpec::new(
        "hf://datasets/o/shared/a.parquet",
        TableReference::bare("t"),
    );
    let ctx = session();
    ctx.register_table("one", one.table(&dataset).await.expect("registers"))
        .expect("registered");
    ctx.register_table("two", two.table(&dataset).await.expect("registers"))
        .expect("registered");
    for (table, token) in [("one", "hf_one"), ("two", "hf_two"), ("one", "hf_one")] {
        one.store.hub().forget_revisions();
        two.store.hub().forget_revisions();
        mock.clear_seen();
        assert_rows(
            &query(&ctx, &format!("SELECT * FROM {table} ORDER BY id")).await,
            &rows(0..20),
        );
        let bearer = format!("Bearer {token}");
        let hub_requests: Vec<_> = mock.seen().into_iter().filter(|s| !s.cdn).collect();
        assert!(!hub_requests.is_empty(), "{table} resolved its revision");
        assert!(
            hub_requests
                .iter()
                .all(|s| s.authorization.as_deref() == Some(bearer.as_str())),
            "{table} must read with {token}: {hub_requests:#?}"
        );
    }
}

/// A revision or a dataset name with a dot is not a file extension: the format of a whole
/// repository is inferred from its files.
#[tokio::test(flavor = "multi_thread")]
async fn dots_in_a_revision_or_name_are_not_file_extensions() {
    let mock = Mock::start().await;
    mock.commit(
        "o/my.data",
        C1,
        vec![
            ("README.md", text("# A dataset\n")),
            ("data/part-0.parquet", parquet(&rows(0..5))),
        ],
    );
    mock.branch("o/my.data", "v1.0", C1);
    mock.branch("o/my.data", "release.csv", C1);
    mock.commit(
        "o/csvtag",
        C1,
        vec![("data/a.csv", text("id,name\n1,one\n"))],
    );
    mock.branch("o/csvtag", "v1.gz", C1);
    let inferred = connector(&mock, None, &[]);
    let named = connector(&mock, None, &[("file_format", "parquet")]);
    let ctx = session();
    for (name, from, connector) in [
        ("tagged", "hf://datasets/o/my.data@v1.0", &inferred),
        (
            "csv_named",
            "hf://datasets/o/my.data@release.csv/",
            &inferred,
        ),
        ("tagged_parquet", "hf://datasets/o/my.data@v1.0", &named),
    ] {
        let dataset = DatasetSpec::new(from, TableReference::bare(name));
        ctx.register_table(
            name,
            connector
                .table(&dataset)
                .await
                .unwrap_or_else(|e| panic!("{from}: {e}")),
        )
        .expect("registered");
        assert_rows(
            &query(&ctx, &format!("SELECT * FROM {name} ORDER BY id")).await,
            &rows(0..5),
        );
    }

    // A tag that looks like a compression suffix does not make plain CSV read as gzip.
    let csv = connector(&mock, None, &[("file_format", "csv")]);
    let dataset = DatasetSpec::new("hf://datasets/o/csvtag@v1.gz", TableReference::bare("c"));
    ctx.register_table("c", csv.table(&dataset).await.expect("registers"))
        .expect("registered");
    let batches = query(&ctx, "SELECT id, name FROM c").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("printable")
            .to_string(),
        "+----+------+\n| id | name |\n+----+------+\n| 1  | one  |\n+----+------+"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_and_jsonl_files_are_read_through_the_hub_cache() {
    let mock = Mock::start().await;
    mock.commit(
        "o/text",
        C1,
        vec![
            ("csv/a.csv", text("id,name\n1,one\n2,\n3,three\n")),
            (
                "jsonl/a.jsonl",
                text("{\"id\":1,\"name\":\"one\"}\n{\"id\":2,\"name\":null}\n"),
            ),
        ],
    );
    mock.branch("o/text", "main", C1);
    let connector = connector(&mock, None, &[]);
    let ctx = session();
    for (name, from) in [
        ("c", "hf://datasets/o/text/csv/"),
        ("j", "hf://datasets/o/text/jsonl/"),
    ] {
        let dataset = DatasetSpec::new(from, TableReference::bare(name));
        ctx.register_table(name, connector.table(&dataset).await.expect("registers"))
            .expect("registered");
    }
    let csv = query(&ctx, "SELECT id, name FROM c ORDER BY id").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&csv)
            .expect("printable")
            .to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 1  | one   |\n| 2  |       |\n| 3  | three |\n+----+-------+"
    );
    let jsonl = query(&ctx, "SELECT id, name FROM j ORDER BY id").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&jsonl)
            .expect("printable")
            .to_string(),
        "+----+------+\n| id | name |\n+----+------+\n| 1  | one  |\n| 2  |      |\n+----+------+"
    );
    // Small files are served by the Hub's own `resolve-cache`, never the CDN.
    assert!(mock.seen().iter().all(|s| !s.cdn));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_of_mixed_formats_asks_for_a_narrower_location() {
    let mock = Mock::start().await;
    mock.commit(
        "o/mixed",
        C1,
        vec![
            ("data/a.csv", text("id\n1\n")),
            ("data/b.parquet", parquet(&rows(0..1))),
            ("docs/readme.md", text("hi")),
        ],
    );
    mock.branch("o/mixed", "main", C1);
    let connector = connector(&mock, None, &[]);

    let mixed = DatasetSpec::new("hf://datasets/o/mixed/data/", TableReference::bare("t"));
    let error = connector.table(&mixed).await.expect_err("two formats");
    assert_eq!(
        error.to_string(),
        "Cannot setup the dataset t (hf) with an invalid configuration. 'data' in dataset 'o/mixed' holds files of more than one format (.csv, .parquet). Narrow `from` to one format's files, for example with a glob such as '*.parquet', or set `file_format`. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );

    let docs = DatasetSpec::new("hf://datasets/o/mixed/docs/", TableReference::bare("t"));
    let error = connector.table(&docs).await.expect_err("no data files");
    assert_eq!(
        error.to_string(),
        "Cannot setup the dataset t (hf) with an invalid configuration. No Parquet, CSV, TSV, JSON or ORC files were found under 'docs' in dataset 'o/mixed' (found .md). Point `from` at the dataset's data files, or read the Hub's Parquet conversion of it with '@~parquet'. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );

    mock.commit("o/mixed", C2, vec![("data/raw", text("id\n1\n"))]);
    mock.branch("o/mixed", "next", C2);
    let raw = DatasetSpec::new(
        "hf://datasets/o/mixed@next/data/raw",
        TableReference::bare("t"),
    );
    assert_eq!(
        connector
            .table(&raw)
            .await
            .expect_err("no extension")
            .to_string(),
        "Cannot setup the dataset t (hf) with an invalid configuration. 'data/raw' in dataset 'o/mixed' has no file extension to infer its format from. Set `file_format` to parquet, csv, tsv, json, jsonl or orc. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );
    let folder = DatasetSpec::new(
        "hf://datasets/o/mixed@next/data/",
        TableReference::bare("t"),
    );
    assert_eq!(
        connector
            .table(&folder)
            .await
            .expect_err("no data files")
            .to_string(),
        "Cannot setup the dataset t (hf) with an invalid configuration. No Parquet, CSV, TSV, JSON or ORC files were found under 'data' in dataset 'o/mixed' (found files without an extension). Point `from` at the dataset's data files, or read the Hub's Parquet conversion of it with '@~parquet'. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );

    // `file_format` settles it.
    let connector = self::connector(&mock, None, &[("file_format", "csv")]);
    let ctx = session();
    ctx.register_table("t", connector.table(&mixed).await.expect("csv registers"))
        .expect("registered");
    let batches = query(&ctx, "SELECT count(*) AS n FROM t").await;
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("printable")
            .to_string(),
        "+---+\n| n |\n+---+\n| 1 |\n+---+"
    );
}

async fn registration_error(connector: &HuggingFace, from: &str) -> String {
    let dataset = DatasetSpec::new(from, TableReference::bare("t"));
    connector.table(&dataset).await.expect_err(from).to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn registration_errors_name_the_dataset_and_the_fix() {
    let mock = Mock::start().await;
    mock.commit("o/errors", C1, vec![("a.parquet", parquet(&rows(0..1)))]);
    mock.branch("o/errors", "main", C1);
    mock.commit("o/gated", C1, vec![("a.parquet", parquet(&rows(0..1)))]);
    mock.branch("o/gated", "main", C1);
    mock.0
        .lock()
        .repos
        .get_mut("o/gated")
        .expect("the repo")
        .gated = true;
    let connector = connector(&mock, None, &[]);
    let error = |from: &'static str| registration_error(&connector, from);

    assert_eq!(
        error("hf://datasets/o/missing").await,
        "Cannot setup the dataset t (hf) with an invalid configuration. Hugging Face dataset 'o/missing' was not found or it is private: a private dataset needs `hf_token`. Check the owner and dataset name in `from`. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );
    assert_eq!(
        error("hf://datasets/o/errors@nope").await,
        "Cannot setup the dataset t (hf) with an invalid configuration. Revision 'nope' does not exist in Hugging Face dataset 'o/errors'. Use an existing branch, tag or commit after '@' in `from`, or remove '@nope' to read 'main'."
    );
    let endpoint = mock.endpoint();
    assert_eq!(
        error("hf://datasets/o/gated/a.parquet").await,
        format!(
            "Insufficient permissions to access the dataset t (hf). Hugging Face dataset 'o/gated' is gated and no `hf_token` is set: accept its access conditions at {endpoint}/datasets/o/gated with the account `hf_token` belongs to. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
        )
    );
    // A gated folder: its tree is public, its files are not.
    assert_eq!(
        error("hf://datasets/o/gated/").await,
        format!(
            "Insufficient permissions to access the dataset t (hf). Hugging Face dataset 'o/gated' is gated and no `hf_token` is set: accept its access conditions at {endpoint}/datasets/o/gated with the account `hf_token` belongs to. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
        )
    );
    assert_eq!(
        error("hf://datasets/o").await,
        "Cannot setup the dataset t (hf) with an invalid configuration. \"hf://datasets/o\" does not name a dataset repository. Use a location like 'hf://datasets/<owner>/<dataset>', for example 'hf://datasets/o/<dataset>'. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_token_is_sent_to_the_hub_and_never_to_the_cdn() {
    let mock = Mock::start().await;
    mock.commit("o/private", C1, vec![("a.parquet", parquet(&rows(0..50)))]);
    mock.branch("o/private", "main", C1);
    mock.0.lock().token = Some(TOKEN.to_string());

    let anonymous = connector(&mock, None, &[]);
    let dataset = DatasetSpec::new(
        "hf://datasets/o/private/a.parquet",
        TableReference::bare("t"),
    );
    assert_eq!(
        anonymous
            .table(&dataset)
            .await
            .expect_err("no token")
            .to_string(),
        "Cannot setup the dataset t (hf) with an invalid configuration. Hugging Face dataset 'o/private' was not found or it is private: a private dataset needs `hf_token`. Check the owner and dataset name in `from`. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );

    let wrong = connector(&mock, Some("hf_wrong"), &[]);
    assert_eq!(
        wrong
            .table(&dataset)
            .await
            .expect_err("a wrong token")
            .to_string(),
        "Insufficient permissions to access the dataset t (hf). The Hugging Face Hub rejected `hf_token` for dataset 'o/private' (HTTP 401). Check that the token is valid and grants read access to the dataset. See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
    );

    let authorized = connector(&mock, Some(TOKEN), &[]);
    mock.clear_seen();
    let ctx = session();
    ctx.register_table("t", authorized.table(&dataset).await.expect("registers"))
        .expect("registered");
    assert_rows(
        &query(&ctx, "SELECT * FROM t ORDER BY id").await,
        &rows(0..50),
    );

    let seen = mock.seen();
    let (cdn, hub): (Vec<_>, Vec<_>) = seen.iter().partition(|s| s.cdn);
    assert!(!cdn.is_empty() && !hub.is_empty(), "{seen:#?}");
    let bearer = format!("Bearer {TOKEN}");
    assert!(
        hub.iter()
            .all(|s| s.authorization.as_deref() == Some(bearer.as_str())),
        "{hub:#?}"
    );
    assert!(cdn.iter().all(|s| s.authorization.is_none()), "{cdn:#?}");
}

/// The store, below `DataFusion`: ranges, resumption and the faults a reader must not read
/// past.
mod store_reads {
    use super::*;

    fn registered(mock: &Mock, _repo: &str) -> (Arc<HuggingFaceStore>, Arc<Hub>) {
        let connector = connector(mock, None, &[]);
        let hub = Arc::clone(connector.store.hub());
        (connector.store, hub)
    }

    fn file_bytes() -> Bytes {
        Bytes::from(
            (0..10_000u32)
                .flat_map(u32::to_le_bytes)
                .collect::<Vec<u8>>(),
        )
    }

    fn path(repo: &str, file: &str) -> Path {
        Path::parse(format!("{repo}@{C1}/{file}")).expect("a valid path")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ranges_suffixes_and_offsets_return_exactly_their_bytes() {
        let mock = Mock::start().await;
        let bytes = file_bytes();
        mock.commit(
            "r/ranges",
            C1,
            vec![(
                "blob.bin",
                File {
                    bytes: bytes.clone(),
                    lfs: true,
                },
            )],
        );
        let (store, _) = registered(&mock, "r/ranges");
        let location = path("r/ranges", "blob.bin");

        assert_eq!(
            store
                .get_range(&location, 100..4_100)
                .await
                .expect("a range"),
            bytes.slice(100..4_100)
        );
        let suffix = store
            .get_opts(
                &location,
                object_store::GetOptions {
                    range: Some(object_store::GetRange::Suffix(8)),
                    ..Default::default()
                },
            )
            .await
            .expect("a suffix");
        assert_eq!(suffix.range, 39_992..40_000);
        assert_eq!(suffix.bytes().await.expect("bytes"), bytes.slice(39_992..));
        let offset = store
            .get_opts(
                &location,
                object_store::GetOptions {
                    range: Some(object_store::GetRange::Offset(39_000)),
                    ..Default::default()
                },
            )
            .await
            .expect("an offset");
        assert_eq!(offset.bytes().await.expect("bytes"), bytes.slice(39_000..));
        assert_eq!(
            store
                .get(&location)
                .await
                .expect("the file")
                .bytes()
                .await
                .expect("bytes"),
            bytes
        );
        assert_eq!(
            store
                .get_range(&location, 7..7)
                .await
                .expect("an empty range"),
            Bytes::new()
        );
        let meta = store.head(&location).await.expect("the file's description");
        assert_eq!(meta.size, 40_000);
        assert_eq!(meta.version.as_deref(), Some(C1));
        assert!(matches!(
            store.head(&path("r/ranges", "missing.bin")).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_download_cut_off_part_way_resumes_where_it_stopped() {
        let mock = Mock::start().await;
        let bytes = file_bytes();
        mock.commit(
            "r/resume",
            C1,
            vec![(
                "blob.bin",
                File {
                    bytes: bytes.clone(),
                    lfs: true,
                },
            )],
        );
        let (store, _) = registered(&mock, "r/resume");
        mock.0.lock().faults.truncated = 1;
        mock.clear_seen();

        let read = store
            .get_range(&path("r/resume", "blob.bin"), 1_000..21_000)
            .await
            .expect("the range");
        assert_eq!(read, bytes.slice(1_000..21_000));
        let ranges: Vec<_> = mock
            .seen()
            .iter()
            .filter(|s| s.cdn)
            .filter_map(|s| s.range.clone())
            .collect();
        assert_eq!(ranges.first().map(String::as_str), Some("bytes=1000-20999"));
        assert_eq!(ranges.len(), 2, "one resumed request: {ranges:?}");
        let resumed = ranges[1]
            .strip_prefix("bytes=")
            .and_then(|r| r.split_once('-'))
            .expect("a range");
        assert!(
            resumed.0.parse::<u64>().expect("a start") > 1_000,
            "{ranges:?}"
        );
        assert_eq!(resumed.1, "20999");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_whole_file_without_content_length_is_sized_from_its_description() {
        let mock = Mock::start().await;
        let bytes = file_bytes();
        mock.commit(
            "r/chunked",
            C1,
            vec![(
                "blob.bin",
                File {
                    bytes: bytes.clone(),
                    lfs: true,
                },
            )],
        );
        let (store, _) = registered(&mock, "r/chunked");
        mock.0.lock().faults.chunked = true;
        let read = store
            .get(&path("r/chunked", "blob.bin"))
            .await
            .expect("a whole-file read");
        assert_eq!(read.meta.size, 40_000);
        assert_eq!(read.bytes().await.expect("the bytes"), bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_server_that_ignores_range_is_an_error_not_wrong_bytes() {
        let mock = Mock::start().await;
        mock.commit(
            "r/ignore",
            C1,
            vec![(
                "blob.bin",
                File {
                    bytes: file_bytes(),
                    lfs: true,
                },
            )],
        );
        let (store, _) = registered(&mock, "r/ignore");
        mock.0.lock().faults.ignore_range = true;
        let error = store
            .get_range(&path("r/ignore", "blob.bin"), 10..20)
            .await
            .expect_err("a full response to a ranged read");
        assert!(
            error.to_string().contains("a read of bytes=10-19 of 'blob.bin' returned HTTP 200 OK instead of the requested range"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_expired_cdn_url_is_resolved_again() {
        let mock = Mock::start().await;
        let bytes = file_bytes();
        mock.commit(
            "r/expired",
            C1,
            vec![(
                "blob.bin",
                File {
                    bytes: bytes.clone(),
                    lfs: true,
                },
            )],
        );
        let (store, _) = registered(&mock, "r/expired");
        let location = path("r/expired", "blob.bin");
        assert_eq!(
            store.get_range(&location, 0..10).await.expect("first read"),
            bytes.slice(0..10)
        );
        mock.0.lock().faults.expired = 1;
        mock.clear_seen();
        assert_eq!(
            store
                .get_range(&location, 10..20)
                .await
                .expect("after expiry"),
            bytes.slice(10..20)
        );
        let seen = mock.seen();
        assert_eq!(
            seen.iter().filter(|s| s.path.contains("/resolve/")).count(),
            1,
            "{seen:#?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rate_limited_lookups_are_retried() {
        let mock = Mock::start().await;
        mock.commit("r/limited", C1, vec![("a.csv", text("id\n1\n"))]);
        mock.branch("r/limited", "main", C1);
        let (_, hub) = registered(&mock, "r/limited");
        mock.0.lock().faults.rate_limited = 2;
        let repo = RepoId::new("r", "limited").expect("a valid repository");
        let commit = hub
            .commit(&repo, "main")
            .await
            .expect("the third attempt succeeds");
        assert_eq!(commit.sha, C1);
        let lookups = mock
            .seen()
            .iter()
            .filter(|s| s.path.contains("/revision/"))
            .count();
        assert_eq!(lookups, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_listing_whose_later_page_is_missing_is_an_error_not_a_shorter_listing() {
        let mock = Mock::start().await;
        let files: Vec<(String, File)> = (0..9)
            .map(|i| (format!("data/f-{i}.csv"), text("id\n1\n")))
            .collect();
        mock.commit(
            "r/cut",
            C1,
            files.iter().map(|(p, f)| (p.as_str(), f.clone())).collect(),
        );
        {
            let mut state = mock.0.lock();
            state.page_size = 4;
            state.faults.missing_later_pages = true;
        }
        let (store, _) = registered(&mock, "r/cut");
        let listed =
            futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&path("r/cut", "data"))))
                .await;
        assert!(
            matches!(listed, Err(object_store::Error::NotFound { .. })),
            "a listing cut short must fail: {listed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_listing_follows_every_page() {
        let mock = Mock::start().await;
        let files: Vec<(String, File)> = (0..25)
            .map(|i| (format!("data/f-{i:02}.csv"), text("id\n1\n")))
            .collect();
        mock.commit(
            "r/paged",
            C1,
            files.iter().map(|(p, f)| (p.as_str(), f.clone())).collect(),
        );
        mock.0.lock().page_size = 4;
        let (store, _) = registered(&mock, "r/paged");
        mock.clear_seen();
        let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
            store.list(Some(&path("r/paged", "data"))),
        )
        .await
        .expect("the listing");
        let mut names: Vec<_> = listed
            .iter()
            .map(|meta| meta.location.to_string())
            .collect();
        names.sort();
        let expected: Vec<_> = files
            .iter()
            .map(|(p, _)| format!("r/paged@{C1}/{p}"))
            .collect();
        assert_eq!(names, expected);
        // 1 folder + 25 files, 4 per page.
        assert_eq!(
            mock.seen()
                .iter()
                .filter(|s| s.path.contains("/tree/"))
                .count(),
            7
        );
        // A second listing of the same commit is served from the cache.
        let again = futures::TryStreamExt::try_collect::<Vec<_>>(
            store.list(Some(&path("r/paged", "data"))),
        )
        .await
        .expect("the listing");
        assert_eq!(again.len(), 25);
        assert_eq!(
            mock.seen()
                .iter()
                .filter(|s| s.path.contains("/tree/"))
                .count(),
            7
        );
    }
}

#[test]
fn hub_config_debug_redacts_the_token() {
    let config = HubConfig {
        endpoint: url::Url::parse("https://huggingface.co").expect("a URL"),
        token: Some(SecretString::from("hf_secret_value".to_string())),
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("hf_secret_value"), "{debug}");
    assert!(debug.contains("[REDACTED]"), "{debug}");
}

#[test]
fn endpoints_must_be_https_unless_loopback() {
    for valid in [
        "https://huggingface.co",
        "https://hf-mirror.com/",
        "http://127.0.0.1:8080",
        "http://localhost:9000",
        "http://[::1]:80",
    ] {
        assert!(crate::parse_endpoint(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "http://huggingface.co",
        "ftp://huggingface.co",
        "huggingface.co",
        "https://user:pass@huggingface.co",
        "https://huggingface.co/?x=1",
    ] {
        assert_eq!(
            crate::parse_endpoint(invalid)
                .expect_err(invalid)
                .to_string(),
            format!(
                "`hf_endpoint` {invalid:?} is not a valid endpoint: use an https:// URL such as 'https://huggingface.co' (http:// is accepted only for a loopback address). See: https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md"
            ),
            "{invalid}"
        );
    }
}
