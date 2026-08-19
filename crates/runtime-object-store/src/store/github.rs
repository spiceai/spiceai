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

#![allow(clippy::missing_errors_doc)]

use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::TimeZone;
use futures::{StreamExt, stream::BoxStream};
use http::{
    HeaderMap, HeaderValue,
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
};
use object_store::{
    ClientOptions, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    client::SpawnedReqwestConnector,
    http::{HttpBuilder, HttpStore},
    path::Path,
};
use serde::Deserialize;
use snafu::prelude::*;
use tokio::runtime::Handle;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An internal error occured while connecting to GitHub to download files. {source}"
    ))]
    HttpBuilderFailed { source: object_store::Error },

    #[snafu(display("An invalid GitHub token was provided."))]
    InvalidToken,

    // `{value:?}` debug-formats the untrusted input so control characters
    // (e.g. a newline) are escaped rather than echoed verbatim, keeping the
    // message single-line and preventing log injection.
    #[snafu(display(
        "Invalid GitHub {component} {value:?}: only letters, digits, '.', '_', '-' (and '/' for a revision) are allowed, and '..' is not permitted."
    ))]
    InvalidComponent {
        component: &'static str,
        value: String,
    },
}

/// The two GitHub surfaces this store talks to: raw file content, and the REST
/// API used for listing (the raw endpoint cannot list a directory).
///
/// These are separate from the org/repo/rev because the *bases* are what a test
/// redirects at a local server, while the path built on top of them stays the
/// production path — so coverage asserts this store's behaviour rather than
/// GitHub's availability (spiceai/spiceai#13206).
#[derive(Debug, Clone, PartialEq, Eq)]
struct GitHubEndpoints {
    raw_base: String,
    api_base: String,
    /// Whether the raw store may talk plaintext HTTP. Only a loopback stub in
    /// this module's tests sets this; `github_com` pins it off, so no production
    /// path can be talked into sending a token over cleartext.
    allow_http: bool,
}

impl GitHubEndpoints {
    /// The public GitHub endpoints, used by every non-test caller.
    fn github_com() -> Self {
        Self {
            raw_base: "https://raw.githubusercontent.com".to_string(),
            api_base: "https://api.github.com".to_string(),
            allow_http: false,
        }
    }

    /// The revision-scoped prefix every raw file path is resolved against.
    fn raw_url(&self, org: &str, repo: &str, rev: &str) -> String {
        format!("{}/{org}/{repo}/{rev}", self.raw_base)
    }

    /// The recursive git-tree endpoint backing `list`.
    fn git_tree_url(&self, org: &str, repo: &str, rev: &str) -> String {
        format!(
            "{}/repos/{org}/{repo}/git/trees/{rev}?recursive=true",
            self.api_base
        )
    }
}

struct GitHubClientConfig {
    org: String,
    repo: String,
    rev: String,
    token: Option<String>,
    endpoints: GitHubEndpoints,
}

impl std::fmt::Debug for GitHubClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the GitHub token so a `{:?}` of this config cannot leak it.
        f.debug_struct("GitHubClientConfig")
            .field("org", &self.org)
            .field("repo", &self.repo)
            .field("rev", &self.rev)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("endpoints", &self.endpoints)
            .finish()
    }
}

impl GitHubClientConfig {
    fn new(
        org: impl Display,
        repo: impl Display,
        rev: impl Display,
        token: Option<&str>,
        endpoints: GitHubEndpoints,
    ) -> Self {
        Self {
            org: org.to_string(),
            repo: repo.to_string(),
            rev: rev.to_string(),
            token: token.map(ToString::to_string),
            endpoints,
        }
    }
}

/// Validates a user-supplied GitHub URL component (organization, repository, or
/// revision) before it is interpolated into a `raw.githubusercontent.com` or
/// GitHub API URL. Rejects characters that could change the request target —
/// path traversal (`..`), query/fragment injection (`?`, `#`), userinfo/host
/// confusion (`@`), percent-encoding (`%`), whitespace, and control characters.
///
/// `allow_slash` permits `/` so revision refs like `feature/foo` are accepted;
/// organizations and repositories must not contain `/`.
fn validate_github_component(
    component: &'static str,
    value: &str,
    allow_slash: bool,
) -> Result<(), Error> {
    let valid = !value.is_empty()
        && !value.contains("..")
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') || (allow_slash && c == '/')
        });

    if valid {
        Ok(())
    } else {
        InvalidComponentSnafu {
            component,
            value: value.to_string(),
        }
        .fail()
    }
}

/// An implementation of the `ObjectStore` trait for raw.githubusercontent.com
///
/// This is logically a small wrapper on the existing HTTP Object Store, but just constrained to specific GitHub URLs
#[derive(Debug)]
pub struct GitHubRawObjectStore {
    http_store: HttpStore,
    config: Arc<GitHubClientConfig>,
}

impl GitHubRawObjectStore {
    pub fn try_new(
        org: impl Display,
        repo: impl Display,
        rev: impl Display,
        token: Option<&str>,
        io_runtime: Handle,
    ) -> Result<Self, Error> {
        Self::try_new_with_endpoints(
            org,
            repo,
            rev,
            token,
            io_runtime,
            GitHubEndpoints::github_com(),
        )
    }

    /// `try_new`, with the GitHub base URLs supplied rather than hardcoded.
    ///
    /// Every caller outside this module's tests goes through `try_new`; the
    /// paths built on top of the bases, the component validation and the header
    /// wiring are shared, so a test that redirects the bases still exercises
    /// this constructor rather than a copy of it.
    fn try_new_with_endpoints(
        org: impl Display,
        repo: impl Display,
        rev: impl Display,
        token: Option<&str>,
        io_runtime: Handle,
        endpoints: GitHubEndpoints,
    ) -> Result<Self, Error> {
        let org = org.to_string();
        let repo = repo.to_string();
        let rev = rev.to_string();
        validate_github_component("organization", &org, false)?;
        validate_github_component("repository", &repo, false)?;
        validate_github_component("revision", &rev, true)?;

        let mut headers = HeaderMap::with_capacity(1);
        if let Some(token) = token {
            headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("token {token}"))
                    .map_err(|_| InvalidTokenSnafu.build())?,
            );
        }
        let http_store = HttpBuilder::new()
            .with_url(endpoints.raw_url(&org, &repo, &rev))
            .with_client_options(
                ClientOptions::default()
                    .with_default_headers(headers)
                    .with_allow_http(endpoints.allow_http),
            )
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .build()
            .context(HttpBuilderFailedSnafu)?;
        Ok(Self {
            http_store,
            config: Arc::new(GitHubClientConfig::new(&org, &repo, &rev, token, endpoints)),
        })
    }
}

impl Display for GitHubRawObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GitHubRawObjectStore")
    }
}

#[async_trait]
impl ObjectStore for GitHubRawObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.http_store.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        Err(not_implemented("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        Err(not_implemented("put_multipart_opts"))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        locations
            .map(|location| match location {
                Ok(_) => Err(not_implemented("delete_stream")),
                Err(err) => Err(err),
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        // Github raw content endpoint does not support listing files in a directory, so we need to use the GitHub API
        // to get the list of files and then create the ObjectMeta objects from the response.

        // ensure prefix ends with a /
        let prefix = prefix.map(|p| {
            if p.to_string().ends_with('/') {
                p.to_string()
            } else {
                format!("{p}/")
            }
        });

        let config = Arc::clone(&self.config);

        Box::pin(async_stream::stream! {
            let gh_rest_api = match GithubRestClient::new(config.token.as_deref()) {
                Ok(client) => client,
                Err(err) => {
                    yield Err(object_store::Error::Generic {
                        store: "GitHubRawObjectStore",
                        source: Box::new(std::io::Error::other(format!(
                            "Failed to create GitHub client: {err}"
                        ))),
                    });
                    return;
                }
            };
            let tree_url = config.endpoints.git_tree_url(&config.org, &config.repo, &config.rev);
            let git_tree = match gh_rest_api.fetch_git_tree(&tree_url).await {
                Ok(tree) => tree,
                Err(e) => {
                    yield Err(object_store::Error::Generic {
                        store: "GitHubRawObjectStore",
                        source: Box::new(std::io::Error::other(format!("GitHub API error: {e}"))),
                    });
                    return;
                }
            };

            // Keep only file entries within the prefix path
            let files: Vec<GitTreeNode> = git_tree
                .tree
                .into_iter()
                .filter(|node| node.node_type == "blob" && prefix.as_ref().is_none_or(|p| node.path.starts_with(&p.clone())))
                .collect();

            for file in files {
                let path = Path::from(file.path);
                let metadata = ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    size: u64::try_from(file.size.unwrap_or(0)).unwrap_or_default(),
                    e_tag: None,
                    version: None,
                };
                yield Ok(metadata);
            }
        })
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        Err(not_implemented("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        Err(not_implemented("copy_opts"))
    }
}

fn not_implemented(operation: &'static str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_string(),
        implementer: "GitHubRawObjectStore".to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct GitTree {
    tree: Vec<GitTreeNode>,
}

#[derive(Debug, Deserialize)]
struct GitTreeNode {
    path: String,
    #[serde(rename = "type")]
    node_type: String,
    size: Option<i64>,
}

static SPICE_USER_AGENT: &str = "spice";

pub struct GithubRestClient {
    client: reqwest::Client,
    token: Option<String>,
}

impl GithubRestClient {
    pub fn new(token: Option<&str>) -> reqwest::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_mins(2))
            .build()?;

        Ok(Self {
            client,
            token: token.map(ToString::to_string),
        })
    }

    /// Fetches the recursive git tree from `endpoint`, which
    /// `GitHubEndpoints::git_tree_url` built.
    async fn fetch_git_tree(
        &self,
        endpoint: &str,
    ) -> Result<GitTree, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github.v3+json"),
        );

        if let Some(token) = self.token.as_ref()
            && let Ok(header) = HeaderValue::from_str(&format!("token {token}"))
        {
            headers.insert(AUTHORIZATION, header);
        }

        tracing::debug!("fetch_git_tree: endpoint: {}", endpoint);

        let response = self.client.get(endpoint).headers(headers).send().await?;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!("fetch_git_tree returned {} entities", git_tree.tree.len());
            return Ok(git_tree);
        }

        let response_status = response.status().as_u16();
        let err_msg =
            format!("The Github API ({endpoint}) failed with status code {response_status}");
        Err(err_msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn github_client_config_debug_redacts_token() {
        let cfg = GitHubClientConfig::new(
            "org",
            "repo",
            "main",
            Some("ghp_supersecret"),
            GitHubEndpoints::github_com(),
        );
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("ghp_supersecret"),
            "Debug leaked the GitHub token: {dbg}"
        );
        assert!(dbg.contains("org") && dbg.contains("[REDACTED]"));
    }

    /// A request this store made, as the stub server saw it.
    #[derive(Debug, Clone)]
    struct SeenRequest {
        path_and_query: String,
        authorization: Option<String>,
    }

    /// A stub for one GitHub surface: it records what the store asked for and
    /// replies with a canned status and body.
    #[derive(Clone)]
    struct StubSurface {
        seen: Arc<std::sync::Mutex<Vec<SeenRequest>>>,
        status: u16,
        body: bytes::Bytes,
    }

    impl StubSurface {
        fn responding(status: u16, body: impl Into<bytes::Bytes>) -> Self {
            Self {
                seen: Arc::new(std::sync::Mutex::new(Vec::new())),
                status,
                body: body.into(),
            }
        }

        /// Serves the stub on an ephemeral loopback port and returns its base
        /// URL. Nothing leaves the host, so the result cannot depend on a third
        /// party's availability (spiceai/spiceai#13206).
        async fn serve(&self) -> String {
            use axum::{Router, body::Body, extract::State, http::Request, response::Response};

            async fn handler(
                State(stub): State<StubSurface>,
                request: Request<Body>,
            ) -> Response<Body> {
                let path_and_query = request
                    .uri()
                    .path_and_query()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let authorization = request
                    .headers()
                    .get(AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .map(ToString::to_string);
                stub.seen
                    .lock()
                    .expect("the stub's request log is not poisoned")
                    .push(SeenRequest {
                        path_and_query,
                        authorization,
                    });
                Response::builder()
                    .status(stub.status)
                    .body(Body::from(stub.body))
                    .expect("the stub response is well-formed")
            }

            let app = Router::new()
                .fallback(axum::routing::any(handler))
                .with_state(self.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the stub server binds a loopback port");
            let address = listener
                .local_addr()
                .expect("the stub server has an address");
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            format!("http://{address}")
        }

        fn requests(&self) -> Vec<SeenRequest> {
            self.seen
                .lock()
                .expect("the stub's request log is not poisoned")
                .clone()
        }

        /// The paths this surface was asked for, in order.
        fn paths(&self) -> Vec<String> {
            self.requests()
                .into_iter()
                .map(|request| request.path_and_query)
                .collect()
        }
    }

    /// A recursive git-tree payload in the shape the GitHub API returns.
    fn git_tree_json(entries: &[(&str, &str, i64)]) -> String {
        let tree: Vec<serde_json::Value> = entries
            .iter()
            .map(|(path, node_type, size)| {
                serde_json::json!({ "path": path, "type": node_type, "size": size })
            })
            .collect();
        serde_json::json!({ "tree": tree }).to_string()
    }

    /// Builds a store whose two surfaces are the supplied stubs. `try_new` is
    /// the only difference from production: the paths, the validation and the
    /// header wiring are the same code.
    async fn store_against(
        raw: &StubSurface,
        api: &StubSurface,
        token: Option<&str>,
    ) -> GitHubRawObjectStore {
        let endpoints = GitHubEndpoints {
            raw_base: raw.serve().await,
            api_base: api.serve().await,
            // The stubs are loopback-only, so plaintext is contained to this test.
            allow_http: true,
        };
        GitHubRawObjectStore::try_new_with_endpoints(
            "spiceai",
            "spiceai",
            "refs/heads/trunk",
            token,
            Handle::current(),
            endpoints,
        )
        .expect("the store is built from valid components")
    }

    #[test]
    fn endpoints_build_revision_scoped_github_urls() {
        let endpoints = GitHubEndpoints::github_com();
        assert_eq!(
            endpoints.raw_url("spiceai", "spiceai", "refs/heads/trunk"),
            "https://raw.githubusercontent.com/spiceai/spiceai/refs/heads/trunk"
        );
        assert_eq!(
            endpoints.git_tree_url("spiceai", "spiceai", "refs/heads/trunk"),
            "https://api.github.com/repos/spiceai/spiceai/git/trees/refs/heads/trunk?recursive=true"
        );
    }

    /// The plaintext escape hatch exists only for this module's loopback stubs.
    /// A production store sends the token, so it must be HTTPS-only.
    #[test]
    fn production_endpoints_are_https_and_forbid_cleartext() {
        let endpoints = GitHubEndpoints::github_com();
        assert!(
            endpoints.raw_base.starts_with("https://"),
            "raw base must be HTTPS: {}",
            endpoints.raw_base
        );
        assert!(
            endpoints.api_base.starts_with("https://"),
            "API base must be HTTPS: {}",
            endpoints.api_base
        );
        assert!(
            !endpoints.allow_http,
            "a production store must not be able to send its token over cleartext"
        );
    }

    #[tokio::test]
    async fn get_opts_reads_the_path_under_the_revision_prefix() {
        let raw = StubSurface::responding(200, b"# Spice.ai".to_vec());
        let api = StubSurface::responding(200, git_tree_json(&[]));
        let store = store_against(&raw, &api, None).await;

        let result = store
            .get_opts(&Path::from("README.md"), GetOptions::default())
            .await
            .expect("the stub serves README.md");
        let body = result.bytes().await.expect("the body is readable");
        assert_eq!(body.as_ref(), b"# Spice.ai");

        let paths = raw.paths();
        assert!(
            paths.contains(&"/spiceai/spiceai/refs/heads/trunk/README.md".to_string()),
            "the fetch must be scoped to org/repo/revision, saw {paths:?}"
        );
    }

    /// AC: coverage still fails loudly when the store itself is broken. A 404 is
    /// used rather than a 429 deliberately — `object_store` retries a 429 ten
    /// times over ~132s (spiceai/spiceai#13206), which is the cost this issue is
    /// about, and a non-retryable status keeps the assertion about *this* store.
    #[tokio::test]
    async fn get_opts_surfaces_a_missing_object() {
        let raw = StubSurface::responding(404, b"not found".to_vec());
        let api = StubSurface::responding(200, git_tree_json(&[]));
        let store = store_against(&raw, &api, None).await;

        let error = store
            .get_opts(&Path::from("missing.md"), GetOptions::default())
            .await
            .expect_err("a rejected raw fetch must not read as success");
        assert!(
            matches!(error, object_store::Error::NotFound { .. }),
            "a 404 from the raw endpoint must map to NotFound, got {error:?}"
        );
    }

    #[tokio::test]
    async fn list_requests_the_recursive_git_tree_for_the_revision() {
        let raw = StubSurface::responding(200, Vec::new());
        let api = StubSurface::responding(200, git_tree_json(&[("README.md", "blob", 10)]));
        let store = store_against(&raw, &api, None).await;

        let _entries: Vec<_> = store.list(None).collect::<Vec<_>>().await;

        let paths = api.paths();
        assert!(
            paths.contains(
                &"/repos/spiceai/spiceai/git/trees/refs/heads/trunk?recursive=true".to_string()
            ),
            "listing must ask the API for the revision's recursive tree, saw {paths:?}"
        );
    }

    #[tokio::test]
    async fn list_yields_only_blobs_under_the_prefix() {
        let raw = StubSurface::responding(200, Vec::new());
        let api = StubSurface::responding(
            200,
            git_tree_json(&[
                ("docs/release_notes/rc", "tree", 0),
                ("docs/release_notes/rc/v1.0.0-rc.1.md", "blob", 128),
                ("docs/release_notes/rc/v1.0.0-rc.2.md", "blob", 256),
                // A directory entry under the prefix is not an object.
                ("docs/release_notes/rc/nested", "tree", 0),
                // Outside the prefix.
                ("docs/release_notes/v1.0.0.md", "blob", 512),
                ("README.md", "blob", 10),
            ]),
        );
        let store = store_against(&raw, &api, None).await;

        let entries: Vec<ObjectMeta> = store
            .list(Some(&Path::from("docs/release_notes/rc")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("every listed entry is Ok");

        let listed: Vec<(String, u64)> = entries
            .iter()
            .map(|meta| (meta.location.to_string(), meta.size))
            .collect();
        assert_eq!(
            listed,
            vec![
                ("docs/release_notes/rc/v1.0.0-rc.1.md".to_string(), 128),
                ("docs/release_notes/rc/v1.0.0-rc.2.md".to_string(), 256),
            ],
            "only blobs under the prefix are objects, and their sizes carry through"
        );
    }

    /// The prefix is a directory, so a sibling whose name merely starts with the
    /// same characters must not be listed.
    #[tokio::test]
    async fn list_scopes_a_prefix_without_a_trailing_slash_to_the_directory() {
        let raw = StubSurface::responding(200, Vec::new());
        let api = StubSurface::responding(
            200,
            git_tree_json(&[
                ("docs/release_notes/rc/v1.0.0-rc.1.md", "blob", 1),
                ("docs/release_notes/rc-archive/old.md", "blob", 2),
            ]),
        );
        let store = store_against(&raw, &api, None).await;

        let entries: Vec<ObjectMeta> = store
            .list(Some(&Path::from("docs/release_notes/rc")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("every listed entry is Ok");

        let listed: Vec<String> = entries
            .iter()
            .map(|meta| meta.location.to_string())
            .collect();
        assert_eq!(
            listed,
            vec!["docs/release_notes/rc/v1.0.0-rc.1.md".to_string()],
            "'rc-archive' is a sibling directory, not part of the 'rc' prefix"
        );
    }

    /// AC: coverage still fails loudly when the store itself is broken, rather
    /// than reporting an empty listing.
    #[tokio::test]
    async fn list_surfaces_a_failing_github_api() {
        let raw = StubSurface::responding(200, Vec::new());
        let api = StubSurface::responding(429, b"rate limited".to_vec());
        let store = store_against(&raw, &api, None).await;

        let results: Vec<_> = store.list(None).collect::<Vec<_>>().await;
        let error = match results.as_slice() {
            [Err(error)] => error.to_string(),
            other => panic!("a rejected API must yield exactly one error, got {other:?}"),
        };
        assert!(
            error.contains("429"),
            "the error must name the status the API returned: {error}"
        );
    }

    #[tokio::test]
    async fn both_surfaces_send_the_configured_token() {
        let raw = StubSurface::responding(200, b"# Spice.ai".to_vec());
        let api = StubSurface::responding(200, git_tree_json(&[("README.md", "blob", 10)]));
        let store = store_against(&raw, &api, Some("ghp_supersecret")).await;

        let _ = store
            .get_opts(&Path::from("README.md"), GetOptions::default())
            .await
            .expect("the stub serves README.md");
        let _entries: Vec<_> = store.list(None).collect::<Vec<_>>().await;

        for (surface, requests) in [("raw", raw.requests()), ("api", api.requests())] {
            assert!(
                requests.iter().any(
                    |request| request.authorization.as_deref() == Some("token ghp_supersecret")
                ),
                "the {surface} surface must carry the token, saw {requests:?}"
            );
        }
    }

    /// Live coverage against GitHub itself. Deliberately `#[ignore]`d: it is a
    /// third-party dependency, and the sign-off gate must not be able to fail
    /// because that third party is rate-limiting or down (spiceai/spiceai#13206).
    /// The gate runs no ignored tests, so this is opt-in with
    /// `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore = "reaches raw.githubusercontent.com and api.github.com; run on demand"]
    async fn live_github_serves_the_repository_contents() {
        let store = GitHubRawObjectStore::try_new(
            "spiceai",
            "spiceai",
            "refs/heads/trunk",
            None,
            Handle::current(),
        )
        .expect("failed to create store");
        store
            .get_opts(&Path::from("README.md"), GetOptions::default())
            .await
            .expect("failed to get README");

        let files: Vec<_> = store
            .list(Some(&Path::from("docs/release_notes/rc")))
            .collect::<Vec<_>>()
            .await;
        assert!(!files.is_empty());
    }

    #[test]
    fn validate_github_component_accepts_valid_values() {
        validate_github_component("organization", "spiceai", false).expect("org");
        validate_github_component("repository", "spice.ai_demo-1", false).expect("repo");
        validate_github_component("revision", "refs/heads/trunk", true).expect("rev with slashes");
        validate_github_component("revision", "v1.2.3", true).expect("tag");
        validate_github_component("revision", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", true)
            .expect("sha");
    }

    #[test]
    fn validate_github_component_rejects_injection_and_traversal() {
        // Path traversal.
        assert!(validate_github_component("revision", "..", true).is_err());
        assert!(validate_github_component("revision", "a/../../b", true).is_err());
        // Empty and slash-edge cases.
        assert!(validate_github_component("repository", "", false).is_err());
        assert!(validate_github_component("revision", "/leading", true).is_err());
        assert!(validate_github_component("revision", "trailing/", true).is_err());
        // Organizations/repositories may not contain '/'.
        assert!(validate_github_component("organization", "org/with/slash", false).is_err());
        // URL-structure-breaking characters are rejected in every component.
        for bad in ["a?b", "a#b", "a@b", "a%2fb", "a b", "a\\b", "a:b", "a\nb"] {
            assert!(
                validate_github_component("repository", bad, false).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn try_new_rejects_malicious_components() {
        // Component validation runs before any network access, at construction.
        assert!(
            GitHubRawObjectStore::try_new("..", "repo", "main", None, Handle::current()).is_err(),
            "traversal organization should be rejected"
        );
        assert!(
            GitHubRawObjectStore::try_new("org", "repo?x=1", "main", None, Handle::current())
                .is_err(),
            "query-injecting repository should be rejected"
        );
        assert!(
            GitHubRawObjectStore::try_new("org", "repo", "a/../../etc", None, Handle::current())
                .is_err(),
            "traversal revision should be rejected"
        );
    }
}
