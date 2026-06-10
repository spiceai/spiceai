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
use futures::stream::BoxStream;
use http::{
    HeaderMap, HeaderValue,
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
};
use object_store::{
    ClientOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
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

    #[snafu(display(
        "Invalid GitHub {component} '{value}': only letters, digits, '.', '_', '-' (and '/' for a revision) are allowed, and '..' is not permitted."
    ))]
    InvalidComponent {
        component: &'static str,
        value: String,
    },
}

struct GitHubClientConfig {
    org: String,
    repo: String,
    rev: String,
    token: Option<String>,
}

impl std::fmt::Debug for GitHubClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the GitHub token so a `{:?}` of this config cannot leak it.
        f.debug_struct("GitHubClientConfig")
            .field("org", &self.org)
            .field("repo", &self.repo)
            .field("rev", &self.rev)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl GitHubClientConfig {
    fn new(org: impl Display, repo: impl Display, rev: impl Display, token: Option<&str>) -> Self {
        Self {
            org: org.to_string(),
            repo: repo.to_string(),
            rev: rev.to_string(),
            token: token.map(ToString::to_string),
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
            .with_url(format!(
                "https://raw.githubusercontent.com/{org}/{repo}/{rev}"
            ))
            .with_client_options(ClientOptions::default().with_default_headers(headers))
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .build()
            .context(HttpBuilderFailedSnafu)?;
        Ok(Self {
            http_store,
            config: Arc::new(GitHubClientConfig::new(&org, &repo, &rev, token)),
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
        Err(object_store::Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        Err(object_store::Error::NotImplemented)
    }

    async fn delete(&self, _location: &Path) -> Result<(), object_store::Error> {
        Err(object_store::Error::NotImplemented)
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
            let git_tree = match gh_rest_api.fetch_git_tree(&config.org, &config.repo, &config.rev).await {
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
        Err(object_store::Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<(), object_store::Error> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> Result<(), object_store::Error> {
        Err(object_store::Error::NotImplemented)
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
            .timeout(Duration::from_secs(120))
            .build()?;

        Ok(Self {
            client,
            token: token.map(ToString::to_string),
        })
    }

    async fn fetch_git_tree(
        &self,
        org: &str,
        repo: &str,
        rev: &str,
    ) -> Result<GitTree, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let endpoint =
            format!("https://api.github.com/repos/{org}/{repo}/git/trees/{rev}?recursive=true");

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

        let response = self.client.get(&endpoint).headers(headers).send().await?;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!("fetch_git_tree returned {} entities", git_tree.tree.len());
            return Ok(git_tree);
        }

        let response_status = response.status().as_u16();
        let err_msg =
            format!("The Github API ({endpoint}) failed with status code {response_status}",);
        Err(err_msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn github_client_config_debug_redacts_token() {
        let cfg = GitHubClientConfig::new("org", "repo", "main", Some("ghp_supersecret"));
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("ghp_supersecret"),
            "Debug leaked the GitHub token: {dbg}"
        );
        assert!(dbg.contains("org") && dbg.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_get_opts() {
        let store = GitHubRawObjectStore::try_new(
            "spiceai",
            "spiceai",
            "refs/heads/trunk",
            None,
            Handle::current(),
        )
        .expect("failed to create store");
        let result = store
            .get_opts(&Path::from("README.md"), GetOptions::default())
            .await
            .expect("failed to get README");
        println!("{result:?}");

        let files: Vec<_> = store
            .list(Some(&Path::from("docs/release_notes/rc")))
            .collect::<Vec<_>>()
            .await;
        println!("{files:?}");
        assert!(!files.is_empty());
    }

    #[test]
    fn validate_github_component_accepts_valid_values() {
        validate_github_component("organization", "spiceai", false).expect("org");
        validate_github_component("repository", "spice.ai_demo-1", false).expect("repo");
        validate_github_component("revision", "refs/heads/trunk", true).expect("rev with slashes");
        validate_github_component("revision", "v1.2.3", true).expect("tag");
        validate_github_component(
            "revision",
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            true,
        )
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
        for bad in [
            "a?b", "a#b", "a@b", "a%2fb", "a b", "a\\b", "a:b", "a\nb",
        ] {
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
