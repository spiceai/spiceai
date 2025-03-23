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

use std::fmt::Display;

use async_trait::async_trait;
use chrono::TimeZone;
use futures::stream::BoxStream;
use http::{
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    HeaderMap, HeaderValue,
};
use object_store::{
    http::{HttpBuilder, HttpStore},
    path::Path,
    ClientOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use serde::Deserialize;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An internal error occured while connecting to GitHub to download files.\n{source}"
    ))]
    HttpBuilderFailed { source: object_store::Error },

    #[snafu(display("An invalid GitHub token was provided."))]
    InvalidToken,
}

/// An implementation of the `ObjectStore` trait for raw.githubusercontent.com
///
/// This is logically a small wrapper on the existing HTTP Object Store, but just constrained to specific GitHub URLs
#[derive(Debug)]
pub struct GitHubRawObjectStore {
    http_store: HttpStore,
    org: String,
    repo: String,
    rev: String,
    token: Option<String>,
}

impl GitHubRawObjectStore {
    pub fn try_new(
        org: impl Display,
        repo: impl Display,
        rev: impl Display,
        token: Option<&str>,
    ) -> Result<Self, Error> {
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
            .build()
            .context(HttpBuilderFailedSnafu)?;
        Ok(Self {
            http_store,
            org: org.to_string(),
            repo: repo.to_string(),
            rev: rev.to_string(),
            token: token.map(ToString::to_string),
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
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        Err(object_store::Error::NotImplemented)
    }

    async fn delete(&self, _location: &Path) -> Result<(), object_store::Error> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
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

        Box::pin(async_stream::stream! {
            let gh_rest_api = GithubRestClient::new(self.token.as_deref());
            let git_tree = match gh_rest_api.fetch_git_tree(&self.org, &self.repo, &self.rev).await {
                Ok(tree) => tree,
                Err(e) => {
                    yield Err(object_store::Error::Generic {
                        store: "GitHubRawObjectStore",
                        source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("GitHub API error: {e}"))),
                    });
                    return;
                }
            };

            // Keep only file entries within the prefix path
            let files: Vec<GitTreeNode> = git_tree
                .tree
                .into_iter()
                .filter(|node| node.node_type == "blob" && prefix.as_ref().is_none_or(|p| node.path.starts_with(&p.to_string())))
                .collect();

            for file in files {
                let path = Path::from(file.path);
                let metadata = ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    size: usize::try_from(file.size.unwrap_or(0)).unwrap_or_default(),
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
    #[must_use]
    pub fn new(token: Option<&str>) -> Self {
        Self {
            client: reqwest::Client::new(),
            token: token.map(ToString::to_string),
        }
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

        if let Some(token) = self.token.as_ref() {
            if let Ok(header) = HeaderValue::from_str(&format!("token {token}")) {
                headers.insert(AUTHORIZATION, header);
            }
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

    #[tokio::test]
    async fn test_get_opts() {
        let store = GitHubRawObjectStore::try_new("spiceai", "spiceai", "refs/heads/trunk", None)
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
}
