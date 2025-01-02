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
use futures::stream::BoxStream;
use http::{HeaderMap, HeaderValue};
use object_store::{
    http::{HttpBuilder, HttpStore},
    path::Path,
    ClientOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
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
        Ok(Self { http_store })
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
        _prefix: Option<&Path>,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        Box::pin(async_stream::stream! {
            yield Err(object_store::Error::NotImplemented);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_opts() {
        let store = GitHubRawObjectStore::try_new("spiceai", "spiceai", "refs/heads/trunk", None)
            .expect("failed to create store");
        let result = store
            .get_opts(&Path::from("README.md"), GetOptions::default())
            .await
            .expect("failed to get README");
        println!("{result:?}");
    }
}
