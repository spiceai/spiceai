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

//! Google Generative AI API client
#![allow(clippy::missing_errors_doc)]

use crate::error::{Error, HttpSnafu, Result};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use snafu::ResultExt;

const BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const API_KEY_HEADER: &str = "x-goog-api-key";

#[expect(clippy::struct_field_names)]
#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("http_client", &self.http_client)
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Client {
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::with_base_url(api_key, BASE_URL)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let api_key = api_key.into();
        if api_key.is_empty() {
            return Err(Error::InvalidApiKey);
        }

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context(HttpSnafu)?;

        Ok(Self {
            http_client,
            api_key,
            base_url: base_url.into(),
        })
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    pub(crate) fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub(crate) fn add_api_key_header(&self, mut headers: HeaderMap) -> HeaderMap {
        if let Ok(value) = HeaderValue::from_str(&self.api_key) {
            headers.insert(API_KEY_HEADER, value);
        }
        headers
    }
}
