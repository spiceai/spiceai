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

//! Jina Reranker client — <https://jina.ai/reranker/>
//!
//! `POST https://api.jina.ai/v1/rerank` with `{model, query, documents}`. The
//! response uses Cohere-compatible `results` with `{index, relevance_score}`.

use async_trait::async_trait;
use reqwest::Client;
use serde::Serialize;
use std::fmt::Debug;

use super::http::{CohereLikeResponse, scores_from_results_strict};
use super::{Error, ModelCallFailedSnafu, Rerank, Result};
use crate::provider::create_http_client;
use snafu::ResultExt;

const DEFAULT_ENDPOINT: &str = "https://api.jina.ai/v1/rerank";
const DEFAULT_MODEL: &str = "jina-reranker-v2-base-multilingual";

#[derive(Debug, Serialize)]
struct RerankRequest<'a> {
    model: &'a str,
    query: &'a str,
    documents: &'a [String],
}

pub struct JinaReranker {
    client: Client,
    endpoint: String,
    name: String,
    model_id: String,
    api_key: String,
}

impl Debug for JinaReranker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JinaReranker")
            .field("name", &self.name)
            .field("model_id", &self.model_id)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl JinaReranker {
    pub fn try_new(name: impl Into<String>, api_key: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let client = create_http_client().ok_or(Error::HttpClientCreationFailed {
            model: name.clone(),
        })?;
        Ok(Self {
            client,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            name,
            model_id: DEFAULT_MODEL.to_string(),
            api_key: api_key.into(),
        })
    }

    #[must_use]
    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = model_id.into();
        self
    }

    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

#[async_trait]
impl Rerank for JinaReranker {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let body = RerankRequest {
            model: &self.model_id,
            query,
            documents,
        };

        let resp: CohereLikeResponse = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ModelCallFailedSnafu {
                model: self.name.clone(),
            })?
            .json()
            .await
            .map_err(|e| Error::UnparseableResponse {
                model: self.name.clone(),
                response: format!("Jina rerank response decode failed: {e}"),
            })?;

        scores_from_results_strict(&resp.results, documents.len(), &self.name)
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn is_remote(&self) -> bool {
        true
    }
}
