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

use crate::client::Client;
use crate::error::{HttpSnafu, Result, handle_unsuccessful_response};
use crate::types::Content;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbedContentRequest {
    pub content: Content,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<TaskType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_dimensionality: Option<i32>,
}

impl EmbedContentRequest {
    #[must_use]
    pub fn new(content: Content) -> Self {
        Self {
            content,
            task_type: None,
            output_dimensionality: None,
        }
    }

    #[must_use]
    pub fn with_task_type(mut self, task_type: TaskType) -> Self {
        self.task_type = Some(task_type);
        self
    }

    #[must_use]
    pub fn with_output_dimensionality(mut self, dimensionality: i32) -> Self {
        self.output_dimensionality = Some(dimensionality);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskType {
    TaskTypeUnspecified,
    RetrievalQuery,
    RetrievalDocument,
    SemanticSimilarity,
    Classification,
    Clustering,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbedContentResponse {
    pub embedding: Embedding,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Embedding {
    pub values: Vec<f32>,
}

impl Client {
    pub async fn embed_content(
        &self,
        model: &str,
        request: EmbedContentRequest,
    ) -> Result<EmbedContentResponse> {
        let url = self.build_url(&format!("/models/{model}:embedContent"));

        let headers = self.add_api_key_header(HeaderMap::new());

        let response = self
            .http_client()
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .context(HttpSnafu)?;

        if !response.status().is_success() {
            return Err(handle_unsuccessful_response(response).await);
        }

        response
            .json::<EmbedContentResponse>()
            .await
            .context(HttpSnafu)
    }

    pub async fn batch_embed_content(
        &self,
        model: &str,
        requests: Vec<EmbedContentRequest>,
    ) -> Result<BatchEmbedContentResponse> {
        let url = self.build_url(&format!("/models/{model}:batchEmbedContents"));

        let headers = self.add_api_key_header(HeaderMap::new());

        let batch_request = BatchEmbedContentRequest { requests };

        let response = self
            .http_client()
            .post(&url)
            .headers(headers)
            .json(&batch_request)
            .send()
            .await
            .context(HttpSnafu)?;

        if !response.status().is_success() {
            return Err(handle_unsuccessful_response(response).await);
        }

        response
            .json::<BatchEmbedContentResponse>()
            .await
            .context(HttpSnafu)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchEmbedContentRequest {
    requests: Vec<EmbedContentRequest>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchEmbedContentResponse {
    pub embeddings: Vec<Embedding>,
}
