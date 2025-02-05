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

use std::pin::Pin;

use async_openai::{
    error::OpenAIError,
    types::{
        CreateChatCompletionRequest, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse,
    },
};
use futures::Stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerplexityRequest {
    #[serde(flatten)]
    pub chat: CreateChatCompletionRequest,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_parameters: Option<PerplexityRequestParameters>,
}

impl From<CreateChatCompletionRequest> for PerplexityRequest {
    fn from(chat: CreateChatCompletionRequest) -> Self {
        Self {
            chat,
            extra_parameters: None,
        }
    }
}

/// Request parameters that only work for Perplexity endpoints (i.e. not `OpenAI` compatible parameters).
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct PerplexityRequestParameters {
    /// Determines whether to return images (default: false).
    #[serde(default)]
    pub return_images: bool,
    /// Determines whether to return related questions (default: false).
    #[serde(default)]
    pub return_related_questions: bool,
    /// Given a list of domains, restrict citations to those URLs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_domain_filter: Option<Vec<String>>,
    /// Returns search results within the specified time interval (e.g. "month", "week", etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_recency_filter: Option<String>,
}

impl PerplexityRequestParameters {
    pub(crate) fn update_overrides(&mut self, overrides: &[(String, String)]) {
        for (key, value) in overrides {
            match key.as_str() {
                "return_images" => self.return_images = value.parse().unwrap_or(false),
                "return_related_questions" => {
                    self.return_related_questions = value.parse().unwrap_or(false);
                }
                "search_domain_filter" => match serde_json::from_str::<Vec<String>>(value.as_str())
                {
                    Ok(v) => self.search_domain_filter = Some(v),
                    Err(e) => {
                        tracing::warn!("Failed to parse search_domain_filter: {}", e);
                    }
                },
                "search_recency_filter" => self.search_recency_filter = Some(value.clone()),
                _ => (),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerplexityResponse {
    #[serde(flatten)]
    pub response: CreateChatCompletionResponse,

    /// Citations for the generated answer.
    pub citations: Vec<String>,
}

pub type PerplexityResponseStream =
    Pin<Box<dyn Stream<Item = Result<PerplexityStreamResponse, OpenAIError>> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerplexityStreamResponse {
    #[serde(flatten)]
    pub response: CreateChatCompletionStreamResponse,

    /// Citations for the generated answer.
    pub citations: Vec<String>,
}
