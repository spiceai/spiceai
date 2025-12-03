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
use crate::error::{HttpSnafu, JsonSnafu, Result, StreamSnafu};
use crate::types::{Candidate, Content, GenerationConfig, SafetySetting, Tool, UsageMetadata};
use bytes::Bytes;
use futures::Stream;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::pin::Pin;
use tokio_stream::StreamExt;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentRequest {
    pub contents: Vec<Content>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_settings: Option<Vec<SafetySetting>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<GenerationConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_instruction: Option<Content>,
}

impl GenerateContentRequest {
    #[must_use]
    pub fn new(contents: Vec<Content>) -> Self {
        Self {
            contents,
            tools: None,
            safety_settings: None,
            generation_config: None,
            system_instruction: None,
        }
    }

    #[must_use]
    pub fn with_generation_config(mut self, config: GenerationConfig) -> Self {
        self.generation_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_tools(mut self, tools: Vec<Tool>) -> Self {
        self.tools = Some(tools);
        self
    }

    #[must_use]
    pub fn with_safety_settings(mut self, settings: Vec<SafetySetting>) -> Self {
        self.safety_settings = Some(settings);
        self
    }

    #[must_use]
    pub fn with_system_instruction(mut self, instruction: Content) -> Self {
        self.system_instruction = Some(instruction);
        self
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentResponse {
    pub candidates: Vec<Candidate>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage_metadata: Option<UsageMetadata>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_feedback: Option<PromptFeedback>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptFeedback {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_ratings: Option<Vec<crate::types::SafetyRating>>,
}

impl Client {
    pub async fn generate_content(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<GenerateContentResponse> {
        let url = self.build_url(&format!("/models/{model}:generateContent"));

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
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();

            return Err(crate::error::Error::ApiError {
                message: format!("API request failed with status {status}: {error_body}"),
                code: Some(i32::from(status.as_u16())),
            });
        }

        response
            .json::<GenerateContentResponse>()
            .await
            .context(HttpSnafu)
    }

    pub async fn stream_generate_content(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<GenerateContentResponse>> + Send>>> {
        let url = format!(
            "{}?alt=sse",
            self.build_url(&format!("/models/{model}:streamGenerateContent"))
        );

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
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();

            return Err(crate::error::Error::ApiError {
                message: format!("API request failed with status {status}: {error_body}"),
                code: Some(i32::from(status.as_u16())),
            });
        }

        let stream = response.bytes_stream();

        let parsed_stream = stream.map(|result| match result {
            Ok(bytes) => parse_sse_chunk(&bytes),
            Err(e) => StreamSnafu {
                message: e.to_string(),
            }
            .fail(),
        });

        Ok(Box::pin(parsed_stream))
    }
}

fn parse_sse_chunk(bytes: &Bytes) -> Result<GenerateContentResponse> {
    let text = std::str::from_utf8(bytes).map_err(|e| crate::error::Error::StreamError {
        message: format!("Invalid UTF-8: {e}"),
    })?;

    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data.trim().is_empty() || data == "[DONE]" {
                continue;
            }

            return serde_json::from_str(data).context(JsonSnafu);
        }
    }

    Err(crate::error::Error::StreamError {
        message: "No data in SSE chunk".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_content_request() {
        let request = GenerateContentRequest::new(vec![Content::user("Hello")])
            .with_generation_config(GenerationConfig {
                temperature: Some(0.7),
                max_output_tokens: Some(1024),
                ..Default::default()
            });

        assert_eq!(request.contents.len(), 1);
        assert!(request.generation_config.is_some());
    }

    #[test]
    fn test_parse_sse_chunk() {
        let data =
            r#"data: {"candidates":[{"content":{"role":"model","parts":[{"text":"Hello"}]}}]}"#;
        let bytes = Bytes::from(data);
        let result = parse_sse_chunk(&bytes);
        assert!(result.is_ok());
    }
}
