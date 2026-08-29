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
use crate::types::{Content, Part};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

/// One text to embed. The model is named in the request URL, not the body.
#[derive(Debug, Clone)]
pub struct EmbedContentRequest {
    pub content: Content,
    pub task_type: Option<TaskType>,
    pub output_dimensionality: Option<u32>,
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
pub struct Embedding {
    pub values: Vec<f32>,
}

impl Client {
    /// Embeds `requests` with `model`. Vertex AI's publisher text-embedding models (e.g.
    /// `text-embedding-004`) are served by the `PredictionService` `:predict` RPC, whose
    /// request/response shape this translates to and from. The embeddings are returned in the
    /// order Vertex produced them; this does not check that it returned one per request.
    pub async fn batch_embed_content(
        &self,
        model: &str,
        requests: &[EmbedContentRequest],
    ) -> Result<Vec<Embedding>> {
        let url = self.build_url(&format!("/models/{model}:predict"));
        let headers = self.auth_headers(HeaderMap::new());

        let instances: Vec<VertexPredictInstance> = requests
            .iter()
            .map(|r| VertexPredictInstance {
                content: text_content(&r.content),
                task_type: r.task_type.clone(),
            })
            .collect();

        // Vertex's `:predict` takes a single `parameters` object for the whole batch, so this
        // only carries over the first request's `output_dimensionality` — every request built
        // by `llms::google::embed` shares the same value anyway (it comes from one
        // `embeddings.params.dimensions` config).
        let output_dimensionality = requests.first().and_then(|r| r.output_dimensionality);

        let predict_request = VertexPredictRequest {
            instances,
            parameters: output_dimensionality.map(|d| VertexPredictParameters {
                output_dimensionality: Some(d),
            }),
        };

        let response = self
            .http_client()
            .post(&url)
            .headers(headers)
            .json(&predict_request)
            .send()
            .await
            .context(HttpSnafu)?;

        if !response.status().is_success() {
            return Err(handle_unsuccessful_response(response).await);
        }

        let parsed: VertexPredictResponse = response.json().await.context(HttpSnafu)?;
        Ok(parsed
            .predictions
            .into_iter()
            .map(|p| Embedding {
                values: p.embeddings.values,
            })
            .collect())
    }
}

/// Concatenates every text part of `content`, in order. Embedding requests built by
/// `llms::google::embed` always construct single-`Part::Text` content, but this degrades
/// gracefully (rather than panicking or erroring) if that ever changes.
fn text_content(content: &Content) -> String {
    content
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// Vertex's `:predict` instances are `snake_case` (`task_type`) even though its `parameters` are
/// `camelCase` (`outputDimensionality`) — so this struct deliberately carries no `rename_all`.
/// Serializing `task_type` as `taskType` makes Vertex ignore or reject the requested task.
#[derive(Debug, Clone, Serialize)]
struct VertexPredictInstance {
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_type: Option<TaskType>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct VertexPredictParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    output_dimensionality: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct VertexPredictRequest {
    instances: Vec<VertexPredictInstance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<VertexPredictParameters>,
}

#[derive(Debug, Clone, Deserialize)]
struct VertexPredictResponse {
    predictions: Vec<VertexPrediction>,
}

#[derive(Debug, Clone, Deserialize)]
struct VertexPrediction {
    embeddings: VertexPredictEmbedding,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VertexPredictEmbedding {
    values: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_content_extracts_single_text_part() {
        assert_eq!(text_content(&Content::user("hello world")), "hello world");
    }

    #[test]
    fn text_content_ignores_non_text_parts() {
        let content = Content {
            role: Some("user".to_string()),
            parts: vec![
                Part::Text {
                    text: "hello ".to_string(),
                },
                Part::Text {
                    text: "world".to_string(),
                },
            ],
        };
        assert_eq!(text_content(&content), "hello world");
    }

    #[test]
    fn vertex_predict_request_serializes_expected_shape() {
        let request = VertexPredictRequest {
            instances: vec![VertexPredictInstance {
                content: "hello".to_string(),
                task_type: Some(TaskType::RetrievalDocument),
            }],
            parameters: Some(VertexPredictParameters {
                output_dimensionality: Some(256),
            }),
        };

        let json = serde_json::to_value(&request).expect("should serialize");
        // Note the mixed casing: Vertex takes `task_type` on an instance but
        // `outputDimensionality` in `parameters`.
        assert_eq!(
            json,
            serde_json::json!({
                "instances": [{"content": "hello", "task_type": "RETRIEVAL_DOCUMENT"}],
                "parameters": {"outputDimensionality": 256}
            })
        );
    }

    #[test]
    fn vertex_predict_response_parses_real_shape() {
        // Real Vertex `:predict` response shape for text-embedding models.
        let json = r#"{
            "predictions": [
                {"embeddings": {"values": [0.1, 0.2, 0.3], "statistics": {"truncated": false, "token_count": 2}}}
            ]
        }"#;
        let parsed: VertexPredictResponse =
            serde_json::from_str(json).expect("should parse a real Vertex predict response");
        assert_eq!(parsed.predictions.len(), 1);
        assert_eq!(parsed.predictions[0].embeddings.values, vec![0.1, 0.2, 0.3]);
    }
}
