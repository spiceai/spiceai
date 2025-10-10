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

use std::{sync::Arc, time::Instant};

use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingResponse, EmbeddingInput, EncodingFormat,
};
use async_trait::async_trait;
use cache::key::CacheKey;
use chunking::{Chunker, ChunkingConfig};
use llms::embeddings::{Embed, Result as EmbedResult, get_or_infer_size};
use runtime_request_context::{AsyncMarker, RequestContext};
use tracing::{Instrument, Span};

use super::metrics::{handle_metrics, request_labels, simple_labels};

#[derive(Debug)]
pub struct TaskEmbed {
    name: String,
    inner: Arc<dyn Embed>,
    vector_size: i32,
}

impl TaskEmbed {
    pub async fn new(name: &str, inner: Arc<dyn Embed>) -> EmbedResult<Self> {
        let size = get_or_infer_size(&inner).await?;
        Ok(Self {
            name: name.to_string(),
            inner,
            vector_size: size,
        })
    }
}

#[async_trait]
impl Embed for TaskEmbed {
    async fn embed<'b>(&'b self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        telemetry::track_text_embedding(&request_context.to_dimensions());

        let num_items = match &input {
            EmbeddingInput::StringArray(arr) => arr.len(),
            EmbeddingInput::ArrayOfIntegerArray(arr) => arr.len(),
            EmbeddingInput::String(_) => 1,
            EmbeddingInput::IntegerArray(arr) => arr.len(),
        };

        let start = std::time::Instant::now();
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "text_embed", input = to_truncated_string(&input));

        let result = match self.inner.embed(input).instrument(span.clone()).await {
            Ok(response) => {
                tracing::info!(target: "task_history", parent: &span, outputs_produced = response.len(), "labels");
                Ok(response)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        };

        tracing::debug!(
            "Embedded {num_items} records in {duration:?} using {name}",
            duration = start.elapsed(),
            name = self.name
        );

        handle_metrics(
            start.elapsed(),
            result.is_err(),
            &simple_labels(self.name.as_str(), &EncodingFormat::default()),
        );
        result
    }

    async fn health<'b>(&'b self) -> EmbedResult<()> {
        self.inner.health().await
    }

    fn model_name(&self) -> Option<&str> {
        Some(self.name.as_str())
    }

    fn embedding_input_cache_key<'a>(&'a self, input: &'a EmbeddingInput) -> Option<CacheKey<'a>> {
        self.inner.embedding_input_cache_key(input)
    }

    fn size(&self) -> i32 {
        self.vector_size
    }

    fn chunker(&self, cfg: &ChunkingConfig) -> EmbedResult<Arc<dyn Chunker>> {
        self.inner.chunker(cfg)
    }

    fn supports_sync_embeddings(&self) -> bool {
        self.inner.supports_sync_embeddings()
    }

    fn parallelism(&self) -> Option<usize> {
        self.inner.parallelism()
    }

    fn embed_sync(&self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        self.inner.embed_sync(input)
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn embed_request<'b>(
        &'b self,
        req: CreateEmbeddingRequest,
    ) -> EmbedResult<CreateEmbeddingResponse> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        telemetry::track_text_embedding(&request_context.to_dimensions());

        let start = Instant::now();
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "text_embed", input = to_truncated_string(&req.input));

        add_request_labels_to_span(&req, &span);
        let metric_labels = request_labels(&req);
        let result = match self.inner.embed_request(req).instrument(span.clone()).await {
            Ok(response) => {
                tracing::info!(target: "task_history", parent: &span, outputs_produced = response.data.len(), "labels");
                Ok(response)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        };
        handle_metrics(start.elapsed(), result.is_err(), &metric_labels);
        result
    }
}

fn add_request_labels_to_span(req: &CreateEmbeddingRequest, span: &Span) {
    let _guard = span.enter();
    tracing::info!(target: "task_history", model = req.model, "labels");

    if let Some(encoding_format) = &req.encoding_format {
        let encoding_format_str = match encoding_format {
            async_openai::types::EncodingFormat::Base64 => "base64",
            async_openai::types::EncodingFormat::Float => "float",
        };
        tracing::info!(target: "task_history", encoding_format = %encoding_format_str, "labels");
    }
    if let Some(user) = &req.user {
        tracing::info!(target: "task_history", user = %user, "labels");
    }

    if let Some(dims) = req.dimensions {
        tracing::info!(target: "task_history", dimensions = %dims, "labels");
    }
}

#[must_use]
pub fn to_truncated_string(input: &EmbeddingInput) -> String {
    match input {
        EmbeddingInput::String(s) => serde_json::to_string(&s).unwrap_or_default(),
        EmbeddingInput::StringArray(arr) => format_array(arr),
        EmbeddingInput::IntegerArray(arr) => serde_json::to_string(&arr).unwrap_or_default(),
        EmbeddingInput::ArrayOfIntegerArray(arrs) => format_array(arrs),
    }
}

const TRUNCATE_LIMIT: usize = 3;

/// Formats arrays for tracing/logging, showing up to 3 items, followed by `...` if there are more.
fn format_array<T: serde::Serialize>(arr: &[T]) -> String {
    let mut parts = Vec::new();
    for (i, v) in arr.iter().enumerate() {
        if i >= TRUNCATE_LIMIT {
            parts.push("...".to_string());
            break;
        }
        parts.push(serde_json::to_string(v).unwrap_or_default());
    }
    format!("[{}]", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string() {
        let input = EmbeddingInput::String("hello".into());
        assert_eq!(to_truncated_string(&input), r#""hello""#);
    }

    #[test]
    fn test_string_array_under_limit() {
        let input = EmbeddingInput::StringArray(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(to_truncated_string(&input), r#"["a","b","c"]"#);
    }

    #[test]
    fn test_string_array_over_limit() {
        let input =
            EmbeddingInput::StringArray(vec!["a".into(), "b".into(), "c".into(), "d".into()]);
        assert_eq!(to_truncated_string(&input), r#"["a","b","c",...]"#);
    }

    #[test]
    fn test_integer_array_under_limit() {
        let input = EmbeddingInput::IntegerArray(vec![1, 2]);
        assert_eq!(to_truncated_string(&input), "[1,2]");
    }

    #[test]
    fn test_integer_array_over_limit() {
        let input = EmbeddingInput::IntegerArray(vec![1, 2, 3, 4]);
        assert_eq!(to_truncated_string(&input), "[1,2,3,4]");
    }

    #[test]
    fn test_array_of_integer_array_under_limit() {
        let input = EmbeddingInput::ArrayOfIntegerArray(vec![vec![1, 2], vec![3, 4]]);
        assert_eq!(to_truncated_string(&input), "[[1,2],[3,4]]");
    }

    #[test]
    fn test_array_of_integer_array_over_limit() {
        let input = EmbeddingInput::ArrayOfIntegerArray(vec![vec![1], vec![2], vec![3], vec![4]]);
        assert_eq!(to_truncated_string(&input), "[[1],[2],[3],...]");
    }
}
