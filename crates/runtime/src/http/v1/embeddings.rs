/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use crate::OpenaiServerStore;
use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput, EncodingFormat};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use tokio::sync::RwLock;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

pub(crate) async fn post(
    Extension(openai_server_store): Extension<Arc<RwLock<OpenaiServerStore>>>,
    body: String,
) -> Response {
    let req: LocalCreateEmbeddingRequest = match serde_json::from_str(&body) {
        Ok(req) => req,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid request").into_response();
        }
    };

    let model_id = req.model.clone().to_string();
    match openai_server_store.read().await.get(&model_id) {
        Some(model_lock) => {
            let mut model = model_lock.write().await;
            match model
                .embed(CreateEmbeddingRequest {
                    model: req.model,
                    input: to_openai_embedding_input(req.input),
                    encoding_format: req.encoding_format.map(to_openai_encoding_format),
                    user: req.user,
                    dimensions: req.dimensions,
                })
                .await
            {
                Ok(response) => Json(response).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        None => (StatusCode::NOT_FOUND, "model not found").into_response(),
    }
}

/// Everything below is needed because `async_openai::types::CreateEmbeddingRequest` does not `derive(Deserialize)`
/// When upstream fork is used instead and fix is in, this will no longer be needed.

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum LocalEmbeddingInput {
    String(String),
    StringArray(Vec<String>),
    // Minimum value is 0, maximum value is 100257 (inclusive).
    IntegerArray(Vec<u32>),
    ArrayOfIntegerArray(Vec<Vec<u32>>),
}

fn to_openai_embedding_input(input: LocalEmbeddingInput) -> EmbeddingInput {
    match input {
        LocalEmbeddingInput::String(s) => EmbeddingInput::String(s),
        LocalEmbeddingInput::StringArray(s) => EmbeddingInput::StringArray(s),
        LocalEmbeddingInput::IntegerArray(s) => EmbeddingInput::IntegerArray(s),
        LocalEmbeddingInput::ArrayOfIntegerArray(s) => EmbeddingInput::ArrayOfIntegerArray(s),
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LocalEncodingFormat {
    #[default]
    Float,
    Base64,
}

#[allow(clippy::needless_pass_by_value)]
fn to_openai_encoding_format(format: LocalEncodingFormat) -> EncodingFormat {
    match format {
        LocalEncodingFormat::Float => EncodingFormat::Float,
        LocalEncodingFormat::Base64 => EncodingFormat::Base64,
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Builder, PartialEq)]
pub struct LocalCreateEmbeddingRequest {
    /// ID of the model to use. You can use the
    /// [List models](https://platform.openai.com/docs/api-reference/models/list)
    /// API to see all of your available models, or see our
    /// [Model overview](https://platform.openai.com/docs/models/overview)
    /// for descriptions of them.
    pub model: String,

    ///  Input text to embed, encoded as a string or array of tokens. To embed multiple inputs in a single request, pass an array of strings or array of token arrays. The input must not exceed the max input tokens for the model (8192 tokens for `text-embedding-ada-002`), cannot be an empty string, and any array must be 2048 dimensions or less. [Example Python code](https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken) for counting tokens.
    pub input: LocalEmbeddingInput,

    /// The format to return the embeddings in. Can be either `float` or [`base64`](https://pypi.org/project/pybase64/). Defaults to float
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_format: Option<LocalEncodingFormat>,

    /// A unique identifier representing your end-user, which will help `OpenAI`
    ///  to monitor and detect abuse. [Learn more](https://platform.openai.com/docs/usage-policies/end-user-ids).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// The number of dimensions the resulting output embeddings should have. Only supported in `text-embedding-3` and later models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,
}
