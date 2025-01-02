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

use std::sync::Arc;

use crate::model::EmbeddingModelStore;
use async_openai::types::CreateEmbeddingRequest;
#[cfg(feature = "openapi")]
use async_openai::types::CreateEmbeddingResponse;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use tokio::sync::RwLock;

/// Create Embeddings
///
/// Creates an embedding vector representing the input text.
///
/// Get a vector representation of a given input that can be easily consumed by machine learning models and algorithms.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/embeddings",
    operation_id = "post_embeddings",
    tag = "AI",
    request_body(
        description = "Embedding creation request parameters",
        content((
            CreateEmbeddingRequest = "application/json",
            example = json!({
                "input": "The food was delicious and the waiter...",
                "model": "text-embedding-ada-002",
                "encoding_format": "float"
            })
        ))
    ),
    responses(
        (status = 200, description = "Embedding created successfully", content((
            CreateEmbeddingResponse = "application/json",
            example = json!({
                "object": "list",
                "data": [
                    {
                        "object": "embedding",
                        "embedding": [
                            0.002_306_425_5,
                            -0.009_327_292,
                            -0.002_884_222_2
                        ],
                        "index": 0
                    }
                ],
                "model": "text-embedding-ada-002",
                "usage": {
                    "prompt_tokens": 8,
                    "total_tokens": 8
                }
            })
        ))),
        (status = 404, description = "Model not found", content((
            String = "application/json",
            example = json!({
                "error": "model not found"
            })
        ))),
        (status = 500, description = "Internal server error", content((
            String = "application/json",
            example = json!({
                "error": "Unexpected internal server error occurred"
            })
        )))
    )
))]
pub(crate) async fn post(
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Json(req): Json<CreateEmbeddingRequest>,
) -> Response {
    let model_id = req.model.clone().to_string();
    match embeddings.read().await.get(&model_id) {
        Some(model) => {
            let resp: Response = match model.embed_request(req).await {
                Ok(response) => Json(response).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            };

            resp
        }
        None => (StatusCode::NOT_FOUND, "model not found").into_response(),
    }
}
