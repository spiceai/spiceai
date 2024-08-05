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

use std::{collections::HashMap, sync::Arc};

use crate::{datafusion::DataFusion, task_history, EmbeddingModelStore};
use async_openai::types::{CreateEmbeddingRequest, EncodingFormat};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use tokio::sync::RwLock;

pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Json(req): Json<CreateEmbeddingRequest>,
) -> Response {
    let context_id = uuid::Uuid::new_v4();

    let Ok(input_text) = serde_json::to_string(&req.input) else {
        return (StatusCode::BAD_REQUEST, "invalid input").into_response();
    };

    let model_id = req.model.clone().to_string();
    match embeddings.read().await.get(&model_id) {
        Some(model_lock) => {
            let mut model = model_lock.write().await;
            let mut task_span = task_history::TaskTracker::new(
                df,
                context_id,
                task_history::TaskType::Embed,
                input_text.as_str().into(),
                None,
            );

            task_span = task_span.labels(labels_from_request(&req));

            let resp: Response = match model.embed_request(req).await {
                Ok(response) => {
                    task_span = task_span.outputs_produced(response.data.len() as u64);
                    Json(response).into_response()
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    task_span = task_span.with_error_message(err_msg.clone());
                    (StatusCode::INTERNAL_SERVER_ERROR, err_msg).into_response()
                }
            };

            task_span.finish();
            resp
        }
        None => (StatusCode::NOT_FOUND, "model not found").into_response(),
    }
}

fn labels_from_request(req: &CreateEmbeddingRequest) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    labels.insert("model".to_string(), req.model.clone());

    if let Some(encoding_format) = &req.encoding_format {
        labels.insert(
            "encoding_format".to_string(),
            match encoding_format {
                EncodingFormat::Base64 => "base64".to_string(),
                EncodingFormat::Float => "float".to_string(),
            },
        );
    }
    if let Some(user) = &req.user {
        labels.insert("user".to_string(), user.clone());
    }

    if let Some(dims) = req.dimensions {
        labels.insert("dimensions".to_string(), dims.to_string());
    }

    labels
}
