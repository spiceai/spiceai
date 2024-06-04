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
use async_openai::types::CreateChatCompletionRequest;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use tokio::sync::RwLock;

pub(crate) async fn post(
    Extension(openai_server_store): Extension<Arc<RwLock<OpenaiServerStore>>>,
    Json(req): Json<CreateChatCompletionRequest>,
) -> Response {
    let model_id = req.model.clone();
    match openai_server_store.read().await.get(&model_id) {
        Some(model_lock) => {
            let mut model = model_lock.write().await;
            match model.chat(req).await {
                Ok(response) => Json(response).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        None => (StatusCode::NOT_FOUND, "model not found").into_response(),
    }
}
