use std::sync::Arc;

use crate::http::{
    traceparent::override_task_history_with_traceparent, v1::chat::openai_error_to_response,
};
use async_openai::types::responses::{
    Content, CreateResponse, OutputContent, Response as OpenAIResponse,
};
use axum::{
    Extension, Json,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use tokio::sync::RwLock;
use tracing::Instrument;

use crate::model::LLMResponsesModelStore;

fn extract_text(resp: &OpenAIResponse) -> Option<String> {
    resp.output
        .first()
        .and_then(|out| {
            if let OutputContent::Message(msg) = out {
                msg.content.first()
            } else {
                None
            }
        })
        .and_then(|content| match content {
            Content::OutputText(output_text) => Some(output_text.text.clone()),
            Content::Refusal(_) => None,
        })
}

pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMResponsesModelStore>>>,
    headers: HeaderMap,
    Json(req): Json<CreateResponse>,
) -> Response {
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_chat",
        input = %serde_json::to_string(&req).unwrap_or_default()
    );
    span.in_scope(|| tracing::info!(target: "task_history", model = %req.model, "labels"));

    override_task_history_with_traceparent(&span.clone(), &headers);

    let span_clone = span.clone();
    async move {
        let model_id = req.model.clone();
        match llms.read().await.get(&model_id) {
            Some(model) => {
                match model.responses_request(req).await {
                    Ok(response) => {
                        if let Some(message) = extract_text(&response) {
                            tracing::info!(target: "task_history", parent: &span_clone, captured_output = %message);
                        }
                        tracing::info!(target: "task_history", parent: &span_clone,  id = %response.id, "labels");

                        Json(response).into_response()
                    }
                    Err(e) => {
                        tracing::error!(target: "task_history", parent: &span_clone, "{e}");
                        tracing::error!("Error from v1/chat: {e}");

                        openai_error_to_response(e)
                    }
                }
            }
            None => (StatusCode::NOT_FOUND, format!("model '{model_id}' not found")).into_response(),
        }
    }
    .instrument(span)
    .await
}
