/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `OpenAI` decision models (`gpt-6-luna`), answering the System One evaluation contract
//! through `OpenAI`'s Decisions API: `POST {endpoint}/decisions`.
//!
//! A decision model is not a chat model: it returns a probability, a choice, or a
//! score for each question, not text. See
//! <https://developers.openai.com/api/docs/guides/decisions>.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::openai::{
    DecisionResponse, decision_response_to_system_one, system_one_to_decision_request,
};
use evaluate_api::{Evaluate, EvaluateRequest, EvaluateResponse, Result};
use reqwest::{Client, RequestBuilder, StatusCode};
use runtime_rate_control::RateController;

use crate::provider::create_http_client;

/// The model id prefix of `OpenAI`'s decision models.
const DECISION_MODEL_PREFIX: &str = "gpt-6-luna";

/// Whether an `OpenAI` model id names a decision model, which answers decisions and not
/// chat completions.
#[must_use]
pub fn is_decision_model_id(model_id: &str) -> bool {
    model_id.starts_with(DECISION_MODEL_PREFIX)
}

/// An `OpenAI` decision model implementing [`Evaluate`].
pub struct OpenAiDecisions {
    client: Client,
    /// The API base, such as `https://api.openai.com/v1`.
    endpoint: String,
    /// Spicepod model name (runtime lookup key).
    name: String,
    /// Upstream model id, such as `gpt-6-luna`.
    model_id: String,
    api_key: String,
    org_id: Option<String>,
    project_id: Option<String>,
    rate_controller: Arc<RateController>,
}

impl Debug for OpenAiDecisions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiDecisions")
            .field("name", &self.name)
            .field("model_id", &self.model_id)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl OpenAiDecisions {
    /// Builds a client for the decision model `model_id` at `endpoint`.
    ///
    /// # Errors
    ///
    /// Returns [`evaluate_api::Error::HttpClientCreationFailed`] when the HTTP client
    /// cannot be built.
    pub fn try_new(
        name: impl Into<String>,
        model_id: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let client = create_http_client().ok_or(evaluate_api::Error::HttpClientCreationFailed {
            model: name.clone(),
        })?;
        Ok(Self {
            client,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            name,
            model_id: model_id.into(),
            api_key: api_key.into(),
            org_id: None,
            project_id: None,
            rate_controller: RateController::builder().build(),
        })
    }

    #[must_use]
    pub fn with_organization(mut self, org_id: Option<String>, project_id: Option<String>) -> Self {
        self.org_id = org_id;
        self.project_id = project_id;
        self
    }

    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Arc<RateController>) -> Self {
        self.rate_controller = rate_controller;
        self
    }

    fn authorized(&self, request: RequestBuilder) -> RequestBuilder {
        let mut request = request.bearer_auth(&self.api_key);
        if let Some(org_id) = &self.org_id {
            request = request.header("OpenAI-Organization", org_id);
        }
        if let Some(project_id) = &self.project_id {
            request = request.header("OpenAI-Project", project_id);
        }
        request
    }

    fn error_for(&self, status: StatusCode, body: String) -> evaluate_api::Error {
        let model = self.name.clone();
        match status {
            StatusCode::UNAUTHORIZED => evaluate_api::Error::AuthenticationFailed {
                model,
                message: body,
            },
            StatusCode::FORBIDDEN => evaluate_api::Error::PermissionDenied {
                model,
                message: body,
            },
            StatusCode::NOT_FOUND => evaluate_api::Error::ModelNotFound {
                model,
                message: body,
            },
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                evaluate_api::Error::InvalidRequest {
                    model,
                    message: body,
                }
            }
            StatusCode::TOO_MANY_REQUESTS => evaluate_api::Error::RateLimited {
                model,
                message: body,
            },
            s if s.is_server_error() => evaluate_api::Error::ServiceUnavailable {
                model,
                message: format!("HTTP {s}: {body}"),
            },
            s => evaluate_api::Error::ModelCallFailed {
                model,
                source: format!("HTTP {s}: {body}").into(),
            },
        }
    }
}

#[async_trait]
impl Evaluate for OpenAiDecisions {
    async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse> {
        let body = system_one_to_decision_request(&request, &self.model_id, None).map_err(|e| {
            evaluate_api::Error::InvalidRequest {
                model: self.name.clone(),
                message: e.message,
            }
        })?;

        let _permit = self.rate_controller.acquire().await.map_err(|e| {
            evaluate_api::Error::RatePermitFailed {
                model: self.name.clone(),
                source: Box::new(e),
            }
        })?;

        let response = self
            .authorized(self.client.post(format!("{}/decisions", self.endpoint)))
            .json(&body)
            .send()
            .await
            .map_err(|e| evaluate_api::Error::ServiceUnavailable {
                model: self.name.clone(),
                message: e.to_string(),
            })?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| evaluate_api::Error::ModelCallFailed {
                model: self.name.clone(),
                source: Box::new(e),
            })?;
        if status != StatusCode::OK {
            return Err(self.error_for(status, text));
        }

        let decision: DecisionResponse =
            serde_json::from_str(&text).map_err(|e| evaluate_api::Error::UnparseableResponse {
                model: self.name.clone(),
                response: format!("{e}; body={text}"),
            })?;
        let answered = decision_response_to_system_one(&request, decision).map_err(|detail| {
            evaluate_api::Error::UnparseableResponse {
                model: self.name.clone(),
                response: detail,
            }
        })?;
        // A 200 that drops, re-types, or answers outside its own options is a wrong
        // result, not a success.
        evaluate_api::check_answers(&self.name, &request.questions, &answered)?;
        Ok(answered)
    }

    /// Checks the account can use the model: `GET {endpoint}/models/{model_id}`.
    async fn health(&self) -> Result<()> {
        let response = self
            .authorized(
                self.client
                    .get(format!("{}/models/{}", self.endpoint, self.model_id)),
            )
            .send()
            .await
            .map_err(|e| evaluate_api::Error::HealthCheckFailed {
                source: Box::new(e),
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(evaluate_api::Error::HealthCheckFailed {
            source: format!(
                "OpenAI did not offer model '{}' to this account (HTTP {status}): {body}",
                self.model_id
            )
            .into(),
        })
    }

    fn is_decision_model(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn request() -> EvaluateRequest {
        serde_json::from_value(json!({
            "model": "luna",
            "state": "The package arrived with a broken screen.",
            "questions": {
                "damaged": {"type": "noul", "instructions": "Does the customer report a damaged item?"},
                "team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "Payments", "support": null}}
            }
        }))
        .expect("request")
    }

    fn client(server: &MockServer) -> OpenAiDecisions {
        OpenAiDecisions::try_new(
            "luna",
            "gpt-6-luna",
            format!("{}/v1", server.uri()),
            "sk-test",
        )
        .expect("client")
        .with_organization(Some("org-1".into()), None)
    }

    #[tokio::test]
    async fn answers_through_the_decisions_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/decisions"))
            .and(header("authorization", "Bearer sk-test"))
            .and(header("openai-organization", "org-1"))
            .and(body_json(json!({
                "model": "gpt-6-luna",
                "input": "The package arrived with a broken screen.",
                "questions": [
                    {"type": "predicate", "name": "damaged", "instructions": "Does the customer report a damaged item?"},
                    {"type": "choice", "name": "team", "instructions": "Which team?", "choices": [{"value": "billing", "description": "Payments"}, {"value": "support"}]}
                ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "gpt-6-luna",
                "answers": [
                    {"type": "predicate", "name": "damaged", "probability": 0.95},
                    {"type": "choice", "name": "team", "choice": "support", "probabilities": [{"value": "billing", "probability": 0.1}, {"value": "support", "probability": 0.9}], "confidence": 0.8}
                ],
                "usage": {"input_tokens": 42, "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0}, "output_tokens": 0, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 42}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let answered = client(&server).evaluate(request()).await.expect("answers");
        assert_eq!(
            serde_json::to_value(&answered).expect("serializes"),
            json!({
                "model": "gpt-6-luna",
                "answers": {
                    "damaged": {"type": "noul", "noul": 0.95},
                    "team": {"type": "choice", "choice": "support", "probabilities": {"billing": 0.1, "support": 0.9}, "confidence": 0.8}
                },
                "usage": {"input_tokens": 42, "output_tokens": 0}
            })
        );
    }

    #[tokio::test]
    async fn errors_keep_their_meaning() {
        for (status, expected) in [
            (
                401,
                "Authentication failed for evaluation model 'luna': bad key",
            ),
            (
                429,
                "Rate limited by evaluation provider for model 'luna': bad key",
            ),
            (
                500,
                "Evaluation provider is unavailable for model 'luna': HTTP 500 Internal Server Error: bad key",
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/decisions"))
                .respond_with(ResponseTemplate::new(status).set_body_string("bad key"))
                .mount(&server)
                .await;
            let err = client(&server)
                .evaluate(request())
                .await
                .expect_err("must fail");
            assert_eq!(err.to_string(), expected);
        }
    }

    /// An answer outside the question's options is a wrong result, not a success.
    #[tokio::test]
    async fn an_answer_outside_the_options_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/decisions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "gpt-6-luna",
                "answers": [
                    {"type": "predicate", "name": "damaged", "probability": 0.95},
                    {"type": "choice", "name": "team", "choice": "sales", "probabilities": [{"value": "billing", "probability": 0.1}, {"value": "sales", "probability": 0.9}], "confidence": 0.8}
                ],
                "usage": {"input_tokens": 1, "output_tokens": 0, "total_tokens": 1}
            })))
            .mount(&server)
            .await;
        let err = client(&server)
            .evaluate(request())
            .await
            .expect_err("must not answer outside the options");
        assert!(
            matches!(err, evaluate_api::Error::UnparseableResponse { .. }),
            "{err}"
        );
    }

    #[test]
    fn decision_model_ids() {
        assert!(is_decision_model_id("gpt-6-luna"));
        assert!(is_decision_model_id("gpt-6-luna-2026-10-01"));
        assert!(!is_decision_model_id("gpt-4o-mini"));
    }
}
