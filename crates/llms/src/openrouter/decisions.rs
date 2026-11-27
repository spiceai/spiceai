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

//! `OpenRouter` decision models (such as `baseten/elephant-1`), answering the System One
//! evaluation contract through `OpenRouter`'s Decisions API:
//! `POST {endpoint}/alpha/decisions`.
//!
//! The wire shape is the System One shape (`state` plus a map of `noul` / `choice` /
//! `score` questions), so requests and answers need no translation. Only the usage block
//! differs: `OpenRouter` nests the cached input token count. See
//! <https://openrouter.ai/docs/api/api-reference/alpha-decisions/create-api-alpha-decisions>.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{
    Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question, Result,
};
use reqwest::{Client, StatusCode};
use runtime_rate_control::RateController;
use serde::{Deserialize, Serialize};

use crate::openai::decisions::provider_message;
use crate::provider::create_http_client;

/// The output modality `OpenRouter` lists for a decision model.
const DECISIONS_MODALITY: &str = "decisions";

/// `OpenRouter`'s Overloaded status, which it documents beside 429 for backoff.
const PROVIDER_OVERLOADED: u16 = 529;

/// An `OpenRouter` decision model implementing [`Evaluate`].
pub struct OpenRouterDecisions {
    client: Client,
    /// The API base, such as `https://openrouter.ai/api`.
    endpoint: String,
    /// Spicepod model name (runtime lookup key).
    name: String,
    /// Upstream model id, such as `baseten/elephant-1`.
    model_id: String,
    api_key: String,
    rate_controller: Arc<RateController>,
}

impl Debug for OpenRouterDecisions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenRouterDecisions")
            .field("name", &self.name)
            .field("model_id", &self.model_id)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

/// The request body of `POST /alpha/decisions`.
#[derive(Serialize)]
struct DecisionsRequest<'a> {
    model: &'a str,
    state: &'a EvaluateState,
    questions: BTreeMap<&'a str, &'a Question>,
}

/// The parts of a decisions response that [`EvaluateResponse`] does not read.
#[derive(Deserialize)]
struct UsageDetails {
    #[serde(default)]
    usage: Option<NestedUsage>,
}

#[derive(Deserialize)]
struct NestedUsage {
    #[serde(default)]
    input_tokens_details: Option<InputTokensDetails>,
}

#[derive(Deserialize)]
struct InputTokensDetails {
    cached_tokens: u64,
}

/// The response of `GET /v1/models/{model_id}/endpoints`.
#[derive(Deserialize)]
struct ModelEndpoints {
    data: ModelEndpointsData,
}

#[derive(Deserialize)]
struct ModelEndpointsData {
    architecture: Architecture,
    #[serde(default)]
    endpoints: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct Architecture {
    #[serde(default)]
    output_modalities: Vec<String>,
}

impl OpenRouterDecisions {
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
            rate_controller: RateController::builder().build(),
        })
    }

    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Arc<RateController>) -> Self {
        self.rate_controller = rate_controller;
        self
    }

    fn decisions_url(&self) -> String {
        format!("{}/alpha/decisions", self.endpoint)
    }

    fn model_endpoints_url(&self) -> String {
        format!("{}/v1/models/{}/endpoints", self.endpoint, self.model_id)
    }

    fn error_for(&self, status: StatusCode, body: String) -> evaluate_api::Error {
        let model = self.name.clone();
        let message = provider_message(body);
        match status {
            StatusCode::UNAUTHORIZED => {
                evaluate_api::Error::AuthenticationFailed { model, message }
            }
            // 402 is an exhausted credit balance: the key is valid but may not spend.
            StatusCode::FORBIDDEN | StatusCode::PAYMENT_REQUIRED => {
                evaluate_api::Error::PermissionDenied { model, message }
            }
            StatusCode::NOT_FOUND => evaluate_api::Error::ModelNotFound { model, message },
            StatusCode::BAD_REQUEST
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::UNPROCESSABLE_ENTITY => {
                evaluate_api::Error::InvalidRequest { model, message }
            }
            s if s == StatusCode::TOO_MANY_REQUESTS || s.as_u16() == PROVIDER_OVERLOADED => {
                evaluate_api::Error::RateLimited { model, message }
            }
            s if s.is_server_error() => evaluate_api::Error::ServiceUnavailable {
                model,
                message: format!("HTTP {s}: {message}"),
            },
            s => evaluate_api::Error::ModelCallFailed {
                model,
                source: format!("HTTP {s}: {message}").into(),
            },
        }
    }

    /// Sends `body` to `POST {endpoint}/alpha/decisions` under a rate-limit permit.
    async fn ask(&self, body: &DecisionsRequest<'_>) -> Result<EvaluateResponse> {
        let _permit = self.rate_controller.acquire().await.map_err(|e| {
            evaluate_api::Error::RatePermitFailed {
                model: self.name.clone(),
                source: Box::new(e),
            }
        })?;

        let response = self
            .client
            .post(self.decisions_url())
            .bearer_auth(&self.api_key)
            .json(body)
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

        let unparseable = |e: serde_json::Error| evaluate_api::Error::UnparseableResponse {
            model: self.name.clone(),
            response: format!("{e}; body={text}"),
        };
        let mut parsed = serde_json::from_str::<EvaluateResponse>(&text).map_err(unparseable)?;
        let details = serde_json::from_str::<UsageDetails>(&text).map_err(unparseable)?;
        if let (Some(usage), Some(cached)) = (
            parsed.usage.as_mut(),
            details
                .usage
                .and_then(|u| u.input_tokens_details)
                .map(|d| d.cached_tokens),
        ) {
            usage.cached_tokens = Some(cached);
        }
        Ok(parsed)
    }
}

#[async_trait]
impl Evaluate for OpenRouterDecisions {
    async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse> {
        // A single-option choice is answered here, as every model answers it, not asked.
        let foregone: BTreeMap<String, Answer> = request
            .questions
            .iter()
            .filter_map(|(id, question)| Some((id.clone(), question.foregone_answer()?)))
            .collect();
        let asked: BTreeMap<&str, &Question> = request
            .questions
            .iter()
            .filter(|(id, _)| !foregone.contains_key(*id))
            .map(|(id, question)| (id.as_str(), question))
            .collect();

        let mut answered = if asked.is_empty() {
            EvaluateResponse {
                model: self.model_id.clone(),
                answers: BTreeMap::new(),
                usage: None,
            }
        } else {
            self.ask(&DecisionsRequest {
                model: &self.model_id,
                state: &request.state,
                questions: asked,
            })
            .await?
        };
        answered.answers.extend(foregone);

        // A 200 that drops, re-types, or answers outside its own options is a wrong
        // result, not a success.
        evaluate_api::check_answers(&self.name, &request.questions, &answered)?;
        // Responses name the model the caller asked for, as chat completions do.
        answered.model.clone_from(&self.name);
        Ok(answered)
    }

    /// Checks `OpenRouter` serves the model as a decision model:
    /// `GET {endpoint}/v1/models/{model_id}/endpoints`.
    ///
    /// That route needs no key, so a bad key is reported on the first evaluation.
    async fn health(&self) -> Result<()> {
        let response = self
            .client
            .get(self.model_endpoints_url())
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| evaluate_api::Error::HealthCheckFailed {
                source: Box::new(e),
            })?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(evaluate_api::Error::HealthCheckFailed {
                source: format!(
                    "OpenRouter has no model '{}'. Set `from:` to an OpenRouter decision model, such as `openrouter:baseten/elephant-1`. See: https://openrouter.ai/models?output_modalities=decisions",
                    self.model_id
                )
                .into(),
            });
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(evaluate_api::Error::HealthCheckFailed {
                source: Box::new(self.error_for(status, body)),
            });
        }

        let model = response
            .json::<ModelEndpoints>()
            .await
            .map_err(|e| evaluate_api::Error::HealthCheckFailed {
                source: format!(
                    "could not read the model description from {}: {e}",
                    self.model_endpoints_url()
                )
                .into(),
            })?
            .data;

        let outputs = &model.architecture.output_modalities;
        if !outputs.iter().any(|m| m == DECISIONS_MODALITY) {
            return Err(evaluate_api::Error::HealthCheckFailed {
                source: format!(
                    "OpenRouter model '{}' is not a decision model (it outputs: {}). Only OpenRouter decision models are supported, such as `openrouter:baseten/elephant-1`. See: https://openrouter.ai/models?output_modalities=decisions",
                    self.model_id,
                    outputs.join(", ")
                )
                .into(),
            });
        }
        if model.endpoints.is_empty() {
            return Err(evaluate_api::Error::HealthCheckFailed {
                source: format!(
                    "OpenRouter lists model '{}', but no provider serves it now",
                    self.model_id
                )
                .into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const MODEL_ID: &str = "baseten/elephant-1";

    fn request() -> EvaluateRequest {
        serde_json::from_value(json!({
            "model": "elephant",
            "state": {"ticket": "My checkout page shows a blank screen after I click Pay."},
            "questions": {
                "is_bug": {
                    "type": "noul",
                    "instructions": "Is the customer reporting a software defect?",
                    "criteria": {"true": "Broken behavior.", "false": "A question."}
                },
                "team": {
                    "type": "choice",
                    "instructions": "Which team should own this ticket?",
                    "criteria": {"frontend": "Rendering issues.", "payments": "Checkout issues."}
                },
                "urgency": {
                    "type": "score",
                    "instructions": "How urgent is this ticket?",
                    "criteria": ["Can wait", "This week", "Now"]
                }
            }
        }))
        .expect("request")
    }

    fn client(server: &MockServer) -> OpenRouterDecisions {
        OpenRouterDecisions::try_new(
            "elephant",
            MODEL_ID,
            format!("{}/api", server.uri()),
            "sk-or-test",
        )
        .expect("client")
    }

    /// The response example from `OpenRouter`'s Decisions API reference, with the
    /// documented optional fields (`id`, `provider`, `cost`, cached tokens).
    fn documented_response() -> serde_json::Value {
        json!({
            "id": "gen-dec-1789738314-X5e5eKGQdvR9rblyX250",
            "model": "baseten/elephant-1-20261021",
            "provider": "BaseTen",
            "answers": {
                "is_bug": {"type": "noul", "noul": 0.96},
                "team": {
                    "type": "choice",
                    "choice": "payments",
                    "confidence": 0.75,
                    "probabilities": {"frontend": 0.16, "payments": 0.84}
                },
                "urgency": {
                    "type": "score",
                    "score": 1.99,
                    "confidence": 0.99,
                    "legend": {"0": "Can wait", "1": "This week", "2": "Now"},
                    "probabilities": {"0": 0.0, "1": 0.01, "2": 0.99}
                }
            },
            "usage": {
                "cost": 0.000_019_992,
                "input_tokens": 476,
                "output_tokens": 70,
                "input_tokens_details": {"cached_tokens": 128}
            }
        })
    }

    #[tokio::test]
    async fn answers_through_the_decisions_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/alpha/decisions"))
            .and(header("authorization", "Bearer sk-or-test"))
            .and(body_json(json!({
                "model": MODEL_ID,
                "state": {"ticket": "My checkout page shows a blank screen after I click Pay."},
                "questions": {
                    "is_bug": {
                        "type": "noul",
                        "instructions": "Is the customer reporting a software defect?",
                        "criteria": {"true": "Broken behavior.", "false": "A question."}
                    },
                    "team": {
                        "type": "choice",
                        "instructions": "Which team should own this ticket?",
                        "criteria": {"frontend": "Rendering issues.", "payments": "Checkout issues."}
                    },
                    "urgency": {
                        "type": "score",
                        "instructions": "How urgent is this ticket?",
                        "criteria": ["Can wait", "This week", "Now"]
                    }
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(documented_response()))
            .expect(1)
            .mount(&server)
            .await;

        let response = client(&server).evaluate(request()).await.expect("evaluate");

        assert_eq!(
            response.model, "elephant",
            "the Spicepod name, not the snapshot"
        );
        assert_eq!(
            response.answers.get("is_bug"),
            Some(&Answer::Noul { noul: 0.96 })
        );
        assert!(matches!(
            response.answers.get("team"),
            Some(Answer::Choice { choice, .. }) if choice == "payments"
        ));
        assert!(matches!(
            response.answers.get("urgency"),
            Some(Answer::Score { score, .. }) if (*score - 1.99).abs() < f64::EPSILON
        ));
        let usage = response.usage.expect("usage");
        assert_eq!(usage.input_tokens, 476);
        assert_eq!(usage.output_tokens, 70);
        assert_eq!(usage.cached_tokens, Some(128));
    }

    #[tokio::test]
    async fn a_refusal_stands_beside_other_answers() {
        let server = MockServer::start().await;
        let mut body = documented_response();
        body["answers"]["team"] = json!({"type": "refusal"});
        Mock::given(method("POST"))
            .and(path("/api/alpha/decisions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let response = client(&server).evaluate(request()).await.expect("evaluate");
        assert_eq!(response.answers.get("team"), Some(&Answer::Refusal {}));
        assert!(matches!(
            response.answers.get("is_bug"),
            Some(Answer::Noul { .. })
        ));
    }

    #[tokio::test]
    async fn a_foregone_answer_needs_no_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let request: EvaluateRequest = serde_json::from_value(json!({
            "model": "elephant",
            "state": "anything",
            "questions": {"only": {"type": "choice", "criteria": {"yes": null}}}
        }))
        .expect("request");
        let response = client(&server).evaluate(request).await.expect("evaluate");
        assert!(matches!(
            response.answers.get("only"),
            Some(Answer::Choice { choice, confidence, .. })
                if choice == "yes" && (*confidence - 1.0).abs() < f64::EPSILON
        ));
        assert_eq!(response.usage, None);
    }

    #[tokio::test]
    async fn an_answer_to_a_question_not_asked_is_refused() {
        let server = MockServer::start().await;
        let mut body = documented_response();
        body["answers"]["extra"] = json!({"type": "noul", "noul": 0.5});
        Mock::given(method("POST"))
            .and(path("/api/alpha/decisions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let err = client(&server)
            .evaluate(request())
            .await
            .expect_err("an answer to a question not asked must fail");
        assert!(err.to_string().contains("extra"), "{err}");
    }

    #[tokio::test]
    async fn provider_errors_keep_their_meaning() {
        for (status, expected) in [
            (400, "Invalid evaluation request"),
            (401, "Authentication failed"),
            (402, "Permission denied"),
            (404, "was not found upstream"),
            (413, "Invalid evaluation request"),
            (429, "Rate limited"),
            (529, "Rate limited"),
            (502, "unavailable"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/alpha/decisions"))
                .respond_with(ResponseTemplate::new(status).set_body_json(json!({
                    "error": {"code": status, "message": "provider says no"}
                })))
                .mount(&server)
                .await;

            let err = client(&server)
                .evaluate(request())
                .await
                .expect_err("a non-200 must fail");
            let text = err.to_string();
            assert!(text.contains(expected), "HTTP {status}: {text}");
            assert!(
                text.contains("provider says no") && !text.contains("\"error\""),
                "HTTP {status} must relay the provider message, not its envelope: {text}"
            );
        }
    }

    fn endpoints_body(output_modalities: &[&str], endpoints: usize) -> serde_json::Value {
        json!({"data": {
            "id": MODEL_ID,
            "architecture": {"output_modalities": output_modalities},
            "endpoints": (0..endpoints).map(|i| json!({"name": format!("ep-{i}")})).collect::<Vec<_>>()
        }})
    }

    #[tokio::test]
    async fn health_accepts_a_served_decision_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/models/{MODEL_ID}/endpoints")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(endpoints_body(&["decisions"], 1)),
            )
            .mount(&server)
            .await;
        client(&server).health().await.expect("healthy");
    }

    #[tokio::test]
    async fn health_refuses_what_cannot_answer_decisions() {
        for (status, body, expected) in [
            (
                404,
                json!({"error": {"code": 404, "message": "Not Found"}}),
                "OpenRouter has no model",
            ),
            (
                200,
                endpoints_body(&["text"], 3),
                "is not a decision model (it outputs: text)",
            ),
            (
                200,
                endpoints_body(&["decisions"], 0),
                "no provider serves it now",
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!("/api/v1/models/{MODEL_ID}/endpoints")))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&server)
                .await;
            let err = client(&server)
                .health()
                .await
                .expect_err("must be unhealthy");
            let text = format!("{err}");
            assert!(text.contains(expected), "{text}");
        }
    }
}
