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

//! `TypeSafe` System One evaluation provider (Jev).
//!
//! Jev is **not** a chat LLM. It evaluates `state` against typed questions via
//! `POST https://api.typesafe.ai/v1/systemone` and returns structured answers
//! (noul / choice / score) with calibrated probabilities.
//!
//! See <https://docs.typesafe.ai/api> and
//! <https://typesafe.ai/blog/introducing-system-one-models-and-jev>.

mod list_models;

pub use list_models::TypeSafeModelLister;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{
    AuthenticationFailedSnafu, Evaluate, EvaluateRequest, EvaluateResponse, HealthCheckFailedSnafu,
    InvalidRequestSnafu, ModelCallFailedSnafu, ModelNotFoundSnafu, RateLimitedSnafu,
    RatePermitFailedSnafu, Result,
};
use reqwest::{Client, StatusCode};
use runtime_rate_control::RateController;
use snafu::ResultExt;

use crate::provider::create_http_client;

/// Default `TypeSafe` API base URL (direct API, not the Vercel AI gateway).
pub const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai";
/// Default model alias when the Spicepod uses `from: typesafe:jev`.
pub const DEFAULT_MODEL: &str = "jev-latest";

/// `TypeSafe` System One client implementing [`Evaluate`].
pub struct TypeSafe {
    client: Client,
    base_url: String,
    /// Spicepod model name (runtime lookup key).
    name: String,
    /// Upstream model id sent in the System One request body.
    model_id: String,
    api_key: String,
    rate_controller: Arc<RateController>,
}

impl Debug for TypeSafe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeSafe")
            .field("name", &self.name)
            .field("model_id", &self.model_id)
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl TypeSafe {
    /// Build a `TypeSafe` client.
    ///
    /// # Errors
    ///
    /// Returns [`evaluate_api::Error::HttpClientCreationFailed`] when the HTTP client cannot be built.
    ///
    /// `model_id` is the Spicepod `from:` suffix (`jev`, `jev-latest`,
    /// `jev-1.13.0`, …). Bare `jev` is normalized to [`DEFAULT_MODEL`].
    pub fn try_new(
        name: impl Into<String>,
        model_id: Option<&str>,
        api_key: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let client = create_http_client().ok_or(evaluate_api::Error::HttpClientCreationFailed {
            model: name.clone(),
        })?;
        Ok(Self {
            client,
            base_url: DEFAULT_BASE_URL.to_string(),
            name,
            model_id: normalize_model_id(model_id),
            api_key: api_key.into(),
            rate_controller: RateController::builder().build(),
        })
    }

    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Arc<RateController>) -> Self {
        self.rate_controller = rate_controller;
        self
    }

    fn systemone_url(&self) -> String {
        format!("{}/v1/systemone", self.base_url)
    }

    fn models_url(&self) -> String {
        format!("{}/v1/models", self.base_url)
    }
}

/// Map Spicepod model id suffixes onto `TypeSafe` aliases.
///
/// `jev` → `jev-latest`; other ids (including versioned pins) pass through.
#[must_use]
pub fn normalize_model_id(model_id: Option<&str>) -> String {
    match model_id.map(str::trim).filter(|s| !s.is_empty()) {
        None | Some("jev") => DEFAULT_MODEL.to_string(),
        Some(id) => id.to_string(),
    }
}

#[async_trait]
impl Evaluate for TypeSafe {
    async fn evaluate(&self, mut request: EvaluateRequest) -> Result<EvaluateResponse> {
        if request.questions.is_empty() {
            return InvalidRequestSnafu {
                model: self.name.clone(),
                message: "questions map must contain at least one question",
            }
            .fail();
        }

        let _permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(RatePermitFailedSnafu {
                model: self.name.clone(),
            })?;

        // Always send the upstream model id, not the Spicepod component name.
        request.model = self.model_id.clone();

        let response = self
            .client
            .post(self.systemone_url())
            .bearer_auth(&self.api_key)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ModelCallFailedSnafu {
                model: self.name.clone(),
            })?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ModelCallFailedSnafu {
                model: self.name.clone(),
            })?;

        match status {
            StatusCode::OK => serde_json::from_str::<EvaluateResponse>(&body).map_err(|e| {
                evaluate_api::Error::UnparseableResponse {
                    model: self.name.clone(),
                    response: format!("{e}; body={body}"),
                }
            }),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => AuthenticationFailedSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            StatusCode::NOT_FOUND => ModelNotFoundSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            StatusCode::UNPROCESSABLE_ENTITY | StatusCode::BAD_REQUEST => InvalidRequestSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            StatusCode::TOO_MANY_REQUESTS => RateLimitedSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            // TypeSafe documents 529 Overloaded alongside 429 for backoff.
            s if s.as_u16() == 529 => RateLimitedSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            _ => Err(evaluate_api::Error::ModelCallFailed {
                model: self.name.clone(),
                source: format!("HTTP {status}: {body}").into(),
            }),
        }
    }

    fn model_name(&self) -> &str {
        &self.name
    }

    fn provider_model_id(&self) -> &str {
        &self.model_id
    }

    async fn health(&self) -> Result<()> {
        let response = self
            .client
            .get(self.models_url())
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(HealthCheckFailedSnafu)?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(evaluate_api::Error::HealthCheckFailed {
                source: format!("HTTP {status}: {body}").into(),
            })
        }
    }
}

/// Clear error when a caller attempts chat completions against a `TypeSafe` model.
#[must_use]
pub fn chat_not_supported_message(model_name: &str) -> String {
    format!(
        "Model '{model_name}' is a TypeSafe System One evaluation model (Jev) and does not support chat completions. \
         Use POST /v1/evaluate with `state` and typed `questions` instead. \
         See https://spiceai.org/docs/components/models/typesafe and https://docs.typesafe.ai/api."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use evaluate_api::{Answer, EntryType, Question};
    use serde_json::json;
    use std::collections::BTreeMap;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn normalize_jev_alias() {
        assert_eq!(normalize_model_id(None), "jev-latest");
        assert_eq!(normalize_model_id(Some("jev")), "jev-latest");
        assert_eq!(normalize_model_id(Some("jev-latest")), "jev-latest");
        assert_eq!(normalize_model_id(Some("jev-1.13.0")), "jev-1.13.0");
        assert_eq!(normalize_model_id(Some("jev-preview")), "jev-preview");
    }

    #[tokio::test]
    async fn evaluate_posts_systemone_shape() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(header("Authorization", "Bearer test-key"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "jev-1.13.0",
                "answers": {
                    "is_urgent": { "type": "noul", "noul": 0.92 },
                    "department": {
                        "type": "choice",
                        "choice": "technical",
                        "probabilities": { "billing": 0.08, "technical": 0.85, "sales": 0.07 },
                        "confidence": 0.82
                    },
                    "frustration": {
                        "type": "score",
                        "score": 1.6,
                        "legend": { "0": "Calm", "1": "Frustrated", "2": "Very angry" },
                        "probabilities": { "0": 0.05, "1": 0.3, "2": 0.65 },
                        "confidence": 0.78
                    }
                },
                "usage": { "input_tokens": 312, "output_tokens": 48 }
            })))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev"), "test-key")
            .expect("client")
            .with_base_url(server.uri());

        let mut questions = BTreeMap::new();
        questions.insert(
            "is_urgent".to_string(),
            Question::Noul {
                instructions: "Does this convey urgency?".into(),
                criteria: None,
            },
        );
        questions.insert(
            "department".to_string(),
            Question::Choice {
                instructions: "Which team should handle this?".into(),
                criteria: BTreeMap::from([
                    ("billing".into(), EntryType::from("Payments")),
                    ("technical".into(), EntryType::from("Bugs")),
                    ("sales".into(), EntryType::Null),
                ]),
            },
        );
        questions.insert(
            "frustration".to_string(),
            Question::Score {
                instructions: "How frustrated is the customer?".into(),
                criteria: vec!["Calm".into(), "Frustrated".into(), "Very angry".into()],
            },
        );

        let resp = client
            .evaluate(EvaluateRequest {
                model: "jev".into(), // spicepod name; provider replaces with jev-latest
                state: json!("Help! My payouts have been failing for 3 days."),
                questions,
            })
            .await
            .expect("evaluate succeeds");

        assert_eq!(resp.model, "jev-1.13.0");
        assert!(matches!(
            resp.answers.get("is_urgent"),
            Some(Answer::Noul { noul }) if (*noul - 0.92).abs() < f64::EPSILON
        ));
        assert!(matches!(
            resp.answers.get("department"),
            Some(Answer::Choice { choice, confidence, .. })
                if choice == "technical" && (*confidence - 0.82).abs() < f64::EPSILON
        ));
        assert!(matches!(
            resp.answers.get("frustration"),
            Some(Answer::Score { score, confidence, .. })
                if (*score - 1.6).abs() < f64::EPSILON && (*confidence - 0.78).abs() < f64::EPSILON
        ));
        assert_eq!(resp.usage.as_ref().map(|u| u.input_tokens), Some(312));

        // Verify the mock received the normalized upstream model id.
        let received = &server.received_requests().await.expect("requests")[0];
        let body: serde_json::Value = serde_json::from_slice(&received.body).expect("json");
        assert_eq!(body["model"], "jev-latest");
    }

    #[tokio::test]
    async fn evaluate_rejects_empty_questions() {
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "test-key").expect("client");
        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: json!("x"),
                questions: BTreeMap::new(),
            })
            .await
            .expect_err("empty questions");
        assert!(matches!(err, evaluate_api::Error::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn evaluate_maps_401() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid key"))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev-latest"), "bad")
            .expect("client")
            .with_base_url(server.uri());

        let mut questions = BTreeMap::new();
        questions.insert(
            "q".into(),
            Question::Noul {
                instructions: "yes?".into(),
                criteria: None,
            },
        );

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: json!("s"),
                questions,
            })
            .await
            .expect_err("401");
        assert!(matches!(
            err,
            evaluate_api::Error::AuthenticationFailed { .. }
        ));
    }

    #[tokio::test]
    async fn evaluate_maps_404() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(404).set_body_string("model not found"))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev-does-not-exist"), "key")
            .expect("client")
            .with_base_url(server.uri());

        let mut questions = BTreeMap::new();
        questions.insert(
            "q".into(),
            Question::Noul {
                instructions: "yes?".into(),
                criteria: None,
            },
        );

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: json!("s"),
                questions,
            })
            .await
            .expect_err("404");
        assert!(matches!(err, evaluate_api::Error::ModelNotFound { .. }));
    }

    #[test]
    fn chat_rejection_mentions_evaluate() {
        let msg = chat_not_supported_message("jev");
        assert!(msg.contains("POST /v1/evaluate"));
        assert!(msg.contains("does not support chat"));
    }
}
