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

use list_models::ModelsResponse;
pub use list_models::TypeSafeModelLister;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{
    Answer, AuthenticationFailedSnafu, Evaluate, EvaluateRequest, EvaluateResponse,
    HealthCheckFailedSnafu, InvalidRequestSnafu, ModelCallFailedSnafu, ModelNotFoundSnafu,
    PermissionDeniedSnafu, Question, RateLimitedSnafu, RatePermitFailedSnafu, Result,
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

    /// Every question asked must come back answered, with an answer of the matching
    /// kind and a value inside the domain the question defined. A 200 that drops,
    /// re-types, or answers outside its own options is a wrong result, not a success,
    /// so it is surfaced as an unparseable response rather than published.
    fn ensure_answers_match(
        &self,
        asked: &BTreeMap<String, Question>,
        response: &EvaluateResponse,
    ) -> Result<()> {
        let bad = |detail: String| evaluate_api::Error::UnparseableResponse {
            model: self.name.clone(),
            response: detail,
        };

        let missing: Vec<&str> = asked
            .keys()
            .filter(|id| !response.answers.contains_key(*id))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(bad(format!(
                "no answer for question(s): {}",
                missing.join(", ")
            )));
        }

        for (id, answer) in &response.answers {
            let Some(question) = asked.get(id) else {
                return Err(bad(format!(
                    "answer for question '{id}', which was not asked"
                )));
            };
            let (expected, got) = (question_kind(question), answer_kind(answer));
            if expected != got {
                return Err(bad(format!(
                    "question '{id}' is a {expected} question but the answer is a {got}"
                )));
            }

            match (question, answer) {
                (_, Answer::Noul { noul }) => {
                    if !is_probability(*noul) {
                        return Err(bad(format!(
                            "question '{id}': noul {noul} is outside [0, 1]"
                        )));
                    }
                }
                (
                    Question::Choice { criteria, .. },
                    Answer::Choice {
                        choice,
                        probabilities,
                        confidence,
                    },
                ) => {
                    if !criteria.contains_key(choice) {
                        return Err(bad(format!(
                            "question '{id}': answer '{choice}' is not one of its options"
                        )));
                    }
                    check_distribution(id, probabilities, *confidence, |key| {
                        criteria.contains_key(key)
                    })
                    .map_err(bad)?;
                }
                (
                    Question::Score { criteria, .. },
                    Answer::Score {
                        score,
                        legend,
                        probabilities,
                        confidence,
                    },
                ) => {
                    let Some(top_idx) = criteria.len().checked_sub(1) else {
                        return Err(bad(format!(
                            "question '{id}': score criteria must contain at least one level"
                        )));
                    };
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "score criteria are bounded at ten levels"
                    )]
                    let top = top_idx as f64;
                    if !score.is_finite() || *score < 0.0 || *score > top {
                        return Err(bad(format!(
                            "question '{id}': score {score} is outside [0, {top}]"
                        )));
                    }
                    for key in legend.keys() {
                        if key.parse::<usize>().ok().is_none_or(|idx| idx > top_idx) {
                            return Err(bad(format!(
                                "question '{id}': legend key '{key}' is not in the score rubric [0, {top_idx}]"
                            )));
                        }
                    }
                    check_distribution(id, probabilities, *confidence, |key| {
                        legend.contains_key(key)
                            || key
                                .parse::<usize>()
                                .ok()
                                .is_some_and(|idx| idx <= top_idx)
                    })
                    .map_err(bad)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Whether the id names an explicit version (`jev-1.13.0`) rather than an alias.
///
/// `TypeSafe` accepts versioned pins that `GET /v1/models` need not list, so a pin is
/// never treated as missing.
fn is_version_pinned(model_id: &str) -> bool {
    model_id.rsplit_once('-').is_some_and(|(_, tail)| {
        tail.split('.')
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
            && tail.contains('.')
    })
}

/// `TypeSafe` reports probabilities and confidence as values in `[0, 1]`.
fn is_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

/// Confidence and every probability in the distribution must be a probability,
/// and each probability key must belong to the question's domain.
fn check_distribution(
    id: &str,
    probabilities: &BTreeMap<String, f64>,
    confidence: f64,
    allowed_key: impl Fn(&str) -> bool,
) -> std::result::Result<(), String> {
    if !is_probability(confidence) {
        return Err(format!(
            "question '{id}': confidence {confidence} is outside [0, 1]"
        ));
    }
    for (key, p) in probabilities {
        if !allowed_key(key) {
            return Err(format!(
                "question '{id}': probability key '{key}' is not in the question's domain"
            ));
        }
        if !is_probability(*p) {
            return Err(format!(
                "question '{id}': probability for '{key}' is {p}, outside [0, 1]"
            ));
        }
    }
    Ok(())
}

/// The primitive a question asks for, used to check the answer that comes back.
fn question_kind(question: &Question) -> &'static str {
    match question {
        Question::Noul { .. } => "noul",
        Question::Choice { .. } => "choice",
        Question::Score { .. } => "score",
    }
}

/// The primitive an answer carries.
fn answer_kind(answer: &Answer) -> &'static str {
    match answer {
        Answer::Noul { .. } => "noul",
        Answer::Choice { .. } => "choice",
        Answer::Score { .. } => "score",
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
            .with_context(|_| RatePermitFailedSnafu {
                model: self.name.clone(),
            })?;

        // Always send the upstream model id, not the Spicepod component name.
        request.model = self.model_id.clone();

        let asked = request.questions.clone();

        let response = self
            .client
            .post(self.systemone_url())
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .with_context(|_| ModelCallFailedSnafu {
                model: self.name.clone(),
            })?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .with_context(|_| ModelCallFailedSnafu {
                model: self.name.clone(),
            })?;

        match status {
            StatusCode::OK => {
                let parsed = serde_json::from_str::<EvaluateResponse>(&body).map_err(|e| {
                    evaluate_api::Error::UnparseableResponse {
                        model: self.name.clone(),
                        response: format!("{e}; body={body}"),
                    }
                })?;
                self.ensure_answers_match(&asked, &parsed)?;
                Ok(parsed)
            }
            StatusCode::UNAUTHORIZED => AuthenticationFailedSnafu {
                model: self.name.clone(),
                message: body,
            }
            .fail(),
            StatusCode::FORBIDDEN => PermissionDeniedSnafu {
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

            // TypeSafe documents 529 Overloaded alongside 429 for backoff.
            s if s == StatusCode::TOO_MANY_REQUESTS || s.as_u16() == 529 => RateLimitedSnafu {
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

    async fn health(&self) -> Result<()> {
        let response = self
            .client
            .get(self.models_url())
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .with_context(|_| HealthCheckFailedSnafu)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(evaluate_api::Error::HealthCheckFailed {
                source: format!("HTTP {status}: {body}").into(),
            });
        }

        // A reachable endpoint is not the same as a usable model: without this, a typo
        // such as `typesafe:jev-does-not-exist` loads Ready and fails every evaluation.
        // A listing we cannot decode is a broken endpoint, not an empty account: only a
        // successfully decoded response may take the permissive path below.
        let listed = response
            .json::<ModelsResponse>()
            .await
            .map(ModelsResponse::into_names)
            .map_err(|e| evaluate_api::Error::HealthCheckFailed {
                source: format!(
                    "could not read the model list from {}: {e}",
                    self.models_url()
                )
                .into(),
            })?;

        // An empty or unreadable list is not evidence the model is missing, and a
        // versioned pin is accepted by TypeSafe even when only aliases are listed.
        if listed.is_empty() || is_version_pinned(&self.model_id) {
            return Ok(());
        }
        if listed.iter().any(|name| name == &self.model_id) {
            return Ok(());
        }
        Err(evaluate_api::Error::HealthCheckFailed {
            source: format!(
                "model '{}' is not offered by TypeSafe for this account (available: {}). Set `from:` to one of those, or to a versioned pin such as `typesafe:jev-1.13.0`. See: https://docs.typesafe.ai/models",
                self.model_id,
                listed.join(", ")
            )
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evaluate_api::{Answer, EntryType, EvaluateState, Question};
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
                state: EvaluateState::from("Help! My payouts have been failing for 3 days."),
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
                state: EvaluateState::from("x"),
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
                state: EvaluateState::from("s"),
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
    async fn evaluate_maps_403() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev-latest"), "key")
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
                state: EvaluateState::from("s"),
                questions,
            })
            .await
            .expect_err("403");
        assert!(matches!(err, evaluate_api::Error::PermissionDenied { .. }));
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
                state: EvaluateState::from("s"),
                questions,
            })
            .await
            .expect_err("404");
        assert!(matches!(err, evaluate_api::Error::ModelNotFound { .. }));
    }
    fn noul_question(id: &str) -> BTreeMap<String, Question> {
        BTreeMap::from([(
            id.to_string(),
            Question::Noul {
                instructions: "urgent?".into(),
                criteria: None,
            },
        )])
    }

    async fn systemone_returning(server: &MockServer, body: serde_json::Value) {
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    /// A 200 that omits an answer is a wrong result, not a success.
    #[tokio::test]
    async fn evaluate_rejects_a_response_missing_an_answer() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"other": {"type": "noul", "noul": 0.5}}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: noul_question("q"),
            })
            .await
            .expect_err("a missing answer must not be published as success");
        let msg = err.to_string();
        assert!(msg.contains("no answer for question(s): q"), "{msg}");
    }

    /// An answer of the wrong primitive is equally a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_an_answer_of_the_wrong_kind() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "a",
                "probabilities": {"a": 1.0}, "confidence": 0.9
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: noul_question("q"),
            })
            .await
            .expect_err("a mismatched answer kind must not be published as success");
        let msg = err.to_string();
        assert!(
            msg.contains("is a noul question but the answer is a choice"),
            "{msg}"
        );
    }

    /// Health must reject a configured model the account cannot use, rather than
    /// marking it Ready and failing every later evaluation.
    #[tokio::test]
    async fn health_rejects_a_model_the_account_does_not_have() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"models": [{"name": "jev-latest"}, {"name": "jev-preview"}]}),
            ))
            .mount(&server)
            .await;

        let bad = TypeSafe::try_new("jev", Some("jev-does-not-exist"), "k")
            .expect("client")
            .with_base_url(server.uri());
        let err = bad
            .health()
            .await
            .expect_err("unlisted model must fail health");
        assert!(
            err.to_string().contains("is not offered by TypeSafe"),
            "{err}"
        );

        let good = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());
        good.health().await.expect("a listed alias is healthy");

        // TypeSafe accepts versioned pins that the listing need not advertise.
        let pinned = TypeSafe::try_new("jev", Some("jev-1.13.0"), "k")
            .expect("client")
            .with_base_url(server.uri());
        pinned.health().await.expect("a versioned pin is healthy");
    }
    fn choice_question(id: &str) -> BTreeMap<String, Question> {
        BTreeMap::from([(
            id.to_string(),
            Question::Choice {
                instructions: "which team?".into(),
                criteria: BTreeMap::from([
                    ("billing".to_string(), EntryType::String("pay".into())),
                    ("technical".to_string(), EntryType::String("bugs".into())),
                ]),
            },
        )])
    }

    /// A choice outside the options the question defined is a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_a_choice_outside_the_offered_options() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "legal",
                "probabilities": {"legal": 1.0}, "confidence": 0.9
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: choice_question("q"),
            })
            .await
            .expect_err("an out-of-domain choice must not be published");
        let msg = err.to_string();
        assert!(msg.contains("'legal' is not one of its options"), "{msg}");
    }

    /// A noul outside its documented [0, 1] range is a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_a_noul_outside_its_range() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {"type": "noul", "noul": 1.7}}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: noul_question("q"),
            })
            .await
            .expect_err("an out-of-range noul must not be published");
        assert!(err.to_string().contains("outside [0, 1]"), "{err}");
    }

    /// Confidence outside [0, 1] is equally a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_confidence_outside_its_range() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 1.0}, "confidence": 4.2
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: choice_question("q"),
            })
            .await
            .expect_err("out-of-range confidence must not be published");
        assert!(
            err.to_string().contains("confidence 4.2 is outside"),
            "{err}"
        );
    }

    /// Probability keys must stay inside the question's own domain.
    #[tokio::test]
    async fn evaluate_rejects_probability_keys_outside_the_question_domain() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.9, "legal": 0.1}, "confidence": 0.9
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: choice_question("q"),
            })
            .await
            .expect_err("an unknown probability key must not be published");
        let msg = err.to_string();
        assert!(
            msg.contains("probability key 'legal' is not in the question's domain"),
            "{msg}"
        );
    }

    /// Empty score criteria via the Rust API must not panic on `len - 1`.
    #[tokio::test]
    async fn evaluate_rejects_empty_score_criteria_without_panicking() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 0.0,
                "legend": {"0": "low"},
                "probabilities": {"0": 1.0}, "confidence": 0.9
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let mut questions = BTreeMap::new();
        questions.insert(
            "q".into(),
            Question::Score {
                instructions: "how bad?".into(),
                criteria: vec![],
            },
        );
        let err = client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions,
            })
            .await
            .expect_err("empty score criteria must fail closed");
        assert!(
            err.to_string().contains("score criteria must contain at least one level"),
            "{err}"
        );
    }

    /// A valid in-domain answer still succeeds.
    #[tokio::test]
    async fn evaluate_accepts_a_well_formed_choice() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "technical",
                "probabilities": {"billing": 0.1, "technical": 0.9}, "confidence": 0.88
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions: choice_question("q"),
            })
            .await
            .expect("a well-formed answer is still accepted");
    }
}
