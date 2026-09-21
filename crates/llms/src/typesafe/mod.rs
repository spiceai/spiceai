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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{
    Answer, AuthenticationFailedSnafu, Evaluate, EvaluateRequest, EvaluateResponse,
    HealthCheckFailedSnafu, InvalidRequestSnafu, ModelCallFailedSnafu, ModelNotFoundSnafu,
    PermissionDeniedSnafu, Question, RateLimitedSnafu, RatePermitFailedSnafu, Result,
    ServiceUnavailableSnafu,
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
                    check_distribution(
                        id,
                        probabilities,
                        *confidence,
                        criteria.keys().map(String::as_str),
                    )
                    .map_err(bad)?;
                    // A choice answer is the selected option. Another option with a
                    // strictly higher probability contradicts that selection; an exact
                    // tie among the max remains valid.
                    let Some(&chosen_p) = probabilities.get(choice) else {
                        return Err(bad(format!(
                            "question '{id}': answer '{choice}' is missing from the distribution"
                        )));
                    };
                    if probabilities
                        .iter()
                        .any(|(option, p)| option != choice && *p > chosen_p)
                    {
                        return Err(bad(format!(
                            "question '{id}': answer '{choice}' is not a highest-probability option"
                        )));
                    }
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
                    // A sparse legend is valid: every supplied key must be in range,
                    // but not every rubric level needs a description. The probability
                    // distribution still covers the full rubric.
                    let domain: Vec<String> = (0..=top_idx).map(|i| i.to_string()).collect();
                    check_distribution(
                        id,
                        probabilities,
                        *confidence,
                        domain.iter().map(String::as_str),
                    )
                    .map_err(bad)?;
                    // `TypeSafe` defines `score` as the probability-weighted average of
                    // the rubric indices. A value that contradicts the distribution is
                    // a wrong result, not a successful evaluation; the slack absorbs
                    // only the rounding a valid response carries.
                    let weighted: f64 = probabilities
                        .iter()
                        .map(|(key, p)| key.parse::<f64>().unwrap_or(f64::NAN) * p)
                        .sum();
                    if !weighted.is_finite()
                        || (score - weighted).abs() > weighted_score_tolerance(top)
                    {
                        return Err(bad(format!(
                            "question '{id}': score {score} is not the probability-weighted average ({weighted})"
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Whether the id is a `jev-<numeric version>` pin (`jev-1.13.0`) rather than an alias.
///
/// `TypeSafe` accepts versioned Jev pins that `GET /v1/models` need not list, so a
/// pin is never treated as missing. Only the documented `jev-` prefix qualifies —
/// `foo-1.2.3` is not a Jev pin and must fail health instead of loading Ready.
fn is_version_pinned(model_id: &str) -> bool {
    model_id.strip_prefix("jev-").is_some_and(|version| {
        version.contains('.')
            && version
                .split('.')
                .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
    })
}

/// `TypeSafe` reports probabilities and confidence as values in `[0, 1]`.
fn is_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

/// Confidence and every probability in the distribution must be a probability.
/// The distribution must cover exactly the question's domain — no missing keys,
/// no extras.
///
/// `TypeSafe` documents probabilities as summing to approximately 1. Responses
/// are commonly rounded to two decimal places (`0.33 + 0.33 + 0.33 = 0.99`).
/// The slack is slightly above 0.01 so that exact 0.01 shortfall is accepted
/// despite floating-point representation of `1.0 - 0.99`.
const PROBABILITY_SUM_TOLERANCE: f64 = 0.011;

/// Half the step of the two-decimal rounding that responses commonly carry.
const ROUNDING_HALF_STEP: f64 = 0.005;

/// How far a reported score may sit from the weighted average recomputed from the
/// reported probabilities, for a rubric whose top index is `top`.
///
/// Rounding each probability by up to half a step moves that average by up to half a
/// step times the level's index, and the score may itself be rounded, so the bound is
/// half a step times the sum of the indices plus one. A tolerance sized for a sum of
/// probabilities in [0, 1] is too tight once the score spans [0, top]: on a ten-level
/// rubric, rounding alone can move the average by more than 0.2.
fn weighted_score_tolerance(top: f64) -> f64 {
    let index_sum = top * (top + 1.0) / 2.0;
    // The same floating-point allowance `PROBABILITY_SUM_TOLERANCE` carries.
    ROUNDING_HALF_STEP * (index_sum + 1.0) + 0.001
}

fn check_distribution<'a>(
    id: &str,
    probabilities: &BTreeMap<String, f64>,
    confidence: f64,
    domain: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<(), String> {
    if !is_probability(confidence) {
        return Err(format!(
            "question '{id}': confidence {confidence} is outside [0, 1]"
        ));
    }
    let domain: BTreeSet<&str> = domain.into_iter().collect();
    for key in &domain {
        if !probabilities.contains_key(*key) {
            return Err(format!(
                "question '{id}': probability key '{key}' is missing from the distribution"
            ));
        }
    }
    for (key, p) in probabilities {
        if !domain.contains(key.as_str()) {
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
    let sum: f64 = probabilities.values().sum();
    if (sum - 1.0).abs() > PROBABILITY_SUM_TOLERANCE {
        return Err(format!(
            "question '{id}': probabilities sum to {sum}, which is not a distribution over [0, 1]"
        ));
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
            StatusCode::SERVICE_UNAVAILABLE => ServiceUnavailableSnafu {
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

        // A versioned pin is accepted by TypeSafe even when only aliases are listed. A
        // decoded empty list is not that: it says the account is offered nothing, so it
        // reaches the error below rather than reporting an alias ready.
        if is_version_pinned(&self.model_id) {
            return Ok(());
        }
        if listed.iter().any(|name| name == &self.model_id) {
            return Ok(());
        }
        let available = if listed.is_empty() {
            "none are offered to this account".to_string()
        } else {
            format!("available: {}", listed.join(", "))
        };
        Err(evaluate_api::Error::HealthCheckFailed {
            source: format!(
                "model '{}' is not offered by TypeSafe for this account ({available}). Set `from:` to one of those, or to a versioned pin such as `typesafe:jev-1.13.0`. See: https://docs.typesafe.ai/models",
                self.model_id
            )
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evaluate_api::{Answer, EntryType, EvaluateState, NonNullEntry, NullableEntry, Question};
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

    #[test]
    fn version_pin_is_only_jev_plus_numeric_version() {
        assert!(is_version_pinned("jev-1.13.0"));
        assert!(is_version_pinned("jev-1.2"));
        assert!(!is_version_pinned("jev-latest"));
        assert!(!is_version_pinned("jev-preview"));
        assert!(!is_version_pinned("jev"));
        assert!(!is_version_pinned("not-jev-1.2"));
        assert!(!is_version_pinned("jev-does-not-exist-1.13.0"));
        assert!(!is_version_pinned("garbage-9.9.9.9"));
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
    /// A decoded empty listing says the account is offered nothing, so an alias must
    /// not load Ready off the back of it; a versioned pin still may.
    #[tokio::test]
    async fn health_rejects_an_alias_when_the_account_is_offered_nothing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"models": []})))
            .mount(&server)
            .await;

        let alias = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());
        let err = alias
            .health()
            .await
            .expect_err("an empty listing must not report an alias ready");
        assert!(
            err.to_string().contains("none are offered to this account"),
            "{err}"
        );

        let pinned = TypeSafe::try_new("jev", Some("jev-1.13.0"), "k")
            .expect("client")
            .with_base_url(server.uri());
        pinned
            .health()
            .await
            .expect("a versioned pin stays healthy");
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

    fn score_question(id: &str, levels: usize) -> BTreeMap<String, Question> {
        BTreeMap::from([(
            id.to_string(),
            Question::Score {
                instructions: NullableEntry::default(),
                criteria: (0..levels)
                    .map(|i| NonNullEntry::String(format!("level {i}")))
                    .collect(),
            },
        )])
    }

    /// A sparse legend is valid so long as every supplied key is in the rubric
    /// and the probability distribution still covers every level.
    #[tokio::test]
    async fn evaluate_accepts_a_sparse_score_legend() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 2.4,
                "legend": {"0": "routine", "3": "critical"},
                "probabilities": {"0": 0.1, "1": 0.1, "2": 0.1, "3": 0.7},
                "confidence": 0.9
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
                questions: score_question("q", 4),
            })
            .await
            .expect("a sparse legend with a full distribution is valid");
    }

    /// A legend key outside the rubric is still a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_a_legend_key_outside_the_rubric() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 0,
                "legend": {"0": "poor", "4": "off-scale"},
                "probabilities": {"0": 0.5, "1": 0.5},
                "confidence": 0.9
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
                questions: score_question("q", 2),
            })
            .await
            .expect_err("an out-of-range legend key must not be published");
        let msg = err.to_string();
        assert!(
            msg.contains("legend key '4' is not in the score rubric"),
            "{msg}"
        );
    }

    /// Probabilities that do not sum to 1 are not a distribution, whatever each
    /// individual value is.
    #[tokio::test]
    async fn evaluate_rejects_probabilities_that_do_not_sum_to_one() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.1, "technical": 0.2}, "confidence": 0.9
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
            .expect_err("probabilities summing to 0.3 must not be published");
        let msg = err.to_string();
        assert!(msg.contains("sum to"), "{msg}");
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
                "probabilities": {"billing": 1.0, "technical": 0.0}, "confidence": 4.2
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
                "probabilities": {"billing": 0.9, "technical": 0.0, "legal": 0.1}, "confidence": 0.9
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
            err.to_string()
                .contains("score criteria must contain at least one level"),
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

    /// A selected choice must be a highest-probability option.
    #[tokio::test]
    async fn evaluate_rejects_a_choice_that_is_not_highest_probability() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.1, "technical": 0.9}, "confidence": 0.88
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
            .expect_err("a non-max choice must not be published");
        let msg = err.to_string();
        assert!(
            msg.contains("'billing' is not a highest-probability option"),
            "{msg}"
        );
    }

    /// An exact tie at the maximum remains a valid selection.
    #[tokio::test]
    async fn evaluate_accepts_a_tied_highest_probability_choice() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.5, "technical": 0.5}, "confidence": 0.4
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
            .expect("a tied maximum is still a valid choice");
    }

    /// A score that is not the probability-weighted rubric average is a wrong result.
    #[tokio::test]
    async fn evaluate_rejects_a_score_that_is_not_the_weighted_average() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 0.0,
                "legend": {"0": "low", "1": "high"},
                "probabilities": {"0": 0.0, "1": 1.0},
                "confidence": 0.9
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
                questions: score_question("q", 2),
            })
            .await
            .expect_err("a contradictory score must not be published");
        let msg = err.to_string();
        assert!(
            msg.contains("is not the probability-weighted average"),
            "{msg}"
        );
    }

    /// Rounded probabilities that sum to approximately 1 remain a valid distribution.
    #[tokio::test]
    async fn evaluate_accepts_rounded_probabilities_that_sum_to_approximately_one() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "choice", "choice": "billing",
                "probabilities": {"billing": 0.33, "technical": 0.33, "sales": 0.33},
                "confidence": 0.4
            }}}),
        )
        .await;
        let client = TypeSafe::try_new("jev", Some("jev-latest"), "k")
            .expect("client")
            .with_base_url(server.uri());

        let questions = BTreeMap::from([(
            "q".to_string(),
            Question::Choice {
                instructions: "which team?".into(),
                criteria: BTreeMap::from([
                    ("billing".to_string(), EntryType::String("pay".into())),
                    ("technical".to_string(), EntryType::String("bugs".into())),
                    ("sales".to_string(), EntryType::String("sales".into())),
                ]),
            },
        )]);

        client
            .evaluate(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::String("s".into()),
                questions,
            })
            .await
            .expect("a two-decimal rounded distribution is still valid");
    }

    /// A ten-level score whose probabilities are the two-decimal rounding of
    /// `{8: 0.005, 9: 0.995}`, reported with that distribution's exact weighted
    /// average. Rounding moves the recomputed average to 9.08, well past a tolerance
    /// sized for a sum of probabilities, so this is where a fixed slack rejects a
    /// valid answer.
    #[tokio::test]
    async fn evaluate_accepts_a_rounded_distribution_with_its_exact_score() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 8.995,
                "legend": {"9": "top"},
                "probabilities": {
                    "0": 0.0, "1": 0.0, "2": 0.0, "3": 0.0, "4": 0.0,
                    "5": 0.0, "6": 0.0, "7": 0.0, "8": 0.01, "9": 1.0
                },
                "confidence": 0.9
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
                questions: score_question("q", 10),
            })
            .await
            .expect("a rounded distribution with its exact score is a valid answer");
    }

    /// The slack grows with the rubric, but a score that contradicts its
    /// distribution is still rejected on the widest rubric there is.
    #[tokio::test]
    async fn evaluate_rejects_a_contradictory_score_on_a_ten_level_rubric() {
        let server = MockServer::start().await;
        systemone_returning(
            &server,
            json!({"model": "jev-latest", "answers": {"q": {
                "type": "score", "score": 8.5,
                "legend": {"9": "top"},
                "probabilities": {
                    "0": 0.0, "1": 0.0, "2": 0.0, "3": 0.0, "4": 0.0,
                    "5": 0.0, "6": 0.0, "7": 0.0, "8": 0.0, "9": 1.0
                },
                "confidence": 0.9
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
                questions: score_question("q", 10),
            })
            .await
            .expect_err("a score half a level from its distribution must not be published");
        assert!(
            err.to_string()
                .contains("is not the probability-weighted average"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn evaluate_maps_503() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
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
            .expect_err("503");
        assert!(matches!(
            err,
            evaluate_api::Error::ServiceUnavailable { .. }
        ));
    }
}
