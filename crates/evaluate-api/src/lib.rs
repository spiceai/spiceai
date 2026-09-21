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

//! The System One evaluation contract.
//!
//! An evaluation model takes unstructured `state` plus a map of typed
//! `questions` and returns structured `answers` (noul / choice / score) with
//! calibrated probabilities. Implemented by provider crates; called by the
//! runtime's `POST /v1/evaluate` endpoint — which never names a provider.
//!
//! Deliberately separate from chat completions: System One models such as
//! `TypeSafe` Jev do not generate strings and must not be faked as `OpenAI` chat.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use snafu::Snafu;

/// Name → evaluation model map. Holds System One providers (e.g. `TypeSafe` Jev).
pub type EvaluateModelStore = std::collections::HashMap<String, Arc<dyn Evaluate>>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Evaluation model '{model}' failed: {source}"))]
    ModelCallFailed {
        model: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Evaluation model '{model}' returned an unparseable response: {response}"))]
    UnparseableResponse { model: String, response: String },

    #[snafu(display(
        "Failed to build HTTP client for evaluation model '{model}' — standard timeout/TLS defaults are unavailable."
    ))]
    HttpClientCreationFailed { model: String },

    #[snafu(display("Evaluation health check failed: {source}"))]
    HealthCheckFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid evaluation request for model '{model}': {message}"))]
    InvalidRequest { model: String, message: String },

    #[snafu(display("Authentication failed for evaluation model '{model}': {message}"))]
    AuthenticationFailed { model: String, message: String },

    #[snafu(display("Permission denied for evaluation model '{model}': {message}"))]
    PermissionDenied { model: String, message: String },

    #[snafu(display("Rate limited by evaluation provider for model '{model}': {message}"))]
    RateLimited { model: String, message: String },

    #[snafu(display("Evaluation provider is unavailable for model '{model}': {message}"))]
    ServiceUnavailable { model: String, message: String },

    #[snafu(display("Evaluation model '{model}' was not found upstream: {message}"))]
    ModelNotFound { model: String, message: String },

    #[snafu(display(
        "Failed to acquire rate-limit permit for evaluation model '{model}': {source}"
    ))]
    RatePermitFailed {
        model: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `TypeSafe` `EntryType`: string, object, array, or null.
///
/// Used for `instructions` and structured criteria descriptions.
/// See <https://docs.typesafe.ai/primitives/advanced>.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum EntryType {
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
    Null,
}

impl From<&str> for EntryType {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for EntryType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Non-null `EntryType` values: string, object, or array (not JSON null).
///
/// Used for score rubric levels so the `OpenAPI` contract matches `TypeSafe`'s
/// non-empty `list[str | object | array]` criteria shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum NonNullEntry {
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
}

impl From<&str> for NonNullEntry {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for NonNullEntry {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Distinguishes an omitted field from an explicit JSON `null` and a present value.
///
/// Serde's `Option<T>` collapses JSON `null` to `None`, which would drop nested
/// null criteria when forwarding to `TypeSafe`. This wrapper preserves that null.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum NullableEntry {
    /// Field was omitted from the JSON object.
    #[default]
    Absent,
    /// Field was present as JSON `null`.
    Null,
    /// Field was present with a concrete `EntryType` value (including nested null
    /// via [`EntryType::Null`] is not used here — top-level null is [`Self::Null`]).
    Value(EntryType),
}

impl Serialize for NullableEntry {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Absent | Self::Null => serializer.serialize_none(),
            Self::Value(v) => v.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for NullableEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Option::<EntryType>::deserialize(deserializer)? {
            None => Ok(Self::Null),
            Some(v) => Ok(Self::Value(v)),
        }
    }
}

impl JsonSchema for NullableEntry {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("NullableEntry")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Documented as EntryType | null (same wire shape as before).
        <EntryType as JsonSchema>::json_schema(generator)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for NullableEntry {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <EntryType as utoipa::PartialSchema>::schema()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for NullableEntry {}

impl<T: Into<EntryType>> From<T> for NullableEntry {
    fn from(value: T) -> Self {
        NullableEntry::Value(value.into())
    }
}

fn nullable_entry_is_absent(value: &NullableEntry) -> bool {
    matches!(value, NullableEntry::Absent)
}

/// A typed question sent to a System One evaluation model.
///
/// `instructions` uses [`NullableEntry`] so an explicit JSON `null` survives the round
/// trip to `TypeSafe` instead of collapsing into an omitted field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Question {
    /// Yes/no probability question. Answer is `noul` in \[0, 1\] (P(yes)).
    Noul {
        #[serde(default, skip_serializing_if = "nullable_entry_is_absent")]
        instructions: NullableEntry,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    /// Closed-set selection. Answer is the highest-probability option plus the full distribution.
    Choice {
        #[serde(default, skip_serializing_if = "nullable_entry_is_absent")]
        instructions: NullableEntry,
        /// Option id → description (`EntryType`, or JSON null).
        criteria: BTreeMap<String, EntryType>,
    },
    /// Ordered rubric score. Answer is a probability-weighted value across levels.
    Score {
        #[serde(default, skip_serializing_if = "nullable_entry_is_absent")]
        instructions: NullableEntry,
        /// Two to ten non-null rubric levels: `TypeSafe` documents score criteria as
        /// "at least two levels and takes up to 10"
        /// (<https://docs.typesafe.ai/primitives/score>).
        #[serde(deserialize_with = "deserialize_score_criteria")]
        #[schemars(length(min = 2, max = 10))]
        #[cfg_attr(feature = "openapi", schema(min_items = 2, max_items = 10))]
        criteria: Vec<NonNullEntry>,
    },
}

/// Optional yes/no rubric for a noul question.
///
/// `true` / `false` use [`NullableEntry`] so explicit JSON `null` is preserved when
/// forwarding to `TypeSafe` (unlike `Option<EntryType>`, which drops nulls).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NoulCriteria {
    #[serde(
        default,
        skip_serializing_if = "nullable_entry_is_absent",
        rename = "true"
    )]
    pub true_meaning: NullableEntry,
    #[serde(
        default,
        skip_serializing_if = "nullable_entry_is_absent",
        rename = "false"
    )]
    pub false_meaning: NullableEntry,
}

fn deserialize_score_criteria<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<NonNullEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let criteria = Vec::<NonNullEntry>::deserialize(deserializer)?;
    if !(2..=10).contains(&criteria.len()) {
        return Err(serde::de::Error::custom(
            "score criteria must contain between two and ten non-null levels",
        ));
    }
    Ok(criteria)
}

/// A successful evaluation answers something; an empty `answers` map is a malformed
/// provider response, not a 200.
fn deserialize_nonempty_answers<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, Answer>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let answers = BTreeMap::<String, Answer>::deserialize(deserializer)?;
    if answers.is_empty() {
        return Err(serde::de::Error::custom(
            "answers must contain at least one answer",
        ));
    }
    Ok(answers)
}

/// Evaluation `state`: string, object, or array (not bool/number/null).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum EvaluateState {
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
}

impl From<&str> for EvaluateState {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for EvaluateState {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// `minProperties` has no utoipa attribute, but `ObjectBuilder` supports it, so these
/// describe the two maps that must carry at least one entry.
#[cfg(feature = "openapi")]
fn nonempty_map_of(component: &str) -> utoipa::openapi::schema::Object {
    utoipa::openapi::ObjectBuilder::new()
        .schema_type(utoipa::openapi::schema::SchemaType::new(
            utoipa::openapi::schema::Type::Object,
        ))
        .additional_properties(Some(utoipa::openapi::Ref::from_schema_name(component)))
        .min_properties(Some(1))
        .build()
}

#[cfg(feature = "openapi")]
fn nonempty_question_map() -> utoipa::openapi::schema::Object {
    nonempty_map_of("Question")
}

#[cfg(feature = "openapi")]
fn nonempty_answer_map() -> utoipa::openapi::schema::Object {
    nonempty_map_of("Answer")
}

/// Request body for `POST /v1/evaluate` and provider System One calls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EvaluateRequest {
    /// Spicepod model name (runtime) or provider model id (provider forward).
    pub model: String,
    /// State for the model to evaluate: string, object, or array.
    pub state: EvaluateState,
    /// Questions keyed by caller-selected identifiers. At least one is required; the
    /// `/v1/evaluate` handler enforces that so an empty map returns the endpoint's
    /// documented 400 body rather than an extractor rejection.
    #[schemars(extend("minProperties" = 1))]
    #[cfg_attr(feature = "openapi", schema(schema_with = nonempty_question_map))]
    pub questions: BTreeMap<String, Question>,
}

/// Token usage reported by the provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Usage {
    /// Defaulted: the provider may report one count without the other, and a partial
    /// `usage` block must not fail an otherwise successful evaluation.
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

/// A typed answer returned for one question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Answer {
    Noul {
        noul: f64,
    },
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
    Score {
        score: f64,
        legend: BTreeMap<String, EntryType>,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
}

/// Response body for evaluation: typed answers plus provider metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EvaluateResponse {
    /// Versioned model id that answered (e.g. `jev-1.13.0`), when the provider reports it.
    pub model: String,
    #[serde(deserialize_with = "deserialize_nonempty_answers")]
    #[schemars(extend("minProperties" = 1))]
    #[cfg_attr(feature = "openapi", schema(schema_with = nonempty_answer_map))]
    pub answers: BTreeMap<String, Answer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// A System One evaluation model: state + typed questions → structured answers.
#[async_trait]
pub trait Evaluate: Send + Sync + Debug {
    /// Run an evaluation against `state` and `questions`.
    ///
    /// The runtime sets `request.model` to the Spicepod model *name*; providers
    /// should substitute their upstream model id before calling the remote API.
    async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse>;

    /// Optional health check (e.g. list models). Default is a no-op.
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn instructions_accept_string_object_and_array() {
        let as_string: Question = serde_json::from_value(json!({
            "type": "noul",
            "instructions": "Is this urgent?"
        }))
        .expect("string");
        assert!(matches!(
            as_string,
            Question::Noul {
                instructions: NullableEntry::Value(EntryType::String(_)),
                ..
            }
        ));

        let as_object: Question = serde_json::from_value(json!({
            "type": "noul",
            "instructions": { "question": "Is this urgent?", "focus": "timing" }
        }))
        .expect("object");
        assert!(matches!(
            as_object,
            Question::Noul {
                instructions: NullableEntry::Value(EntryType::Object(_)),
                ..
            }
        ));

        let as_array: Question = serde_json::from_value(json!({
            "type": "choice",
            "instructions": ["Pick a team", "Prefer technical when ambiguous"],
            "criteria": { "technical": "Bugs", "billing": null }
        }))
        .expect("array");
        assert!(matches!(
            as_array,
            Question::Choice {
                instructions: NullableEntry::Value(EntryType::Array(_)),
                ..
            }
        ));
        if let Question::Choice { criteria, .. } = as_array {
            assert!(matches!(criteria.get("billing"), Some(EntryType::Null)));
        }
    }

    #[test]
    fn instructions_optional_and_null() {
        let omitted: Question = serde_json::from_value(json!({ "type": "noul" })).expect("omit");
        assert!(matches!(
            omitted,
            Question::Noul {
                instructions: NullableEntry::Absent,
                ..
            }
        ));

        // An explicit null is kept distinct from omission, so it can be forwarded as
        // sent; see `instructions_preserve_explicit_null_when_forwarded`.
        let null_instr: Question = serde_json::from_value(json!({
            "type": "noul",
            "instructions": null
        }))
        .expect("null");
        assert!(matches!(
            null_instr,
            Question::Noul {
                instructions: NullableEntry::Null,
                ..
            }
        ));
    }

    #[test]
    fn evaluate_state_rejects_bool_number_null() {
        for bad in [json!(true), json!(1), json!(null)] {
            let err = serde_json::from_value::<EvaluateRequest>(json!({
                "model": "m",
                "state": bad,
                "questions": {"q": {"type": "noul"}}
            }));
            assert!(err.is_err(), "expected reject for {bad}");
        }

        let ok: EvaluateRequest = serde_json::from_value(json!({
            "model": "m",
            "state": "hello",
            "questions": {"q": {"type": "noul"}}
        }))
        .expect("string state");
        assert!(matches!(ok.state, EvaluateState::String(_)));
    }

    #[test]
    fn score_legend_accepts_entry_types() {
        let answer: Answer = serde_json::from_value(json!({
            "type": "score",
            "score": 1.5,
            "legend": {
                "0": "low",
                "1": { "label": "mid" },
                "2": ["high", "detail"]
            },
            "probabilities": { "0": 0.1, "1": 0.2, "2": 0.7 },
            "confidence": 0.9
        }))
        .expect("legend");
        if let Answer::Score { legend, .. } = answer {
            assert!(matches!(legend.get("0"), Some(EntryType::String(_))));
            assert!(matches!(legend.get("1"), Some(EntryType::Object(_))));
            assert!(matches!(legend.get("2"), Some(EntryType::Array(_))));
        } else {
            panic!("expected Score");
        }
    }

    #[test]
    fn score_criteria_rejects_empty_and_null() {
        let empty = serde_json::from_value::<Question>(json!({
            "type": "score",
            "criteria": []
        }));
        assert!(empty.is_err(), "empty score criteria must fail");

        let with_null = serde_json::from_value::<Question>(json!({
            "type": "score",
            "criteria": [null]
        }));
        assert!(with_null.is_err(), "null score criteria items must fail");

        let ok: Question = serde_json::from_value(json!({
            "type": "score",
            "criteria": ["low", {"label": "mid"}]
        }))
        .expect("non-empty non-null");
        assert!(matches!(ok, Question::Score { criteria, .. } if criteria.len() == 2));
    }

    #[test]
    fn noul_criteria_preserves_explicit_null() {
        let q: Question = serde_json::from_value(json!({
            "type": "noul",
            "criteria": { "true": null, "false": "not urgent" }
        }))
        .expect("noul");
        let Question::Noul {
            criteria: Some(c), ..
        } = q
        else {
            panic!("expected noul with criteria");
        };
        assert!(matches!(c.true_meaning, NullableEntry::Null));
        assert!(matches!(
            c.false_meaning,
            NullableEntry::Value(EntryType::String(_))
        ));

        let forwarded = serde_json::to_value(&c).expect("serialize");
        assert_eq!(forwarded.get("true"), Some(&json!(null)));
        assert_eq!(forwarded.get("false"), Some(&json!("not urgent")));

        let omitted: NoulCriteria = serde_json::from_value(json!({})).expect("omit");
        assert!(matches!(omitted.true_meaning, NullableEntry::Absent));
        let omitted_json = serde_json::to_value(&omitted);
        assert!(omitted_json.is_ok(), "serialize omitted criteria");
        match omitted_json.ok().and_then(|v| v.as_object().cloned()) {
            Some(obj) => assert!(obj.is_empty()),
            None => panic!("expected empty JSON object"),
        }
    }
    /// Regression: `Option<EntryType>` collapsed an explicit `null` to `None`, and
    /// `skip_serializing_if` then dropped the key, so `TypeSafe` could not tell an
    /// explicit null from an omitted field.
    #[test]
    fn instructions_preserve_explicit_null_when_forwarded() {
        let explicit: Question =
            serde_json::from_value(json!({"type": "noul", "instructions": null}))
                .expect("explicit null instructions");
        let Question::Noul { instructions, .. } = &explicit else {
            panic!("expected noul, got {explicit:?}");
        };
        assert!(matches!(instructions, NullableEntry::Null));
        assert_eq!(
            serde_json::to_value(&explicit).expect("ser"),
            json!({"type": "noul", "instructions": null}),
            "an explicit null must survive the round trip"
        );

        let omitted: Question =
            serde_json::from_value(json!({"type": "noul"})).expect("omitted instructions");
        let Question::Noul { instructions, .. } = &omitted else {
            panic!("expected noul, got {omitted:?}");
        };
        assert!(matches!(instructions, NullableEntry::Absent));
        assert_eq!(
            serde_json::to_value(&omitted).expect("ser"),
            json!({"type": "noul"}),
            "an omitted field must stay omitted"
        );
    }

    /// `TypeSafe` documents score criteria as two to ten levels.
    #[test]
    fn score_criteria_requires_two_levels() {
        let one = serde_json::from_value::<Question>(
            json!({"type": "score", "instructions": "how bad?", "criteria": ["only"]}),
        );
        assert!(one.is_err(), "one level must be rejected: {one:?}");

        serde_json::from_value::<Question>(
            json!({"type": "score", "instructions": "how bad?", "criteria": ["calm", "angry"]}),
        )
        .expect("two levels are valid");

        let ten: Vec<String> = (0..10).map(|i| format!("level {i}")).collect();
        serde_json::from_value::<Question>(
            json!({"type": "score", "instructions": "how bad?", "criteria": ten}),
        )
        .expect("ten levels are valid");

        let eleven: Vec<String> = (0..11).map(|i| format!("level {i}")).collect();
        let over = serde_json::from_value::<Question>(
            json!({"type": "score", "instructions": "how bad?", "criteria": eleven}),
        );
        assert!(over.is_err(), "eleven levels must be rejected: {over:?}");
    }

    /// A provider reply with no answers is malformed, not a successful evaluation.
    #[test]
    fn response_requires_at_least_one_answer() {
        let empty = serde_json::from_value::<EvaluateResponse>(
            json!({"model": "jev-latest", "answers": {}}),
        );
        assert!(empty.is_err(), "empty answers must be rejected: {empty:?}");
    }

    /// A partial `usage` block must not turn a successful evaluation into an error.
    #[test]
    fn partial_usage_defaults_rather_than_failing() {
        let resp: EvaluateResponse = serde_json::from_value(json!({
            "model": "jev-latest",
            "answers": {"q": {"type": "noul", "noul": 0.5}},
            "usage": {"input_tokens": 7}
        }))
        .expect("a partial usage block is still a successful response");
        let usage = resp.usage.expect("usage present");
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 0);
    }
}
