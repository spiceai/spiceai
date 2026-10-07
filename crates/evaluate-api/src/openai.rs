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

//! The `OpenAI` Decisions API wire format (`POST /v1/decisions`) and its mapping to the
//! System One contract.
//!
//! Spice serves `/v1/decisions` with any evaluation model by translating each request
//! into an [`EvaluateRequest`] and each [`EvaluateResponse`] back, and an `OpenAI`
//! decision model answers the System One contract by translating the other way. The
//! values mean the same on both sides — a predicate's `probability` is a noul, a choice
//! is the most probable option, a score is the probability-weighted 0-based level index
//! — so the mapping only reshapes containers.
//!
//! See <https://developers.openai.com/api/reference/resources/decisions/methods/create>.

use std::collections::BTreeMap;
use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    Answer, EntryType, EvaluateRequest, EvaluateResponse, EvaluateState, NonNullEntry,
    NullableEntry, Question, Usage,
};

/// Most questions one decision request may carry.
pub const MAX_QUESTIONS: usize = 200;
/// Fewest options a choice question may offer.
pub const MIN_CHOICES: usize = 2;
/// Most options a choice question may offer.
pub const MAX_CHOICES: usize = 255;
/// Fewest levels a score question may define.
pub const MIN_LEVELS: usize = 2;
/// Most levels a score question may define.
pub const MAX_LEVELS: usize = 10;
/// Longest `safety_identifier` accepted.
pub const MAX_SAFETY_IDENTIFIER_LEN: usize = 128;

/// Request body for `POST /v1/decisions`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DecisionRequest {
    /// The model that answers: the name of a model under `models` in the Spicepod.
    pub model: String,
    /// Shared evidence for every question: a text string, or user messages with text
    /// parts.
    pub input: DecisionInput,
    /// What to decide, in order: between 1 and 200 questions.
    pub questions: Vec<DecisionQuestion>,
    /// Opaque end-user identifier, at most 128 characters. Forwarded to `OpenAI`
    /// decision models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,
}

/// The evidence every question is answered from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum DecisionInput {
    Text(String),
    Messages(Vec<DecisionInputMessage>),
}

/// A user message carrying text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DecisionInputMessage {
    pub role: DecisionInputRole,
    pub content: DecisionMessageContent,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub message_type: Option<DecisionMessageType>,
}

/// Only user messages are accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DecisionInputRole {
    User,
}

/// The optional `type` of an input message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DecisionMessageType {
    Message,
}

/// A message's content: a string, or a list of parts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum DecisionMessageContent {
    Text(String),
    Parts(Vec<DecisionInputPart>),
}

/// One part of a message. Image parts are accepted by the schema and refused with a
/// message naming the limitation, so a client learns why rather than seeing a parse error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionInputPart {
    InputText {
        text: String,
    },
    InputImage {
        image_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// A question about the input, with an optional name echoed in its answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQuestion {
    /// How likely a statement about the input is true. Answered with a `probability`.
    Predicate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        instructions: String,
    },
    /// Which of the supplied options fits the input. Answered with the most probable
    /// option and the distribution over all of them.
    Choice {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        instructions: String,
        /// Between 2 and 255 distinct options.
        choices: Vec<ChoiceOption>,
    },
    /// Where the input sits on ordered levels, lowest first. Answered with the
    /// probability-weighted 0-based level index.
    Score {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        instructions: String,
        /// Between 2 and 10 levels, lowest first.
        levels: Vec<ScoreLevel>,
    },
}

/// One option of a choice question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ChoiceOption {
    pub value: ChoiceValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A choice value. `OpenAI` treats a string and a boolean with the same text as
/// different values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum ChoiceValue {
    Bool(bool),
    String(String),
}

/// One level of a score question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ScoreLevel {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Response body for `POST /v1/decisions`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DecisionResponse {
    /// The model that answered.
    pub model: String,
    /// One answer per question, in question order.
    pub answers: Vec<DecisionAnswer>,
    /// Tokens the decision used. Omitted when the model did not report them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<DecisionUsage>,
}

/// The answer to one question. `name` echoes the question's name, or is null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecisionAnswer {
    Predicate {
        name: Option<String>,
        probability: f64,
    },
    Choice {
        name: Option<String>,
        choice: ChoiceValue,
        probabilities: Vec<ChoiceProbability>,
        confidence: f64,
    },
    Score {
        name: Option<String>,
        score: f64,
        probabilities: Vec<LevelProbability>,
        confidence: f64,
    },
    /// The model declined to answer this question; the other answers stand.
    Refusal { name: Option<String> },
}

/// The probability of one choice option.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChoiceProbability {
    pub value: ChoiceValue,
    pub probability: f64,
}

/// The probability of one score level, by its 0-based index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct LevelProbability {
    pub value: u32,
    pub label: String,
    pub probability: f64,
}

/// Tokens a decision used.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DecisionUsage {
    pub input_tokens: u64,
    #[serde(default)]
    pub input_tokens_details: InputTokensDetails,
    pub output_tokens: u64,
    #[serde(default)]
    pub output_tokens_details: OutputTokensDetails,
    #[serde(default)]
    pub total_tokens: u64,
}

/// Breakdown of input tokens. Zero unless the model reports a cache.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct InputTokensDetails {
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub cache_write_tokens: u64,
}

/// Breakdown of output tokens. Zero unless the model reports reasoning.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct OutputTokensDetails {
    #[serde(default)]
    pub reasoning_tokens: u64,
}

/// A decision request that cannot be served, naming the offending field the way
/// `OpenAI`'s `param` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidDecisionRequest {
    pub param: String,
    pub message: String,
}

impl InvalidDecisionRequest {
    fn new(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            param: param.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for InvalidDecisionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.param, self.message)
    }
}

impl std::error::Error for InvalidDecisionRequest {}

/// A decision request in System One form, with what is needed to put the answers back
/// in `OpenAI`'s order and types.
#[derive(Debug, Clone)]
pub struct SystemOneRequest {
    pub request: EvaluateRequest,
    asked: Vec<AskedQuestion>,
}

#[derive(Debug, Clone)]
struct AskedQuestion {
    id: String,
    name: Option<String>,
    kind: AskedKind,
}

#[derive(Debug, Clone)]
enum AskedKind {
    Predicate,
    /// Each option's System One key and its typed value, in request order.
    Choice(Vec<(String, ChoiceValue)>),
    /// Each level's label, lowest first.
    Score(Vec<String>),
}

/// The System One question id for the question at `index`. Zero-padded so ids sort in
/// question order.
fn question_id(index: usize) -> String {
    format!("q{index:03}")
}

impl DecisionRequest {
    /// Validates the request against the Decisions API limits and translates it into
    /// the System One contract.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidDecisionRequest`] naming the first field outside the limits,
    /// or an image input, which System One state cannot carry.
    pub fn to_system_one(&self) -> Result<SystemOneRequest, InvalidDecisionRequest> {
        if self.model.trim().is_empty() {
            return Err(InvalidDecisionRequest::new(
                "model",
                "`model` must name a model under `models` in the Spicepod.",
            ));
        }
        if let Some(id) = &self.safety_identifier
            && id.chars().count() > MAX_SAFETY_IDENTIFIER_LEN
        {
            return Err(InvalidDecisionRequest::new(
                "safety_identifier",
                format!(
                    "`safety_identifier` must be at most {MAX_SAFETY_IDENTIFIER_LEN} characters."
                ),
            ));
        }
        if self.questions.is_empty() || self.questions.len() > MAX_QUESTIONS {
            return Err(InvalidDecisionRequest::new(
                "questions",
                format!(
                    "`questions` must contain between 1 and {MAX_QUESTIONS} questions; it has {}.",
                    self.questions.len()
                ),
            ));
        }

        let state = self.input.to_state()?;
        let mut questions = BTreeMap::new();
        let mut asked = Vec::with_capacity(self.questions.len());
        for (index, question) in self.questions.iter().enumerate() {
            let id = question_id(index);
            let (system_one, kind, name) = translate_question(index, question)?;
            questions.insert(id.clone(), system_one);
            asked.push(AskedQuestion { id, name, kind });
        }

        Ok(SystemOneRequest {
            request: EvaluateRequest {
                model: self.model.clone(),
                state,
                questions,
                safety_identifier: self.safety_identifier.clone(),
            },
            asked,
        })
    }
}

impl DecisionInput {
    fn to_state(&self) -> Result<EvaluateState, InvalidDecisionRequest> {
        match self {
            Self::Text(text) => Ok(EvaluateState::String(text.clone())),
            Self::Messages(messages) => {
                let mut texts = Vec::new();
                for (m, message) in messages.iter().enumerate() {
                    match &message.content {
                        DecisionMessageContent::Text(text) => texts.push(text.as_str()),
                        DecisionMessageContent::Parts(parts) => {
                            for (p, part) in parts.iter().enumerate() {
                                match part {
                                    DecisionInputPart::InputText { text } => {
                                        texts.push(text.as_str());
                                    }
                                    DecisionInputPart::InputImage { .. } => {
                                        return Err(InvalidDecisionRequest::new(
                                            format!("input[{m}].content[{p}]"),
                                            "Image inputs are not supported yet. Send the evidence as text in `input`.",
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(EvaluateState::String(texts.join("\n\n")))
            }
        }
    }
}

fn instructions_entry(instructions: &str) -> NullableEntry {
    NullableEntry::Value(EntryType::String(instructions.to_string()))
}

/// The System One key for a choice value: its text, with booleans as `true`/`false`.
fn choice_key(value: &ChoiceValue) -> String {
    match value {
        ChoiceValue::Bool(b) => b.to_string(),
        ChoiceValue::String(s) => s.clone(),
    }
}

fn translate_question(
    index: usize,
    question: &DecisionQuestion,
) -> Result<(Question, AskedKind, Option<String>), InvalidDecisionRequest> {
    match question {
        DecisionQuestion::Predicate { name, instructions } => Ok((
            Question::Noul {
                instructions: instructions_entry(instructions),
                criteria: None,
            },
            AskedKind::Predicate,
            name.clone(),
        )),
        DecisionQuestion::Choice {
            name,
            instructions,
            choices,
        } => {
            let param = format!("questions[{index}].choices");
            if !(MIN_CHOICES..=MAX_CHOICES).contains(&choices.len()) {
                return Err(InvalidDecisionRequest::new(
                    param,
                    format!(
                        "A choice question needs between {MIN_CHOICES} and {MAX_CHOICES} choices; it has {}.",
                        choices.len()
                    ),
                ));
            }
            let mut criteria = BTreeMap::new();
            let mut options = Vec::with_capacity(choices.len());
            for option in choices {
                let key = choice_key(&option.value);
                let description = option
                    .description
                    .as_ref()
                    .map_or(EntryType::Null, |d| EntryType::String(d.clone()));
                if criteria.insert(key.clone(), description).is_some() {
                    return Err(InvalidDecisionRequest::new(
                        param,
                        format!(
                            "Choice value '{key}' appears more than once. Give each choice a distinct value."
                        ),
                    ));
                }
                options.push((key, option.value.clone()));
            }
            Ok((
                Question::Choice {
                    instructions: instructions_entry(instructions),
                    criteria,
                },
                AskedKind::Choice(options),
                name.clone(),
            ))
        }
        DecisionQuestion::Score {
            name,
            instructions,
            levels,
        } => {
            if !(MIN_LEVELS..=MAX_LEVELS).contains(&levels.len()) {
                return Err(InvalidDecisionRequest::new(
                    format!("questions[{index}].levels"),
                    format!(
                        "A score question needs between {MIN_LEVELS} and {MAX_LEVELS} levels; it has {}.",
                        levels.len()
                    ),
                ));
            }
            let criteria = levels
                .iter()
                .map(|level| match &level.description {
                    Some(description) => NonNullEntry::Object(Map::from_iter([
                        ("label".to_string(), Value::String(level.label.clone())),
                        (
                            "description".to_string(),
                            Value::String(description.clone()),
                        ),
                    ])),
                    None => NonNullEntry::String(level.label.clone()),
                })
                .collect();
            Ok((
                Question::Score {
                    instructions: instructions_entry(instructions),
                    criteria,
                },
                AskedKind::Score(levels.iter().map(|l| l.label.clone()).collect()),
                name.clone(),
            ))
        }
    }
}

impl SystemOneRequest {
    /// Puts a System One response into `OpenAI`'s shape: one answer per question, in
    /// question order, with each name echoed.
    ///
    /// # Errors
    ///
    /// Returns a description of the first question the response does not answer with
    /// a matching, complete answer. The model's answers are checked against their
    /// questions before they reach here, so this only fails on a provider bug, and a
    /// missing value is reported rather than filled in.
    pub fn decision_response(
        &self,
        response: EvaluateResponse,
    ) -> Result<DecisionResponse, String> {
        let mut answers = Vec::with_capacity(self.asked.len());
        for (index, asked) in self.asked.iter().enumerate() {
            let Some(answer) = response.answers.get(&asked.id) else {
                return Err(format!("the model returned no answer for question {index}"));
            };
            let name = asked.name.clone();
            let translated = match (&asked.kind, answer) {
                (_, Answer::Refusal {}) => DecisionAnswer::Refusal { name },
                (AskedKind::Predicate, Answer::Noul { noul }) => DecisionAnswer::Predicate {
                    name,
                    probability: *noul,
                },
                (
                    AskedKind::Choice(options),
                    Answer::Choice {
                        choice,
                        probabilities,
                        confidence,
                    },
                ) => {
                    let Some((_, chosen)) = options.iter().find(|(key, _)| key == choice) else {
                        return Err(format!(
                            "question {index}: the model chose '{choice}', which is not one of its choices"
                        ));
                    };
                    let mut distribution = Vec::with_capacity(options.len());
                    for (key, value) in options {
                        let Some(&probability) = probabilities.get(key) else {
                            return Err(format!(
                                "question {index}: the model gave no probability for choice '{key}'"
                            ));
                        };
                        distribution.push(ChoiceProbability {
                            value: value.clone(),
                            probability,
                        });
                    }
                    DecisionAnswer::Choice {
                        name,
                        choice: chosen.clone(),
                        probabilities: distribution,
                        confidence: *confidence,
                    }
                }
                (
                    AskedKind::Score(labels),
                    Answer::Score {
                        score,
                        probabilities,
                        confidence,
                        ..
                    },
                ) => {
                    let mut distribution = Vec::with_capacity(labels.len());
                    for (level, label) in (0_u32..).zip(labels) {
                        let Some(&probability) = probabilities.get(&level.to_string()) else {
                            return Err(format!(
                                "question {index}: the model gave no probability for level {level}"
                            ));
                        };
                        distribution.push(LevelProbability {
                            value: level,
                            label: label.clone(),
                            probability,
                        });
                    }
                    DecisionAnswer::Score {
                        name,
                        score: *score,
                        probabilities: distribution,
                        confidence: *confidence,
                    }
                }
                _ => {
                    return Err(format!(
                        "question {index}: the model answered with a different kind of answer"
                    ));
                }
            };
            answers.push(translated);
        }

        Ok(DecisionResponse {
            model: response.model,
            answers,
            usage: response.usage.map(|usage| DecisionUsage {
                input_tokens: usage.input_tokens,
                input_tokens_details: InputTokensDetails::default(),
                output_tokens: usage.output_tokens,
                output_tokens_details: OutputTokensDetails::default(),
                total_tokens: usage.input_tokens.saturating_add(usage.output_tokens),
            }),
        })
    }
}

/// Text for a System One entry inside an `OpenAI` string field: a string as is, and an
/// object or array as compact JSON. `None` for a null or empty entry.
fn entry_text(entry: &EntryType) -> Option<String> {
    match entry {
        EntryType::String(text) if text.is_empty() => None,
        EntryType::String(text) => Some(text.clone()),
        EntryType::Null => None,
        EntryType::Array(_) | EntryType::Object(_) => serde_json::to_string(entry).ok(),
    }
}

fn nullable_text(entry: &NullableEntry) -> Option<String> {
    match entry {
        NullableEntry::Absent | NullableEntry::Null => None,
        NullableEntry::Value(value) => entry_text(value),
    }
}

/// A score level's label and description: an object's `label` and `description`
/// fields when it has a string `label`, otherwise the whole entry as its label.
fn level_label(level: &NonNullEntry) -> (String, Option<String>) {
    if let NonNullEntry::Object(fields) = level
        && let Some(Value::String(label)) = fields.get("label")
    {
        let description = match fields.get("description") {
            Some(Value::String(d)) => Some(d.clone()),
            None | Some(Value::Null) => None,
            Some(other) => Some(other.to_string()),
        };
        return (label.clone(), description);
    }
    let text = entry_text(&EntryType::from(level)).unwrap_or_default();
    (text, None)
}

/// The legend a System One score answer carries: each level's description by index.
fn score_legend(criteria: &[NonNullEntry]) -> BTreeMap<String, EntryType> {
    criteria
        .iter()
        .enumerate()
        .map(|(i, level)| (i.to_string(), EntryType::from(level)))
        .collect()
}

/// Builds the `OpenAI` decision request that asks `request`'s questions of `model`,
/// naming each question by its System One id so its answer can be matched back, and
/// forwarding the request's `safety_identifier`.
///
/// # Errors
///
/// Returns [`InvalidDecisionRequest`] for a question `OpenAI` cannot ask: a choice
/// with fewer than two options, or more than 200 questions.
pub fn system_one_to_decision_request(
    request: &EvaluateRequest,
    model: &str,
) -> Result<DecisionRequest, InvalidDecisionRequest> {
    if request.questions.len() > MAX_QUESTIONS {
        return Err(InvalidDecisionRequest::new(
            "questions",
            format!(
                "OpenAI decision models answer at most {MAX_QUESTIONS} questions per request; this request has {}.",
                request.questions.len()
            ),
        ));
    }
    let input = match &request.state {
        EvaluateState::String(text) => DecisionInput::Text(text.clone()),
        structured @ (EvaluateState::Array(_) | EvaluateState::Object(_)) => {
            DecisionInput::Text(serde_json::to_string(structured).map_err(|e| {
                InvalidDecisionRequest::new("state", format!("The state is not serializable: {e}"))
            })?)
        }
    };

    let mut questions = Vec::with_capacity(request.questions.len());
    for (id, question) in &request.questions {
        let name = Some(id.clone());
        questions.push(match question {
            Question::Noul {
                instructions,
                criteria,
            } => {
                let mut text = nullable_text(instructions).unwrap_or_default();
                if let Some(criteria) = criteria {
                    for (label, entry) in [
                        ("Criteria for true", &criteria.true_meaning),
                        ("Criteria for false", &criteria.false_meaning),
                    ] {
                        if let Some(meaning) = nullable_text(entry) {
                            if !text.is_empty() {
                                text.push('\n');
                            }
                            text.push_str(label);
                            text.push_str(": ");
                            text.push_str(&meaning);
                        }
                    }
                }
                DecisionQuestion::Predicate {
                    name,
                    instructions: text,
                }
            }
            Question::Choice {
                instructions,
                criteria,
            } => {
                if criteria.len() < MIN_CHOICES || criteria.len() > MAX_CHOICES {
                    return Err(InvalidDecisionRequest::new(
                        format!("questions.{id}.criteria"),
                        format!(
                            "OpenAI decision models need between {MIN_CHOICES} and {MAX_CHOICES} options in a choice question; '{id}' has {}.",
                            criteria.len()
                        ),
                    ));
                }
                DecisionQuestion::Choice {
                    name,
                    instructions: nullable_text(instructions).unwrap_or_default(),
                    choices: criteria
                        .iter()
                        .map(|(value, description)| ChoiceOption {
                            value: ChoiceValue::String(value.clone()),
                            description: entry_text(description),
                        })
                        .collect(),
                }
            }
            Question::Score {
                instructions,
                criteria,
            } => DecisionQuestion::Score {
                name,
                instructions: nullable_text(instructions).unwrap_or_default(),
                levels: criteria
                    .iter()
                    .map(|level| {
                        let (label, description) = level_label(level);
                        ScoreLevel { label, description }
                    })
                    .collect(),
            },
        });
    }

    Ok(DecisionRequest {
        model: model.to_string(),
        input,
        questions,
        safety_identifier: request.safety_identifier.clone(),
    })
}

/// Reads an `OpenAI` decision response as System One answers to `request`'s questions,
/// matched by name.
///
/// # Errors
///
/// Returns a description of the first answer that names no question, repeats one,
/// carries a boolean choice for a string option, or gives a level outside the rubric.
/// The result still has to pass [`crate::check_answers`].
pub fn decision_response_to_system_one(
    request: &EvaluateRequest,
    response: DecisionResponse,
) -> Result<EvaluateResponse, String> {
    let mut answers = BTreeMap::new();
    for answer in response.answers {
        let name = match &answer {
            DecisionAnswer::Predicate { name, .. }
            | DecisionAnswer::Choice { name, .. }
            | DecisionAnswer::Score { name, .. }
            | DecisionAnswer::Refusal { name } => name.clone(),
        };
        let Some(id) = name else {
            return Err("an answer has no name, so it cannot be matched to its question".into());
        };
        let Some(question) = request.questions.get(&id) else {
            return Err(format!("an answer names '{id}', which was not asked"));
        };
        let translated = match answer {
            DecisionAnswer::Predicate { probability, .. } => Answer::Noul { noul: probability },
            DecisionAnswer::Choice {
                choice,
                probabilities,
                confidence,
                ..
            } => {
                let ChoiceValue::String(choice) = choice else {
                    return Err(format!(
                        "question '{id}': the choice is a boolean, but its options are strings"
                    ));
                };
                let mut distribution = BTreeMap::new();
                for entry in probabilities {
                    let ChoiceValue::String(value) = entry.value else {
                        return Err(format!(
                            "question '{id}': a probability is for a boolean, but its options are strings"
                        ));
                    };
                    distribution.insert(value, entry.probability);
                }
                Answer::Choice {
                    choice,
                    probabilities: distribution,
                    confidence,
                }
            }
            DecisionAnswer::Score {
                score,
                probabilities,
                confidence,
                ..
            } => {
                let Question::Score { criteria, .. } = question else {
                    return Err(format!(
                        "question '{id}' is not a score question, but its answer is a score"
                    ));
                };
                let distribution = probabilities
                    .into_iter()
                    .map(|entry| (entry.value.to_string(), entry.probability))
                    .collect();
                Answer::Score {
                    score,
                    legend: score_legend(criteria),
                    probabilities: distribution,
                    confidence,
                }
            }
            DecisionAnswer::Refusal { .. } => Answer::Refusal {},
        };
        if answers.insert(id.clone(), translated).is_some() {
            return Err(format!("question '{id}' was answered more than once"));
        }
    }

    Ok(EvaluateResponse {
        model: response.model,
        answers,
        usage: response.usage.map(|usage| Usage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(value: serde_json::Value) -> DecisionRequest {
        serde_json::from_value(value).expect("valid decision request")
    }

    #[test]
    fn the_safety_identifier_travels_with_the_request() {
        let req = request(json!({
            "model": "gpt-6-luna", "input": "x", "safety_identifier": "user-1",
            "questions": [{"type": "predicate", "instructions": "?"}]
        }));
        let translated = req.to_system_one().expect("translates");
        assert_eq!(
            translated.request.safety_identifier.as_deref(),
            Some("user-1")
        );
        let upstream =
            system_one_to_decision_request(&translated.request, "gpt-6-luna").expect("builds");
        assert_eq!(upstream.safety_identifier.as_deref(), Some("user-1"));
        assert!(
            !serde_json::to_string(&translated.request)
                .expect("serializes")
                .contains("user-1"),
            "the identifier must not reach providers that serialize the System One request"
        );
    }

    /// The reference request from `OpenAI`'s API docs parses and translates to a noul.
    #[test]
    fn reference_request_translates_to_a_noul() {
        let req = request(json!({
            "model": "gpt-6-luna",
            "input": "The package arrived with a broken screen.",
            "questions": [{"type": "predicate", "name": "damaged", "instructions": "Does the customer report a damaged item?"}]
        }));
        let translated = req.to_system_one().expect("translates");
        assert_eq!(
            serde_json::to_value(&translated.request).expect("serializes"),
            json!({
                "model": "gpt-6-luna",
                "state": "The package arrived with a broken screen.",
                "questions": {"q000": {"type": "noul", "instructions": "Does the customer report a damaged item?"}}
            })
        );
    }

    #[test]
    fn unknown_fields_are_rejected_like_openai() {
        for body in [
            json!({"model": "m", "input": "x", "questions": [{"type": "predicate", "instructions": "i"}], "stream": true}),
            json!({"model": "m", "input": "x", "questions": [{"type": "predicate", "instructions": "i", "criteria": {"true": "t"}}]}),
            json!({"model": "m", "input": "x", "questions": [{"type": "choice", "instructions": "i", "choices": [{"value": "a", "weight": 1}, {"value": "b"}]}]}),
        ] {
            let err = serde_json::from_value::<DecisionRequest>(body.clone())
                .expect_err("an unknown field must be rejected");
            assert!(err.to_string().contains("unknown field"), "{body}: {err}");
        }
    }

    #[test]
    fn every_kind_translates_and_answers_come_back_in_question_order() {
        let req = request(json!({
            "model": "jev",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "First."}, {"type": "input_text", "text": "Second."}]}],
            "questions": [
                {"type": "score", "name": "severity", "instructions": "How severe?", "levels": [{"label": "Cosmetic"}, {"label": "Blocked", "description": "Cannot proceed"}]},
                {"type": "choice", "instructions": "Which team?", "choices": [{"value": "technical"}, {"value": "billing", "description": "Payments"}]},
                {"type": "predicate", "name": "urgent", "instructions": "Is it urgent?"}
            ]
        }));
        let translated = req.to_system_one().expect("translates");
        assert_eq!(
            serde_json::to_value(&translated.request).expect("serializes"),
            json!({
                "model": "jev",
                "state": "First.\n\nSecond.",
                "questions": {
                    "q000": {"type": "score", "instructions": "How severe?", "criteria": ["Cosmetic", {"label": "Blocked", "description": "Cannot proceed"}]},
                    "q001": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "Payments", "technical": null}},
                    "q002": {"type": "noul", "instructions": "Is it urgent?"}
                }
            })
        );

        let answers: BTreeMap<String, Answer> = serde_json::from_value(json!({
            "q000": {"type": "score", "score": 1.1, "legend": {}, "probabilities": {"0": 0.1, "1": 0.9}, "confidence": 0.8},
            "q001": {"type": "choice", "choice": "billing", "probabilities": {"billing": 0.7, "technical": 0.3}, "confidence": 0.4},
            "q002": {"type": "noul", "noul": 0.25}
        }))
        .expect("answers");
        let response = translated
            .decision_response(EvaluateResponse {
                model: "jev-1.13.0".into(),
                answers,
                usage: Some(Usage {
                    input_tokens: 40,
                    output_tokens: 2,
                }),
            })
            .expect("maps back");
        assert_eq!(
            serde_json::to_value(&response).expect("serializes"),
            json!({
                "model": "jev-1.13.0",
                "answers": [
                    {"type": "score", "name": "severity", "score": 1.1, "probabilities": [
                        {"value": 0, "label": "Cosmetic", "probability": 0.1},
                        {"value": 1, "label": "Blocked", "probability": 0.9}
                    ], "confidence": 0.8},
                    {"type": "choice", "name": null, "choice": "billing", "probabilities": [
                        {"value": "technical", "probability": 0.3},
                        {"value": "billing", "probability": 0.7}
                    ], "confidence": 0.4},
                    {"type": "predicate", "name": "urgent", "probability": 0.25}
                ],
                "usage": {
                    "input_tokens": 40,
                    "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                    "output_tokens": 2,
                    "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 42
                }
            })
        );
    }

    #[test]
    fn boolean_choices_keep_their_type() {
        let req = request(json!({
            "model": "jev",
            "input": "x",
            "questions": [{"type": "choice", "instructions": "Valid?", "choices": [{"value": true}, {"value": false}]}]
        }));
        let translated = req.to_system_one().expect("translates");
        let answers: BTreeMap<String, Answer> = serde_json::from_value(json!({
            "q000": {"type": "choice", "choice": "false", "probabilities": {"false": 0.6, "true": 0.4}, "confidence": 0.2}
        }))
        .expect("answers");
        let response = translated
            .decision_response(EvaluateResponse {
                model: "jev".into(),
                answers,
                usage: None,
            })
            .expect("maps back");
        assert_eq!(
            serde_json::to_value(&response).expect("serializes"),
            json!({"model": "jev", "answers": [{"type": "choice", "name": null, "choice": false, "probabilities": [
                {"value": true, "probability": 0.4}, {"value": false, "probability": 0.6}
            ], "confidence": 0.2}]})
        );
    }

    #[test]
    fn limits_and_images_are_refused_with_the_field_named() {
        let cases = [
            (
                json!({"model": "m", "input": "x", "questions": []}),
                "questions",
            ),
            (
                json!({"model": "m", "input": "x", "questions": [{"type": "choice", "instructions": "i", "choices": [{"value": "only"}]}]}),
                "questions[0].choices",
            ),
            (
                json!({"model": "m", "input": "x", "questions": [{"type": "choice", "instructions": "i", "choices": [{"value": "a"}, {"value": "a"}]}]}),
                "questions[0].choices",
            ),
            (
                json!({"model": "m", "input": "x", "questions": [{"type": "score", "instructions": "i", "levels": [{"label": "one"}]}]}),
                "questions[0].levels",
            ),
            (
                json!({"model": "m", "input": [{"role": "user", "content": [{"type": "input_image", "image_url": "data:image/png;base64,AAAA"}]}], "questions": [{"type": "predicate", "instructions": "i"}]}),
                "input[0].content[0]",
            ),
            (
                json!({"model": " ", "input": "x", "questions": [{"type": "predicate", "instructions": "i"}]}),
                "model",
            ),
        ];
        for (body, param) in cases {
            let err = request(body.clone())
                .to_system_one()
                .expect_err("must be refused");
            assert_eq!(err.param, param, "{body}");
        }

        let eleven: Vec<_> = (0..11).map(|i| json!({"label": format!("l{i}")})).collect();
        let err = request(json!({"model": "m", "input": "x", "questions": [{"type": "score", "instructions": "i", "levels": eleven}]}))
            .to_system_one()
            .expect_err("eleven levels");
        assert_eq!(
            err.message,
            "A score question needs between 2 and 10 levels; it has 11."
        );

        let many: Vec<_> = (0..201)
            .map(|_| json!({"type": "predicate", "instructions": "i"}))
            .collect();
        let err = request(json!({"model": "m", "input": "x", "questions": many}))
            .to_system_one()
            .expect_err("201 questions");
        assert_eq!(
            err.message,
            "`questions` must contain between 1 and 200 questions; it has 201."
        );
    }

    /// The reference response from `OpenAI`'s API docs reads back as System One answers.
    #[test]
    fn openai_response_reads_back_as_system_one() {
        let asked: EvaluateRequest = serde_json::from_value(json!({
            "model": "luna",
            "state": {"ticket": "The package arrived with a broken screen."},
            "questions": {
                "damaged": {"type": "noul", "instructions": "Damaged?", "criteria": {"true": "The item is broken", "false": "The item works"}},
                "team": {"type": "choice", "criteria": {"billing": "Payments", "technical": null}},
                "severity": {"type": "score", "instructions": "How severe?", "criteria": ["Cosmetic", {"label": "Blocked", "description": "Cannot proceed"}]}
            }
        }))
        .expect("system one request");

        let decision = system_one_to_decision_request(&asked, "gpt-6-luna").expect("builds");
        assert_eq!(
            serde_json::to_value(&decision).expect("serializes"),
            json!({
                "model": "gpt-6-luna",
                "input": "{\"ticket\":\"The package arrived with a broken screen.\"}",
                "questions": [
                    {"type": "predicate", "name": "damaged", "instructions": "Damaged?\nCriteria for true: The item is broken\nCriteria for false: The item works"},
                    {"type": "score", "name": "severity", "instructions": "How severe?", "levels": [{"label": "Cosmetic"}, {"label": "Blocked", "description": "Cannot proceed"}]},
                    {"type": "choice", "name": "team", "instructions": "", "choices": [{"value": "billing", "description": "Payments"}, {"value": "technical"}]}
                ]
            })
        );

        let response: DecisionResponse = serde_json::from_value(json!({
            "model": "gpt-6-luna",
            "answers": [
                {"type": "predicate", "name": "damaged", "probability": 0.95},
                {"type": "score", "name": "severity", "score": 0.9, "probabilities": [{"value": 0, "label": "Cosmetic", "probability": 0.1}, {"value": 1, "label": "Blocked", "probability": 0.9}], "confidence": 0.8},
                {"type": "refusal", "name": "team"}
            ],
            "usage": {"input_tokens": 42, "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0}, "output_tokens": 3, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 45}
        }))
        .expect("openai response");
        let system_one = decision_response_to_system_one(&asked, response).expect("reads back");
        crate::check_answers("luna", &asked.questions, &system_one).expect("answers hold");
        assert_eq!(
            serde_json::to_value(&system_one).expect("serializes"),
            json!({
                "model": "gpt-6-luna",
                "answers": {
                    "damaged": {"type": "noul", "noul": 0.95},
                    "severity": {"type": "score", "score": 0.9, "legend": {"0": "Cosmetic", "1": {"label": "Blocked", "description": "Cannot proceed"}}, "probabilities": {"0": 0.1, "1": 0.9}, "confidence": 0.8},
                    "team": {"type": "refusal"}
                },
                "usage": {"input_tokens": 42, "output_tokens": 3}
            })
        );
    }

    #[test]
    fn an_unmatched_or_repeated_answer_is_an_error_not_a_guess() {
        let asked: EvaluateRequest = serde_json::from_value(json!({
            "model": "luna", "state": "x",
            "questions": {"a": {"type": "noul", "instructions": "A?"}}
        }))
        .expect("request");
        for answers in [
            json!([{"type": "predicate", "name": null, "probability": 0.5}]),
            json!([{"type": "predicate", "name": "b", "probability": 0.5}]),
            json!([{"type": "predicate", "name": "a", "probability": 0.5}, {"type": "predicate", "name": "a", "probability": 0.4}]),
        ] {
            let response: DecisionResponse = serde_json::from_value(
                json!({"model": "gpt-6-luna", "answers": answers, "usage": {"input_tokens": 1, "output_tokens": 0}}),
            )
            .expect("response");
            decision_response_to_system_one(&asked, response)
                .expect_err("must not be matched by guesswork");
        }
    }
}
