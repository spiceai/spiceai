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

//! Answers System One evaluations with any chat model.
//!
//! [`ChatEvaluator`] implements [`Evaluate`] on top of a [`Chat`] model, so
//! `POST /v1/evaluate` works with every chat model a Spicepod configures, not only
//! System One models such as `TypeSafe` Jev. Each evaluation:
//!
//! 1. builds a JSON schema from the typed questions (see `schema`), pinning every
//!    answer to its own question's options or rubric levels;
//! 2. sends a system prompt that treats `state` as an untrusted document, and the
//!    document itself;
//! 3. validates the reply against the questions, and on a malformed reply sends the
//!    problems back as a corrective turn, up to
//!    [`ChatEvaluatorOptions::max_corrective_retries`] times;
//! 4. converts the reply into typed answers — a choice is its most probable option, a
//!    score the probability-weighted average level — and holds them to
//!    [`evaluate_api::check_answers`], the same invariants a System One provider's
//!    answers must meet.
//!
//! The probabilities are the model's own estimates. Unlike a System One model's, they
//! are not calibrated, and in [`AnswerMode::Discrete`] every distribution is one-hot.
//!
//! The prompts and answer conversion follow `TypeSafe`'s `system-one-adapter-python`
//! (<https://github.com/typesafe-ai/system-one-adapter-python>, MIT License; see
//! `NOTICE` in this crate), so results can be compared with that adapter's.

use std::fmt::Debug;
use std::sync::Arc;

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestMessage,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestUserMessage,
    CreateChatCompletionRequest, CreateChatCompletionResponse, FinishReason, ResponseFormat,
    ResponseFormatJsonSchema,
};
use async_trait::async_trait;
use chat_api::{ApiErrorKind, Chat};
use evaluate_api::{
    Error, Evaluate, EvaluateRequest, EvaluateResponse, Result, Usage, check_answers,
    check_questions,
};

mod decode;
mod prompt;
mod schema;

/// The longest excerpt of a rejected reply quoted in the error.
const MAX_QUOTED_REPLY_CHARS: usize = 500;

/// `text` cut to `max_chars` characters, with an ellipsis when anything was cut.
pub(crate) fn truncated(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// How the model reports each answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AnswerMode {
    /// A probability for each noul, and a probability for every option of a choice
    /// and every level of a score.
    #[default]
    Probabilities,
    /// One value per question: `true`/`false`, an option, or a level. Distributions
    /// are one-hot, so every confidence is 1.
    Discrete,
}

/// How the reply's schema reaches the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    /// The schema is written into the system prompt and the reply is validated here.
    /// Works with any chat model.
    #[default]
    Prompted,
    /// The schema is sent as a strict JSON-schema `response_format`, for models whose
    /// provider enforces it. A provider that ignores `response_format` leaves the model
    /// without the schema, so use this only where enforcement is known.
    Native,
}

/// How a [`ChatEvaluator`] asks for and checks answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEvaluatorOptions {
    pub answer_mode: AnswerMode,
    pub output_mode: OutputMode,
    /// How many times a reply that fails validation is sent back to the model with
    /// its problems before the evaluation fails.
    pub max_corrective_retries: usize,
}

impl Default for ChatEvaluatorOptions {
    fn default() -> Self {
        Self {
            answer_mode: AnswerMode::default(),
            output_mode: OutputMode::default(),
            max_corrective_retries: 1,
        }
    }
}

/// An [`Evaluate`] model answered by a chat model.
pub struct ChatEvaluator {
    /// Spicepod model name: the name the chat model is called by, and the one errors
    /// and responses report.
    name: String,
    chat: Arc<dyn Chat>,
    options: ChatEvaluatorOptions,
}

impl Debug for ChatEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatEvaluator")
            .field("name", &self.name)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl ChatEvaluator {
    /// Evaluates with `chat`, the chat model the Spicepod names `name`.
    #[must_use]
    pub fn new(name: impl Into<String>, chat: Arc<dyn Chat>) -> Self {
        Self {
            name: name.into(),
            chat,
            options: ChatEvaluatorOptions::default(),
        }
    }

    #[must_use]
    pub fn with_options(mut self, options: ChatEvaluatorOptions) -> Self {
        self.options = options;
        self
    }

    /// The reply text of a completed response, or the reason there is none.
    fn reply_text(&self, response: CreateChatCompletionResponse) -> Result<String> {
        let failed = |reason: String| Error::ModelCallFailed {
            model: self.name.clone(),
            source: reason.into(),
        };
        let Some(choice) = response.choices.into_iter().next() else {
            return Err(failed("the model returned no reply".to_string()));
        };
        if let Some(refusal) = choice.message.refusal {
            return Err(failed(format!("the model refused to answer: {refusal}")));
        }
        match choice.finish_reason {
            Some(FinishReason::Stop) | None => Ok(choice.message.content.unwrap_or_default()),
            Some(FinishReason::Length) => Err(failed(
                "the reply was cut off at the model's output token limit before every question was answered. Raise the model's maximum output tokens, or ask fewer questions per request".to_string(),
            )),
            Some(FinishReason::ContentFilter) => Err(failed(
                "the provider's content filter withheld the reply".to_string(),
            )),
            Some(FinishReason::ToolCalls | FinishReason::FunctionCall) => Err(failed(
                "the model called a tool instead of answering".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Evaluate for ChatEvaluator {
    async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse> {
        check_questions(&self.name, &request.questions)?;
        let EvaluateRequest {
            state, questions, ..
        } = request;
        let ChatEvaluatorOptions {
            answer_mode,
            output_mode,
            max_corrective_retries,
        } = self.options;

        let document = prompt::document(&state).map_err(|e| Error::InvalidRequest {
            model: self.name.clone(),
            message: format!("`state` could not be written as JSON: {e}"),
        })?;
        // `state` can be large, and every attempt sends the document built from it.
        drop(state);

        let schema = schema::reply_schema(&questions, answer_mode);
        let mut messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessage::from(prompt::system_prompt(
                answer_mode,
                output_mode,
                &schema,
            ))
            .into(),
            ChatCompletionRequestUserMessage::from(document).into(),
        ];
        let response_format = match output_mode {
            OutputMode::Prompted => ResponseFormat::Text,
            OutputMode::Native => ResponseFormat::JsonSchema {
                json_schema: ResponseFormatJsonSchema {
                    name: "evaluation".to_string(),
                    description: None,
                    schema: Some(schema),
                    strict: Some(true),
                },
            },
        };

        // Every attempt is billed, so usage is the total across them. A count some
        // attempt did not report makes the total unknown rather than too low.
        let mut usage = Some(Usage {
            input_tokens: 0,
            output_tokens: 0,
        });
        let mut corrective_retries = 0;
        loop {
            let response = self
                .chat
                .chat_request(CreateChatCompletionRequest {
                    model: self.name.clone(),
                    messages: messages.clone(),
                    // Always set, so a `response_format` default configured on the chat
                    // model cannot replace the one this evaluation needs.
                    response_format: Some(response_format.clone()),
                    ..Default::default()
                })
                .await
                .map_err(|e| chat_error(&self.name, e))?;
            usage = match (usage, response.usage.as_ref()) {
                (Some(total), Some(reported)) => Some(Usage {
                    input_tokens: total.input_tokens + u64::from(reported.prompt_tokens),
                    output_tokens: total.output_tokens + u64::from(reported.completion_tokens),
                }),
                _ => None,
            };
            let reply = self.reply_text(response)?;

            match decode::parse_reply(&reply, &questions, answer_mode) {
                Ok(answers) => {
                    let response = EvaluateResponse {
                        model: self.name.clone(),
                        answers,
                        usage,
                    };
                    check_answers(&self.name, &questions, &response)?;
                    return Ok(response);
                }
                Err(problem) if corrective_retries < max_corrective_retries => {
                    corrective_retries += 1;
                    messages.push(ChatCompletionRequestAssistantMessage::from(reply).into());
                    messages.push(
                        ChatCompletionRequestUserMessage::from(prompt::correction(&problem)).into(),
                    );
                }
                Err(problem) => {
                    return Err(Error::UnparseableResponse {
                        model: self.name.clone(),
                        response: format!(
                            "{problem} (after {corrective_retries} corrective {}). Reply: {}",
                            if corrective_retries == 1 {
                                "retry"
                            } else {
                                "retries"
                            },
                            truncated(&reply, MAX_QUOTED_REPLY_CHARS)
                        ),
                    });
                }
            }
        }
    }

    async fn health(&self) -> Result<()> {
        self.chat
            .health()
            .await
            .map_err(|e| Error::HealthCheckFailed {
                source: Box::new(e),
            })
    }
}

/// Maps a chat model's failure onto the evaluation error with the same meaning: a
/// rejected request, key or rate limit keeps its kind, and a provider that cannot be
/// reached is unavailable (as a System One provider's transport failure is). Anything
/// else is a failed call.
///
/// The kind is read by [`ApiErrorKind::of`], the same reading `/v1/chat/completions`
/// takes of the same failure.
fn chat_error(model: &str, error: OpenAIError) -> Error {
    let model = model.to_string();
    match error {
        OpenAIError::InvalidArgument(message) => Error::InvalidRequest { model, message },
        OpenAIError::ApiError(api) => match ApiErrorKind::of(&api) {
            Some(ApiErrorKind::InvalidRequest) => Error::InvalidRequest {
                model,
                message: api.message,
            },
            Some(ApiErrorKind::Authentication) => Error::AuthenticationFailed {
                model,
                message: api.message,
            },
            Some(ApiErrorKind::PermissionDenied) => Error::PermissionDenied {
                model,
                message: api.message,
            },
            Some(ApiErrorKind::RateLimited) => Error::RateLimited {
                model,
                message: api.message,
            },
            Some(ApiErrorKind::InsufficientQuota) | None => Error::ModelCallFailed {
                model,
                source: Box::new(OpenAIError::ApiError(api)),
            },
        },
        OpenAIError::Reqwest(e) => Error::ServiceUnavailable {
            model,
            message: e.to_string(),
        },
        other => Error::ModelCallFailed {
            model,
            source: Box::new(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use async_openai::error::ApiError;
    use chat_api::{SqlGeneration, message_to_content};
    use evaluate_api::Answer;
    use serde_json::{Value, json};

    /// A chat model that answers with scripted replies and records every request.
    struct ScriptedChat {
        replies: Mutex<VecDeque<std::result::Result<CreateChatCompletionResponse, OpenAIError>>>,
        requests: Mutex<Vec<CreateChatCompletionRequest>>,
    }

    impl ScriptedChat {
        fn new(
            replies: impl IntoIterator<
                Item = std::result::Result<CreateChatCompletionResponse, OpenAIError>,
            >,
        ) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(replies.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            })
        }

        fn requests(&self) -> Vec<CreateChatCompletionRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[async_trait]
    impl Chat for ScriptedChat {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }

        async fn chat_request(
            &self,
            req: CreateChatCompletionRequest,
        ) -> std::result::Result<CreateChatCompletionResponse, OpenAIError> {
            self.requests.lock().expect("requests lock").push(req);
            self.replies
                .lock()
                .expect("replies lock")
                .pop_front()
                .expect("a scripted reply for every request")
        }
    }

    fn completion(
        content: &str,
        finish_reason: &str,
        usage: Option<(u32, u32)>,
    ) -> CreateChatCompletionResponse {
        let mut response = json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 0,
            "model": "judge",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason
            }]
        });
        if let Some((prompt_tokens, completion_tokens)) = usage {
            response["usage"] = json!({
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens
            });
        }
        serde_json::from_value(response).expect("chat completion")
    }

    fn reply(content: &Value) -> CreateChatCompletionResponse {
        completion(&content.to_string(), "stop", Some((100, 20)))
    }

    fn request() -> EvaluateRequest {
        serde_json::from_value(json!({
            "model": "judge",
            "state": "Help! My payouts have been failing for 3 days.",
            "questions": {
                "is_urgent": {"type": "noul", "instructions": "Does this convey urgency?"},
                "team": {
                    "type": "choice",
                    "instructions": "Which team should handle this?",
                    "criteria": {"billing": "Payments and payouts", "technical": "Bugs and outages"}
                },
                "tone": {"type": "score", "criteria": ["calm", "annoyed", "furious"]}
            }
        }))
        .expect("request")
    }

    fn valid_answers() -> Value {
        json!({"answers": {
            "is_urgent": 0.9,
            "team": {"billing": 0.8, "technical": 0.2},
            "tone": {"0": 0.1, "1": 0.6, "2": 0.3}
        }})
    }

    fn evaluator(chat: &Arc<ScriptedChat>) -> ChatEvaluator {
        ChatEvaluator::new("judge", Arc::clone(chat) as Arc<dyn Chat>)
    }

    #[tokio::test]
    async fn answers_every_question_from_one_reply() {
        let chat = ScriptedChat::new([Ok(reply(&valid_answers()))]);
        let evaluator = evaluator(&chat);

        let response = evaluator.evaluate(request()).await.expect("evaluation");

        assert_eq!(response.model, "judge");
        assert_eq!(
            response.usage,
            Some(Usage {
                input_tokens: 100,
                output_tokens: 20
            })
        );
        assert_eq!(
            response.answers.get("is_urgent"),
            Some(&Answer::Noul { noul: 0.9 })
        );
        let Some(Answer::Choice {
            choice, confidence, ..
        }) = response.answers.get("team")
        else {
            panic!("team: {:?}", response.answers);
        };
        assert_eq!(choice, "billing");
        assert!((confidence - 0.6).abs() < 1e-12, "{confidence}");
        let Some(Answer::Score { score, .. }) = response.answers.get("tone") else {
            panic!("tone: {:?}", response.answers);
        };
        assert!((score - 1.2).abs() < 1e-12, "{score}");

        let requests = chat.requests();
        assert_eq!(requests.len(), 1);
        let sent = requests.first().expect("one request");
        assert_eq!(sent.model, "judge");
        assert_eq!(sent.response_format, Some(ResponseFormat::Text));
        assert!(
            sent.tools.is_none(),
            "an evaluation offers the model no tools"
        );
        let [system, document] = sent.messages.as_slice() else {
            panic!(
                "expected a system prompt and a document: {:?}",
                sent.messages
            );
        };
        insta::assert_snapshot!("prompted_system_prompt", message_to_content(system));
        assert_eq!(
            message_to_content(document),
            "<document>\n\"Help! My payouts have been failing for 3 days.\"\n</document>"
        );
    }

    #[tokio::test]
    async fn a_malformed_reply_is_sent_back_with_its_problems() {
        let malformed = json!({"answers": {
            "is_urgent": 0.9,
            "team": {"billing": 0.9, "technical": 0.3},
            "tone": {"0": 0.1, "1": 0.6, "2": 0.3}
        }});
        let chat = ScriptedChat::new([Ok(reply(&malformed)), Ok(reply(&valid_answers()))]);
        let evaluator = evaluator(&chat);

        let response = evaluator
            .evaluate(request())
            .await
            .expect("corrected evaluation");

        assert_eq!(
            response.usage,
            Some(Usage {
                input_tokens: 200,
                output_tokens: 40
            }),
            "usage totals every attempt"
        );
        let requests = chat.requests();
        let retry = requests.get(1).expect("a corrective request");
        let [_, _, previous, correction] = retry.messages.as_slice() else {
            panic!(
                "expected the earlier reply and a correction: {:?}",
                retry.messages
            );
        };
        assert_eq!(message_to_content(previous), malformed.to_string());
        assert_eq!(
            message_to_content(correction),
            "The previous response did not match the required schema: question 'team': the probabilities sum to 1.200, but they must sum to 1\n\
             Return a single JSON object that matches the schema exactly, with no other text."
        );
    }

    #[tokio::test]
    async fn a_reply_still_malformed_after_the_retries_fails() {
        let chat = ScriptedChat::new([
            Ok(completion("Sure, it is urgent.", "stop", Some((100, 5)))),
            Ok(completion("{\"answers\": {}}", "stop", Some((120, 5)))),
        ]);
        let evaluator = evaluator(&chat);

        let error = evaluator
            .evaluate(request())
            .await
            .expect_err("malformed twice");

        let Error::UnparseableResponse { model, response } = &error else {
            panic!("expected an unparseable response, got {error:?}");
        };
        assert_eq!(model, "judge");
        assert!(
            response.starts_with("'answers' is missing question 'is_urgent'"),
            "{response}"
        );
        assert!(
            response.contains("(after 1 corrective retry)"),
            "{response}"
        );
        assert!(response.ends_with("Reply: {\"answers\": {}}"), "{response}");
        assert_eq!(chat.requests().len(), 2);
    }

    #[tokio::test]
    async fn no_corrective_retries_fails_on_the_first_malformed_reply() {
        let chat = ScriptedChat::new([Ok(completion("not json", "stop", None))]);
        let evaluator = evaluator(&chat).with_options(ChatEvaluatorOptions {
            max_corrective_retries: 0,
            ..Default::default()
        });

        let error = evaluator.evaluate(request()).await.expect_err("malformed");

        assert!(
            matches!(error, Error::UnparseableResponse { .. }),
            "{error:?}"
        );
        assert_eq!(chat.requests().len(), 1);
    }

    #[tokio::test]
    async fn a_truncated_reply_fails_without_a_retry() {
        let chat = ScriptedChat::new([Ok(completion("{\"answers\": {\"is_ur", "length", None))]);
        let evaluator = evaluator(&chat);

        let error = evaluator.evaluate(request()).await.expect_err("truncated");

        assert!(
            error
                .to_string()
                .contains("cut off at the model's output token limit"),
            "{error}"
        );
        assert_eq!(
            chat.requests().len(),
            1,
            "a longer reply needs a new limit, not a retry"
        );
    }

    #[tokio::test]
    async fn a_refusal_fails_without_a_retry() {
        let mut refused = completion("", "stop", None);
        if let Some(choice) = refused.choices.first_mut() {
            choice.message.content = None;
            choice.message.refusal = Some("I can't help with that.".to_string());
        }
        let chat = ScriptedChat::new([Ok(refused)]);
        let evaluator = evaluator(&chat);

        let error = evaluator.evaluate(request()).await.expect_err("refused");

        assert!(
            error
                .to_string()
                .contains("the model refused to answer: I can't help with that."),
            "{error}"
        );
        assert_eq!(chat.requests().len(), 1);
    }

    #[tokio::test]
    async fn usage_is_unknown_when_an_attempt_does_not_report_it() {
        let malformed = json!({"answers": {"is_urgent": 2}});
        let chat = ScriptedChat::new([
            Ok(completion(&malformed.to_string(), "stop", None)),
            Ok(reply(&valid_answers())),
        ]);
        let evaluator = evaluator(&chat);

        let response = evaluator
            .evaluate(request())
            .await
            .expect("corrected evaluation");

        assert_eq!(response.usage, None);
    }

    #[tokio::test]
    async fn native_mode_sends_the_schema_as_the_response_format() {
        let chat = ScriptedChat::new([Ok(reply(&valid_answers()))]);
        let evaluator = evaluator(&chat).with_options(ChatEvaluatorOptions {
            output_mode: OutputMode::Native,
            ..Default::default()
        });

        evaluator.evaluate(request()).await.expect("evaluation");

        let requests = chat.requests();
        let sent = requests.first().expect("one request");
        let Some(ResponseFormat::JsonSchema { json_schema }) = &sent.response_format else {
            panic!(
                "expected a JSON schema response format: {:?}",
                sent.response_format
            );
        };
        assert_eq!(json_schema.strict, Some(true));
        let schema = json_schema.schema.as_ref().expect("schema");
        assert_eq!(
            schema["properties"]["answers"]["required"],
            json!(["is_urgent", "team", "tone"])
        );
        let system = message_to_content(sent.messages.first().expect("system prompt"));
        assert!(!system.contains("\"additionalProperties\""), "{system}");
    }

    #[tokio::test]
    async fn discrete_mode_reports_one_hot_answers() {
        let chat = ScriptedChat::new([Ok(reply(
            &json!({"answers": {"is_urgent": true, "team": "technical", "tone": 2}}),
        ))]);
        let evaluator = evaluator(&chat).with_options(ChatEvaluatorOptions {
            answer_mode: AnswerMode::Discrete,
            ..Default::default()
        });

        let response = evaluator.evaluate(request()).await.expect("evaluation");

        assert_eq!(
            response.answers.get("is_urgent"),
            Some(&Answer::Noul { noul: 1.0 })
        );
        let Some(Answer::Score {
            score, confidence, ..
        }) = response.answers.get("tone")
        else {
            panic!("tone: {:?}", response.answers);
        };
        assert_eq!((*score, *confidence), (2.0, 1.0));
    }

    #[tokio::test]
    async fn a_choice_with_one_option_is_an_invalid_request() {
        let chat = ScriptedChat::new([]);
        let evaluator = evaluator(&chat);
        let mut request = request();
        request.questions.insert(
            "only".to_string(),
            serde_json::from_value(json!({"type": "choice", "criteria": {"yes": null}}))
                .expect("question"),
        );

        let error = evaluator.evaluate(request).await.expect_err("one option");

        assert!(
            matches!(&error, Error::InvalidRequest { message, .. } if message.contains("'only' needs at least two options")),
            "{error:?}"
        );
        assert!(
            chat.requests().is_empty(),
            "nothing is sent for an invalid request"
        );
    }

    #[tokio::test]
    async fn provider_errors_keep_their_meaning() {
        let api_error = |code: &str| {
            Err(OpenAIError::ApiError(ApiError {
                message: format!("upstream {code}"),
                r#type: None,
                param: None,
                code: Some(code.to_string()),
            }))
        };
        // Anthropic (and some gateways) send the discriminator in `type` and leave
        // `code` unset. Those must not become `ModelCallFailed` / HTTP 500.
        let type_only = |kind: &str| {
            Err(OpenAIError::ApiError(ApiError {
                message: format!("upstream {kind}"),
                r#type: Some(kind.to_string()),
                param: None,
                code: None,
            }))
        };
        // `OpenAI` puts `invalid_request_error` in `type` and the specific reason in `code`.
        let openai = |kind: &str, code: &str| {
            Err(OpenAIError::ApiError(ApiError {
                message: format!("upstream {code}"),
                r#type: Some(kind.to_string()),
                param: None,
                code: Some(code.to_string()),
            }))
        };
        for (failure, expected) in [
            (
                openai("invalid_request_error", "invalid_api_key"),
                "AuthenticationFailed",
            ),
            (
                openai("invalid_request_error", "missing_required_parameter"),
                "InvalidRequest",
            ),
            (api_error("rate_limit_exceeded"), "RateLimited"),
            (api_error("invalid_api_key"), "AuthenticationFailed"),
            (api_error("invalid_request_error"), "InvalidRequest"),
            (api_error("server_error"), "ModelCallFailed"),
            (type_only("authentication_error"), "AuthenticationFailed"),
            (type_only("permission_error"), "PermissionDenied"),
            (type_only("rate_limit_error"), "RateLimited"),
            (
                Err(OpenAIError::InvalidArgument("bad".to_string())),
                "InvalidRequest",
            ),
        ] {
            let chat = ScriptedChat::new([failure]);
            let evaluator = evaluator(&chat);
            let error = evaluator
                .evaluate(request())
                .await
                .expect_err("provider error");
            assert!(
                format!("{error:?}").starts_with(expected),
                "{expected}: {error:?}"
            );
        }
    }
}
