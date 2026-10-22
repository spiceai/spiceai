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

//! Maps an `OpenAI` `reasoning_effort` onto the Converse `additionalModelRequestFields` of
//! the Bedrock model family that serves the request. Each family takes a different shape:
//!
//! - Claude 3.7 Sonnet to Claude Opus 4.5: `thinking.type: enabled` with a `budget_tokens`.
//!   <https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-extended-thinking.html>
//! - Claude Opus 4.6, Claude Sonnet 4.6 and later: `thinking.type: adaptive` with
//!   `output_config.effort`.
//!   <https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-adaptive-thinking.html>
//! - Amazon Nova 2 Lite: `reasoningConfig` with `maxReasoningEffort`.
//!   <https://docs.aws.amazon.com/nova/latest/nova2-userguide/extended-thinking.html>
//! - `OpenAI` gpt-oss: the chat-completions `reasoning_effort` field.
//!   <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-openai.html>

use async_openai::error::OpenAIError;
use async_openai::types::chat::ReasoningEffort;
use aws_smithy_types::Document;
use serde::Serialize;

use super::util::value_to_document;

/// Smallest Claude `budget_tokens` that Bedrock accepts.
const CLAUDE_MIN_BUDGET_TOKENS: u32 = 1024;

/// Output tokens kept for the answer, after the thinking budget, when the request sets
/// no `max_tokens`. With the largest budget the total stays below 21,333, the limit above
/// which Bedrock requires a streaming request for Claude.
const CLAUDE_ANSWER_TOKENS: u32 = 4096;

/// How a Bedrock model takes a reasoning effort in the Converse API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReasoningControl {
    /// `thinking.type: enabled` with `budget_tokens`.
    ClaudeBudget,
    /// `thinking.type: adaptive` with `output_config.effort`.
    ClaudeAdaptive,
    /// `reasoningConfig.maxReasoningEffort`.
    Nova,
    /// `reasoning_effort`.
    GptOss,
    /// No documented reasoning control (for example, Claude 3.5 or an application inference
    /// profile ARN, which does not name its model).
    Unsupported,
}

/// Reasoning settings for one Converse request.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReasoningRequest {
    /// Value for the Converse `additionalModelRequestFields`.
    pub(super) additional_model_request_fields: Document,
    /// `maxTokens` to send in place of the request's own value.
    pub(super) max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum Effort {
    Low,
    Medium,
    High,
}

impl Effort {
    /// The nearest effort of the three that every family accepts. `xhigh` is `high`, as in the
    /// Anthropic provider, because some Claude models reject `xhigh`. `None` is `none`.
    fn nearest(effort: &ReasoningEffort) -> Option<Self> {
        match effort {
            ReasoningEffort::None => None,
            ReasoningEffort::Minimal | ReasoningEffort::Low => Some(Self::Low),
            ReasoningEffort::Medium => Some(Self::Medium),
            ReasoningEffort::High | ReasoningEffort::Xhigh => Some(Self::High),
        }
    }

    fn claude_budget_tokens(self) -> u32 {
        match self {
            Self::Low => CLAUDE_MIN_BUDGET_TOKENS,
            Self::Medium => 4096,
            Self::High => 16384,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClaudeThinking {
    Enabled { budget_tokens: u32 },
    Adaptive,
    Disabled,
}

#[derive(Debug, Serialize)]
struct ClaudeOutputConfig {
    effort: Effort,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum NovaReasoningConfig {
    Enabled {
        #[serde(rename = "maxReasoningEffort")]
        max_reasoning_effort: Effort,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AdditionalModelRequestFields {
    Claude {
        thinking: ClaudeThinking,
        #[serde(skip_serializing_if = "Option::is_none")]
        output_config: Option<ClaudeOutputConfig>,
    },
    Nova {
        #[serde(rename = "reasoningConfig")]
        reasoning_config: NovaReasoningConfig,
    },
    GptOss {
        reasoning_effort: Effort,
    },
}

/// A Claude model version, for example `(4, 5)` for Claude Sonnet 4.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ClaudeVersion {
    major: u32,
    minor: u32,
}

impl ClaudeVersion {
    /// First Claude version with extended thinking.
    const EXTENDED_THINKING: Self = Self { major: 3, minor: 7 };
    /// First Claude version with adaptive thinking.
    const ADAPTIVE_THINKING: Self = Self { major: 4, minor: 6 };

    /// Parses the version from a Bedrock Claude model name. Names put the version either
    /// before the family (`claude-3-7-sonnet-20250219-v1:0`) or after it
    /// (`claude-sonnet-4-5-20250929-v1:0`, `claude-opus-4-6-v1`, `claude-opus-5`).
    fn parse(name: &str) -> Option<Self> {
        // A version part has one or two digits, so a date part (`20250514`) is not a minor.
        fn version_part(part: Option<&str>) -> Option<u32> {
            part.filter(|p| (1..=2).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|p| p.parse().ok())
        }

        let mut parts = name.strip_prefix("claude-")?.split('-').peekable();
        if version_part(parts.peek().copied()).is_none() {
            // Skip the family, for example `sonnet`.
            parts.next();
        }
        let major = version_part(parts.next())?;
        let minor = version_part(parts.next()).unwrap_or(0);
        Some(Self { major, minor })
    }
}

impl ReasoningControl {
    /// The reasoning control for a Bedrock model ID, inference profile ID, or ARN.
    pub(super) fn for_model(model_id: &str) -> Self {
        // An ARN ends in `.../<model or inference profile ID>`.
        let id = model_id.rsplit('/').next().unwrap_or(model_id);
        // An inference profile ID puts a geography before the provider, for example
        // `us.anthropic.claude-sonnet-4-5-20250929-v1:0`.
        let mut parts = id.split('.');
        let Some(provider) = parts.find(|p| matches!(*p, "anthropic" | "amazon" | "openai")) else {
            return Self::Unsupported;
        };
        let name = parts.collect::<Vec<_>>().join(".");

        match provider {
            "anthropic" => match ClaudeVersion::parse(&name) {
                Some(v) if v >= ClaudeVersion::ADAPTIVE_THINKING => Self::ClaudeAdaptive,
                Some(v) if v >= ClaudeVersion::EXTENDED_THINKING => Self::ClaudeBudget,
                _ => Self::Unsupported,
            },
            "amazon" if name.starts_with("nova-2-lite") => Self::Nova,
            "openai" if name.starts_with("gpt-oss-") => Self::GptOss,
            _ => Self::Unsupported,
        }
    }

    /// The reasoning settings for a request with `effort` and `max_tokens`, or `None` when the
    /// model's default already gives that effort.
    pub(super) fn request(
        self,
        model_id: &str,
        effort: Option<&ReasoningEffort>,
        max_tokens: Option<u32>,
    ) -> Result<Option<ReasoningRequest>, OpenAIError> {
        let Some(effort) = effort else {
            return Ok(None);
        };

        let (fields, max_tokens) = match (self, Effort::nearest(effort)) {
            // Thinking is off by default on these models.
            (Self::ClaudeBudget | Self::Nova, None) => return Ok(None),
            (Self::ClaudeBudget, Some(effort)) => {
                let (budget_tokens, max_tokens) =
                    claude_budget(model_id, effort.claude_budget_tokens(), max_tokens)?;
                (
                    AdditionalModelRequestFields::Claude {
                        thinking: ClaudeThinking::Enabled { budget_tokens },
                        output_config: None,
                    },
                    max_tokens,
                )
            }
            // Some of these models think by default, and only `disabled` turns it off.
            (Self::ClaudeAdaptive, None) => (
                AdditionalModelRequestFields::Claude {
                    thinking: ClaudeThinking::Disabled,
                    output_config: None,
                },
                max_tokens,
            ),
            (Self::ClaudeAdaptive, Some(effort)) => (
                AdditionalModelRequestFields::Claude {
                    thinking: ClaudeThinking::Adaptive,
                    output_config: Some(ClaudeOutputConfig { effort }),
                },
                max_tokens,
            ),
            (Self::Nova, Some(effort)) => (
                AdditionalModelRequestFields::Nova {
                    reasoning_config: NovaReasoningConfig::Enabled {
                        max_reasoning_effort: effort,
                    },
                },
                max_tokens,
            ),
            (Self::GptOss, Some(effort)) => (
                AdditionalModelRequestFields::GptOss {
                    reasoning_effort: effort,
                },
                max_tokens,
            ),
            (Self::GptOss, None) => {
                tracing::warn!(
                    "Model '{model_id}' always reasons, so `reasoning_effort` 'none' has no effect and the model uses its default effort. Set `reasoning_effort` to 'low', 'medium' or 'high'. See: https://spiceai.org/docs/components/models/aws-bedrock"
                );
                return Ok(None);
            }
            (Self::Unsupported, _) => {
                tracing::warn!(
                    "{}",
                    unsupported_effort_warning(model_id, effort_name(effort))
                );
                return Ok(None);
            }
        };

        let fields = serde_json::to_value(fields)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        Ok(Some(ReasoningRequest {
            additional_model_request_fields: value_to_document(fields),
            max_tokens,
        }))
    }
}

/// The `budget_tokens` and `maxTokens` for a Claude extended thinking request. Bedrock
/// requires `budget_tokens` of at least 1,024 and less than `maxTokens`.
fn claude_budget(
    model_id: &str,
    budget_tokens: u32,
    max_tokens: Option<u32>,
) -> Result<(u32, Option<u32>), OpenAIError> {
    let Some(max_tokens) = max_tokens else {
        return Ok((budget_tokens, Some(budget_tokens + CLAUDE_ANSWER_TOKENS)));
    };
    if max_tokens <= CLAUDE_MIN_BUDGET_TOKENS {
        return Err(OpenAIError::InvalidArgument(format!(
            "Model '{model_id}' needs `max_completion_tokens` greater than {CLAUDE_MIN_BUDGET_TOKENS} to use `reasoning_effort`, because its smallest thinking budget is {CLAUDE_MIN_BUDGET_TOKENS} tokens and must be less than `max_completion_tokens`. Increase `max_completion_tokens`, or omit `reasoning_effort`. See: https://spiceai.org/docs/components/models/aws-bedrock"
        )));
    }
    // Keep at least half of `max_tokens` for the answer when the budget allows it.
    let budget_tokens = budget_tokens
        .min(max_tokens / 2)
        .max(CLAUDE_MIN_BUDGET_TOKENS);
    Ok((budget_tokens, Some(max_tokens)))
}

fn effort_name(effort: &ReasoningEffort) -> &'static str {
    match effort {
        ReasoningEffort::None => "none",
        ReasoningEffort::Minimal => "minimal",
        ReasoningEffort::Low => "low",
        ReasoningEffort::Medium => "medium",
        ReasoningEffort::High => "high",
        ReasoningEffort::Xhigh => "xhigh",
    }
}

fn unsupported_effort_warning(model_id: &str, effort: &str) -> String {
    format!(
        "Model '{model_id}' has no reasoning setting that Spice can send through Amazon Bedrock, so `reasoning_effort` '{effort}' is ignored and the model uses its default. Use a Bedrock model that supports reasoning, or omit `reasoning_effort`. See: https://spiceai.org/docs/components/models/aws-bedrock"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn fields(
        model_id: &str,
        effort: ReasoningEffort,
        max_tokens: Option<u32>,
    ) -> Option<ReasoningRequest> {
        ReasoningControl::for_model(model_id)
            .request(model_id, Some(&effort), max_tokens)
            .expect("reasoning request should build")
    }

    fn expected(value: Value, max_tokens: Option<u32>) -> Option<ReasoningRequest> {
        Some(ReasoningRequest {
            additional_model_request_fields: value_to_document(value),
            max_tokens,
        })
    }

    #[test]
    fn model_ids_map_to_their_reasoning_control() {
        let cases = [
            // Model IDs from the Bedrock extended and adaptive thinking pages.
            (
                "anthropic.claude-3-7-sonnet-20250219-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "us.anthropic.claude-sonnet-4-20250514-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "anthropic.claude-opus-4-1-20250805-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "global.anthropic.claude-haiku-4-5-20251001-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "anthropic.claude-opus-4-5-20251101-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            (
                "us.anthropic.claude-opus-4-6-v1",
                ReasoningControl::ClaudeAdaptive,
            ),
            (
                "anthropic.claude-sonnet-4-6",
                ReasoningControl::ClaudeAdaptive,
            ),
            ("anthropic.claude-opus-5", ReasoningControl::ClaudeAdaptive),
            (
                "anthropic.claude-fable-5-1",
                ReasoningControl::ClaudeAdaptive,
            ),
            (
                "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                ReasoningControl::ClaudeBudget,
            ),
            // Models without a reasoning control.
            (
                "anthropic.claude-3-5-sonnet-20240620-v1:0",
                ReasoningControl::Unsupported,
            ),
            (
                "anthropic.claude-3-haiku-20240307-v1:0",
                ReasoningControl::Unsupported,
            ),
            ("anthropic.claude-v2:1", ReasoningControl::Unsupported),
            ("amazon.nova-pro-v1:0", ReasoningControl::Unsupported),
            (
                "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/a1b2c3",
                ReasoningControl::Unsupported,
            ),
            ("us.amazon.nova-2-lite-v1:0", ReasoningControl::Nova),
            ("openai.gpt-oss-120b-1:0", ReasoningControl::GptOss),
            ("openai.gpt-oss-20b-1:0", ReasoningControl::GptOss),
        ];
        for (model_id, control) in cases {
            assert_eq!(ReasoningControl::for_model(model_id), control, "{model_id}");
        }
    }

    #[test]
    fn claude_extended_thinking_sends_a_thinking_budget() {
        // Regression test for #15409. Shape from the Bedrock extended thinking page.
        assert_eq!(
            fields(
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                ReasoningEffort::High,
                None
            ),
            expected(
                json!({"thinking": {"type": "enabled", "budget_tokens": 16384}}),
                Some(16384 + CLAUDE_ANSWER_TOKENS)
            )
        );
        assert_eq!(
            fields(
                "anthropic.claude-3-7-sonnet-20250219-v1:0",
                ReasoningEffort::Minimal,
                Some(8000)
            ),
            expected(
                json!({"thinking": {"type": "enabled", "budget_tokens": 1024}}),
                Some(8000)
            )
        );
    }

    #[test]
    fn claude_thinking_budget_stays_below_max_tokens() {
        let model = "anthropic.claude-opus-4-5-20251101-v1:0";
        assert_eq!(
            fields(model, ReasoningEffort::High, Some(10000)),
            expected(
                json!({"thinking": {"type": "enabled", "budget_tokens": 5000}}),
                Some(10000)
            )
        );
        assert_eq!(
            fields(model, ReasoningEffort::Medium, Some(1500)),
            expected(
                json!({"thinking": {"type": "enabled", "budget_tokens": 1024}}),
                Some(1500)
            )
        );

        let Err(OpenAIError::InvalidArgument(message)) = ReasoningControl::for_model(model)
            .request(model, Some(&ReasoningEffort::Low), Some(1024))
        else {
            panic!("max_tokens of 1024 leaves no room for the smallest thinking budget");
        };
        assert_eq!(
            message,
            "Model 'anthropic.claude-opus-4-5-20251101-v1:0' needs `max_completion_tokens` greater than 1024 to use `reasoning_effort`, because its smallest thinking budget is 1024 tokens and must be less than `max_completion_tokens`. Increase `max_completion_tokens`, or omit `reasoning_effort`. See: https://spiceai.org/docs/components/models/aws-bedrock"
        );
    }

    #[test]
    fn claude_adaptive_thinking_sends_an_effort() {
        // Shape from the Bedrock adaptive thinking page, Converse API section.
        let model = "us.anthropic.claude-opus-4-6-v1";
        for (effort, level) in [
            (ReasoningEffort::Minimal, "low"),
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Xhigh, "high"),
        ] {
            assert_eq!(
                fields(model, effort.clone(), Some(2000)),
                expected(
                    json!({"thinking": {"type": "adaptive"}, "output_config": {"effort": level}}),
                    Some(2000)
                ),
                "{effort:?}"
            );
        }
        assert_eq!(
            fields(model, ReasoningEffort::None, None),
            expected(json!({"thinking": {"type": "disabled"}}), None)
        );
    }

    #[test]
    fn nova_sends_a_reasoning_config() {
        // Shape from the Amazon Nova 2 extended thinking page.
        assert_eq!(
            fields("us.amazon.nova-2-lite-v1:0", ReasoningEffort::Medium, None),
            expected(
                json!({"reasoningConfig": {"type": "enabled", "maxReasoningEffort": "medium"}}),
                None
            )
        );
        assert_eq!(
            fields("us.amazon.nova-2-lite-v1:0", ReasoningEffort::None, None),
            None
        );
    }

    #[test]
    fn gpt_oss_sends_reasoning_effort() {
        // Converse passes chat-completions fields such as `reasoning_effort` through
        // `additionalModelRequestFields` (Bedrock OpenAI models page).
        assert_eq!(
            fields("openai.gpt-oss-120b-1:0", ReasoningEffort::Low, Some(500)),
            expected(json!({"reasoning_effort": "low"}), Some(500))
        );
    }

    #[test]
    fn no_effort_sends_nothing() {
        for model in [
            "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
            "us.anthropic.claude-opus-4-6-v1",
            "us.amazon.nova-2-lite-v1:0",
            "openai.gpt-oss-120b-1:0",
        ] {
            assert_eq!(
                ReasoningControl::for_model(model)
                    .request(model, None, Some(100))
                    .expect("no effort should build"),
                None,
                "{model}"
            );
        }
    }

    #[test]
    fn unsupported_model_ignores_effort_with_a_warning() {
        assert_eq!(
            fields(
                "anthropic.claude-3-5-sonnet-20240620-v1:0",
                ReasoningEffort::High,
                None
            ),
            None
        );
        assert_eq!(
            unsupported_effort_warning("anthropic.claude-3-5-sonnet-20240620-v1:0", "high"),
            "Model 'anthropic.claude-3-5-sonnet-20240620-v1:0' has no reasoning setting that Spice can send through Amazon Bedrock, so `reasoning_effort` 'high' is ignored and the model uses its default. Use a Bedrock model that supports reasoning, or omit `reasoning_effort`. See: https://spiceai.org/docs/components/models/aws-bedrock"
        );
    }
}
