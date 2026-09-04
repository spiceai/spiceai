/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use async_openai::{
    error::OpenAIError,
    types::chat::{ChatCompletionTool, ChatCompletionTools, CompletionUsage, PromptTokensDetails},
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageCreateParams {
    pub max_tokens: u32,
    pub messages: Vec<MessageParam>,
    pub model: AnthropicModelVariant,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoiceParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolParam>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<RequestServiceTier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<ContainerParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_management: Option<ContextManagementConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<Vec<McpServerDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_config: Option<OutputConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_format: Option<OutputFormat>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ThinkingConfig {
    #[serde(rename = "enabled")]
    Enabled { budget_tokens: u32 },
    #[serde(rename = "disabled")]
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestServiceTier {
    Auto,
    StandardOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContainerParam {
    Id(String),
    Config(ContainerConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skills: Option<Vec<SkillParams>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SkillParams {
    pub skill_id: String,
    #[serde(rename = "type")]
    pub skill_type: SkillType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillType {
    Anthropic,
    Custom,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextManagementConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edits: Option<Vec<ContextManagementEdit>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContextManagementEdit {
    #[serde(rename = "clear_tool_uses_20250919")]
    ClearToolUses {
        #[serde(skip_serializing_if = "Option::is_none")]
        trigger: Option<ContextTrigger>,
        #[serde(skip_serializing_if = "Option::is_none")]
        keep: Option<ToolUsesKeep>,
        #[serde(skip_serializing_if = "Option::is_none")]
        clear_at_least: Option<InputTokensClearAtLeast>,
        #[serde(skip_serializing_if = "Option::is_none")]
        clear_tool_inputs: Option<ClearToolInputs>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exclude_tools: Option<Vec<String>>,
    },
    #[serde(rename = "clear_thinking_20251015")]
    ClearThinking {
        #[serde(skip_serializing_if = "Option::is_none")]
        keep: Option<ThinkingKeep>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContextTrigger {
    #[serde(rename = "input_tokens")]
    InputTokens { value: u32 },
    #[serde(rename = "tool_uses")]
    ToolUses { value: u32 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolUsesKeep {
    #[serde(rename = "type")]
    pub keep_type: String, // Always "tool_uses"
    pub value: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputTokensClearAtLeast {
    #[serde(rename = "type")]
    pub clear_type: String, // Always "input_tokens"
    pub value: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClearToolInputs {
    All(bool),
    Specific(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ThinkingKeep {
    All(ThinkingKeepAll),
    Turns(ThinkingTurns),
    Literal(String), // "all"
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThinkingKeepAll {
    #[serde(rename = "type")]
    pub keep_type: String, // Always "all"
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThinkingTurns {
    #[serde(rename = "type")]
    pub keep_type: String, // Always "thinking_turns"
    pub value: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpServerDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub server_type: String, // Always "url"
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_configuration: Option<McpToolConfiguration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpToolConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<EffortLevel>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputFormat {
    #[serde(rename = "type")]
    pub format_type: String, // Always "json_schema"
    pub schema: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageParam {
    pub content: ContentParam,
    pub role: MessageRole,
}

impl MessageParam {
    pub fn user(content: Vec<ContentBlock>) -> Self {
        Self {
            content: ContentParam::Blocks(content),
            role: MessageRole::User,
        }
    }
    pub fn assistant(content: Vec<ContentBlock>) -> Self {
        Self {
            content: ContentParam::Blocks(content),
            role: MessageRole::Assistant,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ContentParam {
    String(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    pub fn from_opt(r: &str) -> Option<Self> {
        match r {
            "user" => Some(MessageRole::User),
            "assistant" => Some(MessageRole::Assistant),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text(TextBlockParam),
    #[serde(rename = "image")]
    Image(ImageBlockParam),
    #[serde(rename = "tool_use")]
    ToolUse(ToolUseBlockParam),
    #[serde(rename = "tool_result")]
    ToolResult(ToolResultBlockParam),
    #[serde(rename = "thinking")]
    Thinking(ThinkingBlockParam),
    #[serde(rename = "redacted_thinking")]
    RedactedThinking(RedactedThinkingBlockParam),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ThinkingBlockParam {
    pub thinking: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RedactedThinkingBlockParam {
    pub data: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResponseContentBlock {
    #[serde(rename = "text")]
    Text(ResponseTextBlock),
    #[serde(rename = "tool_use")]
    ToolUse(ResponseToolUseBlock),
    #[serde(rename = "thinking")]
    Thinking(ThinkingBlock),
    #[serde(rename = "redacted_thinking")]
    RedactedThinking(RedactedThinkingBlock),
    #[serde(rename = "server_tool_use")]
    ServerToolUse(ServerToolUseBlock),
}

/// Text block for responses - unlike `TextBlockParam`, this doesn't include the `type` field
/// since it's consumed by the `#[serde(tag = "type")]` attribute on `ResponseContentBlock`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResponseTextBlock {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
}

/// Tool use block for responses - unlike `ToolUseBlockParam`, this doesn't include the `type` field
/// since it's consumed by the `#[serde(tag = "type")]` attribute on `ResponseContentBlock`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResponseToolUseBlock {
    pub id: String,
    pub input: serde_json::Value,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<ToolCaller>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ThinkingBlock {
    pub thinking: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RedactedThinkingBlock {
    pub data: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerToolUseBlock {
    pub id: String,
    pub name: ServerToolName,
    pub input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<ToolCaller>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ServerToolName {
    WebSearch,
    WebFetch,
    CodeExecution,
    BashCodeExecution,
    TextEditorCodeExecution,
    ToolSearchToolRegex,
    ToolSearchToolBm25,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TextBlockParam {
    pub text: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "text"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
}

impl TextBlockParam {
    pub fn new(text: String) -> Self {
        Self {
            text,
            block_type: "text".to_string(),
            cache_control: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CacheControlEphemeral {
    #[serde(rename = "type")]
    pub control_type: String, // Always "ephemeral"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<CacheTtl>,
}

impl CacheControlEphemeral {
    pub fn ephemeral() -> Self {
        Self {
            control_type: "ephemeral".to_string(),
            ttl: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MediaType {
    #[serde(rename = "image/jpeg")]
    Jpeg,

    #[serde(rename = "image/png")]
    Png,

    #[serde(rename = "image/gif")]
    Gif,

    #[serde(rename = "image/webp")]
    Webp,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ImageBlockParam {
    pub source: ImageSource,
    #[serde(rename = "type")]
    pub block_type: String, // Always "image"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ImageSource {
    #[serde(rename = "base64")]
    Base64 { data: String, media_type: MediaType },
    #[serde(rename = "url")]
    Url { url: String },
    #[serde(rename = "file")]
    File { file_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolUseBlockParam {
    pub id: String,
    pub input: serde_json::Value, // Using serde_json::Value for generic object
    pub name: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_use"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<ToolCaller>,
}

impl ToolUseBlockParam {
    pub fn new(id: String, input: serde_json::Value, name: String) -> Self {
        Self {
            id,
            input,
            name,
            block_type: "tool_use".to_string(),
            cache_control: None,
            caller: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ToolCaller {
    #[serde(rename = "direct")]
    Direct,
    #[serde(rename = "code_execution_20250825")]
    ServerToolCaller { tool_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolResultBlockParam {
    pub tool_use_id: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_result"
    pub content: ContentParam,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

impl ToolResultBlockParam {
    pub fn new(tool_use_id: String, content: ContentParam) -> Self {
        Self {
            tool_use_id,
            block_type: "tool_result".to_string(),
            content,
            is_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolParam {
    #[serde(rename = "input_schema")]
    pub json_schema: serde_json::Value,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoiceType {
    Auto,
    Any,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolChoiceParam {
    #[serde(rename = "type")]
    pub choice_type: ToolChoiceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub disable_parallel_tool_use: bool,
}

impl ToolChoiceParam {
    pub fn auto(disable_parallel_tool_use: bool) -> Self {
        Self {
            choice_type: ToolChoiceType::Auto,
            name: None,
            disable_parallel_tool_use,
        }
    }

    pub fn any(disable_parallel_tool_use: bool) -> Self {
        Self {
            choice_type: ToolChoiceType::Any,
            name: None,
            disable_parallel_tool_use,
        }
    }

    pub fn tool(name: String, disable_parallel_tool_use: bool) -> Self {
        Self {
            choice_type: ToolChoiceType::Tool,
            name: Some(name),
            disable_parallel_tool_use,
        }
    }
}

impl From<&ChatCompletionTool> for ToolParam {
    fn from(val: &ChatCompletionTool) -> Self {
        ToolParam {
            name: val.function.name.clone(),
            description: val.function.description.clone(),
            json_schema: val.function.parameters.clone().unwrap_or(json!(
                {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "properties": {},
                    "required": [],
                    "title": "",
                    "type": "object"
                }
            )),
        }
    }
}

/// Converts a `ChatCompletionTools` enum to a `ToolParam`, returning `None` for custom tools.
pub fn tool_from_completion_tools(val: &ChatCompletionTools) -> Option<ToolParam> {
    match val {
        ChatCompletionTools::Function(tool) => Some(ToolParam::from(tool)),
        ChatCompletionTools::Custom(_) => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataParam {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

// Combined pattern that matches all three formats:
// 1. Anthropic API: claude-3-5-sonnet-20241022, claude-3-5-sonnet-latest or claude-opus-4-1
// 2. AWS Bedrock: anthropic.claude-3-5-sonnet-20241022-v2:0
// 3. GCP Vertex AI: claude-3-5-sonnet-v2@20241022
// Based on available models from https://docs.claude.com/en/docs/about-claude/models/overview, as of 2025-09-28.
pub(crate) static ANTHROPIC_REGEX: &str = r"(?x) # Enable verbose mode
    (?:anthropic\.)?                              # Optional 'anthropic.' prefix for AWS
    claude-                                       # Required 'claude-' prefix
    (?:instant-)?                                 # Optional 'instant-' for legacy
    (?:\d+(?:[-.]\d+)*-)?                         # Optional leading version segment (e.g. 3-, 3-5-, 3.5-)
    (?:opus|sonnet|haiku)?                        # Optional model type
    (?:-\d+(?:[-.]\d+)*)?                         # Optional trailing version segment (e.g. -4, -4-1)
    (?:
        -(?:latest|\d{8})                         # Anthropic format: -latest or -YYYYMMDD
        |
        -\d{8}-v\d+:\d+                           # AWS format: -YYYYMMDD-v2:0
        |
        -v\d+@\d{8}                               # GCP format: -v2@YYYYMMDD
        |
        @\d{8}                                    # Alternative GCP format: @YYYYMMDD
    )?";
pub type AnthropicModelVariant = String;

pub(crate) fn validate_model_variant(model: &str) -> Result<AnthropicModelVariant, OpenAIError> {
    Regex::new(ANTHROPIC_REGEX)
        .map_err(|e| OpenAIError::InvalidArgument(format!("Regex error: {e}")))?
        .find(model)
        .ok_or(OpenAIError::InvalidArgument(format!(
            "Invalid model variant: {model}"
        )))?;
    Ok(model.to_string())
}

/// Output-token ceiling of Claude 3 and the Claude 1/2/instant generations before it.
const LEGACY_MAX_TOKENS: u32 = 4096;

/// Output tokens every model from Claude 3.5 onward accepts. Deliberately a floor rather than each
/// model's own maximum, which is far higher and differs per model: Anthropic requires a request
/// above roughly 21000 output tokens to stream, so a per-model maximum here would make every
/// non-streaming request fail. Tracking those maxima would also put this constant back in the
/// business of going stale, which is the defect that brought us here.
const MODERN_MAX_TOKENS: u32 = 8192;

/// Model families capped at [`LEGACY_MAX_TOKENS`], as the fragment of the model id that identifies
/// each. Every id Anthropic has published for these carries one of these fragments in all three
/// formats `ANTHROPIC_REGEX` accepts — the AWS and GCP forms only add a prefix or a suffix
/// (`anthropic.claude-3-haiku-20240307-v1:0`, `claude-3-haiku@20240307`).
///
/// `claude-3-5-*` and `claude-3-7-*` are absent by construction, and none of these fragments is a
/// substring of a later id: `claude-3-haiku` does not occur in `claude-3-5-haiku`.
const LEGACY_MODEL_FRAGMENTS: &[&str] = &[
    "claude-instant",
    "claude-1",
    "claude-v1",
    "claude-2",
    "claude-v2",
    "claude-3-opus",
    "claude-3-sonnet",
    "claude-3-haiku",
];

/// Max tokens to request when the caller sets no limit of its own, limited by the model variant.
/// Based on: `<https://docs.anthropic.com/en/docs/about-claude/models#model-comparison-table>`
///
/// Keyed on the legacy families rather than on an allowlist of current ones so that a model
/// Anthropic releases after this code was written gets the higher budget. An allowlist silently
/// truncates every response from a new model at [`LEGACY_MAX_TOKENS`] instead.
pub fn default_max_tokens(model: &AnthropicModelVariant) -> u32 {
    if LEGACY_MODEL_FRAGMENTS
        .iter()
        .any(|fragment| model.as_str().contains(fragment))
    {
        LEGACY_MAX_TOKENS
    } else {
        MODERN_MAX_TOKENS
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageCreateResponse {
    pub id: String,
    pub content: Vec<ResponseContentBlock>,
    pub model: AnthropicModelVariant,
    pub role: MessageRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<StopReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub usage: Usage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    StopSequence,
    ToolUse,
    PauseTurn,
    Refusal,
    ModelContextWindowExceeded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageType {
    #[serde(rename = "message")]
    Message,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation: Option<CacheCreation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_tool_use: Option<ServerToolUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<ServiceTier>,
}

impl From<Usage> for CompletionUsage {
    fn from(usage: Usage) -> Self {
        let cache_creation_input_tokens = usage.cache_creation_input_tokens.unwrap_or_default();
        let cache_read_input_tokens = usage.cache_read_input_tokens.unwrap_or_default();
        let prompt_tokens = usage
            .input_tokens
            .saturating_add(cache_creation_input_tokens)
            .saturating_add(cache_read_input_tokens);
        let prompt_tokens_details = (cache_read_input_tokens > 0).then_some(PromptTokensDetails {
            cached_tokens: Some(cache_read_input_tokens),
            audio_tokens: None,
        });

        CompletionUsage {
            prompt_tokens,
            completion_tokens: usage.output_tokens,
            total_tokens: prompt_tokens.saturating_add(usage.output_tokens),
            prompt_tokens_details,
            completion_tokens_details: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CacheCreation {
    #[serde(default)]
    pub ephemeral_5m_input_tokens: u32,
    #[serde(default)]
    pub ephemeral_1h_input_tokens: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ServerToolUsage {
    #[serde(default)]
    pub web_search_requests: u32,
    #[serde(default)]
    pub web_fetch_requests: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceTier {
    Standard,
    Priority,
    Batch,
}

#[cfg(test)]
mod tests {
    use super::{
        LEGACY_MAX_TOKENS, MODERN_MAX_TOKENS, Usage, default_max_tokens, validate_model_variant,
    };
    use crate::anthropic::DEFAULT_ANTHROPIC_MODEL;
    use async_openai::types::chat::CompletionUsage;

    #[test]
    fn usage_conversion_saturates_token_totals() {
        let usage = CompletionUsage::from(Usage {
            input_tokens: u32::MAX,
            output_tokens: 1,
            cache_creation_input_tokens: Some(1),
            cache_read_input_tokens: Some(2),
            ..Usage::default()
        });

        assert_eq!(usage.prompt_tokens, u32::MAX);
        assert_eq!(usage.completion_tokens, 1);
        assert_eq!(usage.total_tokens, u32::MAX);
        assert_eq!(
            usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| details.cached_tokens),
            Some(2)
        );
    }

    // Current Anthropic model names to validate.
    // Based on the models list from https://docs.claude.com/en/docs/about-claude/models/overview, as of 2025-09-28.
    const VALID_MODELS: &[&str] = &[
        "claude-opus-4-1",
        "claude-opus-4-1-latest",
        "claude-opus-4-1-20250805",
        "claude-opus-4-20250514",
        "claude-opus-4-0",
        "claude-sonnet-4-0",
        "claude-3-7-sonnet-latest",
        "claude-haiku-4-5",
        "claude-sonnet-4-20250514",
        "claude-3-7-sonnet-20250219",
        "claude-3-5-haiku-20241022",
        "anthropic.claude-opus-4-1-20250805-v1:0",
        "anthropic.claude-opus-4-20250514-v1:0",
        "anthropic.claude-sonnet-4-20250514-v1:0",
        "anthropic.claude-3-7-sonnet-20250219-v1:0",
        "anthropic.claude-3-5-haiku-20241022-v1:0",
        "anthropic.claude-3-haiku-20240307-v1:0",
        "claude-opus-4-1@20250805",
        "claude-opus-4@20250514",
        "claude-sonnet-4@20250514",
        "claude-3-7-sonnet@20250219",
        "claude-3-5-haiku@20241022",
        "claude-3-haiku@20240307",
    ];

    #[test]
    fn validates_known_models() {
        for m in VALID_MODELS {
            let res = validate_model_variant(m);
            assert!(res.is_ok(), "model {m} should be valid: {:?}", res.err());
        }
    }

    const INVALID_MODELS: &[&str] = &["anthropic.claude", "gpt-4o"];

    #[test]
    fn invalid_models_rejected() {
        for m in INVALID_MODELS {
            let res = validate_model_variant(m);
            assert!(res.is_err(), "model {m} should be invalid");
        }
    }

    /// Every id Anthropic has published for a family that caps generation at
    /// [`LEGACY_MAX_TOKENS`], across the three formats `ANTHROPIC_REGEX` accepts. Requesting more
    /// output tokens than the model allows is rejected outright, so this direction must not
    /// regress.
    const LEGACY_MODELS: &[&str] = &[
        "claude-instant-1.2",
        "claude-2.0",
        "claude-2.1",
        "claude-3-opus-latest",
        "claude-3-opus-20240229",
        "claude-3-sonnet-20240229",
        "claude-3-haiku-20240307",
        "anthropic.claude-instant-v1",
        "anthropic.claude-v2",
        "anthropic.claude-v2:1",
        "anthropic.claude-3-opus-20240229-v1:0",
        "anthropic.claude-3-sonnet-20240229-v1:0",
        "anthropic.claude-3-haiku-20240307-v1:0",
        "claude-3-opus@20240229",
        "claude-3-haiku@20240307",
    ];

    #[test]
    fn legacy_models_keep_their_lower_ceiling() {
        for model in LEGACY_MODELS {
            assert_eq!(
                default_max_tokens(&(*model).to_string()),
                LEGACY_MAX_TOKENS,
                "{model} caps generation at {LEGACY_MAX_TOKENS} output tokens, so asking for more \
                 would be rejected"
            );
        }
    }

    #[test]
    fn models_from_claude_35_onward_get_the_larger_budget() {
        // `VALID_MODELS` is the list this file already keeps of ids the runtime accepts; the
        // Claude 3 entries in it are the only ones that belong to a legacy family.
        let modern = VALID_MODELS
            .iter()
            .filter(|model| !LEGACY_MODELS.contains(model))
            .chain(
                [
                    "claude-3-5-sonnet-latest",
                    "claude-sonnet-5",
                    "claude-opus-5",
                ]
                .iter(),
            );

        for model in modern {
            assert_eq!(
                default_max_tokens(&(*model).to_string()),
                MODERN_MAX_TOKENS,
                "{model} accepts at least {MODERN_MAX_TOKENS} output tokens, so a response must \
                 not be truncated below that"
            );
        }
    }

    /// Regression test for #13557. The ceiling used to be keyed on the single id
    /// `claude-3-5-sonnet`, which was also the default model — so moving the default off a retired
    /// id silently halved the output budget of every request that names no model.
    #[test]
    fn the_default_model_is_not_truncated_at_the_legacy_ceiling() {
        assert_eq!(
            default_max_tokens(&DEFAULT_ANTHROPIC_MODEL.to_string()),
            MODERN_MAX_TOKENS
        );
    }
}
