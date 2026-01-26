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
    types::chat::{ChatCompletionTool, ChatCompletionTools},
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

/// Max tokens, limited by the model variant
/// Based on: `<https://docs.anthropic.com/en/docs/about-claude/models#model-comparison-table>`
pub fn default_max_tokens(model: &AnthropicModelVariant) -> u32 {
    if model.as_str().contains("claude-3-5-sonnet") {
        8192
    } else {
        4096
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
    use super::validate_model_variant;

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
        "claude-3-5-haiku-latest",
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
}
