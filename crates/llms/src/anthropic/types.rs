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

use async_openai::{error::OpenAIError, types::ChatCompletionTool};
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
    Text(TextBlockParam),
    Image(ImageBlockParam),
    ToolUse(ToolUseBlockParam),
    ToolResult(ToolResultBlockParam),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponseContentBlock {
    Text(TextBlockParam),
    ToolUse(ToolUseBlockParam),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BetaRawContentBlockStartEvent {
    pub content_block: ContentBlock,
    pub index: i32,
    #[serde(rename = "type")]
    pub event_type: String, // Always "content_block_start"
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TextBlockParam {
    pub text: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "text"
}
impl TextBlockParam {
    pub fn new(text: String) -> Self {
        Self {
            text,
            block_type: "text".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Source {
    pub data: String, // Base64 encoded string
    pub media_type: MediaType,
    #[serde(rename = "type")]
    pub r#type: SourceType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    #[serde(rename = "image_jpeg")]
    Jpeg,

    #[serde(rename = "image_png")]
    Png,

    #[serde(rename = "image_gif")]
    Gif,

    #[serde(rename = "image_webp")]
    Webp,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Base64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ImageBlockParam {
    pub source: Source,
    #[serde(rename = "type")]
    pub block_type: String, // Always "image"
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolUseBlockParam {
    pub id: String,
    pub input: serde_json::Value, // Using serde_json::Value for generic object
    pub name: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_use"
}

impl ToolUseBlockParam {
    pub fn new(id: String, input: serde_json::Value, name: String) -> Self {
        Self {
            id,
            input,
            name,
            block_type: "tool_use".to_string(),
        }
    }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolChoiceAutoParam {
    #[serde(rename = "type")]
    pub choice_type: String, // Always "auto"
    pub disable_parallel_tool_use: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolChoiceAnyParam {
    #[serde(rename = "type")]
    pub choice_type: String, // Always "any"
    pub disable_parallel_tool_use: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataParam {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

// Combined pattern that matches all three formats:
// 1. Anthropic API: claude-3-5-sonnet-20241022 or claude-3-5-sonnet-latest
// 2. AWS Bedrock: anthropic.claude-3-5-sonnet-20241022-v2:0
// 3. GCP Vertex AI: claude-3-5-sonnet-v2@20241022
pub(crate) static ANTHROPIC_REGEX: &str = r"(?x) # Enable verbose mode
    (?:anthropic\.)?                              # Optional 'anthropic.' prefix for AWS
    claude-                                       # Required 'claude-' prefix
    (?:instant-)?                                 # Optional 'instant-' for legacy
    (?:\d+(?:\.\d+)?)-?                           # Version number (e.g., 3 or 3.5)
    (?:opus|sonnet|haiku)?                        # Optional model type
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
}
