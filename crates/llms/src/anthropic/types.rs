/*
Copyright 2024 The Spice.ai OSS Authors

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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use std::{fmt::Display, str::FromStr};

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
                    "title": "RandomSampleParams",
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

#[derive(Clone, Debug, PartialEq)]
pub enum AnthropicModelVariant {
    Claude35Sonnet20240620,
    Claude3Opus20240229,
    Claude3Sonnet20240229,
    Claude3Haiku20240307,
    Claude21,
    Claude20,
    ClaudeInstant12,
}

impl std::str::FromStr for AnthropicModelVariant {
    type Err = OpenAIError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "claude-3-5-sonnet-20240620" => Ok(AnthropicModelVariant::Claude35Sonnet20240620),
            "claude-3-opus-20240229" => Ok(AnthropicModelVariant::Claude3Opus20240229),
            "claude-3-sonnet-20240229" => Ok(AnthropicModelVariant::Claude3Sonnet20240229),
            "claude-3-haiku-20240307" => Ok(AnthropicModelVariant::Claude3Haiku20240307),
            "claude-2.1" => Ok(AnthropicModelVariant::Claude21),
            "claude-2.0" => Ok(AnthropicModelVariant::Claude20),
            "claude-instant-1.2" => Ok(AnthropicModelVariant::ClaudeInstant12),
            _ => Err(OpenAIError::InvalidArgument(format!(
                "Unknown model variant: {s}"
            ))),
        }
    }
}

impl Display for AnthropicModelVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let z = match self {
            AnthropicModelVariant::Claude35Sonnet20240620 => "claude-3-5-sonnet-20240620",
            AnthropicModelVariant::Claude3Opus20240229 => "claude-3-opus-20240229",
            AnthropicModelVariant::Claude3Sonnet20240229 => "claude-3-sonnet-20240229",
            AnthropicModelVariant::Claude3Haiku20240307 => "claude-3-haiku-20240307",
            AnthropicModelVariant::Claude21 => "claude-2.1",
            AnthropicModelVariant::Claude20 => "claude-2.0",
            AnthropicModelVariant::ClaudeInstant12 => "claude-instant-1.2",
        };
        write!(f, "{z}")
    }
}

impl Serialize for AnthropicModelVariant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

// Custom Deserialize implementation to use from_str() for deserialization
impl<'de> Deserialize<'de> for AnthropicModelVariant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AnthropicModelVariant::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl AnthropicModelVariant {
    #[must_use]
    pub fn default_max_tokens(&self) -> u32 {
        match self {
            AnthropicModelVariant::Claude35Sonnet20240620
            | AnthropicModelVariant::Claude3Opus20240229 => 8192,
            AnthropicModelVariant::Claude3Sonnet20240229
            | AnthropicModelVariant::Claude3Haiku20240307
            | AnthropicModelVariant::Claude21
            | AnthropicModelVariant::Claude20
            | AnthropicModelVariant::ClaudeInstant12 => 4096,
        }
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
