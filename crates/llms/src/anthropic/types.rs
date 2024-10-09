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

use async_openai::error::OpenAIError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageCreateParams {
    pub max_tokens: u32,
    pub messages: Vec<MessageParam>,
    pub model: ModelVariant,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageParam {
    pub content: ContentParam,
    pub role: MessageRole,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContentParam {
    String(String),
    Blocks(Vec<ContentBlockZ>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlockZ {
    Text(TextBlockParam),
    Image(ImageBlockParam),
    ToolUse(ToolUseBlockParam),
    ToolResult(ToolResultBlockParam),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BetaRawContentBlockStartEvent {
    pub content_block: ContentBlock,
    pub index: i32,
    #[serde(rename = "type")]
    pub event_type: String, // Always "content_block_start"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextBlockParam {
    pub text: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "text"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Source {
    pub data: String, // Base64 encoded string
    pub media_type: MediaType,
    #[serde(rename = "type")]
    pub source_type: SourceType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    ImageJpeg,
    ImagePng,
    ImageGif,
    ImageWebp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Base64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageBlockParam {
    pub source: Source,
    #[serde(rename = "type")]
    pub block_type: String, // Always "image"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolUseBlockParam {
    pub id: String,
    pub input: serde_json::Value, // Using serde_json::Value for generic object
    pub name: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_use"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolResultBlockParam {
    pub tool_use_id: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_result"
    pub content: ContentParam,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    Text(BetaTextBlock),
    ToolUse(BetaToolUseBlock),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BetaTextBlock {
    pub text: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "text"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BetaToolUseBlock {
    pub id: String,
    pub input: serde_json::Value, // Using serde_json::Value for generic object
    pub name: String,
    #[serde(rename = "type")]
    pub block_type: String, // Always "tool_use"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InputSchemaTyped {
    #[serde(rename = "type")]
    pub schema_type: String, // Always "object"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputSchema {
    Typed(InputSchemaTyped),
    Dict(HashMap<String, serde_json::Value>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolParam {
    pub input_schema: InputSchema,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolChoiceType {
    Auto,
    Any,
    Tool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolChoiceParam {
    #[serde(rename = "type")]
    pub choice_type: ToolChoiceType,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_parallel_tool_use: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolChoiceAutoParam {
    #[serde(rename = "type")]
    pub choice_type: String, // Always "auto"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_parallel_tool_use: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolChoiceAnyParam {
    #[serde(rename = "type")]
    pub choice_type: String, // Always "any"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_parallel_tool_use: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataParam {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelVariant {
    Claude35Sonnet20240620,
    Claude3Opus20240229,
    Claude3Sonnet20240229,
    Claude3Haiku20240307,
    Claude21,
    Claude20,
    ClaudeInstant12,
}

impl std::str::FromStr for ModelVariant {
    type Err = OpenAIError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "claude-35-sonnet-2024-06-20" => Ok(ModelVariant::Claude35Sonnet20240620),
            "claude-3-opus-2024-02-29" => Ok(ModelVariant::Claude3Opus20240229),
            "claude-3-sonnet-2024-02-29" => Ok(ModelVariant::Claude3Sonnet20240229),
            "claude-3-haiku-2024-03-07" => Ok(ModelVariant::Claude3Haiku20240307),
            "claude-2-1" => Ok(ModelVariant::Claude21),
            "claude-2-0" => Ok(ModelVariant::Claude20),
            "claude-instant-1-2" => Ok(ModelVariant::ClaudeInstant12),
            _ => Err(OpenAIError::InvalidArgument(format!(
                "Unknown model variant: {}",
                s
            ))),
        }
    }
}

impl ModelVariant {
    pub fn default_max_tokens(&self) -> u32 {
        match self {
            ModelVariant::Claude35Sonnet20240620 => 8192,
            ModelVariant::Claude3Opus20240229 => 8192,
            ModelVariant::Claude3Sonnet20240229 => 4096,
            ModelVariant::Claude3Haiku20240307 => 4096,
            ModelVariant::Claude21 => 4096,
            ModelVariant::Claude20 => 4096,
            ModelVariant::ClaudeInstant12 => 4096,
        }
    }
}
