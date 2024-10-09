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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageCreateParamsBase {
    pub max_tokens: i32,
    pub messages: Vec<MessageParam>,
    pub model: ModelParam,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<SystemParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoiceParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolParam>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i32>,
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
#[serde(untagged)]
pub enum SystemParam {
    String(String),
    Blocks(Vec<TextBlockParam>),
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
pub enum ToolChoiceParam {
    Auto(ToolChoiceAutoParam),
    Any(ToolChoiceAnyParam),
    Tool(ToolChoiceToolParam),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolChoiceToolParam {
    pub name: String,
    #[serde(rename = "type")]
    pub choice_type: String, // Always "tool"
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
#[serde(untagged)]
pub enum ModelParam {
    String(String),
    Variant(ModelVariant),
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
