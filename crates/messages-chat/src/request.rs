/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A Messages API request as a chat completion request.
//!
//! What the chat completion can carry is carried over. What only changes how the request is
//! served, not what the model is asked, is accepted and left out: prompt caching
//! (`cache_control`), `service_tier`, `speed`, `context_management`, `fallbacks` and
//! `diagnostics`. Anything else the chat completion cannot carry is refused by name, so the
//! client never gets an answer to a different question than the one it asked.

use async_openai::types::chat::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls, ChatCompletionNamedToolChoice,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartFile,
    ChatCompletionRequestMessageContentPartImage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionStreamOptions, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionTools, CreateChatCompletionRequest, FileObject, FunctionCall, FunctionName,
    FunctionObject, ImageUrl, ReasoningEffort, ResponseFormat, ResponseFormatJsonSchema,
    StopConfiguration, ToolChoiceOptions,
};
use messages_api::MessagesError;
use messages_api::types::{
    ContentBlockParam, CreateMessageRequest, CustomToolParam, DocumentBlockParam, DocumentSource,
    ImageSource, MessageContent, MessageParam, OutputFormat, Role, SystemPrompt, ToolChoice,
    ToolParam, ToolResultBlockParam, ToolResultContent,
};

/// Where to read about serving the Messages API with a model.
pub(crate) const DOCS: &str = "https://spiceai.org/docs/components/models";

/// Top-level fields that change how a request is served, not what the model is asked, which a
/// chat completion has no equivalent for.
const SERVING_ONLY_FIELDS: [&str; 6] = [
    "cache_control",
    "service_tier",
    "speed",
    "context_management",
    "fallbacks",
    "diagnostics",
];

/// The name `response_format` gives the schema of `output_config.format`.
const OUTPUT_SCHEMA_NAME: &str = "output";

/// The refusal for something the request asks that a chat completion cannot carry.
fn unsupported(model: &str, what: &str, remedy: &str) -> MessagesError {
    MessagesError::invalid_request(format!(
        "Model '{model}' serves the Messages API by translating requests into chat completions, which cannot carry {what}. {remedy} See: {DOCS}"
    ))
}

/// `request` as a chat completion request, or the reason it cannot be one.
///
/// # Errors
///
/// Returns an `invalid_request_error` naming the first part of `request` a chat completion
/// cannot carry.
#[expect(deprecated)] // `user` is how `metadata.user_id` reaches most OpenAI-compatible servers.
pub fn chat_request(
    request: CreateMessageRequest,
) -> Result<CreateChatCompletionRequest, MessagesError> {
    let CreateMessageRequest {
        model,
        max_tokens,
        messages,
        system,
        stream,
        temperature,
        top_p,
        top_k,
        stop_sequences,
        metadata,
        tools,
        tool_choice,
        thinking: _,
        output_config,
        output_format,
        other,
    } = request;

    if let Some(field) = other
        .keys()
        .find(|field| !SERVING_ONLY_FIELDS.contains(&field.as_str()))
    {
        return Err(unsupported(
            &model,
            &format!("`{field}`"),
            &format!("Remove `{field}`, or use a model with `from: anthropic`."),
        ));
    }
    if top_k.is_some() {
        return Err(unsupported(
            &model,
            "`top_k`",
            "Remove `top_k`, or use a model with `from: anthropic`.",
        ));
    }

    let mut chat_messages = Vec::with_capacity(messages.len() + 1);
    if let Some(system) = system {
        chat_messages.push(system_message(&model, system)?);
    }
    for message in messages {
        append_message(&model, message, &mut chat_messages)?;
    }

    let tools = tools
        .map(|tools| {
            tools
                .into_iter()
                .map(|tool| chat_tool(&model, tool))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let offers_tools = tools.as_ref().is_some_and(|tools| !tools.is_empty());
    let parallel_tool_calls = tool_choice
        .as_ref()
        .filter(|choice| offers_tools && choice.disables_parallel_tool_use())
        .map(|_| false);

    let (reasoning_effort, format) = match output_config {
        Some(config) => (
            config
                .effort
                .map(|effort| reasoning_effort(&model, &effort))
                .transpose()?,
            config.format,
        ),
        None => (None, None),
    };
    let response_format = format
        .or(output_format)
        .map(|format| response_format(&model, format))
        .transpose()?;

    let streaming = stream.unwrap_or(false);
    Ok(CreateChatCompletionRequest {
        messages: chat_messages,
        model,
        max_completion_tokens: Some(max_tokens),
        temperature,
        top_p,
        stop: stop_sequences
            .filter(|sequences| !sequences.is_empty())
            .map(StopConfiguration::StringArray),
        user: metadata.and_then(|metadata| metadata.user_id),
        tools,
        tool_choice: tool_choice.map(chat_tool_choice),
        parallel_tool_calls,
        reasoning_effort,
        response_format,
        stream: streaming.then_some(true),
        stream_options: streaming.then_some(ChatCompletionStreamOptions {
            include_usage: Some(true),
            include_obfuscation: None,
        }),
        ..Default::default()
    })
}

fn system_message(
    model: &str,
    system: SystemPrompt,
) -> Result<ChatCompletionRequestMessage, MessagesError> {
    let content = match system {
        SystemPrompt::Text(text) => ChatCompletionRequestSystemMessageContent::Text(text),
        SystemPrompt::Blocks(blocks) => system_content(model, blocks)?,
    };
    Ok(ChatCompletionRequestMessage::System(
        ChatCompletionRequestSystemMessage {
            content,
            name: None,
        },
    ))
}

/// The text blocks of a system prompt or a system message, one part each.
fn system_content(
    model: &str,
    blocks: Vec<ContentBlockParam>,
) -> Result<ChatCompletionRequestSystemMessageContent, MessagesError> {
    let mut parts = blocks
        .into_iter()
        .map(|block| match block {
            ContentBlockParam::Text(text) => Ok(text.text),
            other => Err(unsupported(
                model,
                &format!("a `{}` block in a system prompt", other.block_type()),
                "Send the system prompt as text.",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(if parts.len() == 1 {
        ChatCompletionRequestSystemMessageContent::Text(parts.remove(0))
    } else {
        ChatCompletionRequestSystemMessageContent::Array(
            parts
                .into_iter()
                .map(|text| {
                    ChatCompletionRequestSystemMessageContentPart::Text(
                        ChatCompletionRequestMessageContentPartText { text },
                    )
                })
                .collect(),
        )
    })
}

/// Appends `message` to `chat_messages` as the chat messages that carry it.
fn append_message(
    model: &str,
    message: MessageParam,
    chat_messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), MessagesError> {
    match (message.role, message.content) {
        (Role::System, MessageContent::Text(text)) => {
            chat_messages.push(ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Text(text),
                    name: None,
                },
            ));
        }
        (Role::System, MessageContent::Blocks(blocks)) => {
            // A system message that only changes `output_config` carries no content.
            if !blocks.is_empty() {
                chat_messages.push(ChatCompletionRequestMessage::System(
                    ChatCompletionRequestSystemMessage {
                        content: system_content(model, blocks)?,
                        name: None,
                    },
                ));
            }
        }
        (Role::User, MessageContent::Text(text)) => {
            chat_messages.push(user_message(ChatCompletionRequestUserMessageContent::Text(
                text,
            )));
        }
        (Role::User, MessageContent::Blocks(blocks)) => {
            append_user_blocks(model, blocks, chat_messages)?;
        }
        (Role::Assistant, MessageContent::Text(text)) => {
            chat_messages.push(assistant_message(Some(text), Vec::new()));
        }
        (Role::Assistant, MessageContent::Blocks(blocks)) => {
            if let Some(message) = assistant_blocks(model, blocks)? {
                chat_messages.push(message);
            }
        }
    }
    Ok(())
}

fn user_message(content: ChatCompletionRequestUserMessageContent) -> ChatCompletionRequestMessage {
    ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
        content,
        name: None,
    })
}

#[expect(deprecated)] // `function_call` has no non-deprecated way to be left unset.
fn assistant_message(
    text: Option<String>,
    tool_calls: Vec<ChatCompletionMessageToolCalls>,
) -> ChatCompletionRequestMessage {
    ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
        content: text.map(ChatCompletionRequestAssistantMessageContent::Text),
        refusal: None,
        name: None,
        audio: None,
        tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
        function_call: None,
    })
}

/// A user message's blocks as chat messages: a tool message for each tool result, first, as the
/// chat completion pairs them with the assistant's tool calls; then a user message with the rest,
/// images returned by the tool results first, since a tool message carries only text.
fn append_user_blocks(
    model: &str,
    blocks: Vec<ContentBlockParam>,
    chat_messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), MessagesError> {
    let mut tool_images = Vec::new();
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            ContentBlockParam::ToolResult(result) => {
                chat_messages.push(tool_message(model, result, &mut tool_images)?);
            }
            block => append_user_part(model, block, &mut parts)?,
        }
    }
    tool_images.append(&mut parts);
    if !tool_images.is_empty() {
        chat_messages.push(user_message(user_content(tool_images)));
    }
    Ok(())
}

/// One text part as text, so that every server reads it; anything else as parts.
fn user_content(
    mut parts: Vec<ChatCompletionRequestUserMessageContentPart>,
) -> ChatCompletionRequestUserMessageContent {
    if let [ChatCompletionRequestUserMessageContentPart::Text(_)] = parts.as_slice()
        && let ChatCompletionRequestUserMessageContentPart::Text(text) = parts.remove(0)
    {
        return ChatCompletionRequestUserMessageContent::Text(text.text);
    }
    ChatCompletionRequestUserMessageContent::Array(parts)
}

fn text_part(text: String) -> ChatCompletionRequestUserMessageContentPart {
    ChatCompletionRequestUserMessageContentPart::Text(ChatCompletionRequestMessageContentPartText {
        text,
    })
}

fn append_user_part(
    model: &str,
    block: ContentBlockParam,
    parts: &mut Vec<ChatCompletionRequestUserMessageContentPart>,
) -> Result<(), MessagesError> {
    match block {
        ContentBlockParam::Text(text) => parts.push(text_part(text.text)),
        ContentBlockParam::Image(image) => parts.push(image_part(model, image.source)?),
        ContentBlockParam::Document(document) => append_document(model, document, parts)?,
        other => {
            return Err(unsupported(
                model,
                &format!("a `{}` block in a user message", other.block_type()),
                "Remove the block, or use a model with `from: anthropic`.",
            ));
        }
    }
    Ok(())
}

fn image_part(
    model: &str,
    source: ImageSource,
) -> Result<ChatCompletionRequestUserMessageContentPart, MessagesError> {
    let url = match source {
        ImageSource::Base64 { media_type, data } => format!("data:{media_type};base64,{data}"),
        ImageSource::Url { url } => url,
        ImageSource::File { .. } => {
            return Err(unsupported(
                model,
                "an image from the Anthropic Files API",
                "Send the image as `base64` or `url` instead.",
            ));
        }
    };
    Ok(ChatCompletionRequestUserMessageContentPart::ImageUrl(
        ChatCompletionRequestMessageContentPartImage {
            image_url: ImageUrl { url, detail: None },
        },
    ))
}

/// A document as the parts that carry it: a PDF as a file, text as text after its title and
/// context, and content blocks as their own parts.
fn append_document(
    model: &str,
    document: DocumentBlockParam,
    parts: &mut Vec<ChatCompletionRequestUserMessageContentPart>,
) -> Result<(), MessagesError> {
    if document.citations.is_some_and(|citations| citations.enabled) {
        return Err(unsupported(
            model,
            "citations",
            "Remove `citations` from the document, or use a model with `from: anthropic`.",
        ));
    }
    match document.source {
        DocumentSource::Base64 { media_type, data } => {
            parts.push(ChatCompletionRequestUserMessageContentPart::File(
                ChatCompletionRequestMessageContentPartFile {
                    file: FileObject {
                        file_data: Some(format!("data:{media_type};base64,{data}")),
                        file_id: None,
                        filename: Some(document.title.unwrap_or_else(|| "document.pdf".into())),
                    },
                },
            ));
        }
        DocumentSource::Text { data, .. } => {
            let heading: Vec<String> = [document.title, document.context]
                .into_iter()
                .flatten()
                .collect();
            parts.push(text_part(if heading.is_empty() {
                data
            } else {
                format!("{}\n\n{data}", heading.join("\n"))
            }));
        }
        DocumentSource::Content { content } => match content {
            MessageContent::Text(text) => parts.push(text_part(text)),
            MessageContent::Blocks(blocks) => {
                for block in blocks {
                    append_user_part(model, block, parts)?;
                }
            }
        },
        DocumentSource::Url { .. } | DocumentSource::File { .. } => {
            return Err(unsupported(
                model,
                "a document by URL or from the Anthropic Files API",
                "Send the document as `base64` or `text` instead.",
            ));
        }
    }
    Ok(())
}

/// A tool result as a tool message. A tool message carries only text, so images the result
/// returned are appended to `images`, for the user message that follows.
fn tool_message(
    model: &str,
    result: ToolResultBlockParam,
    images: &mut Vec<ChatCompletionRequestUserMessageContentPart>,
) -> Result<ChatCompletionRequestMessage, MessagesError> {
    let text = match result.content {
        None => String::new(),
        Some(ToolResultContent::Text(text)) => text,
        Some(ToolResultContent::Blocks(blocks)) => {
            let mut texts = Vec::new();
            for block in blocks {
                match block {
                    ContentBlockParam::Text(text) => texts.push(text.text),
                    ContentBlockParam::Image(image) => images.push(image_part(model, image.source)?),
                    ContentBlockParam::Document(document) => {
                        let mut parts = Vec::new();
                        append_document(model, document, &mut parts)?;
                        for part in parts {
                            match part {
                                ChatCompletionRequestUserMessageContentPart::Text(text) => {
                                    texts.push(text.text);
                                }
                                part => images.push(part),
                            }
                        }
                    }
                    other => {
                        return Err(unsupported(
                            model,
                            &format!("a `{}` block in a tool result", other.block_type()),
                            "Return the tool result as text or images.",
                        ));
                    }
                }
            }
            texts.join("\n")
        }
    };
    // A tool message has no field that marks a failed call, so its text does.
    let text = if result.is_error == Some(true) {
        format!("Error: {text}")
    } else {
        text
    };
    Ok(ChatCompletionRequestMessage::Tool(
        ChatCompletionRequestToolMessage {
            content: ChatCompletionRequestToolMessageContent::Text(text),
            tool_call_id: result.tool_use_id,
        },
    ))
}

/// An assistant message's blocks as one assistant message: its text, and its tool calls.
/// Thinking is left out: it is the provider's own record, which another model cannot read.
/// `None` when nothing is left.
fn assistant_blocks(
    model: &str,
    blocks: Vec<ContentBlockParam>,
) -> Result<Option<ChatCompletionRequestMessage>, MessagesError> {
    let mut text: Option<String> = None;
    let mut tool_calls = Vec::new();
    for block in blocks {
        match block {
            ContentBlockParam::Text(block) => text.get_or_insert_default().push_str(&block.text),
            ContentBlockParam::ToolUse(tool_use) => {
                tool_calls.push(ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id: tool_use.id,
                        function: FunctionCall {
                            name: tool_use.name,
                            arguments: tool_use.input.to_string(),
                        },
                    },
                ));
            }
            ContentBlockParam::Thinking(_) | ContentBlockParam::RedactedThinking(_) => {}
            other => {
                return Err(unsupported(
                    model,
                    &format!("a `{}` block in an assistant message", other.block_type()),
                    "Remove the block, or use a model with `from: anthropic`.",
                ));
            }
        }
    }
    if text.is_none() && tool_calls.is_empty() {
        return Ok(None);
    }
    Ok(Some(assistant_message(text, tool_calls)))
}

fn chat_tool(model: &str, tool: ToolParam) -> Result<ChatCompletionTools, MessagesError> {
    match tool {
        ToolParam::Custom(CustomToolParam {
            name,
            description,
            input_schema,
            strict,
        }) => Ok(ChatCompletionTools::Function(ChatCompletionTool {
            function: FunctionObject {
                name,
                description,
                parameters: Some(input_schema),
                strict,
            },
        })),
        ToolParam::Other { tool_type, .. } => Err(unsupported(
            model,
            &format!("the Anthropic-defined tool `{tool_type}`"),
            "Define the tool with an `input_schema` instead, or use a model with `from: anthropic`.",
        )),
    }
}

fn chat_tool_choice(choice: ToolChoice) -> ChatCompletionToolChoiceOption {
    match choice {
        ToolChoice::Auto { .. } => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto),
        ToolChoice::Any { .. } => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required),
        ToolChoice::Tool { name, .. } => {
            ChatCompletionToolChoiceOption::Function(ChatCompletionNamedToolChoice {
                function: FunctionName { name },
            })
        }
        ToolChoice::None => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None),
    }
}

/// `output_config.effort` as a reasoning effort. `max`, which a chat completion has no level
/// for, is its highest level, `xhigh`.
fn reasoning_effort(model: &str, effort: &str) -> Result<ReasoningEffort, MessagesError> {
    match effort {
        "low" => Ok(ReasoningEffort::Low),
        "medium" => Ok(ReasoningEffort::Medium),
        "high" => Ok(ReasoningEffort::High),
        "xhigh" | "max" => Ok(ReasoningEffort::Xhigh),
        other => Err(unsupported(
            model,
            &format!("the effort `{other}`"),
            "Use one of `low`, `medium`, `high`, `xhigh` or `max`.",
        )),
    }
}

fn response_format(model: &str, format: OutputFormat) -> Result<ResponseFormat, MessagesError> {
    match (format.format_type.as_str(), format.schema) {
        ("json_schema", Some(schema)) => Ok(ResponseFormat::JsonSchema {
            json_schema: ResponseFormatJsonSchema {
                description: None,
                name: OUTPUT_SCHEMA_NAME.to_string(),
                schema,
                strict: Some(true),
            },
        }),
        (format_type, _) => Err(unsupported(
            model,
            &format!("the output format `{format_type}` without a `schema`"),
            "Use `{\"type\": \"json_schema\", \"schema\": ...}`.",
        )),
    }
}
