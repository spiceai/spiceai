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
//! (`cache_control`), `service_tier`, `speed`, `context_management`, `fallbacks`,
//! `diagnostics`, `safeguards`, and how hard the model thinks (`thinking`,
//! `output_config.effort`). Anything else the chat completion cannot carry is refused by name, so
//! the client never gets an answer to a different question than the one it asked.
//!
//! `output_config.effort` is not sent as `reasoning_effort`: a model without reasoning refuses
//! that parameter, and Claude Code sets an effort on every request, whatever the model. A
//! reasoning model's effort is its Spicepod `reasoning_effort` param.
//!
//! `safeguards` asks Anthropic's servers to run their own classifiers over the request, which a
//! model served another way does not have. Claude Code sends it on every request in auto mode,
//! where its own classifier still checks each tool call before it runs.
//!
//! A tool with `defer_loading` is offered to the model only once a `tool_reference` block in a
//! tool result names it, as the Messages API loads it. Until then the model sees only the tool's
//! name, wherever the client lists it. A `tool_reference` reads as the tool's definition, one
//! `<function>` line inside a `<functions>` block, which is how Claude Code's tool search tells
//! the model to expect it. A loaded tool joins the chat completion's tools after the tools that
//! were never deferred, in the order the conversation loaded them, so loading one changes only
//! the end of the tool list and a server that caches prompts keeps the rest.

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
    FunctionObject, ImageUrl, ResponseFormat, ResponseFormatJsonSchema, StopConfiguration,
    ToolChoiceOptions,
};
use messages_api::MessagesError;
use messages_api::types::{
    ContentBlockParam, CreateMessageRequest, CustomToolParam, DocumentBlockParam, DocumentSource,
    ImageBlockParam, ImageSource, MessageContent, MessageParam, OutputFormat, Role, SystemPrompt,
    TextBlockParam, ToolChoice, ToolParam, ToolResultBlockParam, ToolResultContent,
};
use serde_json::{Map, Value};

/// Where to read about serving the Messages API with a model.
pub(crate) const DOCS: &str = "https://spiceai.org/docs/components/models";

/// Top-level fields that change how a request is served, not what the model is asked, which a
/// chat completion has no equivalent for.
const SERVING_ONLY_FIELDS: [&str; 7] = [
    "cache_control",
    "service_tier",
    "speed",
    "context_management",
    "fallbacks",
    "diagnostics",
    "safeguards",
];

/// The name `response_format` gives the schema of `output_config.format`.
const OUTPUT_SCHEMA_NAME: &str = "output";

/// The refusal for something the request asks that a chat completion cannot carry.
fn unsupported(model: &str, what: &str, remedy: &str) -> MessagesError {
    MessagesError::invalid_request(format!(
        "Model '{model}' serves the Messages API by translating requests into chat completions, which cannot carry {what}. {remedy} See: {DOCS}"
    ))
}

/// Refuses the first field of `other` that is not in `accepted`, naming it as a field `place`.
fn refuse_other(
    model: &str,
    other: &Map<String, Value>,
    accepted: &[&str],
    place: &str,
) -> Result<(), MessagesError> {
    match other
        .keys()
        .find(|field| !accepted.contains(&field.as_str()))
    {
        Some(field) => Err(unsupported(
            model,
            &format!("`{field}` {place}"),
            &format!("Remove `{field}`, or use a model with `from: anthropic`."),
        )),
        None => Ok(()),
    }
}

/// The fields a content block may carry beside its own: its `type`, and `cache_control`, which
/// only says how to cache the prompt.
const BLOCK_FIELDS: [&str; 2] = ["type", "cache_control"];

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
        thinking,
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
    if let Some(metadata) = &metadata {
        refuse_other(&model, &metadata.other, &[], "in `metadata`")?;
    }
    if let Some(thinking) = &thinking {
        refuse_other(&model, &thinking.other, &[], "in `thinking`")?;
    }
    if let Some(choice) = &tool_choice {
        let (ToolChoice::Auto { other, .. }
        | ToolChoice::Any { other, .. }
        | ToolChoice::Tool { other, .. }
        | ToolChoice::None { other }) = choice;
        refuse_other(&model, other, &[], "in `tool_choice`")?;
    }
    if let Some(config) = &output_config {
        refuse_other(&model, &config.other, &[], "in `output_config`")?;
    }
    let output_format = match output_config.and_then(|config| config.format) {
        Some(format) => Some((format, "in `output_config.format`")),
        None => output_format.map(|format| (format, "in `output_format`")),
    };
    if let Some((format, place)) = &output_format {
        refuse_other(&model, &format.other, &[], place)?;
    }

    let tools = tools
        .map(|tools| Tools::new(&model, tools))
        .transpose()?
        .unwrap_or_default();
    let loaded = loaded_tools(&model, &messages, &tools)?;

    let mut chat_messages = Vec::with_capacity(messages.len() + 1);
    if let Some(system) = system {
        chat_messages.push(system_message(&model, system)?);
    }
    for message in messages {
        append_message(&model, message, &tools, &mut chat_messages)?;
    }

    let tools = tools.offered(&loaded);
    let offers_tools = tools.as_ref().is_some_and(|tools| !tools.is_empty());
    let parallel_tool_calls = tool_choice
        .as_ref()
        .filter(|choice| offers_tools && choice.disables_parallel_tool_use())
        .map(|_| false);

    let response_format = output_format
        .map(|(format, _)| response_format(&model, format))
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
            ContentBlockParam::Text(text) => text_of(model, text),
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
    tools: &Tools,
    chat_messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), MessagesError> {
    refuse_other(model, &message.other, &[], "on a message")?;
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
            append_user_blocks(model, blocks, tools, chat_messages)?;
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

/// A text block's text. Its `cache_control` only says how to cache the prompt, and its
/// `citations`, which point into the documents an earlier answer was given, are left behind
/// like that answer's thinking: the chat completion has no place for either. Any other field is
/// refused by name.
fn text_of(model: &str, block: TextBlockParam) -> Result<String, MessagesError> {
    refuse_other(model, &block.other, &BLOCK_FIELDS, "on a text block")?;
    Ok(block.text)
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
    tools: &Tools,
    chat_messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), MessagesError> {
    let mut tool_images = Vec::new();
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            ContentBlockParam::ToolResult(result) => {
                chat_messages.push(tool_message(model, result, tools, &mut tool_images)?);
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
        ContentBlockParam::Text(text) => parts.push(text_part(text_of(model, text)?)),
        ContentBlockParam::Image(image) => parts.push(image_part(model, image)?),
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
    image: ImageBlockParam,
) -> Result<ChatCompletionRequestUserMessageContentPart, MessagesError> {
    refuse_other(model, &image.other, &BLOCK_FIELDS, "on an image block")?;
    let url = match image.source {
        ImageSource::Base64 {
            media_type,
            data,
            other,
        } => {
            refuse_other(model, &other, &[], "on an image source")?;
            format!("data:{media_type};base64,{data}")
        }
        ImageSource::Url { url, other } => {
            refuse_other(model, &other, &[], "on an image source")?;
            url
        }
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

/// A document as the parts that carry it, after its title and context: a PDF as a file, text
/// as text, and content blocks as their own parts.
fn append_document(
    model: &str,
    document: DocumentBlockParam,
    parts: &mut Vec<ChatCompletionRequestUserMessageContentPart>,
) -> Result<(), MessagesError> {
    refuse_other(model, &document.other, &BLOCK_FIELDS, "on a document block")?;
    if let Some(citations) = &document.citations {
        refuse_other(model, &citations.other, &[], "in a document's `citations`")?;
    }
    if document
        .citations
        .is_some_and(|citations| citations.enabled)
    {
        return Err(unsupported(
            model,
            "citations",
            "Remove `citations` from the document, or use a model with `from: anthropic`.",
        ));
    }
    // The title and context say what the document is and how to read it, so the model is given
    // them whatever the document's source.
    let heading = [document.title.as_deref(), document.context.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    match document.source {
        DocumentSource::Base64 {
            media_type,
            data,
            other,
        } => {
            refuse_other(model, &other, &[], "on a document source")?;
            if !heading.is_empty() {
                parts.push(text_part(heading));
            }
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
        DocumentSource::Text { data, other, .. } => {
            refuse_other(model, &other, &[], "on a document source")?;
            parts.push(text_part(if heading.is_empty() {
                data
            } else {
                format!("{heading}\n\n{data}")
            }));
        }
        DocumentSource::Content { content, other } => {
            refuse_other(model, &other, &[], "on a document source")?;
            if !heading.is_empty() {
                parts.push(text_part(heading));
            }
            match content {
                MessageContent::Text(text) => parts.push(text_part(text)),
                MessageContent::Blocks(blocks) => {
                    for block in blocks {
                        append_user_part(model, block, parts)?;
                    }
                }
            }
        }
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
/// returned are appended to `images`, for the user message that follows, and each
/// `tool_reference` reads as the definition of the tool it names.
fn tool_message(
    model: &str,
    result: ToolResultBlockParam,
    tools: &Tools,
    images: &mut Vec<ChatCompletionRequestUserMessageContentPart>,
) -> Result<ChatCompletionRequestMessage, MessagesError> {
    refuse_other(
        model,
        &result.other,
        &BLOCK_FIELDS,
        "on a tool_result block",
    )?;
    let text = match result.content {
        None => String::new(),
        Some(ToolResultContent::Text(text)) => text,
        Some(ToolResultContent::Blocks(blocks)) => {
            let mut texts = Vec::new();
            // The definitions of a run of `tool_reference` blocks, which read as one
            // `<functions>` block.
            let mut functions = Vec::new();
            for block in blocks {
                if let ContentBlockParam::Other { block_type, block } = &block
                    && block_type == TOOL_REFERENCE
                {
                    let name = tool_reference_name(model, block)?;
                    functions.push(tools.definition(model, name)?);
                    continue;
                }
                if !functions.is_empty() {
                    texts.push(functions_block(&mut functions));
                }
                match block {
                    ContentBlockParam::Text(text) => texts.push(text_of(model, text)?),
                    ContentBlockParam::Image(image) => {
                        images.push(image_part(model, image)?);
                    }
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
                            "Return the tool result as text, images or `tool_reference` blocks.",
                        ));
                    }
                }
            }
            if !functions.is_empty() {
                texts.push(functions_block(&mut functions));
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
            ContentBlockParam::Text(block) => text
                .get_or_insert_default()
                .push_str(&text_of(model, block)?),
            ContentBlockParam::ToolUse(tool_use) => {
                refuse_other(model, &tool_use.other, &BLOCK_FIELDS, "on a tool_use block")?;
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

/// The `type` of the block that loads a deferred tool.
const TOOL_REFERENCE: &str = "tool_reference";

/// A request's tools as chat completion tools, in the request's order, each with whether
/// `defer_loading` holds it back until the conversation loads it.
#[derive(Default)]
struct Tools(Vec<(FunctionObject, bool)>);

impl Tools {
    fn new(model: &str, tools: Vec<ToolParam>) -> Result<Self, MessagesError> {
        tools
            .into_iter()
            .map(|tool| chat_tool(model, tool))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    fn get(&self, name: &str) -> Option<&(FunctionObject, bool)> {
        self.0.iter().find(|(function, _)| function.name == name)
    }

    /// The tool `name` as the line a `tool_reference` to it reads as: its definition in the
    /// encoding of a tool list, `<function>{"description": …, "name": …, "parameters": …}</function>`.
    fn definition(&self, model: &str, name: &str) -> Result<String, MessagesError> {
        let Some((function, _)) = self.get(name) else {
            return Err(unknown_tool_reference(model, name));
        };
        let mut definition = Map::new();
        if let Some(description) = &function.description {
            definition.insert("description".into(), Value::String(description.clone()));
        }
        definition.insert("name".into(), Value::String(function.name.clone()));
        if let Some(parameters) = &function.parameters {
            definition.insert("parameters".into(), parameters.clone());
        }
        Ok(format!(
            "<function>{}</function>",
            Value::Object(definition)
        ))
    }

    /// The tools to offer the model: every tool that was never deferred, in the request's order,
    /// then each deferred tool in `loaded`, in that order. `None` when the request offered none.
    fn offered(self, loaded: &[String]) -> Option<Vec<ChatCompletionTools>> {
        let tool = |function| ChatCompletionTools::Function(ChatCompletionTool { function });
        let (mut deferred, eager): (Vec<_>, Vec<_>) =
            self.0.into_iter().partition(|(_, deferred)| *deferred);
        let mut offered: Vec<_> = eager
            .into_iter()
            .map(|(function, _)| tool(function))
            .collect();
        for name in loaded {
            if let Some(at) = deferred
                .iter()
                .position(|(function, _)| &function.name == name)
            {
                offered.push(tool(deferred.swap_remove(at).0));
            }
        }
        (!offered.is_empty()).then_some(offered)
    }
}

/// `function` lines as one `<functions>` block, emptying `functions`.
fn functions_block(functions: &mut Vec<String>) -> String {
    let block = format!("<functions>\n{}\n</functions>", functions.join("\n"));
    functions.clear();
    block
}

/// The name a `tool_reference` block loads. Its `cache_control` only says how to cache the
/// prompt; any other field is refused by name.
fn tool_reference_name<'a>(model: &str, block: &'a Value) -> Result<&'a str, MessagesError> {
    let Some(name) = block.get("tool_name").and_then(Value::as_str) else {
        return Err(MessagesError::invalid_request(
            "A `tool_reference` block needs a `tool_name`: the name of a tool in `tools`.",
        ));
    };
    if let Some(fields) = block.as_object() {
        let other = fields
            .iter()
            .filter(|(field, _)| field.as_str() != "tool_name")
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect();
        refuse_other(model, &other, &BLOCK_FIELDS, "on a tool_reference block")?;
    }
    Ok(name)
}

fn unknown_tool_reference(model: &str, name: &str) -> MessagesError {
    MessagesError::invalid_request(format!(
        "A `tool_reference` in the request to model '{model}' names the tool '{name}', which is not in `tools`. Add '{name}' to `tools`, with `defer_loading: true` to offer it only once it is referenced. See: {DOCS}"
    ))
}

/// The tools `messages` load, by name, in the order their first `tool_reference` names them.
/// Each must name a tool in `tools`.
fn loaded_tools(
    model: &str,
    messages: &[MessageParam],
    tools: &Tools,
) -> Result<Vec<String>, MessagesError> {
    let mut loaded: Vec<String> = Vec::new();
    let results = messages.iter().flat_map(|message| match &message.content {
        MessageContent::Blocks(blocks) => blocks.as_slice(),
        MessageContent::Text(_) => &[],
    });
    for result in results {
        let ContentBlockParam::ToolResult(ToolResultBlockParam {
            content: Some(ToolResultContent::Blocks(blocks)),
            ..
        }) = result
        else {
            continue;
        };
        for block in blocks {
            if let ContentBlockParam::Other { block_type, block } = block
                && block_type == TOOL_REFERENCE
            {
                let name = tool_reference_name(model, block)?;
                if tools.get(name).is_none() {
                    return Err(unknown_tool_reference(model, name));
                }
                if !loaded.iter().any(|loaded| loaded == name) {
                    loaded.push(name.to_string());
                }
            }
        }
    }
    Ok(loaded)
}

/// `tool` as a chat completion function, with whether `defer_loading` holds it back.
fn chat_tool(model: &str, tool: ToolParam) -> Result<(FunctionObject, bool), MessagesError> {
    match tool {
        ToolParam::Custom(CustomToolParam {
            name,
            description,
            input_schema,
            strict,
            other,
        }) => {
            // `cache_control` and `eager_input_streaming` only change how a request is served.
            refuse_other(
                model,
                &other,
                &[
                    "type",
                    "cache_control",
                    "eager_input_streaming",
                    "defer_loading",
                ],
                &format!("on the tool '{name}'"),
            )?;
            // Only an absent `defer_loading` defaults: an explicit `null` is not `true` or `false`.
            let deferred = match other.get("defer_loading") {
                None => false,
                Some(Value::Bool(deferred)) => *deferred,
                Some(value) => {
                    return Err(MessagesError::invalid_request(format!(
                        "`defer_loading` on the tool '{name}' is {value}, and must be `true` or `false`."
                    )));
                }
            };
            Ok((
                FunctionObject {
                    name,
                    description,
                    parameters: Some(input_schema),
                    strict,
                },
                deferred,
            ))
        }
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
        ToolChoice::None { .. } => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None),
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
