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

//! A Responses API request as a chat completion request.
//!
//! What the chat completion can carry is carried over. What only changes how the request is
//! served or recorded, not what the model is asked, is accepted and left out: `store`,
//! `include`, `metadata`, `prompt_cache_key`, `prompt_cache_retention`, `service_tier`,
//! `truncation`, `safety_identifier`, `stream_options`, `max_tool_calls` (which counts only
//! hosted tool calls, and none run here), `reasoning.summary` and `text.verbosity`. Fields the
//! typed request has no place for, such as `user` and `client_metadata`, never reach it.
//!
//! Anything else a chat completion cannot carry is refused by name: state the provider keeps
//! (`previous_response_id`, `conversation`, item references, stored `prompt` templates),
//! `background` runs, files, log probabilities, and items only a hosted tool produces. The
//! client never gets an answer to a different question than the one it asked.
//!
//! Two things are dropped on purpose, so that a conversation served here before stays
//! servable:
//!
//! - **`reasoning` items.** They hold the provider's own record of an earlier answer's
//!   reasoning, and their `encrypted_content` is readable only by the provider that wrote it:
//!   a chat completion has no place to carry it, and another model could not read it.
//! - **Hosted tools** (`web_search`, `file_search`, `code_interpreter`, `computer_use`,
//!   `image_generation`, `mcp`, `local_shell`, `shell`, `apply_patch`, `tool_search`). They run
//!   on the Responses API provider's servers, or produce items only it defines, so a chat
//!   completion cannot run them. They are left out of the chat request: the model simply does
//!   not see them, and answers as it would without them.
//!
//! A tool in a `namespace` is offered to the model as the function `{namespace}__{name}` (or
//! `{namespace}{name}` when the namespace already ends with `__`, as an MCP server's does), and
//! its calls come back as `function_call` items with the `namespace` and `name` apart. A custom
//! (freeform) tool is offered as a function whose one string parameter, `input`, carries the
//! tool's input, and its calls come back as `custom_tool_call` items.

use std::collections::HashMap;

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionAllowedTools, ChatCompletionAllowedToolsChoice, ChatCompletionMessageToolCall,
    ChatCompletionMessageToolCalls, ChatCompletionNamedToolChoice,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartImage,
    ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
    ChatCompletionRequestUserMessageContentPart, ChatCompletionStreamOptions, ChatCompletionTool,
    ChatCompletionToolChoiceOption, ChatCompletionTools, CreateChatCompletionRequest, FunctionCall,
    FunctionName, FunctionObject, ImageUrl, ResponseFormat,
    ToolChoiceAllowedMode as ChatToolChoiceAllowedMode, ToolChoiceOptions as ChatToolChoiceOptions,
};
use async_openai::types::responses::{
    CreateResponse, CustomGrammarFormatParam, CustomToolCallOutputOutput, CustomToolParam,
    CustomToolParamFormat, EasyInputContent, EasyInputMessage, FunctionCallOutput, FunctionTool,
    FunctionToolParam, GrammarSyntax, ImageDetail, InputContent, InputItem, InputMessage,
    InputParam, InputRole, InputTextContent, Item, MessageItem, NamespaceToolParamTool,
    OutputMessage, OutputMessageContent, Role, TextResponseFormatConfiguration, Tool,
    ToolChoiceAllowed, ToolChoiceAllowedMode, ToolChoiceOptions, ToolChoiceParam,
};
use serde_json::{Value, json};

use crate::response::ResponseContext;

/// Where to read about serving the Responses API with a model.
pub(crate) const DOCS: &str = "https://spiceai.org/docs/components/models";

/// What joins a namespace and the name of one of its tools in the function the model is
/// offered.
const NAMESPACE_SEPARATOR: &str = "__";

/// The one parameter of the function a custom tool is offered as.
pub(crate) const CUSTOM_INPUT: &str = "input";

/// A chat completion request, and what the Response that answers it needs of the request.
#[derive(Debug, Clone)]
pub struct Translated {
    pub chat: CreateChatCompletionRequest,
    pub context: ResponseContext,
    /// The `type` of each hosted tool the request offered, which the chat model is not
    /// offered: see the module documentation.
    pub left_out_tools: Vec<&'static str>,
}

/// The Responses API tool a function the model is offered stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolTarget {
    pub(crate) namespace: Option<String>,
    pub(crate) name: String,
    /// Whether it is a custom (freeform) tool, whose calls carry an `input` string.
    pub(crate) custom: bool,
}

/// The Responses API tools behind the functions the model is offered, by function name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolNames {
    by_function: HashMap<String, ToolTarget>,
}

impl ToolNames {
    /// The tool behind `function`. A function the model was not offered is a plain function
    /// tool of that name, so the client can tell the model it does not exist.
    pub(crate) fn target(&self, function: &str) -> ToolTarget {
        self.by_function
            .get(function)
            .cloned()
            .unwrap_or_else(|| ToolTarget {
                namespace: None,
                name: function.to_string(),
                custom: false,
            })
    }

    fn insert(
        &mut self,
        model: &str,
        function: String,
        target: ToolTarget,
    ) -> Result<(), OpenAIError> {
        if self.by_function.contains_key(&function) {
            return Err(invalid_request(
                "tools",
                format!(
                    "Model '{model}' serves the Responses API by translating requests into chat completions, which offer each tool as a function, and two of the request's tools would both be the function '{function}'. Rename one of them, or use a model whose provider serves the Responses API natively (`from: openai`, `from: azure`, or `from: xai`). See: {DOCS}"
                ),
            ));
        }
        self.by_function.insert(function, target);
        Ok(())
    }
}

/// The name of the function the tool `name` in `namespace` is offered to the model as.
pub(crate) fn function_name(namespace: Option<&str>, name: &str) -> String {
    match namespace.filter(|namespace| !namespace.is_empty()) {
        None => name.to_string(),
        Some(namespace) if namespace.ends_with(NAMESPACE_SEPARATOR) => format!("{namespace}{name}"),
        Some(namespace) => format!("{namespace}{NAMESPACE_SEPARATOR}{name}"),
    }
}

/// The arguments of the function call that stands for a custom tool call with `input`.
pub(crate) fn custom_arguments(input: &str) -> String {
    json!({ CUSTOM_INPUT: input }).to_string()
}

pub(crate) fn invalid_request(param: &str, message: String) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message,
        r#type: Some("invalid_request_error".to_string()),
        param: Some(param.to_string()),
        code: None,
    })
}

/// The refusal for something in the request, at `param`, that a chat completion cannot carry.
fn unsupported(model: &str, param: &str, what: &str, remedy: &str) -> OpenAIError {
    invalid_request(
        param,
        format!(
            "Model '{model}' serves the Responses API by translating requests into chat completions, which cannot carry {what}. {remedy}, or use a model whose provider serves the Responses API natively (`from: openai`, `from: azure`, or `from: xai`). See: {DOCS}"
        ),
    )
}

/// `request` as a chat completion request, or the reason it cannot be one.
///
/// # Errors
///
/// Returns an `invalid_request_error`, with the `param` it is about, naming the first part of
/// `request` a chat completion cannot carry.
pub fn chat_request(request: CreateResponse) -> Result<Translated, OpenAIError> {
    // Destructured whole, so that a field the Responses API gains has to be decided on here.
    let CreateResponse {
        background,
        conversation,
        include: _,
        input,
        instructions,
        max_output_tokens,
        max_tool_calls: _,
        metadata: _,
        model,
        parallel_tool_calls,
        previous_response_id,
        prompt,
        prompt_cache_key: _,
        prompt_cache_retention: _,
        reasoning,
        safety_identifier: _,
        service_tier: _,
        store: _,
        stream,
        stream_options: _,
        temperature,
        text,
        tool_choice,
        tools,
        top_logprobs,
        top_p,
        truncation: _,
    } = request;

    let Some(model) = model.filter(|model| !model.is_empty()) else {
        return Err(invalid_request(
            "model",
            "A Responses API request names the `model` that answers it. Set `model` to the name of a model in the Spicepod.".to_string(),
        ));
    };
    if previous_response_id.is_some() {
        return Err(unsupported(
            &model,
            "previous_response_id",
            "`previous_response_id`, since the provider keeps no earlier responses",
            "Send the whole conversation in `input` instead",
        ));
    }
    if conversation.is_some() {
        return Err(unsupported(
            &model,
            "conversation",
            "`conversation`, since the provider keeps no conversations",
            "Send the whole conversation in `input` instead",
        ));
    }
    if background == Some(true) {
        return Err(unsupported(
            &model,
            "background",
            "`background: true`, since a chat completion runs only while the request waits for it",
            "Remove `background`",
        ));
    }
    if prompt.is_some() {
        return Err(unsupported(
            &model,
            "prompt",
            "a stored `prompt` template",
            "Send the prompt's text in `instructions` and `input` instead",
        ));
    }
    if top_logprobs.is_some_and(|count| count > 0) {
        return Err(unsupported(
            &model,
            "top_logprobs",
            "`top_logprobs`",
            "Remove `top_logprobs`",
        ));
    }

    let mut messages = Messages::default();
    if let Some(instructions) = instructions.as_ref().filter(|text| !text.is_empty()) {
        messages.push(system_message(
            ChatCompletionRequestSystemMessageContent::Text(instructions.clone()),
        ));
    }
    match input {
        InputParam::Text(text) => messages.push(user_message(
            ChatCompletionRequestUserMessageContent::Text(text),
        )),
        InputParam::Items(items) => {
            for item in items {
                append_item(&model, item, &mut messages)?;
            }
        }
    }

    let mut names = ToolNames::default();
    let mut chat_tools = Vec::new();
    for tool in tools.iter().flatten() {
        append_tool(&model, tool, &mut names, &mut chat_tools)?;
    }
    let left_out_tools: Vec<&'static str> = tools
        .iter()
        .flatten()
        .filter_map(hosted_tool_type)
        .collect();
    let offers_tools = !chat_tools.is_empty();
    let chat_tool_choice = match tool_choice
        .as_ref()
        .map(|choice| chat_tool_choice(&model, choice))
        .transpose()?
        .flatten()
    {
        // A chat completion refuses a `tool_choice` without tools, and with none offered, the
        // model can only answer, which is what `auto` and `none` ask of it then.
        Some(ChatCompletionToolChoiceOption::Mode(
            ChatToolChoiceOptions::Auto | ChatToolChoiceOptions::None,
        )) if !offers_tools => None,
        Some(_) if !offers_tools => {
            return Err(unsupported(
                &model,
                "tool_choice",
                "a `tool_choice` that asks for a tool call when the model is offered no function to call",
                "Add the function to `tools`",
            ));
        }
        choice => choice,
    };

    let response_format = text.and_then(|text| match text.format {
        TextResponseFormatConfiguration::Text => None,
        TextResponseFormatConfiguration::JsonObject => Some(ResponseFormat::JsonObject),
        TextResponseFormatConfiguration::JsonSchema(json_schema) => {
            Some(ResponseFormat::JsonSchema { json_schema })
        }
    });

    let streaming = stream == Some(true);
    let chat = CreateChatCompletionRequest {
        messages: messages.finish(),
        model: model.clone(),
        max_completion_tokens: max_output_tokens,
        temperature,
        top_p,
        reasoning_effort: reasoning.and_then(|reasoning| reasoning.effort),
        response_format,
        tools: offers_tools.then_some(chat_tools),
        tool_choice: chat_tool_choice,
        // Some servers refuse `parallel_tool_calls` on a request without tools.
        parallel_tool_calls: parallel_tool_calls.filter(|_| offers_tools),
        stream: streaming.then_some(true),
        stream_options: streaming.then_some(ChatCompletionStreamOptions {
            include_usage: Some(true),
            include_obfuscation: None,
        }),
        ..Default::default()
    };
    Ok(Translated {
        chat,
        left_out_tools,
        context: ResponseContext {
            model,
            instructions,
            // The tools the model was offered: a Response does not list a hosted tool it
            // never saw.
            tools: tools
                .unwrap_or_default()
                .into_iter()
                .filter(|tool| hosted_tool_type(tool).is_none())
                .collect(),
            tool_choice: tool_choice.unwrap_or(ToolChoiceParam::Mode(ToolChoiceOptions::Auto)),
            parallel_tool_calls: parallel_tool_calls.unwrap_or(true),
            temperature,
            top_p,
            max_output_tokens,
            tool_names: names,
        },
    })
}

/// The chat messages a request's input becomes, built in order.
///
/// A tool message carries only text, so the images a tool output returns are held back and
/// given in a user message after the run of tool messages they came in: the chat completion
/// requires the tool messages to follow the assistant message that called the tools.
#[derive(Default)]
struct Messages {
    messages: Vec<ChatCompletionRequestMessage>,
    tool_images: Vec<ChatCompletionRequestUserMessageContentPart>,
}

impl Messages {
    fn flush_tool_images(&mut self) {
        if !self.tool_images.is_empty() {
            let images = std::mem::take(&mut self.tool_images);
            self.messages.push(user_message(
                ChatCompletionRequestUserMessageContent::Array(images),
            ));
        }
    }

    fn push(&mut self, message: ChatCompletionRequestMessage) {
        self.flush_tool_images();
        self.messages.push(message);
    }

    fn push_tool(&mut self, message: ChatCompletionRequestMessage) {
        self.messages.push(message);
    }

    /// Appends assistant text. Text that follows tool calls, before their outputs, joins the
    /// assistant message that made them, since the tool messages have to follow that message.
    fn push_assistant_text(&mut self, text: String) {
        // Some providers refuse an assistant message without text, and an empty one says nothing.
        if text.is_empty() {
            return;
        }
        self.flush_tool_images();
        if let Some(ChatCompletionRequestMessage::Assistant(message)) = self.messages.last_mut()
            && message.tool_calls.is_some()
        {
            message.content = Some(ChatCompletionRequestAssistantMessageContent::Text(
                match message.content.take() {
                    Some(ChatCompletionRequestAssistantMessageContent::Text(before))
                        if !before.is_empty() =>
                    {
                        format!("{before}\n{text}")
                    }
                    _ => text,
                },
            ));
            return;
        }
        self.messages
            .push(assistant_message(Some(text), Vec::new()));
    }

    /// Appends a tool call. Consecutive tool calls, and the assistant text right before them,
    /// are one assistant message, as the chat completion that made them was.
    fn push_tool_call(&mut self, call_id: String, function: String, arguments: String) {
        self.flush_tool_images();
        let call = ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
            id: call_id,
            function: FunctionCall {
                name: function,
                arguments,
            },
        });
        if let Some(ChatCompletionRequestMessage::Assistant(message)) = self.messages.last_mut() {
            message.tool_calls.get_or_insert_default().push(call);
            return;
        }
        self.messages.push(assistant_message(None, vec![call]));
    }

    fn finish(mut self) -> Vec<ChatCompletionRequestMessage> {
        self.flush_tool_images();
        self.messages
    }
}

fn system_message(
    content: ChatCompletionRequestSystemMessageContent,
) -> ChatCompletionRequestMessage {
    ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
        content,
        name: None,
    })
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

/// The `type` `value` is given on the wire, to name it in a refusal.
fn wire_type(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Appends `item` to `messages` as the chat messages that carry it.
fn append_item(model: &str, item: InputItem, messages: &mut Messages) -> Result<(), OpenAIError> {
    match item {
        InputItem::ItemReference(reference) => Err(unsupported(
            model,
            "input",
            &format!(
                "the reference to item '{}', since the provider keeps no earlier items",
                reference.id
            ),
            "Send the item itself instead of a reference to it",
        )),
        InputItem::EasyMessage(EasyInputMessage { role, content, .. }) => {
            let parts = match content {
                EasyInputContent::Text(text) => {
                    vec![InputContent::InputText(InputTextContent { text })]
                }
                EasyInputContent::ContentList(parts) => parts,
            };
            append_parts_message(model, role, parts, messages)
        }
        InputItem::Item(Item::Message(MessageItem::Input(InputMessage {
            content, role, ..
        }))) => {
            let role = match role {
                InputRole::User => Role::User,
                InputRole::System => Role::System,
                InputRole::Developer => Role::Developer,
            };
            append_parts_message(model, role, content, messages)
        }
        InputItem::Item(Item::Message(MessageItem::Output(message))) => {
            messages.push_assistant_text(output_message_text(message));
            Ok(())
        }
        InputItem::Item(Item::FunctionCall(call)) => {
            messages.push_tool_call(
                call.call_id,
                function_name(call.namespace.as_deref(), &call.name),
                call.arguments,
            );
            Ok(())
        }
        InputItem::Item(Item::CustomToolCall(call)) => {
            messages.push_tool_call(
                call.call_id,
                function_name(call.namespace.as_deref(), &call.name),
                custom_arguments(&call.input),
            );
            Ok(())
        }
        InputItem::Item(Item::FunctionCallOutput(output)) => {
            let parts = match output.output {
                FunctionCallOutput::Text(text) => {
                    messages.push_tool(tool_message(output.call_id, text));
                    return Ok(());
                }
                FunctionCallOutput::Content(parts) => parts,
            };
            append_tool_output(model, output.call_id, parts, messages)
        }
        InputItem::Item(Item::CustomToolCallOutput(output)) => {
            let parts = match output.output {
                CustomToolCallOutputOutput::Text(text) => {
                    messages.push_tool(tool_message(output.call_id, text));
                    return Ok(());
                }
                CustomToolCallOutputOutput::List(parts) => parts,
            };
            append_tool_output(model, output.call_id, parts, messages)
        }
        // Dropped: see the module documentation.
        InputItem::Item(Item::Reasoning(_)) => Ok(()),
        InputItem::Item(other) => {
            let item_type = wire_type(&other);
            Err(unsupported(
                model,
                "input",
                &format!("`{item_type}` items, which only the provider's own tools produce"),
                &format!("Remove the `{item_type}` items from `input`"),
            ))
        }
    }
}

/// The text of an earlier answer: its text and its refusals, in order.
fn output_message_text(message: OutputMessage) -> String {
    message
        .content
        .into_iter()
        .map(|part| match part {
            OutputMessageContent::OutputText(text) => text.text,
            OutputMessageContent::Refusal(refusal) => refusal.refusal,
        })
        .collect()
}

/// A message's content parts as the chat message of its role. `developer` is `system`, which
/// is what the developer role means to a chat model that has no role of that name.
fn append_parts_message(
    model: &str,
    role: Role,
    parts: Vec<InputContent>,
    messages: &mut Messages,
) -> Result<(), OpenAIError> {
    match role {
        Role::User => {
            let parts = parts
                .into_iter()
                .map(|part| user_part(model, part))
                .collect::<Result<Vec<_>, _>>()?;
            messages.push(user_message(user_content(parts)));
        }
        Role::System | Role::Developer => {
            let mut texts = text_parts(model, parts, "a system or developer message")?;
            messages.push(system_message(if texts.len() == 1 {
                ChatCompletionRequestSystemMessageContent::Text(texts.remove(0))
            } else {
                ChatCompletionRequestSystemMessageContent::Array(
                    texts
                        .into_iter()
                        .map(|text| {
                            ChatCompletionRequestSystemMessageContentPart::Text(
                                ChatCompletionRequestMessageContentPartText { text },
                            )
                        })
                        .collect(),
                )
            }));
        }
        Role::Assistant => {
            let text = text_parts(model, parts, "an assistant message")?.concat();
            messages.push_assistant_text(text);
        }
    }
    Ok(())
}

/// The text of `parts`, one string per part: a message of a role that carries only text.
fn text_parts(
    model: &str,
    parts: Vec<InputContent>,
    place: &str,
) -> Result<Vec<String>, OpenAIError> {
    parts
        .into_iter()
        .map(|part| match part {
            InputContent::InputText(text) => Ok(text.text),
            InputContent::InputImage(_) => Err(unsupported(
                model,
                "input",
                &format!("an image in {place}"),
                "Send the image in a user message",
            )),
            InputContent::InputFile(_) => Err(input_file(model)),
        })
        .collect()
}

fn input_file(model: &str) -> OpenAIError {
    unsupported(
        model,
        "input",
        "`input_file` content",
        "Send the file's text as `input_text`, or an image as `input_image`",
    )
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

fn user_part(
    model: &str,
    part: InputContent,
) -> Result<ChatCompletionRequestUserMessageContentPart, OpenAIError> {
    match part {
        InputContent::InputText(text) => Ok(ChatCompletionRequestUserMessageContentPart::Text(
            ChatCompletionRequestMessageContentPartText { text: text.text },
        )),
        InputContent::InputImage(image) => {
            let Some(url) = image.image_url else {
                return Err(unsupported(
                    model,
                    "input",
                    "an image by `file_id`, since the provider keeps no files",
                    "Send the image as an `image_url`, which may be a `data:` URL",
                ));
            };
            Ok(ChatCompletionRequestUserMessageContentPart::ImageUrl(
                ChatCompletionRequestMessageContentPartImage {
                    image_url: ImageUrl {
                        url,
                        // `auto` is every server's default, and not every server reads `detail`.
                        detail: (image.detail != ImageDetail::Auto).then_some(image.detail),
                    },
                },
            ))
        }
        InputContent::InputFile(_) => Err(input_file(model)),
    }
}

fn tool_message(call_id: String, text: String) -> ChatCompletionRequestMessage {
    ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
        content: ChatCompletionRequestToolMessageContent::Text(text),
        tool_call_id: call_id,
    })
}

/// A tool output given as parts: its text parts, joined, as the tool message, and its images
/// held back for the user message after the tool messages.
fn append_tool_output(
    model: &str,
    call_id: String,
    parts: Vec<InputContent>,
    messages: &mut Messages,
) -> Result<(), OpenAIError> {
    let mut texts = Vec::new();
    for part in parts {
        match user_part(model, part)? {
            ChatCompletionRequestUserMessageContentPart::Text(text) => texts.push(text.text),
            image => messages.tool_images.push(image),
        }
    }
    messages.push_tool(tool_message(call_id, texts.join("\n")));
    Ok(())
}

fn function_object(
    name: String,
    description: Option<String>,
    parameters: Option<Value>,
    strict: Option<bool>,
) -> FunctionObject {
    FunctionObject {
        name,
        description,
        parameters,
        strict,
    }
}

/// The parameters of the function a custom tool is offered as: one string, `input`, which says
/// what the tool's grammar, if it has one, requires of it.
fn custom_parameters(format: &CustomToolParamFormat) -> Value {
    let description = match format {
        CustomToolParamFormat::Text => "The tool's input, as free-form text.".to_string(),
        CustomToolParamFormat::Grammar(CustomGrammarFormatParam { definition, syntax }) => {
            let syntax = match syntax {
                GrammarSyntax::Lark => "Lark grammar",
                GrammarSyntax::Regex => "regular expression",
            };
            format!("The tool's input, as free-form text that matches this {syntax}:\n{definition}")
        }
    };
    json!({
        "type": "object",
        "properties": {CUSTOM_INPUT: {"type": "string", "description": description}},
        "required": [CUSTOM_INPUT],
        "additionalProperties": false,
    })
}

fn append_tool(
    model: &str,
    tool: &Tool,
    names: &mut ToolNames,
    chat_tools: &mut Vec<ChatCompletionTools>,
) -> Result<(), OpenAIError> {
    let mut add = |namespace: Option<&str>, name: &str, custom: bool, function: FunctionObject| {
        names.insert(
            model,
            function.name.clone(),
            ToolTarget {
                namespace: namespace
                    .filter(|namespace| !namespace.is_empty())
                    .map(str::to_string),
                name: name.to_string(),
                custom,
            },
        )?;
        chat_tools.push(ChatCompletionTools::Function(ChatCompletionTool {
            function,
        }));
        Ok::<(), OpenAIError>(())
    };
    match tool {
        Tool::Function(FunctionTool {
            name,
            parameters,
            strict,
            description,
            defer_loading: _,
        }) => add(
            None,
            name,
            false,
            function_object(
                name.clone(),
                description.clone(),
                parameters.clone(),
                *strict,
            ),
        ),
        Tool::Custom(CustomToolParam {
            name,
            description,
            format,
            defer_loading: _,
        }) => add(
            None,
            name,
            true,
            function_object(
                name.clone(),
                description.clone(),
                Some(custom_parameters(format)),
                None,
            ),
        ),
        Tool::Namespace(namespace) => {
            let space = Some(namespace.name.as_str());
            for tool in &namespace.tools {
                match tool {
                    NamespaceToolParamTool::Function(FunctionToolParam {
                        name,
                        description,
                        parameters,
                        strict,
                        defer_loading: _,
                    }) => add(
                        space,
                        name,
                        false,
                        function_object(
                            function_name(space, name),
                            description.clone(),
                            parameters.clone(),
                            *strict,
                        ),
                    )?,
                    NamespaceToolParamTool::Custom(CustomToolParam {
                        name,
                        description,
                        format,
                        defer_loading: _,
                    }) => add(
                        space,
                        name,
                        true,
                        function_object(
                            function_name(space, name),
                            description.clone(),
                            Some(custom_parameters(format)),
                            None,
                        ),
                    )?,
                }
            }
            Ok(())
        }
        // Hosted tools: see the module documentation.
        Tool::FileSearch(_)
        | Tool::ComputerUsePreview(_)
        | Tool::WebSearch(_)
        | Tool::WebSearch20250826(_)
        | Tool::Mcp(_)
        | Tool::CodeInterpreter(_)
        | Tool::ImageGeneration(_)
        | Tool::LocalShell
        | Tool::Shell(_)
        | Tool::Computer(_)
        | Tool::ToolSearch(_)
        | Tool::WebSearchPreview(_)
        | Tool::WebSearchPreview20250311(_)
        | Tool::ApplyPatch => Ok(()),
    }
}

/// The `type` of `tool` when it is a hosted tool, one a chat completion cannot run.
fn hosted_tool_type(tool: &Tool) -> Option<&'static str> {
    Some(match tool {
        Tool::Function(_) | Tool::Custom(_) | Tool::Namespace(_) => return None,
        Tool::FileSearch(_) => "file_search",
        Tool::ComputerUsePreview(_) => "computer_use_preview",
        Tool::WebSearch(_) => "web_search",
        Tool::WebSearch20250826(_) => "web_search_2025_08_26",
        Tool::Mcp(_) => "mcp",
        Tool::CodeInterpreter(_) => "code_interpreter",
        Tool::ImageGeneration(_) => "image_generation",
        Tool::LocalShell => "local_shell",
        Tool::Shell(_) => "shell",
        Tool::Computer(_) => "computer",
        Tool::ToolSearch(_) => "tool_search",
        Tool::WebSearchPreview(_) => "web_search_preview",
        Tool::WebSearchPreview20250311(_) => "web_search_preview_2025_03_11",
        Tool::ApplyPatch => "apply_patch",
    })
}

fn named_function(name: String) -> ChatCompletionToolChoiceOption {
    ChatCompletionToolChoiceOption::Function(ChatCompletionNamedToolChoice {
        function: FunctionName { name },
    })
}

/// The chat completion's `tool_choice` for `choice`; `None` leaves it to the chat model's own
/// default.
fn chat_tool_choice(
    model: &str,
    choice: &ToolChoiceParam,
) -> Result<Option<ChatCompletionToolChoiceOption>, OpenAIError> {
    let mode = |mode| Ok(Some(ChatCompletionToolChoiceOption::Mode(mode)));
    match choice {
        ToolChoiceParam::Mode(ToolChoiceOptions::None) => mode(ChatToolChoiceOptions::None),
        ToolChoiceParam::Mode(ToolChoiceOptions::Auto) => mode(ChatToolChoiceOptions::Auto),
        ToolChoiceParam::Mode(ToolChoiceOptions::Required) => mode(ChatToolChoiceOptions::Required),
        ToolChoiceParam::Function(function) => Ok(Some(named_function(function.name.clone()))),
        ToolChoiceParam::Custom(custom) => Ok(Some(named_function(custom.name.clone()))),
        ToolChoiceParam::AllowedTools(allowed) => allowed_tools(model, allowed),
        ToolChoiceParam::Hosted(_)
        | ToolChoiceParam::Mcp(_)
        | ToolChoiceParam::ApplyPatch
        | ToolChoiceParam::Shell => {
            let choice_type = wire_type(choice);
            Err(unsupported(
                model,
                "tool_choice",
                &format!(
                    "a `tool_choice` of the provider's own tool `{choice_type}`, which the model is not offered"
                ),
                "Choose `auto`, `none`, `required`, or a function",
            ))
        }
    }
}

/// `allowed_tools` over the functions the model is offered. The provider's own tools in it are
/// left out, as they are left out of the tools.
fn allowed_tools(
    model: &str,
    allowed: &ToolChoiceAllowed,
) -> Result<Option<ChatCompletionToolChoiceOption>, OpenAIError> {
    let functions: Vec<Value> = allowed
        .tools
        .iter()
        .filter(|tool| {
            matches!(
                tool.get("type").and_then(Value::as_str),
                Some("function" | "custom")
            )
        })
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?;
            let namespace = tool.get("namespace").and_then(Value::as_str);
            Some(json!({"type": "function", "function": {"name": function_name(namespace, name)}}))
        })
        .collect();
    match (&allowed.mode, functions.is_empty()) {
        // Only the provider's own tools were allowed, and none of them is offered: the model
        // can only answer.
        (ToolChoiceAllowedMode::Auto, true) => Ok(Some(ChatCompletionToolChoiceOption::Mode(
            ChatToolChoiceOptions::None,
        ))),
        (ToolChoiceAllowedMode::Required, true) => Err(unsupported(
            model,
            "tool_choice",
            "a `tool_choice` that requires one of the provider's own tools, which the model is not offered",
            "Allow a function in `tool_choice.tools`",
        )),
        (mode, false) => Ok(Some(ChatCompletionToolChoiceOption::AllowedTools(
            ChatCompletionAllowedToolsChoice {
                allowed_tools: ChatCompletionAllowedTools {
                    mode: match mode {
                        ToolChoiceAllowedMode::Auto => ChatToolChoiceAllowedMode::Auto,
                        ToolChoiceAllowedMode::Required => ChatToolChoiceAllowedMode::Required,
                    },
                    tools: functions,
                },
            },
        ))),
    }
}
