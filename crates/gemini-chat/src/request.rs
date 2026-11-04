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

//! A Gemini API request as a chat completion request.
//!
//! What a chat completion can carry is carried over: the system instruction, the turns with
//! their text, images and function calls and responses, the declared functions, how the model
//! may call them, and the sampling and output settings of `generationConfig`.
//!
//! Some fields are accepted and left out, because they only change how the request is served, or
//! because the chat completion has no equivalent and the model's own behavior stands in:
//!
//! - `safetySettings`: the chat model applies its own safety.
//! - `generationConfig.topK`: a chat completion has no top-k sampling.
//! - `generationConfig.thinkingConfig`: a chat completion has no per-request reasoning budget; a
//!   reasoning model's effort is its Spicepod `reasoning_effort` param. The model's reasoning is
//!   left out of the answer, which carries no `thought` parts.
//! - Tools Google runs (`googleSearch`, `googleSearchRetrieval`, `codeExecution`, `urlContext`,
//!   `fileSearch`, `googleMaps`, `computerUse`, `mcpServers`): a chat completion cannot run them,
//!   so only the declared functions are offered.
//! - The model's reasoning in earlier turns (parts marked `thought`) and `thoughtSignature`s: the
//!   provider's own record, which another model cannot read.
//! - Fields outside the public API, such as `labels` and `sessionId`.
//!
//! What would change the answer is refused with `INVALID_ARGUMENT`, naming it: more than one
//! candidate, cached content, media other than images, data by URI, code execution history, and
//! output other than text or JSON. The client never gets an answer to a different question than
//! the one it asked.

use async_openai::types::chat::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls, ChatCompletionNamedToolChoice,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartImage,
    ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionStreamOptions, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionTools, CreateChatCompletionRequest, FunctionCall as ChatFunctionCall,
    FunctionName, FunctionObject, ImageUrl, ResponseFormat, ResponseFormatJsonSchema,
    StopConfiguration, ToolChoiceOptions,
};
use gemini_api::{
    Blob, Content, FunctionCallingConfig, FunctionCallingMode, FunctionDeclaration,
    FunctionResponse, FunctionResponsePart, GeminiError, GenerateContentRequest, GenerationConfig,
    MODEL_ROLE, Part, Tool, USER_ROLE,
};
use serde_json::{Map, Value, json};

/// Where to read about serving the Gemini API with a model.
pub(crate) const DOCS: &str = "https://spiceai.org/docs/components/models";

/// Where Google documents the OpenAPI-subset Schema of `parameters` and `responseSchema`.
const SCHEMA_DOCS: &str = "https://ai.google.dev/api/caching#Schema";

/// The name `response_format` gives the schema of `responseSchema`.
const RESPONSE_SCHEMA_NAME: &str = "response";

/// The role older Gemini clients give the turn that carries function responses.
const FUNCTION_ROLE: &str = "function";

/// The refusal for something the request asks that a chat completion cannot carry.
fn unsupported(model: &str, what: &str, remedy: &str) -> GeminiError {
    GeminiError::invalid_argument(format!(
        "Model '{model}' serves the Gemini API by translating requests into chat completions, which cannot carry {what}. {remedy} See: {DOCS}"
    ))
}

/// `request` as a chat completion request for `model`, streamed when `stream`.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first part of `request` that a chat completion cannot
/// carry, or that is not a valid Gemini API request.
#[expect(deprecated)] // `seed` is how Gemini's `seed` reaches the OpenAI-compatible servers that honor it.
pub fn chat_request(
    model: &str,
    request: GenerateContentRequest,
    stream: bool,
) -> Result<CreateChatCompletionRequest, GeminiError> {
    let GenerateContentRequest {
        contents,
        system_instruction,
        tools,
        tool_config,
        generation_config,
        // The chat model applies its own safety.
        safety_settings: _,
        cached_content,
        // `labels`, `sessionId` and the like only describe the request.
        other: _,
    } = request;

    if let Some(name) = cached_content.filter(|name| !name.is_empty()) {
        return Err(unsupported(
            model,
            &format!("cached content ('{name}')"),
            "Send the cached contents in `contents` instead of `cachedContent`.",
        ));
    }
    let GenerationConfig {
        temperature,
        top_p,
        // A chat completion has no top-k sampling.
        top_k: _,
        candidate_count,
        max_output_tokens,
        mut stop_sequences,
        presence_penalty,
        frequency_penalty,
        seed,
        response_mime_type,
        response_schema,
        response_json_schema,
        response_modalities,
        // A chat completion has no per-request reasoning budget.
        thinking_config: _,
    } = generation_config.unwrap_or_default();
    if let Some(count @ 2..) = candidate_count {
        return Err(unsupported(
            model,
            &format!("{count} candidates"),
            "Set `candidateCount` to 1, or leave it unset, and send a request for each answer.",
        ));
    }
    if let Some(modality) = response_modalities.iter().find(|modality| {
        !matches!(
            modality.to_ascii_uppercase().as_str(),
            "TEXT" | "MODALITY_UNSPECIFIED"
        )
    }) {
        return Err(unsupported(
            model,
            &format!("the response modality '{modality}'"),
            "Ask for `responseModalities` [\"TEXT\"], or leave it unset.",
        ));
    }
    let response_format = response_format(
        model,
        response_mime_type,
        response_schema,
        response_json_schema,
    )?;
    if contents.is_empty() {
        return Err(GeminiError::invalid_argument(
            "The request has no `contents`. Send the conversation in `contents`, at least one turn.",
        ));
    }

    let mut messages = Vec::with_capacity(contents.len() + 1);
    if let Some(system) = system_instruction
        && let Some(message) = system_message(model, system)?
    {
        messages.push(message);
    }
    let mut calls = Calls::default();
    for (index, content) in contents.into_iter().enumerate() {
        append_content(model, index, content, &mut calls, &mut messages)?;
    }

    let (tools, tool_choice) = chat_tools(
        tools,
        tool_config.and_then(|config| config.function_calling_config),
    )?;

    stop_sequences.retain(|sequence| !sequence.is_empty());
    Ok(CreateChatCompletionRequest {
        messages,
        model: model.to_string(),
        max_completion_tokens: max_output_tokens,
        temperature,
        top_p,
        stop: (!stop_sequences.is_empty())
            .then_some(StopConfiguration::StringArray(stop_sequences)),
        presence_penalty,
        frequency_penalty,
        seed: seed.map(i64::from),
        response_format,
        tools,
        tool_choice,
        stream: stream.then_some(true),
        stream_options: stream.then_some(ChatCompletionStreamOptions {
            include_usage: Some(true),
            include_obfuscation: None,
        }),
        ..Default::default()
    })
}

/// The system instruction's text parts, joined, as a system message; `None` when it has no text.
fn system_message(
    model: &str,
    system: Content,
) -> Result<Option<ChatCompletionRequestMessage>, GeminiError> {
    let place = "`systemInstruction`";
    let mut texts = Vec::new();
    for part in system.parts {
        match part_data(place, part)? {
            PartData::Skip => {}
            PartData::Text(text) => {
                if !text.is_empty() {
                    texts.push(text);
                }
            }
            other => {
                return Err(unsupported(
                    model,
                    &format!("{} in {place}", other.describe()),
                    "Send the system instruction as text.",
                ));
            }
        }
    }
    if texts.is_empty() {
        return Ok(None);
    }
    Ok(Some(ChatCompletionRequestMessage::System(
        ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Text(texts.join("\n\n")),
            name: None,
        },
    )))
}

/// The data a part carries.
enum PartData {
    /// Nothing the model is asked: the model's reasoning, or a part with no data.
    Skip,
    Text(String),
    InlineData(Blob),
    FileData {
        file_uri: String,
    },
    FunctionCall(gemini_api::FunctionCall),
    FunctionResponse(FunctionResponse),
    ExecutableCode,
    CodeExecutionResult,
    /// Data of a kind not named above, by its field.
    Other(String),
}

impl PartData {
    fn describe(&self) -> String {
        match self {
            PartData::Skip | PartData::Text(_) => "a `text` part".to_string(),
            PartData::InlineData(blob) => format!("inline data of type '{}'", blob.mime_type),
            PartData::FileData { file_uri } => format!("data by URI ('{file_uri}')"),
            PartData::FunctionCall(_) => "a `functionCall` part".to_string(),
            PartData::FunctionResponse(_) => "a `functionResponse` part".to_string(),
            PartData::ExecutableCode => "an `executableCode` part".to_string(),
            PartData::CodeExecutionResult => "a `codeExecutionResult` part".to_string(),
            PartData::Other(field) => format!("a `{field}` part"),
        }
    }
}

/// The data `part` carries. A part carries one kind of data; one with more is refused.
fn part_data(place: &str, part: Part) -> Result<PartData, GeminiError> {
    let Part {
        text,
        thought,
        // The provider's record of its reasoning, which another model cannot read.
        thought_signature: _,
        inline_data,
        file_data,
        function_call,
        function_response,
        executable_code,
        code_execution_result,
        other,
    } = part;
    // The model's reasoning in an earlier turn is the provider's own record, which another model
    // cannot read.
    if thought == Some(true) {
        return Ok(PartData::Skip);
    }
    let kinds: Vec<&str> = [
        ("text", text.is_some()),
        ("inlineData", inline_data.is_some()),
        ("fileData", file_data.is_some()),
        ("functionCall", function_call.is_some()),
        ("functionResponse", function_response.is_some()),
        ("executableCode", executable_code.is_some()),
        ("codeExecutionResult", code_execution_result.is_some()),
    ]
    .into_iter()
    .filter_map(|(kind, present)| present.then_some(kind))
    .collect();
    if kinds.len() > 1 {
        return Err(GeminiError::invalid_argument(format!(
            "A part of {place} carries {}, but a part carries one kind of data. Send each in a part of its own.",
            kinds
                .iter()
                .map(|kind| format!("`{kind}`"))
                .collect::<Vec<_>>()
                .join(" and ")
        )));
    }
    Ok(if let Some(text) = text {
        PartData::Text(text)
    } else if let Some(blob) = inline_data {
        PartData::InlineData(blob)
    } else if let Some(file) = file_data {
        PartData::FileData {
            file_uri: file.file_uri,
        }
    } else if let Some(call) = function_call {
        PartData::FunctionCall(call)
    } else if let Some(response) = function_response {
        PartData::FunctionResponse(response)
    } else if executable_code.is_some() {
        PartData::ExecutableCode
    } else if code_execution_result.is_some() {
        PartData::CodeExecutionResult
    } else if let Some((field, _)) = other.into_iter().next() {
        PartData::Other(field)
    } else {
        PartData::Skip
    })
}

/// The refusal of a part a turn of `role` cannot carry.
fn refused(model: &str, place: &str, data: &PartData, role: &str) -> GeminiError {
    let what = format!("{} in {place}", data.describe());
    match data {
        PartData::InlineData(_) if role == MODEL_ROLE => unsupported(
            model,
            &what,
            "A model turn carries only text and function calls.",
        ),
        PartData::InlineData(_) => unsupported(
            model,
            &what,
            "A chat completion carries only images: send the content as text, or as an image (`image/*`).",
        ),
        PartData::FileData { .. } => unsupported(
            model,
            &what,
            "Send the data inline as `inlineData` (an image) or as text.",
        ),
        PartData::FunctionCall(_) => GeminiError::invalid_argument(format!(
            "{place} has the role '{role}' and carries a `functionCall` part, but only the model calls functions. Send function calls in a turn with the role `model`."
        )),
        PartData::FunctionResponse(_) => GeminiError::invalid_argument(format!(
            "{place} has the role '{role}' and carries a `functionResponse` part. Send function responses in a turn with the role `user`."
        )),
        PartData::ExecutableCode | PartData::CodeExecutionResult => unsupported(
            model,
            &what,
            "A chat completion cannot run the `codeExecution` tool: remove the part.",
        ),
        PartData::Skip | PartData::Text(_) | PartData::Other(_) => {
            unsupported(model, &what, "Remove the part.")
        }
    }
}

/// Appends the turn `contents[index]` to `messages`, as the chat messages that carry it.
fn append_content(
    model: &str,
    index: usize,
    content: Content,
    calls: &mut Calls,
    messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), GeminiError> {
    let place = format!("`contents[{index}]`");
    match content.role.as_deref() {
        None | Some("" | USER_ROLE | FUNCTION_ROLE) => {
            append_user_turn(model, &place, content.parts, calls, messages)
        }
        Some(MODEL_ROLE) => append_model_turn(model, &place, content.parts, calls, messages),
        Some(other) => Err(GeminiError::invalid_argument(format!(
            "{place} has the role '{other}'. Use the role `user` or `model`."
        ))),
    }
}

/// A user's turn as chat messages: a tool message for each function response, first, as the chat
/// completion pairs them with the model's function calls; then a user message with the rest, the
/// images the functions returned first, since a tool message carries only text.
fn append_user_turn(
    model: &str,
    place: &str,
    parts: Vec<Part>,
    calls: &mut Calls,
    messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), GeminiError> {
    let mut tool_messages = Vec::new();
    let mut tool_images = Vec::new();
    let mut user_parts = Vec::new();
    for part in parts {
        match part_data(place, part)? {
            PartData::Skip => {}
            PartData::Text(text) => push_text(&mut user_parts, text),
            PartData::InlineData(blob) if is_image(&blob.mime_type) => {
                user_parts.push(image_part(blob));
            }
            PartData::FunctionResponse(FunctionResponse {
                id,
                name,
                response,
                parts,
            }) => {
                let tool_call_id = calls.answer(place, id, &name)?;
                for part in parts {
                    tool_images.push(function_response_image(model, place, &name, part)?);
                }
                tool_messages.push(ChatCompletionRequestMessage::Tool(
                    ChatCompletionRequestToolMessage {
                        content: ChatCompletionRequestToolMessageContent::Text(
                            response.to_string(),
                        ),
                        tool_call_id,
                    },
                ));
            }
            other => return Err(refused(model, place, &other, USER_ROLE)),
        }
    }
    messages.append(&mut tool_messages);
    tool_images.append(&mut user_parts);
    if !tool_images.is_empty() {
        messages.push(ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessage {
                content: user_content(tool_images),
                name: None,
            },
        ));
    }
    Ok(())
}

/// A model's turn as one assistant message: its text, and its function calls. Nothing is appended
/// when nothing is left, as when the turn carried only reasoning.
#[expect(deprecated)] // `function_call` has no non-deprecated way to be left unset.
fn append_model_turn(
    model: &str,
    place: &str,
    parts: Vec<Part>,
    calls: &mut Calls,
    messages: &mut Vec<ChatCompletionRequestMessage>,
) -> Result<(), GeminiError> {
    calls.start_turn();
    let mut text: Option<String> = None;
    let mut tool_calls = Vec::new();
    for part in parts {
        match part_data(place, part)? {
            PartData::Skip => {}
            PartData::Text(part) => {
                if !part.is_empty() {
                    text.get_or_insert_default().push_str(&part);
                }
            }
            PartData::FunctionCall(call) => {
                let id = calls.call(call.id, &call.name);
                tool_calls.push(ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id,
                        function: ChatFunctionCall {
                            name: call.name,
                            arguments: call.args.unwrap_or_else(|| json!({})).to_string(),
                        },
                    },
                ));
            }
            other => return Err(refused(model, place, &other, MODEL_ROLE)),
        }
    }
    if text.is_none() && tool_calls.is_empty() {
        return Ok(());
    }
    messages.push(ChatCompletionRequestMessage::Assistant(
        ChatCompletionRequestAssistantMessage {
            content: text.map(ChatCompletionRequestAssistantMessageContent::Text),
            refusal: None,
            name: None,
            audio: None,
            tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
            function_call: None,
        },
    ));
    Ok(())
}

/// Appends `text` to the text part that ends `parts`, or as a part of its own after anything
/// else. Gemini clients split a turn's text into parts as it was streamed, and join it back
/// without a separator.
fn push_text(parts: &mut Vec<ChatCompletionRequestUserMessageContentPart>, text: String) {
    if text.is_empty() {
        return;
    }
    if let Some(ChatCompletionRequestUserMessageContentPart::Text(last)) = parts.last_mut() {
        last.text.push_str(&text);
    } else {
        parts.push(ChatCompletionRequestUserMessageContentPart::Text(
            ChatCompletionRequestMessageContentPartText { text },
        ));
    }
}

/// One text part as text, so that every server reads it; anything else as parts.
fn user_content(
    mut parts: Vec<ChatCompletionRequestUserMessageContentPart>,
) -> ChatCompletionRequestUserMessageContent {
    if let [ChatCompletionRequestUserMessageContentPart::Text(_)] = parts.as_slice()
        && let Some(ChatCompletionRequestUserMessageContentPart::Text(text)) = parts.pop()
    {
        return ChatCompletionRequestUserMessageContent::Text(text.text);
    }
    ChatCompletionRequestUserMessageContent::Array(parts)
}

fn is_image(mime_type: &str) -> bool {
    mime_type
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("image/"))
}

/// An image as a data URI.
fn image_part(blob: Blob) -> ChatCompletionRequestUserMessageContentPart {
    ChatCompletionRequestUserMessageContentPart::ImageUrl(
        ChatCompletionRequestMessageContentPartImage {
            image_url: ImageUrl {
                url: format!(
                    "data:{};base64,{}",
                    blob.mime_type,
                    standard_base64(blob.data)
                ),
                detail: None,
            },
        },
    )
}

/// `data` in the standard base64 alphabet, padded, as a data URI needs it. Google's parser also
/// reads the URL-safe alphabet, and base64 without its padding.
fn standard_base64(data: String) -> String {
    if !data.contains(['-', '_'])
        && !data.contains(|c: char| c.is_ascii_whitespace())
        && data.len().is_multiple_of(4)
    {
        return data;
    }
    let mut standard: String = data
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            c => c,
        })
        .collect();
    while !standard.len().is_multiple_of(4) {
        standard.push('=');
    }
    standard
}

/// An image a function returned, for the user message after the tool messages.
fn function_response_image(
    model: &str,
    place: &str,
    function: &str,
    part: FunctionResponsePart,
) -> Result<ChatCompletionRequestUserMessageContentPart, GeminiError> {
    match part {
        FunctionResponsePart {
            inline_data: Some(blob),
            file_data: None,
        } if is_image(&blob.mime_type) => Ok(image_part(blob)),
        FunctionResponsePart {
            inline_data: Some(blob),
            file_data: None,
        } => Err(unsupported(
            model,
            &format!(
                "inline data of type '{}' in the response of '{function}' in {place}",
                blob.mime_type
            ),
            "A chat completion carries only images: return the content in `response`, or as an image (`image/*`).",
        )),
        FunctionResponsePart {
            file_data: Some(file),
            ..
        } => Err(unsupported(
            model,
            &format!(
                "data by URI ('{}') in the response of '{function}' in {place}",
                file.file_uri
            ),
            "Return the data inline as `inlineData` (an image), or in `response`.",
        )),
        FunctionResponsePart {
            inline_data: None,
            file_data: None,
        } => Err(GeminiError::invalid_argument(format!(
            "A part of the response of '{function}' in {place} carries no data. Send `inlineData` in each part, or remove the part."
        ))),
    }
}

/// The function calls of the latest model turn, which the function responses of the user turn
/// after it answer.
#[derive(Debug, Default)]
struct Calls {
    turn: Vec<PendingCall>,
    /// The IDs generated so far for calls that came without one.
    generated: usize,
}

#[derive(Debug)]
struct PendingCall {
    id: String,
    name: String,
    answered: bool,
}

impl Calls {
    fn start_turn(&mut self) {
        self.turn.clear();
    }

    /// The ID of a call of the model turn: its own, or `call_<n>`, stable within the request.
    fn call(&mut self, id: Option<String>, name: &str) -> String {
        let id = id.filter(|id| !id.is_empty()).unwrap_or_else(|| {
            self.generated += 1;
            format!("call_{}", self.generated)
        });
        self.turn.push(PendingCall {
            id: id.clone(),
            name: name.to_string(),
            answered: false,
        });
        id
    }

    /// The ID of the call a function response answers: the unanswered call with its `id`, or
    /// else the first unanswered call to the function it names.
    fn answer(
        &mut self,
        place: &str,
        id: Option<String>,
        name: &str,
    ) -> Result<String, GeminiError> {
        let by_id = id.filter(|id| !id.is_empty()).and_then(|id| {
            self.turn
                .iter()
                .position(|call| !call.answered && call.id == id)
        });
        let index = by_id.or_else(|| {
            self.turn
                .iter()
                .position(|call| !call.answered && call.name == name)
        });
        match index.and_then(|index| self.turn.get_mut(index)) {
            Some(call) => {
                call.answered = true;
                Ok(call.id.clone())
            }
            None => Err(GeminiError::invalid_argument(format!(
                "The `functionResponse` of '{name}' in {place} answers none of the `functionCall` parts of the model turn before it. Send each function response in the user turn right after the model turn that called the function, with the call's `id` or the function's `name`."
            ))),
        }
    }
}

/// The declared functions as chat tools, and how the model may call them. Tools Google runs are
/// left out: a chat completion cannot run them.
fn chat_tools(
    tools: Vec<Tool>,
    config: Option<FunctionCallingConfig>,
) -> Result<
    (
        Option<Vec<ChatCompletionTools>>,
        Option<ChatCompletionToolChoiceOption>,
    ),
    GeminiError,
> {
    let FunctionCallingConfig {
        mode,
        allowed_function_names: allowed,
    } = config.unwrap_or_default();
    let mut declarations: Vec<FunctionDeclaration> = tools
        .into_iter()
        .flat_map(|tool| tool.function_declarations)
        .collect();
    if !allowed.is_empty() {
        if let Some(name) = allowed.iter().find(|name| {
            !declarations
                .iter()
                .any(|declaration| &declaration.name == *name)
        }) {
            return Err(GeminiError::invalid_argument(format!(
                "`allowedFunctionNames` names '{name}', which no function declaration in `tools` declares. Declare the function, or remove it from `allowedFunctionNames`."
            )));
        }
        declarations.retain(|declaration| allowed.contains(&declaration.name));
    }
    if declarations.is_empty() {
        if mode == Some(FunctionCallingMode::Any) {
            return Err(GeminiError::invalid_argument(
                "`functionCallingConfig` has the mode ANY, which has the model call a function, but `tools` declares no function. Declare functions in `tools`, or use the mode AUTO.",
            ));
        }
        return Ok((None, None));
    }
    let tool_choice = match mode {
        None | Some(FunctionCallingMode::ModeUnspecified) => None,
        // VALIDATED is AUTO with calls held to their schemas, which a chat model's function calls
        // are as far as it can hold them.
        Some(FunctionCallingMode::Auto | FunctionCallingMode::Validated) => Some(
            ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto),
        ),
        Some(FunctionCallingMode::None) => Some(ChatCompletionToolChoiceOption::Mode(
            ToolChoiceOptions::None,
        )),
        Some(FunctionCallingMode::Any) => Some(match allowed.as_slice() {
            [name] => ChatCompletionToolChoiceOption::Function(ChatCompletionNamedToolChoice {
                function: FunctionName { name: name.clone() },
            }),
            _ => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required),
        }),
    };
    let tools = declarations
        .into_iter()
        .map(chat_tool)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((Some(tools), tool_choice))
}

fn chat_tool(declaration: FunctionDeclaration) -> Result<ChatCompletionTools, GeminiError> {
    let FunctionDeclaration {
        name,
        description,
        parameters,
        parameters_json_schema,
        ..
    } = declaration;
    let parameters = match (parameters_json_schema, parameters) {
        (Some(schema), _) => Some(schema),
        (None, Some(schema)) => Some(json_schema(schema, "parameters").map_err(|error| {
            error.refusal(&format!("the `parameters` of the function '{name}'"))
        })?),
        (None, None) => None,
    };
    Ok(ChatCompletionTools::Function(ChatCompletionTool {
        function: FunctionObject {
            name,
            description,
            parameters,
            strict: None,
        },
    }))
}

/// The output format `responseMimeType` and its schema ask for.
fn response_format(
    model: &str,
    mime_type: Option<String>,
    schema: Option<Value>,
    json_schema_value: Option<Value>,
) -> Result<Option<ResponseFormat>, GeminiError> {
    let mime_type = mime_type
        .map(|mime_type| mime_type.trim().to_ascii_lowercase())
        .filter(|mime_type| !mime_type.is_empty());
    match mime_type.as_deref() {
        Some("application/json") => {
            let schema = match (json_schema_value, schema) {
                (Some(schema), _) => Some(schema),
                (None, Some(schema)) => Some(
                    json_schema(schema, "generationConfig.responseSchema")
                        .map_err(|error| error.refusal("`generationConfig.responseSchema`"))?,
                ),
                (None, None) => None,
            };
            // `strict` is left unset: OpenAI's strict mode holds a schema to rules (every property
            // required, no additional properties) that a Gemini schema need not follow.
            Ok(Some(schema.map_or(ResponseFormat::JsonObject, |schema| {
                ResponseFormat::JsonSchema {
                    json_schema: ResponseFormatJsonSchema {
                        description: None,
                        name: RESPONSE_SCHEMA_NAME.to_string(),
                        schema,
                        strict: None,
                    },
                }
            })))
        }
        None | Some("text/plain") => {
            let field = if json_schema_value.is_some() {
                Some("responseJsonSchema")
            } else if schema.is_some() {
                Some("responseSchema")
            } else {
                None
            };
            match field {
                Some(field) => Err(GeminiError::invalid_argument(format!(
                    "`generationConfig.{field}` needs the `responseMimeType` 'application/json'. Set `responseMimeType` to 'application/json', or remove `{field}`."
                ))),
                None => Ok(None),
            }
        }
        Some(other) => Err(unsupported(
            model,
            &format!("the response MIME type '{other}'"),
            "Use the `responseMimeType` 'application/json' or 'text/plain'.",
        )),
    }
}

/// Why a Schema cannot be converted: the keyword at fault, and what is wrong with it.
#[derive(Debug)]
pub(crate) struct SchemaError {
    at: String,
    problem: String,
}

impl SchemaError {
    fn new(at: impl Into<String>, problem: impl Into<String>) -> Self {
        Self {
            at: at.into(),
            problem: problem.into(),
        }
    }

    /// The refusal of `what`, a Schema of the request, for this error.
    fn refusal(self, what: &str) -> GeminiError {
        GeminiError::invalid_argument(format!(
            "Failed to read {what} as a Schema: `{}` {}. See: {SCHEMA_DOCS}",
            self.at, self.problem
        ))
    }
}

/// `schema`, a Gemini OpenAPI-subset Schema, as JSON Schema. `at` names `schema` in the request,
/// for errors.
///
/// Types become lower case (`OBJECT` to `object`), and `nullable: true` a type union with
/// `null` (`["string", "null"]`), with `null` added to an `enum` or an `anyOf`. `properties`,
/// `items` and `anyOf` are converted in turn, and every other keyword is kept, but for what JSON
/// Schema has no place for: `propertyOrdering`, and the `format` `enum`, which marks an enum
/// string. A single `example` becomes `examples`, and a count written as a string (`"5"`), as
/// int64 fields are in Google's JSON, a number. Keywords written in snake case take the
/// camelCase JSON Schema names them.
pub(crate) fn json_schema(schema: Value, at: &str) -> Result<Value, SchemaError> {
    let Value::Object(fields) = schema else {
        return Err(SchemaError::new(
            at,
            format!("is {schema}, not a Schema object"),
        ));
    };
    let nullable = fields.get("nullable").and_then(Value::as_bool) == Some(true);
    let mut converted = Map::with_capacity(fields.len());
    for (keyword, value) in fields {
        let keyword = match keyword.as_str() {
            "any_of" => "anyOf".to_string(),
            "min_items" => "minItems".to_string(),
            "max_items" => "maxItems".to_string(),
            "min_length" => "minLength".to_string(),
            "max_length" => "maxLength".to_string(),
            "min_properties" => "minProperties".to_string(),
            "max_properties" => "maxProperties".to_string(),
            "property_ordering" => "propertyOrdering".to_string(),
            _ => keyword,
        };
        match keyword.as_str() {
            "type" => {
                if let Some(json_type) = json_type(&value, at)? {
                    converted.insert(keyword, Value::String(json_type.to_string()));
                }
            }
            "nullable" | "propertyOrdering" => {}
            "format" if value.as_str() == Some("enum") => {}
            "properties" => {
                let Value::Object(properties) = value else {
                    return Err(SchemaError::new(
                        format!("{at}.properties"),
                        format!("is {value}, not an object of Schemas"),
                    ));
                };
                let mut converted_properties = Map::with_capacity(properties.len());
                for (name, property) in properties {
                    let property = json_schema(property, &format!("{at}.properties.{name}"))?;
                    converted_properties.insert(name, property);
                }
                converted.insert(keyword, Value::Object(converted_properties));
            }
            "items" => {
                let items = json_schema(value, &format!("{at}.items"))?;
                converted.insert(keyword, items);
            }
            "anyOf" => {
                let Value::Array(options) = value else {
                    return Err(SchemaError::new(
                        format!("{at}.anyOf"),
                        format!("is {value}, not an array of Schemas"),
                    ));
                };
                let options = options
                    .into_iter()
                    .enumerate()
                    .map(|(index, option)| json_schema(option, &format!("{at}.anyOf[{index}]")))
                    .collect::<Result<Vec<_>, _>>()?;
                converted.insert(keyword, Value::Array(options));
            }
            "minItems" | "maxItems" | "minLength" | "maxLength" | "minProperties"
            | "maxProperties" => {
                let count = count(&value, &format!("{at}.{keyword}"))?;
                converted.insert(keyword, count);
            }
            "example" => {
                converted.insert("examples".to_string(), Value::Array(vec![value]));
            }
            _ => {
                converted.insert(keyword, value);
            }
        }
    }
    if nullable {
        allow_null(&mut converted);
    }
    Ok(Value::Object(converted))
}

/// The JSON Schema type a Schema `type` names; `None` for `TYPE_UNSPECIFIED`. Read in any case,
/// as Google reads it.
fn json_type(value: &Value, at: &str) -> Result<Option<&'static str>, SchemaError> {
    let name = value.as_str().map(str::to_ascii_uppercase);
    match name.as_deref() {
        Some("TYPE_UNSPECIFIED") => Ok(None),
        Some("STRING") => Ok(Some("string")),
        Some("NUMBER") => Ok(Some("number")),
        Some("INTEGER") => Ok(Some("integer")),
        Some("BOOLEAN") => Ok(Some("boolean")),
        Some("ARRAY") => Ok(Some("array")),
        Some("OBJECT") => Ok(Some("object")),
        Some("NULL") => Ok(Some("null")),
        _ => Err(SchemaError::new(
            format!("{at}.type"),
            format!(
                "is {value}, which is not one of STRING, NUMBER, INTEGER, BOOLEAN, ARRAY, OBJECT and NULL"
            ),
        )),
    }
}

/// A count, which Google's JSON writes as a string (int64), as a number.
fn count(value: &Value, at: &str) -> Result<Value, SchemaError> {
    match value {
        Value::Number(number) if number.is_u64() => Ok(value.clone()),
        Value::String(text) => text
            .parse::<u64>()
            .map(Value::from)
            .map_err(|_| SchemaError::new(at, format!("is {value}, not a count"))),
        _ => Err(SchemaError::new(at, format!("is {value}, not a count"))),
    }
}

/// Lets the converted schema `fields` accept `null`, as `nullable: true` does.
fn allow_null(fields: &mut Map<String, Value>) {
    let null_type = Value::String("null".to_string());
    match fields.get("type") {
        Some(Value::String(name)) if name != "null" => {
            let union = Value::Array(vec![Value::String(name.clone()), null_type]);
            fields.insert("type".to_string(), union);
        }
        Some(_) => {}
        None => {
            if let Some(Value::Array(options)) = fields.get_mut("anyOf") {
                let null_option = json!({"type": "null"});
                if !options.contains(&null_option) {
                    options.push(null_option);
                }
            }
        }
    }
    if let Some(Value::Array(values)) = fields.get_mut("enum")
        && !values.contains(&Value::Null)
    {
        values.push(Value::Null);
    }
}
