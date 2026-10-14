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

//! Keeps the tool steps Spice runs during a chat completion, so that later turns of the same
//! conversation send them to the model again.
//!
//! When the model calls a Spice tool, Spice runs it and calls the model again with the
//! assistant tool-call message and the tool results appended. The client only receives the
//! final answer, so it cannot send those steps back on its next turn, and the model would
//! never see them again. This store keeps them, keyed by the caller and the conversation as
//! the client sees it, and puts them back in place when the client continues the conversation.
//!
//! ```text
//! turn 1  client sends  [A]
//!         model sees    [A, B, C]           B: tool call, C: tool result
//!         client gets   D                   store: key(A, D) -> {B, C before index 1}
//! turn 2  client sends  [A, D, E]
//!         model sees    [A, B, C, D, E]
//!         client gets   F                   store: key(A, D, E, F) -> {B, C before index 1}
//!                                                  key(A, D) is removed
//! ```
//!
//! Only the latest point of a conversation is kept: an entry is removed when a turn that
//! used it succeeds. A client that sends a turn again after it succeeded (to regenerate an
//! answer, say) gets no steps restored for it.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_openai::types::chat::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestAssistantMessageContentPart, ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestDeveloperMessageContentPart, ChatCompletionRequestMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestToolMessageContentPart,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FileObject,
};
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use runtime_auth::AuthRequestContext;
use runtime_request_context::RequestContext;
use sha2::{Digest, Sha256};

/// Bound on the summed weight of every stored entry (see [`ToolTrace::weight`]). Entries do not
/// expire: a restored step is what the model saw, however old. At the bound, the least recently
/// used entry is evicted.
const DEFAULT_MAX_CAPACITY_BYTES: u64 = 128 * 1024 * 1024;

/// Whose conversation a request continues. Steps stored for one caller are never restored for
/// another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallerScope {
    /// No authenticated principal, or one without a stable id.
    Anonymous,
    /// An authenticated principal, by its `stable_id`.
    Principal(String),
}

impl CallerScope {
    /// The scope of the request `context` belongs to.
    pub(crate) fn of(context: &RequestContext) -> Self {
        match context
            .auth_principal()
            .and_then(|principal| principal.stable_id())
        {
            Some(id) => Self::Principal(id.into_owned()),
            None => Self::Anonymous,
        }
    }
}

/// One point in one conversation: a SHA-256 digest of the caller's scope and the client-visible
/// messages up to that point, in canonical form (see [`encode_message`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ToolTraceKey([u8; 32]);

/// Messages Spice sent the model that the client never received, and where they go in the
/// messages the client sends.
#[derive(Debug, Clone)]
struct HiddenSteps {
    /// The index, in the client-visible messages, of the message these go before.
    before_visible_index: usize,
    messages: Arc<[ChatCompletionRequestMessage]>,
}

/// Every hidden step of a conversation so far, in conversation order.
#[derive(Debug, Clone, Default)]
struct ToolTrace(Vec<HiddenSteps>);

impl ToolTrace {
    /// Appends `messages`, to go before client-visible message `before_visible_index`.
    ///
    /// Each turn appends at the client's message count for that turn, which only grows, so
    /// steps stay in conversation order.
    fn push(&mut self, before_visible_index: usize, messages: Vec<ChatCompletionRequestMessage>) {
        if messages.is_empty() {
            return;
        }
        self.0.push(HiddenSteps {
            before_visible_index,
            messages: messages.into(),
        });
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn message_count(&self) -> usize {
        self.0.iter().map(|step| step.messages.len()).sum()
    }

    /// Inserts each step before its client-visible message. A step whose index is past the end
    /// of `visible` goes last.
    fn restore(
        &self,
        visible: Vec<ChatCompletionRequestMessage>,
    ) -> Vec<ChatCompletionRequestMessage> {
        let mut restored = Vec::with_capacity(visible.len() + self.message_count());
        let mut steps = self.0.iter().peekable();
        for (index, message) in visible.into_iter().enumerate() {
            while let Some(step) = steps.next_if(|step| step.before_visible_index <= index) {
                restored.extend(step.messages.iter().cloned());
            }
            restored.push(message);
        }
        for step in steps {
            restored.extend(step.messages.iter().cloned());
        }
        restored
    }

    /// An estimate, in bytes, of the memory the trace holds: the length of its canonical
    /// encoding.
    fn weight(&self) -> u32 {
        let mut len = EncodedLen::default();
        for message in self.0.iter().flat_map(|step| step.messages.iter()) {
            encode_message(&mut len, message);
        }
        u32::try_from(len.0).unwrap_or(u32::MAX)
    }
}

/// The trace restored into a turn, and the key it was stored under.
struct RestoredTrace {
    key: ToolTraceKey,
    trace: ToolTrace,
}

/// A turn on its way to the model: what [`ToolTraceStore::finish_turn`] needs once it succeeds.
pub(crate) struct PendingTurn {
    /// The digest of the caller's scope and every message the client sent.
    digest: ConversationDigest,
    /// How many messages the client sent.
    client_message_count: usize,
    restored: Option<RestoredTrace>,
}

/// The hidden steps of recent conversations, one entry per conversation, keyed at the
/// conversation's latest assistant message.
pub(crate) struct ToolTraceStore {
    traces: Cache<ToolTraceKey, ToolTrace>,
}

impl Default for ToolTraceStore {
    fn default() -> Self {
        Self {
            traces: Cache::builder()
                .max_capacity(DEFAULT_MAX_CAPACITY_BYTES)
                .weigher(|_key: &ToolTraceKey, trace: &ToolTrace| trace.weight())
                // Each entry is read at most once, by the conversation's next turn, so recency
                // is the only signal worth keeping.
                .eviction_policy(EvictionPolicy::lru())
                .build(),
        }
    }
}

impl ToolTraceStore {
    /// Restores the hidden steps of earlier turns into `messages`, the messages a client sent.
    ///
    /// The conversation is looked up at its last assistant message: the one Spice returned on
    /// the previous turn. Returns the messages to send the model (`messages` unchanged when
    /// nothing is stored), and the turn to pass to [`Self::finish_turn`] once it succeeds.
    pub(crate) async fn begin_turn(
        &self,
        scope: &CallerScope,
        messages: Vec<ChatCompletionRequestMessage>,
    ) -> (Vec<ChatCompletionRequestMessage>, PendingTurn) {
        let last_assistant = messages
            .iter()
            .rposition(|message| matches!(message, ChatCompletionRequestMessage::Assistant(_)));

        let mut digest = ConversationDigest::new(scope);
        let mut lookup = None;
        for (index, message) in messages.iter().enumerate() {
            digest.push(message);
            if Some(index) == last_assistant {
                lookup = Some(digest.key());
            }
        }

        let restored = match lookup {
            Some(key) => self
                .traces
                .get(&key)
                .await
                .map(|trace| RestoredTrace { key, trace }),
            None => None,
        };

        let client_message_count = messages.len();
        let messages = match &restored {
            Some(RestoredTrace { trace, .. }) => {
                tracing::debug!(
                    "Restored {} tool-use messages from earlier turns of this conversation",
                    trace.message_count()
                );
                trace.restore(messages)
            }
            None => messages,
        };

        (
            messages,
            PendingTurn {
                digest,
                client_message_count,
                restored,
            },
        )
    }

    /// Records a turn that succeeded.
    ///
    /// `hidden` holds the messages this turn's tool rounds added for the model, and `returned`
    /// is the assistant message the client received, as the client sends it back. The trace
    /// restored into the turn, plus `hidden`, is stored at the conversation's new latest point,
    /// even when `hidden` is empty, so that the next turn still restores earlier steps. The
    /// entry the turn restored from is then removed.
    pub(crate) async fn finish_turn(
        &self,
        turn: PendingTurn,
        hidden: Vec<ChatCompletionRequestMessage>,
        returned: &ChatCompletionRequestMessage,
    ) {
        let PendingTurn {
            mut digest,
            client_message_count,
            restored,
        } = turn;
        let (used_key, mut trace) = match restored {
            Some(RestoredTrace { key, trace }) => (Some(key), trace),
            None => (None, ToolTrace::default()),
        };

        // The returned message is the next turn's message at `client_message_count`.
        trace.push(client_message_count, hidden);
        if !trace.is_empty() {
            digest.push(returned);
            self.traces.insert(digest.key(), trace).await;
        }
        if let Some(used_key) = used_key {
            self.traces.invalidate(&used_key).await;
        }
    }
}

/// The assistant message a client sends back for the first choice of `response`.
pub(crate) fn returned_message(
    response: &CreateChatCompletionResponse,
) -> Option<ChatCompletionRequestMessage> {
    response.choices.first().map(|choice| {
        assistant_message(
            choice.message.content.clone(),
            choice.message.tool_calls.clone(),
        )
    })
}

/// The assistant message a client rebuilds from the chunks of a streamed completion: the text
/// of choice 0, and its tool calls merged by index.
#[derive(Debug, Default)]
pub(crate) struct StreamedReply {
    content: String,
    tool_calls: BTreeMap<u32, ChatCompletionMessageToolCall>,
}

impl StreamedReply {
    /// Adds what `chunk` sends the client for choice 0.
    pub(crate) fn observe(&mut self, chunk: &CreateChatCompletionStreamResponse) {
        for choice in chunk.choices.iter().filter(|choice| choice.index == 0) {
            if let Some(text) = &choice.delta.content {
                self.content.push_str(text);
            }
            for call in choice.delta.tool_calls.iter().flatten() {
                let merged = self.tool_calls.entry(call.index).or_default();
                if merged.id.is_empty()
                    && let Some(id) = &call.id
                {
                    merged.id.clone_from(id);
                }
                if let Some(function) = &call.function {
                    if merged.function.name.is_empty()
                        && let Some(name) = &function.name
                    {
                        merged.function.name.clone_from(name);
                    }
                    if let Some(arguments) = &function.arguments {
                        merged.function.arguments.push_str(arguments);
                    }
                }
            }
        }
    }

    /// The message, as the client sends it back.
    pub(crate) fn into_message(self) -> ChatCompletionRequestMessage {
        let Self {
            content,
            tool_calls,
        } = self;
        let tool_calls = (!tool_calls.is_empty()).then(|| {
            tool_calls
                .into_values()
                .map(ChatCompletionMessageToolCalls::Function)
                .collect::<Vec<_>>()
        });
        assistant_message((!content.is_empty()).then_some(content), tool_calls)
    }
}

#[expect(
    deprecated,
    reason = "`function_call` is deprecated; a returned message never carries one"
)]
fn assistant_message(
    content: Option<String>,
    tool_calls: Option<Vec<ChatCompletionMessageToolCalls>>,
) -> ChatCompletionRequestMessage {
    ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
        content: content.map(ChatCompletionRequestAssistantMessageContent::Text),
        refusal: None,
        name: None,
        audio: None,
        tool_calls,
        function_call: None,
    })
}

/// Tags of the canonical message encoding. A list item starts with a non-zero tag, and
/// `END` closes a list.
mod tag {
    pub(super) const END: u8 = 0;
    pub(super) const ABSENT: u8 = 1;
    pub(super) const PRESENT: u8 = 2;
    pub(super) const ANONYMOUS: u8 = 3;
    pub(super) const PRINCIPAL: u8 = 4;
    pub(super) const DEVELOPER: u8 = 5;
    pub(super) const SYSTEM: u8 = 6;
    pub(super) const USER: u8 = 7;
    pub(super) const ASSISTANT: u8 = 8;
    pub(super) const TOOL: u8 = 9;
    pub(super) const FUNCTION: u8 = 10;
    pub(super) const TEXT: u8 = 11;
    pub(super) const REFUSAL: u8 = 12;
    pub(super) const IMAGE_URL: u8 = 13;
    pub(super) const INPUT_AUDIO: u8 = 14;
    pub(super) const FILE: u8 = 15;
    pub(super) const FUNCTION_CALL: u8 = 16;
    pub(super) const CUSTOM_CALL: u8 = 17;
}

/// Receives a canonical encoding.
trait Encoder {
    /// Writes one tag byte.
    fn tag(&mut self, tag: u8);
    /// Writes `bytes`, prefixed with their length.
    fn bytes(&mut self, bytes: &[u8]);
}

/// A SHA-256 over a caller's scope and a conversation's messages, fed one message at a time.
#[derive(Clone)]
struct ConversationDigest(Sha256);

impl ConversationDigest {
    fn new(scope: &CallerScope) -> Self {
        let mut digest = Self(Sha256::new());
        match scope {
            CallerScope::Anonymous => digest.tag(tag::ANONYMOUS),
            CallerScope::Principal(id) => {
                digest.tag(tag::PRINCIPAL);
                digest.bytes(id.as_bytes());
            }
        }
        digest
    }

    fn push(&mut self, message: &ChatCompletionRequestMessage) {
        encode_message(self, message);
    }

    /// The key of the conversation as fed so far.
    fn key(&self) -> ToolTraceKey {
        ToolTraceKey(self.0.clone().finalize().into())
    }
}

impl Encoder for ConversationDigest {
    fn tag(&mut self, tag: u8) {
        self.0.update([tag]);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.0.update((bytes.len() as u64).to_le_bytes());
        self.0.update(bytes);
    }
}

/// The length, in bytes, of a canonical encoding.
#[derive(Default)]
struct EncodedLen(usize);

impl Encoder for EncodedLen {
    fn tag(&mut self, _tag: u8) {
        self.0 = self.0.saturating_add(1);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.0 = self
            .0
            .saturating_add(size_of::<u64>())
            .saturating_add(bytes.len());
    }
}

/// One piece of a message's content.
enum Part<'a> {
    Text(&'a str),
    Refusal(&'a str),
    ImageUrl(&'a str),
    InputAudio(&'a str),
    File(&'a FileObject),
}

/// Writes `message` in a canonical form, so that the forms a client may echo a message in
/// encode alike, and different message lists never write the same bytes.
///
/// - Every value is tagged or length-prefixed, so one message's encoding is never a prefix of
///   another's.
/// - Content is a list of parts: a string and an array of one text part with the same text
///   encode alike, and empty text parts are left out, so a missing, `null` or empty content
///   encode alike.
/// - A missing, `null` or empty `name`, and missing, `null` or empty `tool_calls`, encode
///   alike.
/// - Fields a client may drop when it echoes a message are left out: an assistant's
///   `refusal` field, `audio` and the deprecated `function_call`, and an image's `detail`.
fn encode_message(e: &mut impl Encoder, message: &ChatCompletionRequestMessage) {
    match message {
        ChatCompletionRequestMessage::Developer(m) => {
            e.tag(tag::DEVELOPER);
            encode_optional(e, m.name.as_deref());
            match &m.content {
                ChatCompletionRequestDeveloperMessageContent::Text(text) => {
                    encode_parts(e, [Part::Text(text)]);
                }
                ChatCompletionRequestDeveloperMessageContent::Array(parts) => encode_parts(
                    e,
                    parts.iter().map(
                        |ChatCompletionRequestDeveloperMessageContentPart::Text(part)| {
                            Part::Text(&part.text)
                        },
                    ),
                ),
            }
        }
        ChatCompletionRequestMessage::System(m) => {
            e.tag(tag::SYSTEM);
            encode_optional(e, m.name.as_deref());
            match &m.content {
                ChatCompletionRequestSystemMessageContent::Text(text) => {
                    encode_parts(e, [Part::Text(text)]);
                }
                ChatCompletionRequestSystemMessageContent::Array(parts) => encode_parts(
                    e,
                    parts.iter().map(
                        |ChatCompletionRequestSystemMessageContentPart::Text(part)| {
                            Part::Text(&part.text)
                        },
                    ),
                ),
            }
        }
        ChatCompletionRequestMessage::User(m) => {
            e.tag(tag::USER);
            encode_optional(e, m.name.as_deref());
            match &m.content {
                ChatCompletionRequestUserMessageContent::Text(text) => {
                    encode_parts(e, [Part::Text(text)]);
                }
                ChatCompletionRequestUserMessageContent::Array(parts) => {
                    encode_parts(
                        e,
                        parts.iter().map(|part| match part {
                            ChatCompletionRequestUserMessageContentPart::Text(part) => {
                                Part::Text(&part.text)
                            }
                            ChatCompletionRequestUserMessageContentPart::ImageUrl(part) => {
                                Part::ImageUrl(&part.image_url.url)
                            }
                            ChatCompletionRequestUserMessageContentPart::InputAudio(part) => {
                                Part::InputAudio(&part.input_audio.data)
                            }
                            ChatCompletionRequestUserMessageContentPart::File(part) => {
                                Part::File(&part.file)
                            }
                        }),
                    );
                }
            }
        }
        ChatCompletionRequestMessage::Assistant(m) => {
            e.tag(tag::ASSISTANT);
            encode_optional(e, m.name.as_deref());
            match &m.content {
                None => encode_parts(e, std::iter::empty::<Part<'_>>()),
                Some(ChatCompletionRequestAssistantMessageContent::Text(text)) => {
                    encode_parts(e, [Part::Text(text)]);
                }
                Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => {
                    encode_parts(
                        e,
                        parts.iter().map(|part| match part {
                            ChatCompletionRequestAssistantMessageContentPart::Text(part) => {
                                Part::Text(&part.text)
                            }
                            ChatCompletionRequestAssistantMessageContentPart::Refusal(part) => {
                                Part::Refusal(&part.refusal)
                            }
                        }),
                    );
                }
            }
            for call in m.tool_calls.iter().flatten() {
                match call {
                    ChatCompletionMessageToolCalls::Function(call) => {
                        e.tag(tag::FUNCTION_CALL);
                        e.bytes(call.id.as_bytes());
                        e.bytes(call.function.name.as_bytes());
                        e.bytes(call.function.arguments.as_bytes());
                    }
                    ChatCompletionMessageToolCalls::Custom(call) => {
                        e.tag(tag::CUSTOM_CALL);
                        e.bytes(call.id.as_bytes());
                        e.bytes(call.custom_tool.name.as_bytes());
                        e.bytes(call.custom_tool.input.as_bytes());
                    }
                }
            }
            e.tag(tag::END);
        }
        ChatCompletionRequestMessage::Tool(m) => {
            e.tag(tag::TOOL);
            e.bytes(m.tool_call_id.as_bytes());
            match &m.content {
                ChatCompletionRequestToolMessageContent::Text(text) => {
                    encode_parts(e, [Part::Text(text)]);
                }
                ChatCompletionRequestToolMessageContent::Array(parts) => encode_parts(
                    e,
                    parts
                        .iter()
                        .map(|ChatCompletionRequestToolMessageContentPart::Text(part)| {
                            Part::Text(&part.text)
                        }),
                ),
            }
        }
        ChatCompletionRequestMessage::Function(m) => {
            e.tag(tag::FUNCTION);
            e.bytes(m.name.as_bytes());
            encode_parts(e, m.content.as_deref().map(Part::Text));
        }
    }
}

/// Writes `value`, encoding a missing value and an empty one alike.
fn encode_optional(e: &mut impl Encoder, value: Option<&str>) {
    match value.filter(|value| !value.is_empty()) {
        Some(value) => {
            e.tag(tag::PRESENT);
            e.bytes(value.as_bytes());
        }
        None => e.tag(tag::ABSENT),
    }
}

/// Writes `parts` as a list, leaving out empty text.
fn encode_parts<'a>(e: &mut impl Encoder, parts: impl IntoIterator<Item = Part<'a>>) {
    for part in parts {
        match part {
            Part::Text("") => {}
            Part::Text(text) => {
                e.tag(tag::TEXT);
                e.bytes(text.as_bytes());
            }
            Part::Refusal(refusal) => {
                e.tag(tag::REFUSAL);
                e.bytes(refusal.as_bytes());
            }
            Part::ImageUrl(url) => {
                e.tag(tag::IMAGE_URL);
                e.bytes(url.as_bytes());
            }
            Part::InputAudio(data) => {
                e.tag(tag::INPUT_AUDIO);
                e.bytes(data.as_bytes());
            }
            Part::File(file) => {
                e.tag(tag::FILE);
                encode_optional(e, file.file_data.as_deref());
                encode_optional(e, file.file_id.as_deref());
                encode_optional(e, file.filename.as_deref());
            }
        }
    }
    e.tag(tag::END);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::HashSet;

    fn message(value: Value) -> ChatCompletionRequestMessage {
        serde_json::from_value(value).expect("a valid message")
    }

    fn user(text: &str) -> ChatCompletionRequestMessage {
        message(json!({ "role": "user", "content": text }))
    }

    fn assistant(text: &str) -> ChatCompletionRequestMessage {
        message(json!({ "role": "assistant", "content": text }))
    }

    fn tool(call_id: &str, text: &str) -> ChatCompletionRequestMessage {
        message(json!({ "role": "tool", "tool_call_id": call_id, "content": text }))
    }

    fn assistant_calling(
        call_id: &str,
        name: &str,
        arguments: &str,
    ) -> ChatCompletionRequestMessage {
        message(json!({
            "role": "assistant",
            "tool_calls": [{
                "type": "function", "id": call_id,
                "function": { "name": name, "arguments": arguments },
            }],
        }))
    }

    fn key(scope: &CallerScope, messages: &[ChatCompletionRequestMessage]) -> ToolTraceKey {
        let mut digest = ConversationDigest::new(scope);
        for message in messages {
            digest.push(message);
        }
        digest.key()
    }

    fn anonymous_key(messages: &[ChatCompletionRequestMessage]) -> ToolTraceKey {
        key(&CallerScope::Anonymous, messages)
    }

    #[test]
    fn string_content_and_one_text_part_encode_alike() {
        for role in ["developer", "system", "user", "assistant", "tool"] {
            let mut string = json!({ "role": role, "content": "hello" });
            let mut parts =
                json!({ "role": role, "content": [{ "type": "text", "text": "hello" }] });
            if role == "tool" {
                string["tool_call_id"] = json!("call_1");
                parts["tool_call_id"] = json!("call_1");
            }
            assert_eq!(
                anonymous_key(&[message(string)]),
                anonymous_key(&[message(parts)]),
                "{role}"
            );
        }
    }

    #[test]
    fn absent_null_and_empty_fields_encode_alike() {
        let bare = assistant("D");
        let nulls = message(json!({
            "role": "assistant", "content": "D", "name": null, "refusal": null,
            "audio": null, "tool_calls": null, "function_call": null,
        }));
        let empty = message(json!({
            "role": "assistant", "content": "D", "name": "", "tool_calls": [],
        }));
        assert_eq!(anonymous_key(&[bare.clone()]), anonymous_key(&[nulls]));
        assert_eq!(anonymous_key(&[bare]), anonymous_key(&[empty]));

        // A tool-call-only turn, echoed without content, with `null`, `""` or `[]`.
        let calls = json!([{
            "type": "function", "id": "call_1",
            "function": { "name": "sql", "arguments": "{}" },
        }]);
        let without =
            anonymous_key(&[message(json!({ "role": "assistant", "tool_calls": calls }))]);
        for content in [json!(null), json!(""), json!([])] {
            assert_eq!(
                anonymous_key(&[message(
                    json!({ "role": "assistant", "content": content, "tool_calls": calls }),
                )]),
                without,
                "{content}"
            );
        }
    }

    #[test]
    fn different_conversations_get_different_keys() {
        let conversations = [
            vec![],
            vec![user("ab"), user("c")],
            vec![user("a"), user("bc")],
            vec![user("abc")],
            vec![message(
                json!({ "role": "user", "content": "abc", "name": "bob" }),
            )],
            vec![message(json!({ "role": "system", "content": "abc" }))],
            vec![assistant("abc")],
            vec![assistant("abc"), user("")],
            vec![tool("call_1", "abc")],
            vec![tool("call_2", "abc")],
            vec![assistant_calling("call_1", "sql", "{}")],
            vec![assistant_calling("call_2", "sql", "{}")],
            vec![assistant_calling("call_1", "search", "{}")],
            vec![assistant_calling(
                "call_1",
                "sql",
                r#"{"query":"select 1"}"#,
            )],
        ];
        let keys: HashSet<_> = conversations
            .iter()
            .map(|messages| anonymous_key(messages.as_slice()))
            .collect();
        assert_eq!(keys.len(), conversations.len());
    }

    #[test]
    fn each_caller_gets_its_own_keys() {
        let messages = [user("A"), assistant("D")];
        let keys: HashSet<_> = [
            CallerScope::Anonymous,
            CallerScope::Principal("apikey:alice".to_string()),
            CallerScope::Principal("apikey:bob".to_string()),
        ]
        .iter()
        .map(|scope| key(scope, &messages))
        .collect();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn restore_puts_each_step_before_its_visible_message() {
        let mut trace = ToolTrace::default();
        trace.push(
            1,
            vec![
                assistant_calling("call_0", "sql", "{}"),
                tool("call_0", "B"),
            ],
        );
        trace.push(3, vec![]);
        trace.push(
            3,
            vec![
                assistant_calling("call_2", "sql", "{}"),
                tool("call_2", "C"),
            ],
        );
        assert_eq!(trace.message_count(), 4);

        assert_eq!(
            trace.restore(vec![
                user("A"),
                assistant("D"),
                user("E"),
                assistant("F"),
                user("G")
            ]),
            vec![
                user("A"),
                assistant_calling("call_0", "sql", "{}"),
                tool("call_0", "B"),
                assistant("D"),
                user("E"),
                assistant_calling("call_2", "sql", "{}"),
                tool("call_2", "C"),
                assistant("F"),
                user("G"),
            ]
        );

        // A step past the end goes last.
        let mut trace = ToolTrace::default();
        trace.push(5, vec![tool("call_0", "B")]);
        assert_eq!(
            trace.restore(vec![user("A")]),
            vec![user("A"), tool("call_0", "B")]
        );
    }

    #[test]
    fn a_trace_weighs_its_encoded_text() {
        let mut trace = ToolTrace::default();
        assert_eq!(trace.weight(), 0);
        trace.push(1, vec![tool("call_0", &"x".repeat(1000))]);
        assert!(trace.weight() > 1000, "{}", trace.weight());
    }

    #[tokio::test]
    async fn a_finished_turn_is_restored_once_then_carried_forward() {
        let store = ToolTraceStore::default();
        let scope = CallerScope::Anonymous;
        let steps = vec![
            assistant_calling("call_0", "sql", "{}"),
            tool("call_0", "B"),
        ];

        // Turn 1: nothing to restore.
        let (messages, turn) = store.begin_turn(&scope, vec![user("A")]).await;
        assert_eq!(messages, vec![user("A")]);
        store
            .finish_turn(turn, steps.clone(), &assistant("D"))
            .await;

        // Turn 2 uses no tools: the steps move to the new latest point.
        let turn_2 = vec![user("A"), assistant("D"), user("E")];
        let (messages, turn) = store.begin_turn(&scope, turn_2.clone()).await;
        assert_eq!(
            messages,
            [&turn_2[..1], steps.as_slice(), &turn_2[1..]].concat()
        );
        store.finish_turn(turn, vec![], &assistant("F")).await;
        assert!(
            store
                .traces
                .get(&anonymous_key(&[user("A"), assistant("D")]))
                .await
                .is_none()
        );

        // Turn 3 still sees turn 1's steps.
        let turn_3 = vec![
            user("A"),
            assistant("D"),
            user("E"),
            assistant("F"),
            user("G"),
        ];
        let (messages, _turn) = store.begin_turn(&scope, turn_3.clone()).await;
        assert_eq!(
            messages,
            [&turn_3[..1], steps.as_slice(), &turn_3[1..]].concat()
        );
    }

    #[tokio::test]
    async fn a_turn_without_steps_stores_nothing() {
        let store = ToolTraceStore::default();
        let (_, turn) = store
            .begin_turn(&CallerScope::Anonymous, vec![user("A")])
            .await;
        store.finish_turn(turn, vec![], &assistant("D")).await;
        assert!(
            store
                .traces
                .get(&anonymous_key(&[user("A"), assistant("D")]))
                .await
                .is_none()
        );
    }

    #[test]
    fn a_streamed_reply_is_rebuilt_as_a_client_would() {
        let chunk = |choices: Value| -> CreateChatCompletionStreamResponse {
            serde_json::from_value(json!({
                "id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 0,
                "model": "scripted", "choices": choices,
            }))
            .expect("a valid chunk")
        };
        let mut reply = StreamedReply::default();
        for choices in [
            json!([{ "index": 0, "delta": { "role": "assistant", "content": "Let me " } }]),
            json!([{ "index": 1, "delta": { "content": "another choice" } }]),
            json!([{ "index": 0, "delta": { "content": "check." } }]),
            json!([{ "index": 0, "delta": { "tool_calls": [{
                "index": 0, "id": "call_1", "type": "function",
                "function": { "name": "lookup", "arguments": "{\"q\":" },
            }] } }]),
            json!([{ "index": 0, "delta": { "tool_calls": [{
                "index": 0, "function": { "arguments": "\"spice\"}" },
            }] } }]),
            json!([{ "index": 0, "delta": { "tool_calls": [{
                "index": 1, "id": "call_2", "type": "function",
                "function": { "name": "other", "arguments": "{}" },
            }] } }]),
            json!([{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }]),
        ] {
            reply.observe(&chunk(choices));
        }

        assert_eq!(
            reply.into_message(),
            message(json!({
                "role": "assistant",
                "content": "Let me check.",
                "tool_calls": [
                    {
                        "type": "function", "id": "call_1",
                        "function": { "name": "lookup", "arguments": "{\"q\":\"spice\"}" },
                    },
                    {
                        "type": "function", "id": "call_2",
                        "function": { "name": "other", "arguments": "{}" },
                    },
                ],
            }))
        );
    }

    #[test]
    fn a_returned_message_is_the_first_choice_as_a_request_message() {
        let response: CreateChatCompletionResponse = serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 0, "model": "scripted",
            "choices": [
                {
                    "index": 0, "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant", "content": "Calling.",
                        "tool_calls": [{
                            "type": "function", "id": "call_1",
                            "function": { "name": "lookup", "arguments": "{}" },
                        }],
                    },
                },
                { "index": 1, "message": { "role": "assistant", "content": "other" } },
            ],
        }))
        .expect("a valid response");

        assert_eq!(
            returned_message(&response),
            Some(message(json!({
                "role": "assistant",
                "content": "Calling.",
                "tool_calls": [{
                    "type": "function", "id": "call_1",
                    "function": { "name": "lookup", "arguments": "{}" },
                }],
            })))
        );
    }
}
