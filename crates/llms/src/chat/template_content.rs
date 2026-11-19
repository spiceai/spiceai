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

//! Which shapes of a message's `content` a chat template reads, for each role, and a
//! request's messages rewritten to one it reads in full.
//!
//! A request may send a message's content as a string or as an array of text parts, which
//! reach the template as `[{"type": "text", "text": …}]`. Templates differ in what they read,
//! and one template may differ by role: Qwen3.5's reads both, and renders a message's parts
//! one after another; Qwen3's reads only a string and fails on parts; Gemma 3's reads every
//! part of a user message but only the first of a system message; one that prints `content`
//! as it stands prints the array itself; and mistral.rs's vision pipelines read only a
//! message's first part. As llama-server does (`common/chat.cpp` and `common/jinja/caps.cpp`
//! in llama.cpp), the template is probed when the model loads, by rendering markers in each
//! shape, and a request's messages are rewritten to the shape the template reads in full:
//! parts joined with newlines for a template that reads only strings, a string as one text
//! part for a template that reads only parts, and each message as it came for a template that
//! reads both. Unlike llama-server, which probes a user message alone, each role is probed in
//! a conversation of its own, so a template that reads a system message's first part gets the
//! system message's parts joined instead of losing all but the first.
//!
//! A request may also send a system or developer message after the conversation has started,
//! as Claude Code sends its environment after the first user message. Templates differ here
//! too: Qwen3's renders such a message in place, Qwen3.5's renders only the system and
//! developer messages the conversation starts with and leaves later ones out, and others raise
//! `System message must be at the beginning`. The probe renders one after a user message, and
//! for a template that does not render it there with its text, the message reaches the
//! template as a user's: its text joined to the user message before it, after a blank line, or
//! as a user message of its own when the message before it is not a user's. Its text keeps its
//! place in the conversation, so the prompt before it is the one the same conversation renders
//! without it. llama-server probes only a system message that starts the conversation, and
//! sends a later one to the template as it came.

use either::Either;
use indexmap::IndexMap;
use mistralrs::MessageContent;
use serde_json::{Value, json};

/// Text no chat template writes itself, so a probe's prompt holds it only when the template
/// rendered the content it was given.
const MARKERS: [&str; 2] = ["SPICE-CONTENT-PROBE-ONE", "SPICE-CONTENT-PROBE-TWO"];

/// A key on each part of the parts probe that no chat template reads: a template that prints
/// the parts themselves, rather than their text, prints it as well.
const PROBE_KEY: &str = "spice_content_probe";

/// The roles probed, in the order [`RoleShapes`] holds them. Any other role's message is
/// rewritten as a user message is.
const ROLES: [&str; 5] = ["system", "developer", "user", "assistant", "tool"];
const USER: usize = 2;

/// The roles of [`ROLES`] that instruct the model, which a chat template may render only at the
/// start of a conversation: `ROLES[0]` and `ROLES[1]`.
const INSTRUCTIONS: usize = 2;

/// What a model's chat templates read of a request's messages: the template a request without
/// tools renders with, and the one a request with tools renders with. The two differ only for
/// a model with named `default` and `tool_use` templates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentSupport {
    without_tools: TemplateReads,
    with_tools: TemplateReads,
}

/// What one chat template reads of a request's messages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TemplateReads {
    shapes: RoleShapes,
    /// For each of the [`INSTRUCTIONS`] roles, whether a message of that role after the
    /// conversation has started reaches the template as a user's, because the template does
    /// not render it there. A template that was not probed gets it as it came.
    later_as_user: [bool; INSTRUCTIONS],
}

/// What one chat template reads as the content of a message of each of [`ROLES`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RoleShapes([Shapes; ROLES.len()]);

/// What one chat template reads as the content of a message of one role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Shapes {
    /// The template renders a string.
    strings: bool,
    /// The template renders every text part of an array.
    parts: bool,
}

impl Default for Shapes {
    /// A template that was not probed is taken to read only strings, as llama.cpp takes one,
    /// so a message's parts reach it as their text.
    fn default() -> Self {
        Self {
            strings: true,
            parts: false,
        }
    }
}

impl ContentSupport {
    /// Probes a model's chat templates through `render`, which renders a conversation with
    /// the template a request with tools (`true`) or without them (`false`) renders with, or
    /// returns `None` when the template fails on it.
    pub fn from_renders(
        render: impl Fn(Vec<IndexMap<String, MessageContent>>, bool) -> Option<String>,
    ) -> Self {
        Self {
            without_tools: TemplateReads::probe(&|messages| render(messages, false)),
            with_tools: TemplateReads::probe(&|messages| render(messages, true)),
        }
    }

    /// Rewrites a request's messages to ones the template it renders with reads in full: each
    /// message's `content` in a shape the template reads for the message's role, and a system
    /// or developer message after the conversation has started as a user's when the template
    /// does not render it there. `with_tools` is whether the request offers tools.
    pub fn rewrite(self, messages: &mut Vec<IndexMap<String, MessageContent>>, with_tools: bool) {
        let reads = if with_tools {
            self.with_tools
        } else {
            self.without_tools
        };
        for message in messages.iter_mut() {
            let shapes = reads.shapes.0[role_of(message).unwrap_or(USER)];
            if let Some(content) = message.get_mut("content") {
                shapes.fit(content);
            }
        }
        reads.place_later_instructions(messages);
    }
}

impl TemplateReads {
    /// Probes one chat template through `render`: the shapes of content it reads for each role,
    /// and whether it renders a system or developer message after a user message.
    fn probe(render: &impl Fn(Vec<IndexMap<String, MessageContent>>) -> Option<String>) -> Self {
        let shapes = RoleShapes::probe(render);
        let later_as_user = std::array::from_fn(|role| !shapes.renders_later(render, role));
        Self {
            shapes,
            later_as_user,
        }
    }

    /// Each system or developer message after the first message of any other role that this
    /// template gets as a user's ([`Self::later_as_user`]): its text joined to the user message
    /// before it, after a blank line, or the message itself as a user's when the message before
    /// it is not a user's.
    fn place_later_instructions(self, messages: &mut Vec<IndexMap<String, MessageContent>>) {
        let is_instruction = |message: &IndexMap<String, MessageContent>| {
            role_of(message).is_some_and(|role| role < INSTRUCTIONS)
        };
        let Some(start) = messages.iter().position(|m| !is_instruction(m)) else {
            return;
        };
        let as_user = |message: &IndexMap<String, MessageContent>| {
            role_of(message)
                .and_then(|role| self.later_as_user.get(role))
                .is_some_and(|as_user| *as_user)
        };
        if !messages.iter().skip(start).any(as_user) {
            return;
        }
        let mut placed: Vec<IndexMap<String, MessageContent>> = Vec::with_capacity(messages.len());
        for (index, mut message) in std::mem::take(messages).into_iter().enumerate() {
            if index < start || !as_user(&message) {
                placed.push(message);
                continue;
            }
            let text = message.get("content").map(content_text).unwrap_or_default();
            match placed.last_mut() {
                Some(before) if role_of(before) == Some(USER) => join_text(before, &text),
                _ => {
                    message.insert("role".to_string(), Either::Left(ROLES[USER].to_string()));
                    if let Some(content) = message.get_mut("content") {
                        self.shapes.0[USER].fit(content);
                    }
                    placed.push(message);
                }
            }
        }
        *messages = placed;
    }
}

impl Shapes {
    /// Rewrites `content` to a shape a template that reads `self` reads in full.
    fn fit(self, content: &mut MessageContent) {
        *content = match (self.strings, self.parts, &*content) {
            (true, false, Either::Right(sent)) => Either::Left(joined_text(sent)),
            (false, true, Either::Left(text)) => text_parts([text.as_str()]),
            // A template that reads both takes each message as it came, and one that reads
            // neither fails on it either way.
            _ => return,
        };
    }

    /// `text` in a shape a template that reads `self` reads: a string, unless it reads only parts.
    fn content(self, text: &str) -> MessageContent {
        if !self.strings && self.parts {
            text_parts([text])
        } else {
            Either::Left(text.to_string())
        }
    }
}

impl RoleShapes {
    /// Probes one chat template through `render`, a conversation for each role with a marker
    /// as a string and as two parts in that role's message. A role whose conversation the
    /// template renders in neither shape, as Gemma 3's refuses a `tool` message, is taken to
    /// read what a user message reads.
    fn probe(render: &impl Fn(Vec<IndexMap<String, MessageContent>>) -> Option<String>) -> Self {
        let mut probed = ROLES.map(|role| {
            let strings = render(conversation(role, Either::Left(MARKERS[0].to_string())))
                .is_some_and(|prompt| prompt.contains(MARKERS[0]));
            let parts = render(conversation(role, probe_parts())).is_some_and(|prompt| {
                MARKERS.iter().all(|marker| prompt.contains(marker)) && !prompt.contains(PROBE_KEY)
            });
            Shapes { strings, parts }
        });
        let user = probed[USER];
        for shapes in &mut probed {
            if !shapes.strings && !shapes.parts {
                *shapes = user;
            }
        }
        Self(probed)
    }

    /// Whether the template renders, with its text, a message of `ROLES[role]` that follows a
    /// user message, in a conversation that starts with a system message as Claude Code's does.
    /// Each message is in a shape the template reads for its role.
    fn renders_later(
        self,
        render: &impl Fn(Vec<IndexMap<String, MessageContent>>) -> Option<String>,
        role: usize,
    ) -> bool {
        let hi = || message(ROLES[USER], self.0[USER].content("Hi"));
        let conversation = vec![
            message(ROLES[0], self.0[0].content("Be brief.")),
            hi(),
            message(ROLES[role], self.0[role].content(MARKERS[0])),
            hi(),
        ];
        render(conversation).is_some_and(|prompt| prompt.contains(MARKERS[0]))
    }
}

/// The index in [`ROLES`] of `message`'s role, `None` for any other role.
fn role_of(message: &IndexMap<String, MessageContent>) -> Option<usize> {
    match message.get("role") {
        Some(Either::Left(role)) => ROLES.iter().position(|r| r == role),
        _ => None,
    }
}

/// A message of `role` with `content`.
fn message(role: &str, content: MessageContent) -> IndexMap<String, MessageContent> {
    IndexMap::from([
        ("role".to_string(), Either::Left(role.to_string())),
        ("content".to_string(), content),
    ])
}

/// The text of a message's content: a string as it is, and text parts joined with newlines.
fn content_text(content: &MessageContent) -> String {
    match content {
        Either::Left(text) => text.clone(),
        Either::Right(parts) => joined_text(parts),
    }
}

/// Joins `text` to `message`'s content after a blank line: to its string, or as a text part
/// after its parts, which a template renders one after another.
fn join_text(message: &mut IndexMap<String, MessageContent>, text: &str) {
    match message.get_mut("content") {
        Some(Either::Left(content)) => {
            content.push_str("\n\n");
            content.push_str(text);
        }
        Some(Either::Right(parts)) => parts.push(text_part(format!("\n\n{text}"))),
        None => {
            message.insert("content".to_string(), Either::Left(text.to_string()));
        }
    }
}

/// A conversation in which the message of `role` has `content`, and which a chat template
/// renders as it renders a request's: a system or developer message before a user message,
/// an assistant message between user messages, and a tool message after an assistant's call.
fn conversation(role: &str, content: MessageContent) -> Vec<IndexMap<String, MessageContent>> {
    let hi = || message("user", Either::Left("Hi".to_string()));
    match role {
        "system" | "developer" => vec![message(role, content), hi()],
        "assistant" => vec![hi(), message(role, content), hi()],
        "tool" => {
            let mut call = message("assistant", Either::Left(String::new()));
            call.insert(
                "tool_calls".to_string(),
                Either::Right(vec![IndexMap::from([
                    ("id".to_string(), json!("probe")),
                    ("type".to_string(), json!("function")),
                    (
                        "function".to_string(),
                        json!({ "name": "probe", "arguments": "{}" }),
                    ),
                ])]),
            );
            let mut result = message(role, content);
            result.insert(
                "tool_call_id".to_string(),
                Either::Left("probe".to_string()),
            );
            vec![hi(), call, result]
        }
        _ => vec![message(role, content)],
    }
}

/// [`MARKERS`] as text parts, each carrying [`PROBE_KEY`].
fn probe_parts() -> MessageContent {
    let mut parts = text_parts(MARKERS);
    if let Either::Right(parts) = &mut parts {
        for part in parts {
            part.insert(PROBE_KEY.to_string(), Value::Bool(true));
        }
    }
    parts
}

/// `texts` as an array of text parts, the shape chat templates read a message's parts in.
pub(crate) fn text_parts<'a>(texts: impl IntoIterator<Item = &'a str>) -> MessageContent {
    Either::Right(
        texts
            .into_iter()
            .map(|text| text_part(text.to_string()))
            .collect(),
    )
}

/// `text` as a text part.
fn text_part(text: String) -> IndexMap<String, Value> {
    IndexMap::from([
        ("type".to_string(), Value::String("text".to_string())),
        ("text".to_string(), Value::String(text)),
    ])
}

/// The text of a message's text parts, joined with newlines, as llama-server joins them for a
/// template that reads only strings.
fn joined_text(parts: &[IndexMap<String, Value>]) -> String {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}
