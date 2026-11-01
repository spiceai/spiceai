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

/// What a model's chat templates read as a message's `content`: the template a request without
/// tools renders with, and the one a request with tools renders with. The two differ only for
/// a model with named `default` and `tool_use` templates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentSupport {
    without_tools: RoleShapes,
    with_tools: RoleShapes,
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
            without_tools: RoleShapes::probe(|messages| render(messages, false)),
            with_tools: RoleShapes::probe(|messages| render(messages, true)),
        }
    }

    /// Rewrites each message's `content` to a shape the template a request renders with reads
    /// in full for the message's role; `with_tools` is whether the request offers tools.
    pub fn rewrite(self, messages: &mut [IndexMap<String, MessageContent>], with_tools: bool) {
        let shapes = if with_tools {
            self.with_tools
        } else {
            self.without_tools
        };
        for message in messages {
            let role = match message.get("role") {
                Some(Either::Left(role)) => ROLES.iter().position(|r| r == role),
                _ => None,
            };
            let Shapes { strings, parts } = shapes.0[role.unwrap_or(USER)];
            let Some(content) = message.get_mut("content") else {
                continue;
            };
            *content = match (strings, parts, &*content) {
                (true, false, Either::Right(sent)) => Either::Left(joined_text(sent)),
                (false, true, Either::Left(text)) => text_parts([text.as_str()]),
                // A template that reads both takes each message as it came, and one that reads
                // neither fails on it either way.
                _ => continue,
            };
        }
    }
}

impl RoleShapes {
    /// Probes one chat template through `render`, a conversation for each role with a marker
    /// as a string and as two parts in that role's message. A role whose conversation the
    /// template renders in neither shape, as Gemma 3's refuses a `tool` message, is taken to
    /// read what a user message reads.
    fn probe(render: impl Fn(Vec<IndexMap<String, MessageContent>>) -> Option<String>) -> Self {
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
}

/// A conversation in which the message of `role` has `content`, and which a chat template
/// renders as it renders a request's: a system or developer message before a user message,
/// an assistant message between user messages, and a tool message after an assistant's call.
fn conversation(role: &str, content: MessageContent) -> Vec<IndexMap<String, MessageContent>> {
    let message = |role: &str, content: MessageContent| {
        IndexMap::from([
            ("role".to_string(), Either::Left(role.to_string())),
            ("content".to_string(), content),
        ])
    };
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
            .map(|text| {
                IndexMap::from([
                    ("type".to_string(), Value::String("text".to_string())),
                    ("text".to_string(), Value::String(text.to_string())),
                ])
            })
            .collect(),
    )
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
