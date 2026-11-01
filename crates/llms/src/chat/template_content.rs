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

//! Which shapes of a message's `content` a chat template reads, and a request's messages
//! rewritten to one it reads in full.
//!
//! A request may send a message's content as a string or as an array of text parts, which
//! reach the template as `[{"type": "text", "text": …}]`. Templates differ in what they read:
//! Qwen3.5's reads both, and renders a message's parts one after another; Qwen3's reads only
//! a string and fails on parts; one that prints `content` as it stands prints the array
//! itself; and one that reads only a message's first part, as mistral.rs's vision pipelines
//! do, drops the rest. As llama-server does (`common/chat.cpp` and `common/jinja/caps.cpp` in
//! llama.cpp), the template is probed when the model loads, by rendering markers in each
//! shape, and a request's messages are rewritten to the shape the template reads in full:
//! parts joined with newlines for a template that reads only strings, a string as one text
//! part for a template that reads only parts, and each message as it came for a template that
//! reads both.

use either::Either;
use indexmap::IndexMap;
use mistralrs::MessageContent;
use serde_json::Value;

/// Text no chat template writes itself, so a probe's prompt holds it only when the template
/// rendered the content it was given.
const MARKERS: [&str; 2] = ["SPICE-CONTENT-PROBE-ONE", "SPICE-CONTENT-PROBE-TWO"];

/// A key on each part of the parts probe that no chat template reads: a template that prints
/// the parts themselves, rather than their text, prints it as well.
const PROBE_KEY: &str = "spice_content_probe";

/// What a model's chat templates read as a message's `content`: the template a request without
/// tools renders with, and the one a request with tools renders with. The two differ only for
/// a model with named `default` and `tool_use` templates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentSupport {
    without_tools: Shapes,
    with_tools: Shapes,
}

/// What one chat template reads as a message's `content`.
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
    /// Probes a model's chat templates through `render`, which renders a conversation of one
    /// user message with the given content, with the template a request with tools (`true`)
    /// or without them (`false`) renders with, or returns `None` when the template fails.
    pub fn from_renders(render: impl Fn(MessageContent, bool) -> Option<String>) -> Self {
        let probe = |with_tools: bool| {
            let strings = render(Either::Left(MARKERS[0].to_string()), with_tools)
                .is_some_and(|prompt| prompt.contains(MARKERS[0]));
            let mut parts = text_parts(MARKERS);
            if let Either::Right(parts) = &mut parts {
                for part in parts {
                    part.insert(PROBE_KEY.to_string(), Value::Bool(true));
                }
            }
            let parts = render(parts, with_tools).is_some_and(|prompt| {
                MARKERS.iter().all(|marker| prompt.contains(marker)) && !prompt.contains(PROBE_KEY)
            });
            Shapes { strings, parts }
        };
        Self {
            without_tools: probe(false),
            with_tools: probe(true),
        }
    }

    /// Rewrites each message's `content` to a shape the template a request renders with reads
    /// in full; `with_tools` is whether the request offers tools.
    pub fn rewrite(self, messages: &mut [IndexMap<String, MessageContent>], with_tools: bool) {
        let shapes = if with_tools {
            self.with_tools
        } else {
            self.without_tools
        };
        let contents = messages
            .iter_mut()
            .filter_map(|message| message.get_mut("content"));
        match (shapes.strings, shapes.parts) {
            (true, false) => {
                for content in contents {
                    if let Either::Right(parts) = content {
                        *content = Either::Left(joined_text(parts));
                    }
                }
            }
            (false, true) => {
                for content in contents {
                    if let Either::Left(text) = content {
                        *content = text_parts([text.as_str()]);
                    }
                }
            }
            // A template that reads both takes each message as it came, and one that reads
            // neither fails on it either way.
            (true, true) | (false, false) => {}
        }
    }
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
