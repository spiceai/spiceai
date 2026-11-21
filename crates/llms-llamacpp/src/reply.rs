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

//! The reply formats this engine reads token by token, by their markers rather than as text:
//! Harmony (gpt-oss, [`harmony`]) and Inkling ([`inkling`]). Each carries a reply's reasoning,
//! content and tool calls in blocks its marker tokens delimit, so a reply in either is read as
//! llama.cpp's server reads it, and [`Reply`] is the one reader the engine drives for both.

use llama_cpp::{Token, Vocab};
use mistralrs_core::{
    CalledFunction, ToolCallResponse, ToolCallType, reasoning_parsers::harmony::is_harmony_template,
};

use crate::{harmony, inkling};

/// What a grammar holds a reply to, as llama.cpp's server holds it: the whole reply, or none
/// of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    Nothing,
    /// Any reasoning, then content that is a `response_format`'s JSON.
    Reply,
    /// Any reasoning, then a call to one of the tools a required call may name.
    Call,
}

/// Where a piece of a reply's text goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Text {
    Reasoning,
    Content,
    /// Read into a header, or a call's arguments, or dropped: nothing to send.
    Held,
}

/// What a marker does to the reply.
#[derive(Debug)]
pub(crate) enum Mark {
    /// The reply goes on.
    Next,
    /// The reply ends with these tool calls.
    Calls(Vec<ToolCallResponse>),
    /// The reply ends.
    Done,
}

/// The markers of each reply format a model's vocabulary holds: a format's only when every one
/// of its markers is a single token.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Formats {
    pub(crate) harmony: Option<harmony::Markers>,
    pub(crate) inkling: Option<inkling::Markers>,
}

impl Formats {
    pub(crate) fn find(vocab: Vocab<'_>) -> Self {
        Self {
            harmony: harmony::Markers::find(vocab),
            inkling: inkling::Markers::find(vocab),
        }
    }

    /// The format a reply to a request rendered with `template` is read in: the one the
    /// template writes, by the markers the vocabulary holds for it. Named templates may mix a
    /// template that writes a format with one that does not, so this is decided per request.
    pub(crate) fn of(&self, template: &str) -> Option<Format> {
        if is_harmony_template(template) {
            self.harmony.map(Format::Harmony)
        } else if inkling::is_inkling_template(template) {
            self.inkling.map(Format::Inkling)
        } else {
            None
        }
    }
}

/// A reply format, with the markers its replies are read by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Harmony(harmony::Markers),
    Inkling(inkling::Markers),
}

impl Format {
    /// The reader of one reply that may call `tools` and is held to a grammar as `held` says,
    /// started where `prompt`, rendered with the template's generation prompt, leaves the
    /// reply; `conversation` is the same conversation rendered without it, which tells what the
    /// generation prompt adds.
    pub(crate) fn reader(
        self,
        tools: Vec<String>,
        held: Held,
        prompt: &str,
        conversation: Option<&str>,
    ) -> Reply {
        match self {
            Self::Harmony(markers) => Reply::Harmony(
                harmony::Reader::new(markers, tools, held).after(harmony::reply_text(
                    &markers,
                    prompt,
                    conversation,
                )),
            ),
            Self::Inkling(markers) => Reply::Inkling(
                inkling::Reader::new(markers, tools, held).after(inkling::reply_text(
                    &markers,
                    prompt,
                    conversation,
                )),
            ),
        }
    }
}

/// Reads one reply in the format its template writes.
#[derive(Debug, Clone)]
pub(crate) enum Reply {
    Harmony(harmony::Reader),
    Inkling(inkling::Reader),
}

impl Reply {
    /// The reply's text the prompt wrote, which the grammar is to be advanced over before the
    /// reply goes on from where the prompt left it; empty when there is none to advance over.
    pub(crate) fn pending(&self) -> &str {
        match self {
            Self::Harmony(reader) => reader.pending(),
            Self::Inkling(reader) => reader.pending(),
        }
    }

    /// Starts the reply where the prompt left it when it `goes_on` from there, and returns the
    /// text the prompt began the opened block's body with, to send first. Otherwise the reply
    /// starts afresh, where the grammar, built again at its root, holds it.
    pub(crate) fn begin(&mut self, goes_on: bool) -> String {
        match self {
            Self::Harmony(reader) => reader.begin(goes_on),
            Self::Inkling(reader) => reader.begin(goes_on),
        }
    }

    /// Whether a grammar holds the token that comes next.
    pub(crate) fn in_grammar(&self) -> bool {
        match self {
            Self::Harmony(reader) => reader.in_grammar(),
            Self::Inkling(reader) => reader.in_grammar(),
        }
    }

    /// The token that ends a reply with a tool call, which a required call must leave
    /// possible: Harmony's `<|call|>`. An Inkling reply makes its calls at the end-of-generation
    /// token that ends any reply, which its grammar allows only once a call is made.
    pub(crate) fn ends_with_call(&self) -> Option<Token> {
        match self {
            Self::Harmony(reader) => Some(reader.markers().call()),
            Self::Inkling(_) => None,
        }
    }

    /// Whether `token` is a marker here, rather than text of the body it is in. `held` is the
    /// body's text that has not reached the reader yet, as a stop sequence holds it back.
    pub(crate) fn marks(&self, token: Token, held: &str) -> bool {
        match self {
            Self::Harmony(reader) => reader.marks(token, held),
            Self::Inkling(reader) => reader.marks(token, held),
        }
    }

    /// Whether the reply is in a header, whose text is read here rather than sent.
    pub(crate) fn in_header(&self) -> bool {
        match self {
            Self::Harmony(reader) => reader.in_header(),
            Self::Inkling(reader) => reader.in_header(),
        }
    }

    /// Reads `token`, a marker.
    pub(crate) fn mark(&mut self, token: Token) -> Mark {
        match self {
            Self::Harmony(reader) => match reader.mark(token) {
                harmony::Mark::Next => Mark::Next,
                harmony::Mark::Call(call) => Mark::Calls(vec![call]),
                harmony::Mark::Done => Mark::Done,
            },
            Self::Inkling(reader) => reader.mark(token),
        }
    }

    /// Reads `text`, which is not a marker, and says where it goes.
    pub(crate) fn text(&mut self, text: &str) -> Text {
        match self {
            Self::Harmony(reader) => reader.text(text),
            Self::Inkling(reader) => reader.text(text),
        }
    }

    /// The calls a reply that ends without its closing marker, at a stop sequence, the token
    /// budget or another end-of-generation token, still makes, as llama.cpp's server reads it:
    /// those it has made, and one whose arguments are already a complete JSON value.
    pub(crate) fn finish(&mut self) -> Vec<ToolCallResponse> {
        match self {
            Self::Harmony(reader) => reader.finish().into_iter().collect(),
            Self::Inkling(reader) => reader.finish(),
        }
    }
}

/// `text` as the `markers` it spells, each as its token with its text, and the text between
/// them, as a prompt tokenized with its markers as tokens holds it. Every marker starts with
/// `<|`, so the text is scanned once, for those two bytes.
pub(crate) fn spell<'t>(text: &'t str, markers: &[(&str, Token)]) -> Vec<(Option<Token>, &'t str)> {
    let mut out = Vec::new();
    // The text before `from` is spelled; a marker may start at `at` or after it.
    let (mut from, mut at) = (0, 0);
    while let Some(found) = text[at..].find("<|") {
        let i = at + found;
        if let Some((s, token)) = markers.iter().find(|(s, _)| text[i..].starts_with(s)) {
            if from < i {
                out.push((None, &text[from..i]));
            }
            out.push((Some(*token), &text[i..i + s.len()]));
            from = i + s.len();
            at = from;
        } else {
            at = i + "<|".len();
        }
    }
    if from < text.len() {
        out.push((None, &text[from..]));
    }
    out
}

/// A call to the tool `name` with `arguments`, the JSON the model wrote for them.
pub(crate) fn call(name: String, arguments: &str) -> ToolCallResponse {
    ToolCallResponse {
        index: 0,
        id: format!("call-{}", uuid::Uuid::new_v4()),
        tp: ToolCallType::Function,
        function: CalledFunction {
            name,
            arguments: arguments.to_string(),
        },
    }
}
