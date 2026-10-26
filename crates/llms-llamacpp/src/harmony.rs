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

//! Reads a Harmony reply (gpt-oss) token by token. A reply is a run of messages, each a header
//! naming its channel and any recipient, then `<|message|>` and the body:
//!
//! ```text
//! <|channel|>analysis<|message|>…<|end|><|start|>assistant<|channel|>final<|message|>…<|return|>
//! <|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{…}<|call|>
//! ```
//!
//! The messages are read as llama.cpp's server reads them, so both engines agree: `analysis` is
//! the reasoning, `final` and a `commentary` preamble addressed to no one are the content, and a
//! message addressed to `functions.NAME`, in its role header or its channel header, is a call
//! to that tool with the body as its JSON arguments. A message addressed to anything else, such
//! as a built-in tool the request does not offer, is dropped.
//!
//! A body ends only at `<|end|>` or with the reply, so any other marker in it is text. A call's
//! arguments and a final message held to a `response_format` are JSON, which may spell a marker
//! inside a string: a call ends at `<|end|>` only once its arguments are a complete JSON value,
//! and a structured final message only with the reply.
//!
//! The markers are read as tokens, never as text, so text that spells one is only text.

use llama_cpp::{Token, Vocab};
use mistralrs_core::{CalledFunction, ToolCallResponse, ToolCallType};

const CHANNEL: &str = "<|channel|>";
const CONSTRAIN: &str = "<|constrain|>";

/// The control tokens that mark a Harmony reply's messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Markers {
    start: Token,
    channel: Token,
    message: Token,
    end: Token,
    /// `<|return|>`, which ends the reply.
    done: Token,
    call: Token,
    constrain: Token,
}

impl Markers {
    /// The markers in `vocab`, when it holds each one as a single token.
    pub(crate) fn find(vocab: Vocab<'_>) -> Option<Self> {
        let one = |text: &str| match vocab.tokenize(text, false, true).ok()?.as_slice() {
            [token] => Some(*token),
            _ => None,
        };
        Some(Self {
            start: one("<|start|>")?,
            channel: one(CHANNEL)?,
            message: one("<|message|>")?,
            end: one("<|end|>")?,
            done: one("<|return|>")?,
            call: one("<|call|>")?,
            constrain: one(CONSTRAIN)?,
        })
    }

    /// The token that ends a reply with a tool call, which a required call must leave
    /// possible.
    pub(crate) fn call(&self) -> Token {
        self.call
    }
}

/// Where the text of the message being read goes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    /// The message's header, up to `<|message|>`.
    Header(String),
    /// An `analysis` message's body: the reply's reasoning.
    Reasoning,
    /// A `final` message's body, or a preamble's.
    Content { last: bool },
    /// The JSON arguments of a call to the tool `name`.
    Call { name: String, arguments: String },
    /// A message addressed to a tool the request does not offer.
    Dropped,
}

/// Where a piece of a reply's text goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Text {
    Reasoning,
    Content,
    /// Read into the message's header, or a call's arguments, or dropped: nothing to send.
    Held,
}

/// What a marker does to the reply.
#[derive(Debug)]
pub(crate) enum Mark {
    /// The reply goes on.
    Next,
    /// The reply ends with this tool call.
    Call(ToolCallResponse),
    /// The reply ends.
    Done,
}

/// What part of a reply a grammar holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    /// None of it.
    Nothing,
    /// The final message, to a `response_format`'s schema.
    Final,
    /// All of it, to a call one of the tools a required call may name.
    Whole,
}

/// Reads one reply.
#[derive(Debug)]
pub(crate) struct Reader {
    markers: Markers,
    /// The tools a call may name.
    tools: Vec<String>,
    held: Held,
    route: Route,
}

impl Reader {
    /// The chat template's generation prompt opens the reply's first message, so the reply
    /// starts in its header.
    pub(crate) fn new(markers: Markers, tools: Vec<String>, held: Held) -> Self {
        Self {
            markers,
            tools,
            held,
            route: Route::Header(String::new()),
        }
    }

    pub(crate) fn markers(&self) -> &Markers {
        &self.markers
    }

    /// Whether `token` is a marker here, rather than text of the body it is in.
    pub(crate) fn marks(&self, token: Token) -> bool {
        let m = &self.markers;
        if token == m.done || token == m.call {
            return true;
        }
        match &self.route {
            Route::Header(_) => {
                [m.start, m.channel, m.message, m.end, m.constrain].contains(&token)
            }
            Route::Call { arguments, .. } => token == m.end && is_json(arguments),
            Route::Content { last: true } if self.held == Held::Final => false,
            Route::Reasoning | Route::Content { .. } | Route::Dropped => token == m.end,
        }
    }

    /// Whether the reply is in a header, whose text is read here rather than sent.
    pub(crate) fn in_header(&self) -> bool {
        matches!(self.route, Route::Header(_))
    }

    /// Whether the grammar holds the token that comes next: in the final message, a
    /// `response_format`'s; anywhere, a required call's.
    pub(crate) fn in_grammar(&self) -> bool {
        match self.held {
            Held::Nothing => false,
            Held::Final => matches!(self.route, Route::Content { last: true }),
            Held::Whole => true,
        }
    }

    /// Reads `token`, a marker.
    pub(crate) fn mark(&mut self, token: Token) -> Mark {
        let m = self.markers;
        if let Route::Header(header) = &mut self.route {
            if token == m.channel {
                header.push_str(CHANNEL);
            } else if token == m.constrain {
                header.push_str(CONSTRAIN);
            } else if token == m.message {
                self.route = route(header, &self.tools);
            } else if token == m.start || token == m.end {
                header.clear();
            } else {
                return Mark::Done;
            }
            return Mark::Next;
        }
        if let Route::Call { name, arguments } = &mut self.route {
            return Mark::Call(call(std::mem::take(name), arguments.trim()));
        }
        if token == m.done || token == m.call {
            return Mark::Done;
        }
        // `<|end|>` closes the message, and the next one's header follows.
        self.route = Route::Header(String::new());
        Mark::Next
    }

    /// The call a reply that ends without its `<|call|>`, at a stop sequence or the token
    /// budget, still makes: one whose arguments are already a complete JSON value, as
    /// llama.cpp's server reads it.
    pub(crate) fn finish(&mut self) -> Option<ToolCallResponse> {
        match &mut self.route {
            Route::Call { name, arguments } if is_json(arguments) => {
                Some(call(std::mem::take(name), arguments.trim()))
            }
            _ => None,
        }
    }

    /// Reads `text`, which is not a marker, and says where it goes.
    pub(crate) fn text(&mut self, text: &str) -> Text {
        match &mut self.route {
            Route::Header(header) => header.push_str(text),
            Route::Call { arguments, .. } => arguments.push_str(text),
            Route::Reasoning => return Text::Reasoning,
            Route::Content { .. } => return Text::Content,
            Route::Dropped => {}
        }
        Text::Held
    }
}

/// Where the body of a message with `header` goes.
fn route(header: &str, tools: &[String]) -> Route {
    let channel = header
        .rfind(CHANNEL)
        .map(|at| word(&header[at + CHANNEL.len()..]));
    // `to=assistant` addresses the reply itself, as a stray `commentary` header before a
    // message sometimes does.
    let recipient = header
        .match_indices("to=")
        .filter(|&(at, _)| at == 0 || header[..at].ends_with(char::is_whitespace))
        .map(|(at, _)| recipient(&header[at + 3..]))
        .filter(|&r| r != "assistant")
        .last();
    match recipient {
        Some(recipient) => match recipient.strip_prefix("functions.") {
            Some(name) if tools.iter().any(|t| t == name) => Route::Call {
                name: name.to_string(),
                arguments: String::new(),
            },
            _ => Route::Dropped,
        },
        None => match channel {
            Some("analysis") => Route::Reasoning,
            Some("final") => Route::Content { last: true },
            _ => Route::Content { last: false },
        },
    }
}

/// Whether `text` is one complete JSON value.
fn is_json(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok()
}

/// The channel name `text` starts with.
fn word(text: &str) -> &str {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(text.len());
    &text[..end]
}

/// The recipient `text` starts with: up to whitespace or the next marker.
fn recipient(text: &str) -> &str {
    let end = text
        .find(|c: char| c.is_whitespace() || c == '<')
        .unwrap_or(text.len());
    &text[..end]
}

fn call(name: String, arguments: &str) -> ToolCallResponse {
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

/// Markers for tests, as a vocabulary with them as tokens 1 to 7 would hold them.
#[cfg(test)]
pub(crate) const TEST_MARKERS: Markers = Markers {
    start: Token(1),
    channel: Token(2),
    message: Token(3),
    end: Token(4),
    done: Token(5),
    call: Token(6),
    constrain: Token(7),
};

/// A reply as [`TEST_MARKERS`] spell it: each marker as its token, with its text, and the text
/// between markers.
#[cfg(test)]
pub(crate) fn spell(reply: &str) -> Vec<(Option<Token>, String)> {
    const SPELLED: [(&str, Token); 7] = [
        ("<|start|>", TEST_MARKERS.start),
        ("<|channel|>", TEST_MARKERS.channel),
        ("<|message|>", TEST_MARKERS.message),
        ("<|end|>", TEST_MARKERS.end),
        ("<|return|>", TEST_MARKERS.done),
        ("<|call|>", TEST_MARKERS.call),
        ("<|constrain|>", TEST_MARKERS.constrain),
    ];
    let mut out = Vec::new();
    let mut rest = reply;
    while !rest.is_empty() {
        if let Some((spelled, token)) = SPELLED.iter().find(|(s, _)| rest.starts_with(s)) {
            out.push((Some(*token), (*spelled).to_string()));
            rest = &rest[spelled.len()..];
        } else {
            let next = SPELLED
                .iter()
                .filter_map(|(s, _)| rest.find(s))
                .min()
                .unwrap_or(rest.len());
            out.push((None, rest[..next].to_string()));
            rest = &rest[next..];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reasoning, the content and the call `reply` reads as, and whether it ended.
    fn read(reply: &str, tools: &[&str]) -> (String, String, Option<(String, String)>, bool) {
        read_as(reply, tools, Held::Nothing)
    }

    fn read_as(
        reply: &str,
        tools: &[&str],
        held: Held,
    ) -> (String, String, Option<(String, String)>, bool) {
        let tools = tools.iter().map(ToString::to_string).collect();
        let mut reader = Reader::new(TEST_MARKERS, tools, held);
        let (mut reasoning, mut content) = (String::new(), String::new());
        for (token, text) in spell(reply) {
            if let Some(token) = token
                && reader.marks(token)
            {
                match reader.mark(token) {
                    Mark::Next => continue,
                    Mark::Call(call) => {
                        let called = (call.function.name, call.function.arguments);
                        return (reasoning, content, Some(called), true);
                    }
                    Mark::Done => return (reasoning, content, None, true),
                }
            }
            match reader.text(&text) {
                Text::Reasoning => reasoning.push_str(&text),
                Text::Content => content.push_str(&text),
                Text::Held => {}
            }
        }
        (reasoning, content, None, false)
    }

    fn answer(reasoning: &str, content: &str) -> (String, String, Option<(String, String)>, bool) {
        (reasoning.to_string(), content.to_string(), None, true)
    }

    fn called(
        reasoning: &str,
        name: &str,
        arguments: &str,
    ) -> (String, String, Option<(String, String)>, bool) {
        let call = Some((name.to_string(), arguments.to_string()));
        (reasoning.to_string(), String::new(), call, true)
    }

    #[test]
    fn analysis_is_the_reasoning_and_final_the_content() {
        assert_eq!(
            read(
                "<|channel|>analysis<|message|>Add them.<|end|><|start|>assistant<|channel|>final<|message|>4<|return|>",
                &[]
            ),
            answer("Add them.", "4")
        );
        assert_eq!(
            read("<|channel|>final<|message|>Hi<|return|>", &[]),
            answer("", "Hi")
        );
    }

    #[test]
    fn a_call_is_read_from_its_role_or_its_channel_header() {
        let tools = ["get_weather"];
        assert_eq!(
            read(
                "<|channel|>analysis<|message|>Look it up.<|end|><|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{\"city\":\"SF\"}<|call|>",
                &tools
            ),
            called("Look it up.", "get_weather", "{\"city\":\"SF\"}")
        );
        assert_eq!(
            read(
                "<|channel|>analysis<|message|>x<|end|><|start|>assistant to=functions.get_weather<|channel|>commentary json<|message|>{}<|call|>",
                &tools
            ),
            called("x", "get_weather", "{}")
        );
        // from the analysis channel too, as llama.cpp's server reads one
        assert_eq!(
            read(
                "<|channel|>analysis to=functions.get_weather<|message|> {\"a\":1} <|call|>",
                &tools
            ),
            called("", "get_weather", "{\"a\":1}")
        );
    }

    #[test]
    fn a_preamble_is_content_and_a_call_to_a_tool_not_offered_is_dropped() {
        assert_eq!(
            read(
                "<|channel|>commentary<|message|>Checking. <|end|><|start|>assistant<|channel|>commentary to=functions.get_weather<|message|>{}<|call|>",
                &["get_weather"]
            ),
            (
                String::new(),
                "Checking. ".to_string(),
                Some(("get_weather".to_string(), "{}".to_string())),
                true
            )
        );
        // a built-in tool, and a function the request does not offer
        assert_eq!(
            read(
                "<|channel|>analysis to=browser.search<|message|>{\"q\":1}<|call|>",
                &["get_weather"]
            ),
            answer("", "")
        );
        assert_eq!(
            read(
                "<|channel|>commentary to=functions.get_time<|message|>{}<|end|><|start|>assistant<|channel|>final<|message|>ok<|return|>",
                &["get_weather"]
            ),
            answer("", "ok")
        );
    }

    #[test]
    fn a_stray_commentary_header_addressed_to_the_reply_is_skipped() {
        assert_eq!(
            read(
                "<|channel|>commentary to=assistant<|channel|>analysis<|message|>think<|end|><|start|>assistant<|channel|>commentary<|channel|>final<|message|>done<|return|>",
                &[]
            ),
            answer("think", "done")
        );
    }

    #[test]
    fn marker_text_in_a_body_is_text() {
        // `<|message|>` and `<|constrain|>` as tokens in a body mark nothing
        assert_eq!(
            read(
                "<|channel|>final<|message|>a<|message|>b<|constrain|>c<|return|>",
                &[]
            ),
            answer("", "a<|message|>b<|constrain|>c")
        );
    }

    #[test]
    fn a_body_ends_only_at_its_end_marker() {
        // as llama.cpp's server reads a body: up to `<|end|>`
        assert_eq!(
            read(
                "<|channel|>analysis<|message|>r<|channel|>final<|message|>c<|end|><|start|>assistant<|channel|>final<|message|>d<|return|>",
                &[]
            ),
            answer("r<|channel|>final<|message|>c", "d")
        );
        assert_eq!(
            read("<|channel|>final<|message|>a<|start|>b<|return|>", &[]),
            answer("", "a<|start|>b")
        );
    }

    #[test]
    fn json_may_spell_an_end_marker_inside_a_string() {
        // a call's arguments end at `<|end|>` only once they are complete
        assert_eq!(
            read(
                "<|channel|>commentary to=functions.f<|message|>{\"s\":\"a<|end|>b\"}<|call|>",
                &["f"]
            ),
            called("", "f", "{\"s\":\"a<|end|>b\"}")
        );
        assert_eq!(
            read(
                "<|channel|>commentary to=functions.f<|message|>{\"s\":1}<|end|><|start|>assistant<|channel|>final<|message|>x<|return|>",
                &["f"]
            ),
            called("", "f", "{\"s\":1}")
        );
        // a final message held to a schema ends only with the reply
        assert_eq!(
            read_as(
                "<|channel|>final<|message|>{\"s\":\"<|end|>\"}<|return|>",
                &[],
                Held::Final
            ),
            answer("", "{\"s\":\"<|end|>\"}")
        );
        assert_eq!(
            read_as(
                "<|channel|>final<|message|>a<|end|><|start|>assistant<|channel|>final<|message|>b<|return|>",
                &[],
                Held::Nothing
            ),
            answer("", "ab")
        );
    }

    #[test]
    fn a_reply_that_ends_after_complete_arguments_still_makes_its_call() {
        let mut reader = Reader::new(TEST_MARKERS, vec!["f".to_string()], Held::Nothing);
        for (token, text) in spell("<|channel|>commentary to=functions.f<|message|>") {
            match token {
                Some(token) => {
                    reader.mark(token);
                }
                None => {
                    reader.text(&text);
                }
            }
        }
        reader.text("{\"a\":");
        assert!(
            reader.finish().is_none(),
            "arguments cut short make no call"
        );
        reader.text("1} ");
        let call = reader.finish().expect("complete arguments make a call");
        assert_eq!(
            (
                call.function.name.as_str(),
                call.function.arguments.as_str()
            ),
            ("f", "{\"a\":1}")
        );
    }

    #[test]
    fn only_the_final_message_is_held_to_a_response_format() {
        let mut reader = Reader::new(TEST_MARKERS, Vec::new(), Held::Final);
        assert!(!reader.in_grammar() && reader.in_header());
        reader.mark(TEST_MARKERS.channel);
        reader.text("commentary");
        reader.mark(TEST_MARKERS.message);
        assert!(!reader.in_grammar() && !reader.in_header(), "a preamble");
        reader.mark(TEST_MARKERS.end);
        reader.mark(TEST_MARKERS.start);
        reader.text("assistant");
        reader.mark(TEST_MARKERS.channel);
        reader.text("final");
        reader.mark(TEST_MARKERS.message);
        assert!(reader.in_grammar());
        let whole = Reader::new(TEST_MARKERS, Vec::new(), Held::Whole);
        assert!(
            whole.in_grammar(),
            "a required call's grammar holds the whole reply"
        );
    }

    #[test]
    fn a_reply_that_runs_out_mid_call_makes_no_call() {
        assert_eq!(
            read(
                "<|channel|>commentary to=functions.f<|message|>{\"a\":",
                &["f"]
            ),
            (String::new(), String::new(), None, false)
        );
    }
}
