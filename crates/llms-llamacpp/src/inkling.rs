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

//! Reads an Inkling reply (Thinking Machines' Inkling models) token by token. A reply is a run
//! of blocks, each a header that names the block, a content marker that says what it holds,
//! the body and `<|end_message|>`, and the reply ends at `<|content_model_end_sampling|>`:
//!
//! ```text
//! <|message_model|><|content_thinking|>…<|end_message|><|message_model|><|content_text|>…<|end_message|>
//! <|message_model|>get_weather<|content_invoke_tool_json|>{"name":"get_weather","args":{…}}<|end_message|>
//! <|content_model_end_sampling|>
//! ```
//!
//! The blocks are read as llama.cpp's server reads them, so both engines agree: a thinking
//! block is the reasoning, a text block the content, and a `content_invoke_tool_json` block a
//! call to the tool its header names, whose arguments are the `args` of the JSON it holds. A
//! call block is exactly `NAME<|content_invoke_tool_json|>{"name":NAME,"args":{…}}`, naming a
//! tool the request offers; any other is dropped. Calls are the last blocks of a turn, and a
//! turn may make several when the request allows parallel calls, so a call does not end the
//! reply: the reply makes its calls when it ends. Without parallel calls, the turn's first call
//! ends it. As the model's tokenizer configuration reads them, a `content_tool_error` or a
//! `content_invoke_tool_text` block is content too, and so is text outside any block, as when a
//! model skips a header.
//!
//! The chat template has no generation prompt: the prompt ends after the last message's
//! `<|end_message|>`, and the model writes each block's header itself, so a reply starts
//! between blocks.
//!
//! A body ends only at `<|end_message|>` or with the reply, so any other marker in it is text.
//! A call's body and a text block held to a `response_format` are JSON, which may spell
//! `<|end_message|>` inside a string: such a body ends there only once it is a complete JSON
//! value, or once it can never become one.
//!
//! The markers are read as tokens, never as text, so text that spells one is only text.

use llama_cpp::{Token, Vocab};
use mistralrs_core::ToolCallResponse;
use serde_json::value::RawValue;

use crate::reply::{self, Held, Mark, Text};

const HEADER: &str = "<|message_model|>";
const THINKING: &str = "<|content_thinking|>";
const TEXT: &str = "<|content_text|>";
const CALL: &str = "<|content_invoke_tool_json|>";
const CALL_TEXT: &str = "<|content_invoke_tool_text|>";
const TOOL_ERROR: &str = "<|content_tool_error|>";
const END: &str = "<|end_message|>";
const DONE: &str = "<|content_model_end_sampling|>";

/// Whether a chat template writes Inkling messages, as llama.cpp's server tells one: by its
/// user-message marker and its image marker.
pub(crate) fn is_inkling_template(template: &str) -> bool {
    template.contains("<|message_user|>") && template.contains("<|content_image|>")
}

/// The control tokens that mark an Inkling reply's blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Markers {
    /// `<|message_model|>`, which opens the header of each block the model writes.
    header: Token,
    thinking: Token,
    text: Token,
    call: Token,
    /// `<|content_invoke_tool_text|>`, a call written as text, which is read as content.
    call_text: Token,
    tool_error: Token,
    end: Token,
    /// `<|content_model_end_sampling|>`, which ends the reply: the vocabulary's
    /// end-of-generation token.
    done: Token,
}

impl Markers {
    /// The markers in `vocab`, when it holds each one as a single token.
    pub(crate) fn find(vocab: Vocab<'_>) -> Option<Self> {
        let one = |text: &str| match vocab.tokenize(text, false, true).ok()?.as_slice() {
            [token] => Some(*token),
            _ => None,
        };
        Some(Self {
            header: one(HEADER)?,
            thinking: one(THINKING)?,
            text: one(TEXT)?,
            call: one(CALL)?,
            call_text: one(CALL_TEXT)?,
            tool_error: one(TOOL_ERROR)?,
            end: one(END)?,
            done: one(DONE)?,
        })
    }

    /// GBNF terminals matching each marker's token alone, never text that spells it, as
    /// `(header, thinking, text, call, end)`: a grammar that let a reply spell a marker would
    /// let it write a block this reader cannot read.
    pub(crate) fn terminals(&self) -> [String; 5] {
        [self.header, self.thinking, self.text, self.call, self.end]
            .map(|token| format!("<[{}]>", token.0))
    }

    /// `text` as the markers it spells, each as its token with its text, and the text between
    /// them, as a prompt tokenized with its markers as tokens holds it.
    fn spell<'t>(&self, text: &'t str) -> Vec<(Option<Token>, &'t str)> {
        reply::spell(
            text,
            &[
                (HEADER, self.header),
                (THINKING, self.thinking),
                (TEXT, self.text),
                (CALL, self.call),
                (CALL_TEXT, self.call_text),
                (TOOL_ERROR, self.tool_error),
                (END, self.end),
                (DONE, self.done),
            ],
        )
    }

    /// Whether `token` opens a block's body: after its header, or in place of one.
    fn opens(&self, token: Token) -> bool {
        [
            self.thinking,
            self.text,
            self.call,
            self.call_text,
            self.tool_error,
        ]
        .contains(&token)
    }
}

/// Where the text of the block being read goes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    /// Between blocks, where a reply starts: text here is outside any block.
    Between,
    /// A block's header, after `<|message_model|>`: the block's name.
    Header(String),
    /// A thinking block's body: the reply's reasoning.
    Reasoning,
    /// A text block's body, or a tool-text or tool-error block's: the content. `json` is the
    /// body read so far when it is held to a `response_format`'s JSON.
    Content { json: Option<String> },
    /// A call block's body, `{"name":NAME,"args":ARGS}`, after a header that names `name`.
    Call { name: String, body: String },
}

/// Reads one reply.
#[derive(Debug, Clone)]
pub(crate) struct Reader {
    markers: Markers,
    /// The tools a call may name.
    tools: Vec<String>,
    /// Whether the reply may make more than one call; without parallel calls, its first call
    /// ends it.
    parallel: bool,
    held: Held,
    route: Route,
    /// The calls the reply has made, which it sends when it ends.
    calls: Vec<ToolCallResponse>,
    /// The reply's text the prompt wrote, which the grammar is to be advanced over before the
    /// reply goes on from it; empty when the reply starts between blocks.
    pending: String,
    /// The text the prompt wrote of the body of the reasoning or content block it opened,
    /// which is the start of the reply's.
    begun: String,
}

impl Reader {
    /// A reply starts between blocks, where Inkling's template leaves it; [`Reader::after`]
    /// starts it where a prompt that goes further leaves it.
    pub(crate) fn new(markers: Markers, tools: Vec<String>, held: Held) -> Self {
        Self {
            markers,
            tools,
            parallel: true,
            held,
            route: Route::Between,
            calls: Vec::new(),
            pending: String::new(),
            begun: String::new(),
        }
    }

    /// Allows the reply `parallel` calls, as it is by default, as `parallel_tool_calls` is; or
    /// not, so the reply ends at its first call's `<|end_message|>`, as llama.cpp's grammar
    /// then allows nothing after it.
    pub(crate) fn parallel_calls(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Starts the reply where `text`, what the rendered prompt leaves of it ([`reply_text`]),
    /// leaves it: between blocks when the prompt ends after a message, as Inkling's own
    /// template's does, or in the block a custom template's prompt opens, such as
    /// `<|message_model|><|content_thinking|>`. `text` is read as a reply is, so a marker marks
    /// only where it would in a reply, and a call goes on from the arguments the prompt began.
    /// Blocks `text` closes, and the calls they make, are the prompt's, not the reply's.
    ///
    /// A reply held to a grammar goes on from a block the prompt opened only once the grammar
    /// is advanced over that block's text ([`Reader::pending`]), which [`Reader::begin`] is
    /// told the outcome of; when the grammar does not allow it, the reply starts between
    /// blocks, where the grammar holds the whole reply.
    pub(crate) fn after(mut self, text: &str) -> Self {
        let open = self.read(text);
        self.calls.clear();
        if self.held != Held::Nothing
            && let Some(from) = open
        {
            self.pending = text[from..].to_string();
        }
        self
    }

    /// Reads `text` as a reply, keeping the text of the reasoning or content body it leaves
    /// open as [`Reader::begun`], and returns where the block it leaves open starts: at the
    /// header, or the content marker that opened it without one.
    fn read(&mut self, text: &str) -> Option<usize> {
        let (mut open, mut at) = (None, 0);
        for (token, piece) in self.markers.spell(text) {
            match token {
                Some(token) if self.marks(token, "") => {
                    if self.route == Route::Between || token == self.markers.header {
                        open = Some(at);
                    }
                    self.begun.clear();
                    self.mark(token);
                    if self.route == Route::Between {
                        open = None;
                    }
                }
                _ => {
                    if matches!(self.route, Route::Reasoning | Route::Content { .. }) {
                        self.begun.push_str(piece);
                    }
                    self.text(piece);
                }
            }
            at += piece.len();
        }
        open
    }

    /// The reply's text the prompt wrote, which the grammar is to be advanced over before the
    /// reply goes on from where the prompt left it; empty when there is none to advance over.
    pub(crate) fn pending(&self) -> &str {
        &self.pending
    }

    /// Starts the reply where the prompt left it when it `goes_on` from there, as it does
    /// unless the grammar would not be advanced over [`Reader::pending`], and returns the text
    /// the prompt began the opened block's body with, to send first. Otherwise the reply
    /// starts between blocks, where the grammar, built again at its root, holds it.
    pub(crate) fn begin(&mut self, goes_on: bool) -> String {
        if !goes_on {
            self.route = Route::Between;
            self.begun.clear();
        }
        self.pending.clear();
        std::mem::take(&mut self.begun)
    }

    pub(crate) fn markers(&self) -> &Markers {
        &self.markers
    }

    /// Whether a grammar holds the token that comes next.
    pub(crate) fn in_grammar(&self) -> bool {
        self.held != Held::Nothing
    }

    /// Whether the reply is in a header, whose text is read here rather than sent.
    pub(crate) fn in_header(&self) -> bool {
        matches!(self.route, Route::Header(_))
    }

    /// Whether `token` is a marker here, rather than text of the body it is in. `held` is the
    /// body's text that has not reached the reader yet, as a stop sequence holds it back.
    pub(crate) fn marks(&self, token: Token, held: &str) -> bool {
        let m = &self.markers;
        if token == m.done {
            return true;
        }
        match &self.route {
            Route::Between | Route::Header(_) => {
                token == m.header || token == m.end || m.opens(token)
            }
            Route::Reasoning | Route::Content { json: None } => token == m.end,
            Route::Content { json: Some(body) } | Route::Call { body, .. } => {
                token == m.end && closes(&format!("{body}{held}"))
            }
        }
    }

    /// Reads `token`, a marker.
    pub(crate) fn mark(&mut self, token: Token) -> Mark {
        let m = self.markers;
        if token == m.done {
            let calls = self.finish();
            return if calls.is_empty() {
                Mark::Done
            } else {
                Mark::Calls(calls)
            };
        }
        self.route = match std::mem::replace(&mut self.route, Route::Between) {
            // A header after a header leaves the first block empty.
            Route::Between | Route::Header(_) if token == m.header => Route::Header(String::new()),
            Route::Between => self.open(token, ""),
            Route::Header(name) => self.open(token, &name),
            // `<|end_message|>` closes the block.
            Route::Call { name, body } => {
                self.call(&name, &body);
                if !self.parallel && !self.calls.is_empty() {
                    return Mark::Calls(std::mem::take(&mut self.calls));
                }
                Route::Between
            }
            Route::Reasoning | Route::Content { .. } => Route::Between,
        };
        Mark::Next
    }

    /// The body `token` opens, after a header that names the block `name`; at
    /// `<|end_message|>`, a header closes with no body.
    fn open(&self, token: Token, name: &str) -> Route {
        let m = &self.markers;
        if token == m.thinking {
            Route::Reasoning
        } else if token == m.text {
            Route::Content {
                json: (self.held == Held::Reply).then(String::new),
            }
        } else if token == m.call {
            Route::Call {
                name: name.to_string(),
                body: String::new(),
            }
        } else if token == m.call_text || token == m.tool_error {
            Route::Content { json: None }
        } else {
            Route::Between
        }
    }

    /// Makes the call a closed call block holds, when it holds one to a tool the request
    /// offers ([`call`]).
    fn call(&mut self, name: &str, body: &str) {
        if let Some(call) = call(name, body, &self.tools) {
            self.calls.push(call);
        }
    }

    /// The calls the reply makes when it ends: those its call blocks made, and, when it ends
    /// inside a call block without its `<|end_message|>`, at a stop sequence or the token
    /// budget, that block's call too once its body is complete JSON, as llama.cpp's server
    /// reads a reply cut short.
    pub(crate) fn finish(&mut self) -> Vec<ToolCallResponse> {
        if let Route::Call { name, body } = std::mem::replace(&mut self.route, Route::Between) {
            self.call(&name, &body);
        }
        std::mem::take(&mut self.calls)
    }

    /// Reads `text`, which is not a marker, and says where it goes.
    pub(crate) fn text(&mut self, text: &str) -> Text {
        match &mut self.route {
            Route::Between | Route::Content { json: None } => Text::Content,
            Route::Content { json: Some(body) } => {
                body.push_str(text);
                Text::Content
            }
            Route::Reasoning => Text::Reasoning,
            Route::Header(name) => {
                name.push_str(text);
                Text::Held
            }
            Route::Call { body, .. } => {
                body.push_str(text);
                Text::Held
            }
        }
    }
}

/// The text of `prompt` a reply is read from ([`Reader::after`]): what the generation prompt
/// adds after the `conversation` the template renders without it, which is nothing for
/// Inkling's own template, whose prompt ends after the last message. When the conversation
/// leaves a block open, as a template that opens the reply's block whatever is asked for does,
/// it is read from that block's start. Without the conversation, or when the template renders
/// it differently without a generation prompt, the whole prompt is read for the block it
/// leaves open.
pub(crate) fn reply_text<'p>(
    markers: &Markers,
    prompt: &'p str,
    conversation: Option<&str>,
) -> &'p str {
    let read = conversation
        .filter(|conversation| prompt.starts_with(conversation))
        .unwrap_or(prompt);
    let from = open_block(markers, read).unwrap_or(read.len());
    &prompt[from..]
}

/// Where the block `text` leaves open starts, or `None` when `text` ends between blocks. The
/// text is read by the rules of a reply without a grammar, under which a text block ends at
/// its `<|end_message|>`, so each of a conversation's messages closes where the template
/// closes it.
fn open_block(markers: &Markers, text: &str) -> Option<usize> {
    Reader::new(*markers, Vec::new(), Held::Nothing).read(text)
}

/// Whether a JSON body, `body` so far, ends at `<|end_message|>`: once it is one complete
/// JSON value, or once it can never become one, as when it is empty or is not JSON. Only
/// inside a string the JSON has not finished can the marker be the body's own text.
fn closes(body: &str) -> bool {
    match serde_json::from_str::<&RawValue>(body) {
        Ok(_) => true,
        Err(e) => !e.is_eof() || body.trim().is_empty(),
    }
}

/// What a call block holds: exactly `{"name":NAME,"args":ARGS}`, in either order and any JSON
/// spacing, with no other key and neither key twice.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Invoke<'b> {
    name: String,
    #[serde(borrow)]
    args: &'b RawValue,
}

/// The call a closed call block makes, as llama.cpp's server reads one: its `header` names a
/// tool the request offers, and its `body` is the wrapper ([`Invoke`]) naming the same tool,
/// whose ARGS is a JSON object, which is the call's arguments as the model wrote them. Any
/// other call block makes no call.
fn call(header: &str, body: &str, tools: &[String]) -> Option<ToolCallResponse> {
    let invoke: Invoke<'_> = serde_json::from_str(body).ok()?;
    let arguments = invoke.args.get().trim();
    let made = !header.is_empty()
        && invoke.name == header
        && tools.iter().any(|tool| tool == header)
        && arguments.starts_with('{');
    made.then(|| reply::call(invoke.name, arguments))
}

/// Markers for tests, as a vocabulary with them as tokens 11 to 18 would hold them.
#[cfg(test)]
pub(crate) const TEST_MARKERS: Markers = Markers {
    header: Token(11),
    thinking: Token(12),
    text: Token(13),
    call: Token(14),
    call_text: Token(15),
    tool_error: Token(16),
    end: Token(17),
    done: Token(18),
};

/// A reply as [`TEST_MARKERS`] spell it: each marker as its token, with its text, and the text
/// between markers.
#[cfg(test)]
pub(crate) fn spell(reply: &str) -> Vec<(Option<Token>, String)> {
    TEST_MARKERS
        .spell(reply)
        .into_iter()
        .map(|(token, text)| (token, text.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a reply read: its reasoning, its content, its calls as `name(arguments)`, and
    /// whether it ended.
    #[derive(Debug, PartialEq, Eq)]
    struct Read {
        reasoning: String,
        content: String,
        calls: Vec<String>,
        ended: bool,
    }

    fn read(reply: &str, tools: &[&str]) -> Read {
        read_as(
            Reader::new(TEST_MARKERS, names(tools), Held::Nothing),
            reply,
        )
    }

    fn names(tools: &[&str]) -> Vec<String> {
        tools.iter().map(ToString::to_string).collect()
    }

    fn called(calls: Vec<ToolCallResponse>) -> Vec<String> {
        calls
            .into_iter()
            .map(|c| format!("{}({})", c.function.name, c.function.arguments))
            .collect()
    }

    /// Reads `reply` with `reader`, token by token, as the engine drives it.
    fn read_as(mut reader: Reader, reply: &str) -> Read {
        let mut read = Read {
            reasoning: String::new(),
            content: String::new(),
            calls: Vec::new(),
            ended: false,
        };
        for (token, text) in spell(reply) {
            if let Some(token) = token
                && reader.marks(token, "")
            {
                match reader.mark(token) {
                    Mark::Next => continue,
                    Mark::Calls(calls) => {
                        read.calls = called(calls);
                        read.ended = true;
                        return read;
                    }
                    Mark::Done => {
                        read.ended = true;
                        return read;
                    }
                }
            }
            match reader.text(&text) {
                Text::Reasoning => read.reasoning.push_str(&text),
                Text::Content => read.content.push_str(&text),
                Text::Held => {}
            }
        }
        read.calls = called(reader.finish());
        read
    }

    fn answer(reasoning: &str, content: &str, calls: &[&str]) -> Read {
        Read {
            reasoning: reasoning.to_string(),
            content: content.to_string(),
            calls: names(calls),
            ended: true,
        }
    }

    #[test]
    fn thinking_is_the_reasoning_and_text_the_content() {
        assert_eq!(
            read(
                "<|message_model|><|content_thinking|>Add them.<|end_message|><|message_model|><|content_text|>4<|end_message|><|content_model_end_sampling|>",
                &[]
            ),
            answer("Add them.", "4", &[])
        );
    }

    #[test]
    fn several_blocks_read_in_turn() {
        assert_eq!(
            read(
                "<|message_model|><|content_thinking|>One.<|end_message|><|message_model|><|content_text|>A<|end_message|><|message_model|><|content_thinking|> Two.<|end_message|><|message_model|><|content_text|>B<|end_message|><|content_model_end_sampling|>",
                &[]
            ),
            answer("One. Two.", "AB", &[])
        );
    }

    #[test]
    fn a_call_block_is_a_call_with_its_args_as_arguments() {
        assert_eq!(
            read(
                "<|message_model|><|content_thinking|>Look it up.<|end_message|><|message_model|>get_weather<|content_invoke_tool_json|>{\"name\":\"get_weather\",\"args\":{\"city\":\"Paris\"}}<|end_message|><|content_model_end_sampling|>",
                &["get_weather"]
            ),
            answer("Look it up.", "", &["get_weather({\"city\":\"Paris\"})"])
        );
        // the arguments as the model wrote them, spacing included
        assert_eq!(
            read(
                "<|message_model|>f<|content_invoke_tool_json|>{\"name\": \"f\", \"args\": {\"a\": [1, 2]}}<|end_message|><|content_model_end_sampling|>",
                &["f"]
            ),
            answer("", "", &["f({\"a\": [1, 2]})"])
        );
    }

    /// Without parallel calls, the first call a reply makes ends it at its `<|end_message|>`,
    /// as llama.cpp's grammar then allows nothing after it; a dropped call block makes no call,
    /// so it does not.
    #[test]
    fn without_parallel_calls_the_first_call_ends_the_reply() {
        let one_call =
            || Reader::new(TEST_MARKERS, names(&["f", "g"]), Held::Nothing).parallel_calls(false);
        let mut reader = one_call();
        for (token, text) in spell(
            "<|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{\"a\":1}}",
        ) {
            match token {
                Some(token) => assert!(matches!(reader.mark(token), Mark::Next)),
                None => assert_eq!(reader.text(&text), Text::Held),
            }
        }
        let Mark::Calls(calls) = reader.mark(TEST_MARKERS.end) else {
            panic!("the call's `<|end_message|>` ends the reply");
        };
        assert_eq!(called(calls), ["f({\"a\":1})"]);
        assert_eq!(
            read_as(
                one_call(),
                "<|message_model|>h<|content_invoke_tool_json|>{\"name\":\"h\",\"args\":{}}<|end_message|><|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{}}<|end_message|><|message_model|>g<|content_invoke_tool_json|>{\"name\":\"g\",\"args\":{}}<|end_message|><|content_model_end_sampling|>"
            ),
            answer("", "", &["f({})"])
        );
    }

    /// A call block does not end the reply: a turn's calls are its last blocks, and each is
    /// made when the reply ends.
    #[test]
    fn parallel_call_blocks_are_all_made() {
        assert_eq!(
            read(
                "<|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{\"a\":1}}<|end_message|><|message_model|>g<|content_invoke_tool_json|>{\"name\":\"g\",\"args\":{}}<|end_message|><|content_model_end_sampling|>",
                &["f", "g"]
            ),
            answer("", "", &["f({\"a\":1})", "g({})"])
        );
    }

    #[test]
    fn a_call_to_a_tool_not_offered_is_dropped() {
        assert_eq!(
            read(
                "<|message_model|>get_time<|content_invoke_tool_json|>{\"name\":\"get_time\",\"args\":{}}<|end_message|><|message_model|><|content_text|>ok<|end_message|><|content_model_end_sampling|>",
                &["get_weather"]
            ),
            answer("", "ok", &[])
        );
        // beside a call to one that is
        assert_eq!(
            read(
                "<|message_model|>get_time<|content_invoke_tool_json|>{\"name\":\"get_time\",\"args\":{}}<|end_message|><|message_model|>get_weather<|content_invoke_tool_json|>{\"name\":\"get_weather\",\"args\":{}}<|end_message|><|content_model_end_sampling|>",
                &["get_weather"]
            ),
            answer("", "", &["get_weather({})"])
        );
    }

    /// A call block is made only as llama.cpp's server reads one: its header names an offered
    /// tool, and its body is `{"name":NAME,"args":{…}}` naming the same tool, with no other key,
    /// in either order and any JSON spacing. Any other call block is dropped.
    #[test]
    fn a_call_names_its_tool_in_its_header_and_its_wrapper() {
        let tools = ["f", "g"];
        let block = |header: &str, body: &str| {
            read(
                &format!(
                    "<|message_model|>{header}<|content_invoke_tool_json|>{body}<|end_message|><|content_model_end_sampling|>"
                ),
                &tools,
            )
        };
        let dropped = [
            // no header name
            ("", "{\"name\":\"g\",\"args\":{}}"),
            // the header and the wrapper name different tools
            ("f", "{\"name\":\"g\",\"args\":{}}"),
            // a key beside the two, or one of them twice
            ("f", "{\"name\":\"f\",\"args\":{},\"id\":1}"),
            ("f", "{\"name\":\"f\",\"name\":\"f\",\"args\":{}}"),
            // arguments that are not an object
            ("f", "{\"name\":\"f\",\"args\":[1]}"),
            ("f", "{\"name\":\"f\",\"args\":\"{}\"}"),
            // a missing key, a name that is not a string, another key, or no JSON at all
            ("f", "{\"name\":\"f\"}"),
            ("f", "{\"name\":1,\"args\":{}}"),
            ("f", "{\"name\":\"f\",\"arguments\":{}}"),
            ("f", "not json"),
            // a header that names the tool with more than its name
            (" f", "{\"name\":\"f\",\"args\":{}}"),
        ];
        assert_eq!(
            dropped.map(|(header, body)| ((header, body), block(header, body))),
            dropped.map(|case| (case, answer("", "", &[])))
        );
        // the keys in either order, with JSON spacing: the arguments as the model wrote them
        assert_eq!(
            block("f", "{ \"args\": {\"a\": 1}, \"name\": \"f\" }"),
            answer("", "", &["f({\"a\": 1})"])
        );
    }

    #[test]
    fn marker_text_in_a_body_is_text() {
        // a header or content marker as a token in a body marks nothing
        assert_eq!(
            read(
                "<|message_model|><|content_text|>a<|message_model|>b<|content_thinking|>c<|end_message|><|content_model_end_sampling|>",
                &[]
            ),
            answer("", "a<|message_model|>b<|content_thinking|>c", &[])
        );
        assert_eq!(
            read(
                "<|message_model|><|content_thinking|>x<|content_text|>y<|end_message|><|content_model_end_sampling|>",
                &[]
            ),
            answer("x<|content_text|>y", "", &[])
        );
    }

    /// A call's body ends at `<|end_message|>` only once it is complete JSON, so the marker a
    /// string spells is the string's.
    #[test]
    fn a_calls_json_may_spell_its_end_marker_inside_a_string() {
        assert_eq!(
            read(
                "<|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{\"s\":\"a<|end_message|>b\"}}<|end_message|><|content_model_end_sampling|>",
                &["f"]
            ),
            answer("", "", &["f({\"s\":\"a<|end_message|>b\"})"])
        );
        // and a body that can never become JSON ends at the marker, making no call, rather
        // than taking the rest of the reply
        assert_eq!(
            read(
                "<|message_model|>f<|content_invoke_tool_json|>oops<|end_message|><|message_model|><|content_text|>sorry<|end_message|><|content_model_end_sampling|>",
                &["f"]
            ),
            answer("", "sorry", &[])
        );
    }

    /// A text block held to a `response_format` is JSON, which ends only once complete; one
    /// that is not held ends at its first `<|end_message|>`.
    #[test]
    fn a_structured_text_block_may_spell_its_end_marker_inside_a_string() {
        let reply = "<|message_model|><|content_text|>{\"s\":\"<|end_message|>\"}<|end_message|><|content_model_end_sampling|>";
        assert_eq!(
            read_as(Reader::new(TEST_MARKERS, Vec::new(), Held::Reply), reply),
            answer("", "{\"s\":\"<|end_message|>\"}", &[])
        );
        assert_eq!(read(reply, &[]), answer("", "{\"s\":\"\"}", &[]));
    }

    /// A reply cut short, at the budget or a stop sequence, makes the calls it has made, and
    /// a call whose body is already complete JSON though its `<|end_message|>` never came.
    #[test]
    fn a_reply_cut_short_after_complete_call_json_still_makes_the_call() {
        let tools = ["f", "g"];
        assert_eq!(
            read(
                "<|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{}}<|end_message|><|message_model|>g<|content_invoke_tool_json|>{\"name\":\"g\",\"args\":{\"a\":1}}",
                &tools
            ),
            Read {
                ended: false,
                ..answer("", "", &["f({})", "g({\"a\":1})"])
            }
        );
        // a body cut short makes no call
        assert_eq!(
            read(
                "<|message_model|>g<|content_invoke_tool_json|>{\"name\":\"g\",\"args\":{\"a\":",
                &tools
            ),
            Read {
                ended: false,
                ..answer("", "", &[])
            }
        );
        // nor does the reply's end inside a call: the end-of-generation token makes it too
        assert_eq!(
            read(
                "<|message_model|>g<|content_invoke_tool_json|>{\"name\":\"g\",\"args\":{}}<|content_model_end_sampling|>",
                &tools
            ),
            answer("", "", &["g({})"])
        );
    }

    #[test]
    fn a_call_is_complete_with_the_text_a_stop_sequence_holds_back() {
        // a stop sequence that may begin at the last `}` holds it back, yet the body it closes
        // is complete, so the `<|end_message|>` after it ends the call
        let mut reader = Reader::new(TEST_MARKERS, names(&["f"]), Held::Nothing);
        reader.mark(TEST_MARKERS.header);
        reader.text("f");
        reader.mark(TEST_MARKERS.call);
        reader.text("{\"name\":\"f\",\"args\":{}");
        assert!(
            !reader.marks(TEST_MARKERS.end, ""),
            "the body is not complete"
        );
        assert!(reader.marks(TEST_MARKERS.end, "}"));
    }

    /// Text outside any block, as a model that skips a header writes, is content, not dropped;
    /// so is a block opened by its content marker alone, and a tool-text or tool-error block.
    #[test]
    fn text_outside_any_block_is_content() {
        assert_eq!(
            read(
                "Hello<|message_model|><|content_thinking|>r<|end_message|> there<|content_model_end_sampling|>",
                &[]
            ),
            answer("r", "Hello there", &[])
        );
        assert_eq!(
            read(
                "<|content_thinking|>r<|end_message|><|content_text|>a<|end_message|><|message_model|><|content_tool_error|>b<|end_message|><|message_model|>x<|content_invoke_tool_text|>c<|end_message|><|content_model_end_sampling|>",
                &[]
            ),
            answer("r", "abc", &[])
        );
    }

    /// Inkling's template has no generation prompt: the prompt ends after the last message's
    /// `<|end_message|>`, and the reply starts between blocks, with nothing the grammar is to
    /// be advanced over and nothing begun.
    #[test]
    fn a_reply_starts_between_blocks_after_a_prompt_with_no_generation_prompt() {
        let prompt = "<|message_system|>tool_declare<|content_xml|>[]<|end_message|><|message_user|><|content_text|>Hi<|end_message|><|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{\"s\":\"}\"}}<|end_message|><|content_model_end_sampling|><|message_tool|>f<|content_text|>{}<|end_message|>";
        // the same conversation without a generation prompt, as Inkling's template renders it
        let text = reply_text(&TEST_MARKERS, prompt, Some(prompt));
        assert_eq!(text, "");
        for held in [Held::Nothing, Held::Reply, Held::Call] {
            let mut reader = Reader::new(TEST_MARKERS, names(&["f"]), held).after(text);
            let pending = reader.pending().to_string();
            let begun = reader.begin(true);
            assert_eq!(
                (&reader.route, pending.as_str(), begun.as_str()),
                (&Route::Between, "", ""),
                "{held:?}"
            );
        }
        // read whole, without a conversation to tell what the generation prompt adds, the
        // prompt still leaves the reply between blocks, its own call made by an earlier turn
        let text = reply_text(&TEST_MARKERS, prompt, None);
        assert_eq!(text, "");
        let reader = Reader::new(TEST_MARKERS, names(&["f"]), Held::Nothing).after(prompt);
        assert_eq!((&reader.route, reader.calls.len()), (&Route::Between, 0));
        // and the reply that follows is read from between blocks
        assert_eq!(
            read_as(
                Reader::new(TEST_MARKERS, Vec::new(), Held::Nothing).after(text),
                "<|message_model|><|content_thinking|>r<|end_message|><|message_model|><|content_text|>c<|end_message|><|content_model_end_sampling|>"
            ),
            answer("r", "c", &[])
        );
    }

    /// A custom template may open the reply's block in its generation prompt, or whatever is
    /// asked for; the reply goes on in that block, and under a grammar the block's text is
    /// what the grammar is to be advanced over.
    #[test]
    fn a_reply_goes_on_in_the_block_a_prompt_opens() {
        let conversation = "<|message_user|><|content_text|>Hi<|end_message|>";
        let thinking = format!("{conversation}<|message_model|><|content_thinking|>Let me");
        let text = reply_text(&TEST_MARKERS, &thinking, Some(conversation));
        assert_eq!(text, "<|message_model|><|content_thinking|>Let me");
        let mut free = Reader::new(TEST_MARKERS, Vec::new(), Held::Nothing).after(text);
        assert_eq!(free.pending(), "");
        assert_eq!(free.begin(true), "Let me");
        assert_eq!(free.route, Route::Reasoning);
        // written whatever is asked for, so the conversation leaves the block open
        assert_eq!(
            reply_text(&TEST_MARKERS, &thinking, Some(&thinking)),
            "<|message_model|><|content_thinking|>Let me"
        );
        // a structured text block's JSON, which spells the end marker in a string
        let structured = format!(
            "{conversation}<|message_model|><|content_text|>{{\"note\":\"<|end_message|>\",\"a\":\"Ly"
        );
        let text = reply_text(&TEST_MARKERS, &structured, Some(conversation));
        let mut held = Reader::new(TEST_MARKERS, Vec::new(), Held::Reply).after(text);
        assert_eq!(held.pending(), text);
        let mut refused = held.clone();
        assert_eq!(held.begin(true), "{\"note\":\"<|end_message|>\",\"a\":\"Ly");
        assert_eq!(
            held.route,
            Route::Content {
                json: Some("{\"note\":\"<|end_message|>\",\"a\":\"Ly".to_string())
            }
        );
        // a grammar that does not allow the block starts the reply between blocks
        assert_eq!(refused.begin(false), "");
        assert_eq!(
            (refused.route, refused.pending),
            (Route::Between, String::new())
        );
        // a call's arguments the prompt began are the call's, not text to send
        let call = format!(
            "{conversation}<|message_model|>f<|content_invoke_tool_json|>{{\"name\":\"f\",\"args\":{{\"a\":"
        );
        let mut reader = Reader::new(TEST_MARKERS, names(&["f"]), Held::Call).after(reply_text(
            &TEST_MARKERS,
            &call,
            Some(conversation),
        ));
        assert_eq!(
            reader.pending(),
            "<|message_model|>f<|content_invoke_tool_json|>{\"name\":\"f\",\"args\":{\"a\":"
        );
        assert_eq!(reader.begin(true), "");
        assert_eq!(
            read_as(reader, "1}}<|end_message|><|content_model_end_sampling|>"),
            answer("", "", &["f({\"a\":1})"])
        );
    }

    #[test]
    fn a_grammar_holds_the_whole_reply() {
        for held in [Held::Reply, Held::Call] {
            let mut reader = Reader::new(TEST_MARKERS, Vec::new(), held);
            assert!(reader.in_grammar());
            reader.mark(TEST_MARKERS.header);
            assert!(reader.in_header());
            reader.mark(TEST_MARKERS.thinking);
            assert!(reader.in_grammar() && !reader.in_header(), "{held:?}");
        }
        assert!(!Reader::new(TEST_MARKERS, Vec::new(), Held::Nothing).in_grammar());
    }

    #[test]
    fn a_template_is_inklings_by_its_user_and_image_markers() {
        assert!(is_inkling_template(
            "{{ '<|message_user|>' }}{{ '<|content_image|>' }}"
        ));
        assert!(!is_inkling_template("{{ '<|message_user|>' }}"));
        assert!(!is_inkling_template(
            "<|im_start|>user<|im_end|><|content_image|>"
        ));
    }
}
