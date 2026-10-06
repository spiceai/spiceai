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

//! Reassembling Server-Sent Events from a byte stream.
//!
//! Chunk boundaries in an HTTP body are a property of the transport, not of the protocol
//! carried over it: an SSE event can straddle two reads, and a multi-byte character can
//! straddle two reads inside that. A reader that treats each chunk as a whole number of
//! lines loses whichever events happen to be split, silently — there is nothing malformed
//! about the stream, only about the reading of it.
//!
//! [`SseDecoder`] buffers what has arrived and yields only events it has seen the end of.
//! Bytes are held rather than decoded on arrival, so a character split across two reads is
//! reassembled instead of becoming a replacement character: `\n` cannot occur inside a
//! multi-byte UTF-8 sequence, so a line boundary is always a character boundary too.
//!
//! The field grammar is the one from the [HTML standard](https://html.spec.whatwg.org/multipage/server-sent-events.html):
//! a line of `field: value`, a comment line starting with `:`, and a blank line ending the
//! event. All three terminators the standard allows are accepted — `\n`, `\r\n` and a lone
//! `\r`. The last is not hypothetical: axum's `Event::data` writes the payload's own line
//! breaks through verbatim, so a value containing a `\r` is framed with one
//! (`axum::response::sse`, `EventDataWriter::write_buf`), and `sse-starlette`, which serves
//! many OpenAI-compatible endpoints, can be configured to separate with `\r` throughout.
//!
//! Dispatch follows the standard too: a blank line ends an event only if a `data` field was
//! seen. A frame carrying just `event: ping` is *not* an event — dispatching one would tell
//! the caller the stream is producing when it is not.
//!
//! `eventsource-stream` is already in this binary's dependency tree (through `async-openai`)
//! and covers the framing above. It is not used here because it yields whole events and
//! nothing else, and the three things this decoder is for all live outside that: the caller
//! needs to know a `data` line arrived *before* its event ends, to tell a large event still
//! being received from a stalled stream; it needs the event a server left unterminated when
//! it closed, which that crate drops; and it needs a bound on how much one unterminated event
//! may buffer, which that crate does not impose.

/// The most bytes one event may occupy before the decoder refuses to keep buffering.
///
/// A stream that never sends a line terminator would otherwise grow this buffer without
/// bound. Individual chat completion chunks are a few hundred bytes; a tool call carrying a
/// large argument payload is the realistic upper end, and stays far below this.
pub(crate) const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// One dispatched SSE event.
#[derive(Debug)]
pub(crate) struct SseEvent {
    /// The `event:` field, when the stream set one.
    pub(crate) name: Option<String>,
    /// The `data:` fields, joined with newlines as the standard specifies.
    pub(crate) data: String,
}

/// Reassembles [`SseEvent`]s from a byte stream delivered in arbitrary pieces.
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    /// Bytes received but not yet handed out. Lines are taken by advancing `cursor` rather
    /// than by draining, so a read carrying many events costs one compaction and not one
    /// memmove of the remainder per event.
    pending: Vec<u8>,
    /// How much of `pending` has already been read out.
    cursor: usize,
    /// The `event:` field of the event being accumulated.
    name: Option<String>,
    /// The `data:` fields of the event being accumulated, already joined.
    data: String,
    /// Whether a `data` field has been read since the last dispatch. The standard dispatches
    /// on a blank line only when the data buffer has been written to, so a frame of nothing
    /// but `event:` or a comment ends no event.
    data_seen: bool,
    /// How many `data` fields have been read in total. A caller bounding the stream's
    /// liveness uses this to tell a partly-received event from a stalled one.
    data_fields: u64,
    /// Whether a leading byte-order mark has been dealt with.
    bom_checked: bool,
    /// Whether the stream has ended, so a held-back `\r` is a terminator rather than half of
    /// a `\r\n` still arriving.
    at_eof: bool,
}

/// The byte-order mark the standard requires be stripped from the head of the stream.
const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

impl SseDecoder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Take another piece of the stream.
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        self.compact();
        self.pending.extend_from_slice(bytes);
        self.strip_bom();
    }

    /// Drop what has already been read out, so the buffer holds only the unread remainder.
    fn compact(&mut self) {
        if self.cursor > 0 {
            self.pending.drain(..self.cursor);
            self.cursor = 0;
        }
    }

    /// The bytes received but not yet read out.
    fn unread(&self) -> &[u8] {
        &self.pending[self.cursor..]
    }

    /// Tell the decoder that no more bytes are coming, so what it is holding is all there is.
    pub(crate) fn close(&mut self) {
        self.at_eof = true;
    }

    /// How many bytes one unterminated event has grown to, once past [`MAX_EVENT_BYTES`].
    ///
    /// Ask this only after draining with [`SseDecoder::next_event`]: before that the buffer
    /// also holds whole events waiting to be read, and one transport read may carry many of
    /// them. The limit is on an event the stream never ends, not on how much arrives at once.
    pub(crate) fn oversized_bytes(&self) -> Option<usize> {
        let buffered = self.unread().len() + self.data.len();
        (buffered > MAX_EVENT_BYTES).then_some(buffered)
    }

    /// How many `data` fields have been read so far.
    ///
    /// This advances when a data line is read, not when the event it belongs to is
    /// dispatched, so a caller can tell a large event still arriving from a stalled stream.
    pub(crate) fn data_fields_seen(&self) -> u64 {
        self.data_fields
    }

    /// Drop a leading BOM, waiting for as many bytes as it takes to know there is not one.
    fn strip_bom(&mut self) {
        if self.bom_checked {
            return;
        }

        if self.unread().len() >= BOM.len() {
            if self.unread().starts_with(&BOM) {
                self.cursor += BOM.len();
            }
            self.bom_checked = true;
        } else if !BOM.starts_with(self.unread()) {
            // What has arrived already rules a BOM out.
            self.bom_checked = true;
        }
    }

    /// Yield the next event that has arrived in full, if any.
    ///
    /// Call this until it returns `None`: one read can carry several events. After
    /// [`SseDecoder::close`] it also yields what the stream left unterminated at its end.
    ///
    /// The standard discards an event the stream did not terminate. That rule guards a
    /// consumer that would act on half a payload; this one cannot, because the payload still
    /// has to parse as a whole. And the tail is only reached after a *complete* HTTP body —
    /// a body cut short surfaces as a transport error before the stream ends — so a server
    /// that simply closed without its final blank line has its last event read rather than
    /// dropped, while a genuinely truncated payload still fails to parse.
    pub(crate) fn next_event(&mut self) -> Option<SseEvent> {
        loop {
            while let Some(line) = self.take_line() {
                if let Some(event) = self.read_field(&line) {
                    return Some(event);
                }
            }

            if !self.at_eof {
                return None;
            }

            if self.unread().is_empty() {
                return self.data_seen.then(|| self.dispatch());
            }

            let line = String::from_utf8_lossy(self.unread()).into_owned();
            self.cursor = self.pending.len();
            if let Some(event) = self.read_field(&line) {
                return Some(event);
            }
        }
    }

    /// Split off the next complete line, without its terminator.
    ///
    /// All three terminators the standard allows are recognised. A `\r` at the very end of
    /// the buffer is held back rather than treated as a line ending: the `\n` that would
    /// make it a `\r\n` may be in the next read, and splitting the pair would invent a blank
    /// line that ends an event early. Once the stream is closed there is no next read, so the
    /// `\r` is a terminator of its own.
    fn take_line(&mut self) -> Option<String> {
        let unread = self.unread();
        let end = unread
            .iter()
            .position(|byte| *byte == b'\n' || *byte == b'\r')?;

        let carriage_return = unread[end] == b'\r';
        if carriage_return && end + 1 == unread.len() && !self.at_eof {
            return None;
        }

        let terminator = if carriage_return && unread.get(end + 1) == Some(&b'\n') {
            2
        } else {
            1
        };

        let line = String::from_utf8_lossy(&unread[..end]).into_owned();
        self.cursor += end + terminator;

        Some(line)
    }

    /// Apply one line to the event being accumulated, returning an event if it ended one.
    fn read_field(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            if self.data_seen {
                return Some(self.dispatch());
            }

            // The standard clears the buffers and dispatches nothing when the data buffer is
            // empty, so a frame of only `event:` or comments is not an event.
            self.name = None;
            return None;
        }

        // A comment. The runtime's keep-alive is one of these: it says the connection is up,
        // not that anything has been produced, so it must not start an event.
        if line.starts_with(':') {
            return None;
        }

        let (field, value) = match line.split_once(':') {
            // The standard strips a single leading space, and only one.
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            // A field with no colon carries an empty value.
            None => (line, ""),
        };

        match field {
            "data" => {
                if self.data_seen {
                    self.data.push('\n');
                }
                self.data_seen = true;
                self.data_fields += 1;
                self.data.push_str(value);
            }
            "event" => {
                self.name = Some(value.to_string());
            }
            // `id` and `retry` steer reconnection, which this client does not do. Unknown
            // fields are ignored by the standard.
            _ => {}
        }

        None
    }

    /// Emit the accumulated event and reset for the next one.
    fn dispatch(&mut self) -> SseEvent {
        self.data_seen = false;

        SseEvent {
            name: self.name.take(),
            data: std::mem::take(&mut self.data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `pieces` to a decoder in order and collect everything it dispatches, including
    /// the tail. The split between pieces is what each test is varying.
    fn decode(pieces: &[&str]) -> Vec<SseEvent> {
        let mut decoder = SseDecoder::new();
        let mut events = Vec::new();

        for piece in pieces {
            decoder.push(piece.as_bytes());
            while let Some(event) = decoder.next_event() {
                events.push(event);
            }
            assert!(
                decoder.oversized_bytes().is_none(),
                "test payloads are far below the size cap"
            );
        }

        decoder.close();
        while let Some(event) = decoder.next_event() {
            events.push(event);
        }
        events
    }

    fn data_of(events: &[SseEvent]) -> Vec<&str> {
        events.iter().map(|event| event.data.as_str()).collect()
    }

    #[test]
    fn an_event_delivered_whole_is_dispatched() {
        let events = decode(&["data: {\"a\":1}\n\n"]);

        assert_eq!(data_of(&events), vec!["{\"a\":1}"]);
        assert_eq!(events[0].name, None);
    }

    /// The defect this module exists for: the same event, split mid-payload across two
    /// reads. Reading each read as whole lines drops both halves — the first is truncated
    /// JSON, the second has no `data:` prefix.
    #[test]
    fn an_event_split_mid_payload_survives() {
        let events = decode(&["data: {\"cho", "ices\":[]}\n\n"]);

        assert_eq!(data_of(&events), vec!["{\"choices\":[]}"]);
    }

    #[test]
    fn an_event_split_inside_its_field_name_survives() {
        let events = decode(&["da", "ta: hello\n\n"]);

        assert_eq!(data_of(&events), vec!["hello"]);
    }

    #[test]
    fn an_event_split_between_its_payload_and_its_terminator_survives() {
        let events = decode(&["data: hello\n", "\n"]);

        assert_eq!(data_of(&events), vec!["hello"]);
    }

    /// A multi-byte character split across two reads must be reassembled, not replaced.
    /// Decoding each read on its own turns the halves into U+FFFD.
    #[test]
    fn a_character_split_across_reads_is_reassembled() {
        let snowman = "☃".as_bytes();
        let mut decoder = SseDecoder::new();

        decoder.push(b"data: ");
        decoder.push(&snowman[..1]);
        assert!(decoder.next_event().is_none());
        decoder.push(&snowman[1..]);
        decoder.push(b"\n\n");

        let event = decoder.next_event().expect("the event is complete");
        assert_eq!(event.data, "☃");
    }

    #[test]
    fn several_events_in_one_read_are_all_dispatched() {
        let events = decode(&["data: one\n\ndata: two\n\ndata: three\n\n"]);

        assert_eq!(data_of(&events), vec!["one", "two", "three"]);
    }

    #[test]
    fn a_comment_does_not_dispatch_an_event() {
        let events = decode(&[": keep-alive\n\n", "data: real\n\n"]);

        assert_eq!(data_of(&events), vec!["real"]);
    }

    #[test]
    fn an_event_name_is_reported_with_its_payload() {
        let events = decode(&["event: error\ndata: {\"message\":\"boom\"}\n\n"]);

        assert_eq!(events[0].name.as_deref(), Some("error"));
        assert_eq!(events[0].data, "{\"message\":\"boom\"}");
    }

    #[test]
    fn a_name_does_not_carry_over_to_the_next_event() {
        let events = decode(&["event: error\ndata: first\n\n", "data: second\n\n"]);

        assert_eq!(events[0].name.as_deref(), Some("error"));
        assert_eq!(events[1].name, None);
    }

    /// The standard joins repeated `data:` fields with a newline.
    #[test]
    fn repeated_data_fields_are_joined_with_newlines() {
        let events = decode(&["data: one\ndata: two\n\n"]);

        assert_eq!(data_of(&events), vec!["one\ntwo"]);
    }

    #[test]
    fn crlf_terminators_are_accepted() {
        let events = decode(&["data: hello\r\n\r\n"]);

        assert_eq!(data_of(&events), vec!["hello"]);
    }

    /// Only one leading space is stripped, so a payload's own indentation survives.
    #[test]
    fn only_one_leading_space_is_stripped_from_a_value() {
        let events = decode(&["data:  padded\n\n"]);

        assert_eq!(data_of(&events), vec![" padded"]);
    }

    #[test]
    fn a_field_with_no_value_is_read_as_empty() {
        let events = decode(&["data\n\n"]);

        assert_eq!(data_of(&events), vec![""]);
    }

    /// A server that closes without its final blank line still gets its last event read.
    #[test]
    fn an_unterminated_final_event_is_dispatched_at_the_end() {
        let events = decode(&["data: last"]);

        assert_eq!(data_of(&events), vec!["last"]);
    }

    #[test]
    fn a_stream_that_ends_cleanly_dispatches_nothing_extra() {
        let events = decode(&["data: only\n\n"]);

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn a_stream_of_only_comments_dispatches_nothing() {
        let events = decode(&[": one\n", ": two\n"]);

        assert!(events.is_empty());
    }

    /// A stream that never terminates a line must not grow the buffer without bound.
    #[test]
    fn an_event_past_the_size_cap_is_refused() {
        let mut decoder = SseDecoder::new();
        let mut pushed = 0usize;

        while decoder.oversized_bytes().is_none() {
            let chunk = vec![b'x'; 1024 * 1024];
            decoder.push(&chunk);
            pushed += chunk.len();
            assert!(
                pushed <= MAX_EVENT_BYTES + chunk.len(),
                "the decoder buffered {pushed} bytes without refusing"
            );
        }

        let buffered = decoder
            .oversized_bytes()
            .expect("the loop ended on the cap");
        assert!(buffered > MAX_EVENT_BYTES);
    }

    /// The cap bounds one unterminated event, not one transport read. A read that happens to
    /// carry many whole small events is ordinary, and rejecting it would report an event the
    /// stream never ended when in fact every event ended.
    #[test]
    fn many_whole_events_in_one_read_are_not_over_the_cap() {
        let event = "data: {\"choices\":[]}\n\n";
        let repeats = (MAX_EVENT_BYTES / event.len()) + 16;
        let mut decoder = SseDecoder::new();

        decoder.push(event.repeat(repeats).as_bytes());

        let mut dispatched = 0usize;
        while decoder.next_event().is_some() {
            dispatched += 1;
        }

        assert_eq!(dispatched, repeats);
        assert!(
            decoder.oversized_bytes().is_none(),
            "every event was terminated, so nothing is over the cap"
        );
    }

    /// A frame carrying no `data` field is not an event. Dispatching one would tell a caller
    /// that bounds liveness by events that the stream is producing when it is not.
    #[test]
    fn a_frame_with_only_an_event_name_dispatches_nothing() {
        let events = decode(&["event: ping\n\n", "event: ping\n\n"]);

        assert!(events.is_empty(), "got {events:?}");
    }

    /// ...and the name it carried must not leak onto the next event.
    #[test]
    fn a_name_from_an_undispatched_frame_does_not_leak() {
        let events = decode(&["event: ping\n\n", "data: real\n\n"]);

        assert_eq!(data_of(&events), vec!["real"]);
        assert_eq!(events[0].name, None);
    }

    /// axum writes a payload's own line breaks through verbatim, so a value containing a
    /// carriage return is framed with a lone `\r`.
    #[test]
    fn a_lone_carriage_return_terminates_a_line() {
        let events = decode(&["data: one\rdata: two\r\r"]);

        assert_eq!(data_of(&events), vec!["one\ntwo"]);
    }

    /// A `\r` at the end of a read may be the first half of a `\r\n` still arriving.
    /// Splitting the pair would invent a blank line and end the event early.
    #[test]
    fn a_carriage_return_split_from_its_newline_is_not_a_blank_line() {
        let events = decode(&["data: whole\r", "\ndata: same event\r\n\r\n"]);

        assert_eq!(data_of(&events), vec!["whole\nsame event"]);
    }

    /// Once the stream is closed there is no next read, so a held-back `\r` is a terminator.
    #[test]
    fn a_trailing_carriage_return_at_the_end_of_the_stream_is_a_terminator() {
        let events = decode(&["data: last\r"]);

        assert_eq!(data_of(&events), vec!["last"]);
    }

    /// The standard strips a byte-order mark from the head of the stream. Reading it as part
    /// of the first field name would silently empty the first event.
    #[test]
    fn a_leading_byte_order_mark_is_stripped() {
        let events = decode(&["\u{feff}data: first\n\n"]);

        assert_eq!(data_of(&events), vec!["first"]);
    }

    #[test]
    fn a_byte_order_mark_split_across_reads_is_stripped() {
        let bom = "\u{feff}".as_bytes();
        let mut decoder = SseDecoder::new();

        decoder.push(&bom[..1]);
        decoder.push(&bom[1..]);
        decoder.push(b"data: first\n\n");

        let event = decoder.next_event().expect("the event is complete");
        assert_eq!(event.data, "first");
    }

    /// Only at the head of the stream: the same bytes later are the payload's own.
    #[test]
    fn a_byte_order_mark_inside_a_payload_is_kept() {
        let events = decode(&["data: first\n\n", "data: \u{feff}second\n\n"]);

        assert_eq!(data_of(&events), vec!["first", "\u{feff}second"]);
    }

    /// Progress is a `data` line arriving, not an event completing -- a caller bounding
    /// liveness must be able to see a large event still being received.
    #[test]
    fn a_data_line_counts_as_progress_before_its_event_ends() {
        let mut decoder = SseDecoder::new();
        assert_eq!(decoder.data_fields_seen(), 0);

        decoder.push(b"data: still arriving\n");
        assert!(decoder.next_event().is_none(), "the event has not ended");
        assert_eq!(
            decoder.data_fields_seen(),
            1,
            "a data line arrived, so the stream is producing"
        );

        // A comment is not progress: it says the connection is up, not that anything is
        // being produced.
        decoder.push(b": keep-alive\n");
        while decoder.next_event().is_some() {}
        assert_eq!(decoder.data_fields_seen(), 1);
    }
}
