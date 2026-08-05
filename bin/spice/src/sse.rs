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
//! event. Line terminators are `\n` and `\r\n`; a lone `\r`, which the standard also allows
//! but which nothing in this stack emits, is treated as part of the line rather than as a
//! terminator — recognising it would mean a server that never sends `\n` could hold the
//! decoder's buffer open indefinitely.

/// The most bytes one event may occupy before the decoder refuses to keep buffering.
///
/// A stream that never sends a line terminator would otherwise grow this buffer without
/// bound. Individual chat completion chunks are a few hundred bytes; a tool call carrying a
/// large argument payload is the realistic upper end, and stays far below this.
pub(crate) const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// One dispatched SSE event.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SseEvent {
    /// The `event:` field, when the stream set one.
    pub(crate) name: Option<String>,
    /// The `data:` fields, joined with newlines as the standard specifies.
    pub(crate) data: String,
}

/// An event grew past [`MAX_EVENT_BYTES`] without being terminated.
#[derive(Debug)]
pub(crate) struct OversizedEvent {
    /// How many bytes had accumulated when the decoder gave up.
    pub(crate) bytes: usize,
}

/// Reassembles [`SseEvent`]s from a byte stream delivered in arbitrary pieces.
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    /// Bytes received that are not yet a complete line.
    pending: Vec<u8>,
    /// The `event:` field of the event being accumulated.
    name: Option<String>,
    /// The `data:` fields of the event being accumulated, already joined.
    data: String,
    /// Whether any field has been read since the last dispatch. Distinguishes a blank line
    /// that ends an event from one that separates two events, so a keep-alive comment
    /// followed by a blank line does not dispatch an empty event.
    started: bool,
}

impl SseDecoder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Take another piece of the stream.
    ///
    /// # Errors
    ///
    /// Returns [`OversizedEvent`] if a single event has grown past [`MAX_EVENT_BYTES`]
    /// without being terminated.
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<(), OversizedEvent> {
        self.pending.extend_from_slice(bytes);

        let buffered = self.pending.len() + self.data.len();
        if buffered > MAX_EVENT_BYTES {
            return Err(OversizedEvent { bytes: buffered });
        }

        Ok(())
    }

    /// Yield the next event that has arrived in full, if any.
    ///
    /// Call this until it returns `None`: one read can carry several events.
    pub(crate) fn next_event(&mut self) -> Option<SseEvent> {
        while let Some(line) = self.take_line() {
            if let Some(event) = self.read_field(&line) {
                return Some(event);
            }
        }

        None
    }

    /// Dispatch what the stream left unterminated at its end.
    ///
    /// The standard discards an event the stream did not terminate. That rule protects a
    /// consumer that acts on partial data, which this one does not — the payload still has
    /// to parse as a whole — so the final event of a server that closes without its blank
    /// line is kept rather than lost, and a genuinely truncated one surfaces as a parse
    /// failure rather than as a quietly shorter answer.
    pub(crate) fn finish(&mut self) -> Option<SseEvent> {
        if !self.pending.is_empty() {
            let line = String::from_utf8_lossy(&self.pending).into_owned();
            self.pending.clear();
            if let Some(event) = self.read_field(&line) {
                return Some(event);
            }
        }

        self.started.then(|| self.dispatch())
    }

    /// Split off the next complete line, without its terminator.
    fn take_line(&mut self) -> Option<String> {
        let end = self.pending.iter().position(|byte| *byte == b'\n')?;
        let mut line: Vec<u8> = self.pending.drain(..=end).collect();

        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }

        Some(String::from_utf8_lossy(&line).into_owned())
    }

    /// Apply one line to the event being accumulated, returning an event if it ended one.
    fn read_field(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.started.then(|| self.dispatch());
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
                self.started = true;
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(value);
            }
            "event" => {
                self.started = true;
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
        self.started = false;

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
            decoder
                .push(piece.as_bytes())
                .expect("test payloads are far below the size cap");
            while let Some(event) = decoder.next_event() {
                events.push(event);
            }
        }

        events.extend(decoder.finish());
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

        decoder
            .push(b"data: ")
            .expect("the payload is far below the size cap");
        decoder
            .push(&snowman[..1])
            .expect("the payload is far below the size cap");
        assert!(decoder.next_event().is_none());
        decoder
            .push(&snowman[1..])
            .expect("the payload is far below the size cap");
        decoder
            .push(b"\n\n")
            .expect("the payload is far below the size cap");

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

        loop {
            let chunk = vec![b'x'; 1024 * 1024];
            match decoder.push(&chunk) {
                Ok(()) => pushed += chunk.len(),
                Err(oversized) => {
                    assert!(oversized.bytes > MAX_EVENT_BYTES);
                    assert!(pushed <= MAX_EVENT_BYTES);
                    return;
                }
            }
            assert!(
                pushed <= MAX_EVENT_BYTES,
                "the decoder buffered {pushed} bytes without refusing"
            );
        }
    }
}
