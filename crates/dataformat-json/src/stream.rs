/*
Copyright 2025 The Spice.ai OSS Authors

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

use serde_json::error::Category;
use serde_json::{Deserializer, StreamDeserializer, de::IoRead, value::RawValue};
use std::collections::VecDeque;
use std::io::{self, BufRead, Read};
use std::sync::{Arc, Mutex};

/* -------------------------------------------------------------
Tee: duplicates every byte read so we can replay it later
-----------------------------------------------------------*/
struct Tee<R: Read + Send> {
    inner: R,
    buf: Vec<u8>, // rolling buffer of ALL bytes read so far
}
impl<R: Read + Send> Tee<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::new(),
        }
    }
    /// Discard the first `n` bytes we no longer need.
    fn drain_front(&mut self, n: usize) {
        self.buf.drain(..n);
    }
}

/* Wrapper that the JSON deserializer will read from. It shares the
 * same internal buffer via `Arc<Mutex<_>>`, so we can still see the
 * bytes after serde has consumed them. */
#[derive(Clone)]
struct TeeReader<R: Read + Send> {
    shared: Arc<Mutex<Tee<R>>>,
}
impl<R: Read + Send> Read for TeeReader<R> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let mut tee = match self.shared.lock() {
            Ok(tee) => tee,
            Err(e) => e.into_inner(),
        };
        let n = tee.inner.read(dst)?;
        tee.buf.extend_from_slice(&dst[..n]); // single copy into rolling buf
        Ok(n)
    }
}

/* -------------------------------------------------------------
ArrayToNdjson – implements `BufRead` so downstream can pull NDJSON
-------------------------------------------------------------*/
/// Streaming adapter that converts a JSON array like
/// `[ {...}, {...}, ... ]` into newline‑delimited JSON (NDJSON).
///
/// * Uses `serde_json` for robust parsing.
/// * Keeps at most the largest single element in memory.
/// * Implements `BufRead`, so any existing NDJSON consumer can drive it.
/// * **Strips any `\n` or `\r` characters that appear inside the original
///   JSON element bytes before emitting them**, because some NDJSON
///   consumers choke on embedded new‑lines.
pub struct ArrayToNdjson<R: Read + Send> {
    shared: Arc<Mutex<Tee<R>>>, // rolling buffer
    stream: StreamDeserializer<'static, IoRead<TeeReader<R>>, Box<RawValue>>, // serde iterator
    drained: usize,             // bytes already drained from tee.buf
    prev_off: usize,            // byte_offset() after previous element
    pending: VecDeque<u8>,      // data ready for BufRead
    eof: bool,
}

impl<R: Read + Send> ArrayToNdjson<R> {
    /// Create a new adapter.  Consumes whitespace and the leading `[`.
    ///
    /// # Errors
    ///
    /// Returns an error if the input does not start with a valid JSON array opening bracket `[`,
    /// or if there are I/O errors while reading the input.
    pub fn try_new(mut inner: R) -> io::Result<Self> {
        skip_ws_until(&mut inner, b'[')?; // eat prologue

        // Shared tee so we can inspect bytes that serde has read.
        let shared = Arc::new(Mutex::new(Tee::new(inner)));
        let reader = TeeReader {
            shared: Arc::clone(&shared),
        };
        // `from_reader` takes ownership of `reader` and wraps it in IoRead.
        let stream = Deserializer::from_reader(reader).into_iter::<Box<RawValue>>();

        Ok(Self {
            shared,
            stream,
            drained: 0,
            prev_off: 0,
            pending: VecDeque::new(),
            eof: false,
        })
    }

    /// Consume the adapter and return the original inner reader.
    /// This allows you to recover the original reader after processing the JSON array.
    ///
    /// # Errors
    ///
    /// Returns an error if the adapter cannot be consumed due to multiple outstanding
    /// references to the shared buffer.
    pub fn finish(self) -> Result<R, io::Error> {
        // Drop the stream to release its reference to the shared Tee
        drop(self.stream);

        // Try to unwrap the Arc - this should succeed since we dropped the stream
        let tee = Arc::try_unwrap(self.shared)
            .map_err(|_| {
                io::Error::other("Failed to recover inner reader - multiple references still exist")
            })?
            .into_inner();

        let tee = match tee {
            Ok(tee) => tee,
            Err(e) => e.into_inner(),
        };

        Ok(tee.inner)
    }

    /// Ensure `pending` holds at least one complete element + `\n`.
    fn fill_pending(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() || self.eof {
            return Ok(());
        }

        // Pull the next element from serde.
        match self.stream.next() {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                // Check if this is an empty array case
                if e.classify() == Category::Syntax {
                    // This likely means we hit an empty array - peek to confirm
                    if matches!(self.peek_next_non_ws_byte(), Ok(b']')) {
                        // Empty array - consume the closing bracket and mark as EOF
                        self.consume_delimiter()?;
                        self.eof = true;
                        return Ok(());
                    }
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
            None => {
                self.eof = true;
                return Ok(());
            }
        }

        // Access the shared rolling buffer.
        let mut tee = match self.shared.lock() {
            Ok(tee) => tee,
            Err(e) => e.into_inner(),
        };

        let slice = &tee.buf[..];
        let tee_buf_len = tee.buf.len();

        // Push the clean element (without internal newlines and carriage returns) plus newline to `pending`.
        filter_element_bytes(slice, &mut self.pending);

        // Discard bytes we no longer need from tee.buf.
        tee.drain_front(tee_buf_len);

        drop(tee);

        let next = self.peek_next_non_ws_byte()?;
        match next {
            b',' => {
                self.consume_delimiter()?; // another element coming
                // println!("Found comma, expecting another element")
            }
            b']' => {
                self.consume_delimiter()?;
                self.eof = true;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected ',' or ']' but found '{char}'",
                        char = next as char
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Read (and buffer) bytes until we find the first non-whitespace byte,
    /// then return it **without** removing it from the buffer.
    fn peek_next_non_ws_byte(&mut self) -> io::Result<u8> {
        let mut tee = match self.shared.lock() {
            Ok(tee) => tee,
            Err(e) => e.into_inner(),
        };
        loop {
            /* -------- 1. look in the bytes we already have -------- */
            {
                // Everything read so far (but not yet drained) lives in tee.buf.
                // We start scanning from the point just after the last element.
                let mut i = self.prev_off - self.drained;
                while i < tee.buf.len() {
                    let b = tee.buf[i];
                    if !b.is_ascii_whitespace() {
                        return Ok(b); // found it – return without consuming
                    }
                    i += 1;
                }
            }

            /* -------- 2. need more data: read one byte from the source -------- */
            let mut byte = [0u8; 1];
            if tee.inner.read(&mut byte)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF while peeking next byte",
                ));
            }

            // Keep the rolling buffer in sync so serde can still “see” the byte later.
            tee.buf.push(byte[0]);

            // Loop: if the byte we just read was whitespace,
            // we’ll read again until we hit a non-WS byte.
        }
    }

    /// Remove the comma (`','`) **or** closing bracket (`']'`) that we just
    /// peeked, together with any preceding whitespace, and update the
    /// `drained` / `prev_off` counters so slicing the next element works.
    fn consume_delimiter(&mut self) -> io::Result<()> {
        let mut tee = match self.shared.lock() {
            Ok(tee) => tee,
            Err(e) => e.into_inner(),
        };

        // 1️⃣  Drop leading whitespace that we may have read while peeking.
        while let Some(&b) = tee.buf.first() {
            if !b.is_ascii_whitespace() {
                break;
            }
            tee.drain_front(1);
            self.drained += 1;
            self.prev_off += 1;
        }

        // 2️⃣  Now the first byte must be the delimiter itself.
        if tee.buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "buffer ended while trying to consume delimiter",
            ));
        }
        tee.drain_front(1); // discard ',' or ']'
        self.drained += 1;
        self.prev_off += 1;

        Ok(())
    }
}

/* ---------- Implement I/O traits ---------- */
impl<R: Read + Send> Read for ArrayToNdjson<R> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        self.fill_pending()?;
        let n = dst
            .iter_mut()
            .take(self.pending.len())
            .enumerate()
            .map(|(idx, byte)| *byte = self.pending[idx])
            .count();
        self.pending.drain(..n);
        if n == 0 && self.eof { Ok(0) } else { Ok(n) }
    }
}

impl<R: Read + Send> BufRead for ArrayToNdjson<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.fill_pending()?;
        Ok(self.pending.as_slices().0)
    }
    fn consume(&mut self, amt: usize) {
        self.pending.drain(..amt);
    }
}

// Thread safety implementations
// unsafe impl<R: Read + Send> Send for ArrayToNdjson<R> {}
// unsafe impl<R: Read + Send + Sync> Sync for ArrayToNdjson<R> {}

/* ---------- shared utilities ---------- */

/// Filter out newlines and carriage returns from JSON element bytes,
/// also skip leading and trailing whitespace. Used by both pull and push implementations.
fn filter_element_bytes(element_bytes: &[u8], output: &mut VecDeque<u8>) {
    // Predicate: keep bytes that are not newlines or carriage returns
    let is_content = |&b: &u8| b != b'\n' && b != b'\r';

    // Find the first non-whitespace content byte
    let start = element_bytes
        .iter()
        .position(|b| is_content(b) && !b.is_ascii_whitespace());

    // Find the last non-whitespace content byte
    let end = element_bytes
        .iter()
        .rposition(|b| is_content(b) && !b.is_ascii_whitespace());

    // Add the trimmed content, filtering out newlines/carriage returns in a single pass
    if let (Some(start), Some(end)) = (start, end) {
        for &byte in &element_bytes[start..=end] {
            if is_content(&byte) {
                output.push_back(byte);
            }
        }
    }
    output.push_back(b'\n');
}

/* ---------- helpers ---------- */
/// Read until (and including) `expect`, skipping leading whitespace.
fn skip_ws_until<R: Read>(r: &mut R, expect: u8) -> io::Result<()> {
    let mut byte = [0u8; 1];
    loop {
        if r.read(&mut byte)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("EOF before '{char}'", char = expect as char),
            ));
        }
        match byte[0] {
            b if b.is_ascii_whitespace() => {}
            b if b == expect => return Ok(()),
            b => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected '{expected}' but found '{found}'",
                        expected = expect as char,
                        found = b as char
                    ),
                ));
            }
        }
    }
}

/* -------------------------------------------------------------
Push-based implementation
-------------------------------------------------------------*/

/// Result type for push-based reading operations
#[derive(Debug, PartialEq)]
pub enum ReadResult {
    /// Data is ready, returns the pending bytes
    Ready(Vec<u8>),
    /// Not enough input data available, need more bytes
    NotReady,
    /// End of stream reached
    Eof,
}

/// Push-based adapter that converts a JSON array into NDJSON
///
/// Unlike `ArrayToNdjson`, this version accepts data via `push_bytes()`
/// and provides data via `try_read()` which can return `NotReady`.
///
/// # Example
///
/// ```rust,no_run
/// use dataformat_json::stream::{ArrayToNdjsonPush, ReadResult};
///
/// let mut adapter = ArrayToNdjsonPush::new();
///
/// // Push data incrementally
/// adapter.push_bytes(b"[{\"name\":").unwrap();
/// adapter.push_bytes(b"\"John\"}]").unwrap();
///
/// // Read processed NDJSON
/// match adapter.try_read().unwrap() {
///     ReadResult::Ready(data) => {
///         let output = std::str::from_utf8(&data).unwrap();
///         assert!(output.contains("{\"name\":\"John\"}"));
///     }
///     ReadResult::NotReady => panic!("Should have data ready"),
///     ReadResult::Eof => panic!("Should not be EOF yet"),
/// }
/// ```
#[derive(Debug)]
pub struct ArrayToNdjsonPush {
    buffer: Vec<u8>,       // Accumulates pushed bytes
    pending: VecDeque<u8>, // Ready NDJSON output
    state: ParsingState,   // Current parsing state
}

#[derive(Debug)]
enum ParsingState {
    ExpectingArrayStart,
    ExpectingFirstElement,
    ExpectingElement,
    ExpectingCommaOrClosingBracket,
    Complete,
}

impl ArrayToNdjsonPush {
    /// Create a new push-based adapter
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            pending: VecDeque::new(),
            state: ParsingState::ExpectingArrayStart,
        }
    }

    /// Push new bytes into the adapter
    ///
    /// # Errors
    ///
    /// Returns an error if the pushed data contains invalid JSON syntax.
    pub fn push_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.buffer.extend_from_slice(data);
        self.process_buffer()
    }

    /// Try to read processed NDJSON data
    ///
    /// # Errors
    ///
    /// Returns an error if there are issues with the internal state or JSON parsing.
    pub fn try_read(&mut self) -> ReadResult {
        if self.pending.is_empty() {
            if matches!(self.state, ParsingState::Complete) {
                return ReadResult::Eof;
            }

            return ReadResult::NotReady;
        }

        // Return all pending data and clear it
        let data: Vec<u8> = self.pending.drain(..).collect();
        ReadResult::Ready(data)
    }

    /// Check if there is pending data to be read
    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Check if the adapter has finished processing (reached end of array)
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.state, ParsingState::Complete)
    }

    /// Process accumulated buffer data using `serde_json::StreamDeserializer`
    #[expect(clippy::cast_possible_truncation)]
    fn process_buffer(&mut self) -> io::Result<()> {
        if matches!(self.state, ParsingState::Complete) {
            return Ok(());
        }

        // Skip whitespace and consume opening bracket if not done yet
        if matches!(self.state, ParsingState::ExpectingArrayStart) {
            let mut cursor = io::Cursor::new(&self.buffer);
            if matches!(skip_ws_until(&mut cursor, b'['), Ok(())) {
                let consumed = cursor.position() as usize;
                if consumed <= self.buffer.len() {
                    self.buffer.drain(..consumed);
                    self.state = ParsingState::ExpectingFirstElement;
                }
            } else {
                // Not enough data yet
                return Ok(());
            }
        }

        loop {
            match self.state {
                ParsingState::ExpectingFirstElement => {
                    if self.buffer.is_empty() {
                        return Ok(());
                    }

                    let mut cursor = io::Cursor::new(&self.buffer);
                    // If the buffer contains only a closing bracket and we're expecting a first element, we're done
                    match Self::next_non_ws_byte(&mut cursor) {
                        Ok(b']') => {
                            // End of array
                            let consumed = cursor.position() as usize;
                            self.buffer.drain(..consumed);
                            self.state = ParsingState::Complete;
                            return Ok(());
                        }
                        Ok(_) => {
                            // The next non-whitespace byte is not a closing bracket, so we're expecting an element
                            self.state = ParsingState::ExpectingElement;
                        }
                        Err(_) => {
                            return Ok(());
                        }
                    }
                }
                ParsingState::ExpectingElement => {
                    let cursor = io::Cursor::new(&self.buffer);
                    let mut stream = Deserializer::from_reader(cursor).into_iter::<Box<RawValue>>();
                    match stream.next() {
                        Some(Ok(element)) => {
                            // Successfully parsed an element
                            let element_bytes = element.get().as_bytes();

                            // Filter and add to pending
                            filter_element_bytes(element_bytes, &mut self.pending);

                            // Calculate how many bytes were consumed
                            let consumed = stream.byte_offset();
                            self.buffer.drain(..consumed);

                            self.state = ParsingState::ExpectingCommaOrClosingBracket;
                        }
                        Some(Err(e)) => {
                            // Check if this is a "need more data" error
                            if e.classify() == Category::Eof || e.classify() == Category::Syntax {
                                // This is expected when we have partial data - just wait for more
                                return Ok(());
                            }

                            // This is a real syntax error
                            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                        }
                        None => {
                            // No more complete elements available
                            return Ok(());
                        }
                    }
                }
                ParsingState::ExpectingCommaOrClosingBracket => {
                    let mut cursor = io::Cursor::new(&self.buffer);
                    match Self::next_non_ws_byte(&mut cursor) {
                        Ok(b',') => {
                            // Consume comma and continue
                            let consumed = cursor.position() as usize;
                            self.buffer.drain(..consumed);
                            self.state = ParsingState::ExpectingElement;
                        }
                        Ok(b']') => {
                            // End of array
                            let consumed = cursor.position() as usize;
                            self.buffer.drain(..consumed);
                            self.state = ParsingState::Complete;
                            return Ok(());
                        }
                        Ok(byte) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("expected ',' or ']' but found '{}'", byte as char),
                            ));
                        }
                        Err(_) => {
                            // Not enough data to determine what's next
                            return Ok(());
                        }
                    }
                }
                _ => break,
            }
        }

        Ok(())
    }

    /// Get the next non-whitespace byte
    fn next_non_ws_byte(cursor: &mut io::Cursor<&Vec<u8>>) -> io::Result<u8> {
        let mut byte = [0u8; 1];
        loop {
            if cursor.read(&mut byte)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF while peeking next byte",
                ));
            }
            if !byte[0].is_ascii_whitespace() {
                return Ok(byte[0]);
            }
        }
    }
}

impl Default for ArrayToNdjsonPush {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    /// Read all lines from a `BufRead` implementation and return them as a vector.
    ///
    /// # Errors
    ///
    /// Returns an error if there are I/O errors while reading lines.
    fn read_all_lines<R: BufRead>(mut reader: R) -> io::Result<Vec<String>> {
        let mut lines = Vec::new();
        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            lines.push(line.trim_end().to_string());
            line.clear();
        }
        Ok(lines)
    }

    // Direct unit tests for filter_element_bytes function
    mod filter_element_bytes_tests {
        use super::*;

        fn filter_to_string(input: &[u8]) -> String {
            let mut output = VecDeque::new();
            filter_element_bytes(input, &mut output);
            // Remove trailing newline for easier comparison
            let bytes: Vec<u8> = output.into_iter().collect();
            String::from_utf8_lossy(&bytes)
                .trim_end_matches('\n')
                .to_string()
        }

        #[test]
        fn test_filter_basic_json() {
            let input = b"{\"name\": \"John\"}";
            assert_eq!(filter_to_string(input), r#"{"name": "John"}"#);
        }

        #[test]
        fn test_filter_removes_newlines() {
            let input = b"{\n\"name\":\n\"John\"\n}";
            assert_eq!(filter_to_string(input), r#"{"name":"John"}"#);
        }

        #[test]
        fn test_filter_removes_carriage_returns() {
            let input = b"{\r\"name\":\r\"John\"\r}";
            assert_eq!(filter_to_string(input), r#"{"name":"John"}"#);
        }

        #[test]
        fn test_filter_removes_mixed_line_endings() {
            let input = b"{\r\n\"name\":\n\r\"John\"\r\n}";
            assert_eq!(filter_to_string(input), r#"{"name":"John"}"#);
        }

        #[test]
        fn test_filter_trims_leading_whitespace() {
            let input = b"   \t{\"name\": \"John\"}";
            assert_eq!(filter_to_string(input), r#"{"name": "John"}"#);
        }

        #[test]
        fn test_filter_trims_trailing_whitespace() {
            let input = b"{\"name\": \"John\"}   \t";
            assert_eq!(filter_to_string(input), r#"{"name": "John"}"#);
        }

        #[test]
        fn test_filter_trims_both_ends() {
            let input = b"  \t {\"name\": \"John\"}  \t ";
            assert_eq!(filter_to_string(input), r#"{"name": "John"}"#);
        }

        #[test]
        fn test_filter_handles_leading_newlines_as_whitespace() {
            // Leading newlines should be treated as whitespace and trimmed
            let input = b"\n\n{\"x\": 1}\n\n";
            assert_eq!(filter_to_string(input), r#"{"x": 1}"#);
        }

        #[test]
        fn test_filter_empty_input() {
            // Empty input should just produce a newline
            let input = b"";
            let mut output = VecDeque::new();
            filter_element_bytes(input, &mut output);
            assert_eq!(output.len(), 1);
            assert_eq!(output[0], b'\n');
        }

        #[test]
        fn test_filter_only_whitespace() {
            // Whitespace-only input should just produce a newline
            let input = b"   \t\n  ";
            let mut output = VecDeque::new();
            filter_element_bytes(input, &mut output);
            assert_eq!(output.len(), 1);
            assert_eq!(output[0], b'\n');
        }

        #[test]
        fn test_filter_only_newlines_and_carriage_returns() {
            // Input with only newlines/carriage returns should produce just a newline
            let input = b"\n\r\n\r";
            let mut output = VecDeque::new();
            filter_element_bytes(input, &mut output);
            assert_eq!(output.len(), 1);
            assert_eq!(output[0], b'\n');
        }

        #[test]
        fn test_filter_preserves_internal_spaces() {
            // Spaces inside the JSON content should be preserved
            let input = b"{\"name\":  \"John  Doe\"}";
            assert_eq!(filter_to_string(input), r#"{"name":  "John  Doe"}"#);
        }

        #[test]
        fn test_filter_preserves_internal_tabs() {
            let input = b"{\"name\":\t\"John\"}";
            assert_eq!(filter_to_string(input), "{\"name\":\t\"John\"}");
        }

        #[test]
        fn test_filter_complex_json() {
            let input = b"  \n{\n  \"users\": [\n    {\"name\": \"Alice\"},\n    {\"name\": \"Bob\"}\n  ]\n}\n  ";
            let result = filter_to_string(input);
            // Should have all newlines removed and be trimmed
            assert!(!result.contains('\n'));
            assert!(!result.contains('\r'));
            assert!(result.starts_with('{'));
            assert!(result.ends_with('}'));
        }

        #[test]
        fn test_filter_appends_newline() {
            // The function should always append a newline at the end
            let input = b"{}";
            let mut output = VecDeque::new();
            filter_element_bytes(input, &mut output);
            let bytes: Vec<u8> = output.into_iter().collect();
            assert_eq!(bytes.last(), Some(&b'\n'));
        }

        #[test]
        fn test_filter_single_character() {
            let input = b"1";
            assert_eq!(filter_to_string(input), "1");
        }

        #[test]
        fn test_filter_number() {
            let input = b"  42  ";
            assert_eq!(filter_to_string(input), "42");
        }

        #[test]
        fn test_filter_null() {
            let input = b"  null  ";
            assert_eq!(filter_to_string(input), "null");
        }

        #[test]
        fn test_filter_boolean() {
            let input = b"  true  ";
            assert_eq!(filter_to_string(input), "true");
        }

        #[test]
        fn test_filter_string_with_escaped_newline() {
            // Escaped newlines in strings (\\n) should be preserved as literal characters
            let input = br#"{"text": "line1\nline2"}"#;
            assert_eq!(filter_to_string(input), r#"{"text": "line1\nline2"}"#);
        }
    }

    #[test]
    fn test_empty_array() {
        let input = "[]";
        let cursor = Cursor::new(input);
        let mut adapter =
            ArrayToNdjson::try_new(cursor).expect("Failed to create ArrayToNdjson adapter");

        // For empty arrays, we should immediately hit EOF when trying to read
        let mut buf = Vec::new();
        let result = adapter
            .read_to_end(&mut buf)
            .expect("Failed to read from adapter");
        assert_eq!(result, 0);
        assert_eq!(buf, Vec::<u8>::new());
    }

    #[test]
    fn test_single_element() {
        let input = r#"[{"name": "John", "age": 30}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"name": "John", "age": 30}"#]);
    }

    #[test]
    fn test_multiple_elements() {
        let input = r#"[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"name": "John", "age": 30}"#,
                r#"{"name": "Jane", "age": 25}"#
            ]
        );
    }

    #[test]
    fn test_whitespace_before_array() {
        let input = "   \t\n  [{}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec!["{}"]);
    }

    #[test]
    fn test_whitespace_after_opening_bracket() {
        let input = "[   \t\n  {}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec!["{}"]);
    }

    #[test]
    fn test_whitespace_before_comma() {
        let input = r#"[{"a": 1}   ,   {"b": 2}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"a": 1}"#, r#"{"b": 2}"#]);
    }

    #[test]
    fn test_whitespace_after_comma() {
        let input = "[{\"a\": 1},   \t\n  {\"b\": 2}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"a": 1}"#, r#"{"b": 2}"#]);
    }

    #[test]
    fn test_whitespace_before_closing_bracket() {
        let input = "[{}   \t\n  ]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec!["{}"]);
    }

    #[test]
    fn test_extensive_whitespace() {
        let input = "\n\t   [  \n\t  {\"x\": 1}  \n\t  ,  \n\t  {\"y\": 2}  \n\t  ]  \n\t  ";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"x": 1}"#, r#"{"y": 2}"#]);
    }

    #[test]
    fn test_newlines_inside_json_removed() {
        let input = "[\n{\n\"name\":\n\"John\",\n\"age\":\n30\n}\n]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"name":"John","age":30}"#]);
    }

    #[test]
    fn test_carriage_returns_inside_json_removed() {
        let input = "[{\r\"name\":\r\"John\",\r\"age\":\r30\r}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"name":"John","age":30}"#]);
    }

    #[test]
    fn test_mixed_newlines_and_carriage_returns() {
        let input = "[{\n\r\"mixed\":\r\n\"value\"\n}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec![r#"{"mixed":"value"}"#]);
    }

    #[test]
    fn test_nested_objects_with_whitespace() {
        let input = r#"[  {  "user":  {  "profile":  {  "name":  "John"  }  }  }  ]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![r#"{  "user":  {  "profile":  {  "name":  "John"  }  }  }"#]
        );
    }

    #[test]
    fn test_arrays_inside_objects() {
        let input = r#"[{"numbers": [1, 2, 3]}, {"letters": ["a", "b", "c"]}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"numbers": [1, 2, 3]}"#,
                r#"{"letters": ["a", "b", "c"]}"#
            ]
        );
    }

    #[test]
    fn test_string_values_with_internal_brackets() {
        let input = r#"[{"text": "Hello [world]"}, {"text": "Another ]test["}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"text": "Hello [world]"}"#,
                r#"{"text": "Another ]test["}"#
            ]
        );
    }

    #[test]
    fn test_string_values_with_escaped_quotes() {
        let input = r#"[{"message": "He said \"Hello\""}, {"quote": "She replied \"Hi\""}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"message": "He said \"Hello\""}"#,
                r#"{"quote": "She replied \"Hi\""}"#
            ]
        );
    }

    #[test]
    fn test_numeric_values() {
        let input = r#"[{"int": 42}, {"float": 3.14159}, {"negative": -123}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"int": 42}"#,
                r#"{"float": 3.14159}"#,
                r#"{"negative": -123}"#
            ]
        );
    }

    #[test]
    fn test_boolean_and_null_values() {
        let input = r#"[{"bool": true}, {"nullVal": null}, {"bool2": false}]"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(
            lines,
            vec![
                r#"{"bool": true}"#,
                r#"{"nullVal": null}"#,
                r#"{"bool2": false}"#
            ]
        );
    }

    #[test]
    fn test_finish_method() {
        let input = "[{}]";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec!["{}"]);
    }

    #[test]
    fn test_finish_method_recovers_reader() {
        let input = "[{}]remaining data";
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");
        assert_eq!(lines, vec!["{}"]);
    }

    #[test]
    fn test_invalid_json_missing_opening_bracket() {
        let input = r#"{"name": "John"}"#;
        let cursor = Cursor::new(input);
        let result = ArrayToNdjson::try_new(cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_json_missing_closing_bracket() {
        let input = r#"[{"name": "John"}"#;
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let result = read_all_lines(adapter);
        result.expect_err("Should fail for missing closing bracket");
    }

    #[test]
    fn test_invalid_json_malformed_element() {
        let input = r#"[{"name": John}]"#; // missing quotes around John
        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let result = read_all_lines(adapter);
        result.expect_err("Should fail for malformed JSON element");
    }

    #[test]
    fn test_empty_string_input() {
        let input = "";
        let cursor = Cursor::new(input);
        let result = ArrayToNdjson::try_new(cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_only_whitespace() {
        let input = "   \t\n   ";
        let cursor = Cursor::new(input);
        let result = ArrayToNdjson::try_new(cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_number_of_elements() {
        use std::fmt::Write;
        let mut input = String::from("[");
        for i in 0..1000 {
            if i > 0 {
                input.push(',');
            }
            write!(input, r#"{{"id": {i}}}"#).expect("Writing to string should not fail");
        }
        input.push(']');

        let cursor = Cursor::new(input);
        let adapter = ArrayToNdjson::try_new(cursor).expect("Test should not fail");
        let lines = read_all_lines(adapter).expect("Test should not fail");

        assert_eq!(lines.len(), 1000);
        assert_eq!(lines[0], r#"{"id": 0}"#);
        assert_eq!(lines[999], r#"{"id": 999}"#);
    }

    // Tests for push-based implementation
    mod push_tests {
        use super::*;

        /// Helper to read all available data from push adapter
        fn read_all_push(adapter: &mut ArrayToNdjsonPush) -> Vec<String> {
            let mut lines = Vec::new();

            while let ReadResult::Ready(data) = adapter.try_read() {
                let text = String::from_utf8_lossy(&data);
                for line in text.lines() {
                    if !line.is_empty() {
                        lines.push(line.to_string());
                    }
                }
            }

            lines
        }

        /// Helper to push data in chunks and read results
        fn push_and_read_chunked(input: &str, chunk_size: usize) -> io::Result<Vec<String>> {
            let mut adapter = ArrayToNdjsonPush::new();
            let mut all_lines = Vec::new();

            for chunk in input.as_bytes().chunks(chunk_size) {
                adapter.push_bytes(chunk)?;
                let mut lines = read_all_push(&mut adapter);
                all_lines.append(&mut lines);
            }

            // Read any remaining data
            let mut lines = read_all_push(&mut adapter);
            all_lines.append(&mut lines);

            Ok(all_lines)
        }

        #[test]
        fn test_push_empty_array() {
            let mut adapter = ArrayToNdjsonPush::new();
            adapter.push_bytes(b"[]").expect("Push should succeed");

            assert_eq!(adapter.try_read(), ReadResult::Eof);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_single_element() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = r#"[{"name": "John", "age": 30}]"#;
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"name": "John", "age": 30}"#]);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_multiple_elements() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = r#"[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]"#;
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(
                lines,
                vec![
                    r#"{"name": "John", "age": 30}"#,
                    r#"{"name": "Jane", "age": 25}"#
                ]
            );
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_chunked_input() {
            let input = r#"[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]"#;
            let lines = push_and_read_chunked(input, 5).expect("Chunked processing should succeed");

            assert_eq!(
                lines,
                vec![
                    r#"{"name": "John", "age": 30}"#,
                    r#"{"name": "Jane", "age": 25}"#
                ]
            );
        }

        #[test]
        fn test_push_byte_by_byte() {
            let input = r#"[{"x": 1}, {"y": 2}]"#;
            let lines =
                push_and_read_chunked(input, 1).expect("Byte-by-byte processing should succeed");

            assert_eq!(lines, vec![r#"{"x": 1}"#, r#"{"y": 2}"#]);
        }

        #[test]
        fn test_push_not_ready_state() {
            let mut adapter = ArrayToNdjsonPush::new();

            // Push incomplete data
            adapter
                .push_bytes(b"[{\"name\":")
                .expect("Push should succeed");

            assert_eq!(adapter.try_read(), ReadResult::NotReady);
            assert!(!adapter.is_complete());

            // Complete the element
            adapter
                .push_bytes(b" \"John\"}]")
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"name": "John"}"#]);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_whitespace_handling() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = "   \t\n  [  \n\t  {\"x\": 1}  \n\t  ,  \n\t  {\"y\": 2}  \n\t  ]  \n\t  ";
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"x": 1}"#, r#"{"y": 2}"#]);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_newlines_removed() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = "[\n{\n\"name\":\n\"John\",\n\"age\":\n30\n}\n]";
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"name":"John","age":30}"#]);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_nested_objects() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = r#"[{"user": {"profile": {"name": "John"}}}, {"data": [1, 2, 3]}]"#;
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(
                lines,
                vec![
                    r#"{"user": {"profile": {"name": "John"}}}"#,
                    r#"{"data": [1, 2, 3]}"#
                ]
            );
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_string_with_brackets() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = r#"[{"text": "Hello [world]"}, {"text": "Another ]test["}]"#;
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(
                lines,
                vec![
                    r#"{"text": "Hello [world]"}"#,
                    r#"{"text": "Another ]test["}"#
                ]
            );
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_string_with_escaped_quotes() {
            let mut adapter = ArrayToNdjsonPush::new();
            let input = r#"[{"message": "He said \"Hello\""}, {"quote": "She replied \"Hi\""}]"#;
            adapter
                .push_bytes(input.as_bytes())
                .expect("Push should succeed");

            let lines = read_all_push(&mut adapter);
            assert_eq!(
                lines,
                vec![
                    r#"{"message": "He said \"Hello\""}"#,
                    r#"{"quote": "She replied \"Hi\""}"#
                ]
            );
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_incremental_processing() {
            let mut adapter = ArrayToNdjsonPush::new();

            // Push opening bracket
            adapter.push_bytes(b"[").expect("Push should succeed");
            assert_eq!(adapter.try_read(), ReadResult::NotReady);

            // Push first element
            adapter
                .push_bytes(b"{\"a\": 1}")
                .expect("Push should succeed");
            let result = adapter.try_read();
            if let ReadResult::Ready(data) = result {
                let text = String::from_utf8_lossy(&data);
                assert!(text.contains(r#"{"a": 1}"#));
            } else {
                panic!("Expected Ready result");
            }

            // Push comma
            adapter.push_bytes(b",").expect("Push should succeed");
            assert_eq!(adapter.try_read(), ReadResult::NotReady);

            // Push second element and closing bracket
            adapter
                .push_bytes(b"{\"b\": 2}]")
                .expect("Push should succeed");
            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"b": 2}"#]);
            assert!(adapter.is_complete());
        }

        #[test]
        fn test_push_invalid_json_missing_bracket() {
            let mut adapter = ArrayToNdjsonPush::new();
            adapter
                .push_bytes(b"{\"name\": \"John\"}")
                .expect("Push should succeed");
            assert!(adapter.try_read() == ReadResult::NotReady);
        }

        #[test]
        fn test_push_invalid_json_malformed_element() {
            let mut adapter = ArrayToNdjsonPush::new();
            adapter.push_bytes(b"[").expect("Push should succeed");
            adapter
                .push_bytes(b"{\"name\": John}]")
                .expect("Push should succeed"); // missing quotes around John
            assert!(adapter.try_read() == ReadResult::NotReady);
        }

        #[test]
        #[expect(clippy::format_push_string)]
        fn test_push_large_number_of_elements() {
            let mut input = String::from("[");
            for i in 0..100 {
                if i > 0 {
                    input.push(',');
                }
                input.push_str(&format!(r#"{{"id": {i}}}"#));
            }
            input.push(']');

            let lines =
                push_and_read_chunked(&input, 50).expect("Large array processing should succeed");

            assert_eq!(lines.len(), 100);
            assert_eq!(lines[0], r#"{"id": 0}"#);
            assert_eq!(lines[99], r#"{"id": 99}"#);
        }

        #[test]
        fn test_push_multiple_pushes_single_element() {
            let mut adapter = ArrayToNdjsonPush::new();

            // Push the JSON in multiple small chunks
            let chunks = vec![
                &b"["[..],
                &b"{"[..],
                &b"\""[..],
                &b"n"[..],
                &b"ame"[..],
                &b"\""[..],
                &b":"[..],
                &b"\""[..],
                &b"J"[..],
                &b"ohn"[..],
                &b"\""[..],
                &b"}"[..],
                &b"]"[..],
            ];

            for chunk in chunks {
                adapter.push_bytes(chunk).expect("Push should succeed");
            }

            let lines = read_all_push(&mut adapter);
            assert_eq!(lines, vec![r#"{"name":"John"}"#]);
            assert!(adapter.is_complete());
        }
    }
}
