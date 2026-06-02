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

        // If the element is a JSON array (e.g. from SODA `/data`), convert it
        // to a JSON object with positional string keys ("0", "1", …) so that
        // Arrow's JSON reader can handle it (it requires objects, not arrays).
        let first_non_ws = slice.iter().find(|b| !b.is_ascii_whitespace());
        if first_non_ws == Some(&b'[') {
            if let Ok(arr) = serde_json::from_slice::<Vec<serde_json::Value>>(slice) {
                let obj: serde_json::Map<String, serde_json::Value> = arr
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| (i.to_string(), v))
                    .collect();
                if let Ok(serialized) = serde_json::to_string(&serde_json::Value::Object(obj)) {
                    self.pending.extend(serialized.bytes());
                    self.pending.push_back(b'\n');
                } else {
                    filter_element_bytes(slice, &mut self.pending);
                }
            } else {
                filter_element_bytes(slice, &mut self.pending);
            }
        } else {
            // Push the clean element (without internal newlines and carriage returns) plus newline to `pending`.
            filter_element_bytes(slice, &mut self.pending);
        }

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
/// Read until (and including) `expect`, skipping a leading UTF-8 BOM and
/// any ASCII whitespace.
fn skip_ws_until<R: Read>(r: &mut R, expect: u8) -> io::Result<()> {
    let mut byte = [0u8; 1];
    // State machine for UTF-8 BOM detection: 0 = initial, 1 = seen 0xEF,
    // 2 = seen 0xEF 0xBB, 3 = BOM consumed.
    let mut bom: u8 = 0;
    loop {
        if r.read(&mut byte)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("EOF before '{char}'", char = expect as char),
            ));
        }
        // Try to match a leading UTF-8 BOM before anything else.
        if bom < 3 {
            match (bom, byte[0]) {
                (0, 0xEF) => {
                    bom = 1;
                    continue;
                }
                (1, 0xBB) => {
                    bom = 2;
                    continue;
                }
                (2, 0xBF) => {
                    bom = 3;
                    continue;
                }
                // Incomplete BOM prefix — the bytes read so far aren't
                // whitespace or the expected character, so the input is
                // invalid.
                (1 | 2, _) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "expected '{expected}' but found non-ASCII byte while looking for array start",
                            expected = expect as char,
                        ),
                    ));
                }
                _ => {
                    bom = 3;
                } // No BOM; fall through to normal check
            }
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
JsonPointerReader – extracts data at a JSON Pointer path from a JSON value
-------------------------------------------------------------*/

/// Reader adapter that extracts data from a specified path within a JSON value
/// using [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901) syntax.
///
/// Paths use `/` as the separator and start with `/`:
/// - `/data` extracts the `"data"` key from `{"data": [...]}`
/// - `/response/items` navigates `{"response": {"items": [...]}}`
///
/// As a convenience, a leading `/` is added automatically if missing,
/// so `"data"` is treated as `"/data"`.
///
/// Keys containing `~` or `/` literals are escaped per RFC 6901:
/// `~` → `~0`, `/` → `~1`.
#[derive(Debug)]
pub struct JsonPointerReader {
    inner: io::Cursor<Vec<u8>>,
}

impl JsonPointerReader {
    /// Create a new `JsonPointerReader` that reads from `reader` and extracts the value
    /// at the given [JSON Pointer (RFC 6901)](https://www.rfc-editor.org/rfc/rfc6901) `path`.
    ///
    /// A leading `/` is added if the path does not start with one.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The input cannot be parsed as valid JSON
    /// - The pointer path does not resolve to a value
    /// - There are I/O errors while reading the input
    pub fn new<R: Read>(mut reader: R, path: &str) -> io::Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Self::from_vec(&buf, path)
    }

    /// Create a new `JsonPointerReader` from an already-buffered byte slice,
    /// extracting the value at the given JSON Pointer `path`.
    ///
    /// This avoids an extra allocation when the caller already has the data in memory.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be parsed as valid JSON or the pointer
    /// path does not resolve to a value.
    pub fn from_vec(buf: &[u8], path: &str) -> io::Result<Self> {
        // Strip leading UTF-8 BOM if present so serde_json can parse the input.
        let buf = buf.strip_prefix(&UTF8_BOM).unwrap_or(buf);

        // An empty pointer means "the whole document" per RFC 6901.
        if path.is_empty() {
            return Ok(Self {
                inner: io::Cursor::new(buf.to_vec()),
            });
        }

        let value: serde_json::Value = serde_json::from_slice(buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse JSON for json_pointer extraction: {e}"),
            )
        })?;

        // Normalise: ensure the pointer starts with '/'
        let pointer = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };

        let current = value.pointer(&pointer).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("json_pointer '{pointer}' not found in JSON (RFC 6901 JSON Pointer)"),
            )
        })?;

        let extracted = serde_json::to_vec(current)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            inner: io::Cursor::new(extracted),
        })
    }
}

impl Read for JsonPointerReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

/* -------------------------------------------------------------
peek_first_non_ws_byte – auto-detect JSON format
-------------------------------------------------------------*/

/// UTF-8 BOM prefix (`\xEF\xBB\xBF`).
const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// Peek at the first non-whitespace byte from a `BufRead` reader without
/// consuming non-whitespace content.
///
/// A leading UTF-8 BOM (`\xEF\xBB\xBF`) is silently consumed if present.
/// Leading whitespace bytes are consumed from the buffer, but the first
/// non-whitespace byte remains available for subsequent reads.
///
/// # Errors
///
/// Returns an error if the reader is empty or contains only whitespace.
pub fn peek_first_non_ws_byte<R: BufRead>(reader: &mut R) -> io::Result<u8> {
    // Skip UTF-8 BOM if present at the start of the stream.
    // Handle incrementally: the BOM bytes may arrive split across buffers.
    {
        let buf = reader.fill_buf()?;
        if buf.len() >= 3 && buf[..3] == UTF8_BOM {
            reader.consume(3);
        } else if !buf.is_empty() && buf[0] == UTF8_BOM[0] {
            // Potential partial BOM — read byte-by-byte to confirm.
            let mut bom_buf = [0u8; 3];
            bom_buf[0] = buf[0];
            reader.consume(1);
            let b1 = reader.fill_buf()?;
            if !b1.is_empty() && b1[0] == UTF8_BOM[1] {
                bom_buf[1] = b1[0];
                reader.consume(1);
                let b2 = reader.fill_buf()?;
                if !b2.is_empty() && b2[0] == UTF8_BOM[2] {
                    // Full BOM consumed.
                    reader.consume(1);
                } else {
                    // Only 0xEF 0xBB seen — not a BOM, put back by returning 0xEF.
                    // We can't un-consume, so treat what we read as content.
                    // 0xEF is not ascii whitespace, so return it.
                    return Ok(bom_buf[0]);
                }
            } else {
                // Only 0xEF seen — not a BOM. 0xEF is not whitespace.
                return Ok(bom_buf[0]);
            }
        }
    }

    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Empty input while detecting JSON format",
            ));
        }
        for (i, &byte) in buf.iter().enumerate() {
            if !byte.is_ascii_whitespace() {
                // Consume only the leading whitespace, leave the non-ws byte
                reader.consume(i);
                return Ok(byte);
            }
        }
        let len = buf.len();
        reader.consume(len);
    }
}

/* -------------------------------------------------------------
SODA (Socrata Open Data API) format support
-------------------------------------------------------------*/

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
/// Returns `true` if the given bytes look like a SODA (Socrata Open Data API) response.
///
/// A SODA response is a JSON object containing:
/// - `meta`: an object containing at least one child object (e.g. `view`)
/// - `data`: an array of row arrays
///
/// This fully parses the JSON via `serde_json::from_slice` to inspect its structure,
/// so it should only be called on already-buffered data.
pub fn is_soda_response(buf: &[u8]) -> bool {
    let buf = buf.strip_prefix(&UTF8_BOM).unwrap_or(buf);
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(buf) else {
        return false;
    };
    let has_meta_with_child_object = value
        .get("meta")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|m| m.values().any(serde_json::Value::is_object));
    let has_data_array = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .is_some();
    has_meta_with_child_object && has_data_array
}

/// Build an Arrow [`Field`] for a Socrata column.
///
/// Maps Socrata `dataTypeName` values to Arrow [`DataType`]s.
///
/// Numeric types map to `Float64`, `calendar_date` maps to `Timestamp(Second)`,
/// and most other types (including `uuid`) map to `Utf8`.
///
/// For `"meta_data"` columns, the type is inferred from the well-known Socrata
/// system field names (`:id`, `:position`, `:created_at`, `:updated_at`, etc.).
fn soda_field(field_name: &str, data_type_name: &str) -> Field {
    match data_type_name {
        "number" | "money" | "percent" => Field::new(field_name, DataType::Float64, true),
        "checkbox" => Field::new(field_name, DataType::Boolean, true),
        "calendar_date" => Field::new(
            field_name,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        "meta_data" => soda_meta_data_field(field_name),
        // uuid, text, url, location, point, html, and all other types → Utf8.
        // UUID values arrive as JSON strings; SodaReader does not coerce them to binary.
        _ => Field::new(field_name, DataType::Utf8, true),
    }
}

/// Build an Arrow [`Field`] for a Socrata `meta_data` system column.
///
/// Socrata system columns have well-known semantics:
/// - `:id` — row UUID
/// - `:position` — integer row position
/// - `:created_at` / `:updated_at` — integer epoch timestamps (seconds)
/// - `:sid`, `:created_meta`, `:updated_meta`, `:meta` — string/JSON metadata
fn soda_meta_data_field(field_name: &str) -> Field {
    match field_name {
        // :position — integer row position; :created_at / :updated_at — epoch timestamps
        ":position" | ":created_at" | ":updated_at" => {
            Field::new(field_name, DataType::Int64, true)
        }
        // :id, :sid, :created_meta, :updated_meta, :meta, and any unknown meta fields → Utf8
        _ => Field::new(field_name, DataType::Utf8, true),
    }
}

/// Extract an Arrow [`Schema`] from the `meta.view.columns` array in a SODA response.
///
/// Columns with `dataTypeName == "meta_data"` are internal Socrata metadata and are
/// excluded from the returned schema unless `include_metadata` is `true`.
///
/// # Errors
///
/// Returns an error if the JSON is not a valid SODA response (missing `meta.view.columns`
/// or if the columns array cannot be read).
pub fn soda_schema_from_meta(
    value: &serde_json::Value,
    include_metadata: bool,
) -> io::Result<Schema> {
    let columns = value
        .pointer("/meta/view/columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "SODA response missing 'meta.view.columns' array",
            )
        })?;

    let fields: Vec<Field> = columns
        .iter()
        .filter_map(|col| {
            let type_name = col.get("dataTypeName")?.as_str()?;
            if !include_metadata && type_name == "meta_data" {
                return None;
            }
            let field_name = col.get("fieldName")?.as_str()?;
            Some(soda_field(field_name, type_name))
        })
        .collect();

    if fields.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SODA response has no user-visible columns in 'meta.view.columns'",
        ));
    }

    Ok(Schema::new(fields))
}

/// A reader that converts a SODA (Socrata Open Data API) response into NDJSON.
///
/// SODA responses contain:
/// - `meta.view.columns`: column definitions with `fieldName` and `dataTypeName`
/// - `data`: row data as an array of arrays
///
/// `SodaReader` extracts both, then rebuilds each row as a JSON object keyed
/// by the user-visible column field names, outputting one JSON object per line (NDJSON).
///
/// Internal columns with `dataTypeName == "meta_data"` are excluded.
#[derive(Debug)]
pub struct SodaReader {
    inner: io::Cursor<Vec<u8>>,
    schema: Schema,
}

impl SodaReader {
    /// Create a new `SodaReader` from a reader containing a SODA JSON response.
    ///
    /// When `include_metadata` is `true`, Socrata internal `meta_data` columns
    /// (sid, id, position, etc.) are included in the schema.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The input cannot be parsed as valid JSON
    /// - The JSON is not a valid SODA response (missing `meta.view.columns` or `data`)
    /// - The `data` field is not an array
    pub fn new<R: Read>(mut reader: R, include_metadata: bool) -> io::Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Self::from_vec(&buf, include_metadata)
    }

    /// Create a new `SodaReader` from an already-buffered byte slice.
    ///
    /// This avoids an extra allocation when the caller already has the data in memory.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be parsed as valid JSON or are not
    /// a valid SODA response.
    pub fn from_vec(buf: &[u8], include_metadata: bool) -> io::Result<Self> {
        // Strip leading UTF-8 BOM if present so serde_json can parse the input.
        let buf = buf.strip_prefix(&UTF8_BOM).unwrap_or(buf);

        let value: serde_json::Value = serde_json::from_slice(buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse SODA JSON response: {e}"),
            )
        })?;

        let schema = soda_schema_from_meta(&value, include_metadata)?;

        // Determine which column indices in the data rows correspond to user-visible columns
        let all_columns = value
            .pointer("/meta/view/columns")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SODA response missing 'meta.view.columns' array",
                )
            })?;

        // Build a mapping: for each included column, store its index in the
        // full columns array. Use the same filter as soda_schema_from_meta (both
        // dataTypeName and fieldName must be present, and non-meta unless
        // include_metadata is true) so that visible_indices stays in sync with
        // schema.fields().
        let visible_indices: Vec<usize> = all_columns
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                let is_visible = col
                    .get("dataTypeName")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|t| include_metadata || t != "meta_data");
                let has_field_name = col
                    .get("fieldName")
                    .and_then(serde_json::Value::as_str)
                    .is_some();
                is_visible && has_field_name
            })
            .map(|(i, _)| i)
            .collect();

        let data = value
            .get("data")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SODA response missing 'data' array",
                )
            })?;

        // Convert each data row (array) to a JSON object using field names
        let mut ndjson = Vec::new();
        for row in data {
            let row_arr = row.as_array().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SODA 'data' row is not an array",
                )
            })?;

            let mut obj = serde_json::Map::with_capacity(visible_indices.len());
            for (field_idx, &col_idx) in visible_indices.iter().enumerate() {
                let field = &schema.fields()[field_idx];
                let val = row_arr
                    .get(col_idx)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);

                // For Utf8 fields, coerce non-string values (numbers, objects,
                // arrays, booleans) to their JSON string representation so
                // Arrow's reader doesn't fail on type mismatches.
                let val = if *field.data_type() == DataType::Utf8 {
                    match &val {
                        serde_json::Value::Null | serde_json::Value::String(_) => val,
                        other => serde_json::Value::String(other.to_string()),
                    }
                } else {
                    val
                };

                obj.insert(field.name().clone(), val);
            }

            serde_json::to_writer(&mut ndjson, &serde_json::Value::Object(obj))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            ndjson.push(b'\n');
        }

        Ok(Self {
            inner: io::Cursor::new(ndjson),
            schema,
        })
    }

    /// Returns the Arrow schema derived from the SODA response metadata.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Read for SodaReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
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
/// use dataformat_json::{ArrayToNdjsonPush, ReadResult};
///
/// let mut adapter = ArrayToNdjsonPush::new();
///
/// // Push data incrementally
/// adapter.push_bytes(b"[{\"name\":").unwrap();
/// adapter.push_bytes(b"\"John\"}]").unwrap();
///
/// // Read processed NDJSON
/// match adapter.try_read() {
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
    use std::fmt::Write as _;
    use std::io::{BufReader, Cursor, Read};

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

    mod json_pointer_reader_tests {
        use super::*;
        use std::io::BufReader;

        #[test]
        fn test_extract_simple_path() {
            let input = br#"{"data": [{"id": 1}, {"id": 2}]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/data")
                .expect("should extract data");
            let mut output = String::new();
            BufReader::new(reader)
                .read_to_string(&mut output)
                .expect("should read");
            assert_eq!(output, r#"[{"id":1},{"id":2}]"#);
        }

        #[test]
        fn test_extract_simple_path_without_leading_slash() {
            // Convenience: leading '/' is added automatically
            let input = br#"{"data": [{"id": 1}]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "data")
                .expect("should extract data without leading slash");
            let mut output = String::new();
            BufReader::new(reader)
                .read_to_string(&mut output)
                .expect("should read");
            assert_eq!(output, r#"[{"id":1}]"#);
        }

        #[test]
        fn test_extract_nested_path() {
            let input = br#"{"response": {"items": [{"x": 1}]}}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/response/items")
                .expect("should extract nested path");
            let mut output = String::new();
            BufReader::new(reader)
                .read_to_string(&mut output)
                .expect("should read");
            assert_eq!(output, r#"[{"x":1}]"#);
        }

        #[test]
        fn test_missing_path_segment() {
            let input = br#"{"data": [1, 2, 3]}"#;
            let err = JsonPointerReader::new(Cursor::new(input.to_vec()), "/missing")
                .expect_err("should fail on missing path");
            assert!(
                err.to_string().contains("not found"),
                "error should mention 'not found': {err}"
            );
        }

        #[test]
        fn test_extract_scalar_value() {
            let input = br#"{"count": 42}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/count")
                .expect("should extract scalar");
            let mut output = String::new();
            BufReader::new(reader)
                .read_to_string(&mut output)
                .expect("should read");
            assert_eq!(output, "42");
        }

        #[test]
        fn test_invalid_json() {
            let input = b"not json";
            let err = JsonPointerReader::new(Cursor::new(input.to_vec()), "/data")
                .expect_err("should fail on invalid JSON");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }

        #[test]
        fn test_rfc6901_tilde_escape() {
            // RFC 6901: ~0 = ~, ~1 = /
            let input = br#"{"a/b": 1, "c~d": 2}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/a~1b")
                .expect("should handle ~1 escape for /");
            let output = read_to_string(reader);
            assert_eq!(output, "1");

            let reader2 = JsonPointerReader::new(Cursor::new(input.to_vec()), "/c~0d")
                .expect("should handle ~0 escape for ~");
            let output2 = read_to_string(reader2);
            assert_eq!(output2, "2");
        }

        #[test]
        fn test_array_index_access() {
            // JSON Pointer supports numeric indices into arrays
            let input = br#"{"items": ["a", "b", "c"]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/items/1")
                .expect("should access array index");
            let output = read_to_string(reader);
            assert_eq!(output, r#""b""#);
        }
    }

    mod peek_tests {
        use super::*;
        use std::io::BufReader;

        #[test]
        fn test_peek_array() {
            let mut reader = BufReader::new(Cursor::new(b"  [ {\"a\": 1} ]"));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
            // The reader should still contain the '[' for downstream consumers
            let mut rest = String::new();
            reader.read_to_string(&mut rest).expect("should read rest");
            assert!(rest.starts_with('['), "rest should start with '[': {rest}");
        }

        #[test]
        fn test_peek_object() {
            let mut reader = BufReader::new(Cursor::new(b"\n\t {\"key\": 1}"));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
        }

        #[test]
        fn test_peek_jsonl() {
            let mut reader = BufReader::new(Cursor::new(b"{\"a\":1}\n{\"a\":2}"));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
        }

        #[test]
        fn test_peek_empty() {
            let mut reader = BufReader::new(Cursor::new(b""));
            let err = peek_first_non_ws_byte(&mut reader).expect_err("should fail on empty");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }

        #[test]
        fn test_peek_whitespace_only() {
            let mut reader = BufReader::new(Cursor::new(b"   \n\t  "));
            let err =
                peek_first_non_ws_byte(&mut reader).expect_err("should fail on whitespace-only");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    // ----------------------------------------------------------------
    // Comprehensive tests with various JSON document formats
    // ----------------------------------------------------------------

    /// Helper: read bytes from a Read impl into a String
    fn read_to_string<R: Read>(mut r: R) -> String {
        let mut s = String::new();
        r.read_to_string(&mut s).expect("should read to string");
        s
    }

    mod json_document_format_tests {
        use super::*;
        use std::io::BufReader;

        // ---- JSONL / NDJSON documents ----

        /// Standard JSONL: one JSON object per line
        #[test]
        fn test_jsonl_standard() {
            let input = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
            // JSONL is read line-by-line; verify the content is intact
            let content = read_to_string(reader);
            assert!(content.starts_with('{'));
            assert!(content.contains("Alice"));
            assert!(content.contains("Bob"));
        }

        /// JSONL with Windows-style line endings
        #[test]
        fn test_jsonl_crlf() {
            let input = b"{\"id\":1}\r\n{\"id\":2}\r\n{\"id\":3}\r\n";
            let lines = read_all_lines(BufReader::new(Cursor::new(input.as_slice())))
                .expect("should read lines");
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[0], r#"{"id":1}"#);
            assert_eq!(lines[2], r#"{"id":3}"#);
        }

        /// JSONL with mixed value types (objects, arrays, scalars)
        #[test]
        fn test_jsonl_mixed_types() {
            let input = b"{\"type\":\"object\"}\n[1,2,3]\n42\n\"hello\"\ntrue\nnull\n";
            let lines = read_all_lines(BufReader::new(Cursor::new(input.as_slice())))
                .expect("should read lines");
            assert_eq!(lines.len(), 6);
            assert_eq!(lines[0], r#"{"type":"object"}"#);
            assert_eq!(lines[1], "[1,2,3]");
            assert_eq!(lines[2], "42");
            assert_eq!(lines[3], "\"hello\"");
            assert_eq!(lines[4], "true");
            assert_eq!(lines[5], "null");
        }

        /// JSONL with blank lines between records
        #[test]
        fn test_jsonl_blank_lines() {
            let input = b"{\"a\":1}\n\n{\"b\":2}\n\n";
            let count = read_all_lines(BufReader::new(Cursor::new(input.as_slice())))
                .expect("should read lines")
                .into_iter()
                .filter(|l| !l.is_empty())
                .count();
            assert_eq!(count, 2);
        }

        /// JSONL with extra whitespace before each line
        #[test]
        fn test_jsonl_leading_whitespace() {
            let input = b"  {\"a\":1}\n\t{\"b\":2}\n";
            let lines = read_all_lines(BufReader::new(Cursor::new(input.as_slice())))
                .expect("should read lines");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains("\"a\":1"));
        }

        // ---- JSON Array documents ----

        /// Standard JSON array
        #[test]
        fn test_array_standard() {
            let input = Cursor::new(br#"[{"name":"Alice"},{"name":"Bob"}]"#.to_vec());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse array");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 2);
            assert_eq!(lines[0], r#"{"name":"Alice"}"#);
            assert_eq!(lines[1], r#"{"name":"Bob"}"#);
        }

        /// JSON array with pretty-printed / multi-line elements
        #[test]
        fn test_array_pretty_printed() {
            let input = Cursor::new(
                br#"[
  {
    "name": "Alice",
    "age": 30
  },
  {
    "name": "Bob",
    "age": 25
  }
]"#
                .to_vec(),
            );
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse array");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains("Alice"));
            assert!(lines[1].contains("Bob"));
            // Multi-line content should be collapsed to single lines
            assert!(!lines[0].contains('\n'));
        }

        /// Empty JSON array
        #[test]
        fn test_array_empty() {
            let input = Cursor::new(b"[]".to_vec());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse empty array");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert!(lines.is_empty());
        }

        /// JSON array with a single element
        #[test]
        fn test_array_single_element() {
            let input = Cursor::new(br#"[{"only":"one"}]"#.to_vec());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], r#"{"only":"one"}"#);
        }

        /// JSON array with nested arrays and objects
        #[test]
        fn test_array_nested_complex() {
            let input = Cursor::new(
                br#"[{"tags":["rust","json"],"meta":{"v":1}},{"tags":[],"meta":{"v":2}}]"#.to_vec(),
            );
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains(r#""tags":["rust","json"]"#));
        }

        /// JSON array with leading/trailing whitespace
        #[test]
        fn test_array_whitespace_around() {
            let input = Cursor::new(b"  \n\t [ {\"a\":1} ] \n ".to_vec());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should handle whitespace");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], r#"{"a":1}"#);
        }

        /// JSON array with deeply nested structures
        #[test]
        fn test_array_deeply_nested() {
            let input = Cursor::new(br#"[{"l1":{"l2":{"l3":{"l4":"deep"}}}}]"#.to_vec());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse");
            let lines = read_all_lines(&mut adapter).expect("should read lines");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("deep"));
        }

        // ---- Single Object documents ----

        /// Single JSON object (the "object" format)
        #[test]
        fn test_object_single() {
            let input = br#"{"name":"Alice","age":30,"active":true}"#;
            // A single object is valid NDJSON (one line)
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("Alice"));
        }

        /// Single JSON object with pretty printing (multi-line)
        #[test]
        fn test_object_pretty_printed() {
            let input = br#"{
  "name": "Alice",
  "age": 30,
  "address": {
    "city": "Wonderland",
    "zip": "12345"
  }
}"#;
            // When read as NDJSON, multi-line object will be read as partial lines
            // but the arrow reader handles this correctly for single objects
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
        }

        /// Single object with null values
        #[test]
        fn test_object_with_nulls() {
            let input = br#"{"name":"Alice","middle_name":null,"scores":[null,95,null]}"#;
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("null"));
        }

        /// Single object with various data types
        #[test]
        fn test_object_mixed_types() {
            let input = br#"{"string":"hello","int":42,"float":3.14,"bool":true,"null_val":null,"array":[1,2],"nested":{"key":"val"}}"#;
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("3.14"));
            assert!(lines[0].contains("\"key\":\"val\""));
        }

        /// Empty object
        #[test]
        fn test_object_empty() {
            let input = b"{}";
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "{}");
        }

        // ---- json_pointer + different formats ----

        /// `json_pointer` extracting an array from an object wrapper
        #[test]
        fn test_json_pointer_extracts_array() {
            let input =
                br#"{"status":"ok","data":[{"id":1,"val":"a"},{"id":2,"val":"b"}],"count":2}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/data")
                .expect("should extract");
            let output = read_to_string(reader);
            assert_eq!(output, r#"[{"id":1,"val":"a"},{"id":2,"val":"b"}]"#);
        }

        /// `json_pointer` extracting a nested object
        #[test]
        fn test_json_pointer_extracts_nested_object() {
            let input = br#"{"meta":{"pagination":{"page":1,"total":100}}}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/meta/pagination")
                .expect("should extract nested");
            let output = read_to_string(reader);
            assert_eq!(output, r#"{"page":1,"total":100}"#);
        }

        /// `json_pointer` extracts from pretty-printed JSON
        #[test]
        fn test_json_pointer_pretty_printed_input() {
            let input = br#"{
  "response": {
    "items": [
      {"id": 1},
      {"id": 2}
    ]
  }
}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/response/items")
                .expect("should extract from pretty-printed input");
            let output = read_to_string(reader);
            assert_eq!(output, r#"[{"id":1},{"id":2}]"#);
        }

        /// `json_pointer` with a single-element path
        #[test]
        fn test_json_pointer_single_segment() {
            let input = br#"{"records":[{"x":10}]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/records")
                .expect("should extract");
            let output = read_to_string(reader);
            assert_eq!(output, r#"[{"x":10}]"#);
        }

        /// `json_pointer` with deeply nested path
        #[test]
        fn test_json_pointer_three_levels() {
            let input = br#"{"a":{"b":{"c":{"d":[1,2,3]}}}}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/a/b/c/d")
                .expect("should extract 3 levels");
            let output = read_to_string(reader);
            assert_eq!(output, "[1,2,3]");
        }

        /// `json_pointer` extracts a string value
        #[test]
        fn test_json_pointer_extracts_string() {
            let input = br#"{"message":"hello world","code":200}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/message")
                .expect("should extract string");
            let output = read_to_string(reader);
            assert_eq!(output, r#""hello world""#);
        }

        /// `json_pointer` extracts a boolean
        #[test]
        fn test_json_pointer_extracts_boolean() {
            let input = br#"{"success":true,"data":[]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/success")
                .expect("should extract bool");
            let output = read_to_string(reader);
            assert_eq!(output, "true");
        }

        /// `json_pointer` extracts null
        #[test]
        fn test_json_pointer_extracts_null() {
            let input = br#"{"error":null}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/error")
                .expect("should extract null");
            let output = read_to_string(reader);
            assert_eq!(output, "null");
        }

        /// `json_pointer` error on wrong path at second segment
        #[test]
        fn test_json_pointer_wrong_second_segment() {
            let input = br#"{"a":{"b":1}}"#;
            let err = JsonPointerReader::new(Cursor::new(input.to_vec()), "/a/missing")
                .expect_err("should fail");
            assert!(err.to_string().contains("not found"));
        }

        /// Empty pointer returns the whole document per RFC 6901
        #[test]
        fn test_json_pointer_empty_returns_whole_doc() {
            let input = br#"{"a":1,"b":2}"#;
            let mut reader =
                JsonPointerReader::from_vec(input, "").expect("empty pointer should succeed");
            let mut out = String::new();
            reader
                .read_to_string(&mut out)
                .expect("should read to string");
            assert_eq!(out, r#"{"a":1,"b":2}"#);
        }

        /// `from_vec` strips a leading UTF-8 BOM before parsing
        #[test]
        fn test_json_pointer_bom_stripped() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(br#"{"data":[1,2]}"#);
            let mut reader =
                JsonPointerReader::from_vec(&input, "/data").expect("BOM input should parse");
            let mut out = String::new();
            reader
                .read_to_string(&mut out)
                .expect("should read to string");
            assert_eq!(out, "[1,2]");
        }

        // ---- Auto-detect via peek ----

        /// Auto-detect: array with significant leading whitespace
        #[test]
        fn test_auto_detect_array_with_bom_whitespace() {
            let input = b"   \n\n  [\n{\"a\":1}\n]";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
        }

        /// BOM split across tiny buffers (`BufReader` with capacity=1)
        #[test]
        fn test_auto_detect_bom_split_buffers() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(b"[{\"a\":1}]");
            // BufReader with capacity=1 forces each fill_buf to return 1 byte
            let mut reader = BufReader::with_capacity(1, Cursor::new(input));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek through split BOM");
            assert_eq!(byte, b'[');
        }

        /// Partial BOM prefix (only 0xEF) — not a real BOM
        #[test]
        fn test_auto_detect_partial_bom_single_byte() {
            let input = vec![0xEF, b'['];
            let mut reader = BufReader::with_capacity(1, Cursor::new(input));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should return 0xEF");
            assert_eq!(byte, 0xEF);
        }

        /// Auto-detect: object
        #[test]
        fn test_auto_detect_object() {
            let input = b" {\"key\":\"value\"}";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
        }

        /// Auto-detect: scalar (number) — would be Jsonl
        #[test]
        fn test_auto_detect_scalar() {
            let input = b"42";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'4');
            // Not '[' or '{', so Auto would treat as Jsonl
        }

        /// Auto-detect: string value starts with quote
        #[test]
        fn test_auto_detect_string() {
            let input = b"\"hello\"";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'"');
        }

        // ---- Edge cases ----

        /// JSON with Unicode characters
        #[test]
        fn test_unicode_content() {
            let input = r#"{"emoji":"🚀","jp":"日本語","name":"Ñoño"}"#;
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_bytes()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("🚀"));
            assert!(lines[0].contains("日本語"));
        }

        /// JSON with escaped characters
        #[test]
        fn test_escaped_content() {
            let input = br#"{"path":"C:\\Users\\test","quote":"He said \"hello\""}"#;
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains(r"C:\\Users\\test"));
        }

        /// JSON array with very large number of small elements
        #[test]
        fn test_array_many_elements() {
            let mut json = String::from("[");
            for i in 0..500 {
                if i > 0 {
                    json.push(',');
                }
                write!(json, r#"{{"i":{i}}}"#).expect("write");
            }
            json.push(']');
            let input = Cursor::new(json.into_bytes());
            let mut adapter = ArrayToNdjson::try_new(input).expect("should parse");
            let lines = read_all_lines(&mut adapter).expect("should read");
            assert_eq!(lines.len(), 500);
            assert_eq!(lines[0], r#"{"i":0}"#);
            assert_eq!(lines[499], r#"{"i":499}"#);
        }

        /// JSON with numeric precision edge cases
        #[test]
        fn test_numeric_precision() {
            let input =
                br#"{"big":9007199254740993,"tiny":0.000000001,"neg":-1.5e10,"sci":1.23e-4}"#;
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("9007199254740993"));
        }

        /// JSONL with trailing newline
        #[test]
        fn test_jsonl_trailing_newline() {
            let input = b"{\"a\":1}\n{\"b\":2}\n";
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// JSONL without trailing newline
        #[test]
        fn test_jsonl_no_trailing_newline() {
            let input = b"{\"a\":1}\n{\"b\":2}";
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_slice()))).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Array with trailing comma (should fail gracefully)
        #[test]
        fn test_array_trailing_comma() {
            let input = Cursor::new(br#"[{"a":1},]"#.to_vec());
            // serde_json rejects trailing commas - this should produce an error
            let adapter = ArrayToNdjson::try_new(input);
            // Depending on implementation, this may fail at try_new or during read
            if let Ok(mut adapter) = adapter {
                let result = read_all_lines(&mut adapter);
                // Either we get an error or we get partial results
                if let Ok(lines) = result {
                    // If it somehow succeeded, we should at least have the valid element
                    assert!(!lines.is_empty());
                }
            }
        }

        /// JSON with very long string values
        #[test]
        fn test_long_string_values() {
            let long_val: String = "x".repeat(10_000);
            let input = format!(r#"{{"data":"{long_val}"}}"#);
            let lines =
                read_all_lines(BufReader::new(Cursor::new(input.as_bytes()))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].len() > 10_000);
        }

        /// `json_pointer` extracting from array-wrapped response
        #[test]
        fn test_json_pointer_then_array_to_ndjson() {
            let input = br#"{"data":[{"id":1},{"id":2},{"id":3}]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/data")
                .expect("should extract");
            let mut adapter = ArrayToNdjson::try_new(reader).expect("should parse extracted array");
            let lines = read_all_lines(&mut adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[0], r#"{"id":1}"#);
            assert_eq!(lines[2], r#"{"id":3}"#);
        }

        /// `json_pointer` then auto-detect as array
        #[test]
        fn test_json_pointer_auto_detect_array() {
            let input = br#"{"result":[{"x":1}]}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/result")
                .expect("should extract");
            let mut buf_reader = BufReader::new(reader);
            let byte = peek_first_non_ws_byte(&mut buf_reader).expect("should peek");
            assert_eq!(byte, b'[', "extracted value should start with '['");
        }

        /// `json_pointer` then auto-detect as object
        #[test]
        fn test_json_pointer_auto_detect_object() {
            let input = br#"{"result":{"name":"test"}}"#;
            let reader = JsonPointerReader::new(Cursor::new(input.to_vec()), "/result")
                .expect("should extract");
            let mut buf_reader = BufReader::new(reader);
            let byte = peek_first_non_ws_byte(&mut buf_reader).expect("should peek");
            assert_eq!(byte, b'{', "extracted value should start with '{{'");
        }
    }

    mod soda_tests {
        use crate::{SodaReader, soda_schema_from_meta};
        use arrow::datatypes::{DataType, TimeUnit};
        use std::io::{Cursor, Read};

        /// Helper: build a minimal SODA response JSON string.
        fn soda_response(
            columns: &[(&str, &str, &str)],
            data: &[Vec<serde_json::Value>],
        ) -> String {
            let cols: Vec<serde_json::Value> = columns
                .iter()
                .enumerate()
                .map(|(i, (field, dtype, name))| {
                    serde_json::json!({
                        "id": i,
                        "name": name,
                        "fieldName": field,
                        "dataTypeName": dtype,
                        "position": i,
                        "renderTypeName": dtype,
                    })
                })
                .collect();
            let body = serde_json::json!({
                "meta": { "view": { "columns": cols } },
                "data": data,
            });
            serde_json::to_string(&body).expect("should serialize")
        }

        // ---- Schema extraction ----

        #[test]
        fn test_soda_schema_basic() {
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    ("name", "text", "Name"),
                    ("count", "number", "Count"),
                ],
                &[],
            );
            let val: serde_json::Value = serde_json::from_str(&input).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
            assert_eq!(schema.field(1).name(), "count");
            assert_eq!(*schema.field(1).data_type(), DataType::Float64);
        }

        #[test]
        fn test_soda_schema_all_meta_data_filtered() {
            let input = soda_response(
                &[(":sid", "meta_data", "SID"), (":id", "meta_data", "ID")],
                &[],
            );
            let val: serde_json::Value = serde_json::from_str(&input).expect("parse");
            let err =
                soda_schema_from_meta(&val, false).expect_err("should fail: no visible columns");
            assert!(err.to_string().contains("no user-visible columns"));
        }

        #[test]
        fn test_soda_schema_include_metadata() {
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    (":id", "meta_data", "ID"),
                    ("name", "text", "Name"),
                ],
                &[],
            );
            let val: serde_json::Value = serde_json::from_str(&input).expect("parse");

            // Without metadata: only user columns
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "name");

            // With metadata: all columns including meta_data
            let schema = soda_schema_from_meta(&val, true).expect("should extract schema");
            assert_eq!(schema.fields().len(), 3);
            assert_eq!(schema.field(0).name(), ":sid");
            assert_eq!(schema.field(1).name(), ":id");
            assert_eq!(schema.field(2).name(), "name");
        }

        #[test]
        fn test_soda_reader_include_metadata() {
            let input = soda_response(
                &[(":sid", "meta_data", "SID"), ("name", "text", "Name")],
                &[vec![
                    serde_json::json!("row-abc"),
                    serde_json::json!("Alice"),
                ]],
            );

            // Without metadata
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 1);
            assert_eq!(soda.schema().field(0).name(), "name");

            // With metadata
            let mut soda =
                SodaReader::new(Cursor::new(input.as_bytes()), true).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 2);
            assert_eq!(soda.schema().field(0).name(), ":sid");
            assert_eq!(soda.schema().field(1).name(), "name");
            let mut output = String::new();
            soda.read_to_string(&mut output).expect("should read");
            let line: serde_json::Value = serde_json::from_str(output.trim()).expect("parse line");
            assert_eq!(line[":sid"], "row-abc");
            assert_eq!(line["name"], "Alice");
        }

        #[test]
        fn test_soda_schema_missing_meta() {
            let input = r#"{"data": []}"#;
            let val: serde_json::Value = serde_json::from_str(input).expect("parse");
            let err = soda_schema_from_meta(&val, false).expect_err("should fail: no meta");
            assert!(err.to_string().contains("meta.view.columns"));
        }

        #[test]
        fn test_soda_schema_type_mapping() {
            let types = vec![
                ("f_text", "text", DataType::Utf8),
                ("f_number", "number", DataType::Float64),
                ("f_money", "money", DataType::Float64),
                ("f_percent", "percent", DataType::Float64),
                ("f_checkbox", "checkbox", DataType::Boolean),
                (
                    "f_date",
                    "calendar_date",
                    DataType::Timestamp(TimeUnit::Second, None),
                ),
                ("f_uuid", "uuid", DataType::Utf8),
                ("f_url", "url", DataType::Utf8),
                ("f_location", "location", DataType::Utf8),
                ("f_point", "point", DataType::Utf8),
                ("f_html", "html", DataType::Utf8),
                ("f_unknown", "some_new_type", DataType::Utf8),
            ];
            let columns: Vec<(&str, &str, &str)> =
                types.iter().map(|(f, t, _)| (*f, *t, *f)).collect();
            let input = soda_response(&columns, &[]);
            let val: serde_json::Value = serde_json::from_str(&input).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            assert_eq!(schema.fields().len(), types.len());
            for (i, (field_name, _, expected_type)) in types.iter().enumerate() {
                assert_eq!(schema.field(i).name(), *field_name);
                assert_eq!(*schema.field(i).data_type(), *expected_type);
            }
        }

        // ---- SodaReader data conversion ----

        #[test]
        fn test_soda_reader_basic() {
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    ("name", "text", "Name"),
                    ("age", "number", "Age"),
                ],
                &[
                    vec![
                        serde_json::json!("row-1"),
                        serde_json::json!("Alice"),
                        serde_json::json!(30),
                    ],
                    vec![
                        serde_json::json!("row-2"),
                        serde_json::json!("Bob"),
                        serde_json::json!(25),
                    ],
                ],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 2);
            assert_eq!(soda.schema().field(0).name(), "name");

            // Read NDJSON output
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2);

            let row0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row0");
            assert_eq!(row0["name"], "Alice");
            assert_eq!(row0["age"], 30);

            let row1: serde_json::Value = serde_json::from_str(lines[1]).expect("parse row1");
            assert_eq!(row1["name"], "Bob");
            assert_eq!(row1["age"], 25);
        }

        #[test]
        fn test_soda_reader_empty_data() {
            let input = soda_response(
                &[("name", "text", "Name"), ("count", "number", "Count")],
                &[],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            assert!(output.trim().is_empty());
        }

        #[test]
        fn test_soda_reader_null_values() {
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    ("name", "text", "Name"),
                    ("age", "number", "Age"),
                ],
                &[vec![
                    serde_json::json!("row-1"),
                    serde_json::json!(null),
                    serde_json::json!(null),
                ]],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let row: serde_json::Value = serde_json::from_str(output.trim()).expect("parse row");
            assert!(row["name"].is_null());
            assert!(row["age"].is_null());
        }

        #[test]
        fn test_soda_reader_multiple_meta_data_columns() {
            // Multiple meta_data columns interspersed with user columns
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    (":id", "meta_data", "ID"),
                    ("city", "text", "City"),
                    (":created_at", "meta_data", "Created"),
                    ("population", "number", "Population"),
                ],
                &[vec![
                    serde_json::json!("sid-1"),
                    serde_json::json!(42),
                    serde_json::json!("Seattle"),
                    serde_json::json!("2024-01-01"),
                    serde_json::json!(750_000),
                ]],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 2);
            assert_eq!(soda.schema().field(0).name(), "city");
            assert_eq!(soda.schema().field(1).name(), "population");

            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let row: serde_json::Value = serde_json::from_str(output.trim()).expect("parse row");
            assert_eq!(row["city"], "Seattle");
            assert_eq!(row["population"], 750_000);
            // Meta columns should not appear in output
            assert!(row.get(":sid").is_none());
            assert!(row.get(":id").is_none());
            assert!(row.get(":created_at").is_none());
        }

        #[test]
        fn test_soda_reader_missing_data_field() {
            let input =
                r#"{"meta":{"view":{"columns":[{"fieldName":"name","dataTypeName":"text"}]}}}"#;
            let err = SodaReader::new(Cursor::new(input.as_bytes()), false)
                .expect_err("should fail: no data field");
            assert!(err.to_string().contains("missing 'data' array"));
        }

        #[test]
        fn test_soda_reader_invalid_json() {
            let input = b"not valid json";
            let err = SodaReader::new(Cursor::new(input.to_vec()), false)
                .expect_err("should fail: invalid JSON");
            assert!(err.to_string().contains("Failed to parse SODA"));
        }

        /// `from_vec` strips a leading UTF-8 BOM before parsing
        #[test]
        fn test_soda_reader_bom_stripped() {
            let json = r#"{"meta":{"view":{"columns":[{"fieldName":"name","dataTypeName":"text"}]}},"data":[["Alice"]]}"#;
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(json.as_bytes());
            let mut reader = SodaReader::from_vec(&input, false).expect("BOM input should parse");
            let mut out = String::new();
            reader
                .read_to_string(&mut out)
                .expect("should read to string");
            assert!(out.contains("Alice"), "expected Alice in output: {out}");
        }

        #[test]
        fn test_soda_reader_data_row_not_array() {
            let input = r#"{"meta":{"view":{"columns":[{"fieldName":"x","dataTypeName":"text"}]}},"data":["not_an_array"]}"#;
            let err = SodaReader::new(Cursor::new(input.as_bytes()), false)
                .expect_err("should fail: row not array");
            assert!(err.to_string().contains("not an array"));
        }

        #[test]
        fn test_soda_reader_row_shorter_than_columns() {
            // If a row has fewer elements than columns, missing values become null
            let input = soda_response(
                &[
                    (":sid", "meta_data", "SID"),
                    ("name", "text", "Name"),
                    ("age", "number", "Age"),
                ],
                &[vec![serde_json::json!("row-1"), serde_json::json!("Alice")]],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let row: serde_json::Value = serde_json::from_str(output.trim()).expect("parse row");
            assert_eq!(row["name"], "Alice");
            assert!(row["age"].is_null());
        }

        #[test]
        fn test_soda_reader_preserves_nested_json() {
            // Nested objects in Utf8 columns are stringified for Arrow compatibility
            let input = soda_response(
                &[("name", "text", "Name"), ("coords", "point", "Coordinates")],
                &[vec![
                    serde_json::json!("Central Park"),
                    serde_json::json!({"lat": 40.785, "lon": -73.968}),
                ]],
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let row: serde_json::Value = serde_json::from_str(output.trim()).expect("parse row");
            assert_eq!(row["name"], "Central Park");
            // coords is Utf8 so nested object is stringified
            let coords_str = row["coords"].as_str().expect("coords should be a string");
            let coords: serde_json::Value = serde_json::from_str(coords_str).expect("parse coords");
            assert_eq!(coords["lat"], 40.785);
        }

        #[test]
        fn test_soda_reader_many_rows() {
            let rows: Vec<Vec<serde_json::Value>> = (0..100)
                .map(|i| vec![serde_json::json!(format!("item-{i}")), serde_json::json!(i)])
                .collect();
            let input = soda_response(
                &[("name", "text", "Name"), ("value", "number", "Value")],
                &rows,
            );
            let soda = SodaReader::new(Cursor::new(input.as_bytes()), false).expect("should parse");
            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            assert_eq!(output.lines().count(), 100);
        }

        #[test]
        fn test_soda_schema_nullable() {
            let input = soda_response(
                &[("name", "text", "Name"), ("count", "number", "Count")],
                &[],
            );
            let val: serde_json::Value = serde_json::from_str(&input).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            // All SODA columns should be nullable
            for field in schema.fields() {
                assert!(
                    field.is_nullable(),
                    "field '{}' should be nullable",
                    field.name()
                );
            }
        }
    }

    // ----------------------------------------------------------------
    // Comprehensive auto-detection tests
    // ----------------------------------------------------------------

    /// Helper: simulate the full auto-detect flow used by `Format::Auto`.
    /// Peeks at the first non-ws byte, then reads data through the appropriate
    /// pipeline (`SodaReader` for SODA, `ArrayToNdjson` for arrays, direct
    /// `BufRead` for objects/JSONL).
    /// Returns the parsed NDJSON lines.
    fn auto_detect_and_read(input: &[u8]) -> io::Result<Vec<String>> {
        // Try SODA first (needs full buffer)
        if is_soda_response(input) {
            let soda = SodaReader::new(Cursor::new(input), false)?;
            return read_all_lines(BufReader::new(soda));
        }

        let mut reader = BufReader::new(Cursor::new(input));
        let first_byte = peek_first_non_ws_byte(&mut reader)?;

        if first_byte == b'[' {
            let adapter = ArrayToNdjson::try_new(reader)?;
            read_all_lines(adapter)
        } else {
            read_all_lines(reader)
        }
    }

    mod auto_detect_tests {
        use super::*;
        use std::io::BufReader;

        // ==================================================
        // Array detection (`[` as first non-ws byte)
        // ==================================================

        /// Auto detects a simple JSON array
        #[test]
        fn test_auto_array_simple() {
            let input = br#"[{"id":1},{"id":2}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains("\"id\":1") || lines[0].contains("\"id\": 1"));
        }

        /// Auto detects a pretty-printed JSON array
        #[test]
        fn test_auto_array_pretty_printed() {
            let input = b"[\n  {\"name\": \"Alice\"},\n  {\"name\": \"Bob\"}\n]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Auto detects array with leading whitespace (spaces, tabs, newlines)
        #[test]
        fn test_auto_array_leading_whitespace() {
            let input = b"  \t\n  \n  [{\"x\":1}]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Auto detects array with only leading newlines
        #[test]
        fn test_auto_array_leading_newlines() {
            let input = b"\n\n\n[{\"a\":1},{\"a\":2},{\"a\":3}]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// Auto detects empty array
        #[test]
        fn test_auto_array_empty() {
            let input = b"[]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 0);
        }

        /// Auto detects empty array with internal whitespace
        #[test]
        fn test_auto_array_empty_with_whitespace() {
            let input = b"[   ]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 0);
        }

        /// Auto detects array with single element
        #[test]
        fn test_auto_array_single_element() {
            let input = br#"[{"only":"one"}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("only"));
        }

        /// Auto detects array with nested objects
        #[test]
        fn test_auto_array_nested_objects() {
            let input = br#"[{"user":{"name":"Alice","addr":{"city":"NYC"}}},{"user":{"name":"Bob","addr":{"city":"LA"}}}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains("Alice"));
            assert!(lines[1].contains("Bob"));
        }

        /// Auto detects array with nested arrays inside objects
        #[test]
        fn test_auto_array_with_inner_arrays() {
            let input = br#"[{"tags":["a","b"]},{"tags":["c"]}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Auto detects array with mixed value types
        #[test]
        fn test_auto_array_mixed_types() {
            let input = br#"[{"a":1,"b":"text","c":true,"d":null,"e":3.14}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Auto detects array with large whitespace gap before bracket
        #[test]
        fn test_auto_array_large_leading_whitespace() {
            let ws = " ".repeat(4096);
            let input = format!("{ws}[{{\"k\":1}}]");
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Auto detects array spanning multiple lines with CRLF
        #[test]
        fn test_auto_array_crlf() {
            let input = b"[\r\n{\"a\":1},\r\n{\"a\":2}\r\n]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Auto detects array with Unicode content
        #[test]
        fn test_auto_array_unicode() {
            let input = r#"[{"emoji":"🎉","name":"日本語"}]"#;
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("🎉"));
        }

        /// Auto detects array with many elements
        #[test]
        fn test_auto_array_many_elements() {
            let elements: Vec<String> = (0..200).map(|i| format!(r#"{{"id":{i}}}"#)).collect();
            let input = format!("[{}]", elements.join(","));
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 200);
        }

        // ==================================================
        // Object detection (`{` as first non-ws byte)
        // ==================================================

        /// Auto detects a single JSON object (treated as JSONL with one line)
        #[test]
        fn test_auto_object_single() {
            let input = br#"{"name":"Alice","age":30}"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("Alice"));
        }

        /// Auto detects object with leading whitespace
        #[test]
        fn test_auto_object_leading_whitespace() {
            let input = b"   \n\t  {\"key\":\"value\"}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Auto detects pretty-printed object — `read_all_lines` splits on newlines,
        /// so a pretty-printed JSON gets multiple lines. The actual Arrow reader
        /// handles multi-line JSON objects correctly.
        #[test]
        fn test_auto_object_pretty_printed() {
            let input = b"{\n  \"name\": \"Alice\",\n  \"age\": 30\n}";
            let lines = auto_detect_and_read(input).expect("should read");
            // Pretty-printed object spans 4 lines; JSONL reader sees each line
            assert!(!lines.is_empty(), "should produce at least one line");
            // The first line starts with '{'
            assert!(lines[0].starts_with('{'));
        }

        /// Auto detects JSONL (multiple objects, each on a line) — first byte is `{`
        #[test]
        fn test_auto_jsonl_multiple_objects() {
            let input = b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// Auto detects JSONL with leading whitespace before first `{`
        #[test]
        fn test_auto_jsonl_leading_whitespace() {
            let input = b"  \n {\"x\":1}\n{\"x\":2}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Auto detects JSONL with blank lines between records
        #[test]
        fn test_auto_jsonl_blank_lines() {
            let input = b"{\"a\":1}\n\n{\"b\":2}\n\n{\"c\":3}";
            let lines = auto_detect_and_read(input).expect("should read");
            // Blank lines produce empty strings when read via read_line
            assert_eq!(lines.iter().filter(|l| !l.is_empty()).count(), 3);
        }

        /// Auto detects JSONL with CRLF line endings
        #[test]
        fn test_auto_jsonl_crlf() {
            let input = b"{\"a\":1}\r\n{\"b\":2}\r\n{\"c\":3}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// Auto detects JSONL with trailing newline
        #[test]
        fn test_auto_jsonl_trailing_newline() {
            let input = b"{\"a\":1}\n{\"b\":2}\n";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Auto detects JSONL with nested objects
        #[test]
        fn test_auto_jsonl_nested_objects() {
            let input = b"{\"user\":{\"name\":\"Alice\"}}\n{\"user\":{\"name\":\"Bob\"}}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
            assert!(lines[0].contains("Alice"));
        }

        /// Object with nested arrays — still detected as object/JSONL (first byte is `{`)
        #[test]
        fn test_auto_object_with_array_values() {
            let input = br#"{"data":[1,2,3],"meta":{"count":3}}"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("\"data\""));
        }

        // ==================================================
        // Scalar / non-standard first bytes → JSONL fallback
        // ==================================================

        /// Number as first byte → JSONL
        #[test]
        fn test_auto_scalar_number() {
            let input = b"42\n99\n7";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[0], "42");
        }

        /// String as first byte → JSONL
        #[test]
        fn test_auto_scalar_string() {
            let input = b"\"hello\"\n\"world\"";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// `true` as first byte → JSONL
        #[test]
        fn test_auto_scalar_true() {
            let input = b"true\nfalse\ntrue";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[0], "true");
        }

        /// `null` as first byte → JSONL
        #[test]
        fn test_auto_scalar_null() {
            let input = b"null\nnull";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
            assert_eq!(lines[0], "null");
        }

        /// Negative number as first byte → JSONL
        #[test]
        fn test_auto_scalar_negative() {
            let input = b"-3.14";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "-3.14");
        }

        // ==================================================
        // Edge cases: empty / whitespace-only input
        // ==================================================

        /// Empty input fails auto-detection
        #[test]
        fn test_auto_empty_input() {
            let input = b"";
            let err = auto_detect_and_read(input).expect_err("should fail on empty");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }

        /// Whitespace-only input fails auto-detection
        #[test]
        fn test_auto_whitespace_only() {
            let input = b"   \n\t\r\n  ";
            let err = auto_detect_and_read(input).expect_err("should fail on whitespace-only");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }

        /// Single space before content
        #[test]
        fn test_auto_single_space_before_array() {
            let input = b" [{\"a\":1}]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Tab before content
        #[test]
        fn test_auto_tab_before_object() {
            let input = b"\t{\"a\":1}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        /// Carriage return before content
        #[test]
        fn test_auto_cr_before_content() {
            let input = b"\r[{\"x\":1}]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        // ==================================================
        // peek preserves content for downstream reading
        // ==================================================

        /// After peek detects array, full content is available for `ArrayToNdjson`
        #[test]
        fn test_auto_peek_preserves_array_content() {
            let input = b"[{\"a\":1},{\"a\":2},{\"a\":3}]";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');

            // Now read through ArrayToNdjson — content must not be lost
            let adapter = ArrayToNdjson::try_new(reader).expect("should adapt");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// After peek detects object, full content is available for direct read
        #[test]
        fn test_auto_peek_preserves_object_content() {
            let input = b"  {\"name\":\"test\",\"value\":42}";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');

            // Read the remaining content — `{` must still be there
            let mut rest = String::new();
            reader.read_to_string(&mut rest).expect("should read");
            assert!(rest.starts_with('{'));
            let parsed: serde_json::Value =
                serde_json::from_str(&rest).expect("should parse as JSON");
            assert_eq!(parsed["name"], "test");
        }

        /// After peek detects JSONL, all lines are preserved
        #[test]
        fn test_auto_peek_preserves_jsonl_content() {
            let input = b"  {\"a\":1}\n{\"b\":2}\n{\"c\":3}";
            let mut reader = BufReader::new(Cursor::new(input.as_slice()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');

            let lines = read_all_lines(reader).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// After peek with large whitespace, content is preserved
        #[test]
        fn test_auto_peek_preserves_after_large_whitespace() {
            let ws = " ".repeat(8192);
            let json = r#"[{"k":"v"}]"#;
            let input = format!("{ws}{json}");
            let mut reader = BufReader::new(Cursor::new(input.as_bytes()));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');

            let adapter = ArrayToNdjson::try_new(reader).expect("should adapt");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 1);
        }

        // ==================================================
        // Combined with json_pointer extraction
        // ==================================================

        /// `json_pointer` extracts array → auto detects as array
        #[test]
        fn test_auto_json_pointer_extracts_array() {
            let input = br#"{"response":{"data":[{"id":1},{"id":2}]}}"#;
            let extracted = JsonPointerReader::new(Cursor::new(input.to_vec()), "/response/data")
                .expect("should extract");

            let mut reader = BufReader::new(extracted);
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');

            let adapter = ArrayToNdjson::try_new(reader).expect("should adapt");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// `json_pointer` extracts object → auto detects as object
        #[test]
        fn test_auto_json_pointer_extracts_object() {
            let input = br#"{"wrapper":{"name":"test","value":99}}"#;
            let extracted = JsonPointerReader::new(Cursor::new(input.to_vec()), "/wrapper")
                .expect("should extract");

            let mut reader = BufReader::new(extracted);
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');

            let lines = read_all_lines(reader).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("test"));
        }

        /// `json_pointer` extracts scalar → auto treats as JSONL
        #[test]
        fn test_auto_json_pointer_extracts_scalar() {
            let input = br#"{"meta":{"count":42}}"#;
            let extracted = JsonPointerReader::new(Cursor::new(input.to_vec()), "/meta/count")
                .expect("should extract");

            let mut reader = BufReader::new(extracted);
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            // "42" starts with '4', not '[' or '{' — treated as JSONL
            assert_eq!(byte, b'4');

            let lines = read_all_lines(reader).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "42");
        }

        // ==================================================
        // Ambiguous / tricky content
        // ==================================================

        /// Object where a value is an array — first byte `{` means it's JSONL/Object, not Array
        #[test]
        fn test_auto_object_containing_array_value() {
            let input = br#"{"items":[1,2,3]}"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            // The whole object is one JSONL line, not 3 array elements
        }

        /// Array of arrays — inner arrays are converted to positional objects
        /// so Arrow's JSON reader can handle them (it requires objects, not arrays).
        #[test]
        fn test_auto_array_of_arrays() {
            let input = br"[[1,2],[3,4]]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
            // Inner arrays become {"0":1,"1":2} etc.
            let parsed: serde_json::Value = serde_json::from_str(&lines[0]).expect("valid JSON");
            assert_eq!(parsed["0"], 1);
            assert_eq!(parsed["1"], 2);
        }

        /// Array containing mixed types (objects of different shapes)
        #[test]
        fn test_auto_array_heterogeneous_objects() {
            let input = br#"[{"a":1},{"b":"two"},{"c":true,"d":null}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
        }

        /// Deeply nested single object — auto sees `{`
        #[test]
        fn test_auto_deeply_nested_object() {
            let input = br#"{"l1":{"l2":{"l3":{"l4":{"value":"deep"}}}}}"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("deep"));
        }

        /// String that looks like JSON inside quotes
        #[test]
        fn test_auto_string_with_brackets() {
            // A JSON string value starting with `"` — not `[` or `{`
            let input = br#""[not an array]""#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], r#""[not an array]""#);
        }

        /// Object with key named "array" containing brackets in string
        #[test]
        fn test_auto_object_with_bracket_string_value() {
            let input = br#"{"key":"[1,2,3]"}"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            // Must be treated as a single object, not parsed as array
        }

        /// Large payload auto-detected as array
        #[test]
        fn test_auto_array_large_payload() {
            let elements: Vec<String> = (0..1000)
                .map(|i| format!(r#"{{"id":{i},"name":"item_{i}","active":true}}"#))
                .collect();
            let input = format!("[{}]", elements.join(","));
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 1000);
        }

        /// Large payload auto-detected as JSONL
        #[test]
        fn test_auto_jsonl_large_payload() {
            let lines_input: Vec<String> = (0..500).map(|i| format!(r#"{{"id":{i}}}"#)).collect();
            let input = lines_input.join("\n");
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 500);
        }

        /// Array with objects containing escaped quotes
        #[test]
        fn test_auto_array_escaped_quotes() {
            let input = br#"[{"msg":"He said \"hello\""},{"msg":"She said \"bye\""}]"#;
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// JSONL with objects containing escaped newlines in values
        #[test]
        fn test_auto_jsonl_escaped_newlines_in_values() {
            // \n inside a JSON string value should NOT split JSONL lines
            let input = "{\"msg\":\"line1\\nline2\"}\n{\"msg\":\"line3\"}";
            let lines = auto_detect_and_read(input.as_bytes()).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        /// Array with empty objects
        #[test]
        fn test_auto_array_empty_objects() {
            let input = br"[{},{},{}]";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[0], "{}");
        }

        /// Single empty object
        #[test]
        fn test_auto_single_empty_object() {
            let input = b"{}";
            let lines = auto_detect_and_read(input).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "{}");
        }

        /// Array with null elements — `ArrayToNdjson` doesn't handle bare scalars,
        /// so this exercises the error path
        #[test]
        fn test_auto_array_null_elements() {
            let input = br"[null,null,null]";
            // ArrayToNdjson uses RawValue which doesn't handle bare scalars
            let result = auto_detect_and_read(input);
            assert!(
                result.is_err(),
                "bare null elements should fail in ArrayToNdjson"
            );
        }

        /// Array with numeric elements — `ArrayToNdjson` doesn't handle bare scalars,
        /// so this exercises the error path
        #[test]
        fn test_auto_array_numeric_elements() {
            let input = br"[1,2,3,4,5]";
            // ArrayToNdjson uses RawValue which doesn't handle bare scalars
            let result = auto_detect_and_read(input);
            assert!(
                result.is_err(),
                "bare numeric elements should fail in ArrayToNdjson"
            );
        }
    }

    // ----------------------------------------------------------------
    // File-based tests using committed JSON fixtures
    // ----------------------------------------------------------------

    /// Base path for test data files (relative to this source file).
    const TEST_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/");

    /// Load a test fixture file by name.
    fn load_fixture(name: &str) -> Vec<u8> {
        let path = format!("{TEST_DATA_DIR}{name}");
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
    }

    mod file_jsonl_tests {
        use super::*;

        #[test]
        fn test_file_jsonl_standard() {
            let data = load_fixture("jsonl_standard.json");
            let lines =
                read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read JSONL");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse row 0");
            assert_eq!(r0["name"], "Alice");
            assert_eq!(r0["age"], 30);
            assert_eq!(r0["active"], true);
            let r2: serde_json::Value = serde_json::from_str(&lines[2]).expect("parse row 2");
            assert_eq!(r2["name"], "Charlie");
        }

        #[test]
        fn test_file_jsonl_crlf() {
            let data = load_fixture("jsonl_crlf.json");
            // Verify file actually has CRLF
            assert!(
                data.windows(2).any(|w| w == b"\r\n"),
                "fixture should contain CRLF"
            );
            let lines =
                read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read CRLF JSONL");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["id"], 1);
            assert_eq!(r0["status"], "ok");
        }

        #[test]
        fn test_file_jsonl_nested() {
            let data = load_fixture("jsonl_nested.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data)))
                .expect("should read nested JSONL");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["user"]["name"], "Alice");
            assert_eq!(r0["user"]["addr"]["city"], "NYC");
            assert_eq!(r0["scores"].as_array().expect("scores array").len(), 3);
        }

        #[test]
        fn test_file_jsonl_auto_detect() {
            let data = load_fixture("jsonl_standard.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect JSONL");
            assert_eq!(lines.len(), 3);
        }

        #[test]
        fn test_file_jsonl_crlf_auto_detect() {
            let data = load_fixture("jsonl_crlf.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect CRLF JSONL");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["id"], 1);
        }

        #[test]
        fn test_file_jsonl_nested_auto_detect() {
            let data = load_fixture("jsonl_nested.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect nested JSONL");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["user"]["name"], "Alice");
        }
    }

    mod file_array_tests {
        use super::*;

        #[test]
        fn test_file_array_standard() {
            let data = load_fixture("array_standard.json");
            let adapter = ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["name"], "Alice");
            assert_eq!(r0["age"], 30);
        }

        #[test]
        fn test_file_array_pretty() {
            let data = load_fixture("array_pretty.json");
            let adapter =
                ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse pretty array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            // Pretty-printed elements should be collapsed to single lines
            for line in &lines {
                assert!(!line.contains('\n'), "lines should not contain newlines");
            }
            let r2: serde_json::Value = serde_json::from_str(&lines[2]).expect("parse");
            assert_eq!(r2["name"], "Charlie");
            assert_eq!(r2["city"], "Chicago");
        }

        #[test]
        fn test_file_array_empty() {
            let data = load_fixture("array_empty.json");
            let adapter =
                ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse empty array");
            let lines = read_all_lines(adapter).expect("should read");
            assert!(lines.is_empty());
        }

        #[test]
        fn test_file_array_single() {
            let data = load_fixture("array_single.json");
            let adapter = ArrayToNdjson::try_new(Cursor::new(data))
                .expect("should parse single-element array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 1);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["only"], "one");
            assert_eq!(r0["value"], 42);
        }

        #[test]
        fn test_file_array_nested() {
            let data = load_fixture("array_nested.json");
            let adapter =
                ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse nested array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["tags"].as_array().expect("tags").len(), 2);
            assert_eq!(r0["meta"]["version"], 1);

            let r2: serde_json::Value = serde_json::from_str(&lines[2]).expect("parse");
            assert!(r2["tags"].as_array().expect("tags").is_empty());
        }

        #[test]
        fn test_file_array_mixed_types() {
            let data = load_fixture("array_mixed_types.json");
            let adapter =
                ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse mixed types");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 1);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["str"], "hello");
            assert_eq!(r0["int"], 42);
            assert!((r0["float"].as_f64().expect("float") - 1.2345).abs() < f64::EPSILON);
            assert_eq!(r0["bool"], true);
            assert!(r0["null_val"].is_null());
            assert_eq!(r0["nested"]["key"], "val");
            assert_eq!(r0["arr"].as_array().expect("arr").len(), 3);
        }

        #[test]
        fn test_file_array_auto_detect() {
            let data = load_fixture("array_standard.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect array");
            assert_eq!(lines.len(), 3);
        }

        #[test]
        fn test_file_array_pretty_auto_detect() {
            let data = load_fixture("array_pretty.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect pretty array");
            assert_eq!(lines.len(), 3);
        }

        #[test]
        fn test_file_array_empty_auto_detect() {
            let data = load_fixture("array_empty.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect empty array");
            assert!(lines.is_empty());
        }

        #[test]
        fn test_file_array_single_auto_detect() {
            let data = load_fixture("array_single.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect single array");
            assert_eq!(lines.len(), 1);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["only"], "one");
        }

        #[test]
        fn test_file_array_nested_auto_detect() {
            let data = load_fixture("array_nested.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect nested array");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["tags"].as_array().expect("tags").len(), 2);
        }

        #[test]
        fn test_file_array_mixed_types_auto_detect() {
            let data = load_fixture("array_mixed_types.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect mixed types array");
            assert_eq!(lines.len(), 1);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["str"], "hello");
            assert_eq!(r0["int"], 42);
        }
    }

    mod file_object_tests {
        use super::*;

        #[test]
        fn test_file_object_single() {
            let data = load_fixture("object_single.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["name"], "Alice");
            assert_eq!(r["age"], 30);
            assert_eq!(r["active"], true);
        }

        #[test]
        fn test_file_object_pretty() {
            let data = load_fixture("object_pretty.json");
            // Pretty-printed object is multi-line, auto-detect sees '{'
            let mut reader = BufReader::new(Cursor::new(&data));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
            // Read entire content as a string and parse it
            let mut content = String::new();
            reader.read_to_string(&mut content).expect("should read");
            let r: serde_json::Value = serde_json::from_str(&content).expect("parse");
            assert_eq!(r["name"], "Alice");
            assert_eq!(r["address"]["city"], "Wonderland");
            assert_eq!(r["tags"].as_array().expect("tags").len(), 2);
        }

        #[test]
        fn test_file_object_nulls() {
            let data = load_fixture("object_nulls.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["name"], "Alice");
            assert!(r["middle_name"].is_null());
            assert!(r["nickname"].is_null());
            assert_eq!(r["age"], 30);
        }

        #[test]
        fn test_file_object_empty() {
            let data = load_fixture("object_empty.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "{}");
        }

        #[test]
        fn test_file_object_auto_detect() {
            let data = load_fixture("object_single.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect object");
            assert_eq!(lines.len(), 1);
        }

        #[test]
        fn test_file_object_nulls_auto_detect() {
            let data = load_fixture("object_nulls.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect nulls object");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert!(r["middle_name"].is_null());
        }

        #[test]
        fn test_file_object_empty_auto_detect() {
            let data = load_fixture("object_empty.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect empty object");
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0], "{}");
        }

        #[test]
        fn test_file_object_airports() {
            let data = load_fixture("object_airports.json");
            // Multi-line object with nested array — auto-detect sees '{'
            let mut reader = BufReader::new(Cursor::new(&data));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
            // Read entire content and parse
            let mut content = String::new();
            reader.read_to_string(&mut content).expect("should read");
            let r: serde_json::Value = serde_json::from_str(&content).expect("parse");
            assert_eq!(r["count"], 5);
            assert_eq!(r["source"], "FAA");
            let airports = r["airports"].as_array().expect("airports array");
            assert_eq!(airports.len(), 5);
            assert_eq!(airports[0]["code"], "ATL");
            assert_eq!(airports[0]["city"], "Atlanta");
            assert_eq!(airports[0]["elevation_ft"], 1026);
        }

        #[test]
        fn test_file_object_airports_json_pointer() {
            use crate::JsonPointerReader;
            let data = load_fixture("object_airports.json");
            // Extract the airports array via json_pointer
            let extracted =
                JsonPointerReader::from_vec(&data, "/airports").expect("extract /airports");
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut BufReader::new(extracted), &mut buf).expect("read");
            // Should be an array of objects
            let adapter =
                ArrayToNdjson::try_new(BufReader::new(Cursor::new(buf))).expect("ArrayToNdjson");
            let lines = read_all_lines(adapter).expect("read lines");
            assert_eq!(lines.len(), 5);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse row 0");
            assert_eq!(r0["code"], "ATL");
            assert_eq!(r0["name"], "Hartsfield-Jackson Atlanta International");
            assert_eq!(r0["elevation_ft"], 1026);
            let r4: serde_json::Value = serde_json::from_str(&lines[4]).expect("parse row 4");
            assert_eq!(r4["code"], "DEN");
            assert_eq!(r4["state"], "CO");
        }
    }

    mod file_pointer_tests {
        use super::*;

        #[test]
        fn test_file_pointer_wrapper_extract_data() {
            let data = load_fixture("pointer_wrapper.json");
            let reader =
                JsonPointerReader::new(Cursor::new(data), "/data").expect("should extract /data");
            let output = read_to_string(reader);
            let parsed: serde_json::Value = serde_json::from_str(&output).expect("parse");
            let arr = parsed.as_array().expect("should be array");
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0]["id"], 1);
            assert_eq!(arr[0]["val"], "alpha");
            assert_eq!(arr[2]["val"], "gamma");
        }

        #[test]
        fn test_file_pointer_wrapper_extract_count() {
            let data = load_fixture("pointer_wrapper.json");
            let reader =
                JsonPointerReader::new(Cursor::new(data), "/count").expect("should extract /count");
            let output = read_to_string(reader);
            assert_eq!(output, "3");
        }

        #[test]
        fn test_file_pointer_wrapper_extract_status() {
            let data = load_fixture("pointer_wrapper.json");
            let reader = JsonPointerReader::new(Cursor::new(data), "/status")
                .expect("should extract /status");
            let output = read_to_string(reader);
            assert_eq!(output, r#""ok""#);
        }

        #[test]
        fn test_file_pointer_wrapper_missing_key() {
            let data = load_fixture("pointer_wrapper.json");
            let err = JsonPointerReader::new(Cursor::new(data), "/missing")
                .expect_err("should fail on missing key");
            assert!(err.to_string().contains("not found"));
        }

        #[test]
        fn test_file_pointer_nested_extract_items() {
            let data = load_fixture("pointer_nested.json");
            let reader = JsonPointerReader::new(Cursor::new(data), "/response/items")
                .expect("should extract /response/items");
            let output = read_to_string(reader);
            let parsed: serde_json::Value = serde_json::from_str(&output).expect("parse");
            let arr = parsed.as_array().expect("should be array");
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[1]["name"], "second");
        }

        #[test]
        fn test_file_pointer_nested_extract_metadata() {
            let data = load_fixture("pointer_nested.json");
            let reader = JsonPointerReader::new(Cursor::new(data), "/response/metadata/request_id")
                .expect("should extract nested");
            let output = read_to_string(reader);
            assert_eq!(output, r#""abc-123""#);
        }

        #[test]
        fn test_file_pointer_deep() {
            let data = load_fixture("pointer_deep.json");
            let reader = JsonPointerReader::new(Cursor::new(data), "/a/b/c/d")
                .expect("should extract deeply nested");
            let output = read_to_string(reader);
            let parsed: serde_json::Value = serde_json::from_str(&output).expect("parse");
            let arr = parsed.as_array().expect("should be array");
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0]["x"], 1);
        }

        #[test]
        fn test_file_pointer_scalar() {
            let data = load_fixture("pointer_scalar.json");
            let reader = JsonPointerReader::new(Cursor::new(data), "/meta/count")
                .expect("should extract scalar");
            let output = read_to_string(reader);
            assert_eq!(output, "42");
        }

        /// Extract then pipe through `ArrayToNdjson`
        #[test]
        fn test_file_pointer_extract_then_array_to_ndjson() {
            let data = load_fixture("pointer_wrapper.json");
            let extracted =
                JsonPointerReader::new(Cursor::new(data), "/data").expect("should extract");
            let adapter = ArrayToNdjson::try_new(extracted).expect("should parse extracted array");
            let lines = read_all_lines(adapter).expect("should read NDJSON");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["id"], 1);
            assert_eq!(r0["val"], "alpha");
        }

        /// Extract nested then pipe through `ArrayToNdjson`
        #[test]
        fn test_file_pointer_nested_extract_then_array() {
            let data = load_fixture("pointer_nested.json");
            let extracted = JsonPointerReader::new(Cursor::new(data), "/response/items")
                .expect("should extract");
            let adapter = ArrayToNdjson::try_new(extracted).expect("should parse extracted array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            let r2: serde_json::Value = serde_json::from_str(&lines[2]).expect("parse");
            assert_eq!(r2["name"], "third");
        }

        /// Auto-detect after pointer extraction: array
        #[test]
        fn test_file_pointer_auto_detect_array() {
            let data = load_fixture("pointer_wrapper.json");
            let extracted =
                JsonPointerReader::new(Cursor::new(data), "/data").expect("should extract");
            let mut reader = BufReader::new(extracted);
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
        }

        /// Auto-detect after pointer extraction: scalar
        #[test]
        fn test_file_pointer_auto_detect_scalar() {
            let data = load_fixture("pointer_scalar.json");
            let extracted =
                JsonPointerReader::new(Cursor::new(data), "/meta/status").expect("should extract");
            let mut reader = BufReader::new(extracted);
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            // "ok" starts with '"'
            assert_eq!(byte, b'"');
        }
    }

    mod file_soda_tests {
        use super::*;
        use crate::{SodaReader, soda_schema_from_meta};
        use arrow::datatypes::DataType;

        #[test]
        fn test_file_soda_schema() {
            let data = load_fixture("soda_response.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");

            // meta_data columns should be filtered out → 3 user columns
            assert_eq!(schema.fields().len(), 3);
            assert_eq!(schema.field(0).name(), "city");
            assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
            assert_eq!(schema.field(1).name(), "population");
            assert_eq!(*schema.field(1).data_type(), DataType::Float64);
            assert_eq!(schema.field(2).name(), "active");
            assert_eq!(*schema.field(2).data_type(), DataType::Boolean);
        }

        #[test]
        fn test_file_soda_reader_data() {
            let data = load_fixture("soda_response.json");
            let soda = SodaReader::new(Cursor::new(data), false).expect("should parse SODA");

            assert_eq!(soda.schema().fields().len(), 3);

            let mut output = String::new();
            let mut reader = soda;
            std::io::Read::read_to_string(&mut reader, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 3);

            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row 0");
            assert_eq!(r0["city"], "Seattle");
            assert_eq!(r0["population"], 750_000);
            assert_eq!(r0["active"], true);

            let r1: serde_json::Value = serde_json::from_str(lines[1]).expect("parse row 1");
            assert_eq!(r1["city"], "Portland");
            assert_eq!(r1["population"], 650_000);
            assert_eq!(r1["active"], false);

            // meta_data columns should NOT appear
            assert!(r0.get(":sid").is_none());
            assert!(r0.get(":id").is_none());
            assert!(r0.get(":created_at").is_none());
        }

        #[test]
        fn test_file_soda_empty_data() {
            let data = load_fixture("soda_empty_data.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse SODA");

            let schema = soda.schema().clone();
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "name");

            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            assert!(
                output.trim().is_empty(),
                "empty data should produce no NDJSON"
            );
        }

        #[test]
        fn test_file_soda_all_nullable() {
            let data = load_fixture("soda_response.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            for field in schema.fields() {
                assert!(
                    field.is_nullable(),
                    "field '{}' should be nullable",
                    field.name()
                );
            }
        }

        #[test]
        fn test_file_soda_auto_detect() {
            let data = load_fixture("soda_response.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect SODA");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["city"], "Seattle");
        }

        #[test]
        fn test_file_soda_empty_data_auto_detect() {
            let data = load_fixture("soda_empty_data.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect empty SODA");
            assert!(lines.is_empty());
        }
    }

    mod file_soda_real_world_tests {
        use super::*;
        use crate::{SodaReader, soda_schema_from_meta};
        use arrow::datatypes::{DataType, TimeUnit};

        #[test]
        fn test_house_price_index_schema() {
            let data = load_fixture("house_price_index_connecticut.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse JSON");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");

            // 10 total columns, 8 meta_data → 2 user-visible columns
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "observation_date");
            assert_eq!(
                *schema.field(0).data_type(),
                DataType::Timestamp(TimeUnit::Second, None)
            ); // calendar_date → Timestamp
            assert_eq!(schema.field(1).name(), "ctsthpi");
            assert_eq!(*schema.field(1).data_type(), DataType::Float64); // number → Float64
        }

        #[test]
        fn test_house_price_index_data() {
            let data = load_fixture("house_price_index_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse SODA");

            assert_eq!(soda.schema().fields().len(), 2);

            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 203);

            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row 0");
            assert_eq!(r0["observation_date"], "1975-01-01T00:00:00");
            assert_eq!(r0["ctsthpi"], "62.9");

            // meta_data columns must not appear
            assert!(r0.get(":sid").is_none());
            assert!(r0.get(":id").is_none());
            assert!(r0.get(":position").is_none());
            assert!(r0.get(":created_at").is_none());
            assert!(r0.get(":meta").is_none());

            let last: serde_json::Value = serde_json::from_str(lines[202]).expect("parse last row");
            assert_eq!(last["observation_date"], "2025-07-01T00:00:00");
            assert_eq!(last["ctsthpi"], "708.94");
        }

        #[test]
        fn test_single_fmly_home_schema() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse JSON");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");

            // 12 total columns, 8 meta_data → 4 user-visible columns
            assert_eq!(schema.fields().len(), 4);
            assert_eq!(schema.field(0).name(), "date");
            assert_eq!(
                *schema.field(0).data_type(),
                DataType::Timestamp(TimeUnit::Second, None)
            ); // calendar_date → Timestamp
            assert_eq!(schema.field(1).name(), "median_sale_price");
            assert_eq!(*schema.field(1).data_type(), DataType::Float64); // number → Float64
            assert_eq!(schema.field(2).name(), "average_sale_price");
            assert_eq!(*schema.field(2).data_type(), DataType::Float64); // number → Float64
            assert_eq!(schema.field(3).name(), "county");
            assert_eq!(*schema.field(3).data_type(), DataType::Utf8); // text → Utf8
        }

        #[test]
        fn test_single_fmly_home_data() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse SODA");

            assert_eq!(soda.schema().fields().len(), 4);

            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2358);

            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row 0");
            assert_eq!(r0["date"], "2001-01-01T00:00:00");
            assert_eq!(r0["county"], "Fairfield");
            // Check that numeric columns appear and are non-null
            assert!(!r0["median_sale_price"].is_null());
            assert!(!r0["average_sale_price"].is_null());

            // meta_data columns must not appear
            assert!(r0.get(":sid").is_none());
            assert!(r0.get(":id").is_none());
        }

        #[test]
        fn test_single_fmly_home_all_nullable() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse");
            let schema = soda_schema_from_meta(&val, false).expect("should extract schema");
            for field in schema.fields() {
                assert!(
                    field.is_nullable(),
                    "field '{}' should be nullable",
                    field.name()
                );
            }
        }

        #[test]
        fn test_house_price_index_auto_detect() {
            let data = load_fixture("house_price_index_connecticut.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect house price SODA");
            assert_eq!(lines.len(), 203);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["observation_date"], "1975-01-01T00:00:00");
            // meta_data columns must not appear
            assert!(r0.get(":sid").is_none());
        }

        #[test]
        fn test_single_fmly_home_auto_detect() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect single fmly SODA");
            assert_eq!(lines.len(), 2358);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["date"], "2001-01-01T00:00:00");
            assert_eq!(r0["county"], "Fairfield");
        }

        // ---- Scenarios matching the 8-dataset YAML config ----

        // Config 1 & 2: no file_format, no json_pointer → Auto → SODA detected
        // (Already covered by test_house_price_index_auto_detect and test_single_fmly_home_auto_detect)

        // Config 3 & 4: file_format: soda (explicit) → SODA reader, no metadata
        #[test]
        fn test_house_price_index_explicit_soda() {
            let data = load_fixture("house_price_index_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 2);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 203);
            // No meta_data columns
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            assert!(r0.get(":sid").is_none());
            assert!(r0.get(":position").is_none());
        }

        #[test]
        fn test_single_fmly_home_explicit_soda() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 4);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2358);
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            assert!(r0.get(":sid").is_none());
        }

        // Config 5 & 6: file_format: soda + soda_metadata: enabled → all columns
        #[test]
        fn test_house_price_index_soda_with_metadata() {
            let data = load_fixture("house_price_index_connecticut.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse");
            let schema = soda_schema_from_meta(&val, true).expect("schema with metadata");

            // 10 total columns (8 meta + 2 user)
            assert_eq!(schema.fields().len(), 10);
            assert_eq!(schema.field(0).name(), ":sid");
            assert_eq!(schema.field(7).name(), ":meta");
            assert_eq!(schema.field(8).name(), "observation_date");
            assert_eq!(schema.field(9).name(), "ctsthpi");

            let mut soda = SodaReader::new(Cursor::new(data), true).expect("should parse");
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 203);

            // Verify meta_data columns ARE present with correct types
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row 0");
            assert!(r0.get(":sid").is_some(), ":sid should be present");
            assert!(r0.get(":id").is_some(), ":id should be present");
            assert!(r0.get(":position").is_some(), ":position should be present");
            // :position is Int64 — should remain a JSON number
            assert!(
                r0[":position"].is_number(),
                ":position should be a number, got {:?}",
                r0[":position"]
            );
            // :created_at is Int64 — should remain a JSON number
            assert!(
                r0[":created_at"].is_number(),
                ":created_at should be a number, got {:?}",
                r0[":created_at"]
            );
            // :meta is Utf8 — an object in raw data, coerced to string
            assert!(
                r0[":meta"].is_string(),
                ":meta should be a string, got {:?}",
                r0[":meta"]
            );
            // User columns still present
            assert_eq!(r0["observation_date"], "1975-01-01T00:00:00");
            assert_eq!(r0["ctsthpi"], "62.9");
        }

        #[test]
        fn test_single_fmly_home_soda_with_metadata() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let val: serde_json::Value = serde_json::from_slice(&data).expect("parse");
            let schema = soda_schema_from_meta(&val, true).expect("schema with metadata");

            // 12 total columns (8 meta + 4 user)
            assert_eq!(schema.fields().len(), 12);

            let mut soda = SodaReader::new(Cursor::new(data), true).expect("should parse");
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2358);

            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse row 0");
            // Meta columns present with correct types
            assert!(r0.get(":sid").is_some());
            assert!(r0[":position"].is_number(), ":position should be a number");
            assert!(
                r0[":created_at"].is_number(),
                ":created_at should be a number"
            );
            // User columns present
            assert_eq!(r0["date"], "2001-01-01T00:00:00");
            assert_eq!(r0["county"], "Fairfield");
        }

        // Config 7 & 8: file_format: json + json_pointer: /data → plain JSON, arrays of arrays
        #[test]
        fn test_house_price_index_json_pointer_data() {
            use crate::JsonPointerReader;
            let data = load_fixture("house_price_index_connecticut.json");
            let extracted = JsonPointerReader::from_vec(&data, "/data").expect("extract /data");
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut std::io::BufReader::new(extracted), &mut buf)
                .expect("read");
            // Should be an array of arrays — use ArrayToNdjson
            let adapter =
                ArrayToNdjson::try_new(BufReader::new(Cursor::new(buf))).expect("ArrayToNdjson");
            let lines = read_all_lines(adapter).expect("read lines");
            assert_eq!(lines.len(), 203);
            // Each inner array → positional object with keys "0", "1", ...
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse row 0");
            // 10 values per row (all columns including meta)
            assert!(r0.get("0").is_some(), "positional key '0' should exist");
            assert!(r0.get("9").is_some(), "positional key '9' should exist");
        }

        #[test]
        fn test_single_fmly_home_json_pointer_data() {
            use crate::JsonPointerReader;
            let data = load_fixture("single_fmly_home_connecticut.json");
            let extracted = JsonPointerReader::from_vec(&data, "/data").expect("extract /data");
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut std::io::BufReader::new(extracted), &mut buf)
                .expect("read");
            let adapter =
                ArrayToNdjson::try_new(BufReader::new(Cursor::new(buf))).expect("ArrayToNdjson");
            let lines = read_all_lines(adapter).expect("read lines");
            assert_eq!(lines.len(), 2358);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse row 0");
            // 12 values per row
            assert!(r0.get("0").is_some());
            assert!(r0.get("11").is_some());
        }

        // Config 5 & 6: json_format: soda (overrides auto default) — equivalent to explicit SODA
        // At the stream level, json_format=soda produces the same output as file_format=soda.
        // These tests verify the SodaReader path produces correct output when invoked explicitly.
        #[test]
        fn test_house_price_index_json_format_soda() {
            let data = load_fixture("house_price_index_connecticut.json");
            // json_format: soda → SodaReader with no metadata (same as explicit soda)
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 2);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 203);
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            assert_eq!(r0["observation_date"], "1975-01-01T00:00:00");
            assert!(r0.get(":sid").is_none());
        }

        #[test]
        fn test_single_fmly_home_json_format_soda() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), false).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 4);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2358);
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            assert_eq!(r0["date"], "2001-01-01T00:00:00");
            assert!(r0.get(":sid").is_none());
        }

        // Config 9 & 10: file_format: json + json_format: soda + soda_metadata: enabled
        // The connector routes this as Format::Json default, then json_format overrides to Soda.
        // At the stream level, this is SodaReader with metadata enabled.
        #[test]
        fn test_house_price_index_json_soda_with_metadata() {
            let data = load_fixture("house_price_index_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), true).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 10);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 203);
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            // Meta columns present
            assert!(r0.get(":sid").is_some());
            assert!(r0.get(":id").is_some());
            assert!(r0[":position"].is_number());
            assert!(r0[":created_at"].is_number());
            // User columns present
            assert_eq!(r0["observation_date"], "1975-01-01T00:00:00");
        }

        #[test]
        fn test_single_fmly_home_json_soda_with_metadata() {
            let data = load_fixture("single_fmly_home_connecticut.json");
            let mut soda = SodaReader::new(Cursor::new(data), true).expect("should parse");
            assert_eq!(soda.schema().fields().len(), 12);
            let mut output = String::new();
            std::io::Read::read_to_string(&mut soda, &mut output).expect("should read");
            let lines: Vec<&str> = output.lines().collect();
            assert_eq!(lines.len(), 2358);
            let r0: serde_json::Value = serde_json::from_str(lines[0]).expect("parse");
            assert!(r0.get(":sid").is_some());
            assert!(r0[":position"].is_number());
            assert_eq!(r0["date"], "2001-01-01T00:00:00");
        }
    }

    mod soda_auto_detect_tests {
        use crate::is_soda_response;

        #[test]
        fn test_auto_detect_soda_house_price_index() {
            let data = std::fs::read(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/test_data/house_price_index_connecticut.json"
            ))
            .expect("read fixture");
            assert!(
                is_soda_response(&data),
                "house_price_index_connecticut.json should be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_soda_single_fmly_home() {
            let data = std::fs::read(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/test_data/single_fmly_home_connecticut.json"
            ))
            .expect("read fixture");
            assert!(
                is_soda_response(&data),
                "single_fmly_home_connecticut.json should be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_soda_synthetic() {
            let data = std::fs::read(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/test_data/soda_response.json"
            ))
            .expect("read fixture");
            assert!(
                is_soda_response(&data),
                "soda_response.json should be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_plain_json_array() {
            let data = br#"[{"name":"Alice"},{"name":"Bob"}]"#;
            assert!(
                !is_soda_response(data),
                "plain JSON array should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_plain_json_object() {
            let data = br#"{"name":"Alice","age":30}"#;
            assert!(
                !is_soda_response(data),
                "plain JSON object should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_ndjson() {
            let data = b"{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n";
            assert!(
                !is_soda_response(data),
                "NDJSON should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_object_with_meta_but_no_data() {
            let data =
                br#"{"meta":{"view":{"columns":[{"fieldName":"x","dataTypeName":"text"}]}}}"#;
            assert!(
                !is_soda_response(data),
                "object with meta but no data should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_object_with_data_but_no_meta() {
            let data = br#"{"data":[[1,2],[3,4]]}"#;
            assert!(
                !is_soda_response(data),
                "object with data but no meta should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_meta_without_child_objects() {
            // meta exists but has no child objects — just scalar/string values
            let data = br#"{"meta":{"version":"1.0"},"data":[[1,2]]}"#;
            assert!(
                !is_soda_response(data),
                "meta with only scalar children should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_soda_without_view_columns() {
            // meta.view exists as a child object but has no columns key
            let data = br#"{"meta":{"view":{"name":"test"}},"data":[[1,2]]}"#;
            assert!(
                is_soda_response(data),
                "meta with a child object + data array should be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_rejects_invalid_json() {
            let data = b"not valid json at all";
            assert!(
                !is_soda_response(data),
                "invalid JSON should not be detected as SODA"
            );
        }

        #[test]
        fn test_auto_detect_soda_with_bom() {
            let mut data = vec![0xEF, 0xBB, 0xBF]; // UTF-8 BOM
            data.extend_from_slice(br#"{"meta":{"view":{"columns":[{"fieldName":"x","dataTypeName":"text"}]}},"data":[[1]]}"#);
            assert!(
                is_soda_response(&data),
                "SODA with BOM prefix should be detected"
            );
        }
    }

    mod file_edge_case_tests {
        use super::*;

        #[test]
        fn test_file_unicode() {
            let data = load_fixture("unicode.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["emoji"], "🚀");
            assert_eq!(r["japanese"], "日本語");
            assert_eq!(r["spanish"], "Ñoño");
            assert_eq!(r["math"], "∑∏∫");
            assert_eq!(r["currency"], "€£¥");
        }

        #[test]
        fn test_file_unicode_auto_detect() {
            let data = load_fixture("unicode.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect unicode");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("🚀"));
        }

        #[test]
        fn test_file_escaped() {
            let data = load_fixture("escaped.json");
            let lines = read_all_lines(BufReader::new(Cursor::new(&data))).expect("should read");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["path"], "C:\\Users\\test\\file.txt");
            assert_eq!(r["quote"], "He said \"hello\"");
            assert_eq!(r["tab"], "col1\tcol2");
            assert_eq!(r["newline"], "line1\nline2");
        }

        #[test]
        fn test_file_escaped_auto_detect() {
            let data = load_fixture("escaped.json");
            let lines = auto_detect_and_read(&data).expect("auto-detect escaped");
            assert_eq!(lines.len(), 1);
        }
    }

    // ----------------------------------------------------------------
    // BOM (Byte Order Mark) handling tests
    // ----------------------------------------------------------------

    mod bom_tests {
        use super::*;

        // -- inline BOM tests --

        #[test]
        fn test_peek_bom_then_array() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(b"[{\"a\":1}]");
            let mut reader = BufReader::new(Cursor::new(input));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
        }

        #[test]
        fn test_peek_bom_then_object() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(br#"{"k":"v"}"#);
            let mut reader = BufReader::new(Cursor::new(input));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'{');
        }

        #[test]
        fn test_peek_bom_then_whitespace_then_array() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(b"  \n\t [{\"a\":1}]");
            let mut reader = BufReader::new(Cursor::new(input));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
        }

        #[test]
        fn test_peek_bom_only_returns_eof() {
            let input = vec![0xEF, 0xBB, 0xBF];
            let mut reader = BufReader::new(Cursor::new(input));
            let err =
                peek_first_non_ws_byte(&mut reader).expect_err("should fail on BOM-only input");
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }

        #[test]
        fn test_peek_no_bom_still_works() {
            let input = b"[{\"a\":1}]";
            let mut reader = BufReader::new(Cursor::new(&input[..]));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[');
        }

        #[test]
        fn test_skip_ws_until_bom_then_bracket() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.push(b'[');
            let mut cursor = Cursor::new(input);
            skip_ws_until(&mut cursor, b'[').expect("should find [");
        }

        #[test]
        fn test_skip_ws_until_bom_then_ws_then_bracket() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(b"  \n  [");
            let mut cursor = Cursor::new(input);
            skip_ws_until(&mut cursor, b'[').expect("should find [");
        }

        #[test]
        fn test_auto_detect_bom_array() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(br#"[{"id":1},{"id":2}]"#);
            let lines = auto_detect_and_read(&input).expect("should auto-detect");
            assert_eq!(lines.len(), 2);
        }

        #[test]
        fn test_auto_detect_bom_object() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(br#"{"name":"Alice","age":30}"#);
            let lines = auto_detect_and_read(&input).expect("should auto-detect");
            assert_eq!(lines.len(), 1);
            assert!(lines[0].contains("Alice"));
        }

        #[test]
        fn test_array_to_ndjson_bom() {
            let mut input = vec![0xEF, 0xBB, 0xBF];
            input.extend_from_slice(br#"[{"x":1},{"x":2}]"#);
            let adapter = ArrayToNdjson::try_new(Cursor::new(input))
                .expect("should parse BOM-prefixed array");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 2);
        }

        // -- file-based BOM tests --

        #[test]
        fn test_file_bom_array() {
            let data = load_fixture("bom_array.json");
            // Verify BOM is present
            assert_eq!(
                &data[..3],
                &[0xEF, 0xBB, 0xBF],
                "fixture should start with BOM"
            );

            let lines = auto_detect_and_read(&data).expect("auto-detect BOM array");
            assert_eq!(lines.len(), 3);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["id"], 1);
            assert_eq!(r0["val"], "alpha");
        }

        #[test]
        fn test_file_bom_object() {
            let data = load_fixture("bom_object.json");
            assert_eq!(
                &data[..3],
                &[0xEF, 0xBB, 0xBF],
                "fixture should start with BOM"
            );

            let lines = auto_detect_and_read(&data).expect("auto-detect BOM object");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["name"], "Alice");
            assert_eq!(r["age"], 30);
        }

        #[test]
        fn test_file_bom_jsonl() {
            let data = load_fixture("bom_jsonl.json");
            assert_eq!(
                &data[..3],
                &[0xEF, 0xBB, 0xBF],
                "fixture should start with BOM"
            );

            let lines = auto_detect_and_read(&data).expect("auto-detect BOM JSONL");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["name"], "Alice");
            let r1: serde_json::Value = serde_json::from_str(&lines[1]).expect("parse");
            assert_eq!(r1["name"], "Bob");
        }

        #[test]
        fn test_file_bom_whitespace_array() {
            let data = load_fixture("bom_whitespace_array.json");
            assert_eq!(
                &data[..3],
                &[0xEF, 0xBB, 0xBF],
                "fixture should start with BOM"
            );

            let lines = auto_detect_and_read(&data).expect("auto-detect BOM+ws array");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["x"], 1);
        }

        #[test]
        fn test_file_bom_array_via_array_to_ndjson() {
            let data = load_fixture("bom_array.json");
            let adapter = ArrayToNdjson::try_new(Cursor::new(data))
                .expect("should parse BOM-prefixed array file");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 3);
            let r2: serde_json::Value = serde_json::from_str(&lines[2]).expect("parse");
            assert_eq!(r2["val"], "gamma");
        }

        #[test]
        fn test_file_bom_array_pointer() {
            let data = load_fixture("bom_array.json");
            // BOM is consumed by peek, then peek sees '[', all is well
            let mut reader = BufReader::new(Cursor::new(&data));
            let byte = peek_first_non_ws_byte(&mut reader).expect("should peek");
            assert_eq!(byte, b'[', "should detect array after BOM");
        }

        // -- native format variants for BOM files --

        #[test]
        fn test_file_bom_object_native() {
            let data = load_fixture("bom_object.json");
            // Strip BOM, then read as single-line object via BufReader
            let stripped = data.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&data);
            let lines =
                read_all_lines(BufReader::new(Cursor::new(stripped))).expect("should read object");
            assert_eq!(lines.len(), 1);
            let r: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r["name"], "Alice");
            assert_eq!(r["age"], 30);
        }

        #[test]
        fn test_file_bom_jsonl_native() {
            let data = load_fixture("bom_jsonl.json");
            // Strip BOM, then read as JSONL (line-delimited) via BufReader
            let stripped = data.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&data);
            let lines =
                read_all_lines(BufReader::new(Cursor::new(stripped))).expect("should read JSONL");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["name"], "Alice");
        }

        #[test]
        fn test_file_bom_whitespace_array_native() {
            let data = load_fixture("bom_whitespace_array.json");
            // Native: read as array via ArrayToNdjson
            let adapter =
                ArrayToNdjson::try_new(Cursor::new(data)).expect("should parse BOM+ws array file");
            let lines = read_all_lines(adapter).expect("should read");
            assert_eq!(lines.len(), 2);
            let r0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse");
            assert_eq!(r0["x"], 1);
        }
    }
}
