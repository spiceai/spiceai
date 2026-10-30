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

//! Bounded decompression of Office Open XML packages (`.docx`, `.xlsx`, `.pptx`).
//!
//! An OOXML file is a zip archive, and deflate expands up to ~1000:1 per entry
//! (more with entries that share compressed data), so a document of a few
//! megabytes can inflate to many gigabytes. The `zip` crate decodes a deflate
//! entry to its end-of-stream marker rather than to the size the archive
//! declares, so declared sizes cannot be trusted: every limit here is enforced
//! on the bytes actually decompressed.

use bytes::Bytes;
use snafu::ResultExt;
use std::collections::VecDeque;
use std::io::{Cursor, Read, Write};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

use crate::{DocumentType, Error, InternalParsingSnafu, Result};

const MIB: u64 = 1024 * 1024;

/// Largest decompressed size of a single package entry. Far above any
/// legitimate slide, document body, worksheet, or embedded image.
pub(crate) const MAX_ENTRY_BYTES: u64 = 64 * MIB;

/// Largest decompressed size of all the entries read from one package.
pub(crate) const MAX_PACKAGE_BYTES: u64 = 256 * MIB;

/// Largest text extracted from one document.
pub(crate) const MAX_TEXT_BYTES: u64 = 64 * MIB;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub entry_bytes: u64,
    pub package_bytes: u64,
}

impl Limits {
    pub(crate) const DEFAULT: Self = Self {
        entry_bytes: MAX_ENTRY_BYTES,
        package_bytes: MAX_PACKAGE_BYTES,
    };
}

fn describe_bytes(bytes: u64) -> String {
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Tracks the decompressed bytes read from one package against [`Limits`].
pub(crate) struct DecompressionBudget {
    format: DocumentType,
    limits: Limits,
    consumed: u64,
}

impl DecompressionBudget {
    pub(crate) fn new(format: DocumentType, limits: Limits) -> Self {
        Self {
            format,
            limits,
            consumed: 0,
        }
    }

    /// Decompresses one entry, failing as soon as it exceeds the per-entry
    /// limit or the package's remaining budget. Never reads more than one byte
    /// past whichever limit is lower.
    pub(crate) fn read_entry(&mut self, name: &str, entry: impl Read) -> Result<Vec<u8>> {
        let format = self.format;
        let remaining = self.limits.package_bytes.saturating_sub(self.consumed);
        let limit = self.limits.entry_bytes.min(remaining);

        let mut data = Vec::new();
        entry
            .take(limit.saturating_add(1))
            .read_to_end(&mut data)
            .boxed()
            .context(InternalParsingSnafu { format })?;
        let read = data.len() as u64;

        if read > self.limits.entry_bytes {
            return Err(Error::DocumentTooLarge {
                format,
                reason: format!(
                    "archive entry '{name}' decompresses to more than {}, the per-entry limit",
                    describe_bytes(self.limits.entry_bytes)
                ),
            });
        }
        if read > remaining {
            return Err(Error::DocumentTooLarge {
                format,
                reason: format!(
                    "the archive decompresses to more than {} in total, the per-document limit",
                    describe_bytes(self.limits.package_bytes)
                ),
            });
        }
        self.consumed += read;
        Ok(data)
    }
}

/// Fails when extracted text of `len` bytes exceeds [`MAX_TEXT_BYTES`].
pub(crate) fn ensure_text_within_limit(len: u64, format: DocumentType) -> Result<()> {
    if len > MAX_TEXT_BYTES {
        return Err(Error::DocumentTooLarge {
            format,
            reason: format!(
                "the extracted text exceeds {}, the per-document limit",
                describe_bytes(MAX_TEXT_BYTES)
            ),
        });
    }
    Ok(())
}

/// Opens a zip archive, rejecting it when a local file header disagrees with
/// its central directory entry, two entries share a name, or entries overlap.
/// Such an archive can show different content to different zip readers ("zip
/// cloaking", GHSA-m665-v7pm-7grj), so it is refused before any entry is read.
pub(crate) fn open_consistent_archive(
    raw: &Bytes,
    format: DocumentType,
) -> Result<ZipArchive<Cursor<Bytes>>> {
    let mut archive = ZipArchive::new(Cursor::new(raw.clone()))
        .boxed()
        .context(InternalParsingSnafu { format })?;
    archive
        .validate_header_consistency()
        .boxed()
        .context(InternalParsingSnafu { format })?;
    Ok(archive)
}

/// Magic number of the OLE compound file format, which Office uses for
/// password-protected OOXML and the legacy binary formats (`.doc`, `.xls`).
const COMPOUND_FILE_MAGIC: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];

/// Decompresses every entry of an OOXML package within `limits` and returns an
/// equivalent package whose entries are stored uncompressed.
///
/// Third-party readers (`docx-rs`, `calamine`) decompress entries without any
/// size limit, so they are only ever handed the rebuilt package: what they
/// read is exactly the bytes checked here, with no second zip parser that
/// could interpret the original archive differently.
pub(crate) fn rebuild_package(raw: &Bytes, format: DocumentType, limits: Limits) -> Result<Bytes> {
    if raw.starts_with(&COMPOUND_FILE_MAGIC) {
        return Err(std::io::Error::other(
            "the file is password-protected or in a legacy binary format; only unencrypted Office Open XML files are supported",
        ))
        .boxed()
        .context(InternalParsingSnafu { format });
    }

    let mut archive = open_consistent_archive(raw, format)?;
    let mut budget = DecompressionBudget::new(format, limits);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .boxed()
            .context(InternalParsingSnafu { format })?;
        if entry.is_dir() {
            continue;
        }
        let name = entry
            .name()
            .boxed()
            .context(InternalParsingSnafu { format })?
            .into_owned();
        let data = budget.read_entry(&name, entry)?;
        writer
            .start_file(name, options)
            .boxed()
            .context(InternalParsingSnafu { format })?;
        writer
            .write_all(&data)
            .boxed()
            .context(InternalParsingSnafu { format })?;
    }

    let rebuilt = writer
        .finish()
        .boxed()
        .context(InternalParsingSnafu { format })?;
    Ok(Bytes::from(rebuilt.into_inner()))
}

/// Bounds the bytes a PDF would decompress to, defending against a
/// content-stream bomb before the bytes reach `PDFium`, which applies each
/// stream's filter chain internally with no size limit.
///
/// A byte-level cap on archive entries (as [`rebuild_package`] applies to
/// OOXML) cannot help here: the compression is inside the PDF's own stream
/// objects, not a surrounding zip. Instead every stream's declared `/Filter`
/// chain is applied through bounded decoders that fail as soon as the running
/// total crosses [`MAX_PACKAGE_BYTES`]. Applying the whole chain is what makes
/// a nested filter such as `[/FlateDecode /FlateDecode]` or
/// `[/ASCII85Decode /FlateDecode]` safe: a single pass would only measure the
/// small intermediate and let `PDFium` expand the rest unbounded. No decoder
/// reads past `MAX_PACKAGE_BYTES`, so scanning an 8 GiB bomb costs the CPU and
/// memory to decode the budget and no more.
///
/// The image codecs (`DCTDecode`, `JPXDecode`, `CCITTFaxDecode`, `JBIG2Decode`)
/// are not expanded here: their output is an image raster bounded by the
/// declared dimensions rather than a text content-stream bomb, so budgeting
/// stops at a terminal one. A general-purpose `LZWDecode`, the `Crypt` filter,
/// an unrecognised filter, or a `/Filter` we cannot resolve (an indirect
/// reference) is not decoded and cannot be bounded, so the document is rejected
/// rather than forwarded to `PDFium` with an unchecked expansion.
///
/// The dictionary in front of each `stream` keyword is tokenised, not scanned
/// for byte patterns. PDF whitespace (including NUL) and `%` comments between
/// tokens cannot hide a key, and a `/Filter` that appears inside a string or a
/// nested dictionary is not the stream's filter. A `stream` that does not
/// follow a dictionary is content, not a stream; a dictionary that does follow
/// one but cannot be parsed fails closed.
pub(crate) fn ensure_pdf_within_budget(raw: &Bytes) -> Result<()> {
    let budget = MAX_PACKAGE_BYTES;
    let mut consumed: u64 = 0;

    for stream in pdf_streams(raw) {
        let remaining = budget.saturating_sub(consumed);
        let decoded = decoded_len_within(&stream, raw, remaining)?;
        consumed = consumed.saturating_add(decoded);
    }
    Ok(())
}

/// A declared PDF stream filter, classified by how its output is bounded.
///
/// `Flate`, `Ascii85`, `AsciiHex`, and `RunLength` are decoded here, so a chain
/// built from them is budgeted end to end. `Image` is a terminal codec whose
/// output is a raster bounded by the image's declared dimensions, not a text
/// content-stream expansion, so budgeting stops at it. `Unbounded` is a filter
/// whose expansion we cannot bound — general-purpose `LZWDecode`, the `Crypt`
/// filter, an unrecognised name, or a `/Filter` we could not resolve (an
/// indirect reference or an unparsable declaration) — and reaching one fails
/// the document closed rather than forwarding an unchecked stream to `PDFium`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PdfFilter {
    Flate,
    Ascii85,
    AsciiHex,
    RunLength,
    Image,
    Unbounded,
}

fn classify_filter(name: &[u8]) -> PdfFilter {
    match name {
        b"FlateDecode" | b"Fl" => PdfFilter::Flate,
        b"ASCII85Decode" | b"A85" => PdfFilter::Ascii85,
        b"ASCIIHexDecode" | b"AHx" => PdfFilter::AsciiHex,
        b"RunLengthDecode" | b"RL" => PdfFilter::RunLength,
        // Terminal image codecs: their output is a raster bounded by the
        // image's declared dimensions, so budgeting stops at them.
        b"DCTDecode" | b"DCT" | b"JPXDecode" | b"CCITTFaxDecode" | b"CCF" | b"JBIG2Decode" => {
            PdfFilter::Image
        }
        // `LZWDecode` is general-purpose compression we do not decode, `Crypt`
        // is encryption, and anything else is unrecognised. None can be bounded
        // here, so fail closed rather than forward an unchecked stream.
        _ => PdfFilter::Unbounded,
    }
}

/// One PDF stream object: the declared filter chain (empty when `/Filter` is
/// absent, i.e. stored uncompressed) and the byte offset of its data.
struct PdfStream {
    filters: Vec<PdfFilter>,
    data_start: usize,
}

/// The decompressed size `PDFium` would produce for one stream, capping every
/// decode at `remaining + 1` bytes so a bomb is detected without expanding it,
/// and failing once any stage crosses the remaining budget. A terminal
/// [`PdfFilter::Image`] stage stops budgeting (its raster is bounded by the
/// image's declared dimensions); a [`PdfFilter::Unbounded`] stage, or an image
/// codec anywhere but last, fails the document closed because its expansion
/// cannot be verified before the bytes reach `PDFium`.
fn decoded_len_within(stream: &PdfStream, raw: &[u8], remaining: u64) -> Result<u64> {
    let cap = remaining.saturating_add(1);
    let mut buf: Vec<u8> = Vec::new();
    let mut size: u64 = 0;
    let last = stream.filters.len().saturating_sub(1);
    for (idx, &filter) in stream.filters.iter().enumerate() {
        match filter {
            // A chain whose expansion we cannot bound (LZW, Crypt, an unknown
            // or unresolvable filter) is rejected rather than forwarded.
            PdfFilter::Unbounded => return Err(unverifiable_pdf_filter()),
            // A terminal image codec is a dimension-bounded raster; stop here.
            // An image codec that is not last means a malformed chain whose
            // continuation we cannot bound, so fail closed.
            PdfFilter::Image => {
                if idx == last {
                    return Ok(size);
                }
                return Err(unverifiable_pdf_filter());
            }
            _ => {}
        }
        let input: &[u8] = if idx == 0 {
            &raw[stream.data_start..]
        } else {
            &buf
        };
        // The final stage's bytes are never fed anywhere, so count them through a
        // sink instead of materializing: a bomb whose last stage is the one that
        // explodes then costs no memory to reject.
        if idx == last {
            size = count_decode(filter, input, cap);
            return ensure_within(size, remaining);
        }
        let decoded = decode_stage(filter, input, cap);
        size = decoded.len() as u64;
        ensure_within(size, remaining)?;
        buf = decoded;
    }
    Ok(size)
}

/// The error for a PDF whose filter chain cannot be bounded before `PDFium`
/// would expand it: an indirect, `LZWDecode`, encrypted, or unrecognised
/// `/Filter`. Such a stream is refused rather than forwarded unchecked.
fn unverifiable_pdf_filter() -> Error {
    Error::DocumentTooLarge {
        format: DocumentType::Pdf,
        reason: "it declares a stream filter whose decompressed size cannot be verified \
                 (an indirect, LZW, encrypted, or unrecognised /Filter)"
            .to_owned(),
    }
}

fn ensure_within(size: u64, remaining: u64) -> Result<u64> {
    if size > remaining {
        return Err(Error::DocumentTooLarge {
            format: DocumentType::Pdf,
            reason: format!(
                "its compressed streams decompress to more than {}, the per-document limit",
                describe_bytes(MAX_PACKAGE_BYTES)
            ),
        });
    }
    Ok(size)
}

/// Counts the output of one filter stage without keeping it, reading no more
/// than `cap` bytes. Used for the final stage, whose bytes feed nothing: Flate
/// (the expanding filter that matters here) streams through a sink, so a bomb
/// whose last stage explodes costs no memory to reject. The ASCII filters only
/// shrink, so materializing them is already bounded by their input.
fn count_decode(filter: PdfFilter, data: &[u8], cap: u64) -> u64 {
    match filter {
        PdfFilter::Flate => inflate_counting(data, cap),
        other => decode_stage(other, data, cap).len() as u64,
    }
}

/// Applies one filter stage, reading no more than `cap` output bytes. The
/// `Image` and `Unbounded` stages are resolved by [`decoded_len_within`] before
/// a stage is decoded, so they never reach here.
fn decode_stage(filter: PdfFilter, data: &[u8], cap: u64) -> Vec<u8> {
    match filter {
        PdfFilter::Flate => inflate_bounded(data, cap),
        PdfFilter::Ascii85 => ascii85_decode_bounded(data, cap),
        PdfFilter::AsciiHex => asciihex_decode_bounded(data, cap),
        PdfFilter::RunLength => runlength_decode_bounded(data, cap),
        PdfFilter::Image | PdfFilter::Unbounded => Vec::new(),
    }
}

/// Every stream object in `raw`, each paired with its declared filter chain.
/// A `stream` byte sequence that is not the keyword after a dictionary — the
/// word inside content, or `endstream` — is not a stream object. After each
/// real stream is recorded, its payload is skipped (via `/Length`, or by
/// finding `endstream`) so dictionary-like bytes inside the content cannot be
/// mistaken for another stream declaration.
fn pdf_streams(raw: &[u8]) -> Vec<PdfStream> {
    const KEYWORD: &[u8] = b"stream";
    let mut streams = Vec::new();
    let mut from = 0;
    // Strings and comments are lexed from the end of the last validated payload
    // so binary stream bytes (e.g. a `0x28` in an image) cannot look like `(`.
    let mut lexer = PdfLexer::new(0);
    while let Some(rel) = find(&raw[from..], KEYWORD) {
        let pos = from + rel;
        from = pos + KEYWORD.len();
        if !is_stream_keyword(raw, pos) || lexer.covers(raw, pos) {
            continue;
        }
        let Some(stream_info) = stream_before(raw, &lexer) else {
            continue;
        };
        // Stream data begins after the end-of-line that follows the keyword.
        let mut data = from;
        if raw.get(data) == Some(&b'\r') {
            data += 1;
        }
        if raw.get(data) == Some(&b'\n') {
            data += 1;
        }
        streams.push(PdfStream {
            filters: stream_info.filters,
            data_start: data,
        });
        // Do not resume the keyword search inside this stream's payload, and
        // do not let that payload participate in later string detection.
        from = stream_payload_end(raw, data, stream_info.length);
        lexer.restart(from);
    }
    streams
}

/// `stream` at `pos` is a keyword when a token boundary precedes it. `>` covers
/// a dictionary closed flush against the keyword (`>>stream`); `/stream` is a
/// name and `endstream` is a different keyword, so neither matches.
fn is_stream_keyword(raw: &[u8], pos: usize) -> bool {
    pos == 0 || is_pdf_whitespace(raw[pos - 1]) || raw[pos - 1] == b'>'
}

/// Index just past a stream's payload: `/Length` bytes from `data_start` when
/// known *and* an `endstream` sits at that boundary; otherwise the first
/// `endstream` after `data_start`.
///
/// Trusting a bare length would let `/Length 999999999` on an empty stream
/// skip to EOF and hide a later Flate bomb. Requiring `endstream` at the
/// claimed boundary still allows a payload that embeds the ASCII bytes
/// `endstream` mid-content, because the real terminator is the one at
/// `data_start + Length`.
fn stream_payload_end(raw: &[u8], data_start: usize, length: Option<u64>) -> usize {
    if let Some(len) = length {
        let by_length = data_start
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .min(raw.len());
        let after = after_endstream(raw, by_length);
        if after > by_length {
            return after;
        }
        // Claimed length has no matching endstream — treat as unverifiable.
    }
    find_endstream(raw, data_start).map_or(raw.len(), |pos| pos + b"endstream".len())
}

/// If `endstream` sits at or just after `at` (with optional whitespace), return
/// the index past that keyword; otherwise `at`.
fn after_endstream(raw: &[u8], at: usize) -> usize {
    let mut i = at;
    i += ws_len(&raw[i..]);
    if raw[i..].starts_with(b"endstream")
        && (i + b"endstream".len() == raw.len()
            || is_pdf_whitespace(raw[i + b"endstream".len()])
            || is_pdf_delimiter(raw[i + b"endstream".len()]))
    {
        return i + b"endstream".len();
    }
    at
}

fn find_endstream(raw: &[u8], from: usize) -> Option<usize> {
    const KEYWORD: &[u8] = b"endstream";
    let mut cursor = from;
    while let Some(rel) = find(&raw[cursor..], KEYWORD) {
        let pos = cursor + rel;
        let boundary_before = pos == 0 || is_pdf_whitespace(raw[pos - 1]);
        let after = pos + KEYWORD.len();
        let boundary_after =
            after == raw.len() || is_pdf_whitespace(raw[after]) || is_pdf_delimiter(raw[after]);
        if boundary_before && boundary_after {
            return Some(pos);
        }
        cursor = pos + 1;
    }
    None
}

struct StreamInfo {
    filters: Vec<PdfFilter>,
    /// Direct `/Length` in bytes, or the uniquely resolved value of an
    /// indirect `/Length N G R`. `None` when the length is absent or when more
    /// than one candidate object definition exists (ambiguous / decoy).
    length: Option<u64>,
}

/// The `/Filter` chain and `/Length` of the dictionary that ends just before
/// the `stream` keyword `lexer` last reached (see [`PdfLexer::covers`]).
///
/// `None` when `stream` is not preceded by a dictionary (content, not a stream
/// object). When a dictionary is there but its chain cannot be determined, the
/// filters are a single [`PdfFilter::Unbounded`] stage so the document fails
/// closed.
fn stream_before(raw: &[u8], lexer: &PdfLexer) -> Option<StreamInfo> {
    let close = lexer.dict_closer(raw)?;
    let parsed = filters_of_dict(raw, close, lexer).unwrap_or(ParsedDict {
        consumed: 0,
        filters: vec![PdfFilter::Unbounded],
        length: None,
        length_ref: None,
    });
    let length = parsed.length.or_else(|| {
        parsed
            .length_ref
            .and_then(|(obj, generation)| resolve_indirect_int(raw, obj, generation))
    });
    Some(StreamInfo {
        filters: parsed.filters,
        length,
    })
}

/// Resolves an indirect object `obj gen R` whose body is an integer, used for
/// `/Length N G R`. Returns a value only when exactly one matching object
/// header exists in `raw` (PDF whitespace between the numbers is allowed, so
/// `6\t0 obj` counts). A Catalog string that embeds a second fake definition
/// makes the reference ambiguous, so discovery falls back to `endstream`
/// instead of trusting a decoy integer.
fn resolve_indirect_int(raw: &[u8], obj: u32, generation: u32) -> Option<u64> {
    let mut values = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        if i > 0 && !is_pdf_whitespace(raw[i - 1]) {
            i += 1;
            continue;
        }
        let Some((header_end, parsed_obj, parsed_gen)) = parse_object_header(&raw[i..]) else {
            i += 1;
            continue;
        };
        i += header_end;
        if parsed_obj != obj || parsed_gen != generation {
            continue;
        }
        let rest = skip_ws(&raw[i..]);
        if let Some(value) = parse_u64(rest) {
            values.push(value);
        }
    }
    (values.len() == 1).then(|| values[0])
}

/// Parses an `obj gen obj` header at the start of `data`, allowing PDF
/// whitespace between the tokens. Returns `(bytes_consumed, obj, generation)`.
fn parse_object_header(data: &[u8]) -> Option<(usize, u32, u32)> {
    let obj = parse_u64(data)?;
    let after_obj = skip_number(data)?;
    let gen_at = after_obj + ws_len(&data[after_obj..]);
    let generation = parse_u64(&data[gen_at..])?;
    let after_gen = gen_at + skip_number(&data[gen_at..])?;
    let kw_at = after_gen + ws_len(&data[after_gen..]);
    if !data[kw_at..].starts_with(b"obj") || !token_ends(data, kw_at + 3) {
        return None;
    }
    let obj = u32::try_from(obj).ok()?;
    let generation = u32::try_from(generation).ok()?;
    Some((kw_at + 3, obj, generation))
}

fn parse_u64(data: &[u8]) -> Option<u64> {
    let mut i = 0;
    // PDF integers may carry an optional leading `+` (ISO 32000-1 §7.3.3).
    if data.first() == Some(&b'+') {
        i = 1;
    }
    let mut value: u64 = 0;
    let mut digits = 0usize;
    while i < data.len() && data[i].is_ascii_digit() {
        value = value
            .checked_mul(10)?
            .checked_add(u64::from(data[i] - b'0'))?;
        digits += 1;
        i += 1;
    }
    (digits > 0).then_some(value)
}

/// A forward scan of the bytes from the end of the last validated stream
/// payload to the `stream` keyword being checked. Each byte is classified once,
/// and what the dictionary lookup needs is kept as the scan goes, so finding
/// every stream stays linear in the document's size however many `stream`
/// words, comments, or `<<` bytes the document holds.
///
/// The scan starts outside any string or comment: at the start of the document
/// or just past a validated payload, so binary stream bytes (e.g. a `0x28` in
/// an image) never open a string. Literal strings and `%` comments are
/// consumed as whole tokens. The first `%` outside a string owns the rest of
/// its line, so `>>`, `(`, or a second `%` inside it is comment text. A `%`
/// inside a string is string text, including one in a string opened on an
/// earlier line, so it cannot hide the dictionary's `>>`. A string that never
/// closes runs to the end of the document.
struct PdfLexer {
    /// Where the scan started.
    start: usize,
    /// Every byte before `cursor` is classified. A string or comment is
    /// consumed whole, so `cursor` can sit past a position it covers.
    cursor: usize,
    /// Index just past the last byte before `cursor` that is neither PDF
    /// whitespace nor comment text: where a dictionary's `>>` has to end for
    /// the `stream` keyword after it to belong to that dictionary.
    significant_end: usize,
    /// Starts of the nearest [`MAX_DICT_CANDIDATES`] `<<` outside strings and
    /// comments, oldest first: the candidate dictionary openers.
    dict_openers: VecDeque<usize>,
}

impl PdfLexer {
    fn new(start: usize) -> Self {
        Self {
            start,
            cursor: start,
            significant_end: start,
            dict_openers: VecDeque::with_capacity(MAX_DICT_CANDIDATES),
        }
    }

    /// Starts a new scan at `start`, the end of a validated stream payload.
    fn restart(&mut self, start: usize) {
        self.start = start;
        self.cursor = start;
        self.significant_end = start;
        self.dict_openers.clear();
    }

    /// Whether `pos` sits inside a literal string or a `%` comment, scanning
    /// forward to it. A `stream` there is text, not a keyword, so a Catalog
    /// string such as `(<< /Length 999999999 >>stream)` cannot invent a
    /// phantom stream. `pos` never decreases between calls on the same scan.
    fn covers(&mut self, raw: &[u8], pos: usize) -> bool {
        while self.cursor < pos {
            let i = self.cursor;
            match raw[i] {
                b'%' => {
                    self.cursor = raw[i..]
                        .iter()
                        .position(|&b| b == b'\n' || b == b'\r')
                        .map_or(raw.len(), |offset| i + offset);
                }
                b'(' => {
                    self.cursor = skip_literal_string(&raw[i..]).map_or(raw.len(), |len| i + len);
                    self.significant_end = self.cursor;
                }
                byte => {
                    if !is_pdf_whitespace(byte) {
                        self.significant_end = i + 1;
                    }
                    if byte == b'<' && raw.get(i + 1) == Some(&b'<') {
                        if self.dict_openers.len() == MAX_DICT_CANDIDATES {
                            self.dict_openers.pop_front();
                        }
                        self.dict_openers.push_back(i);
                    }
                    self.cursor = i + 1;
                }
            }
        }
        self.cursor > pos
    }

    /// Index just past the `>>` that closes the dictionary before the position
    /// last passed to [`Self::covers`], after PDF whitespace and comments.
    /// `None` when no dictionary closer sits there, which is what distinguishes
    /// a real stream from the word `stream`.
    fn dict_closer(&self, raw: &[u8]) -> Option<usize> {
        let close = self.significant_end;
        (close >= 2 && &raw[close - 2..close] == b">>").then_some(close)
    }
}

/// How far back from a dictionary closer its opening `<<` is looked for.
/// Stream dictionaries are a few hundred bytes; past this the chain cannot be
/// attributed and the stream fails closed.
const DICT_LOOKBACK_BYTES: usize = 1024 * 1024;

/// Cap on opening-`<<` candidates tried per stream, nearest first. The match is
/// the candidate whose token parse ends exactly on the closer; the rest open
/// dictionaries nested inside it. Stopping here bounds the work per stream, and
/// failing closed when none match is safe.
const MAX_DICT_CANDIDATES: usize = 64;

/// Deepest nesting of arrays/dictionaries accepted while skipping a value.
/// Past this the parse fails closed rather than overflowing the stack on an
/// attacker-controlled nest.
const MAX_VALUE_DEPTH: usize = 32;

/// The top-level `/Filter` chain and `/Length` of the dictionary that closes
/// at `close`, trying `lexer`'s `<<` openers nearest first. A `<<` inside a
/// `%` comment or a literal string is not an opener, so it was never recorded.
/// `None` when no candidate opening parses as that dictionary.
fn filters_of_dict(raw: &[u8], close: usize, lexer: &PdfLexer) -> Option<ParsedDict> {
    let window_start = close.saturating_sub(DICT_LOOKBACK_BYTES);
    lexer
        .dict_openers
        .iter()
        .rev()
        .take_while(|&&start| start >= window_start)
        .find_map(|&start| {
            parse_dictionary(&raw[start..close], 0)
                .filter(|parsed| parsed.consumed == close - start)
        })
}

struct ParsedDict {
    consumed: usize,
    /// Empty when the dictionary declares no `/Filter`.
    filters: Vec<PdfFilter>,
    /// Direct integer `/Length`, when declared as a number.
    length: Option<u64>,
    /// Indirect `/Length N G R`, when declared as a reference.
    length_ref: Option<(u32, u32)>,
}

/// Parses one `<< … >>` dictionary starting at `data[0]`, recording its
/// top-level `/Filter` and `/Length`. Nested dictionaries, strings, and arrays
/// are skipped as values, so a `/Filter` inside one of them cannot shadow the
/// real key. `depth` bounds how deep nested values may go.
fn parse_dictionary(data: &[u8], depth: usize) -> Option<ParsedDict> {
    if depth > MAX_VALUE_DEPTH || data.len() < 4 || &data[..2] != b"<<" {
        return None;
    }
    let mut i = 2;
    let mut filters: Option<Vec<PdfFilter>> = None;
    let mut length: Option<u64> = None;
    let mut length_ref: Option<(u32, u32)> = None;
    loop {
        i += ws_len(&data[i..]);
        if data.get(i..i + 2) == Some(b">>") {
            return Some(ParsedDict {
                consumed: i + 2,
                filters: filters.unwrap_or_default(),
                length,
                length_ref,
            });
        }
        if data.get(i) != Some(&b'/') {
            return None;
        }
        let (name, name_len) = read_name(&data[i + 1..]);
        i += 1 + name_len;
        i += ws_len(&data[i..]);
        match name.as_slice() {
            b"Filter" => {
                let (value, value_len) = parse_filter_value(&data[i..], depth)?;
                i += value_len;
                // A duplicate key is undefined in PDF. Fail closed rather than
                // guess which value a reader applies.
                filters = Some(if filters.is_some() {
                    vec![PdfFilter::Unbounded]
                } else {
                    value
                });
            }
            b"Length" => {
                let (value, value_len) = parse_length_value(&data[i..])?;
                i += value_len;
                match value {
                    LengthValue::Direct(n) => {
                        length = Some(n);
                        length_ref = None;
                    }
                    LengthValue::Indirect { obj, generation } => {
                        length = None;
                        length_ref = Some((obj, generation));
                    }
                }
            }
            // A `/Predictor` (or `/P`) in DecodeParms inserts PNG-style row
            // tags between Flate stages that we do not reverse, so a nested
            // Flate chain can under-count. Fail closed when a predictor is
            // declared or cannot be ruled out; other DecodeParms values are
            // skipped as usual.
            b"DecodeParms" | b"DP" => {
                let value_start = i;
                let value_len = skip_value(&data[i..], depth + 1)?;
                if decode_parms_may_predict(&data[value_start..value_start + value_len]) {
                    filters = Some(vec![PdfFilter::Unbounded]);
                }
                i += value_len;
            }
            _ => {
                i += skip_value(&data[i..], depth + 1)?;
            }
        }
    }
}

/// True unless a `/DecodeParms` value is provably predictor-free: a direct
/// dictionary, `null`, or an array of those, with no `/Predictor` or `/P`.
/// Those keys insert PNG-style row tags that this scanner does not reverse.
/// An indirect reference (`5 0 R`) is not resolved, so it fails closed, as
/// does anything that does not parse.
fn decode_parms_may_predict(data: &[u8]) -> bool {
    if data.first() != Some(&b'[') {
        return decode_parms_entry_may_predict(data);
    }
    let mut i = 1;
    loop {
        i += ws_len(&data[i..]);
        match data.get(i) {
            Some(b']') => return false,
            None => return true,
            Some(_) => {
                if decode_parms_entry_may_predict(&data[i..]) {
                    return true;
                }
                match skip_value(&data[i..], 1) {
                    Some(len) => i += len,
                    None => return true,
                }
            }
        }
    }
}

fn decode_parms_entry_may_predict(data: &[u8]) -> bool {
    if data.starts_with(b"null") && token_ends(data, 4) {
        return false;
    }
    if !data.starts_with(b"<<") {
        return true;
    }
    decode_parms_has_predictor_at(data, 0)
}

/// True when `data` holds a `/Predictor` or `/P` key, searching nested
/// dictionaries and arrays. Parse failures and excessive nesting count as a
/// predictor so the caller fails closed.
fn decode_parms_has_predictor_at(data: &[u8], depth: usize) -> bool {
    if depth > MAX_VALUE_DEPTH {
        return true;
    }
    match data.first() {
        Some(b'<') if data.get(1) == Some(&b'<') => {
            let mut i = 2;
            loop {
                i += ws_len(&data[i..]);
                if data.get(i..i + 2) == Some(b">>") {
                    return false;
                }
                if data.get(i) != Some(&b'/') {
                    return true;
                }
                let (name, name_len) = read_name(&data[i + 1..]);
                i += 1 + name_len;
                i += ws_len(&data[i..]);
                if name.as_slice() == b"Predictor" || name.as_slice() == b"P" {
                    return true;
                }
                // Nested DecodeParms arrays/dicts can still hold a predictor.
                if decode_parms_has_predictor_at(&data[i..], depth + 1) {
                    return true;
                }
                match skip_value(&data[i..], depth + 1) {
                    Some(len) => i += len,
                    None => return true,
                }
            }
        }
        Some(b'[') => {
            let mut i = 1;
            loop {
                i += ws_len(&data[i..]);
                match data.get(i) {
                    Some(b']') => return false,
                    None => return true,
                    Some(_) => {
                        if decode_parms_has_predictor_at(&data[i..], depth + 1) {
                            return true;
                        }
                        match skip_value(&data[i..], depth + 1) {
                            Some(len) => i += len,
                            None => return true,
                        }
                    }
                }
            }
        }
        _ => false,
    }
}

enum LengthValue {
    Direct(u64),
    Indirect { obj: u32, generation: u32 },
}

/// A `/Length` value: a direct integer, or an `N G R` indirect reference.
fn parse_length_value(data: &[u8]) -> Option<(LengthValue, usize)> {
    let n_len = skip_number(data)?;
    let n = parse_u64(data)?;
    let gen_at = n_len + ws_len(&data[n_len..]);
    if let Some(gen_len) = skip_number(&data[gen_at..]) {
        let r_at = gen_at + gen_len + ws_len(&data[gen_at + gen_len..]);
        if data.get(r_at) == Some(&b'R') && token_ends(data, r_at + 1) {
            let generation = parse_u64(&data[gen_at..])?;
            let obj = u32::try_from(n).ok()?;
            let generation = u32::try_from(generation).ok()?;
            return Some((LengthValue::Indirect { obj, generation }, r_at + 1));
        }
    }
    Some((LengthValue::Direct(n), n_len))
}

/// The `/Filter` value at `data[0]` (already past whitespace): a name, an array
/// of names, or something whose chain cannot be resolved.
fn parse_filter_value(data: &[u8], depth: usize) -> Option<(Vec<PdfFilter>, usize)> {
    match data.first() {
        Some(b'/') => {
            let (name, name_len) = read_name(&data[1..]);
            Some((vec![classify_filter(&name)], 1 + name_len))
        }
        Some(b'[') => parse_filter_array(data, depth),
        Some(_) => {
            let consumed = skip_value(data, depth + 1)?;
            Some((vec![PdfFilter::Unbounded], consumed))
        }
        None => None,
    }
}

fn parse_filter_array(data: &[u8], depth: usize) -> Option<(Vec<PdfFilter>, usize)> {
    let mut i = 1;
    let mut filters = Vec::new();
    let mut unresolved = false;
    loop {
        i += ws_len(&data[i..]);
        match data.get(i) {
            Some(b']') => {
                let filters = if unresolved {
                    vec![PdfFilter::Unbounded]
                } else {
                    filters
                };
                return Some((filters, i + 1));
            }
            Some(b'/') => {
                let (name, name_len) = read_name(&data[i + 1..]);
                filters.push(classify_filter(&name));
                i += 1 + name_len;
            }
            Some(_) => {
                unresolved = true;
                i += skip_value(&data[i..], depth + 1)?;
            }
            None => return None,
        }
    }
}

/// Bytes consumed by one PDF value: a name, string, hex string, nested
/// dictionary, array, keyword, number, or indirect reference. `depth` counts
/// nested arrays and dictionaries; past [`MAX_VALUE_DEPTH`] the value is
/// rejected rather than recursing.
fn skip_value(data: &[u8], depth: usize) -> Option<usize> {
    if depth > MAX_VALUE_DEPTH {
        return None;
    }
    match data.first() {
        Some(b'/') => {
            let (_, name_len) = read_name(&data[1..]);
            Some(1 + name_len)
        }
        Some(b'(') => skip_literal_string(data),
        Some(b'<') => {
            if data.get(1) == Some(&b'<') {
                parse_dictionary(data, depth + 1).map(|parsed| parsed.consumed)
            } else {
                skip_hex_string(data)
            }
        }
        Some(b'[') => skip_array(data, depth),
        Some(b't') => skip_keyword(data, b"true"),
        Some(b'f') => skip_keyword(data, b"false"),
        Some(b'n') => skip_keyword(data, b"null"),
        Some(b'+' | b'-' | b'.' | b'0'..=b'9') => skip_number_or_reference(data),
        _ => None,
    }
}

fn skip_array(data: &[u8], depth: usize) -> Option<usize> {
    if depth > MAX_VALUE_DEPTH {
        return None;
    }
    let mut i = 1;
    loop {
        i += ws_len(&data[i..]);
        match data.get(i) {
            Some(b']') => return Some(i + 1),
            Some(_) => i += skip_value(&data[i..], depth + 1)?,
            None => return None,
        }
    }
}

fn skip_keyword(data: &[u8], keyword: &[u8]) -> Option<usize> {
    (data.starts_with(keyword) && token_ends(data, keyword.len())).then_some(keyword.len())
}

/// A number, or the `int int R` indirect reference that PDF writers use for
/// `/Length`. Consuming the whole reference is what lets the key after it parse.
fn skip_number_or_reference(data: &[u8]) -> Option<usize> {
    let i = skip_number(data)?;
    let gen_at = i + ws_len(&data[i..]);
    if let Some(gen_len) = skip_number(&data[gen_at..]) {
        let r_at = gen_at + gen_len + ws_len(&data[gen_at + gen_len..]);
        if data.get(r_at) == Some(&b'R') && token_ends(data, r_at + 1) {
            return Some(r_at + 1);
        }
    }
    Some(i)
}

fn skip_number(data: &[u8]) -> Option<usize> {
    let mut i = 0;
    if matches!(data.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let mut digits = 0usize;
    let mut dot = false;
    while i < data.len() {
        match data[i] {
            b'0'..=b'9' => {
                digits += 1;
                i += 1;
            }
            b'.' if !dot => {
                dot = true;
                i += 1;
            }
            _ => break,
        }
    }
    (digits > 0).then_some(i)
}

fn skip_literal_string(data: &[u8]) -> Option<usize> {
    let mut depth = 1i32;
    let mut i = 1;
    while i < data.len() && depth > 0 {
        match data[i] {
            b'\\' => {
                i += 1;
                if data.get(i).is_some_and(|b| is_octal(*b)) {
                    i += 1;
                    let mut extra = 1;
                    while extra < 3 && i < data.len() && is_octal(data[i]) {
                        i += 1;
                        extra += 1;
                    }
                } else if data.get(i) == Some(&b'\r') {
                    i += 1;
                    if data.get(i) == Some(&b'\n') {
                        i += 1;
                    }
                } else if i < data.len() {
                    i += 1;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    (depth == 0).then_some(i)
}

fn skip_hex_string(data: &[u8]) -> Option<usize> {
    let mut i = 1;
    while i < data.len() {
        match data[i] {
            b'>' => return Some(i + 1),
            b if b.is_ascii_hexdigit() || is_pdf_whitespace(b) => i += 1,
            _ => return None,
        }
    }
    None
}

fn token_ends(data: &[u8], at: usize) -> bool {
    data.get(at)
        .is_none_or(|&b| is_pdf_whitespace(b) || is_pdf_delimiter(b))
}

/// PDF whitespace is NUL, tab, LF, form feed, CR, and space (ISO 32000-1
/// §7.2.2). Rust's `is_ascii_whitespace` leaves NUL out, which would split a
/// `/Filter` key from its value.
fn is_octal(b: u8) -> bool {
    matches!(b, b'0'..=b'7')
}

fn is_pdf_whitespace(b: u8) -> bool {
    matches!(b, 0x00 | 0x09 | 0x0A | 0x0C | 0x0D | 0x20)
}

fn is_pdf_delimiter(b: u8) -> bool {
    matches!(
        b,
        b'(' | b')' | b'<' | b'>' | b'[' | b']' | b'{' | b'}' | b'/' | b'%'
    )
}

/// Bytes of leading PDF whitespace and `%` comments.
fn ws_len(data: &[u8]) -> usize {
    let mut i = 0;
    while i < data.len() {
        if is_pdf_whitespace(data[i]) {
            i += 1;
        } else if data[i] == b'%' {
            i += 1;
            while i < data.len() && data[i] != b'\n' && data[i] != b'\r' {
                i += 1;
            }
        } else {
            break;
        }
    }
    i
}

fn skip_ws(data: &[u8]) -> &[u8] {
    &data[ws_len(data)..]
}

/// Test helper view of [`stream_before`]: just the filter chain.
#[cfg(test)]
fn filters_before(raw: &[u8], keyword_pos: usize) -> Option<Vec<PdfFilter>> {
    let mut lexer = PdfLexer::new(0);
    if lexer.covers(raw, keyword_pos) {
        return None;
    }
    stream_before(raw, &lexer).map(|info| info.filters)
}

/// A PDF name token (without its leading `/`), with `#XX` hex escapes decoded,
/// and the number of raw bytes consumed. Decoding escapes is what makes an
/// obfuscated `/Fl#61teDecode` classify as `FlateDecode`.
fn read_name(data: &[u8]) -> (Vec<u8>, usize) {
    let mut name = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        if is_pdf_whitespace(b) || is_pdf_delimiter(b) {
            break;
        }
        if b == b'#'
            && i + 2 < data.len()
            && let (Some(hi), Some(lo)) = (hex_value(data[i + 1]), hex_value(data[i + 2]))
        {
            name.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        name.push(b);
        i += 1;
    }
    (name, i)
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Inflates `data` as a zlib stream (PDF `/FlateDecode`), falling back to raw
/// deflate as `PDFium` does, returning the bytes produced but reading no more
/// than `limit` of them.
fn inflate_bounded(data: &[u8], limit: u64) -> Vec<u8> {
    use flate2::read::{DeflateDecoder, ZlibDecoder};
    use std::io::Read;

    let mut out = Vec::new();
    if ZlibDecoder::new(data)
        .take(limit)
        .read_to_end(&mut out)
        .is_ok()
        && !out.is_empty()
    {
        return out;
    }
    // A non-conformant producer may omit the zlib header; PDFium accepts the
    // raw deflate, so the budget must see it too.
    out.clear();
    let _ = DeflateDecoder::new(data).take(limit).read_to_end(&mut out);
    out
}

/// Inflates `data` as Flate (zlib, then raw deflate) and returns the number of
/// bytes produced, reading no more than `limit` of them and discarding the
/// output. Holds no more than one decoder block, so counting an 8 GiB bomb
/// costs the CPU to inflate `limit` bytes and essentially no memory.
fn inflate_counting(data: &[u8], limit: u64) -> u64 {
    use flate2::read::{DeflateDecoder, ZlibDecoder};
    use std::io::{Read, copy, sink};

    let zlib = copy(&mut ZlibDecoder::new(data).take(limit), &mut sink()).unwrap_or(0);
    if zlib > 0 {
        return zlib;
    }
    copy(&mut DeflateDecoder::new(data).take(limit), &mut sink()).unwrap_or(0)
}

/// Decodes a PDF `/ASCII85Decode` stream up to `limit` output bytes. The output
/// is always smaller than the input, so this stage cannot be a bomb on its own;
/// it exists to feed the filter that follows it in a chain.
fn ascii85_decode_bounded(data: &[u8], limit: u64) -> Vec<u8> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut out = Vec::new();
    let mut group = [0u8; 5];
    let mut count = 0usize;
    for &b in data {
        if out.len() as u64 >= limit as u64 {
            break;
        }
        match b {
            b'~' => break,
            b'z' if count == 0 => out.extend_from_slice(&[0, 0, 0, 0]),
            b'!'..=b'u' => {
                group[count] = b - b'!';
                count += 1;
                if count == 5 {
                    let value = group.iter().fold(0u32, |acc, &d| {
                        acc.wrapping_mul(85).wrapping_add(u32::from(d))
                    });
                    out.extend_from_slice(&value.to_be_bytes());
                    count = 0;
                }
            }
            _ => {} // whitespace and stray bytes are ignored, as in the spec
        }
    }
    if count > 1 {
        for slot in group.iter_mut().skip(count) {
            *slot = 84;
        }
        let value = group.iter().fold(0u32, |acc, &d| {
            acc.wrapping_mul(85).wrapping_add(u32::from(d))
        });
        out.extend_from_slice(&value.to_be_bytes()[..count - 1]);
    }
    out.truncate(limit);
    out
}

/// Decodes a PDF `/ASCIIHexDecode` stream up to `limit` output bytes. Like
/// ASCII85 its output is smaller than its input; it only feeds a later stage.
fn asciihex_decode_bounded(data: &[u8], limit: u64) -> Vec<u8> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut out = Vec::new();
    let mut hi: Option<u8> = None;
    for &b in data {
        if out.len() >= limit {
            break;
        }
        let nibble = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            b'>' => break,
            _ => continue,
        };
        match hi.take() {
            None => hi = Some(nibble),
            Some(h) => out.push((h << 4) | nibble),
        }
    }
    if let Some(h) = hi {
        out.push(h << 4);
    }
    out.truncate(limit);
    out
}

/// Decodes a PDF `/RunLengthDecode` stream up to `limit` output bytes.
fn runlength_decode_bounded(data: &[u8], limit: u64) -> Vec<u8> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() && out.len() < limit {
        let length = data[i];
        i += 1;
        match length {
            128 => break, // end-of-data marker
            0..=127 => {
                let run = usize::from(length) + 1;
                let end = (i + run).min(data.len());
                out.extend_from_slice(&data[i..end]);
                i = end;
            }
            129..=255 => {
                let run = 257 - usize::from(length);
                if let Some(&byte) = data.get(i) {
                    out.resize(out.len() + run, byte);
                    i += 1;
                } else {
                    break;
                }
            }
        }
    }
    out.truncate(limit);
    out
}

#[cfg(test)]
pub(crate) mod test_support {
    use bytes::Bytes;
    use std::io::Write;
    use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

    /// The content of one entry of a test package.
    pub(crate) enum Part<'a> {
        Bytes(&'a [u8]),
        /// `prefix`, then `byte` repeated `len` times, then `suffix`: deflates
        /// to a tiny entry however large `len` is.
        Repeated {
            prefix: &'a [u8],
            byte: u8,
            len: usize,
            suffix: &'a [u8],
        },
    }

    /// Builds a deflate-compressed zip from `parts`, streaming repeated content
    /// in 1 MiB chunks so that building a bomb never holds it in memory.
    pub(crate) fn deflated_package(parts: &[(&str, Part<'_>)]) -> Bytes {
        let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(1));
        for (name, part) in parts {
            zip.start_file(*name, options).expect("start zip entry");
            match part {
                Part::Bytes(bytes) => zip.write_all(bytes).expect("write zip entry"),
                Part::Repeated {
                    prefix,
                    byte,
                    len,
                    suffix,
                } => {
                    zip.write_all(prefix).expect("write zip entry prefix");
                    let chunk = vec![*byte; 1024 * 1024];
                    let mut left = *len;
                    while left > 0 {
                        let n = left.min(chunk.len());
                        zip.write_all(&chunk[..n]).expect("write zip entry chunk");
                        left -= n;
                    }
                    zip.write_all(suffix).expect("write zip entry suffix");
                }
            }
        }
        Bytes::from(zip.finish().expect("finish zip").into_inner())
    }

    /// A structurally complete single-page PDF whose page `/Contents` stream
    /// carries `filter_decl` and `data`. Catalog, Pages, Page, xref, and trailer
    /// are all present, so `PDFium` would load and expand the stream if the
    /// preflight did not reject it first — the shape Copilot asked for as a
    /// loadable chained-filter regression fixture.
    pub(crate) fn loadable_pdf_with_content_stream(filter_decl: &[u8], data: &[u8]) -> Bytes {
        let mut content = Vec::new();
        content.extend_from_slice(b"<< /Length ");
        content.extend_from_slice(data.len().to_string().as_bytes());
        content.extend_from_slice(b" /Filter ");
        content.extend_from_slice(filter_decl);
        content.extend_from_slice(b" >>\nstream\n");
        content.extend_from_slice(data);
        content.extend_from_slice(b"\nendstream");

        let objs: [&[u8]; 4] = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << >> >>",
            content.as_slice(),
        ];

        let mut out = b"%PDF-1.7\n".to_vec();
        let mut offsets = Vec::with_capacity(objs.len());
        for (i, body) in objs.iter().enumerate() {
            offsets.push(out.len());
            out.extend_from_slice(format!("{} 0 obj\n", i + 1).as_bytes());
            out.extend_from_slice(body);
            out.extend_from_slice(b"\nendobj\n");
        }
        let xref_pos = out.len();
        let n = objs.len() + 1;
        out.extend_from_slice(format!("xref\n0 {n}\n").as_bytes());
        out.extend_from_slice(b"0000000000 65535 f \n");
        for off in offsets {
            out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
        }
        out.extend_from_slice(
            format!("trailer\n<< /Size {n} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF")
                .as_bytes(),
        );
        Bytes::from(out)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{Part, deflated_package, loadable_pdf_with_content_stream};
    use super::*;

    const LIMITS: Limits = Limits {
        entry_bytes: 1024,
        package_bytes: 4096,
    };

    fn repeated(len: usize) -> Part<'static> {
        Part::Repeated {
            prefix: b"",
            byte: b'a',
            len,
            suffix: b"",
        }
    }

    fn too_large_reason(result: Result<Bytes>) -> String {
        match result {
            Err(Error::DocumentTooLarge {
                format: DocumentType::Docx,
                reason,
            }) => reason,
            Err(other) => panic!("expected a DocumentTooLarge error, got: {other}"),
            Ok(_) => panic!("expected a DocumentTooLarge error, but the package was accepted"),
        }
    }

    #[test]
    fn rebuilt_package_holds_the_same_entries_uncompressed() {
        let raw = deflated_package(&[
            ("a.xml", Part::Bytes(b"<a>first</a>")),
            ("dir/b.xml", repeated(1024)),
        ]);
        let rebuilt =
            rebuild_package(&raw, DocumentType::Docx, LIMITS).expect("package within limits");

        let mut archive = ZipArchive::new(Cursor::new(rebuilt)).expect("rebuilt package opens");
        let mut entries = Vec::new();
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index).expect("entry");
            assert_eq!(entry.compression(), CompressionMethod::Stored);
            let name = entry.name().expect("entry name").into_owned();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).expect("read entry");
            entries.push((name, data));
        }
        assert_eq!(
            entries,
            vec![
                ("a.xml".to_owned(), b"<a>first</a>".to_vec()),
                ("dir/b.xml".to_owned(), vec![b'a'; 1024]),
            ]
        );
    }

    #[test]
    fn rejects_an_entry_one_byte_over_the_entry_limit() {
        let raw = deflated_package(&[("big.xml", repeated(1025))]);
        assert_eq!(
            too_large_reason(rebuild_package(&raw, DocumentType::Docx, LIMITS)),
            "archive entry 'big.xml' decompresses to more than 1024 bytes, the per-entry limit"
        );
    }

    /// Entries each within the per-entry limit, together over the per-document
    /// limit — the shape of a bomb built from many entries, or from entries
    /// that share one compressed stream.
    #[test]
    fn rejects_entries_that_together_exceed_the_package_limit() {
        let raw = deflated_package(&[
            ("1.xml", repeated(1024)),
            ("2.xml", repeated(1024)),
            ("3.xml", repeated(1024)),
            ("4.xml", repeated(1024)),
        ]);
        rebuild_package(&raw, DocumentType::Docx, LIMITS)
            .expect("exactly at the package limit is accepted");

        let raw = deflated_package(&[
            ("1.xml", repeated(1024)),
            ("2.xml", repeated(1024)),
            ("3.xml", repeated(1024)),
            ("4.xml", repeated(1024)),
            ("5.xml", repeated(1)),
        ]);
        assert_eq!(
            too_large_reason(rebuild_package(&raw, DocumentType::Docx, LIMITS)),
            "the archive decompresses to more than 4096 bytes in total, the per-document limit"
        );
    }

    #[test]
    fn default_limits_are_described_in_mib() {
        let err = ensure_text_within_limit(MAX_TEXT_BYTES + 1, DocumentType::Xlsx)
            .expect_err("text over the limit");
        assert_eq!(
            err.to_string(),
            "Failed to parse XLSX document: the extracted text exceeds 64 MiB, the per-document \
             limit. Documents this large are not supported, and the file may be maliciously \
             crafted (for example, a decompression bomb). \
             See: https://spiceai.org/docs/features/document-processing"
        );
        ensure_text_within_limit(MAX_TEXT_BYTES, DocumentType::Xlsx)
            .expect("text exactly at the limit is accepted");
    }

    fn deflate(bytes: &[u8]) -> Vec<u8> {
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(bytes).expect("deflate");
        encoder.finish().expect("finish deflate")
    }

    /// Deflates `n` space bytes without ever holding them, so a bomb's inner
    /// payload can be built however large `n` is.
    fn deflate_spaces(n: usize) -> Vec<u8> {
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        let chunk = vec![b' '; 1024 * 1024];
        let mut left = n;
        while left > 0 {
            let k = left.min(chunk.len());
            encoder.write_all(&chunk[..k]).expect("deflate chunk");
            left -= k;
        }
        encoder.finish().expect("finish deflate")
    }

    fn ascii85_encode(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in data.chunks(4) {
            let mut buf = [0u8; 4];
            buf[..chunk.len()].copy_from_slice(chunk);
            let mut value = u32::from_be_bytes(buf);
            let mut group = [0u8; 5];
            for slot in group.iter_mut().rev() {
                *slot = (value % 85) as u8 + b'!';
                value /= 85;
            }
            out.extend_from_slice(&group[..=chunk.len()]);
        }
        out.extend_from_slice(b"~>");
        out
    }

    /// Builds a one-stream PDF whose dictionary carries `dict_entries` verbatim
    /// (between the `/Length` entry and the `>>`), so a test can supply an
    /// escaped key, a comment, or an indirect `/Filter` reference.
    fn pdf_with_dict(dict_entries: &str, data: &[u8]) -> Bytes {
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n1 0 obj\n<< /Length ");
        pdf.extend_from_slice(data.len().to_string().as_bytes());
        pdf.push(b' ');
        pdf.extend_from_slice(dict_entries.as_bytes());
        pdf.extend_from_slice(b" >>\nstream\n");
        pdf.extend_from_slice(data);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        Bytes::from(pdf)
    }

    fn pdf_too_large_reason(raw: &Bytes) -> String {
        match ensure_pdf_within_budget(raw) {
            Err(Error::DocumentTooLarge {
                format: DocumentType::Pdf,
                reason,
            }) => reason,
            Err(other) => panic!("expected a DocumentTooLarge error, got: {other}"),
            Ok(()) => panic!("expected a DocumentTooLarge error, but the PDF was accepted"),
        }
    }

    #[test]
    fn ascii85_decodes_a_known_vector() {
        assert_eq!(ascii85_decode_bounded(b"9jqo^~>", 64), b"Man ");
        assert_eq!(ascii85_decode_bounded(b"z~>", 64), vec![0, 0, 0, 0]);
    }

    #[test]
    fn asciihex_decodes_bytes_and_a_trailing_nibble() {
        assert_eq!(asciihex_decode_bounded(b"48656C6C6F>", 64), b"Hello");
        assert_eq!(asciihex_decode_bounded(b"4>", 64), vec![0x40]);
    }

    #[test]
    fn runlength_decodes_literal_and_replicated_runs() {
        assert_eq!(
            runlength_decode_bounded(&[2, b'a', b'b', b'c', 254, b'x', 128], 64),
            b"abcxxx"
        );
    }

    #[test]
    fn filters_before_reads_a_name_and_an_array() {
        let single = b"<< /Length 3 /Filter /FlateDecode >>stream";
        assert_eq!(
            filters_before(single, single.len() - b"stream".len()),
            Some(vec![PdfFilter::Flate])
        );

        let chain = b"<< /Filter [/ASCII85Decode /FlateDecode] /DecodeParms << /x 1 >> >>stream";
        assert_eq!(
            filters_before(chain, chain.len() - b"stream".len()),
            Some(vec![PdfFilter::Ascii85, PdfFilter::Flate])
        );

        let stored = b"<< /Length 3 >>stream";
        assert_eq!(
            filters_before(stored, stored.len() - b"stream".len()),
            Some(vec![])
        );
    }

    /// A single-stage Flate scan measures only the first expansion, so a
    /// `[/FlateDecode /FlateDecode]` stream whose inner payload exceeds the
    /// budget must be caught by applying the whole chain. The fixture is a
    /// loadable single-page PDF (Catalog/Pages/Page/xref), so `PDFium` would
    /// expand the chain if the preflight missed it.
    #[test]
    fn rejects_a_nested_flate_bomb() {
        let over = usize::try_from(MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let data = deflate(&deflate_spaces(over));
        let pdf = loadable_pdf_with_content_stream(b"[/FlateDecode /FlateDecode]", &data);
        assert!(
            pdf.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            pdf.len()
        );
        assert!(
            pdf.windows(b"xref".len()).any(|w| w == b"xref"),
            "the fixture must be a loadable PDF with an xref"
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// An ASCII85 wrapper makes the inner Flate data unreadable to a scan that
    /// only tries to inflate the raw stream bytes; decoding the chain in order
    /// still reaches the bomb. Same loadable single-page shape as the nested
    /// Flate fixture.
    #[test]
    fn rejects_an_ascii85_wrapped_flate_bomb() {
        let over = usize::try_from(MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let data = ascii85_encode(&deflate_spaces(over));
        let pdf = loadable_pdf_with_content_stream(b"[/ASCII85Decode /FlateDecode]", &data);
        assert!(
            pdf.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            pdf.len()
        );
        assert!(
            pdf.windows(b"/Type /Catalog".len())
                .any(|w| w == b"/Type /Catalog"),
            "the fixture must be a loadable PDF with a Catalog"
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A legitimately chained stream well within the budget is accepted, so the
    /// filter-aware scan does not reject ordinary ASCII85+Flate content.
    #[test]
    fn accepts_a_small_chained_stream() {
        let data = ascii85_encode(&deflate(b"hello world"));
        let pdf = loadable_pdf_with_content_stream(b"[/ASCII85Decode /FlateDecode]", &data);
        ensure_pdf_within_budget(&pdf).expect("a small chained stream is within the budget");
    }

    const UNVERIFIABLE_REASON: &str = "it declares a stream filter whose decompressed size cannot be verified \
         (an indirect, LZW, encrypted, or unrecognised /Filter)";

    fn flate_bomb() -> Vec<u8> {
        let over = usize::try_from(MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        deflate_spaces(over)
    }

    /// A `/Filter` key obscured with a `#XX` escape (`/Fil#74er` is `/Filter`)
    /// must still be recognised, so the Flate bomb behind it is budgeted.
    #[test]
    fn rejects_a_bomb_behind_an_escaped_filter_key() {
        let pdf = pdf_with_dict("/Fil#74er /FlateDecode", &flate_bomb());
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A filter name obscured with a `#XX` escape (`/Fl#61teDecode` is
    /// `/FlateDecode`) must classify as Flate, so its expansion is budgeted.
    #[test]
    fn rejects_a_bomb_behind_an_escaped_filter_name() {
        let pdf = pdf_with_dict("/Filter /Fl#61teDecode", &flate_bomb());
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A literal string that opens on one line and holds a `%` on the next is
    /// string text, not a comment, so the dictionary's `>>` after it still
    /// closes the dictionary and the Flate bomb behind it is budgeted.
    #[test]
    fn rejects_a_bomb_behind_a_multiline_literal_holding_a_percent() {
        let pdf = pdf_with_dict(
            "/Filter /FlateDecode /Note (harmless\n% literal)",
            &flate_bomb(),
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// Each byte is lexed once, so a document packed with what would cost a
    /// rescan per occurrence if each check restarted at the last payload —
    /// `stream` in comment lines or as a bare word, comment lines between a
    /// dictionary and its `stream`, `<<` in a comment inside the dictionary —
    /// is scanned in linear time, and the stream behind it is still found with
    /// its filter. A per-occurrence rescan takes minutes at these sizes, which
    /// trips the test timeout.
    #[test]
    fn finds_a_stream_behind_megabytes_of_comments_and_stream_words() {
        let pdf = |before_dict: &[u8], in_dict: &[u8], gap: &[u8]| {
            let mut pdf = b"%PDF-1.7\n".to_vec();
            pdf.extend_from_slice(before_dict);
            pdf.extend_from_slice(b"1 0 obj\n<< /Length 3 /Filter /FlateDecode");
            pdf.extend_from_slice(in_dict);
            pdf.extend_from_slice(b" >>\n");
            pdf.extend_from_slice(gap);
            pdf.extend_from_slice(b"stream\nabc\nendstream\nendobj\n");
            pdf
        };
        let lt_comment = [b" % ".as_slice(), &b"<".repeat(500_000), b"\n"].concat();
        let shapes = [
            (
                "stream in comment lines",
                pdf(&b"% stream\n".repeat(200_000), b"", b""),
            ),
            (
                "bare stream words",
                pdf(&b"x stream\n".repeat(200_000), b"", b""),
            ),
            (
                "comment lines before stream",
                pdf(b"", b"", &b"%c\n".repeat(500_000)),
            ),
            ("<< inside a dictionary comment", pdf(b"", &lt_comment, b"")),
        ];
        for (shape, pdf) in shapes {
            let data_start = pdf.len() - b"abc\nendstream\nendobj\n".len();
            let found: Vec<(usize, Vec<PdfFilter>)> = pdf_streams(&pdf)
                .into_iter()
                .map(|stream| (stream.data_start, stream.filters))
                .collect();
            assert_eq!(found, vec![(data_start, vec![PdfFilter::Flate])], "{shape}");
        }
    }

    /// A comment between filter-array elements must not truncate the chain, so
    /// the second Flate stage of a nested bomb is still applied.
    #[test]
    fn rejects_a_bomb_with_a_comment_in_the_filter_array() {
        let data = deflate(&flate_bomb());
        let pdf = pdf_with_dict(
            "/Filter [/FlateDecode %drop the rest\n /FlateDecode]",
            &data,
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// An indirect `/Filter` reference cannot be resolved by this scan, so the
    /// document is rejected rather than forwarded with an unchecked expansion.
    #[test]
    fn rejects_an_indirect_filter_reference() {
        let pdf = pdf_with_dict("/Filter 5 0 R", &flate_bomb());
        assert_eq!(pdf_too_large_reason(&pdf), UNVERIFIABLE_REASON);
    }

    /// A general-purpose `LZWDecode` stage is not decoded here; a chain that
    /// reaches one is rejected rather than left unbudgeted.
    #[test]
    fn rejects_an_lzw_filter_chain() {
        let pdf = pdf_with_dict("/Filter [/LZWDecode /FlateDecode]", b"anything at all");
        assert_eq!(pdf_too_large_reason(&pdf), UNVERIFIABLE_REASON);
    }

    /// An unrecognised filter name is rejected rather than silently skipped.
    #[test]
    fn rejects_an_unknown_filter() {
        let pdf = pdf_with_dict("/Filter /NotARealFilter", b"anything at all");
        assert_eq!(pdf_too_large_reason(&pdf), UNVERIFIABLE_REASON);
    }

    /// A terminal image codec produces a dimension-bounded raster, not a text
    /// content-stream bomb, so budgeting stops at it and the stream is accepted.
    #[test]
    fn accepts_a_terminal_image_codec() {
        let pdf = pdf_with_dict("/Filter /DCTDecode", b"\xff\xd8\xff\xe0 pretend jpeg bytes");
        ensure_pdf_within_budget(&pdf).expect("a terminal image codec stream is accepted");
    }

    /// Builds `<< /Length N <dict> stream\n <data>` and asserts the one real
    /// stream's chain is actually budgeted: the final expansion exceeds
    /// `remaining`, so a scan that drops a stage, treats the stream as stored,
    /// or stops at a decoy image codec would accept it instead.
    fn assert_chain_budgeted(dict: &[u8], data: &[u8]) {
        let mut pdf = b"%PDF-1.7\n1 0 obj\n<< /Length ".to_vec();
        pdf.extend_from_slice(data.len().to_string().as_bytes());
        pdf.push(b' ');
        pdf.extend_from_slice(dict);
        pdf.extend_from_slice(b" stream\n");
        pdf.extend_from_slice(data);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        let streams = pdf_streams(&pdf);
        assert_eq!(streams.len(), 1, "expected exactly one stream object");
        let err = decoded_len_within(&streams[0], &pdf, 8 * 1024)
            .expect_err("the declared chain must exceed the budget");
        match err {
            Error::DocumentTooLarge { reason, .. } => assert!(
                reason.contains("decompress to more than"),
                "expected the size budget to fire, got: {reason}"
            ),
            other => panic!("expected DocumentTooLarge, got: {other}"),
        }
    }

    fn double_flate() -> Vec<u8> {
        deflate(&deflate_spaces(20_000))
    }

    fn single_flate() -> Vec<u8> {
        deflate_spaces(20_000)
    }

    /// A comment between the dictionary closer and `stream` is whitespace. The
    /// double-Flate chain must still be read; guessing one Flate stage would
    /// only measure the small intermediate.
    #[test]
    fn rejects_a_chain_with_a_comment_before_stream() {
        let dict = b"<< /Filter [/FlateDecode /FlateDecode] >> % not a token\n";
        assert_chain_budgeted(dict, &double_flate());
    }

    /// A comment body may contain `>>` without ending the comment. That text
    /// must not be taken for the dictionary's closer.
    #[test]
    fn rejects_a_chain_with_closer_text_inside_the_comment() {
        let dict = b"<< /Filter [/FlateDecode /FlateDecode] >> % ignored >> ignored\n";
        assert_chain_budgeted(dict, &double_flate());
    }

    /// A second `%` inside a comment is still comment text. Taking the last `%`
    /// on the line would leave `ignored` as non-comment bytes and hide the
    /// stream; the first comment-starting `%` owns the rest of the line.
    #[test]
    fn rejects_a_chain_with_a_percent_inside_the_comment() {
        let dict = b"<< /Filter [/FlateDecode /FlateDecode] >> % ignored%again\n";
        assert_chain_budgeted(dict, &double_flate());
    }

    /// Same closer-in-comment case as a loadable single-page PDF: Catalog /
    /// Pages / Page / xref are present, and the comment sits between the
    /// content dictionary's `>>` and `stream`.
    #[test]
    fn rejects_a_loadable_bomb_with_closer_text_inside_the_comment() {
        let data = deflate(&flate_bomb());
        let mut content = Vec::new();
        content.extend_from_slice(b"<< /Length ");
        content.extend_from_slice(data.len().to_string().as_bytes());
        content.extend_from_slice(
            b" /Filter [/FlateDecode /FlateDecode] >> % ignored >> ignored\nstream\n",
        );
        content.extend_from_slice(&data);
        content.extend_from_slice(b"\nendstream");
        let objs: [&[u8]; 4] = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << >> >>",
            content.as_slice(),
        ];
        let mut out = b"%PDF-1.7\n".to_vec();
        let mut offsets = Vec::with_capacity(objs.len());
        for (i, body) in objs.iter().enumerate() {
            offsets.push(out.len());
            out.extend_from_slice(format!("{} 0 obj\n", i + 1).as_bytes());
            out.extend_from_slice(body);
            out.extend_from_slice(b"\nendobj\n");
        }
        let xref_pos = out.len();
        let n = objs.len() + 1;
        out.extend_from_slice(format!("xref\n0 {n}\n").as_bytes());
        out.extend_from_slice(b"0000000000 65535 f \n");
        for off in offsets {
            out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
        }
        out.extend_from_slice(
            format!("trailer\n<< /Size {n} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF")
                .as_bytes(),
        );
        let pdf = Bytes::from(out);
        assert!(pdf.windows(b"xref".len()).any(|w| w == b"xref"));
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// NUL is PDF whitespace, so it cannot hide the dictionary from `stream`
    /// nor split a `/Filter` key from its value.
    #[test]
    fn rejects_a_chain_with_nul_whitespace() {
        let mut before_keyword = b"<< /Filter [/FlateDecode /FlateDecode] >>".to_vec();
        before_keyword.push(0);
        assert_chain_budgeted(&before_keyword, &double_flate());

        let mut split_key = b"<< /Filter".to_vec();
        split_key.push(0);
        split_key.extend_from_slice(b"/FlateDecode >>\n");
        assert_chain_budgeted(&split_key, &single_flate());
    }

    /// A `/Filter` inside a string or a nested dictionary is not the stream's
    /// filter. The top-level Flate entry is.
    #[test]
    fn rejects_a_bomb_hidden_behind_a_decoy_filter() {
        assert_chain_budgeted(
            b"<< /Note (/Filter /DCTDecode) /Filter /FlateDecode >>\n",
            &single_flate(),
        );
        assert_chain_budgeted(
            b"<< /DecodeParms << /Filter /DCTDecode >> /Filter /FlateDecode >>\n",
            &single_flate(),
        );
        assert_chain_budgeted(b"<< /Filter /FlateDecode /Note (<<) >>\n", &single_flate());
        // A `%` inside a string on the dictionary's line is not a comment, so
        // it must not hide the closer or the real `/Filter`.
        assert_chain_budgeted(
            b"<< /Note (100% pure) /Filter /FlateDecode >>\n",
            &single_flate(),
        );
        assert_chain_budgeted(
            b"<< /Note (100 % pure) /Filter /FlateDecode >>\n",
            &single_flate(),
        );
        assert_chain_budgeted(
            b"<< /Note (% harmless) /Filter /FlateDecode >>\n",
            &single_flate(),
        );
    }

    /// The word `stream` inside content is not a stream object, so it is not
    /// given a guessed filter and cannot reject the document on its own.
    #[test]
    fn a_stream_word_inside_content_is_not_a_stream() {
        let pdf = pdf_with_dict("", b"hello stream world");
        let streams = pdf_streams(&pdf);
        assert_eq!(streams.len(), 1);
        assert!(streams[0].filters.is_empty());
    }

    /// An indirect `/Length` is how most writers declare a stream. It must not
    /// stop the dictionary parse before `/Filter` is read.
    #[test]
    fn reads_a_filter_after_an_indirect_length() {
        let raw = b"<< /Length 4 0 R /Filter /FlateDecode >>stream";
        let streams = pdf_streams(raw);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].filters, vec![PdfFilter::Flate]);
    }

    /// Unfiltered page content that happens to contain the word `stream` (and
    /// even a decoy `/Filter /LZWDecode`) must stay one stream with an empty
    /// filter chain. Scanning inside the payload would invent a second,
    /// unbounded stream and reject a legitimate document.
    #[test]
    fn accepts_content_that_mentions_stream_inside_the_payload() {
        let content = b"BT (<< /Filter /LZWDecode >>stream) Tj ET";
        // Build a loadable single-page PDF whose content stream is stored
        // (no `/Filter`) and whose `/Length` covers the whole payload.
        let mut content_obj = Vec::new();
        content_obj.extend_from_slice(b"<< /Length ");
        content_obj.extend_from_slice(content.len().to_string().as_bytes());
        content_obj.extend_from_slice(b" >>\nstream\n");
        content_obj.extend_from_slice(content);
        content_obj.extend_from_slice(b"\nendstream");
        let objs: [&[u8]; 4] = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << >> >>",
            content_obj.as_slice(),
        ];
        let mut out = b"%PDF-1.7\n".to_vec();
        let mut offsets = Vec::with_capacity(objs.len());
        for (i, body) in objs.iter().enumerate() {
            offsets.push(out.len());
            out.extend_from_slice(format!("{} 0 obj\n", i + 1).as_bytes());
            out.extend_from_slice(body);
            out.extend_from_slice(b"\nendobj\n");
        }
        let xref_pos = out.len();
        let n = objs.len() + 1;
        out.extend_from_slice(format!("xref\n0 {n}\n").as_bytes());
        out.extend_from_slice(b"0000000000 65535 f \n");
        for off in offsets {
            out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
        }
        out.extend_from_slice(
            format!("trailer\n<< /Size {n} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF")
                .as_bytes(),
        );
        let pdf = Bytes::from(out);

        let streams = pdf_streams(&pdf);
        assert_eq!(
            streams.len(),
            1,
            "payload text must not invent a second stream"
        );
        assert!(
            streams[0].filters.is_empty(),
            "an unfiltered content stream must stay unfiltered, got {:?}",
            streams[0].filters
        );
        ensure_pdf_within_budget(&pdf)
            .expect("ordinary text that mentions stream must not be rejected");
    }

    /// A direct `/Length 999999999` on an empty stream must not skip to EOF
    /// and hide a later Flate bomb: the claimed length has no matching
    /// `endstream`, so discovery falls back to the real terminator.
    #[test]
    fn still_budgets_after_a_direct_length_decoy() {
        let bomb = flate_bomb();
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        pdf.extend_from_slice(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");
        pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
        pdf.extend_from_slice(
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [4 0 R 5 0 R] /Resources << >> >>\nendobj\n",
        );
        pdf.extend_from_slice(b"4 0 obj\n<< /Length 999999999 >>\nstream\nendstream\nendobj\n");
        pdf.extend_from_slice(b"5 0 obj\n<< /Length ");
        pdf.extend_from_slice(bomb.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
        pdf.extend_from_slice(&bomb);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        let raw = Bytes::from(pdf);
        assert_eq!(pdf_streams(&raw).len(), 2);
        assert_eq!(
            pdf_too_large_reason(&raw),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// Object headers may use a tab between numbers (`6\t0 obj`). That form
    /// must still resolve so a Catalog-string decoy spelled with spaces is
    /// not the unique match.
    #[test]
    fn still_budgets_when_the_real_length_object_uses_a_tab() {
        let bomb = flate_bomb();
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        pdf.extend_from_slice(
            b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R /Note ( 6 0 obj\n99999999\nendobj) >>\nendobj\n",
        );
        pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
        pdf.extend_from_slice(
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [4 0 R 5 0 R] /Resources << >> >>\nendobj\n",
        );
        pdf.extend_from_slice(b"4 0 obj\n<< /Length 6 0 R >>\nstream\nendstream\nendobj\n");
        pdf.extend_from_slice(b"5 0 obj\n<< /Length ");
        pdf.extend_from_slice(bomb.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
        pdf.extend_from_slice(&bomb);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        // Real length object uses a tab between obj and generation numbers.
        pdf.extend_from_slice(b"6\t0 obj\n0\nendobj\n");
        let raw = Bytes::from(pdf);
        assert_eq!(pdf_streams(&raw).len(), 2);
        assert_eq!(
            pdf_too_large_reason(&raw),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// `/DecodeParms` with a predictor changes what Flate emits between
    /// stages. We do not reverse predictors, so such a stream is rejected
    /// rather than under-counted.
    #[test]
    fn rejects_a_stream_with_decode_parms() {
        let data = deflate(&flate_bomb());
        let pdf = loadable_pdf_with_content_stream(
            b"[/FlateDecode /FlateDecode] /DecodeParms << /Predictor 12 /Columns 1 >>",
            &data,
        );
        assert_eq!(pdf_too_large_reason(&pdf), UNVERIFIABLE_REASON);
    }

    /// A predictor reached through an indirect `/DecodeParms` reference, alone
    /// or inside a parameter array, is not resolved, so the stream fails
    /// closed instead of feeding predictor-tagged bytes to the second Flate.
    #[test]
    fn rejects_a_predictor_bomb_behind_indirect_decode_parms() {
        // PNG predictor with `/Columns 1`: each byte gets a `0` (None) tag.
        let predicted: Vec<u8> = flate_bomb().into_iter().flat_map(|b| [0, b]).collect();
        let data = deflate(&predicted);
        for decode_parms in [b"5 0 R".as_slice(), b"[5 0 R null]".as_slice()] {
            let mut decl = b"[/FlateDecode /FlateDecode] /DecodeParms ".to_vec();
            decl.extend_from_slice(decode_parms);
            let pdf = loadable_pdf_with_content_stream(&decl, &data);
            assert_eq!(
                pdf_too_large_reason(&pdf),
                UNVERIFIABLE_REASON,
                "/DecodeParms {}",
                String::from_utf8_lossy(decode_parms)
            );
        }
    }

    /// `null` entries and direct predictor-free dictionaries, even ones with an
    /// indirect non-predictor value, still have their expansion budgeted.
    #[test]
    fn still_budgets_with_predictor_free_decode_parms() {
        let pdf = loadable_pdf_with_content_stream(
            b"/FlateDecode /DecodeParms [null << /Columns 5 0 R >>]",
            &flate_bomb(),
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A Catalog string that contains the bytes of a fake `N 0 obj` /Length
    /// must not make discovery skip past a later Flate stream. The empty
    /// stream's real length is 0; trusting the decoy would advance to EOF.
    #[test]
    fn still_budgets_a_stream_after_an_indirect_length_decoy() {
        let bomb = flate_bomb();
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        // Object 1: Catalog with a decoy string that looks like object 6.
        pdf.extend_from_slice(
            b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R /Note ( 6 0 obj\n99999999\nendobj) >>\nendobj\n",
        );
        pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
        pdf.extend_from_slice(
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [4 0 R 5 0 R] /Resources << >> >>\nendobj\n",
        );
        // Object 4: empty stored stream whose /Length is the real object 6 (0).
        pdf.extend_from_slice(b"4 0 obj\n<< /Length 6 0 R >>\nstream\nendstream\nendobj\n");
        // Object 5: Flate bomb that must still be discovered and budgeted.
        pdf.extend_from_slice(b"5 0 obj\n<< /Length ");
        pdf.extend_from_slice(bomb.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
        pdf.extend_from_slice(&bomb);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        // Object 6: the real /Length value is 0.
        pdf.extend_from_slice(b"6 0 obj\n0\nendobj\n");
        let raw = Bytes::from(pdf);
        let streams = pdf_streams(&raw);
        assert!(
            streams.len() >= 2,
            "the Flate stream after the empty one must still be discovered, got {} streams",
            streams.len()
        );
        assert_eq!(
            pdf_too_large_reason(&raw),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A `%` inside a literal string after whitespace, e.g. `( % literal)`, is
    /// not a comment. Treating it as one would lose the dictionary before
    /// `stream` and omit the Flate bomb.
    #[test]
    fn rejects_a_bomb_with_percent_after_space_inside_a_string() {
        assert_chain_budgeted(
            b"<< /Note ( % literal) /Filter /FlateDecode >>\n",
            &single_flate(),
        );
    }

    /// A comment may start immediately after a name (`/FlateDecode% …`) with no
    /// whitespace. The `<<` that follows on the comment must not become the
    /// dictionary opener.
    #[test]
    fn rejects_a_bomb_with_a_comment_glued_to_the_filter_name() {
        let data = flate_bomb();
        let mut dict = b"<< /Filter /FlateDecode% <<\n /Length ".to_vec();
        dict.extend_from_slice(data.len().to_string().as_bytes());
        dict.extend_from_slice(b" >>\n");
        assert_chain_budgeted(&dict, &data);
    }

    /// A `<<` that appears inside a `%` comment is not the dictionary opener.
    /// Taking it would parse a suffix with no `/Filter` and count the Flate
    /// stream as stored.
    #[test]
    fn rejects_a_bomb_when_a_comment_contains_a_dict_opener() {
        let data = flate_bomb();
        let mut dict = b"<< /Filter /FlateDecode % <<\n /Length ".to_vec();
        dict.extend_from_slice(data.len().to_string().as_bytes());
        dict.extend_from_slice(b" >>\n");
        assert_chain_budgeted(&dict, &data);
    }

    /// A `/Length` that correctly covers a payload containing the bytes
    /// `endstream` must still skip the whole payload. Stopping at the embedded
    /// keyword would resume inside the image and hide the following Flate bomb.
    /// Covers both a direct length and a uniquely resolved indirect length.
    #[test]
    fn still_budgets_after_a_payload_with_embedded_endstream_bytes() {
        let bomb = flate_bomb();
        let image = b"aaa\nendstream\n(bbb";
        for length_decl in [
            image.len().to_string(),
            "6 0 R".to_owned(), // unique indirect → image.len()
        ] {
            let mut pdf = Vec::new();
            pdf.extend_from_slice(b"%PDF-1.7\n");
            pdf.extend_from_slice(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");
            pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
            pdf.extend_from_slice(
                b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [4 0 R 5 0 R] /Resources << >> >>\nendobj\n",
            );
            pdf.extend_from_slice(b"4 0 obj\n<< /Length ");
            pdf.extend_from_slice(length_decl.as_bytes());
            pdf.extend_from_slice(b" >>\nstream\n");
            pdf.extend_from_slice(image);
            pdf.extend_from_slice(b"\nendstream\nendobj\n");
            pdf.extend_from_slice(b"5 0 obj\n<< /Length ");
            pdf.extend_from_slice(bomb.len().to_string().as_bytes());
            pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
            pdf.extend_from_slice(&bomb);
            pdf.extend_from_slice(b"\nendstream\nendobj\n");
            pdf.extend_from_slice(b"6 0 obj\n");
            pdf.extend_from_slice(image.len().to_string().as_bytes());
            pdf.extend_from_slice(b"\nendobj\n");
            let raw = Bytes::from(pdf);
            assert_eq!(
                pdf_streams(&raw).len(),
                2,
                "length decl {length_decl:?} must still discover both streams"
            );
            assert_eq!(
                pdf_too_large_reason(&raw),
                "its compressed streams decompress to more than 256 MiB, the per-document limit"
            );
        }
    }

    /// A legal `/Length +N` (optional leading plus) must still be read as a
    /// direct length so the stream is budgeted, not rejected as unboundable.
    #[test]
    fn budgets_a_stream_whose_length_has_a_leading_plus() {
        let data = single_flate();
        let mut dict = b"<< /Length +".to_vec();
        dict.extend_from_slice(data.len().to_string().as_bytes());
        dict.extend_from_slice(b" /Filter /FlateDecode >>\n");
        assert_chain_budgeted(&dict, &data);
    }

    /// An earlier unfiltered stream whose payload contains a `(` byte must not
    /// make the following Flate stream look like it sits inside a string.
    #[test]
    fn still_budgets_a_flate_stream_after_a_payload_with_a_paren_byte() {
        let bomb = flate_bomb();
        let image = b"\x28one pixel\x29"; // leading 0x28 == b'('
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        pdf.extend_from_slice(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");
        pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
        pdf.extend_from_slice(
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents [4 0 R 5 0 R] /Resources << >> >>\nendobj\n",
        );
        pdf.extend_from_slice(b"4 0 obj\n<< /Length ");
        pdf.extend_from_slice(image.len().to_string().as_bytes());
        pdf.extend_from_slice(b" >>\nstream\n");
        pdf.extend_from_slice(image);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        pdf.extend_from_slice(b"5 0 obj\n<< /Length ");
        pdf.extend_from_slice(bomb.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
        pdf.extend_from_slice(&bomb);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        let raw = Bytes::from(pdf);
        assert_eq!(pdf_streams(&raw).len(), 2);
        assert_eq!(
            pdf_too_large_reason(&raw),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A Catalog comment that ends with the word `stream` is not a stream
    /// object and must not consume the real Flate content stream.
    #[test]
    fn still_budgets_when_a_catalog_comment_mentions_stream() {
        let bomb = flate_bomb();
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        pdf.extend_from_slice(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >> % stream\nendobj\n");
        pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
        pdf.extend_from_slice(
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << >> >>\nendobj\n",
        );
        pdf.extend_from_slice(b"4 0 obj\n<< /Length ");
        pdf.extend_from_slice(bomb.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
        pdf.extend_from_slice(&bomb);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        let raw = Bytes::from(pdf);
        assert_eq!(pdf_streams(&raw).len(), 1);
        assert_eq!(
            pdf_too_large_reason(&raw),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A literal string that contains dictionary-like text and the word
    /// `stream` must not invent a phantom stream whose huge `/Length` hides
    /// the real Flate page stream. Covers both a `/Length` decoy and a bare
    /// `<< >>stream` with no length.
    #[test]
    fn still_budgets_when_a_string_contains_a_fake_stream() {
        for note in [
            b" (<< /Length 99999999 >>stream\nplaceholder) ".as_slice(),
            b" (<< >>stream) ".as_slice(),
        ] {
            let bomb = flate_bomb();
            let mut pdf = Vec::new();
            pdf.extend_from_slice(b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R /Note");
            pdf.extend_from_slice(note);
            pdf.extend_from_slice(b">>\nendobj\n");
            pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
            pdf.extend_from_slice(
                b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << >> >>\nendobj\n",
            );
            pdf.extend_from_slice(b"4 0 obj\n<< /Length ");
            pdf.extend_from_slice(bomb.len().to_string().as_bytes());
            pdf.extend_from_slice(b" /Filter /FlateDecode >>\nstream\n");
            pdf.extend_from_slice(&bomb);
            pdf.extend_from_slice(b"\nendstream\nendobj\n");
            let raw = Bytes::from(pdf);
            assert_eq!(
                pdf_streams(&raw).len(),
                1,
                "the string must not invent a second stream"
            );
            assert_eq!(
                pdf_too_large_reason(&raw),
                "its compressed streams decompress to more than 256 MiB, the per-document limit"
            );
        }
    }

    /// Attacker-controlled nesting in a skipped value must fail closed without
    /// overflowing the stack. A nest past [`MAX_VALUE_DEPTH`] makes the
    /// dictionary unparsable, so the stream is unbounded and the document is
    /// rejected.
    #[test]
    fn rejects_a_dictionary_with_excessive_value_nesting() {
        let mut dict = b"<< /Filter /FlateDecode /Note ".to_vec();
        let depth = MAX_VALUE_DEPTH + 8;
        dict.extend(std::iter::repeat_n(b'[', depth));
        dict.extend(std::iter::repeat_n(b']', depth));
        dict.extend_from_slice(b" >>");
        let pdf = {
            let mut out = b"%PDF-1.7\n1 0 obj\n".to_vec();
            out.extend_from_slice(&dict);
            out.extend_from_slice(b"\nstream\nhello\nendstream\nendobj\n");
            Bytes::from(out)
        };
        assert_eq!(pdf_too_large_reason(&pdf), UNVERIFIABLE_REASON);
    }
}
