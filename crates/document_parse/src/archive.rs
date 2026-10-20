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

    let mut archive = ZipArchive::new(Cursor::new(raw.clone()))
        .boxed()
        .context(InternalParsingSnafu { format })?;
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
        let name = entry.name().to_owned();
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
/// `LZWDecode` and the image codecs (`DCTDecode`, `JPXDecode`, …) are not
/// expanded here: their output is an image raster bounded by the declared
/// dimensions rather than a text content-stream bomb, so budgeting stops at
/// them. A chain that reaches one of those filters is therefore not bounded by
/// this check.
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

/// A decompressing filter stage we model, plus the catch-all for filters whose
/// output is not an unbounded text expansion (image codecs, `LZWDecode`) or
/// that we do not recognise. Budgeting stops at an [`PdfFilter::Opaque`] stage.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PdfFilter {
    Flate,
    Ascii85,
    AsciiHex,
    RunLength,
    Opaque,
}

fn classify_filter(name: &[u8]) -> PdfFilter {
    match name {
        b"FlateDecode" | b"Fl" => PdfFilter::Flate,
        b"ASCII85Decode" | b"A85" => PdfFilter::Ascii85,
        b"ASCIIHexDecode" | b"AHx" => PdfFilter::AsciiHex,
        b"RunLengthDecode" | b"RL" => PdfFilter::RunLength,
        _ => PdfFilter::Opaque,
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
/// and failing once any stage crosses the remaining budget. Stages up to an
/// [`PdfFilter::Opaque`] one are still budgeted; the opaque output itself (an
/// image raster, stored bytes, or an unmodelled filter) is not, matching the
/// previous behaviour of counting non-Flate streams as zero.
fn decoded_len_within(stream: &PdfStream, raw: &[u8], remaining: u64) -> Result<u64> {
    let cap = remaining.saturating_add(1);
    let mut buf: Vec<u8> = Vec::new();
    let mut size: u64 = 0;
    let last = stream.filters.len().saturating_sub(1);
    for (idx, &filter) in stream.filters.iter().enumerate() {
        if filter == PdfFilter::Opaque {
            return Ok(size);
        }
        let input: &[u8] = if idx == 0 { &raw[stream.data_start..] } else { &buf };
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

/// Applies one filter stage, reading no more than `cap` output bytes. `Opaque`
/// is handled by the caller and never reaches here.
fn decode_stage(filter: PdfFilter, data: &[u8], cap: u64) -> Vec<u8> {
    match filter {
        PdfFilter::Flate => inflate_bounded(data, cap),
        PdfFilter::Ascii85 => ascii85_decode_bounded(data, cap),
        PdfFilter::AsciiHex => asciihex_decode_bounded(data, cap),
        PdfFilter::RunLength => runlength_decode_bounded(data, cap),
        PdfFilter::Opaque => Vec::new(),
    }
}

/// Every stream object in `raw`, each paired with its declared filter chain.
fn pdf_streams(raw: &[u8]) -> Vec<PdfStream> {
    const KEYWORD: &[u8] = b"stream";
    let mut streams = Vec::new();
    let mut from = 0;
    while let Some(rel) = find(&raw[from..], KEYWORD) {
        let pos = from + rel;
        from = pos + KEYWORD.len();
        // `endstream` also contains `stream`; skip it.
        if pos >= 3 && &raw[pos - 3..pos] == b"end" {
            continue;
        }
        // Stream data begins after an end-of-line that is CRLF or a bare LF.
        let mut data = from;
        if raw.get(data) == Some(&b'\r') {
            data += 1;
        }
        if raw.get(data) == Some(&b'\n') {
            data += 1;
        }
        if data >= raw.len() {
            continue;
        }
        streams.push(PdfStream {
            filters: filters_before(raw, pos),
            data_start: data,
        });
    }
    streams
}

/// The `/Filter` chain of the dictionary that ends just before the `stream`
/// keyword at `keyword_pos`. An absent `/Filter` yields an empty chain; a
/// `/Filter` that cannot be read as a name or an array of names (for example an
/// indirect reference) falls back to a single [`PdfFilter::Flate`] stage, the
/// previous behaviour, so an unparsable dictionary is no worse than before.
fn filters_before(raw: &[u8], keyword_pos: usize) -> Vec<PdfFilter> {
    let Some((open, close)) = dict_before(raw, keyword_pos) else {
        return vec![PdfFilter::Flate];
    };
    let dict = &raw[open..close];
    let Some(after) = find(dict, b"/Filter").map(|p| p + b"/Filter".len()) else {
        // A stream with no `/Filter` is stored uncompressed.
        return Vec::new();
    };

    let rest = skip_ws(&dict[after..]);
    match rest.first() {
        Some(b'/') => vec![classify_filter(read_name(&rest[1..]))],
        Some(b'[') => {
            let mut filters = Vec::new();
            let mut cursor = &rest[1..];
            loop {
                cursor = skip_ws(cursor);
                match cursor.first() {
                    Some(b'/') => {
                        let name = read_name(&cursor[1..]);
                        filters.push(classify_filter(name));
                        cursor = &cursor[1 + name.len()..];
                    }
                    // The closing `]`, the end of the dictionary, or any
                    // unexpected token (e.g. an indirect reference): stop and
                    // keep what was read.
                    _ => break,
                }
            }
            if filters.is_empty() {
                vec![PdfFilter::Flate]
            } else {
                filters
            }
        }
        // `/Filter` present but neither a name nor an array (e.g. an indirect
        // reference). Fall back to the single-Flate best effort.
        _ => vec![PdfFilter::Flate],
    }
}

/// The byte range `[open, close)` of the `<< … >>` dictionary whose closing
/// `>>` sits (after optional whitespace) immediately before `keyword_pos`,
/// balancing nested dictionaries such as `/DecodeParms << … >>`.
fn dict_before(raw: &[u8], keyword_pos: usize) -> Option<(usize, usize)> {
    let mut i = keyword_pos;
    while i > 0 && raw[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i < 2 || &raw[i - 2..i] != b">>" {
        return None;
    }
    let close = i;
    let mut depth = 0i32;
    let mut j = close;
    while j >= 2 {
        if &raw[j - 2..j] == b">>" {
            depth += 1;
            j -= 2;
        } else if &raw[j - 2..j] == b"<<" {
            depth -= 1;
            j -= 2;
            if depth == 0 {
                return Some((j, close));
            }
        } else {
            j -= 1;
        }
    }
    None
}

fn skip_ws(data: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < data.len() && data[i].is_ascii_whitespace() {
        i += 1;
    }
    &data[i..]
}

/// A PDF name token's bytes, up to the next whitespace or delimiter.
fn read_name(data: &[u8]) -> &[u8] {
    const DELIMS: &[u8] = b"/[]<>(){}%";
    let end = data
        .iter()
        .position(|b| b.is_ascii_whitespace() || DELIMS.contains(b))
        .unwrap_or(data.len());
    &data[..end]
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
                    let value = group
                        .iter()
                        .fold(0u32, |acc, &d| acc.wrapping_mul(85).wrapping_add(u32::from(d)));
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
        let value = group
            .iter()
            .fold(0u32, |acc, &d| acc.wrapping_mul(85).wrapping_add(u32::from(d)));
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
}

#[cfg(test)]
mod tests {
    use super::test_support::{Part, deflated_package};
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
            let name = entry.name().to_owned();
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

    fn pdf_with_stream(filter_decl: &str, data: &[u8]) -> Bytes {
        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n1 0 obj\n<< /Length ");
        pdf.extend_from_slice(data.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter ");
        pdf.extend_from_slice(filter_decl.as_bytes());
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
            vec![PdfFilter::Flate]
        );

        let chain = b"<< /Filter [/ASCII85Decode /FlateDecode] /DecodeParms << /x 1 >> >>stream";
        assert_eq!(
            filters_before(chain, chain.len() - b"stream".len()),
            vec![PdfFilter::Ascii85, PdfFilter::Flate]
        );

        let stored = b"<< /Length 3 >>stream";
        assert!(filters_before(stored, stored.len() - b"stream".len()).is_empty());
    }

    /// A single-stage Flate scan measures only the first expansion, so a
    /// `[/FlateDecode /FlateDecode]` stream whose inner payload exceeds the
    /// budget must be caught by applying the whole chain.
    #[test]
    fn rejects_a_nested_flate_bomb() {
        let over = usize::try_from(MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let data = deflate(&deflate_spaces(over));
        let pdf = pdf_with_stream("[/FlateDecode /FlateDecode]", &data);
        assert!(
            pdf.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            pdf.len()
        );
        assert_eq!(
            pdf_too_large_reason(&pdf),
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// An ASCII85 wrapper makes the inner Flate data unreadable to a scan that
    /// only tries to inflate the raw stream bytes; decoding the chain in order
    /// still reaches the bomb.
    #[test]
    fn rejects_an_ascii85_wrapped_flate_bomb() {
        let over = usize::try_from(MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let data = ascii85_encode(&deflate_spaces(over));
        let pdf = pdf_with_stream("[/ASCII85Decode /FlateDecode]", &data);
        assert!(
            pdf.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            pdf.len()
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
        let pdf = pdf_with_stream("[/ASCII85Decode /FlateDecode]", &data);
        ensure_pdf_within_budget(&pdf).expect("a small chained stream is within the budget");
    }
}
