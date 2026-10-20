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

/// Bounds the bytes a PDF would decompress to, defending against a Flate
/// content-stream bomb before the bytes reach `PDFium`, which inflates
/// `/FlateDecode` streams internally with no size limit.
///
/// A byte-level cap on the archive entries (as [`rebuild_package`] applies to
/// OOXML) cannot help here: the compression is inside the PDF's own object
/// streams, not a surrounding zip. Instead every Flate stream in the file is
/// inflated through a counter that discards the output, failing as soon as the
/// running total crosses [`MAX_PACKAGE_BYTES`]. The counter never holds more
/// than one 32 KiB block, so scanning an 8 GiB bomb costs the CPU to inflate
/// `MAX_PACKAGE_BYTES` and no more.
pub(crate) fn ensure_pdf_within_budget(raw: &Bytes) -> Result<()> {
    let budget = MAX_PACKAGE_BYTES;
    let mut consumed: u64 = 0;

    for start in flate_stream_starts(raw) {
        let remaining = budget.saturating_sub(consumed);
        let inflated = inflate_counting(&raw[start..], remaining.saturating_add(1));
        if inflated > remaining {
            return Err(Error::DocumentTooLarge {
                format: DocumentType::Pdf,
                reason: format!(
                    "its compressed streams decompress to more than {}, the per-document limit",
                    describe_bytes(budget)
                ),
            });
        }
        consumed += inflated;
    }
    Ok(())
}

/// Byte offsets just past each `stream` keyword's end-of-line, i.e. the first
/// byte of stream data. `endstream` is skipped. Over-counting is harmless: an
/// offset that is not really Flate data inflates to nothing.
fn flate_stream_starts(raw: &[u8]) -> Vec<usize> {
    const KEYWORD: &[u8] = b"stream";
    let mut starts = Vec::new();
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
        if data < raw.len() {
            starts.push(data);
        }
    }
    starts
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
/// deflate as `PDFium` does, and returns the number of bytes produced, reading no
/// more than `limit` of them. The decompressed bytes are discarded.
fn inflate_counting(data: &[u8], limit: u64) -> u64 {
    use flate2::read::{DeflateDecoder, ZlibDecoder};
    use std::io::{Read, copy, sink};

    let zlib = copy(&mut ZlibDecoder::new(data).take(limit), &mut sink()).unwrap_or(0);
    if zlib > 0 {
        return zlib;
    }
    // A non-conformant producer may omit the zlib header; PDFium accepts the
    // raw deflate, so the budget must see it too.
    copy(&mut DeflateDecoder::new(data).take(limit), &mut sink()).unwrap_or(0)
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
}
