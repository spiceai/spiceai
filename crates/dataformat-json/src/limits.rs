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

//! Bounds on reading a JSON file.
//!
//! - Every decoder of a compressed file is held to the compression-bomb check every
//!   connector shares ([`http_body_limit::Expansion`]): once a file has decoded past
//!   64 MiB, it is refused if it has expanded far past what real data in its codec
//!   compresses to. The check is per file, in schema inference, schema-drift probes and
//!   scans alike.
//! - A probe that must parse a file whole reads it through
//!   [`http_body_limit::read_to_end`] under a reservation on the query memory pool, so a
//!   file the pool cannot hold is refused instead of exhausting the process.
//!
//! Either refusal names the file and, when the format knows it, the dataset and the
//! connector whose page the error links.

use std::io::Read;

use bytes::Bytes;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::{DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http_body_limit::{Codec, Expansion, Input};
use object_store::path::Path;

/// The dataset a file belongs to and the connector that reads it, which a refusal names
/// and links.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceLabel {
    /// The dataset's name.
    pub dataset: String,
    /// The connector's documentation page name under `components/data-connectors/`, such
    /// as `s3` or `https`.
    pub connector: String,
}

/// The codec a compressed file is checked as, or `None` for an uncompressed file.
#[must_use]
pub fn codec_of(compression: FileCompressionType) -> Option<Codec> {
    match compression.get_variant() {
        CompressionTypeVariant::GZIP => Some(Codec::Gzip),
        CompressionTypeVariant::BZIP2 => Some(Codec::Bzip2),
        CompressionTypeVariant::XZ => Some(Codec::Xz),
        CompressionTypeVariant::ZSTD => Some(Codec::Zstd),
        CompressionTypeVariant::UNCOMPRESSED => None,
    }
}

/// A reader of `raw` decompressed as `compression`, whose reads fail once the file
/// expands past its codec's compression-bomb threshold.
///
/// # Errors
///
/// Returns the error creating the decoder.
pub fn decoder(
    compression: FileCompressionType,
    raw: impl Read + Send + 'static,
) -> Result<Box<dyn Read + Send>> {
    checked_decoder(compression, raw).map(|(reader, _)| reader)
}

/// [`decoder`], and the check it reads under, for a caller whose parser turns a failed
/// read into text: [`Expansion::refused`] still tells it the file was a compression bomb.
///
/// # Errors
///
/// Returns the error creating the decoder.
pub fn checked_decoder(
    compression: FileCompressionType,
    raw: impl Read + Send + 'static,
) -> Result<(Box<dyn Read + Send>, Option<Expansion>)> {
    let Some(codec) = codec_of(compression) else {
        return Ok((Box::new(raw), None));
    };
    let expansion = Expansion::new(codec, Input::File);
    let decoded = compression.convert_read(expansion.count_input(raw))?;
    Ok((Box::new(expansion.check_output(decoded)), Some(expansion)))
}

/// `stream` decompressed as `compression`, failing once the file expands past its
/// codec's compression-bomb threshold with the error [`refused`] builds.
///
/// # Errors
///
/// Returns the error creating the decoder.
pub fn decoded_stream(
    compression: FileCompressionType,
    stream: BoxStream<'static, Result<Bytes>>,
    kind: &'static str,
    location: &Path,
    source: Option<&SourceLabel>,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let Some(codec) = codec_of(compression) else {
        return compression.convert_stream(stream);
    };
    let expansion = Expansion::new(codec, Input::File);
    let received = expansion.clone();
    let counted = stream
        .inspect_ok(move |chunk| received.received(chunk.len()))
        .boxed();
    let location = location.clone();
    let source = source.cloned();
    Ok(compression
        .convert_stream(counted)?
        .and_then(move |chunk| {
            let checked = expansion
                .decoded(chunk.len())
                .map(|()| chunk)
                .map_err(|refusal| refused(kind, &location, source.as_ref(), &refusal));
            futures::future::ready(checked)
        })
        .boxed())
}

/// The error for a `kind` file (`JSON`, `CSV`) the runtime refused, naming the file, the
/// dataset when known, and the fix: a compression bomb's fix is how the file is written,
/// and a file too large for the query memory pool's is the memory limit.
#[must_use]
pub fn refused(
    kind: &str,
    location: &Path,
    source: Option<&SourceLabel>,
    refusal: &http_body_limit::Error,
) -> DataFusionError {
    let subject = match source {
        Some(source) => format!(
            "Failed to read {kind} file '{location}' for dataset '{}'",
            source.dataset
        ),
        None => format!("Failed to read {kind} file '{location}'"),
    };
    match refusal {
        http_body_limit::Error::CompressionRatio { .. } => {
            let link = match source {
                Some(source) => format!(
                    "https://spiceai.org/docs/components/data-connectors/{}",
                    source.connector
                ),
                None => "https://spiceai.org/docs/components/data-connectors".to_string(),
            };
            let message = format!("{subject}: {refusal} See: {link}");
            match http_body_limit::CompressionRefusal::new(message.clone(), refusal) {
                Some(typed) => DataFusionError::External(Box::new(typed)),
                None => DataFusionError::Execution(message),
            }
        }
        _ => DataFusionError::ResourcesExhausted(format!(
            "{subject}: {refusal}. Raise `runtime.query.memory_limit`, or split the file into \
             smaller files. See: https://spiceai.org/docs/reference/memory"
        )),
    }
}

/// `error`, or the error for a refused file when a check in this module refused it
/// anywhere along `error`'s chain of sources.
#[must_use]
pub fn locate_refusal(
    kind: &str,
    location: &Path,
    source: Option<&SourceLabel>,
    error: DataFusionError,
) -> DataFusionError {
    match http_body_limit::find_refusal(&error) {
        Some(refusal) => refused(kind, location, source, refusal),
        None => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const MIB: usize = 1024 * 1024;

    /// A file of one byte repeated past the floor, compressed at gzip's best level: it
    /// expands near deflate's ceiling of about 1,000:1 on any deflate implementation,
    /// far past gzip's 400:1 threshold and what real data reaches.
    fn bomb() -> Vec<u8> {
        let decoded = http_body_limit::RATIO_CHECK_FLOOR_BYTES + 8 * MIB;
        let block = vec![b'a'; MIB];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        for _ in 0..decoded / MIB {
            encoder.write_all(&block).expect("compress");
        }
        let compressed = encoder.finish().expect("finish gzip");
        assert!(
            compressed.len() * 2 * http_body_limit::Codec::Gzip.max_ratio() < decoded,
            "the fixture expands well past gzip's threshold: {} bytes compressed",
            compressed.len()
        );
        compressed
    }

    fn label() -> SourceLabel {
        SourceLabel {
            dataset: "events".to_string(),
            connector: "s3".to_string(),
        }
    }

    #[test]
    fn compressed_file_past_the_threshold_is_refused_and_named() {
        let location = Path::from("bucket/events/part-0.jsonl.gz");
        let compressed = bomb();
        let mut decoded =
            decoder(FileCompressionType::GZIP, std::io::Cursor::new(compressed)).expect("decoder");
        let error = std::io::copy(&mut decoded, &mut std::io::sink())
            .expect_err("a file that expands past the threshold is refused");
        let error = locate_refusal(
            "JSON",
            &location,
            Some(&label()),
            DataFusionError::IoError(error),
        );
        assert!(
            http_body_limit::is_compression_refusal(&error),
            "the refusal keeps its type: {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.starts_with(
                "External error: Failed to read JSON file 'bucket/events/part-0.jsonl.gz' for \
                 dataset 'events': the gzip-compressed file expanded more than 400:1 ("
            ),
            "{message}"
        );
        assert!(
            message.ends_with(
                "read), far past what real data compresses to, so it was refused as a \
                 compression bomb. If the data is genuine, write it uncompressed or at a lower \
                 compression level. See: https://spiceai.org/docs/components/data-connectors/s3"
            ),
            "{message}"
        );
        assert!(!message.contains("memory_limit"), "{message}");
    }

    #[test]
    fn uncompressed_and_small_compressed_files_read_whole() {
        let text: Vec<u8> = (0..2 * MIB).map(|i| b"0123456789\n"[i % 11]).collect();
        let mut out = Vec::new();
        decoder(
            FileCompressionType::UNCOMPRESSED,
            std::io::Cursor::new(text.clone()),
        )
        .expect("decoder")
        .read_to_end(&mut out)
        .expect("an uncompressed file reads");
        assert_eq!(out, text);

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(&text).expect("compress");
        let compressed = encoder.finish().expect("finish gzip");
        let mut out = Vec::new();
        decoder(FileCompressionType::GZIP, std::io::Cursor::new(compressed))
            .expect("decoder")
            .read_to_end(&mut out)
            .expect("a small compressed file reads at any ratio");
        assert_eq!(out, text);
    }

    #[tokio::test]
    async fn compressed_stream_past_the_threshold_is_refused_and_named() {
        let location = Path::from("bucket/events/part-0.jsonl.gz");
        let compressed = Bytes::from(bomb());
        let chunks: Vec<Result<Bytes>> = compressed
            .chunks(64 * 1024)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let stream = futures::stream::iter(chunks).boxed();
        let mut decoded = decoded_stream(
            FileCompressionType::GZIP,
            stream,
            "JSON",
            &location,
            Some(&label()),
        )
        .expect("stream");
        let mut total = 0usize;
        let error = loop {
            match decoded.next().await {
                Some(Ok(chunk)) => total += chunk.len(),
                Some(Err(error)) => break error,
                None => panic!("the stream ended after {total} bytes without a refusal"),
            }
        };
        assert!(
            http_body_limit::is_compression_refusal(&error),
            "the refusal keeps its type: {error:?}"
        );
        assert!(
            error.to_string().starts_with(
                "External error: Failed to read JSON file 'bucket/events/part-0.jsonl.gz' for \
                 dataset 'events': the gzip-compressed file expanded more than 400:1"
            ),
            "{error}"
        );
    }
}
