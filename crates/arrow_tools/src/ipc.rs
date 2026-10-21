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

//! What an Arrow IPC message declares, read from its header.
//!
//! Every Flight receiver has to decide what a `FlightData` message carries before it decodes
//! it, and the body length does not answer that question — see [`declared_message_header`].
//! The predicates live here rather than beside one receiver so that the scheduler's
//! write-through path and the runtime's `DoPut` handler share one answer.
//!
//! [`check_stream_framing`] answers the same kind of question for a whole IPC stream held in
//! memory: whether the lengths its messages declare fit inside it, before a reader allocates them.

use arrow::error::ArrowError;

/// What an IPC message's header declares, or `None` when the message carries no header bytes.
///
/// The header is the discriminator, not the body length: a batch of zero rows — and a batch
/// whose columns need no buffers — is sent with an empty body, so treating an empty body as
/// "no data" both drops rows the writer sent and under-counts the ones a failed write discarded.
///
/// A message with no header bytes at all declares nothing — Flight allows a metadata-only
/// message, and there is nothing there to misread. A header that has bytes but will not parse is
/// neither a declaration nor the absence of one: it is a malformed stream, and the `Err` is what
/// lets a caller report that parse failure instead of the "carries no batch" diagnosis a `false`
/// would produce, which names the wrong problem and hides the reason the IPC was rejected.
///
/// # Errors
///
/// Returns the flatbuffer verifier's message when `data_header` is non-empty but is not a
/// readable IPC `Message`.
pub fn declared_message_header(
    data_header: &[u8],
) -> Result<Option<arrow_ipc::MessageHeader>, String> {
    if data_header.is_empty() {
        return Ok(None);
    }

    arrow_ipc::root_as_message(data_header)
        .map(|message| Some(message.header_type()))
        .map_err(|e| e.to_string())
}

/// Whether an IPC message's header declares a record batch — the messages a write decodes.
///
/// # Errors
///
/// Propagates the parse failure from [`declared_message_header`].
pub fn declares_record_batch(data_header: &[u8]) -> Result<bool, String> {
    Ok(declared_message_header(data_header)? == Some(arrow_ipc::MessageHeader::RecordBatch))
}

/// Whether an IPC message's header declares data a write needed: a record batch, or a
/// dictionary the batches referencing it cannot be decoded without.
///
/// Wider than [`declares_record_batch`] because it answers a different question. That one asks
/// what to decode; this one asks what was lost. A dictionary message carries the values its
/// batch refers to, so a batch that references one carries nothing without it — a discarded
/// dictionary is discarded client data even though it is not itself a batch.
///
/// # Errors
///
/// Propagates the parse failure from [`declared_message_header`].
pub fn declares_ipc_data(data_header: &[u8]) -> Result<bool, String> {
    Ok(matches!(
        declared_message_header(data_header)?,
        Some(arrow_ipc::MessageHeader::RecordBatch | arrow_ipc::MessageHeader::DictionaryBatch)
    ))
}

/// The four bytes that precede each message's metadata length in an Arrow IPC stream.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// What [`check_stream_framing`] learned about a stream while walking its messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamFraming {
    /// Number of messages before the end of the stream, the schema message included.
    pub messages: usize,
    /// Whether any record batch or dictionary batch declares body compression. A compressed
    /// buffer declares its own uncompressed length, which the decoder reserves before
    /// decompressing, so that length is bounded by nothing in the stream's size.
    pub compressed: bool,
}

/// Checks that every length an Arrow IPC stream declares fits inside the bytes that carry it.
///
/// `arrow-ipc`'s `StreamReader` allocates each message's metadata buffer and zero-filled body
/// buffer at the sizes the message declares — the 32-bit metadata length prefix and the
/// flatbuffer's 64-bit `bodyLength` — before it reads either, so a stream of a few hundred bytes
/// can declare a body of any size and have it allocated. Walking the message headers first and
/// refusing a stream whose declared lengths run past its end bounds every one of those
/// allocations by `bytes.len()`. A stream this accepts is framed the way the reader will read it;
/// one it refuses is one the reader fails on too, only after the allocation.
///
/// The walk follows the reader's own end-of-stream rules: no bytes left, fewer than four bytes
/// left, or a zero metadata length all end the stream.
///
/// # Errors
///
/// Returns [`ArrowError::ParseError`] when a message's metadata length is negative or exceeds the
/// bytes left, its metadata is not a readable IPC `Message`, or its body length is negative or
/// exceeds the bytes left after the metadata.
pub fn check_stream_framing(bytes: &[u8]) -> Result<StreamFraming, ArrowError> {
    let mut framing = StreamFraming {
        messages: 0,
        compressed: false,
    };
    let mut rest = bytes;
    loop {
        let Some((prefix, after_prefix)) = rest.split_first_chunk::<4>() else {
            return Ok(framing);
        };
        let (declared, after_length) = if *prefix == CONTINUATION_MARKER {
            after_prefix.split_first_chunk::<4>().ok_or_else(|| {
                framing_error(format!(
                    "message {} ends inside its metadata length",
                    framing.messages
                ))
            })?
        } else {
            (prefix, after_prefix)
        };
        let metadata_len = i32::from_le_bytes(*declared);
        if metadata_len == 0 {
            return Ok(framing);
        }
        let metadata_len = usize::try_from(metadata_len)
            .ok()
            .filter(|&len| len <= after_length.len())
            .ok_or_else(|| {
                framing_error(format!(
                    "message {} declares {metadata_len} bytes of metadata, but only {} bytes remain",
                    framing.messages,
                    after_length.len()
                ))
            })?;
        let (metadata, after_metadata) = after_length.split_at(metadata_len);
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            framing_error(format!(
                "message {} has unreadable metadata: {error}",
                framing.messages
            ))
        })?;
        let body_len = message.bodyLength();
        let body_len = usize::try_from(body_len)
            .ok()
            .filter(|&len| len <= after_metadata.len())
            .ok_or_else(|| {
                framing_error(format!(
                    "message {} declares a {body_len} byte body, but only {} bytes remain",
                    framing.messages,
                    after_metadata.len()
                ))
            })?;
        framing.compressed |= message
            .header_as_record_batch()
            .or_else(|| {
                message
                    .header_as_dictionary_batch()
                    .and_then(|dictionary| dictionary.data())
            })
            .is_some_and(|batch| batch.compression().is_some());
        framing.messages += 1;
        rest = &after_metadata[body_len..];
    }
}

fn framing_error(detail: String) -> ArrowError {
    ArrowError::ParseError(format!("Arrow IPC stream is malformed: {detail}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, NullArray, RecordBatch};
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::{declared_message_header, declares_ipc_data, declares_record_batch};

    /// Encodes `batch` the way an IPC writer does and returns `(schema_header, batch_header,
    /// batch_body)`. Building the headers by hand would let the test agree with the code about a
    /// layout neither shares with a real writer.
    fn encode(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let generator = IpcDataGenerator::default();
        let options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);

        let schema = generator.schema_to_bytes_with_dictionary_tracker(
            &batch.schema(),
            &mut tracker,
            &options,
        );
        let (dictionaries, encoded) = generator
            .encode(
                batch,
                &mut tracker,
                &options,
                &mut IpcWriteContext::default(),
            )
            .expect("encoding a batch");
        assert!(
            dictionaries.is_empty(),
            "these fixtures are not dictionary-encoded"
        );

        (schema.ipc_message, encoded.ipc_message, encoded.arrow_data)
    }

    fn int_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("building an int batch")
    }

    /// A batch whose only column needs no buffers. Arrow encodes it with an empty body even
    /// though it carries rows, which is the case a body-length discriminator reads as no batch.
    fn null_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(rows)) as ArrayRef])
            .expect("building a null batch")
    }

    #[test]
    fn a_schema_message_declares_a_schema() {
        let (schema_header, _, _) = encode(&int_batch());
        assert_eq!(
            declared_message_header(&schema_header),
            Ok(Some(arrow_ipc::MessageHeader::Schema))
        );
        assert_eq!(declares_record_batch(&schema_header), Ok(false));
        assert_eq!(declares_ipc_data(&schema_header), Ok(false));
    }

    #[test]
    fn a_batch_message_declares_a_record_batch() {
        let (_, batch_header, _) = encode(&int_batch());
        assert_eq!(
            declared_message_header(&batch_header),
            Ok(Some(arrow_ipc::MessageHeader::RecordBatch))
        );
        assert_eq!(declares_record_batch(&batch_header), Ok(true));
        assert_eq!(declares_ipc_data(&batch_header), Ok(true));
    }

    /// The case the body length gets wrong: rows present, body empty.
    #[test]
    fn a_buffer_free_batch_declares_a_record_batch_with_an_empty_body() {
        let batch = null_batch(3);
        let (_, batch_header, batch_body) = encode(&batch);

        assert_eq!(batch.num_rows(), 3);
        assert!(
            batch_body.is_empty(),
            "a batch whose columns need no buffers is expected to encode with an empty body; without that this case does not exercise the confusion"
        );
        assert_eq!(declares_record_batch(&batch_header), Ok(true));
    }

    /// A zero-row batch also encodes with an empty body, and it too is a record batch.
    #[test]
    fn a_zero_row_batch_declares_a_record_batch_with_an_empty_body() {
        let (_, batch_header, batch_body) = encode(&RecordBatch::new_empty(int_batch().schema()));

        assert!(batch_body.is_empty());
        assert_eq!(declares_record_batch(&batch_header), Ok(true));
    }

    /// Flight allows a message with no header at all. There is nothing there to misread, so it
    /// declares nothing rather than failing.
    #[test]
    fn no_header_declares_nothing() {
        assert_eq!(declared_message_header(&[]), Ok(None));
        assert_eq!(declares_record_batch(&[]), Ok(false));
        assert_eq!(declares_ipc_data(&[]), Ok(false));
    }

    /// A header with bytes that will not parse is a malformed stream, not the absence of a
    /// batch: reporting it as `false` would name the wrong problem.
    #[test]
    fn an_unparseable_header_is_an_error_not_an_absence() {
        let garbage = [0xff_u8; 8];
        // The root offset points far past the eight bytes, so the flatbuffer verifier refuses
        // it. Both predicates must pass that exact failure through rather than substitute
        // their own message or report `Ok(false)`.
        let refused = "Type `i32` at position 4294967295 is unaligned.\n\n".to_string();
        assert_eq!(declared_message_header(&garbage), Err(refused.clone()));
        assert_eq!(declares_record_batch(&garbage), Err(refused.clone()));
        assert_eq!(declares_ipc_data(&garbage), Err(refused));
    }

    /// A truncated header is the shape a keepalive-sized message has: fewer bytes than the
    /// flatbuffer root needs.
    #[test]
    fn a_truncated_header_is_an_error() {
        let truncated = [0x01_u8, 0x02];
        // The four-byte root offset does not fit in two bytes.
        let refused = "Range [0, 4) is out of bounds.\n\n".to_string();
        assert_eq!(declared_message_header(&truncated), Err(refused.clone()));
        assert_eq!(declares_record_batch(&truncated), Err(refused.clone()));
        assert_eq!(declares_ipc_data(&truncated), Err(refused));
    }

    mod framing {
        use std::ops::Range;

        use arrow::array::RecordBatch;
        use arrow_ipc::CompressionType;
        use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

        use super::super::{StreamFraming, check_stream_framing};
        use super::int_batch;
        use crate::map_entries::{Error, read_ipc_stream};

        /// Declared far beyond any memory this test runs with: a reader that allocates it before
        /// reading fails the allocation rather than the read.
        const OVER_DECLARED: i64 = 500_000_000_000;

        fn stream(batches: &[RecordBatch], options: IpcWriteOptions) -> Vec<u8> {
            let mut bytes = Vec::new();
            let mut writer =
                StreamWriter::try_new_with_options(&mut bytes, &int_batch().schema(), options)
                    .expect("stream writer");
            for batch in batches {
                writer.write(batch).expect("write batch");
            }
            writer.finish().expect("finish stream");
            drop(writer);
            bytes
        }

        /// Where each message's metadata flatbuffer lies, walking the framing the writer used
        /// (continuation marker, length, metadata, body) up to the end-of-stream marker.
        fn metadata_ranges(bytes: &[u8]) -> Vec<Range<usize>> {
            let mut ranges = Vec::new();
            let mut at = 0;
            loop {
                assert_eq!(
                    bytes[at..at + 4],
                    [0xff; 4],
                    "writer emits continuation markers"
                );
                let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("4 bytes"));
                if len == 0 {
                    return ranges;
                }
                let metadata = at + 8..at + 8 + len as usize;
                let body = arrow_ipc::root_as_message(&bytes[metadata.clone()])
                    .expect("writer metadata parses")
                    .bodyLength();
                at = metadata.end + usize::try_from(body).expect("non-negative body");
                ranges.push(metadata);
            }
        }

        /// Rewrites the `bodyLength` message `index` declares, leaving every other byte alone.
        ///
        /// The field is found by value rather than by flatbuffer offset: each occurrence of the
        /// current length is tried, and the one that changes what the parsed message reports is
        /// the field.
        fn with_declared_body_length(bytes: &[u8], index: usize, declared: i64) -> Vec<u8> {
            let metadata = metadata_ranges(bytes)[index].clone();
            let current = arrow_ipc::root_as_message(&bytes[metadata.clone()])
                .expect("metadata parses")
                .bodyLength()
                .to_le_bytes();
            for at in metadata.start..=metadata.end - current.len() {
                if bytes[at..at + current.len()] != current {
                    continue;
                }
                let mut patched = bytes.to_vec();
                patched[at..at + current.len()].copy_from_slice(&declared.to_le_bytes());
                if arrow_ipc::root_as_message(&patched[metadata.clone()])
                    .is_ok_and(|message| message.bodyLength() == declared)
                {
                    return patched;
                }
            }
            panic!("message {index} has no bodyLength field holding its body length");
        }

        fn refusal(bytes: &[u8]) -> String {
            check_stream_framing(bytes)
                .expect_err("the framing must be refused")
                .to_string()
        }

        #[test]
        fn a_written_stream_passes_with_every_message_counted() {
            let batch = int_batch();
            assert_eq!(
                check_stream_framing(&stream(&[], IpcWriteOptions::default())).expect("framing"),
                StreamFraming {
                    messages: 1,
                    compressed: false
                }
            );
            assert_eq!(
                check_stream_framing(&stream(&[batch.clone(), batch], IpcWriteOptions::default()))
                    .expect("framing"),
                StreamFraming {
                    messages: 3,
                    compressed: false
                }
            );
        }

        #[test]
        fn empty_and_end_marker_only_inputs_are_empty_streams() {
            let empty = StreamFraming {
                messages: 0,
                compressed: false,
            };
            assert_eq!(check_stream_framing(&[]).expect("framing"), empty);
            assert_eq!(
                check_stream_framing(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]).expect("framing"),
                empty
            );
        }

        #[test]
        fn a_compressed_batch_is_reported() {
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .expect("lz4 is available");
            assert_eq!(
                check_stream_framing(&stream(&[int_batch()], options)).expect("framing"),
                StreamFraming {
                    messages: 2,
                    compressed: true
                }
            );
        }

        #[test]
        fn a_body_longer_than_the_stream_is_refused() {
            let written = stream(&[int_batch()], IpcWriteOptions::default());
            // Measured on the unpatched stream: patching the declared length does not move the
            // body, and walking the patched stream would follow the declared length off its end.
            let body_at = metadata_ranges(&written)[1].end;
            let bytes = with_declared_body_length(&written, 1, OVER_DECLARED);
            let remaining = bytes.len() - body_at;
            assert_eq!(
                refusal(&bytes),
                format!(
                    "Parser error: Arrow IPC stream is malformed: message 1 declares a {OVER_DECLARED} byte body, but only {remaining} bytes remain"
                )
            );
        }

        #[test]
        fn a_body_one_byte_longer_than_the_stream_is_refused_and_an_exact_fit_passes() {
            let written = stream(&[int_batch()], IpcWriteOptions::default());
            let batch_metadata = metadata_ranges(&written)[1].clone();
            let body = arrow_ipc::root_as_message(&written[batch_metadata.clone()])
                .expect("metadata parses")
                .bodyLength();
            // The batch body is followed by the 8-byte end-of-stream marker, so a body declared
            // 8 bytes longer ends exactly at the end of the input, and 9 bytes longer runs past it.
            let exact = with_declared_body_length(&written, 1, body + 8);
            assert_eq!(
                check_stream_framing(&exact).expect("an exact fit is in bounds"),
                StreamFraming {
                    messages: 2,
                    compressed: false
                }
            );
            let over = with_declared_body_length(&written, 1, body + 9);
            assert_eq!(
                refusal(&over),
                format!(
                    "Parser error: Arrow IPC stream is malformed: message 1 declares a {} byte body, but only {} bytes remain",
                    body + 9,
                    body + 8
                )
            );
        }

        #[test]
        fn a_negative_body_length_is_refused() {
            let bytes = with_declared_body_length(
                &stream(&[int_batch()], IpcWriteOptions::default()),
                1,
                -1,
            );
            assert!(
                refusal(&bytes).contains("message 1 declares a -1 byte body"),
                "{}",
                refusal(&bytes)
            );
        }

        /// A schema message omits a zero `bodyLength`, so the first message that carries the
        /// field is the batch. Slicing the stream to start there makes that batch message 0.
        #[test]
        fn a_first_message_body_longer_than_the_stream_is_refused() {
            let written = stream(&[int_batch()], IpcWriteOptions::default());
            let batch = metadata_ranges(&written)[1].clone();
            let start = batch.start - 8;
            let bytes = with_declared_body_length(&written, 1, OVER_DECLARED)[start..].to_vec();
            let remaining = bytes.len() - (batch.end - start);
            assert_eq!(
                refusal(&bytes),
                format!(
                    "Parser error: Arrow IPC stream is malformed: message 0 declares a {OVER_DECLARED} byte body, but only {remaining} bytes remain"
                )
            );
        }

        #[test]
        fn metadata_longer_than_the_stream_is_refused() {
            let mut bytes = stream(&[int_batch()], IpcWriteOptions::default());
            let remaining = bytes.len() - 8;
            bytes[4..8].copy_from_slice(&i32::MAX.to_le_bytes());
            assert_eq!(
                refusal(&bytes),
                format!(
                    "Parser error: Arrow IPC stream is malformed: message 0 declares {} bytes of metadata, but only {remaining} bytes remain",
                    i32::MAX
                )
            );
        }

        #[test]
        fn a_negative_metadata_length_is_refused() {
            let mut bytes = stream(&[int_batch()], IpcWriteOptions::default());
            bytes[4..8].copy_from_slice(&(-8_i32).to_le_bytes());
            assert!(
                refusal(&bytes).contains("message 0 declares -8 bytes of metadata"),
                "{}",
                refusal(&bytes)
            );
        }

        #[test]
        fn a_stream_ending_inside_a_length_prefix_is_refused() {
            let bytes = stream(&[int_batch()], IpcWriteOptions::default());
            let cut = metadata_ranges(&bytes)[1].start - 2;
            assert_eq!(
                refusal(&bytes[..cut]),
                "Parser error: Arrow IPC stream is malformed: message 1 ends inside its metadata length"
            );
        }

        /// The decode itself must refuse the over-declared body rather than allocate it: without
        /// the framing check, `StreamReader` zero-fills a buffer of the declared size before
        /// reading, and an allocation this size aborts the process.
        #[test]
        fn read_ipc_stream_refuses_an_over_declared_body_before_allocating_it() {
            let bytes = with_declared_body_length(
                &stream(&[int_batch()], IpcWriteOptions::default()),
                1,
                OVER_DECLARED,
            );
            match read_ipc_stream(&bytes) {
                Err(Error::UndecodableStream { source }) => assert!(
                    source
                        .to_string()
                        .contains(&format!("declares a {OVER_DECLARED} byte body")),
                    "{source}"
                ),
                other => panic!("expected an undecodable stream, got {other:?}"),
            }
        }

        #[test]
        fn read_ipc_stream_still_decodes_a_written_stream() {
            let batch = int_batch();
            let read_back = read_ipc_stream(&stream(
                &[batch.clone(), batch.clone()],
                IpcWriteOptions::default(),
            ))
            .expect("a written stream decodes");
            assert_eq!(read_back, vec![batch.clone(), batch]);
        }
    }
}
