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

/// Total decompressed size a compressed IPC message may declare regardless of its body length.
pub const DECOMPRESSED_SIZE_FLOOR: u64 = 64 * 1024 * 1024;

/// Largest ratio of declared decompressed size to body length accepted above
/// [`DECOMPRESSED_SIZE_FLOOR`].
pub const MAX_DECOMPRESSION_RATIO: u64 = 100;

/// Refuses a compressed IPC message whose buffers declare more decompressed bytes than
/// [`DECOMPRESSED_SIZE_FLOOR`] and more than [`MAX_DECOMPRESSION_RATIO`] times `data_body.len()`.
///
/// The Arrow IPC reader allocates each buffer's declared decompressed length before it
/// decompresses, so this has to run before the message reaches a decoder. Messages that are not
/// compressed record or dictionary batches, or whose header does not parse, are left to the
/// decoder.
///
/// # Errors
///
/// Returns a message naming the declared and received sizes when the bound is exceeded.
pub fn check_declared_decompressed_size(
    data_header: &[u8],
    data_body: &[u8],
) -> Result<(), String> {
    let Ok(message) = arrow_ipc::root_as_message(data_header) else {
        return Ok(());
    };
    let batch = match message.header_type() {
        arrow_ipc::MessageHeader::RecordBatch => message.header_as_record_batch(),
        arrow_ipc::MessageHeader::DictionaryBatch => message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data()),
        _ => None,
    };
    let Some(batch) = batch else {
        return Ok(());
    };
    if batch.compression().is_none() {
        return Ok(());
    }

    let declared = batch
        .buffers()
        .into_iter()
        .flatten()
        .filter(|buffer| buffer.length() >= 8)
        .filter_map(|buffer| {
            let start = usize::try_from(buffer.offset()).ok()?;
            let prefix = data_body.get(start..start.checked_add(8)?)?;
            u64::try_from(i64::from_le_bytes(prefix.try_into().ok()?)).ok()
        })
        .fold(0_u64, u64::saturating_add);

    if exceeds_decompression_bound(declared, data_body.len()) {
        return Err(format!(
            "compressed Arrow IPC message declares {declared} decompressed bytes from a {}-byte \
             body, more than {MAX_DECOMPRESSION_RATIO} times its size",
            data_body.len()
        ));
    }
    Ok(())
}

fn exceeds_decompression_bound(declared: u64, body_len: usize) -> bool {
    let body_len = u64::try_from(body_len).unwrap_or(u64::MAX);
    declared > DECOMPRESSED_SIZE_FLOOR
        && declared > body_len.saturating_mul(MAX_DECOMPRESSION_RATIO)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, NullArray, RecordBatch};
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::{
        DECOMPRESSED_SIZE_FLOOR, MAX_DECOMPRESSION_RATIO, check_declared_decompressed_size,
        declared_message_header, declares_ipc_data, declares_record_batch,
        exceeds_decompression_bound,
    };

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

    /// Encodes `batch` with `compression` and returns `(batch_header, batch_body)`.
    fn encode_compressed(
        batch: &RecordBatch,
        compression: arrow_ipc::CompressionType,
    ) -> (Vec<u8>, Vec<u8>) {
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(compression))
            .expect("compression options");
        let (_, encoded) = IpcDataGenerator::default()
            .encode(
                batch,
                &mut DictionaryTracker::new(false),
                &options,
                &mut IpcWriteContext::default(),
            )
            .expect("encoding a compressed batch");
        (encoded.ipc_message, encoded.arrow_data)
    }

    /// A single-column batch large enough that zstd compresses its values buffer.
    fn compressible_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![7; 4096])) as ArrayRef],
        )
        .expect("building a compressible batch")
    }

    /// Rewrites the declared-length prefixes in `body` so the message declares `declared`
    /// decompressed bytes in total: the values buffer carries all of it, the others none.
    fn with_declared_length(header: &[u8], body: &[u8], declared: u64) -> Vec<u8> {
        let message = arrow_ipc::root_as_message(header).expect("batch header");
        let batch = message.header_as_record_batch().expect("record batch");
        let mut body = body.to_vec();
        for (index, buffer) in batch.buffers().expect("buffers").iter().enumerate() {
            if buffer.length() < 8 {
                continue;
            }
            let offset = usize::try_from(buffer.offset()).expect("offset");
            let length: u64 = if index == 1 { declared } else { 0 };
            body[offset..offset + 8].copy_from_slice(&length.to_le_bytes());
        }
        body
    }

    #[test]
    fn compressed_batches_pass_the_guard_and_decode() {
        let batch = compressible_batch();
        for compression in [
            arrow_ipc::CompressionType::ZSTD,
            arrow_ipc::CompressionType::LZ4_FRAME,
        ] {
            let (header, body) = encode_compressed(&batch, compression);
            assert!(
                body.len() < 4096 * 4,
                "{compression:?} is expected to compress the values buffer"
            );
            assert_eq!(check_declared_decompressed_size(&header, &body), Ok(()));

            let message = arrow_ipc::root_as_message(&header).expect("batch header");
            let decoded = arrow_ipc::reader::read_record_batch(
                &arrow::buffer::Buffer::from(body),
                message.header_as_record_batch().expect("record batch"),
                batch.schema(),
                &std::collections::HashMap::new(),
                None,
                &message.version(),
            )
            .expect("decoding a compressed batch");
            assert_eq!(decoded, batch);
        }
    }

    #[test]
    fn uncompressed_batches_are_not_checked() {
        let (_, header, body) = encode(&int_batch());
        assert_eq!(check_declared_decompressed_size(&header, &body), Ok(()));
        assert_eq!(check_declared_decompressed_size(&[], &[]), Ok(()));
    }

    #[test]
    fn a_declared_length_at_the_floor_is_accepted() {
        let (header, body) =
            encode_compressed(&compressible_batch(), arrow_ipc::CompressionType::ZSTD);
        let body = with_declared_length(&header, &body, DECOMPRESSED_SIZE_FLOOR);
        assert_eq!(check_declared_decompressed_size(&header, &body), Ok(()));
    }

    #[test]
    fn a_declared_length_over_the_floor_and_ratio_is_refused() {
        let (header, body) =
            encode_compressed(&compressible_batch(), arrow_ipc::CompressionType::ZSTD);
        let body = with_declared_length(&header, &body, DECOMPRESSED_SIZE_FLOOR + 1);
        let error = check_declared_decompressed_size(&header, &body)
            .expect_err("a declared length over the bound is refused");
        assert!(
            error.contains(&format!(
                "declares {} decompressed bytes",
                DECOMPRESSED_SIZE_FLOOR + 1
            )),
            "{error}"
        );
    }

    #[test]
    fn the_ratio_bound_applies_above_the_floor() {
        let body_len = 1024 * 1024;
        let at_ratio = u64::try_from(body_len).expect("len") * MAX_DECOMPRESSION_RATIO;
        assert!(!exceeds_decompression_bound(at_ratio, body_len));
        assert!(exceeds_decompression_bound(at_ratio + 1, body_len));
        assert!(!exceeds_decompression_bound(DECOMPRESSED_SIZE_FLOOR, 0));
        assert!(exceeds_decompression_bound(DECOMPRESSED_SIZE_FLOOR + 1, 0));
    }
}
