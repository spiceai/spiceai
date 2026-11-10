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
//! On the sending side, [`split_batch_for_flight`] bounds how much one Flight record-batch
//! message carries, so every Flight encoder splits a large batch the same way.

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

/// Largest Arrow data, in bytes, that one Flight record-batch message should carry.
///
/// gRPC clients reject a message over their receive limit, and most default it to 4 MiB
/// (tonic, grpc-go, grpc-java). Encoding each [`RecordBatch`] as a single message means a
/// large batch — an accelerator scan that yields tens of thousands of rows, a sort that
/// coalesces its output, an engine that hands back its whole result at once — becomes one
/// message the client cannot decode. This matches `arrow-flight`'s own
/// `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES`, the target `FlightDataEncoderBuilder` splits to, which
/// leaves room for the IPC header and padding below the 4 MiB default.
pub const FLIGHT_TARGET_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Split `batch` into zero-copy row slices of at most `max_bytes` of Arrow data each, so
/// that each slice can be sent as its own Flight message.
///
/// Sizes count only the part of each buffer a (possibly already sliced) batch references,
/// including the child values a list, large list, map or fixed-size list slice points at,
/// so a narrow slice of a large decoded chunk, or of a column of lists, is not split further
/// than its own rows need. Dictionary values are not counted, because they go out in their
/// own message rather than with the batch. Rows are first spread evenly
/// across as many slices as the batch's size calls for, and any slice still over `max_bytes`
/// (because its rows are wider than the batch's average) is split again the same way. A
/// batch at or under `max_bytes`, a batch with at most one row, and a `max_bytes` of zero
/// are returned unchanged: a single row is the smallest unit Flight can send, so a row
/// larger than the target still goes out as one message.
///
/// Dictionary values are not sliced: every slice of a dictionary-encoded column shares the
/// column's whole dictionary, which is sent once.
///
/// Slices share the batch's buffers, except that a slice's string and binary view columns
/// (at any nesting depth) are copied into buffers holding only its own values when the
/// slice's buffers add up to more than `max_bytes`, because the IPC writer sends view data buffers
/// whole.
#[must_use]
pub fn split_batch_for_flight(
    batch: arrow::record_batch::RecordBatch,
    max_bytes: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut slices = Vec::new();
    split_into(batch, max_bytes, &mut slices);
    if max_bytes > 0 {
        for slice in &mut slices {
            compact_view_columns(slice, max_bytes);
        }
    }
    slices
}

/// Rewrites, in place, each column of `slice` that holds string or binary views, so it keeps
/// only the view data its own rows use, when the buffers of all of `slice`'s columns add up
/// to more than `max_bytes`.
///
/// The IPC writer sends a view array's data buffers whole, so a slice of a view column, or
/// of a struct or list holding one, would otherwise carry every value the parent batch
/// held. The gate is on the whole slice, not each column: several view columns that each
/// fit can still add up to a message over a client's limit. Batches without views, and
/// slices whose buffers already fit, pay nothing.
fn compact_view_columns(slice: &mut arrow::record_batch::RecordBatch, max_bytes: usize) {
    use arrow::array::Array;

    let has_views = |column: &arrow::array::ArrayRef| contains_view(column.data_type());
    if !slice.columns().iter().any(has_views) {
        return;
    }
    let retained = slice.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(column.get_array_memory_size())
    });
    if retained <= max_bytes {
        return;
    }
    let columns = slice
        .columns()
        .iter()
        .map(|column| {
            if has_views(column) {
                compact_views(column)
            } else {
                std::sync::Arc::clone(column)
            }
        })
        .collect();
    if let Ok(compacted) = arrow::record_batch::RecordBatch::try_new(slice.schema(), columns) {
        *slice = compacted;
    }
}

fn contains_view(data_type: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;

    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_view(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_view(f.data_type())),
        _ => false,
    }
}

/// `array` with its string and binary views, at any depth of struct, list, large list,
/// fixed-size list and map nesting, copied into buffers that hold only the values `array`
/// references. Any part it cannot rebuild is returned as it was.
fn compact_views(array: &arrow::array::ArrayRef) -> arrow::array::ArrayRef {
    use arrow::array::{Array, AsArray, FixedSizeListArray, MapArray, StructArray};
    use arrow_schema::DataType;
    use std::sync::Arc;

    if !contains_view(array.data_type()) {
        return Arc::clone(array);
    }
    let rebuilt: Option<arrow::array::ArrayRef> = match array.data_type() {
        DataType::Utf8View => Some(Arc::new(array.as_string_view().gc())),
        DataType::BinaryView => Some(Arc::new(array.as_binary_view().gc())),
        DataType::Struct(fields) => {
            let parent = array.as_struct();
            let children = parent.columns().iter().map(compact_views).collect();
            StructArray::try_new(fields.clone(), children, parent.nulls().cloned())
                .ok()
                .map(|a| Arc::new(a) as _)
        }
        DataType::List(field) => compact_list::<i32>(array, field),
        DataType::LargeList(field) => compact_list::<i64>(array, field),
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                compact_views(list.values()),
                list.nulls().cloned(),
            )
            .ok()
            .map(|a| Arc::new(a) as _)
        }
        DataType::Map(field, ordered) => {
            let map = array.as_map();
            let (offsets, entries) =
                referenced_values(map.value_offsets(), &(Arc::new(map.entries().clone()) as _));
            entries.and_then(|entries| {
                let entries = compact_views(&entries);
                MapArray::try_new(
                    Arc::clone(field),
                    offsets,
                    entries.as_struct().clone(),
                    map.nulls().cloned(),
                    *ordered,
                )
                .ok()
                .map(|a| Arc::new(a) as _)
            })
        }
        _ => None,
    };
    rebuilt.unwrap_or_else(|| Arc::clone(array))
}

fn compact_list<O: arrow::array::OffsetSizeTrait>(
    array: &arrow::array::ArrayRef,
    field: &arrow_schema::FieldRef,
) -> Option<arrow::array::ArrayRef> {
    use arrow::array::{Array, AsArray, GenericListArray};
    use std::sync::Arc;

    let list = array.as_list::<O>();
    let (offsets, values) = referenced_values(list.value_offsets(), list.values());
    let values = compact_views(&values?);
    GenericListArray::<O>::try_new(Arc::clone(field), offsets, values, list.nulls().cloned())
        .ok()
        .map(|a| Arc::new(a) as _)
}

/// The part of `values` that `offsets` points at, and `offsets` rebased to start at zero.
fn referenced_values<O: arrow::array::OffsetSizeTrait>(
    offsets: &[O],
    values: &arrow::array::ArrayRef,
) -> (
    arrow::buffer::OffsetBuffer<O>,
    Option<arrow::array::ArrayRef>,
) {
    let start = offsets.first().copied().unwrap_or_else(O::zero);
    let end = offsets.last().copied().unwrap_or_else(O::zero);
    let rebased: Vec<O> = offsets.iter().map(|&o| o - start).collect();
    let rebased = if rebased.is_empty() {
        arrow::buffer::OffsetBuffer::new_empty()
    } else {
        arrow::buffer::OffsetBuffer::new(rebased.into())
    };
    let values = start
        .to_usize()
        .zip(end.to_usize())
        .filter(|(start, end)| start <= end && *end <= values.len())
        .map(|(start, end)| values.slice(start, end - start));
    (rebased, values)
}

fn split_into(
    batch: arrow::record_batch::RecordBatch,
    max_bytes: usize,
    slices: &mut Vec<arrow::record_batch::RecordBatch>,
) {
    let num_rows = batch.num_rows();
    if max_bytes == 0 || num_rows <= 1 {
        slices.push(batch);
        return;
    }
    let size = batch.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(referenced_bytes(&column.to_data()))
    });
    if size <= max_bytes {
        slices.push(batch);
        return;
    }
    // `size > max_bytes` and `num_rows >= 2`, so `pieces >= 2` and every piece has at most
    // half the rows (rounded up): each recursion level shrinks the slices, so the depth is
    // bounded by `log2(num_rows)`.
    let pieces = size.div_ceil(max_bytes).min(num_rows);
    let rows_per_piece = num_rows.div_ceil(pieces);
    let mut offset = 0;
    while offset < num_rows {
        let length = rows_per_piece.min(num_rows - offset);
        split_into(batch.slice(offset, length), max_bytes, slices);
        offset += length;
    }
}

/// Bytes of Arrow data `data` references, as the body of an IPC record-batch message for it
/// carries them.
///
/// [`ArrayData::get_slice_memory_size`](arrow::array::ArrayData::get_slice_memory_size)
/// narrows a slice's own buffers but counts a list's whole child array, so every slice of a
/// list column would measure as large as the column. This narrows list, large list, map and
/// fixed-size list children to the range the slice's offsets point at, and counts only the
/// keys of a dictionary-encoded column. Anything it cannot narrow is measured the way
/// `get_slice_memory_size` measures it, which can only overstate the size.
fn referenced_bytes(data: &arrow::array::ArrayData) -> usize {
    use arrow_schema::DataType;

    let len = data.len();
    let validity = data.nulls().map_or(0, |_| len.div_ceil(8));
    match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            list_referenced_bytes(data, validity, 4, |buffer, i| {
                buffer
                    .typed_data::<i32>()
                    .get(i)
                    .and_then(|&o| usize::try_from(o).ok())
            })
        }
        DataType::LargeList(_) => list_referenced_bytes(data, validity, 8, |buffer, i| {
            buffer
                .typed_data::<i64>()
                .get(i)
                .and_then(|&o| usize::try_from(o).ok())
        }),
        DataType::FixedSizeList(_, size) => {
            let size = usize::try_from(*size).unwrap_or(0);
            match data.child_data().first() {
                Some(child) => {
                    let start = data.offset().saturating_mul(size);
                    let count = len.saturating_mul(size);
                    let values = if start.saturating_add(count) <= child.len() {
                        referenced_bytes(&child.slice(start, count))
                    } else {
                        referenced_bytes(child)
                    };
                    validity.saturating_add(values)
                }
                None => unnarrowed_bytes(data),
            }
        }
        // `ArrayData::slice` and `StructArray::slice` both slice a struct's children.
        DataType::Struct(_) => data.child_data().iter().fold(validity, |total, child| {
            total.saturating_add(referenced_bytes(child))
        }),
        // The IPC writer sends a view column's data buffers whole; `split_batch_for_flight`
        // compacts each slice's views, so count only the values this slice references.
        DataType::Utf8View | DataType::BinaryView => {
            let views = data
                .buffers()
                .first()
                .map(arrow::buffer::Buffer::typed_data::<u128>)
                .and_then(|views| views.get(data.offset()..data.offset().saturating_add(len)));
            match views {
                Some(views) => views.iter().fold(
                    validity.saturating_add(len.saturating_mul(16)),
                    |total, &view| {
                        // The low 32 bits of a view are its value's length; values of up to
                        // 12 bytes are stored inline in the view itself.
                        let value_len =
                            u32::try_from(view & u128::from(u32::MAX)).unwrap_or(u32::MAX) as usize;
                        if value_len > 12 {
                            total.saturating_add(value_len)
                        } else {
                            total
                        }
                    },
                ),
                None => unnarrowed_bytes(data),
            }
        }
        DataType::Dictionary(key, _) => {
            validity.saturating_add(len.saturating_mul(key.primitive_width().unwrap_or(8)))
        }
        _ => unnarrowed_bytes(data),
    }
}

/// Bytes of a list-like `data` with `offset_width`-byte offsets: its offsets and validity,
/// plus the child values between its first and last offset.
fn list_referenced_bytes(
    data: &arrow::array::ArrayData,
    validity: usize,
    offset_width: usize,
    offset_at: impl Fn(&arrow::buffer::Buffer, usize) -> Option<usize>,
) -> usize {
    let len = data.len();
    let (Some(buffer), Some(child)) = (data.buffers().first(), data.child_data().first()) else {
        return unnarrowed_bytes(data);
    };
    let start = offset_at(buffer, data.offset());
    let end = offset_at(buffer, data.offset().saturating_add(len));
    match (start, end) {
        (Some(start), Some(end)) if start <= end && end <= child.len() => {
            let offsets = len.saturating_add(1).saturating_mul(offset_width);
            validity
                .saturating_add(offsets)
                .saturating_add(referenced_bytes(&child.slice(start, end - start)))
        }
        _ => unnarrowed_bytes(data),
    }
}

fn unnarrowed_bytes(data: &arrow::array::ArrayData) -> usize {
    use arrow::array::Array;

    data.get_slice_memory_size()
        .unwrap_or_else(|_| arrow::array::make_array(data.clone()).get_array_memory_size())
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

#[cfg(test)]
mod split_batch_for_flight_tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, LargeStringArray, ListArray, RecordBatch};
    use arrow::compute::concat_batches;
    use arrow::datatypes::Int64Type;
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::{FLIGHT_TARGET_MESSAGE_BYTES, split_batch_for_flight};
    use crate::record_batch::slice_memory_size;

    /// The 4 MiB receive limit tonic, grpc-go and grpc-java clients apply by default.
    const CLIENT_DEFAULT_DECODE_LIMIT: usize = 4 * 1024 * 1024;

    /// `rows` rows of an id, a `row_bytes`-wide string and a 4-element list, about
    /// `rows * row_bytes` bytes in all.
    fn wide_batch(rows: usize, row_bytes: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::LargeUtf8, false),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                true,
            ),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| i64::try_from(i).unwrap_or(i64::MAX)),
        ));
        let pad = "x".repeat(row_bytes.saturating_sub(9));
        let strings: ArrayRef = Arc::new(LargeStringArray::from_iter_values(
            (0..rows).map(|i| format!("{i:09}{pad}")),
        ));
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..rows).map(|i| {
                let i = i64::try_from(i).unwrap_or(i64::MAX);
                Some(vec![Some(i), Some(i + 1), None, Some(i + 3)])
            }),
        ));
        RecordBatch::try_new(schema, vec![ids, strings, lists]).expect("valid test batch")
    }

    /// Bytes of the Flight message `batch` encodes to: IPC header plus body.
    fn encoded_message_bytes(batch: &RecordBatch) -> usize {
        let (_, encoded) = IpcDataGenerator::default()
            .encode(
                batch,
                &mut DictionaryTracker::new(false),
                &IpcWriteOptions::default(),
                &mut IpcWriteContext::default(),
            )
            .expect("encode test batch");
        encoded.ipc_message.len() + encoded.arrow_data.len()
    }

    /// A batch whose single message would exceed the client default (the reported
    /// 7.6 MB `SELECT *` message) is split into messages under it, every row kept in
    /// order.
    #[test]
    fn large_batch_splits_into_messages_under_the_client_limit() {
        let batch = wide_batch(32_768, 240);
        assert!(
            encoded_message_bytes(&batch) > CLIENT_DEFAULT_DECODE_LIMIT,
            "fixture must reproduce an over-limit message"
        );

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert!(slices.len() > 1);
        for slice in &slices {
            let bytes = encoded_message_bytes(slice);
            assert!(
                bytes <= CLIENT_DEFAULT_DECODE_LIMIT,
                "slice of {} rows encodes to {bytes} bytes",
                slice.num_rows()
            );
        }
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat slices");
        assert_eq!(rejoined, batch);
    }

    /// A narrow slice of a large parent is sized by the rows it covers, not by the
    /// parent buffers it shares, so it is not split needlessly.
    #[test]
    fn narrow_slice_of_large_parent_is_not_split() {
        let parent = wide_batch(32_768, 240);
        let slice = parent.slice(100, 1_000);
        assert!(slice.get_array_memory_size() > FLIGHT_TARGET_MESSAGE_BYTES);

        let slices = split_batch_for_flight(slice.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0], slice);
    }

    /// Small batches, empty batches, single rows and a zero target pass through as-is.
    #[test]
    fn batches_that_cannot_or_need_not_split_are_unchanged() {
        let small = wide_batch(100, 64);
        assert_eq!(
            split_batch_for_flight(small.clone(), FLIGHT_TARGET_MESSAGE_BYTES),
            vec![small.clone()]
        );
        assert_eq!(split_batch_for_flight(small.clone(), 0), vec![small]);

        let empty = wide_batch(0, 64);
        assert_eq!(
            split_batch_for_flight(empty.clone(), FLIGHT_TARGET_MESSAGE_BYTES),
            vec![empty]
        );

        // One row wider than the target still goes out — as one message.
        let one_huge_row = wide_batch(1, FLIGHT_TARGET_MESSAGE_BYTES * 2);
        let slices = split_batch_for_flight(one_huge_row.clone(), FLIGHT_TARGET_MESSAGE_BYTES);
        assert_eq!(slices, vec![one_huge_row]);
    }

    /// Rows spread evenly: no slice is more than one row longer than another, and
    /// there is no tiny trailing slice.
    #[test]
    fn rows_spread_evenly_across_slices() {
        let batch = wide_batch(10_001, 1_024);
        let slices = split_batch_for_flight(batch, FLIGHT_TARGET_MESSAGE_BYTES);
        let lengths: Vec<usize> = slices.iter().map(RecordBatch::num_rows).collect();
        let max = lengths.iter().copied().max().unwrap_or_default();
        let min = lengths.iter().copied().min().unwrap_or_default();
        assert_eq!(lengths.iter().sum::<usize>(), 10_001);
        assert!(max - min < slices.len(), "uneven slices: {lengths:?}");
        assert!(slices.iter().all(|s| s.column(0).len() == s.num_rows()));
    }

    /// Rows wider than the batch's average, clustered together, are split again rather than
    /// sent as one oversized slice: six 1 MiB strings followed by 94 one-byte strings would
    /// put all six wide rows in the first of four even slices.
    #[test]
    fn clustered_wide_rows_are_split_until_each_slice_fits() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::LargeUtf8,
            false,
        )]));
        let wide = "x".repeat(1024 * 1024);
        let values: Vec<&str> = std::iter::repeat_n(wide.as_str(), 6)
            .chain(std::iter::repeat_n("x", 94))
            .collect();
        let column: ArrayRef = Arc::new(LargeStringArray::from(values));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("batch");

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        for slice in &slices {
            assert!(
                slice.num_rows() == 1 || slice_memory_size(slice) <= FLIGHT_TARGET_MESSAGE_BYTES,
                "a {}-row slice of {} bytes is over the target",
                slice.num_rows(),
                slice_memory_size(slice)
            );
            assert!(encoded_message_bytes(slice) <= CLIENT_DEFAULT_DECODE_LIMIT);
        }
        assert_eq!(slices.iter().map(RecordBatch::num_rows).sum::<usize>(), 100);
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat");
        assert_eq!(rejoined, batch);
    }

    /// A column of lists is sized by the child values each slice points at, not by the
    /// whole child array every slice shares: a batch just over the target splits into a
    /// couple of slices, not one per row.
    #[test]
    fn list_column_is_split_by_the_values_each_slice_references() {
        let rows: i64 = 8_192;
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..rows).map(|row| Some((0..40).map(move |i| Some(row * 40 + i)))),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            lists.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![lists]).expect("batch");
        assert!(
            encoded_message_bytes(&batch) > FLIGHT_TARGET_MESSAGE_BYTES,
            "fixture must be over the target"
        );

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert!(
            (2..=3).contains(&slices.len()),
            "expected 2 or 3 slices, got {}",
            slices.len()
        );
        for slice in &slices {
            assert!(encoded_message_bytes(slice) <= CLIENT_DEFAULT_DECODE_LIMIT);
        }
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat");
        assert_eq!(rejoined, batch);
    }

    /// A struct column of string views is split, and each slice's views are compacted so
    /// it does not carry every value the parent batch held: the IPC writer sends a view
    /// array's data buffers whole.
    #[test]
    fn nested_string_views_are_split_and_compacted() {
        use arrow::array::{StringViewArray, StructArray};

        let value = "x".repeat(1_024);
        let views: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            (0..8_192).map(|_| value.as_str()),
        ));
        let field = Arc::new(Field::new("s", DataType::Utf8View, true));
        let structs: ArrayRef = Arc::new(StructArray::from(vec![(field, views)]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "st",
            structs.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![structs]).expect("batch");
        assert!(
            encoded_message_bytes(&batch) > CLIENT_DEFAULT_DECODE_LIMIT,
            "fixture must be over the client limit"
        );

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert!(
            (4..=6).contains(&slices.len()),
            "expected 4 to 6 slices, got {}",
            slices.len()
        );
        for slice in &slices {
            let bytes = encoded_message_bytes(slice);
            assert!(
                bytes <= CLIENT_DEFAULT_DECODE_LIMIT,
                "a {}-row slice encodes to {bytes} bytes",
                slice.num_rows()
            );
        }
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat");
        assert_eq!(rejoined, batch);
    }

    /// Slicing a list column of string views keeps only the referenced values.
    #[test]
    fn list_of_string_views_is_compacted() {
        use arrow::array::{GenericListArray, StringViewArray};
        use arrow::buffer::OffsetBuffer;

        let value = "y".repeat(512);
        let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            (0..16_384).map(|_| value.as_str()),
        ));
        let item = Arc::new(Field::new("item", DataType::Utf8View, true));
        let lists: ArrayRef = Arc::new(GenericListArray::<i32>::new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2, 8_192)),
            values,
            None,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            lists.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![lists]).expect("batch");

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert!(
            slices.len() > 1 && slices.len() <= 8,
            "got {} slices",
            slices.len()
        );
        for slice in &slices {
            assert!(encoded_message_bytes(slice) <= CLIENT_DEFAULT_DECODE_LIMIT);
        }
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat");
        assert_eq!(rejoined, batch);
    }

    /// Several struct-of-view columns that each fit under the target, but together do not,
    /// are still compacted in every slice.
    #[test]
    fn view_columns_that_only_add_up_past_the_target_are_compacted() {
        use arrow::array::{StringViewArray, StructArray};

        let value = "z".repeat(1_024);
        let column = |name: &str| -> (Field, ArrayRef) {
            let views: ArrayRef = Arc::new(StringViewArray::from_iter_values(
                (0..1_536).map(|_| value.as_str()),
            ));
            let field = Arc::new(Field::new("s", DataType::Utf8View, true));
            let structs: ArrayRef = Arc::new(StructArray::from(vec![(field, views)]));
            (Field::new(name, structs.data_type().clone(), true), structs)
        };
        let (fields, columns): (Vec<_>, Vec<_>) = ["a", "b", "c"].into_iter().map(column).unzip();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");
        assert!(
            encoded_message_bytes(&batch) > CLIENT_DEFAULT_DECODE_LIMIT,
            "fixture must be over the client limit"
        );

        let slices = split_batch_for_flight(batch.clone(), FLIGHT_TARGET_MESSAGE_BYTES);

        assert!(slices.len() > 1, "got {} slices", slices.len());
        for slice in &slices {
            let bytes = encoded_message_bytes(slice);
            assert!(
                bytes <= FLIGHT_TARGET_MESSAGE_BYTES * 2,
                "a {}-row slice encodes to {bytes} bytes",
                slice.num_rows()
            );
        }
        let rejoined = concat_batches(&batch.schema(), &slices).expect("concat");
        assert_eq!(rejoined, batch);
    }
}
