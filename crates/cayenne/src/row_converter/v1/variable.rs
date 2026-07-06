/*
Copyright 2026 The Spice.ai OSS Authors

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

//! V1 variable-length codec for the byte/string family (`Binary`, `LargeBinary`, `Utf8`,
//! `LargeUtf8`, `BinaryView`, `Utf8View`).
//!
//! The byte layout is copied from Apache Arrow's `arrow-row` crate (Apache-2.0): a null is a
//! single sentinel byte, an empty value is a single `1` byte, and a non-empty value is a `2`
//! byte followed by 0-padded fixed-width blocks each terminated by a continuation byte (`0xFF`)
//! or the block's unpadded length. Descending order inverts all bytes.

use std::sync::Arc;

use arrow::array::builder::{
    BinaryBuilder, BinaryViewBuilder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    StringViewBuilder,
};
use arrow::array::types::{ByteArrayType, ByteViewType};
use arrow::array::{Array, ArrayRef, AsArray, GenericByteArray, GenericByteViewArray};
use arrow::datatypes::ArrowNativeType;
use arrow_schema::{ArrowError, SortOptions};

use crate::row_converter::codec::{ColumnCodec, LengthTracker, null_sentinel};

/// The block size of the variable length encoding.
const BLOCK_SIZE: usize = 32;
/// The first block is split into this many mini-blocks to reduce amplification for short values.
const MINI_BLOCK_COUNT: usize = 4;
/// The mini block size.
const MINI_BLOCK_SIZE: usize = BLOCK_SIZE / MINI_BLOCK_COUNT;
/// The continuation token.
const BLOCK_CONTINUATION: u8 = 0xFF;
/// Indicates an empty value.
const EMPTY_SENTINEL: u8 = 1;
/// Indicates a non-empty value.
const NON_EMPTY_SENTINEL: u8 = 2;

/// The byte/string variant this codec handles.
#[derive(Debug, Clone, Copy)]
pub(crate) enum VarKind {
    Binary,
    LargeBinary,
    Utf8,
    LargeUtf8,
    BinaryView,
    Utf8View,
}

/// Codec for the variable-length byte/string family.
#[derive(Debug)]
pub(crate) struct VariableCodec {
    kind: VarKind,
    options: SortOptions,
}

impl VariableCodec {
    pub(crate) fn new(kind: VarKind, options: SortOptions) -> Self {
        Self { kind, options }
    }
}

impl ColumnCodec for VariableCodec {
    fn append_lengths(&self, array: &dyn Array, tracker: &mut LengthTracker) {
        match self.kind {
            VarKind::Binary => push_byte_array_lengths(tracker, array.as_binary::<i32>()),
            VarKind::LargeBinary => push_byte_array_lengths(tracker, array.as_binary::<i64>()),
            VarKind::Utf8 => push_byte_array_lengths(tracker, array.as_string::<i32>()),
            VarKind::LargeUtf8 => push_byte_array_lengths(tracker, array.as_string::<i64>()),
            VarKind::BinaryView => push_byte_view_lengths(tracker, array.as_binary_view()),
            VarKind::Utf8View => push_byte_view_lengths(tracker, array.as_string_view()),
        }
    }

    fn encode(&self, data: &mut [u8], offsets: &mut [usize], array: &dyn Array) {
        let opts = self.options;
        match self.kind {
            VarKind::Binary => encode_byte_array(data, offsets, array.as_binary::<i32>(), opts),
            VarKind::LargeBinary => {
                encode_byte_array(data, offsets, array.as_binary::<i64>(), opts);
            }
            VarKind::Utf8 => encode_byte_array(data, offsets, array.as_string::<i32>(), opts),
            VarKind::LargeUtf8 => encode_byte_array(data, offsets, array.as_string::<i64>(), opts),
            VarKind::BinaryView => encode(data, offsets, array.as_binary_view().iter(), opts),
            VarKind::Utf8View => encode(
                data,
                offsets,
                array.as_string_view().iter().map(|x| x.map(str::as_bytes)),
                opts,
            ),
        }
    }

    fn decode(&self, rows: &mut [&[u8]], _validate_utf8: bool) -> Result<ArrayRef, ArrowError> {
        let opts = self.options;
        let array: ArrayRef = match self.kind {
            VarKind::Binary => {
                let mut builder = BinaryBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(&value),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            VarKind::LargeBinary => {
                let mut builder = LargeBinaryBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(&value),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            VarKind::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(bytes_to_str(value)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            VarKind::LargeUtf8 => {
                let mut builder = LargeStringBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(bytes_to_str(value)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            VarKind::BinaryView => {
                let mut builder = BinaryViewBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(&value),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            VarKind::Utf8View => {
                let mut builder = StringViewBuilder::new();
                for row in rows.iter_mut() {
                    match decode_one(row, opts) {
                        Some(value) => builder.append_value(bytes_to_str(value)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
        };
        Ok(array)
    }
}

fn bytes_to_str(bytes: Vec<u8>) -> Result<String, ArrowError> {
    String::from_utf8(bytes)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("invalid UTF-8 in decoded row: {e}")))
}

/// Returns the padded encoded length of a value of `len` bytes (1 for null).
#[inline]
fn padded_length(a: Option<usize>) -> usize {
    match a {
        Some(a) => non_null_padded_length(a),
        None => 1,
    }
}

/// Returns the padded encoded length of a non-null value of `len` bytes.
#[inline]
fn non_null_padded_length(len: usize) -> usize {
    if len <= BLOCK_SIZE {
        1 + ceil(len, MINI_BLOCK_SIZE) * (MINI_BLOCK_SIZE + 1)
    } else {
        MINI_BLOCK_COUNT + ceil(len, BLOCK_SIZE) * (BLOCK_SIZE + 1)
    }
}

#[inline]
fn ceil(value: usize, divisor: usize) -> usize {
    value.div_ceil(divisor)
}

fn push_byte_array_lengths<T: ByteArrayType>(
    tracker: &mut LengthTracker,
    array: &GenericByteArray<T>,
) {
    if let Some(nulls) = array.nulls().filter(|n| n.null_count() > 0) {
        tracker.push_variable(
            array
                .offsets()
                .lengths()
                .zip(nulls.iter())
                .map(|(length, is_valid)| if is_valid { Some(length) } else { None })
                .map(padded_length),
        );
    } else {
        tracker.push_variable(array.offsets().lengths().map(non_null_padded_length));
    }
}

fn push_byte_view_lengths<T: ByteViewType>(
    tracker: &mut LengthTracker,
    array: &GenericByteViewArray<T>,
) {
    if let Some(nulls) = array.nulls().filter(|n| n.null_count() > 0) {
        tracker.push_variable(
            array
                .lengths()
                .zip(nulls.iter())
                .map(|(length, is_valid)| {
                    if is_valid {
                        Some(length as usize)
                    } else {
                        None
                    }
                })
                .map(padded_length),
        );
    } else {
        tracker.push_variable(array.lengths().map(|len| padded_length(Some(len as usize))));
    }
}

/// Variable length values are encoded as a null/empty sentinel, or `2` followed by blocks.
fn encode<'a, I: Iterator<Item = Option<&'a [u8]>>>(
    data: &mut [u8],
    offsets: &mut [usize],
    i: I,
    opts: SortOptions,
) {
    for (offset, maybe_val) in offsets.iter_mut().skip(1).zip(i) {
        *offset += encode_one(&mut data[*offset..], maybe_val, opts);
    }
}

/// [`encode`] specialized for a contiguous byte array.
fn encode_byte_array<T: ByteArrayType>(
    data: &mut [u8],
    offsets: &mut [usize],
    input_array: &GenericByteArray<T>,
    opts: SortOptions,
) {
    let input_offsets = input_array.value_offsets();
    let bytes = input_array.values().as_slice();

    if let Some(null_buffer) = input_array.nulls().filter(|x| x.null_count() > 0) {
        let input_iter =
            input_offsets
                .windows(2)
                .zip(null_buffer.iter())
                .map(|(start_end, is_valid)| {
                    if is_valid {
                        let item_range = start_end[0].as_usize()..start_end[1].as_usize();
                        Some(&bytes[item_range])
                    } else {
                        None
                    }
                });
        encode(data, offsets, input_iter, opts);
    } else {
        let input_iter = input_offsets.windows(2).map(|start_end| {
            let item_range = start_end[0].as_usize()..start_end[1].as_usize();
            Some(&bytes[item_range])
        });
        encode(data, offsets, input_iter, opts);
    }
}

#[inline]
fn encode_one(out: &mut [u8], val: Option<&[u8]>, opts: SortOptions) -> usize {
    match val {
        None => {
            out[0] = null_sentinel(opts);
            1
        }
        Some([]) => {
            out[0] = if opts.descending {
                !EMPTY_SENTINEL
            } else {
                EMPTY_SENTINEL
            };
            1
        }
        Some(val) => {
            out[0] = NON_EMPTY_SENTINEL;
            let len = if val.len() <= BLOCK_SIZE {
                1 + encode_blocks::<MINI_BLOCK_SIZE>(&mut out[1..], val)
            } else {
                let (initial, rem) = val.split_at(BLOCK_SIZE);
                let offset = encode_blocks::<MINI_BLOCK_SIZE>(&mut out[1..], initial);
                out[offset] = BLOCK_CONTINUATION;
                1 + offset + encode_blocks::<BLOCK_SIZE>(&mut out[1 + offset..], rem)
            };
            if opts.descending {
                out[..len].iter_mut().for_each(|v| *v = !*v);
            }
            len
        }
    }
}

/// Writes `val` in `SIZE` blocks with the appropriate continuation tokens.
#[inline]
fn encode_blocks<const SIZE: usize>(out: &mut [u8], val: &[u8]) -> usize {
    let block_count = ceil(val.len(), SIZE);
    let end_offset = block_count * (SIZE + 1);
    let to_write = &mut out[..end_offset];

    let chunks = val.chunks_exact(SIZE);
    let remainder = chunks.remainder();
    for (input, output) in chunks.clone().zip(to_write.chunks_exact_mut(SIZE + 1)) {
        output[..SIZE].copy_from_slice(input);
        output[SIZE] = BLOCK_CONTINUATION;
    }

    let last = to_write.len() - 1;
    if remainder.is_empty() {
        to_write[last] = block_length_byte(SIZE);
    } else {
        let start_offset = (block_count - 1) * (SIZE + 1);
        to_write[start_offset..start_offset + remainder.len()].copy_from_slice(remainder);
        to_write[last] = block_length_byte(remainder.len());
    }
    end_offset
}

/// The trailing length byte for a block. `len` is always `<= BLOCK_SIZE`, so the truncation is
/// impossible in practice.
#[inline]
#[expect(
    clippy::cast_possible_truncation,
    reason = "block length is bounded by BLOCK_SIZE (32), well within u8"
)]
fn block_length_byte(len: usize) -> u8 {
    len as u8
}

/// Decodes the blocks of a single encoded value, calling `f` with each decoded chunk. Returns
/// the number of bytes consumed.
fn decode_blocks(row: &[u8], options: SortOptions, mut f: impl FnMut(&[u8])) -> usize {
    let (non_empty_sentinel, continuation) = if options.descending {
        (!NON_EMPTY_SENTINEL, !BLOCK_CONTINUATION)
    } else {
        (NON_EMPTY_SENTINEL, BLOCK_CONTINUATION)
    };

    if row[0] != non_empty_sentinel {
        // Empty or null value
        return 1;
    }

    let block_len = |sentinel: u8| {
        if options.descending {
            !sentinel as usize
        } else {
            sentinel as usize
        }
    };

    let mut idx = 1;
    for _ in 0..MINI_BLOCK_COUNT {
        let sentinel = row[idx + MINI_BLOCK_SIZE];
        if sentinel != continuation {
            f(&row[idx..idx + block_len(sentinel)]);
            return idx + MINI_BLOCK_SIZE + 1;
        }
        f(&row[idx..idx + MINI_BLOCK_SIZE]);
        idx += MINI_BLOCK_SIZE + 1;
    }

    loop {
        let sentinel = row[idx + BLOCK_SIZE];
        if sentinel != continuation {
            f(&row[idx..idx + block_len(sentinel)]);
            return idx + BLOCK_SIZE + 1;
        }
        f(&row[idx..idx + BLOCK_SIZE]);
        idx += BLOCK_SIZE + 1;
    }
}

/// Decodes one value from `row`, advancing it past the bytes consumed. `None` is a null.
fn decode_one(row: &mut &[u8], options: SortOptions) -> Option<Vec<u8>> {
    let slice: &[u8] = row;
    let is_null = slice[0] == null_sentinel(options);
    let mut buf = Vec::new();
    let consumed = decode_blocks(slice, options, |b| buf.extend_from_slice(b));
    *row = &slice[consumed..];
    if is_null {
        None
    } else {
        if options.descending {
            for b in &mut buf {
                *b = !*b;
            }
        }
        Some(buf)
    }
}
