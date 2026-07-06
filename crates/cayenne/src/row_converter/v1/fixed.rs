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

//! V1 fixed-width codecs: primitives (via a single generic [`PrimitiveCodec`]) and booleans.
//!
//! The byte layout is copied from Apache Arrow's `arrow-row` crate (Apache-2.0):
//! a fixed-width value is a `1` validity byte followed by the big-endian value (with the sign
//! bit flipped for signed integers and floats total-ordered), or the null sentinel followed by
//! zeroed value bytes. Descending order inverts all value bytes.

use std::sync::Arc;

use arrow::array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::i256;
use arrow_schema::{ArrowError, DataType, SortOptions};

use crate::row_converter::codec::{ColumnCodec, LengthTracker, null_sentinel};

/// Reconstructs a fixed-width encoded value from its bytes, inverting first if `invert` is set.
pub(crate) trait FromSlice {
    fn from_slice(slice: &[u8], invert: bool) -> Self;
}

impl<const N: usize> FromSlice for [u8; N] {
    #[inline]
    fn from_slice(slice: &[u8], invert: bool) -> Self {
        let mut t = [0u8; N];
        t.copy_from_slice(slice);
        if invert {
            for o in &mut t {
                *o = !*o;
            }
        }
        t
    }
}

/// A fixed-width value that encodes to an order-preserving byte sequence.
pub(crate) trait FixedLengthEncoding: Copy {
    const ENCODED_LEN: usize = 1 + std::mem::size_of::<Self::Encoded>();

    type Encoded: Sized + Copy + FromSlice + AsRef<[u8]> + AsMut<[u8]>;

    fn encode(self) -> Self::Encoded;

    fn decode(encoded: Self::Encoded) -> Self;
}

impl FixedLengthEncoding for bool {
    type Encoded = [u8; 1];

    fn encode(self) -> [u8; 1] {
        [u8::from(self)]
    }

    fn decode(encoded: Self::Encoded) -> Self {
        encoded[0] != 0
    }
}

macro_rules! encode_signed {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                let mut b = self.to_be_bytes();
                // Toggle top "sign" bit to ensure consistent sort order
                b[0] ^= 0x80;
                b
            }

            fn decode(mut encoded: Self::Encoded) -> Self {
                // Toggle top "sign" bit
                encoded[0] ^= 0x80;
                Self::from_be_bytes(encoded)
            }
        }
    };
}

encode_signed!(1, i8);
encode_signed!(2, i16);
encode_signed!(4, i32);
encode_signed!(8, i64);
encode_signed!(16, i128);
encode_signed!(32, i256);

macro_rules! encode_unsigned {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                self.to_be_bytes()
            }

            fn decode(encoded: Self::Encoded) -> Self {
                Self::from_be_bytes(encoded)
            }
        }
    };
}

encode_unsigned!(1, u8);
encode_unsigned!(2, u16);
encode_unsigned!(4, u32);
encode_unsigned!(8, u64);

impl FixedLengthEncoding for f32 {
    type Encoded = [u8; 4];

    fn encode(self) -> [u8; 4] {
        let s = self.to_bits().cast_signed();
        let val = s ^ ((s >> 31).cast_unsigned() >> 1).cast_signed();
        val.encode()
    }

    fn decode(encoded: Self::Encoded) -> Self {
        let bits = i32::decode(encoded);
        let val = bits ^ ((bits >> 31).cast_unsigned() >> 1).cast_signed();
        Self::from_bits(val.cast_unsigned())
    }
}

impl FixedLengthEncoding for f64 {
    type Encoded = [u8; 8];

    fn encode(self) -> [u8; 8] {
        let s = self.to_bits().cast_signed();
        let val = s ^ ((s >> 63).cast_unsigned() >> 1).cast_signed();
        val.encode()
    }

    fn decode(encoded: Self::Encoded) -> Self {
        let bits = i64::decode(encoded);
        let val = bits ^ ((bits >> 63).cast_unsigned() >> 1).cast_signed();
        Self::from_bits(val.cast_unsigned())
    }
}

/// Fixed width types are encoded as a `1` validity byte (or the null sentinel) followed by the
/// [`FixedLengthEncoding`] bytes.
fn encode_fixed<T: FixedLengthEncoding>(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &[T],
    nulls: &NullBuffer,
    opts: SortOptions,
) {
    for (value_idx, is_valid) in nulls.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;
        if is_valid {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = values[value_idx].encode();
            if opts.descending {
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v);
            }
            to_write[1..].copy_from_slice(encoded.as_ref());
        } else {
            data[*offset] = null_sentinel(opts);
        }
        *offset = end_offset;
    }
}

/// Encoding for non-nullable fixed-width arrays: iterates values directly, skipping null checks.
fn encode_fixed_not_null<T: FixedLengthEncoding>(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &[T],
    opts: SortOptions,
) {
    for (value_idx, val) in values.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + T::ENCODED_LEN;
        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        let mut encoded = val.encode();
        if opts.descending {
            encoded.as_mut().iter_mut().for_each(|v| *v = !*v);
        }
        to_write[1..].copy_from_slice(encoded.as_ref());
        *offset = end_offset;
    }
}

fn encode_boolean(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &BooleanBuffer,
    nulls: &NullBuffer,
    opts: SortOptions,
) {
    for (idx, is_valid) in nulls.iter().enumerate() {
        let offset = &mut offsets[idx + 1];
        let end_offset = *offset + bool::ENCODED_LEN;
        if is_valid {
            let to_write = &mut data[*offset..end_offset];
            to_write[0] = 1;
            let mut encoded = values.value(idx).encode();
            if opts.descending {
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v);
            }
            to_write[1..].copy_from_slice(encoded.as_ref());
        } else {
            data[*offset] = null_sentinel(opts);
        }
        *offset = end_offset;
    }
}

fn encode_boolean_not_null(
    data: &mut [u8],
    offsets: &mut [usize],
    values: &BooleanBuffer,
    opts: SortOptions,
) {
    for (value_idx, val) in values.iter().enumerate() {
        let offset = &mut offsets[value_idx + 1];
        let end_offset = *offset + bool::ENCODED_LEN;
        let to_write = &mut data[*offset..end_offset];
        to_write[0] = 1;
        let mut encoded = val.encode();
        if opts.descending {
            encoded.as_mut().iter_mut().for_each(|v| *v = !*v);
        }
        to_write[1..].copy_from_slice(encoded.as_ref());
        *offset = end_offset;
    }
}

/// Splits `len` bytes from the front of `src`, advancing it.
#[inline]
fn split_off<'a>(src: &mut &'a [u8], len: usize) -> &'a [u8] {
    let v = &src[..len];
    *src = &src[len..];
    v
}

/// Codec for any primitive type whose native representation implements [`FixedLengthEncoding`].
///
/// A single generic covers every integer, float, temporal, and decimal type; optimizing one of
/// them means adding a dedicated codec and routing that type to it, not editing this one.
pub(crate) struct PrimitiveCodec<T: ArrowPrimitiveType>
where
    T::Native: FixedLengthEncoding,
{
    data_type: DataType,
    options: SortOptions,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for PrimitiveCodec<T>
where
    T::Native: FixedLengthEncoding,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCodec")
            .field("data_type", &self.data_type)
            .field("options", &self.options)
            .finish()
    }
}

impl<T: ArrowPrimitiveType> PrimitiveCodec<T>
where
    T::Native: FixedLengthEncoding,
{
    pub(crate) fn new(data_type: DataType, options: SortOptions) -> Self {
        Self {
            data_type,
            options,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: ArrowPrimitiveType> ColumnCodec for PrimitiveCodec<T>
where
    T::Native: FixedLengthEncoding,
{
    fn append_lengths(&self, _array: &dyn Array, tracker: &mut LengthTracker) {
        tracker.push_fixed(<T::Native as FixedLengthEncoding>::ENCODED_LEN);
    }

    fn encode(&self, data: &mut [u8], offsets: &mut [usize], array: &dyn Array) {
        let column = array.as_primitive::<T>();
        if let Some(nulls) = column.nulls().filter(|n| n.null_count() > 0) {
            encode_fixed(data, offsets, column.values(), nulls, self.options);
        } else {
            encode_fixed_not_null(data, offsets, column.values(), self.options);
        }
    }

    fn decode(&self, rows: &mut [&[u8]], _validate_utf8: bool) -> Result<ArrayRef, ArrowError> {
        let encoded_len = <T::Native as FixedLengthEncoding>::ENCODED_LEN;
        let mut builder = PrimitiveBuilder::<T>::with_capacity(rows.len());
        for row in rows.iter_mut() {
            let encoded = split_off(row, encoded_len);
            if encoded[0] == 1 {
                let bytes = <<T::Native as FixedLengthEncoding>::Encoded as FromSlice>::from_slice(
                    &encoded[1..],
                    self.options.descending,
                );
                builder.append_value(<T::Native as FixedLengthEncoding>::decode(bytes));
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(
            builder.finish().with_data_type(self.data_type.clone()),
        ))
    }
}

/// Codec for `Boolean`.
#[derive(Debug)]
pub(crate) struct BooleanCodec {
    options: SortOptions,
}

impl BooleanCodec {
    pub(crate) fn new(options: SortOptions) -> Self {
        Self { options }
    }
}

impl ColumnCodec for BooleanCodec {
    fn append_lengths(&self, _array: &dyn Array, tracker: &mut LengthTracker) {
        tracker.push_fixed(bool::ENCODED_LEN);
    }

    fn encode(&self, data: &mut [u8], offsets: &mut [usize], array: &dyn Array) {
        let column = array.as_boolean();
        if let Some(nulls) = column.nulls().filter(|n| n.null_count() > 0) {
            encode_boolean(data, offsets, column.values(), nulls, self.options);
        } else {
            encode_boolean_not_null(data, offsets, column.values(), self.options);
        }
    }

    fn decode(&self, rows: &mut [&[u8]], _validate_utf8: bool) -> Result<ArrayRef, ArrowError> {
        let true_val = if self.options.descending { !1u8 } else { 1u8 };
        let mut builder = BooleanBuilder::with_capacity(rows.len());
        for row in rows.iter_mut() {
            let encoded = split_off(row, bool::ENCODED_LEN);
            if encoded[0] == 1 {
                builder.append_value(encoded[1] == true_val);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}
