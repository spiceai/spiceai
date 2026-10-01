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

//! A compact, order-preserving, prefix-free byte encoding of compound Arrow
//! keys, readable one row at a time straight from the columns.
//!
//! A key is the concatenation of its columns' encodings, so the bytes of the
//! leading columns are a prefix of the key and byte order is the order of the
//! key tuple. Each column encodes as:
//!
//! | column | encoding |
//! |---|---|
//! | nullable, NULL | `00` |
//! | nullable, valid | `01` then the value's encoding |
//! | signed integers, dates, times, timestamps, durations, `Decimal128` | big-endian, sign bit flipped |
//! | unsigned integers | big-endian |
//! | `Boolean` | `00` or `01` |
//! | `FixedSizeBinary` | the bytes as is |
//! | strings and binaries (all offset sizes and views) | the bytes with `00` → `01 01` and `01` → `01 02`, then a `00` terminator |
//!
//! Fixed-width values need no terminator: every value of a column has the same
//! length, so no value is a prefix of another. The variable-length escape
//! keeps the terminator the smallest byte that can follow a value, which makes
//! a string sort before every string it is a prefix of, and costs one byte for
//! any value that contains no `00` or `01` byte — against the 32-byte block
//! padding of Arrow's row format.
//!
//! Equal key tuples always produce equal bytes, whichever array type holds the
//! value (`Utf8`, `LargeUtf8` or `Utf8View`), because the encoding depends only
//! on the value. The declared [`KeyField`] type still has to match the bound
//! array exactly, so a mismatch is an error rather than a silent miss.
//!
//! Floating-point columns are refused. Values SQL holds equal can have
//! different bits (`0.0` and `-0.0`, or NaNs with different payloads), so
//! encoding the bits would make a lookup for one miss rows holding the other.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, GenericBinaryArray,
    StringViewArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use snafu::ensure;

use crate::source::{KeySource, Run};
use crate::{ColumnMismatchSnafu, Result, UnsupportedTypeSnafu};

const NULL_MARK: &[u8] = &[0x00];
const VALID_MARK: &[u8] = &[0x01];
const TERMINATOR: &[u8] = &[0x00];
const ESCAPED_00: &[u8] = &[0x01, 0x01];
const ESCAPED_01: &[u8] = &[0x01, 0x02];

/// One column of a compound key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyField {
    /// The column's Arrow type; bound arrays must have exactly this type.
    pub data_type: DataType,
    /// Whether the column admits NULL. A non-nullable column spends no byte on
    /// a validity marker, and rejects arrays that contain NULLs.
    pub nullable: bool,
}

impl KeyField {
    /// A key column of `data_type`.
    #[must_use]
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}

/// Encodes compound keys of a fixed list of column types.
#[derive(Debug, Clone)]
pub struct KeyEncoder {
    fields: Vec<KeyField>,
    /// How a key becomes its 64-bit word; see [`KeyEncoder::key_word`].
    words: WordRule,
}

/// How an encoded key maps to its 64-bit word.
#[derive(Debug, Clone)]
enum WordRule {
    /// Every field is fixed-width and their values fit 8 bytes: the word is
    /// the values' bytes as one big-endian integer, so distinct keys have
    /// distinct words, in key order. Per field: whether it is nullable (its
    /// encoding then starts with a marker byte) and its value's width.
    Exact(Vec<(bool, usize)>),
    /// Otherwise the word is a 64-bit hash of the encoded key, keeping only
    /// its low `bits` bits (64 except in tests that force collisions).
    Hashed { bits: u32 },
}

/// The encoded width of a value of `data_type` when it is fixed, for the
/// types whose encoding is a fixed-width, injective image of the value. Other
/// types return `None` and are hashed, which is always correct.
fn fixed_width(data_type: &DataType) -> Option<usize> {
    Some(match data_type {
        DataType::Int8 | DataType::UInt8 | DataType::Boolean => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Date32 | DataType::Time32(_) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => 8,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok()?,
        _ => return None,
    })
}

impl WordRule {
    fn of(fields: &[KeyField]) -> Self {
        let layout: Option<Vec<(bool, usize)>> = fields
            .iter()
            .map(|field| fixed_width(&field.data_type).map(|width| (field.nullable, width)))
            .collect();
        match layout {
            Some(layout) if layout.iter().map(|&(_, width)| width).sum::<usize>() <= 8 => {
                Self::Exact(layout)
            }
            _ => Self::Hashed { bits: 64 },
        }
    }
}

impl KeyEncoder {
    /// An encoder for keys made of `fields`, in order.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnsupportedType`] when a field's type has no
    /// order-preserving encoding here.
    pub fn new(fields: Vec<KeyField>) -> Result<Self> {
        for field in &fields {
            ensure!(
                is_supported(&field.data_type),
                UnsupportedTypeSnafu {
                    data_type: field.data_type.to_string(),
                }
            );
        }
        let words = WordRule::of(&fields);
        Ok(Self { fields, words })
    }

    /// The key fields.
    #[must_use]
    pub fn fields(&self) -> &[KeyField] {
        &self.fields
    }

    /// The 64-bit word an index stores for the encoded `key` of a row with no
    /// NULL key column (rows with one are never indexed). When every field is
    /// fixed-width and their values fit 8 bytes, the word is those values'
    /// bytes, so distinct keys have distinct words; otherwise it is a 64-bit
    /// hash of the key, and two keys may share a word. An index answers
    /// candidate rows that every query still filters, so a shared word costs
    /// only the other key's rows being read.
    #[must_use]
    pub fn key_word(&self, key: &[u8]) -> u64 {
        let hashed = |key: &[u8]| hash_index::hash_key_bytes_oneshot(key);
        if let WordRule::Hashed { bits } = self.words {
            return hashed(key) & (u64::MAX >> (64 - bits));
        }
        if let WordRule::Exact(layout) = &self.words {
            // The fields' value bytes without their markers: at most 8.
            let mut bytes = [0_u8; 8];
            let (mut at, mut len) = (0, 0);
            for &(nullable, width) in layout {
                at += usize::from(nullable);
                let (Some(value), Some(slot)) =
                    (key.get(at..at + width), bytes.get_mut(len..len + width))
                else {
                    return hashed(key);
                };
                slot.copy_from_slice(value);
                at += width;
                len += width;
            }
            if at == key.len() {
                return crate::word_proof::fold_word(&bytes[..len]);
            }
        }
        hashed(key)
    }

    /// This encoder with every key hashed to a word of only `bits` bits (1 to
    /// 64), so that many keys share a word. For tests that an index's
    /// candidates are filtered: never use it to serve queries.
    #[doc(hidden)]
    #[must_use]
    pub fn with_word_bits(mut self, bits: u32) -> Self {
        self.words = WordRule::Hashed {
            bits: bits.clamp(1, 64),
        };
        self
    }

    /// Whether distinct keys always have distinct [words](Self::key_word).
    #[must_use]
    pub fn exact_words(&self) -> bool {
        matches!(self.words, WordRule::Exact(_))
    }

    /// Bind one array per key field, all of the same length.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ColumnMismatch`] when the number of arrays, a type, a
    /// length, or a NULL in a non-nullable field does not match.
    pub fn bind<'a>(&self, columns: &'a [ArrayRef]) -> Result<BoundKeyColumns<'a>> {
        ensure!(
            columns.len() == self.fields.len(),
            ColumnMismatchSnafu {
                reason: format!(
                    "expected {} key columns but received {}",
                    self.fields.len(),
                    columns.len()
                ),
            }
        );
        let num_rows = columns
            .first()
            .map_or(0, |column| Array::len(column.as_ref()));
        let mut bound = Vec::with_capacity(columns.len());
        for (index, (column, field)) in columns.iter().zip(&self.fields).enumerate() {
            ensure!(
                column.data_type() == &field.data_type,
                ColumnMismatchSnafu {
                    reason: format!(
                        "key column {index} is {} but the key declares {}",
                        column.data_type(),
                        field.data_type
                    ),
                }
            );
            ensure!(
                column.len() == num_rows,
                ColumnMismatchSnafu {
                    reason: format!(
                        "key column {index} has {} rows but key column 0 has {num_rows}",
                        column.len()
                    ),
                }
            );
            ensure!(
                field.nullable || column.null_count() == 0,
                ColumnMismatchSnafu {
                    reason: format!("key column {index} is declared non-nullable but holds NULL"),
                }
            );
            bound.push(BoundColumn {
                data: ColumnData::new(column.as_ref())?,
                nulls: if field.nullable { column.nulls() } else { None },
                nullable: field.nullable,
            });
        }
        Ok(BoundKeyColumns {
            columns: bound,
            num_rows,
        })
    }
}

fn is_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Interval(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// The typed values of one bound column.
#[derive(Debug, Clone, Copy)]
enum ColumnData<'a> {
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    I128(&'a [i128]),
    I256(&'a [arrow_buffer::i256]),
    DayTime(&'a [arrow_buffer::IntervalDayTime]),
    MonthDayNano(&'a [arrow_buffer::IntervalMonthDayNano]),
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
    Bool(&'a BooleanArray),
    Binary(&'a GenericBinaryArray<i32>),
    LargeBinary(&'a GenericBinaryArray<i64>),
    Utf8(&'a arrow_array::StringArray),
    LargeUtf8(&'a arrow_array::LargeStringArray),
    Utf8View(&'a StringViewArray),
    BinaryView(&'a BinaryViewArray),
    FixedBinary(&'a FixedSizeBinaryArray),
}

impl<'a> ColumnData<'a> {
    fn new(array: &'a dyn Array) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Int8 => Self::I8(array.as_primitive::<Int8Type>().values()),
            DataType::Int16 => Self::I16(array.as_primitive::<Int16Type>().values()),
            DataType::Int32 => Self::I32(array.as_primitive::<Int32Type>().values()),
            DataType::Int64 => Self::I64(array.as_primitive::<Int64Type>().values()),
            DataType::UInt8 => Self::U8(array.as_primitive::<UInt8Type>().values()),
            DataType::UInt16 => Self::U16(array.as_primitive::<UInt16Type>().values()),
            DataType::UInt32 => Self::U32(array.as_primitive::<UInt32Type>().values()),
            DataType::UInt64 => Self::U64(array.as_primitive::<UInt64Type>().values()),
            DataType::Boolean => Self::Bool(array.as_boolean()),
            DataType::Date32 => Self::I32(array.as_primitive::<Date32Type>().values()),
            DataType::Date64 => Self::I64(array.as_primitive::<Date64Type>().values()),
            DataType::Time32(TimeUnit::Second) => {
                Self::I32(array.as_primitive::<Time32SecondType>().values())
            }
            DataType::Time32(_) => {
                Self::I32(array.as_primitive::<Time32MillisecondType>().values())
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<Time64NanosecondType>().values())
            }
            DataType::Time64(_) => {
                Self::I64(array.as_primitive::<Time64MicrosecondType>().values())
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Self::I64(array.as_primitive::<TimestampSecondType>().values())
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Self::I64(array.as_primitive::<TimestampMillisecondType>().values())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::I64(array.as_primitive::<TimestampMicrosecondType>().values())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::I64(array.as_primitive::<TimestampNanosecondType>().values())
            }
            DataType::Duration(TimeUnit::Second) => {
                Self::I64(array.as_primitive::<DurationSecondType>().values())
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Self::I64(array.as_primitive::<DurationMillisecondType>().values())
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::I64(array.as_primitive::<DurationMicrosecondType>().values())
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<DurationNanosecondType>().values())
            }
            DataType::Decimal256(_, _) => Self::I256(
                array
                    .as_primitive::<arrow_array::types::Decimal256Type>()
                    .values(),
            ),
            DataType::Interval(IntervalUnit::YearMonth) => Self::I32(
                array
                    .as_primitive::<arrow_array::types::IntervalYearMonthType>()
                    .values(),
            ),
            DataType::Interval(IntervalUnit::DayTime) => Self::DayTime(
                array
                    .as_primitive::<arrow_array::types::IntervalDayTimeType>()
                    .values(),
            ),
            DataType::Interval(IntervalUnit::MonthDayNano) => Self::MonthDayNano(
                array
                    .as_primitive::<arrow_array::types::IntervalMonthDayNanoType>()
                    .values(),
            ),
            DataType::Decimal128(_, _) => {
                Self::I128(array.as_primitive::<Decimal128Type>().values())
            }
            DataType::Utf8 => Self::Utf8(array.as_string::<i32>()),
            DataType::LargeUtf8 => Self::LargeUtf8(array.as_string::<i64>()),
            DataType::Utf8View => Self::Utf8View(array.as_string_view()),
            DataType::Binary => Self::Binary(array.as_binary::<i32>()),
            DataType::LargeBinary => Self::LargeBinary(array.as_binary::<i64>()),
            DataType::BinaryView => Self::BinaryView(array.as_binary_view()),
            DataType::FixedSizeBinary(_) => Self::FixedBinary(array.as_fixed_size_binary()),
            other => {
                return UnsupportedTypeSnafu {
                    data_type: other.to_string(),
                }
                .fail();
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct BoundColumn<'a> {
    data: ColumnData<'a>,
    nulls: Option<&'a NullBuffer>,
    nullable: bool,
}

/// Key columns bound for encoding, one row at a time.
#[derive(Debug, Clone)]
pub struct BoundKeyColumns<'a> {
    columns: Vec<BoundColumn<'a>>,
    num_rows: usize,
}

impl<'a> BoundKeyColumns<'a> {
    /// Number of rows in the bound columns.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Whether any key column of `row` is NULL.
    #[inline]
    #[must_use]
    pub fn has_null(&self, row: usize) -> bool {
        self.columns
            .iter()
            .any(|column| column.nulls.is_some_and(|nulls| nulls.is_null(row)))
    }

    /// A [`KeySource`] that produces the key of `row` from the columns as it
    /// is read. `row` must be below [`Self::num_rows`].
    #[inline]
    #[must_use]
    pub fn source(&self, row: usize) -> RowKeySource<'_, 'a> {
        debug_assert!(row < self.num_rows);
        RowKeySource {
            columns: &self.columns,
            row,
            column: 0,
            pending: Pending::None,
        }
    }

    /// Append the key of `row` to `out`. The same bytes [`Self::source`]
    /// produces, by construction: this drains that source.
    #[inline]
    pub fn encode_row(&self, row: usize, out: &mut Vec<u8>) {
        let mut source = self.source(row);
        while let Some(run) = source.next_run() {
            out.extend_from_slice(run.as_slice());
        }
    }

    /// Every row's key, concatenated, with `offsets[i]..offsets[i + 1]` the
    /// range of row `i`.
    #[must_use]
    pub fn encode_all(&self) -> (Vec<u8>, Vec<usize>) {
        let mut bytes = Vec::new();
        let mut offsets = Vec::with_capacity(self.num_rows + 1);
        offsets.push(0);
        for row in 0..self.num_rows {
            self.encode_row(row, &mut bytes);
            offsets.push(bytes.len());
        }
        (bytes, offsets)
    }
}

/// Bytes of the current column still to be produced.
#[derive(Debug, Clone, Copy)]
enum Pending<'a> {
    None,
    /// A variable-length value, escaped and then terminated.
    Escaped(&'a [u8]),
    /// A fixed-length value, produced as is.
    Raw(&'a [u8]),
    /// The second half of a value wider than one inline run (`Decimal256`).
    Inline([u8; 16]),
}

/// The key of one row, produced column by column from the Arrow arrays
/// without materializing it. Strings and binaries are borrowed from the
/// arrays' buffers in runs between the bytes that need escaping.
#[derive(Debug)]
pub struct RowKeySource<'b, 'a> {
    columns: &'b [BoundColumn<'a>],
    row: usize,
    column: usize,
    pending: Pending<'a>,
}

#[inline]
fn fixed<const N: usize>(marked: bool, bytes: [u8; N]) -> Run<'static> {
    let mut buf = [0_u8; 17];
    let start = usize::from(marked);
    buf[0] = VALID_MARK[0];
    buf[start..start + N].copy_from_slice(&bytes);
    Run::inline(&buf[..start + N])
}

impl<'a> KeySource<'a> for RowKeySource<'_, 'a> {
    #[inline]
    fn next_run(&mut self) -> Option<Run<'a>> {
        loop {
            match self.pending {
                Pending::None => {}
                Pending::Raw(bytes) => {
                    self.pending = Pending::None;
                    self.column += 1;
                    return Some(Run::borrowed(bytes));
                }
                Pending::Inline(bytes) => {
                    self.pending = Pending::None;
                    self.column += 1;
                    return Some(Run::inline(&bytes));
                }
                Pending::Escaped(rest) => {
                    return Some(match rest.iter().position(|&b| b < 0x02) {
                        None if rest.is_empty() => {
                            self.pending = Pending::None;
                            self.column += 1;
                            Run::borrowed(TERMINATOR)
                        }
                        None => {
                            self.pending = Pending::Escaped(&[]);
                            Run::borrowed(rest)
                        }
                        Some(0) => {
                            self.pending = Pending::Escaped(&rest[1..]);
                            Run::borrowed(if rest[0] == 0 { ESCAPED_00 } else { ESCAPED_01 })
                        }
                        Some(at) => {
                            self.pending = Pending::Escaped(&rest[at..]);
                            Run::borrowed(&rest[..at])
                        }
                    });
                }
            }

            let column = self.columns.get(self.column)?;
            let row = self.row;
            if column.nulls.is_some_and(|nulls| nulls.is_null(row)) {
                self.column += 1;
                return Some(Run::borrowed(NULL_MARK));
            }
            let marked = column.nullable;
            let run = match column.data {
                ColumnData::I8(v) => fixed(marked, (v[row].cast_unsigned() ^ 0x80).to_be_bytes()),
                ColumnData::I16(v) => {
                    fixed(marked, (v[row].cast_unsigned() ^ (1 << 15)).to_be_bytes())
                }
                ColumnData::I32(v) => {
                    fixed(marked, (v[row].cast_unsigned() ^ (1 << 31)).to_be_bytes())
                }
                ColumnData::I64(v) => {
                    fixed(marked, (v[row].cast_unsigned() ^ (1 << 63)).to_be_bytes())
                }
                ColumnData::I128(v) => {
                    fixed(marked, (v[row].cast_unsigned() ^ (1 << 127)).to_be_bytes())
                }
                ColumnData::I256(v) => {
                    // 32 bytes, sign bit flipped, in two runs: the high half
                    // (with the marker) now, the low half next.
                    let mut bytes = v[row].to_be_bytes();
                    bytes[0] ^= 0x80;
                    self.pending = Pending::Inline(std::array::from_fn(|i| bytes[16 + i]));
                    let high: [u8; 16] = std::array::from_fn(|i| bytes[i]);
                    return Some(fixed(marked, high));
                }
                ColumnData::DayTime(v) => {
                    let mut bytes = [0_u8; 8];
                    bytes[..4]
                        .copy_from_slice(&(v[row].days.cast_unsigned() ^ (1 << 31)).to_be_bytes());
                    bytes[4..].copy_from_slice(
                        &(v[row].milliseconds.cast_unsigned() ^ (1 << 31)).to_be_bytes(),
                    );
                    fixed(marked, bytes)
                }
                ColumnData::MonthDayNano(v) => {
                    let mut bytes = [0_u8; 16];
                    bytes[..4].copy_from_slice(
                        &(v[row].months.cast_unsigned() ^ (1 << 31)).to_be_bytes(),
                    );
                    bytes[4..8]
                        .copy_from_slice(&(v[row].days.cast_unsigned() ^ (1 << 31)).to_be_bytes());
                    bytes[8..].copy_from_slice(
                        &(v[row].nanoseconds.cast_unsigned() ^ (1 << 63)).to_be_bytes(),
                    );
                    fixed(marked, bytes)
                }
                ColumnData::U8(v) => fixed(marked, v[row].to_be_bytes()),
                ColumnData::U16(v) => fixed(marked, v[row].to_be_bytes()),
                ColumnData::U32(v) => fixed(marked, v[row].to_be_bytes()),
                ColumnData::U64(v) => fixed(marked, v[row].to_be_bytes()),
                ColumnData::Bool(v) => fixed(marked, [u8::from(v.value(row))]),
                ColumnData::FixedBinary(v) => {
                    self.pending = Pending::Raw(v.value(row));
                    if marked {
                        return Some(Run::borrowed(VALID_MARK));
                    }
                    continue;
                }
                ColumnData::Binary(v) => return self.start_escaped(v.value(row), marked),
                ColumnData::LargeBinary(v) => return self.start_escaped(v.value(row), marked),
                ColumnData::Utf8(v) => return self.start_escaped(v.value(row).as_bytes(), marked),
                ColumnData::LargeUtf8(v) => {
                    return self.start_escaped(v.value(row).as_bytes(), marked);
                }
                ColumnData::Utf8View(v) => {
                    return self.start_escaped(v.value(row).as_bytes(), marked);
                }
                ColumnData::BinaryView(v) => return self.start_escaped(v.value(row), marked),
            };
            self.column += 1;
            return Some(run);
        }
    }
}

impl<'a> RowKeySource<'_, 'a> {
    #[inline]
    fn start_escaped(&mut self, value: &'a [u8], marked: bool) -> Option<Run<'a>> {
        self.pending = Pending::Escaped(value);
        if marked {
            Some(Run::borrowed(VALID_MARK))
        } else {
            self.next_run()
        }
    }
}
