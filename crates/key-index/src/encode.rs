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

//! A compact, prefix-free byte encoding of compound Arrow keys, written one
//! row at a time straight from the columns. Byte order is the order of the key
//! tuple, except for the floats the encoding gives one encoding (below):
//! `-0.0` and `0.0` encode alike, and every NaN encodes as the one positive
//! quiet NaN, above `+∞`, wherever the NaN's own bits sort.
//!
//! A key is the concatenation of its columns' encodings, so the bytes of the
//! leading columns are a prefix of the key. Each column encodes as:
//!
//! | column | encoding |
//! |---|---|
//! | nullable, NULL | `00` |
//! | nullable, valid | `01` then the value's encoding |
//! | signed integers, dates, times, timestamps, durations, `Decimal128`, `Decimal256`, `Interval(YearMonth)` | big-endian, sign bit flipped |
//! | `Interval(DayTime)`, `Interval(MonthDayNano)` | each field in order (`i32` days and milliseconds; `i32` months and days and `i64` nanoseconds), big-endian, sign bit flipped |
//! | unsigned integers | big-endian |
//! | floating point | canonical bits big-endian, all bits flipped when negative, else the sign bit |
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
//! A floating-point value is encoded by its bits, except that `-0.0` encodes
//! as `0.0` and every NaN as the positive quiet NaN of its width. Engines
//! disagree on which of those are equal: IEEE comparison holds `-0.0 = 0.0`,
//! and some engines hold every NaN equal, while a total order (`DataFusion`'s)
//! tells them all apart. Giving each group one encoding is correct either
//! way: values an engine holds equal always share an encoding, and values it
//! tells apart that share one only add candidate rows the query's filter
//! drops. Every other value keeps its own bits, and the sign transform orders
//! them as the numbers they are.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, GenericBinaryArray,
    StringViewArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use snafu::{OptionExt, ensure};

use crate::escape_proof::escape_value_into;
use crate::{
    ColumnCountSnafu, ColumnLengthSnafu, ColumnTypeSnafu, Result, UnexpectedNullSnafu,
    UnsupportedTypeSnafu,
};

/// Appends the encoding of a float of `width` bits (16, 32 or 64) with
/// `exponent_bits` exponent bits whose bits are `bits`: `-0.0` as `0.0` and
/// every NaN as the quiet NaN with no payload (see the module docs), then
/// ordered as the number is, all bits flipped for a negative value and the
/// sign bit set for a positive one, big-endian.
fn put_float(out: &mut Vec<u8>, bits: u64, width: u32, exponent_bits: u32) {
    let mask = u64::MAX >> (64 - width);
    let sign = 1_u64 << (width - 1);
    let fraction_bits = width - 1 - exponent_bits;
    let exponent = ((1_u64 << exponent_bits) - 1) << fraction_bits;
    let fraction = (1_u64 << fraction_bits) - 1;
    let magnitude = bits & !sign;
    let canonical = if magnitude == 0 {
        0
    } else if magnitude & exponent == exponent && magnitude & fraction != 0 {
        exponent | (1 << (fraction_bits - 1))
    } else {
        bits
    };
    let ordered = if canonical & sign == 0 {
        canonical | sign
    } else {
        !canonical
    };
    out.extend_from_slice(&(ordered & mask).to_be_bytes()[(64 - width) as usize / 8..]);
}

const NULL_MARK: u8 = 0x00;
const VALID_MARK: u8 = 0x01;

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
    /// Each field's [`Kind`], resolved once when the key is declared.
    kinds: Vec<Kind>,
    /// How a key becomes its 64-bit word; see [`KeyEncoder::key_word`].
    words: WordRule,
}

/// How an encoded key maps to its 64-bit word.
#[derive(Debug, Clone)]
enum WordRule {
    /// Every field is fixed-width and their values fit 8 bytes: the word is
    /// the values' encoded bytes as one big-endian integer, so keys with
    /// distinct encodings have distinct words, in key order. (Floats `-0.0`
    /// and `0.0`, and every NaN, share an encoding by design; see the module
    /// docs.) Per field: whether it is nullable (its
    /// encoding then starts with a marker byte) and its value's width.
    Exact(Vec<(bool, usize)>),
    /// Otherwise the word is a 64-bit hash of the encoded key, keeping only
    /// its low `bits` bits (64 except in tests that force collisions).
    Hashed { bits: u32 },
}

/// A supported key column type: the Arrow types this encoding accepts, each
/// named by how its values are read. [`kind`] is the one list of supported
/// types; everything else matches a `Kind` exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Boolean,
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit),
    Duration(TimeUnit),
    Decimal128,
    Decimal256,
    Interval(IntervalUnit),
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    BinaryView,
    FixedSizeBinary(i32),
}

/// The [`Kind`] of `data_type`, or `None` when it has no key encoding here.
fn kind(data_type: &DataType) -> Option<Kind> {
    Some(match data_type {
        DataType::Int8 => Kind::Int8,
        DataType::Int16 => Kind::Int16,
        DataType::Int32 => Kind::Int32,
        DataType::Int64 => Kind::Int64,
        DataType::UInt8 => Kind::UInt8,
        DataType::UInt16 => Kind::UInt16,
        DataType::UInt32 => Kind::UInt32,
        DataType::UInt64 => Kind::UInt64,
        DataType::Float16 => Kind::Float16,
        DataType::Float32 => Kind::Float32,
        DataType::Float64 => Kind::Float64,
        DataType::Boolean => Kind::Boolean,
        DataType::Date32 => Kind::Date32,
        DataType::Date64 => Kind::Date64,
        // The units Arrow defines for each: it builds no array of any other.
        DataType::Time32(unit @ (TimeUnit::Second | TimeUnit::Millisecond)) => Kind::Time32(*unit),
        DataType::Time64(unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond)) => {
            Kind::Time64(*unit)
        }
        DataType::Timestamp(unit, _) => Kind::Timestamp(*unit),
        DataType::Duration(unit) => Kind::Duration(*unit),
        DataType::Decimal128(_, _) => Kind::Decimal128,
        DataType::Decimal256(_, _) => Kind::Decimal256,
        DataType::Interval(unit) => Kind::Interval(*unit),
        DataType::Utf8 => Kind::Utf8,
        DataType::LargeUtf8 => Kind::LargeUtf8,
        DataType::Utf8View => Kind::Utf8View,
        DataType::Binary => Kind::Binary,
        DataType::LargeBinary => Kind::LargeBinary,
        DataType::BinaryView => Kind::BinaryView,
        DataType::FixedSizeBinary(width) => Kind::FixedSizeBinary(*width),
        _ => return None,
    })
}

/// The type a column of `column_type` is keyed by: its own, or a dictionary
/// column's value type, since equal values must share a key whatever
/// dictionary holds them.
#[must_use]
pub fn key_type(column_type: &DataType) -> &DataType {
    match column_type {
        DataType::Dictionary(_, value) => value,
        other => other,
    }
}

/// Whether a column of `column_type` can be part of a key, keyed by its
/// [`key_type`].
#[must_use]
pub fn can_key(column_type: &DataType) -> bool {
    kind(key_type(column_type)).is_some()
}

impl Kind {
    /// The encoded width of a value when it is fixed, for the kinds whose
    /// encoding is fixed-width and that may fold into an exact word. The
    /// encoding is injective on values, except that floats give `-0.0` and
    /// `0.0`, and every NaN, one encoding each (see the module docs). Others return `None` and are hashed, which is
    /// always correct.
    fn fixed_width(self) -> Option<usize> {
        match self {
            Self::Int8 | Self::UInt8 | Self::Boolean => Some(1),
            Self::Int16 | Self::UInt16 | Self::Float16 => Some(2),
            Self::Int32 | Self::UInt32 | Self::Float32 | Self::Date32 | Self::Time32(_) => Some(4),
            Self::Int64
            | Self::UInt64
            | Self::Float64
            | Self::Date64
            | Self::Time64(_)
            | Self::Timestamp(_)
            | Self::Duration(_) => Some(8),
            Self::FixedSizeBinary(width) => usize::try_from(width).ok(),
            Self::Decimal128
            | Self::Decimal256
            | Self::Interval(_)
            | Self::Utf8
            | Self::LargeUtf8
            | Self::Utf8View
            | Self::Binary
            | Self::LargeBinary
            | Self::BinaryView => None,
        }
    }
}

impl WordRule {
    fn of(fields: &[KeyField], kinds: &[Kind]) -> Self {
        let layout: Option<Vec<(bool, usize)>> = fields
            .iter()
            .zip(kinds)
            .map(|(field, kind)| kind.fixed_width().map(|width| (field.nullable, width)))
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
    /// [`crate::Error::UnsupportedType`] when a field's type has no key
    /// encoding here.
    pub fn new(fields: Vec<KeyField>) -> Result<Self> {
        let kinds = fields
            .iter()
            .map(|field| {
                kind(&field.data_type).context(UnsupportedTypeSnafu {
                    data_type: field.data_type.to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let words = WordRule::of(&fields, &kinds);
        Ok(Self {
            fields,
            kinds,
            words,
        })
    }

    /// The key fields.
    #[must_use]
    pub fn fields(&self) -> &[KeyField] {
        &self.fields
    }

    /// Identifies the words this encoder gives keys: two encoders with the same
    /// identity give every key the same word. It covers each field's type,
    /// whether the field is nullable (a nullable field encodes each value
    /// behind a validity byte), and how keys become words. A run records the
    /// identity of the encoder that built it and an index publishes only runs
    /// of its own, so a run built under another encoding, whose words match
    /// none of the index's keys, never answers a lookup.
    #[must_use]
    pub fn word_identity(&self) -> u64 {
        let mut descriptor = Vec::new();
        for field in &self.fields {
            descriptor.extend_from_slice(field.data_type.to_string().as_bytes());
            // A type's name never contains a control byte, so the separator
            // and the nullability after it keep each field's part distinct.
            descriptor.extend_from_slice(&[0, u8::from(field.nullable)]);
        }
        match &self.words {
            WordRule::Exact(_) => descriptor.push(0),
            WordRule::Hashed { bits } => {
                descriptor.push(1);
                descriptor.extend_from_slice(&bits.to_le_bytes());
            }
        }
        hash_index::hash_key_bytes_oneshot(&descriptor)
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
        match &self.words {
            WordRule::Hashed { bits } => hashed(key) & (u64::MAX >> (64 - bits)),
            WordRule::Exact(layout) => {
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
                    crate::word_proof::fold_word(&bytes[..len])
                } else {
                    hashed(key)
                }
            }
        }
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
    #[cfg(test)]
    pub(crate) fn exact_words(&self) -> bool {
        matches!(self.words, WordRule::Exact(_))
    }

    /// Bind one array per key field, all of the same length.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ColumnCount`], [`crate::Error::ColumnType`],
    /// [`crate::Error::ColumnLength`] or [`crate::Error::UnexpectedNull`] when
    /// the number of arrays, a type, a length, or a NULL in a non-nullable
    /// field does not match.
    pub fn bind<'a>(&self, columns: &'a [ArrayRef]) -> Result<BoundKeyColumns<'a>> {
        ensure!(
            columns.len() == self.fields.len(),
            ColumnCountSnafu {
                expected: self.fields.len(),
                received: columns.len(),
            }
        );
        let num_rows = columns
            .first()
            .map_or(0, |column| Array::len(column.as_ref()));
        let mut bound = Vec::with_capacity(columns.len());
        for (index, ((column, field), &kind)) in columns
            .iter()
            .zip(&self.fields)
            .zip(&self.kinds)
            .enumerate()
        {
            ensure!(
                column.data_type() == &field.data_type,
                ColumnTypeSnafu {
                    index,
                    found: column.data_type().clone(),
                    declared: field.data_type.clone(),
                }
            );
            ensure!(
                column.len() == num_rows,
                ColumnLengthSnafu {
                    index,
                    rows: column.len(),
                    expected: num_rows,
                }
            );
            ensure!(
                field.nullable || column.null_count() == 0,
                UnexpectedNullSnafu { index }
            );
            bound.push(BoundColumn {
                data: ColumnData::new(column.as_ref(), kind),
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
    F16(&'a [<Float16Type as arrow_array::ArrowPrimitiveType>::Native]),
    F32(&'a [f32]),
    F64(&'a [f64]),
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
    /// The values of `array`, whose type `bind` checked is the field's, so
    /// `kind` is its type's kind.
    fn new(array: &'a dyn Array, kind: Kind) -> Self {
        match kind {
            Kind::Int8 => Self::I8(array.as_primitive::<Int8Type>().values()),
            Kind::Int16 => Self::I16(array.as_primitive::<Int16Type>().values()),
            Kind::Int32 => Self::I32(array.as_primitive::<Int32Type>().values()),
            Kind::Int64 => Self::I64(array.as_primitive::<Int64Type>().values()),
            Kind::UInt8 => Self::U8(array.as_primitive::<UInt8Type>().values()),
            Kind::UInt16 => Self::U16(array.as_primitive::<UInt16Type>().values()),
            Kind::UInt32 => Self::U32(array.as_primitive::<UInt32Type>().values()),
            Kind::UInt64 => Self::U64(array.as_primitive::<UInt64Type>().values()),
            Kind::Float16 => Self::F16(array.as_primitive::<Float16Type>().values()),
            Kind::Float32 => Self::F32(array.as_primitive::<Float32Type>().values()),
            Kind::Float64 => Self::F64(array.as_primitive::<Float64Type>().values()),
            Kind::Boolean => Self::Bool(array.as_boolean()),
            Kind::Date32 => Self::I32(array.as_primitive::<Date32Type>().values()),
            Kind::Date64 => Self::I64(array.as_primitive::<Date64Type>().values()),
            Kind::Time32(TimeUnit::Second) => {
                Self::I32(array.as_primitive::<Time32SecondType>().values())
            }
            Kind::Time32(_) => Self::I32(array.as_primitive::<Time32MillisecondType>().values()),
            Kind::Time64(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<Time64NanosecondType>().values())
            }
            Kind::Time64(_) => Self::I64(array.as_primitive::<Time64MicrosecondType>().values()),
            Kind::Timestamp(TimeUnit::Second) => {
                Self::I64(array.as_primitive::<TimestampSecondType>().values())
            }
            Kind::Timestamp(TimeUnit::Millisecond) => {
                Self::I64(array.as_primitive::<TimestampMillisecondType>().values())
            }
            Kind::Timestamp(TimeUnit::Microsecond) => {
                Self::I64(array.as_primitive::<TimestampMicrosecondType>().values())
            }
            Kind::Timestamp(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<TimestampNanosecondType>().values())
            }
            Kind::Duration(TimeUnit::Second) => {
                Self::I64(array.as_primitive::<DurationSecondType>().values())
            }
            Kind::Duration(TimeUnit::Millisecond) => {
                Self::I64(array.as_primitive::<DurationMillisecondType>().values())
            }
            Kind::Duration(TimeUnit::Microsecond) => {
                Self::I64(array.as_primitive::<DurationMicrosecondType>().values())
            }
            Kind::Duration(TimeUnit::Nanosecond) => {
                Self::I64(array.as_primitive::<DurationNanosecondType>().values())
            }
            Kind::Decimal128 => Self::I128(array.as_primitive::<Decimal128Type>().values()),
            Kind::Decimal256 => Self::I256(array.as_primitive::<Decimal256Type>().values()),
            Kind::Interval(IntervalUnit::YearMonth) => {
                Self::I32(array.as_primitive::<IntervalYearMonthType>().values())
            }
            Kind::Interval(IntervalUnit::DayTime) => {
                Self::DayTime(array.as_primitive::<IntervalDayTimeType>().values())
            }
            Kind::Interval(IntervalUnit::MonthDayNano) => {
                Self::MonthDayNano(array.as_primitive::<IntervalMonthDayNanoType>().values())
            }
            Kind::Utf8 => Self::Utf8(array.as_string::<i32>()),
            Kind::LargeUtf8 => Self::LargeUtf8(array.as_string::<i64>()),
            Kind::Utf8View => Self::Utf8View(array.as_string_view()),
            Kind::Binary => Self::Binary(array.as_binary::<i32>()),
            Kind::LargeBinary => Self::LargeBinary(array.as_binary::<i64>()),
            Kind::BinaryView => Self::BinaryView(array.as_binary_view()),
            Kind::FixedSizeBinary(_) => Self::FixedBinary(array.as_fixed_size_binary()),
        }
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

impl BoundKeyColumns<'_> {
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

    /// Append the key of `row` to `out`. `row` must be below
    /// [`Self::num_rows`].
    #[inline]
    pub fn encode_row(&self, row: usize, out: &mut Vec<u8>) {
        debug_assert!(row < self.num_rows);
        for column in &self.columns {
            column.encode(row, out);
        }
    }

    /// Every row's key, concatenated, with `offsets[i]..offsets[i + 1]` the
    /// range of row `i`.
    #[cfg(test)]
    pub(crate) fn encode_all(&self) -> (Vec<u8>, Vec<usize>) {
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

impl BoundColumn<'_> {
    /// Append this column's encoding of `row` to `out`.
    #[inline]
    fn encode(&self, row: usize, out: &mut Vec<u8>) {
        if self.nulls.is_some_and(|nulls| nulls.is_null(row)) {
            out.push(NULL_MARK);
            return;
        }
        if self.nullable {
            out.push(VALID_MARK);
        }
        match self.data {
            ColumnData::I8(v) => out.push(v[row].cast_unsigned() ^ 0x80),
            ColumnData::I16(v) => {
                out.extend_from_slice(&(v[row].cast_unsigned() ^ (1 << 15)).to_be_bytes());
            }
            ColumnData::I32(v) => {
                out.extend_from_slice(&(v[row].cast_unsigned() ^ (1 << 31)).to_be_bytes());
            }
            ColumnData::I64(v) => {
                out.extend_from_slice(&(v[row].cast_unsigned() ^ (1 << 63)).to_be_bytes());
            }
            ColumnData::I128(v) => {
                out.extend_from_slice(&(v[row].cast_unsigned() ^ (1 << 127)).to_be_bytes());
            }
            ColumnData::I256(v) => {
                let mut bytes = v[row].to_be_bytes();
                bytes[0] ^= 0x80;
                out.extend_from_slice(&bytes);
            }
            ColumnData::DayTime(v) => {
                out.extend_from_slice(&(v[row].days.cast_unsigned() ^ (1 << 31)).to_be_bytes());
                out.extend_from_slice(
                    &(v[row].milliseconds.cast_unsigned() ^ (1 << 31)).to_be_bytes(),
                );
            }
            ColumnData::MonthDayNano(v) => {
                out.extend_from_slice(&(v[row].months.cast_unsigned() ^ (1 << 31)).to_be_bytes());
                out.extend_from_slice(&(v[row].days.cast_unsigned() ^ (1 << 31)).to_be_bytes());
                out.extend_from_slice(
                    &(v[row].nanoseconds.cast_unsigned() ^ (1 << 63)).to_be_bytes(),
                );
            }
            ColumnData::U8(v) => out.push(v[row]),
            ColumnData::U16(v) => out.extend_from_slice(&v[row].to_be_bytes()),
            ColumnData::U32(v) => out.extend_from_slice(&v[row].to_be_bytes()),
            ColumnData::U64(v) => out.extend_from_slice(&v[row].to_be_bytes()),
            ColumnData::F16(v) => put_float(out, u64::from(v[row].to_bits()), 16, 5),
            ColumnData::F32(v) => put_float(out, u64::from(v[row].to_bits()), 32, 8),
            ColumnData::F64(v) => put_float(out, v[row].to_bits(), 64, 11),
            ColumnData::Bool(v) => out.push(u8::from(v.value(row))),
            ColumnData::FixedBinary(v) => out.extend_from_slice(v.value(row)),
            ColumnData::Binary(v) => escape_value_into(v.value(row), out),
            ColumnData::LargeBinary(v) => escape_value_into(v.value(row), out),
            ColumnData::Utf8(v) => escape_value_into(v.value(row).as_bytes(), out),
            ColumnData::LargeUtf8(v) => escape_value_into(v.value(row).as_bytes(), out),
            ColumnData::Utf8View(v) => escape_value_into(v.value(row).as_bytes(), out),
            ColumnData::BinaryView(v) => escape_value_into(v.value(row), out),
        }
    }
}
