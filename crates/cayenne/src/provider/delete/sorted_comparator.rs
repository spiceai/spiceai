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

//! Native tuple comparators for sorted deletion application.

use crate::row_converter::SortField;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray, BinaryViewArray,
    FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    make_comparator,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    ArrowNativeType, DataType, Date32Type, Date64Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion_common::{DataFusionError, Result};
use std::cmp::Ordering;

pub(super) trait KeyCompare: Sized {
    fn new(left: &[ArrayRef], right: &[ArrayRef], fields: &[SortField]) -> Result<Self>;
    fn compare(&self, a: usize, b: usize) -> Ordering;
}

struct NullOrder {
    nulls: Option<(Option<NullBuffer>, Option<NullBuffer>)>,
    options: SortOptions,
}

impl NullOrder {
    fn new(left: &dyn Array, right: &dyn Array, options: SortOptions) -> Self {
        let nulls = |array: &dyn Array| {
            (array.null_count() != 0)
                .then(|| array.nulls().cloned())
                .flatten()
        };
        let (left, right) = (nulls(left), nulls(right));
        Self {
            nulls: (left.is_some() || right.is_some()).then_some((left, right)),
            options,
        }
    }

    #[inline]
    fn compare(&self, a: usize, b: usize, value: impl FnOnce() -> Ordering) -> Ordering {
        if let Some((left, right)) = &self.nulls {
            let l = left.as_ref().is_some_and(|n| n.is_null(a));
            let r = right.as_ref().is_some_and(|n| n.is_null(b));
            match (l, r) {
                (true, true) => return Ordering::Equal,
                (true, false) => {
                    return if self.options.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (false, true) => {
                    return if self.options.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                (false, false) => {}
            }
        }
        let order = value();
        if self.options.descending {
            order.reverse()
        } else {
            order
        }
    }
}

trait NativeValue: ArrowNativeType {
    fn compare(self, other: Self) -> Ordering;
}

macro_rules! integer_values {
    ($($t:ty),*) => { $(impl NativeValue for $t {
        #[inline]
        fn compare(self, other: Self) -> Ordering { self.cmp(&other) }
    })* };
}
integer_values!(i8, i16, i32, i64, u8, u16, u32, u64);
impl NativeValue for f32 {
    #[inline]
    fn compare(self, other: Self) -> Ordering {
        self.total_cmp(&other)
    }
}
impl NativeValue for f64 {
    #[inline]
    fn compare(self, other: Self) -> Ordering {
        self.total_cmp(&other)
    }
}

struct NativePair<V: NativeValue> {
    left: ScalarBuffer<V>,
    right: ScalarBuffer<V>,
    order: NullOrder,
}

impl<V: NativeValue> NativePair<V> {
    fn new<T: ArrowPrimitiveType<Native = V>>(
        left: &dyn Array,
        right: &dyn Array,
        options: SortOptions,
    ) -> Result<Self> {
        let left = left.as_primitive_opt::<T>().ok_or_else(|| {
            DataFusionError::Internal("Invalid native deletion comparator left array".into())
        })?;
        let right = right.as_primitive_opt::<T>().ok_or_else(|| {
            DataFusionError::Internal("Invalid native deletion comparator right array".into())
        })?;
        Ok(Self {
            left: left.values().clone(),
            right: right.values().clone(),
            order: NullOrder::new(left, right, options),
        })
    }

    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.order
            .compare(a, b, || self.left[a].compare(self.right[b]))
    }
}

trait ByteValues: Array + Clone {
    fn bytes(&self, i: usize) -> &[u8];
}
macro_rules! byte_values {
    ($($t:ty),*) => { $(impl ByteValues for $t {
        #[inline]
        fn bytes(&self, i: usize) -> &[u8] { self.value(i).as_ref() }
    })* };
}
byte_values!(
    StringArray,
    LargeStringArray,
    StringViewArray,
    BinaryArray,
    LargeBinaryArray,
    BinaryViewArray,
    FixedSizeBinaryArray
);

struct BytePair<A: ByteValues> {
    left: A,
    right: A,
    order: NullOrder,
}
impl<A: ByteValues + 'static> BytePair<A> {
    fn new(left: &dyn Array, right: &dyn Array, options: SortOptions) -> Result<Self> {
        let left = left.as_any().downcast_ref::<A>().ok_or_else(|| {
            DataFusionError::Internal("Invalid byte deletion comparator left array".into())
        })?;
        let right = right.as_any().downcast_ref::<A>().ok_or_else(|| {
            DataFusionError::Internal("Invalid byte deletion comparator right array".into())
        })?;
        Ok(Self {
            left: left.clone(),
            right: right.clone(),
            order: NullOrder::new(left, right, options),
        })
    }
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.order
            .compare(a, b, || self.left.bytes(a).cmp(self.right.bytes(b)))
    }
}

struct BooleanPair {
    left: BooleanBuffer,
    right: BooleanBuffer,
    order: NullOrder,
}
impl BooleanPair {
    fn new(left: &dyn Array, right: &dyn Array, options: SortOptions) -> Result<Self> {
        let left = left.as_boolean_opt().ok_or_else(|| {
            DataFusionError::Internal("Invalid Boolean deletion comparator left array".into())
        })?;
        let right = right.as_boolean_opt().ok_or_else(|| {
            DataFusionError::Internal("Invalid Boolean deletion comparator right array".into())
        })?;
        Ok(Self {
            left: left.values().clone(),
            right: right.values().clone(),
            order: NullOrder::new(left, right, options),
        })
    }
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.order
            .compare(a, b, || self.left.value(a).cmp(&self.right.value(b)))
    }
}

macro_rules! column_comparator {
    ($($variant:ident($pair:ty)),*) => {
        enum ColumnComparator {
            $($variant($pair),)*
            Other(arrow::array::DynComparator),
        }
        impl ColumnComparator {
            #[inline]
            fn compare(&self, a: usize, b: usize) -> Ordering {
                match self { $(Self::$variant(pair) => pair.compare(a, b),)* Self::Other(compare) => compare(a, b) }
            }
        }
    };
}
column_comparator!(
    I8(NativePair<i8>),
    I16(NativePair<i16>),
    I32(NativePair<i32>),
    I64(NativePair<i64>),
    U8(NativePair<u8>),
    U16(NativePair<u16>),
    U32(NativePair<u32>),
    U64(NativePair<u64>),
    F32(NativePair<f32>),
    F64(NativePair<f64>),
    Bool(BooleanPair),
    Utf8(BytePair<StringArray>),
    LargeUtf8(BytePair<LargeStringArray>),
    Utf8View(BytePair<StringViewArray>),
    Binary(BytePair<BinaryArray>),
    LargeBinary(BytePair<LargeBinaryArray>),
    BinaryView(BytePair<BinaryViewArray>),
    FixedBinary(BytePair<FixedSizeBinaryArray>)
);

impl ColumnComparator {
    fn new(left: &dyn Array, right: &dyn Array, options: SortOptions) -> Result<Self> {
        if left.data_type() != right.data_type() {
            return Err(DataFusionError::Internal(
                "Sorted deletion comparator array types differ".into(),
            ));
        }
        macro_rules! primitive {
            ($variant:ident, $t:ty) => {
                Self::$variant(NativePair::new::<$t>(left, right, options)?)
            };
        }
        Ok(match left.data_type() {
            DataType::Int8 => primitive!(I8, Int8Type),
            DataType::Int16 => primitive!(I16, Int16Type),
            DataType::Int32 => primitive!(I32, Int32Type),
            DataType::Int64 => primitive!(I64, Int64Type),
            DataType::UInt8 => primitive!(U8, UInt8Type),
            DataType::UInt16 => primitive!(U16, UInt16Type),
            DataType::UInt32 => primitive!(U32, UInt32Type),
            DataType::UInt64 => primitive!(U64, UInt64Type),
            DataType::Float32 => primitive!(F32, Float32Type),
            DataType::Float64 => primitive!(F64, Float64Type),
            DataType::Date32 => primitive!(I32, Date32Type),
            DataType::Date64 => primitive!(I64, Date64Type),
            DataType::Time32(TimeUnit::Second) => primitive!(I32, Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => primitive!(I32, Time32MillisecondType),
            DataType::Time64(TimeUnit::Microsecond) => primitive!(I64, Time64MicrosecondType),
            DataType::Time64(TimeUnit::Nanosecond) => primitive!(I64, Time64NanosecondType),
            DataType::Timestamp(TimeUnit::Second, _) => primitive!(I64, TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                primitive!(I64, TimestampMillisecondType)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                primitive!(I64, TimestampMicrosecondType)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                primitive!(I64, TimestampNanosecondType)
            }
            DataType::Boolean => Self::Bool(BooleanPair::new(left, right, options)?),
            DataType::Utf8 => Self::Utf8(BytePair::new(left, right, options)?),
            DataType::LargeUtf8 => Self::LargeUtf8(BytePair::new(left, right, options)?),
            DataType::Utf8View => Self::Utf8View(BytePair::new(left, right, options)?),
            DataType::Binary => Self::Binary(BytePair::new(left, right, options)?),
            DataType::LargeBinary => Self::LargeBinary(BytePair::new(left, right, options)?),
            DataType::BinaryView => Self::BinaryView(BytePair::new(left, right, options)?),
            DataType::FixedSizeBinary(_) => Self::FixedBinary(BytePair::new(left, right, options)?),
            _ => Self::Other(make_comparator(left, right, options)?),
        })
    }
}

pub(super) struct TypedComparator(Vec<ColumnComparator>);
impl KeyCompare for TypedComparator {
    fn new(left: &[ArrayRef], right: &[ArrayRef], fields: &[SortField]) -> Result<Self> {
        if left.len() != fields.len() || right.len() != fields.len() {
            return Err(DataFusionError::Internal(
                "Sorted deletion primary key column count differs from the row converter".into(),
            ));
        }
        Ok(Self(
            left.iter()
                .zip(right)
                .zip(fields)
                .map(|((l, r), f)| ColumnComparator::new(l.as_ref(), r.as_ref(), f.options))
                .collect::<Result<_>>()?,
        ))
    }
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        for column in &self.0 {
            let order = column.compare(a, b);
            if !order.is_eq() {
                return order;
            }
        }
        Ordering::Equal
    }
}

enum IntegerColumn {
    I32(NativePair<i32>),
    I64(NativePair<i64>),
}
impl IntegerColumn {
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        match self {
            Self::I32(pair) => pair.compare(a, b),
            Self::I64(pair) => pair.compare(a, b),
        }
    }
}

/// Fixed tuple width lets the compiler specialize the column loop; no column
/// carries a dynamic comparison closure, including mixed Int32/Int64 tuples.
pub(super) struct IntegerComparator<const N: usize>([IntegerColumn; N]);
impl<const N: usize> KeyCompare for IntegerComparator<N> {
    fn new(left: &[ArrayRef], right: &[ArrayRef], fields: &[SortField]) -> Result<Self> {
        if left.len() != N || right.len() != N || fields.len() != N {
            return Err(DataFusionError::Internal(
                "Integer deletion comparator tuple width differs".into(),
            ));
        }
        let columns = left
            .iter()
            .zip(right)
            .zip(fields)
            .map(
                |((l, r), f)| match ColumnComparator::new(l.as_ref(), r.as_ref(), f.options)? {
                    ColumnComparator::I32(pair) => Ok(IntegerColumn::I32(pair)),
                    ColumnComparator::I64(pair) => Ok(IntegerColumn::I64(pair)),
                    _ => Err(DataFusionError::Internal(
                        "Integer deletion comparator requires Int32 or Int64 columns".into(),
                    )),
                },
            )
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(columns.try_into().map_err(|_| {
            DataFusionError::Internal("Integer deletion comparator tuple width differs".into())
        })?))
    }
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        for column in &self.0 {
            let order = column.compare(a, b);
            if !order.is_eq() {
                return order;
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
impl TypedComparator {
    pub(super) fn fallback_columns(&self) -> usize {
        self.0
            .iter()
            .filter(|c| matches!(c, ColumnComparator::Other(_)))
            .count()
    }
}
