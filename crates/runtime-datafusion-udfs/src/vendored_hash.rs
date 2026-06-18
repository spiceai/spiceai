// Copyright 2024-2025 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frozen, DataFusion-independent hashing for the `bucket()` partitioning UDF.
//!
//! `bucket(n, value) = hash(value) % n` is a *partitioning* primitive: a given
//! value must map to the same bucket for the lifetime of a dataset, including
//! across DataFusion upgrades, or persisted partitioned data (e.g. Cayenne
//! file-mode datasets) would be silently mis-pruned.
//!
//! DataFusion's own `create_hashes` carries **no** cross-version stability
//! guarantee: DataFusion 54 switched its hasher from `ahash` to `foldhash` and
//! changed the multi-column combine, which would re-bucket every existing
//! partitioned dataset. To stay backward-compatible, this module pins the
//! DataFusion 53 hashing (`ahash` with a fixed seed) independently of
//! DataFusion. It is a faithful, frozen copy of the single-column path of
//! DataFusion 53's `datafusion_common::hash_utils` (the `HashValue` impls plus
//! the per-element hashing loop). **Do not "modernize" it** — its sole purpose
//! is to never change.
//!
//! `bucket()` only ever hashes a single column, so the multi-column
//! `combine_hashes` path is intentionally omitted.

#![allow(clippy::pedantic)]

pub use ahash::RandomState;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::downcast_primitive_array;
use datafusion::common::{Result, internal_err};

/// Per-value hashing, identical to DataFusion 53's `HashValue`.
pub trait HashValue {
    fn hash_one(&self, state: &RandomState) -> u64;
}

impl<T: HashValue + ?Sized> HashValue for &T {
    fn hash_one(&self, state: &RandomState) -> u64 {
        T::hash_one(self, state)
    }
}

macro_rules! hash_value {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }
        })+
    };
}
hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128);
hash_value!(bool, str, [u8], IntervalDayTime, IntervalMonthDayNano);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));

/// Single-column hashing: non-null elements take `value.hash_one(state)`; null
/// elements are left untouched (so `bucket()` callers that zero-initialise the
/// buffer get bucket `0` for nulls — matching DataFusion 53).
fn hash_primitive<T>(array: &PrimitiveArray<T>, random_state: &RandomState, hashes: &mut [u64])
where
    T: ArrowPrimitiveType,
    T::Native: HashValue,
{
    if array.null_count() == 0 {
        for (hash, &value) in hashes.iter_mut().zip(array.values().iter()) {
            *hash = value.hash_one(random_state);
        }
    } else {
        for (i, hash) in hashes.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one(random_state);
            }
        }
    }
}

fn hash_accessor<T>(array: T, random_state: &RandomState, hashes: &mut [u64])
where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    if array.null_count() == 0 {
        for (i, hash) in hashes.iter_mut().enumerate() {
            let value = unsafe { array.value_unchecked(i) };
            *hash = value.hash_one(random_state);
        }
    } else {
        for (i, hash) in hashes.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one(random_state);
            }
        }
    }
}

/// Stable, DataFusion-independent equivalent of `create_hashes` for the
/// `bucket()` UDF, restricted to a single column. `hashes` must already be the
/// same length as `array` (and zero-initialised for correct null handling).
pub fn create_hashes(
    array: &dyn Array,
    random_state: &RandomState,
    hashes: &mut [u64],
) -> Result<()> {
    debug_assert_eq!(array.len(), hashes.len());
    downcast_primitive_array! {
        array => hash_primitive(array, random_state, hashes),
        DataType::Boolean => hash_accessor(array.as_boolean(), random_state, hashes),
        DataType::Utf8 => hash_accessor(array.as_string::<i32>(), random_state, hashes),
        DataType::LargeUtf8 => hash_accessor(array.as_string::<i64>(), random_state, hashes),
        DataType::Utf8View => hash_accessor(array.as_string_view(), random_state, hashes),
        DataType::Binary => hash_accessor(array.as_binary::<i32>(), random_state, hashes),
        DataType::LargeBinary => hash_accessor(array.as_binary::<i64>(), random_state, hashes),
        DataType::BinaryView => hash_accessor(array.as_binary_view(), random_state, hashes),
        other => {
            return internal_err!(
                "bucket() does not support partitioning by column type: {other}"
            );
        }
    }
    Ok(())
}
