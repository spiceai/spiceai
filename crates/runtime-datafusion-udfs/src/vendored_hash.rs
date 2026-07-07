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
//!
//! Caveat: this module freezes the *seed and the hashing loop*, but still
//! delegates the actual byte hashing to the external `ahash` crate (pinned only
//! as `^0.8`). ahash gives **no** cross-version output guarantee, so a routine
//! `cargo update` could still silently change these hashes (#11277).
//! `bucket::tests::test_bucket_hash_stability_golden_values` pins the current
//! output for every supported type so any such version drift fails CI loudly
//! instead of silently re-bucketing persisted data. Keep that guard passing;
//! only regenerate its goldens as part of a deliberate, format-versioned
//! migration.
//!
//! The other half of #11277 — a build that enables ahash's AES path — is closed
//! by the `compile_error!` guard below. ahash's `aes_hash::AHasher` and
//! `fallback_hash::AHasher` produce **different** `hash_one` output from the
//! same `RandomState` seed, so a build compiled for x86/x86_64 with
//! `target_feature = "aes"` (e.g. `-C target-cpu=native`) would re-bucket every
//! persisted partitioned dataset relative to the shipped fallback-hashed builds.
//! CI never sets `+aes`, so the golden test alone cannot catch it — the guard
//! turns that silent corruption into a loud build failure instead.

#![allow(clippy::pedantic)]

// Mirror the exact cfg ahash uses to switch `RandomState`/`AHasher` to its
// AES-accelerated (and thus output-incompatible) implementation on x86 — see
// `ahash::AHasher` selection in ahash's `lib.rs`. The arm AES paths additionally
// require ahash's `nightly-arm-aes` feature, which this workspace never enables,
// so only the x86 condition can activate through our dependency graph.
#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "aes",
    not(miri)
))]
compile_error!(
    "runtime-datafusion-udfs is being compiled with ahash's AES hasher active \
     (x86/x86_64 + target_feature=\"aes\", e.g. `-C target-cpu=native`). ahash's \
     AES and fallback hashers produce different output, so this would silently \
     re-bucket the bucket() partition transform relative to shipped builds and \
     mis-prune persisted partitioned datasets (#11277). Build this crate without \
     the `aes` target-feature (drop `target-cpu=native` or add `-C \
     target-feature=-aes`), or make the bucket() hash version-independent as a \
     deliberate, format-versioned migration."
);

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
