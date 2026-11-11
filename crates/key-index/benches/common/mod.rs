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

//! Shared by the benches: key shapes, encoded keys, percentiles, and an
//! allocator that counts requested and allocated bytes.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use key_index::{KeyEncoder, KeyField};

/// Bytes requested from the allocator and not yet freed.
pub static REQUESTED: AtomicIsize = AtomicIsize::new(0);
/// Bytes the allocator handed out for them (its size classes included),
/// where the platform reports it; otherwise the requested bytes.
pub static HEAP: AtomicIsize = AtomicIsize::new(0);

#[cfg(target_os = "macos")]
fn usable(ptr: *mut u8, _requested: usize) -> isize {
    unsafe extern "C" {
        fn malloc_size(ptr: *const std::ffi::c_void) -> usize;
    }
    // SAFETY: `ptr` is a live allocation from the system allocator.
    unsafe { malloc_size(ptr.cast()) }.cast_signed()
}

#[cfg(not(target_os = "macos"))]
fn usable(_ptr: *mut u8, requested: usize) -> isize {
    requested.cast_signed()
}

/// Counts [`REQUESTED`] and [`HEAP`] around the system allocator.
pub struct Counting;

// SAFETY: forwards every call to `System` unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            REQUESTED.fetch_add(layout.size().cast_signed(), Ordering::Relaxed);
            HEAP.fetch_add(usable(ptr, layout.size()), Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            REQUESTED.fetch_add(layout.size().cast_signed(), Ordering::Relaxed);
            HEAP.fetch_add(usable(ptr, layout.size()), Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        REQUESTED.fetch_sub(layout.size().cast_signed(), Ordering::Relaxed);
        HEAP.fetch_sub(usable(ptr, layout.size()), Ordering::Relaxed);
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old = usable(ptr, layout.size());
        // SAFETY: forwarded unchanged.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            REQUESTED.fetch_add(
                new_size.cast_signed() - layout.size().cast_signed(),
                Ordering::Relaxed,
            );
            HEAP.fetch_add(usable(new, new_size) - old, Ordering::Relaxed);
        }
        new
    }
}

/// `(requested, heap)` bytes now.
pub fn bytes() -> (isize, isize) {
    (
        REQUESTED.load(Ordering::Relaxed),
        HEAP.load(Ordering::Relaxed),
    )
}

/// SplitMix64.
pub fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// The key shapes the benches run: a random integer, a UUID string, and an
/// (integer, string) pair with a small leading column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    I64Rand,
    Utf8Uuid,
    I64Utf8,
}

impl Shape {
    pub const ALL: [Shape; 3] = [Shape::I64Rand, Shape::Utf8Uuid, Shape::I64Utf8];

    pub fn name(self) -> &'static str {
        match self {
            Shape::I64Rand => "i64_rand",
            Shape::Utf8Uuid => "utf8_uuid",
            Shape::I64Utf8 => "i64_utf8",
        }
    }

    /// The shapes named in `BENCH_SHAPES` (comma-separated), or all.
    pub fn selected() -> Vec<Shape> {
        let names = std::env::var("BENCH_SHAPES").unwrap_or_default();
        Shape::ALL
            .into_iter()
            .filter(|s| names.is_empty() || names.split(',').any(|n| n == s.name()))
            .collect()
    }

    pub fn columns(self, ids: &[u64]) -> (Vec<KeyField>, Vec<ArrayRef>) {
        match self {
            Shape::I64Rand => (
                vec![KeyField::new(DataType::Int64, false)],
                vec![Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|&id| mix(id).cast_signed()),
                ))],
            ),
            Shape::Utf8Uuid => (
                vec![KeyField::new(DataType::Utf8, false)],
                vec![Arc::new(StringArray::from_iter_values(ids.iter().map(
                    |&id| {
                        let (a, b) = (mix(id), mix(id ^ 0x5555_5555_5555_5555));
                        format!(
                            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                            a >> 32,
                            (a >> 16) & 0xffff,
                            a & 0xffff,
                            b >> 48,
                            b & 0xffff_ffff_ffff
                        )
                    },
                )))],
            ),
            Shape::I64Utf8 => (
                vec![
                    KeyField::new(DataType::Int64, false),
                    KeyField::new(DataType::Utf8, false),
                ],
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        ids.iter().map(|&id| (mix(id) % 100).cast_signed()),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| format!("order-{id:010}")),
                    )),
                ],
            ),
        }
    }

    /// The encoded keys of rows `ids`.
    pub fn keys(self, ids: &[u64]) -> Vec<Vec<u8>> {
        let (fields, columns) = self.columns(ids);
        let encoder = KeyEncoder::new(fields).expect("supported key types");
        let bound = encoder.bind(&columns).expect("columns match");
        (0..ids.len())
            .map(|row| {
                let mut key = Vec::new();
                bound.encode_row(row, &mut key);
                key
            })
            .collect()
    }
}

/// `n` distinct ids in a scattered order.
pub fn ids(n: usize) -> Vec<u64> {
    let mut ids: Vec<u64> = (0..n as u64).collect();
    ids.sort_unstable_by_key(|&id| mix(id ^ 0xabcd));
    ids
}

/// An environment variable as a number, or `default`.
pub fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `p50 / p99` of `samples`.
pub fn p50_p99(samples: &mut [f64], digits: usize) -> String {
    samples.sort_by(f64::total_cmp);
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    format!("{:.digits$} / {:.digits$}", at(0.5), at(0.99))
}
