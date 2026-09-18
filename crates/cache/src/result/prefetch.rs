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

//! Software prefetch for a Raw results-cache serve.
//!
//! A hit returns a stream whose first `poll_next` is typically followed
//! immediately by the consumer reading column buffers (HTTP JSON, Flight IPC).
//! Touching those lines here, before the caller is handed the stream, hides
//! the compulsory misses of that first batch — and the next batch's headers
//! so the following poll's `Arc` clone and encode see warm lines.

use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};

/// Cache-line size used when walking a buffer. Prefetch is a hint; being off
/// by a line only wastes a prefetch, it does not change results.
const CACHE_LINE: usize = 64;

/// How many leading columns of a batch have their data buffers prefetched.
/// Wide results (hundreds of columns) would otherwise spend the serve path
/// walking every buffer; the consumer's first reads are the first columns.
const PREFETCH_DATA_COLUMNS: usize = 8;

/// How many cache lines of each selected buffer to request.
const PREFETCH_LINES_PER_BUFFER: usize = 2;

/// Software prefetch of `ptr`'s cache line for a soon-to-follow read.
///
/// Equivalent to the `prefetch_read_data` intrinsic on nightly: a hint on
/// `x86_64`/`aarch64`, and a single volatile byte load elsewhere. The pointer
/// must be valid to dereference on the fallback path; callers only pass
/// Arrow buffer / struct pointers they already own.
#[inline]
pub(crate) fn prefetch_read_data(ptr: *const u8) {
    if ptr.is_null() {
        return;
    }

    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `_mm_prefetch` is a non-faulting hint. `ptr` is non-null
        // and names a byte the caller already owns (an Arrow buffer or a
        // `RecordBatch` / `ArrayRef` allocation).
        unsafe {
            core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: `prfm` is a non-faulting prefetch hint. `ptr` is non-null
        // and names a byte the caller already owns.
        unsafe {
            core::arch::asm!(
                "prfm pldl1keep, [{0}]",
                in(reg) ptr,
                options(readonly, nostack, preserves_flags)
            );
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // SAFETY: `ptr` is non-null and in-bounds of a live allocation the
        // caller owns; this fallback is a single-byte read to pull the line.
        unsafe {
            let _ = std::ptr::read_volatile(ptr);
        }
    }
}

/// Prefetch the first batch's data buffers and the next batch's headers.
///
/// Called from [`super::query::CachedRawStream::from_raw`] before the stream
/// is returned. Empty input is a no-op.
pub(crate) fn prefetch_raw_serve_arced(batches: &[Arc<RecordBatch>]) {
    let Some((first, rest)) = batches.split_first() else {
        return;
    };
    prefetch_batch_data(first.as_ref());
    if let Some(next) = rest.first() {
        prefetch_batch_headers(next.as_ref());
    }
}

/// Prefetch the `RecordBatch` header, its column `ArrayRef` slots, and the
/// first few `dyn Array` allocations — the lines a later poll and encode
/// touch before any cell values.
pub(crate) fn prefetch_batch_headers(batch: &RecordBatch) {
    prefetch_read_data(std::ptr::from_ref(batch).cast());
    let columns = batch.columns();
    if !columns.is_empty() {
        prefetch_read_data(columns.as_ptr().cast());
    }
    for column in columns.iter().take(PREFETCH_DATA_COLUMNS) {
        prefetch_read_data(Arc::as_ptr(column).cast());
    }
}

/// Prefetch the leading data (and null) buffers of `batch`.
fn prefetch_batch_data(batch: &RecordBatch) {
    prefetch_batch_headers(batch);
    for column in batch.columns().iter().take(PREFETCH_DATA_COLUMNS) {
        prefetch_array_buffers(column.as_ref());
    }
}

/// Touch value (and null) buffers without `Array::to_data`, which would
/// clone every `Buffer` `Arc` on the serve path.
fn prefetch_array_buffers(array: &dyn Array) {
    if let Some(nulls) = array.nulls() {
        prefetch_buffer_bytes(nulls.validity());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        prefetch_typed_values(arr.values());
        return;
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        prefetch_typed_values(arr.values());
        return;
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        prefetch_typed_values(arr.values());
        return;
    }
    if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        prefetch_typed_values(arr.values());
        return;
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        prefetch_buffer_bytes(arr.values());
    }
    // Unknown physical type: headers + null bitmap only. Do not call
    // `to_data()` — that clones buffer `Arc`s on every Raw hit.
}

fn prefetch_typed_values<T>(values: &[T]) {
    if values.is_empty() {
        return;
    }
    let ptr = std::ptr::from_ref(&values[0]).cast::<u8>();
    prefetch_read_data(ptr);
    if std::mem::size_of_val(values) > CACHE_LINE {
        prefetch_read_data(ptr.wrapping_add(CACHE_LINE));
    }
}

fn prefetch_buffer_bytes(bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    let ptr = bytes.as_ptr();
    for line in 0..PREFETCH_LINES_PER_BUFFER {
        let offset = line.saturating_mul(CACHE_LINE);
        if offset >= bytes.len() {
            break;
        }
        // `add` is in-bounds: `offset < bytes.len()`.
        prefetch_read_data(ptr.wrapping_add(offset));
    }
}

#[cfg(test)]
mod tests {
    use super::{prefetch_batch_headers, prefetch_raw_serve_arced};
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1; rows]))])
            .expect("batch")
    }

    #[test]
    fn prefetch_of_empty_and_columnless_batches_does_not_panic() {
        prefetch_raw_serve_arced(&[]);

        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        prefetch_batch_headers(&empty);
        prefetch_raw_serve_arced(&[Arc::new(empty)]);
    }

    #[test]
    fn prefetch_of_a_populated_batch_does_not_panic_or_change_rows() {
        let first = batch(8);
        let second = batch(3);
        prefetch_raw_serve_arced(&[Arc::new(first.clone()), Arc::new(second.clone())]);
        assert_eq!(first.num_rows(), 8);
        assert_eq!(second.num_rows(), 3);
        assert_eq!(first.num_columns(), 1);
    }
}
