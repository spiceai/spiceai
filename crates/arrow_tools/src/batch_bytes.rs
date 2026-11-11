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

//! Resident memory of Arrow batches, counting each physical allocation once.
//!
//! [`RecordBatch::get_array_memory_size`] sums `Buffer::capacity()` once per
//! buffer *reference*, and `capacity()` reports the whole parent allocation
//! however narrow a slice the reference covers. A batch decoded from Arrow IPC
//! (Flight, Flight SQL, a stored IPC blob) reads each message body into one
//! allocation and points every column and child buffer at a slice of it, so that
//! sum bills one allocation once per buffer: on an eleven-column,
//! nineteen-buffer schema it reported 19.0x the memory the batches held
//! (15,379,456 B against 808,960 B resident for ~4,000 rows).
//!
//! # Dedupe key
//!
//! [`Buffer::data_ptr`] — the parent allocation's base address — and never
//! `as_ptr()`, which has the slice offset already applied and so hands every
//! slice of one allocation a distinct key, dedupes nothing, and silently
//! reproduces the per-reference sum. An address identifies an allocation only
//! while it is alive, so a [`RetainedBytes`] must not outlive the batches it
//! was fed: every caller holds them across the accumulation.

use arrow::array::{Array, ArrayData};
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::num::NonZeroUsize;

/// Accumulates the resident memory of a set of batches, counting each physical
/// allocation once however many buffers, columns, batches or slices point into
/// it.
#[derive(Debug, Default)]
pub struct RetainedBytes {
    seen: HashSet<NonZeroUsize>,
    buffer_bytes: usize,
    bookkeeping_bytes: usize,
}

impl RetainedBytes {
    /// An empty accumulator, counting nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The resident memory of `batches`: [`Self::total`] over all of them.
    #[must_use]
    pub fn of(batches: &[RecordBatch]) -> u64 {
        let mut bytes = Self::new();
        for batch in batches {
            bytes.add(batch);
        }
        bytes.total()
    }

    /// Count every allocation `batch` reaches that a previous [`Self::add`]
    /// has not already counted, plus this batch's own Arrow bookkeeping.
    pub fn add(&mut self, batch: &RecordBatch) {
        for column in batch.columns() {
            // What `get_array_memory_size` adds beyond buffer capacity: the
            // fixed-size Arrow structs for the array and each buffer reference.
            // A real per-batch cost — on one-row batches it rivals the values
            // — so a memory limit counts it; it is paid per reference, so it is
            // not deduped.
            self.bookkeeping_bytes = self.bookkeeping_bytes.saturating_add(
                column
                    .get_array_memory_size()
                    .saturating_sub(column.get_buffer_memory_size()),
            );
            self.add_distinct_allocations(&column.to_data());
        }
    }

    /// Buffer capacity of every distinct allocation added so far, and nothing
    /// else.
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.buffer_bytes
    }

    /// Distinct buffer capacity plus the Arrow bookkeeping structs. Equal to
    /// the per-reference [`RecordBatch::get_array_memory_size`] sum when no two
    /// buffers share an allocation, and below it by exactly the double-count
    /// when they do.
    #[must_use]
    pub fn total(&self) -> u64 {
        u64::try_from(self.buffer_bytes.saturating_add(self.bookkeeping_bytes)).unwrap_or(u64::MAX)
    }

    /// Add every allocation reachable from `data` not already counted,
    /// recursing through child arrays.
    fn add_distinct_allocations(&mut self, data: &ArrayData) {
        for buffer in data.buffers() {
            self.add_buffer_once(buffer);
        }
        if let Some(nulls) = data.nulls() {
            self.add_buffer_once(nulls.buffer());
        }
        for child in data.child_data() {
            self.add_distinct_allocations(child);
        }
    }

    fn add_buffer_once(&mut self, buffer: &Buffer) {
        if self.seen.insert(buffer.data_ptr().addr()) {
            self.buffer_bytes = self.buffer_bytes.saturating_add(buffer.capacity());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn fixture() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("maybe", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(
                    ids.iter()
                        .map(|id| (id % 3 == 0).then_some(*id))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    ids.iter()
                        .map(|id| format!("name-{id}"))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("the fixture batch should build")
    }

    fn through_ipc(batch: &RecordBatch) -> (RecordBatch, usize) {
        let mut ipc = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &batch.schema())
                .expect("the IPC writer should build");
            writer.write(batch).expect("the batch should serialize");
            writer.finish().expect("the stream should finish");
        }
        let decoded = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&ipc), None)
            .expect("the IPC reader should build")
            .next()
            .expect("one batch")
            .expect("the batch should decode");
        (decoded, ipc.len())
    }

    /// A limit that moved for batches with nothing shared would loosen the bound
    /// every existing caller was sized against. Nothing is shared here, so the
    /// count must be the per-reference sum exactly.
    #[test]
    fn an_unshared_batch_counts_exactly_what_get_array_memory_size_does() {
        let batch = fixture();
        assert_eq!(
            RetainedBytes::of(std::slice::from_ref(&batch)),
            batch.get_array_memory_size() as u64
        );
    }

    /// The shape the bug lives in: every buffer a slice of one body allocation.
    /// That allocation cannot exceed the stream it was read from, which bounds the
    /// buffer bytes from outside how they are computed.
    #[test]
    fn an_ipc_decoded_batch_counts_its_body_allocation_once() {
        let (decoded, ipc_len) = through_ipc(&fixture());
        let mut bytes = RetainedBytes::new();
        bytes.add(&decoded);
        assert!(
            bytes.buffer_bytes() <= ipc_len,
            "{} B of buffers counted for a batch decoded from {ipc_len} B of IPC",
            bytes.buffer_bytes()
        );
        assert!(
            (decoded.get_array_memory_size() as u64) > bytes.total() * 2,
            "the fixture no longer shares one allocation across its buffers, so this test has \
             stopped covering the over-count"
        );
    }

    /// Slices of one parent spread across a burst hold the parent's allocations
    /// once between them; the bookkeeping is paid per slice.
    #[test]
    fn slices_across_batches_share_one_count_of_the_parent() {
        let parent = fixture();
        let mut whole = RetainedBytes::new();
        whole.add(&parent);
        let mut sliced = RetainedBytes::new();
        for row in 0..parent.num_rows() {
            sliced.add(&parent.slice(row, 1));
        }
        assert_eq!(sliced.buffer_bytes(), whole.buffer_bytes());
    }
}
