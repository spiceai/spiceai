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

//! Decoding a CDC Avro body against a client-supplied schema allocates in
//! proportion to the body, not to the item counts its array and map block
//! headers declare. An Avro block header states its item count before the
//! items, so a five-byte body can declare 536,870,912 of them, and items such
//! as a record of `null` fields decode from zero bytes.
//!
//! Measures the heap with a counting `#[global_allocator]`, so it is its own
//! test binary, and runs every case from one test so no other test's
//! allocations land in the measurement. The allocator refuses to grow the heap
//! past [`HEAP_CEILING_BYTES`], so an unbounded decode aborts the test instead
//! of exhausting the machine.

#![cfg(feature = "debezium")]
#![expect(clippy::expect_used, reason = "integration-test helpers")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use apache_avro::{Schema, to_avro_datum, types::Value};
use data_components::debezium::avro::{AvroDecodeOptions, parse_avro_change_events};

struct PeakAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Live heap the allocator refuses to exceed.
const HEAP_CEILING_BYTES: usize = 1024 * 1024 * 1024;

fn grow(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn exceeds_ceiling(bytes: usize) -> bool {
    LIVE_BYTES.load(Ordering::Relaxed).saturating_add(bytes) > HEAP_CEILING_BYTES
}

unsafe impl GlobalAlloc for PeakAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if exceeds_ceiling(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if exceeds_ceiling(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if exceeds_ceiling(new_size) {
            return std::ptr::null_mut();
        }
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            grow(new_size);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: PeakAllocator = PeakAllocator;

/// Heap a rejected body may claim: the reserve a block is allowed before its
/// items decode, plus the schema and the error, with room to spare. Without the
/// bound, the within-budget cases below reserve 32 MB to 120 MB.
const MAX_HEAP_BYTES: usize = 1024 * 1024;

/// apache-avro's default allocation limit, which bounds every collection in one
/// datum together.
const MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;

/// A block header declaring 536,870,912 items (zig-zag varint `2^29`) and
/// nothing after it.
const HUGE_BLOCK_COUNT: [u8; 5] = [0x80, 0x80, 0x80, 0x80, 0x04];

/// Decodes `body` against `schema_json` the way a CDC ingest request with an
/// `X-Avro-Schema` header does, returning the error text and the peak heap
/// growth during the decode.
fn decode_error_and_peak_heap(
    runtime: &tokio::runtime::Runtime,
    schema_json: &str,
    body: &[u8],
) -> (String, usize) {
    let options = AvroDecodeOptions {
        avro_schema_json: Some(schema_json.to_string()),
        ..Default::default()
    };
    let base = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(base, Ordering::Relaxed);
    let result = runtime.block_on(parse_avro_change_events(body, &options));
    let peak = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(base);
    match result {
        Ok(events) => panic!(
            "a body of {} bytes decoded to {} change events",
            body.len(),
            events.len()
        ),
        Err(err) => (err.to_string(), peak),
    }
}

/// The body of a single block header declaring `count` items, written by
/// apache-avro's encoder (a block count is a zig-zag varint `long`).
fn block_count(count: i64) -> Vec<u8> {
    to_avro_datum(&Schema::Long, Value::Long(count)).expect("a long encodes")
}

#[test]
fn declared_collection_sizes_do_not_drive_allocation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime builds");

    let map = r#"{"type": "map", "values": "long"}"#;
    let array = r#"{"type": "array", "items": "long"}"#;
    let null_array = r#"{"type": "array", "items": "null"}"#;
    let declared: usize = 536_870_912;
    let map_entry = size_of::<(String, Value)>();
    let array_item = size_of::<Value>();

    // A declared count whose items would not fit in the allocation limit is
    // refused before anything is reserved. Items of `null` encode in zero bytes,
    // so for them the declared count is all that bounds the collection.
    for (schema, item_bytes) in [
        (map, map_entry),
        (array, array_item),
        (null_array, array_item),
    ] {
        let (error, peak) = decode_error_and_peak_heap(&runtime, schema, &HUGE_BLOCK_COUNT);
        assert_eq!(
            error,
            format!(
                "Failed to decode Avro datum: Unable to allocate {} bytes (maximum allowed: {MAX_ALLOCATION_BYTES})",
                declared * item_bytes
            ),
            "{schema}"
        );
        assert!(
            peak < MAX_HEAP_BYTES,
            "{schema}: decoding a 5-byte body grew the heap by {peak} bytes"
        );
    }

    // A declared count that fits in the limit but has no items behind it fails
    // on the first missing item, having reserved only a bounded prefix.
    let truncated = "Failed to decode Avro datum: Failed to read bytes for decoding variable length integer: failed to fill whole buffer";
    for schema in [map, array] {
        let body = block_count(1_000_000);
        let (error, peak) = decode_error_and_peak_heap(&runtime, schema, &body);
        assert_eq!(error, truncated, "{schema}");
        assert!(
            peak < MAX_HEAP_BYTES,
            "{schema}: a block declaring 1,000,000 items with none behind it grew the heap by {peak} bytes"
        );
    }

    // Items that decode from zero bytes but still build heap values: a record
    // of `null` fields allocates its field list and names, and a union read at
    // the end of the input defaults to a boxed `null`. The array's slots take
    // 504,000,000 of the 536,870,912-byte budget, and the items' own values
    // spend the remaining 32,870,912 bytes until one does not fit.
    let block_of_9m = block_count(9_000_000);
    let fields: Vec<String> = (0..64)
        .map(|i| format!(r#"{{"name": "field_{i:02}", "type": "null"}}"#))
        .collect();
    let record_array = format!(
        r#"{{"type": "array", "items": {{"type": "record", "name": "nulls", "fields": [{}]}}}}"#,
        fields.join(", ")
    );
    let union_array = r#"{"type": "array", "items": ["null", "long"]}"#.to_string();
    let record_fields_bytes = 64 * map_entry;
    for (schema, refused_bytes, max_peak) in [
        (record_array, record_fields_bytes, 48 * 1024 * 1024),
        (union_array, array_item, 160 * 1024 * 1024),
    ] {
        let (error, peak) = decode_error_and_peak_heap(&runtime, &schema, &block_of_9m);
        assert_eq!(
            error,
            format!(
                "Failed to decode Avro datum: Unable to allocate {refused_bytes} bytes (maximum allowed: {MAX_ALLOCATION_BYTES})"
            ),
            "{schema}"
        );
        assert!(
            peak < max_peak,
            "{schema}: a block of zero-byte items grew the heap by {peak} bytes"
        );
    }
}
