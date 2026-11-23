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

//! Decoding an Avro datum allocates in proportion to its input, not to the
//! item counts its array and map block headers declare. A block header states
//! its item count before the items, and items such as a record of `null` fields
//! decode from zero bytes.
//!
//! CDC ingest holds a body to a budget that scales with the body's size. Other
//! decodes, such as Iceberg manifest reads, hold each datum to apache-avro's
//! per-datum limit.
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

use apache_avro::{Schema, from_avro_datum, to_avro_datum, types::Value};
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
/// bound, the within-budget cases below reserve 32 MB to 120 MB. Also the slack
/// allowed past the budget for a datum that spends it.
const MAX_HEAP_BYTES: usize = 1024 * 1024;

/// apache-avro's default allocation limit, which bounds every collection in one
/// datum together.
const MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;

/// The budget of a CDC body smaller than 256 KiB.
const CDC_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// A block header declaring 536,870,912 items (zig-zag varint `2^29`) and
/// nothing after it.
const HUGE_BLOCK_COUNT: [u8; 5] = [0x80, 0x80, 0x80, 0x80, 0x04];

/// Runs `decode`, returning its result and the peak heap growth while it ran.
fn with_peak_heap<T>(decode: impl FnOnce() -> T) -> (T, usize) {
    let base = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(base, Ordering::Relaxed);
    let result = decode();
    let peak = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(base);
    (result, peak)
}

/// Decodes `body` against `schema_json` the way a CDC ingest request with an
/// `X-Avro-Schema` header does, returning the error text and the peak heap
/// growth during the decode.
fn cdc_error_and_peak_heap(
    runtime: &tokio::runtime::Runtime,
    schema_json: &str,
    body: &[u8],
) -> (String, usize) {
    let options = AvroDecodeOptions {
        avro_schema_json: Some(schema_json.to_string()),
        ..Default::default()
    };
    let (result, peak) =
        with_peak_heap(|| runtime.block_on(parse_avro_change_events(body, &options)));
    match result {
        Ok(events) => panic!(
            "a body of {} bytes decoded to {} change events",
            body.len(),
            events.len()
        ),
        Err(err) => (err.to_string(), peak),
    }
}

/// The error a CDC body gets when decoding it would pass its budget.
fn cdc_limit_error(body: &[u8]) -> String {
    format!(
        "Failed to decode Avro datum: a {}-byte datum can decode to at most {CDC_BUDGET_BYTES} bytes, and this one declares more. Check that its array, map, bytes, string and fixed lengths match the data it carries. For details, visit: https://spiceai.org/docs/features/cdc",
        body.len()
    )
}

/// Decodes `body` against `schema_json` with apache-avro's per-datum limit, as
/// an Iceberg manifest read does, returning the error text and the peak heap
/// growth during the decode.
fn datum_error_and_peak_heap(schema_json: &str, body: &[u8]) -> (String, usize) {
    let schema = Schema::parse_str(schema_json).expect("the schema parses");
    let (result, peak) = with_peak_heap(|| from_avro_datum(&schema, &mut &body[..], None));
    match result {
        Ok(_) => panic!("a body of {} bytes decoded", body.len()),
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
    let map_entry = size_of::<(String, Value)>();
    let array_item = size_of::<Value>();

    // A declared count whose items would not fit in a CDC body's budget is
    // refused before anything is reserved. Items of `null` encode in zero bytes,
    // so for them the declared count is all that bounds the collection.
    for schema in [map, array, null_array] {
        let (error, peak) = cdc_error_and_peak_heap(&runtime, schema, &HUGE_BLOCK_COUNT);
        assert_eq!(error, cdc_limit_error(&HUGE_BLOCK_COUNT), "{schema}");
        assert!(
            peak < MAX_HEAP_BYTES,
            "{schema}: decoding a 5-byte body grew the heap by {peak} bytes"
        );
    }

    // A small CDC body holds its `null` items to the 64 MiB budget, less the
    // record that holds them: the most that fit decode, and one more is refused
    // before anything is reserved. Growing the array to its last block's size
    // briefly holds its old and its new buffer.
    let null_field = r#"{"type": "record", "name": "r", "fields": [{"name": "a", "type": {"type": "array", "items": "null"}}]}"#;
    let fitting = (CDC_BUDGET_BYTES - map_entry - "a".len()) / array_item;
    let mut body = block_count(i64::try_from(fitting).expect("the count fits in a long"));
    body.push(0);
    let (error, peak) = cdc_error_and_peak_heap(&runtime, null_field, &body);
    assert_eq!(
        error,
        "Failed to map Avro Debezium payload: missing or invalid op field: None"
    );
    assert!(
        peak < 2 * CDC_BUDGET_BYTES + MAX_HEAP_BYTES,
        "{fitting} null items grew the heap by {peak} bytes"
    );
    let mut body = block_count(i64::try_from(fitting + 1).expect("the count fits in a long"));
    body.push(0);
    let (error, peak) = cdc_error_and_peak_heap(&runtime, null_field, &body);
    assert_eq!(error, cdc_limit_error(&body));
    assert!(
        peak < MAX_HEAP_BYTES,
        "{} null items grew the heap by {peak} bytes",
        fitting + 1
    );

    // A fixed value's size comes from the schema, and its buffer is allocated
    // before the input is read, so it is checked against the body's budget.
    let fixed = format!(
        r#"{{"type": "fixed", "name": "f", "size": {}}}"#,
        CDC_BUDGET_BYTES + 1
    );
    let (error, peak) = cdc_error_and_peak_heap(&runtime, &fixed, b"x");
    assert_eq!(error, cdc_limit_error(b"x"));
    assert!(
        peak < MAX_HEAP_BYTES,
        "a one-byte body with a fixed schema past the budget grew the heap by {peak} bytes"
    );

    // The cases below decode with apache-avro's per-datum limit.

    // A declared count that fits in the limit but has no items behind it fails
    // on the first missing item, having reserved only a bounded prefix.
    let truncated =
        "Failed to read bytes for decoding variable length integer: failed to fill whole buffer";
    for schema in [map, array] {
        let body = block_count(1_000_000);
        let (error, peak) = datum_error_and_peak_heap(schema, &body);
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
    // spend the remaining 32,870,912 bytes until one does not fit. Once 1024
    // items have decoded, the array reserves the slots the budget was debited
    // for, so the heap reaches the budget but not past it.
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
    for (schema, refused_bytes) in [
        (record_array, record_fields_bytes),
        (union_array, array_item),
    ] {
        let (error, peak) = datum_error_and_peak_heap(&schema, &block_of_9m);
        assert_eq!(
            error,
            format!(
                "Unable to allocate {refused_bytes} bytes (maximum allowed: {MAX_ALLOCATION_BYTES})"
            ),
            "{schema}"
        );
        assert!(
            peak < MAX_ALLOCATION_BYTES + MAX_HEAP_BYTES,
            "{schema}: a block of zero-byte items grew the heap by {peak} bytes"
        );
    }

    // An enum item encodes in one byte but clones its symbol, so one byte of
    // input per 4,096-byte symbol spends the same remaining budget.
    let symbol = format!("S{}", "a".repeat(4095));
    let enum_array = format!(
        r#"{{"type": "array", "items": {{"type": "enum", "name": "e", "symbols": ["{symbol}"]}}}}"#
    );
    let mut enum_body = block_of_9m;
    enum_body.resize(enum_body.len() + 9_000, 0);
    let (error, peak) = datum_error_and_peak_heap(&enum_array, &enum_body);
    assert_eq!(
        error,
        format!(
            "Unable to allocate {} bytes (maximum allowed: {MAX_ALLOCATION_BYTES})",
            symbol.len()
        )
    );
    assert!(
        peak < MAX_ALLOCATION_BYTES + MAX_HEAP_BYTES,
        "an array of enum items grew the heap by {peak} bytes"
    );

    // A block whose first items are present and whose input then stops has
    // reserved at most 1024 times the items it supplied: here 1,100 items, so
    // 1,024 then 1,048,576 more, against declared counts that would reserve
    // the whole budget. A map's hash table rounds its capacity up to a power of
    // two of at least 8/7 of the request, plus one control byte per bucket.
    let reserved_items: usize = 1024 + 1024 * 1024;
    let map_buckets = (reserved_items * 8 / 7).next_power_of_two();
    for (schema, count, max_peak) in [
        (array, 9_000_000, reserved_items * array_item),
        (map, 6_000_000, map_buckets * (map_entry + 1)),
    ] {
        let mut body = block_count(count);
        for i in 0..1_100 {
            if schema == map {
                body.extend(
                    to_avro_datum(&Schema::String, Value::String(format!("k{i}")))
                        .expect("a key encodes"),
                );
            }
            body.extend(to_avro_datum(&Schema::Long, Value::Long(i)).expect("a long encodes"));
        }
        let (error, peak) = datum_error_and_peak_heap(schema, &body);
        assert_eq!(error, truncated, "{schema}");
        assert!(
            peak < max_peak + MAX_HEAP_BYTES,
            "{schema}: a block declaring {count} items with 1,100 behind it grew the heap by {peak} bytes"
        );
    }

    // An array split over blocks grows to the items debited for all of them
    // and no further, though growing briefly holds the old and the new buffer:
    // two blocks of `null` items totalling 9,500,000 peak at the first block's
    // 5,000,000 slots plus 9,500,000, not at a doubled 10,000,000.
    let schema = Schema::parse_str(null_array).expect("the schema parses");
    let mut body = block_count(5_000_000);
    body.extend(block_count(4_500_000));
    body.push(0);
    let (decoded, peak) = with_peak_heap(|| from_avro_datum(&schema, &mut &body[..], None));
    let Ok(Value::Array(items)) = decoded else {
        panic!("two blocks of null items did not decode to an array: {decoded:?}");
    };
    assert_eq!(items.len(), 9_500_000);
    drop(items);
    assert!(
        peak < (5_000_000 + 9_500_000) * array_item + MAX_HEAP_BYTES,
        "an array of two blocks grew the heap by {peak} bytes"
    );

    // A fixed value's size is checked against the limit before its buffer is
    // allocated.
    let fixed_size: usize = 1 << 40;
    let fixed = format!(r#"{{"type": "fixed", "name": "f", "size": {fixed_size}}}"#);
    let (error, peak) = datum_error_and_peak_heap(&fixed, b"x");
    assert_eq!(
        error,
        format!("Unable to allocate {fixed_size} bytes (maximum allowed: {MAX_ALLOCATION_BYTES})")
    );
    assert!(
        peak < MAX_HEAP_BYTES,
        "a one-byte body with a {fixed_size}-byte fixed schema grew the heap by {peak} bytes"
    );
}
