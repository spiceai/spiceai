# apache-avro (spiceai vendored fork)

The Avro decoder behind CDC Avro ingest (`data_components::debezium::avro`) and Iceberg manifest
reads (`iceberg`).

## Why this is vendored

This is a local, minimally modified fork of [`apache-avro`] **v0.21.0**
(upstream: <https://github.com/apache/avro-rs>), pulled into the spiceai tree via
`[patch.crates-io]` in the workspace root `Cargo.toml`.

An Avro array or map is written as blocks, and each block header states its item count before
the items. 0.21.0 calls `reserve` with that count before it decodes any item, and checks the count
only against `max_allocation_bytes` (512 MiB) as if it were a byte count. A five-byte body that
declares 536,870,912 items therefore asks for about 30 GB for an array of `long` and about 87 GB
for a map. Depending on the allocator and the host, the process aborts on the failed allocation,
or it keeps the touched part (about 1 GiB of hash table for the map), so a few concurrent requests
exhaust memory. CDC ingest decodes a request body against the schema in its `X-Avro-Schema`
header, so any client with write access to a `cdc:` dataset could do this. Iceberg manifests are
decoded by the same code.

Capping the reserve alone is not enough. Items that encode in zero bytes, such as `null` or a
record of `null` fields, take no input, so an array of them grows by the declared count with
nothing behind it, and each record item also builds a field list and clones every field name.

0.23.0 adds a similar per-datum budget, but it still reserves up to the whole budget before it
decodes an item, and moving to it needs a matching `iceberg` release, so the fix is carried here.

## What changed vs. upstream 0.21.0

`src/decode.rs` only:

- `decode_internal` gives each datum one budget of `max_allocation_bytes` bytes and decodes it
  with `decode_with_budget`, which is the 0.21.0 `decode_internal` body with the budget passed to
  every recursive call. Each array block debits its declared count times
  `size_of::<Value>()`, and each map block its count times `size_of::<(String, Value)>()`, before
  any of its items is decoded. A block that does not fit fails with the crate's existing
  `MemoryAllocation` error. Nested collections share the budget, so they cannot multiply it.
- The other values the decoder builds without copying them from the input debit the same budget:
  each record its field list (`size_of::<(String, Value)>()` per field) plus its field-name bytes,
  each union its boxed `Value`, and each enum its symbol bytes.
- A fixed value's size, which comes from the schema, is checked against `max_allocation_bytes`
  before its buffer is allocated, as 0.21.0 already does for the declared length of bytes and
  strings. Before, a one-byte body with a `fixed` schema of 1 TiB allocated the whole buffer and
  kept about 1 GB of it resident after the decode failed.
- Bytes, strings and fixed values are not debited from the budget: each is allocated at its
  declared length, at most 512 MiB, and survives the decode only once the input has filled it, so
  what a datum keeps of them is bounded by the input length.
- The array and map arms reserve at most `MAX_PREALLOCATED_ITEMS` (1024) items before decoding,
  so a declared count with no items behind it costs a bounded allocation. Once that many items
  have decoded, the block reserves the rest of its declared count in one step, as 0.21.0 does up
  front, so a long collection is not regrown and rehashed as it fills. That reservation is at
  most what the budget was already debited for, and it is a `try_reserve`: if it fails, the
  collection grows item by item.

A datum holding more than 512 MiB of these decoded values now fails to decode; with a 56-byte
`Value`, that is about 9.6 million array items in one datum. The heap one datum claims stays at
about the budget: in the allocation test, arrays of zero-byte records, unions and enums each peak
within 40 KB of 512 MiB before they are refused.

Decode time against upstream 0.21.0, in the same process with the arms interleaved, is within
run-to-run noise for a 100,000-item `long` array, a 100,000-entry map, 50,000 eight-field records
and a two-field CDC envelope.

The callers of `decode_internal` (`decode`, `from_avro_datum`, the file
reader and the single-object reader) are unchanged.

The rest of the crate is byte-for-byte the published 0.21.0 (`src/`, `LICENSE`, `NOTICE`; crate
checksum `36fa98bc79671c7981272d91a8753a928ff6a1cd8e4f20a44c45bd5d313840bf`). `Cargo.toml` is the
published manifest without the example, test and bench targets and dev-dependencies, which are
not vendored, plus a `[lints.rust]` entry that quiets upstream's unfulfilled
`#[expect(deprecated)]` on its `digest` imports.

To re-sync with a newer upstream release, re-vendor it and re-apply the change, or drop the fork
once upstream reserves no more than the input can fill. `declared_collection_sizes_do_not_drive_allocation`
in `crates/data_components/tests/avro_decode_allocation.rs` fails if the change is lost, and
`decodes_arrays_and_maps_longer_than_the_up_front_reservation` in
`crates/data_components/src/debezium/avro.rs` checks that collections longer than the reserve
still decode whole.

[`apache-avro`]: https://crates.io/crates/apache-avro
