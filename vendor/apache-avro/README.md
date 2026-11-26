# apache-avro (spiceai vendored fork)

[`apache-avro`] **v0.21.0** (upstream: <https://github.com/apache/avro-rs>) with one patch to
`src/decode.rs` and its export from `src/lib.rs`, wired in through `[patch.crates-io]` in the
workspace root `Cargo.toml`. It decodes CDC Avro ingest bodies (`data_components::debezium::avro`)
and Iceberg manifests (`iceberg`).

## What the patch does

An Avro array or map block states its item count before its items, and values such as `null` or a
record of `null` fields encode in zero bytes. Upstream reserves each block's declared count before
decoding it, so the declared count, not the input, decides how much memory a decode claims. The
patch bounds that:

- Each datum has one budget of `max_allocation_bytes` bytes (512 MiB by default). Each array or map
  block debits its declared count times the in-memory item size before any of its items decodes,
  and each record, union and enum debits the field list, box or symbol it builds. A debit that does
  not fit fails with the crate's `MemoryAllocation` error. Nested values share the budget.
- The declared length of each bytes, string, fixed and big-decimal value is checked against the
  budget's limit before its buffer is allocated. These values are not debited: each is kept only
  once the input has filled it.
- A block reserves at most `MAX_PREALLOCATED_ITEMS` (1024) items before decoding, then, each time
  the reservation fills, up to `RESERVATION_GROWTH` (1024) times the items decoded so far, with
  `try_reserve`. An array's capacity never grows past the items debited across its blocks. A map's
  hash table rounds its capacity up, so it can reserve up to about 2.3 times the bytes debited for
  it.

A datum whose decoded values exceed its budget fails to decode. With a 56-byte `Value`, the default
budget holds about 9.6 million array items.

`decode_with_max_allocation` decodes a datum with a caller-chosen limit in place of
`max_allocation_bytes`.

## Maintaining the fork

The rest of the crate is byte-for-byte the published 0.21.0 (`src/`, `LICENSE`, `NOTICE`; crate
checksum `36fa98bc79671c7981272d91a8753a928ff6a1cd8e4f20a44c45bd5d313840bf`). `Cargo.toml` is the
published manifest without the example, test and bench targets and dev-dependencies, plus a
`[lints.rust]` entry that quiets upstream's unfulfilled `#[expect(deprecated)]` on its `digest`
imports.

To move to a newer upstream release, re-vendor it and re-apply the patch, or drop the fork once
upstream bounds what a block reserves by its input. `crates/data_components/tests/avro_decode_allocation.rs`
fails if the patch is lost, and `decodes_arrays_and_maps_longer_than_the_up_front_reservation` in
`crates/data_components/src/debezium/avro.rs` checks that long collections still decode whole.

[`apache-avro`]: https://crates.io/crates/apache-avro
