# quick-xml (spiceai vendored forks)

The XML reader, writer, and serde (de)serializer used by `object_store`, `opendal`, `reqsign`,
`calamine`, and `document_parse`.

## Why this is vendored

These are local, minimally modified forks of [`quick-xml`] **v0.39.4**, **v0.40.1**, **v0.41.0**
and **v0.42.0** (upstream: <https://github.com/tafia/quick-xml>), one directory per version,
pulled into the spiceai tree via `[patch.crates-io]` in the workspace root `Cargo.toml`. Each
version is a separate semver line that different dependencies require, so each needs its own copy:

| Fork | Required by |
|---|---|
| `0.39.4/` | `object_store` 0.13 (S3, GCS, Azure, and HTTP stores), `document_parse` (PPTX) |
| `0.40.1/` | `object_store` 0.14 (via `snowflake-api`) |
| `0.41.0/` | `opendal` (S3 and GCS services) |
| `0.42.0/` | `reqsign-aws-core`/`reqsign-aws-v4` (STS responses), `calamine` |

By default, `Attributes` rejects a start tag that repeats an attribute name. Up to 0.40.1 it
compared each new name with every name already seen in the tag, so a tag with `N` attributes cost
`O(N²)` comparisons ([RUSTSEC-2026-0194], [tafia/quick-xml#969]). The serde deserializer, `has_nil`
(reached for every `Option` field read through serde), and any `attributes()` loop all run this
check. It is pure CPU after the body has been read, so a request timeout cannot interrupt it. A
storage or STS endpoint that answers with one crafted start tag could hold a runtime CPU core for
minutes: one tag with 64,000 attributes costs about 4 seconds in a release build of 0.39.4, and
the cost grows with the square of the count.

0.41.0 adds a hash pre-filter once a tag has more than 32 attributes, but a hit on the filter
still scans every earlier name. `has_nil` keeps iterating after `AttrError::Duplicated`, so a tag
of 32,000 distinct names followed by 32,000 repeats of the last one still costs `O(N²)`: about
1.7 seconds in 0.41.0, against 6 milliseconds for 64,000 distinct names. The current upstream
release (0.42.0) and `master` behave the same way.

## What changed vs. upstream

Only `IterState`'s duplicate check, in `src/events/attributes.rs`, changes. The check is the same
in all four forks:

- Up to 32 attributes, the check is upstream's linear scan, unchanged. Real tags stay
  allocation-free.
- Past 32, a boxed `KeyIndex` holds a set of the hashes of the names seen so far, as upstream
  0.41.0 does, so a new name costs one lookup. On the first repeated hash it also builds a map from
  each hash to the position of the first name with that hash, and keeps it up to date from then
  on. A repeated name then costs one lookup and one comparison, and reports the same earlier
  position the scan would have, because the recorded names are distinct. A full scan runs only
  when two different names share a 64-bit hash. A tag of distinct names never builds the map.
- Names are hashed with a randomly keyed SipHash (`RandomState`), so a document author cannot
  precompute names that collide and push the check back onto the scan. Upstream 0.41.0 hashes
  with a fixed key.
- The index is boxed, so every `Attributes` carries one pointer for it, where upstream 0.41.0
  carries an inline `Option<HashSet>`.

0.39.4 and 0.40.1 had no hashed check; their `IterState` gets the whole change, identical to the
other two forks. 0.41.0 and 0.42.0 replace upstream's pre-filter with `KeyIndex`. Every `AttrError`
result and position is the same as upstream's for every input.

Each fork adds `repeats_past_hash_threshold` next to upstream's `duplicate_past_hash_threshold`
(0.39.4 and 0.40.1 also gain that upstream test). Both fail if the index drops names from the
linear phase or reports a wrong position. The rest of each crate is byte-for-byte the published
release (`src/`, `Cargo.toml` apart from a header comment, `LICENSE-MIT.md`), including its CRLF
line endings.

To check a fork, copy its directory out of the workspace, append `[workspace]` to its `Cargo.toml`,
and run `cargo test --lib --features serialize`.

## Advisories

cargo-deny matches advisories only against crates.io packages, so it no longer reports quick-xml
advisories for these forks. Check new quick-xml advisories against each fork by hand.
[RUSTSEC-2026-0195] (unbounded `xmlns` declarations in `NsReader`, fixed upstream in 0.41.0) still
applies to `0.39.4/` and `0.40.1/`, which do not carry its fix; `deny.toml` keeps its entry as that
record.

## Dropping a fork

Drop a fork, and its `[patch.crates-io]` entry, once every crate on that line depends on a
quick-xml release whose duplicate check is `O(N)` for repeated names as well as distinct ones.
Moving `object_store` to 0.14.1 or later only moves its line from `0.40.1/` to `0.41.0/`, and does
not by itself let a fork go.

[`quick-xml`]: https://crates.io/crates/quick-xml
[RUSTSEC-2026-0194]: https://rustsec.org/advisories/RUSTSEC-2026-0194.html
[RUSTSEC-2026-0195]: https://rustsec.org/advisories/RUSTSEC-2026-0195.html
[tafia/quick-xml#969]: https://github.com/tafia/quick-xml/issues/969
