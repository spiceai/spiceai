# chrono-tz (Spice.ai)

IANA time zones for [chrono]: the part of [chrono-tz] 0.10.4 that Spice.ai and its dependencies
use, generated from the tz database release in `tz/`.

## Why this crate exists

chrono-tz's maintainer is winding the crate down along with chrono ([chronotope/chrono#1768]), and
a RustSec advisory that marks chrono-tz and chrono-tz-build unmaintained is open
([rustsec/advisory-db#3214]). Its last release, 0.10.4, carries tz database 2025b. IANA has
released several databases since, and some of them change current time: from 2026-11-01, British
Columbia, Alberta, the Northwest Territories and Manitoba keep standard time, Cuba keeps daylight
time, and Morocco moved to permanent UTC on 2026-09-20. With the 2025b tables, Spice shows those
zones one hour off.

Arrow, DataFusion, orc-rust and tera depend on chrono-tz in their public APIs, so Spice cannot
stop depending on it. The root `Cargo.toml` patches crates.io `chrono-tz` 0.10 to this crate, and
0.9, which tera asks for, to `compat/0.9`, which re-exports this crate. Every time zone
conversion those crates make reads the same tables.

clickhouse-rs and the ClickHouse connector still use crates.io `chrono-tz` 0.8.6. A re-export
cannot serve them: the `spiceai/clickhouse-rs` fork converts 0.8's `ParseError`, a `String`, with
`?`, and `Tz` brings its own `FromStr` error type. Once the fork asks for 0.10, the root patch
covers it too.

## What it contains

- `src/`: the code of chrono-tz 0.10.4 from crates.io (the `.crate` whose checksum `Cargo.lock`
  recorded, `a6139a85…26c3`), with the items the build uses, as upstream wrote them.
- `build.rs`: the code generator of chrono-tz-build 0.5.0, which produced chrono-tz 0.10.4's
  tables, without its `filter-by-regex` and `case-insensitive` modes. It reads `tz/` and writes
  the `Tz` enum, its name lookup and every zone's transitions to `OUT_DIR`. Run over 0.10.4's
  own `tz/` with parse-zoneinfo 0.4.1 and phf_codegen 0.12, it writes 0.10.4's
  `src/prebuilt/directory.rs` byte for byte, and its `timezones.rs` except for three intended
  edits: no `arbitrary` derive, `ParseError` implements `Error` without a `std` feature, and
  `TZ_VARIANTS` is test-only. This crate uses phf 0.13, which lays out the name lookup
  differently and finds the same zones.
- `tz/`: the IANA source files parse-zoneinfo reads (`africa`, `antarctica`, `asia`,
  `australasia`, `backward`, `etcetera`, `europe`, `northamerica`, `southamerica`), `NEWS`, whose
  first `Release` line names the release (`IANA_TZDB_VERSION`), and `LICENSE`.
- **Kept**: every item that a workspace crate or a dependency references, with the `release`
  feature set and all targets on macOS: `Tz`, with every zone of the release, its `FromStr` parsing and
  `name()`; `ParseError`; `TzOffset`, the offset type of `Tz`'s `chrono::TimeZone` impl; and `UTC`.
  Uses were found by marking each public chrono-tz item `#[deprecated]` with a unique note and
  checking the workspace with `RUSTFLAGS="--force-warn deprecated"`, which reports uses inside
  dependencies too. The zones themselves all stay, because dependents parse zone names that
  arrive at run time.
- **Removed**: the rest of the public API: `OffsetComponents`, `OffsetName`, `GapInfo` and
  `Default for Tz`; the `serde`, `arbitrary`, `case-insensitive` and `filter-by-regex` features;
  and the `no_std` paths. `TZ_VARIANTS`, `IANA_TZDB_VERSION` and the `Europe::London`-style
  constant for each zone are no longer public and stay for the tests only (`#[cfg(test)]`).
- **Features**: `default` and `std`, the names dependents request, are no-ops.
- **Tests**: upstream's tests of the kept code; `tests/tzdata_releases.rs`, which checks the rule
  changes each release since 2025b announced against the local times its `NEWS` entry gives;
  and `src/system_tzdb.rs`, which compares every zone with the system tz database (below).

## Updating the tz database

IANA announces releases on the [tz-announce] list. To take one:

1. Download `tzdata<release>.tar.gz` from <https://data.iana.org/time-zones/releases/> and check
   it against its `.asc` signature, or against the files at the release's tag in
   <https://github.com/eggert/tz>.
2. Copy the nine source files above, `NEWS` and `LICENSE` from it into `tz/`. The diff of `tz/` is
   the release itself: review reads the rule changes.
3. Add the release's changes to current or future time to `tests/tzdata_releases.rs`, with the
   local times its `NEWS` entry gives, and to `test/spicescript/timezone-rules.yaml`.
4. Run `cargo test -p chrono-tz`, and the system comparison on a machine whose system tz database
   is the new release (macOS records it in `/usr/share/zoneinfo/+VERSION`):
   `cargo test -p chrono-tz --lib -- --ignored system_tzdb`.

`build.rs` reruns when `tz/` changes, so every crate that converts time zones rebuilds with the
new tables.

## When a dependency needs a removed item

A dependency update that calls a removed item fails to compile with an error such as
`cannot find ... in crate chrono_tz`. Copy that item, and the tests that cover it, from the
[chrono-tz 0.10.4 source] into the same module here.

[chrono]: https://github.com/chronotope/chrono
[chrono-tz]: https://github.com/chronotope/chrono-tz
[chrono-tz 0.10.4 source]: https://docs.rs/crate/chrono-tz/0.10.4/source/
[chronotope/chrono#1768]: https://github.com/chronotope/chrono/issues/1768
[rustsec/advisory-db#3214]: https://github.com/rustsec/advisory-db/pull/3214
[tz-announce]: https://lists.iana.org/postorius/lists/tz-announce.iana.org/
