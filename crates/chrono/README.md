# chrono (Spice.ai)

The date and time types Spice.ai builds on: the part of [chrono] 0.4.45 that Spice and its
dependencies use.

## Why this crate exists

chrono's maintainer is winding the crate down ([chronotope/chrono#1768]), and a RustSec advisory
that marks it unmaintained is open ([rustsec/advisory-db#3214]). Arrow, DataFusion, Parquet,
Iceberg, Delta Lake, chrono-tz and most of the database drivers Spice links use chrono types in
their public APIs, so Spice cannot stop depending on them. The root `Cargo.toml` patches crates.io
`chrono` to this crate, so every crate in the build compiles against one copy that Spice owns.

## What it contains

The source is chrono 0.4.45 from crates.io (the `.crate` whose checksum `Cargo.lock` recorded,
`1aa79e62…6327`). The crate keeps chrono's module paths and the items the build uses, as upstream
wrote them:

- **Kept**: every item that a workspace crate or a dependency references, with the
  `release` feature set and all targets. Uses were found by marking each public chrono item
  `#[deprecated]` with a unique note and checking the workspace with
  `RUSTFLAGS="--force-warn deprecated"`, which reports uses inside dependencies too.
  Anything only the removed code used was then removed as dead code, checked on macOS, Linux and
  Windows.
- **Removed**: the rest of the public API, for example `Month`, `WeekdaySet`, `NaiveWeek`,
  `SubsecRound`, `DurationRound::duration_round_up`, `Datelike::{day0, ordinal0, with_ordinal,
  with_ordinal0, year_ce, num_days_in_month}`, the `chrono::serde::ts_*` modules, and the
  deprecated `MIN_DATE`/`MAX_DATE` constants. Also removed: the code behind features that no
  dependency enables (`rkyv`, `arbitrary`, `defmt`, `unstable-locales`), the `no_std` paths, and
  the code for targets Spice does not build (wasm, Android, OpenHarmony, AIX).
- **Features**: the crates.io feature names dependencies request (`alloc`, `std`, `clock`, `now`,
  `oldtime`, `wasmbind`, `serde`) still exist, and all of them are no-ops: the standard library,
  the system clock, the local time zone and serde support are always built.
- **Tests**: upstream's tests of the kept code still run. Tests of removed API are gone, and so
  are the `bincode` round trips. `TimeDelta::{MIN, MAX}` and `Weekday::{succ, pred}` stay for
  the tests only (`#[cfg(test)]`).
- **Lints**: the crate uses the workspace lints. `src/lib.rs` expects the ones the upstream code
  trips, with the reason for each group. An expectation that stops firing fails the build, so
  remove it from the list when the code it covers changes.

## When a dependency needs a removed item

A dependency update that calls a removed item fails to compile with an error such as
`no method named … found for struct NaiveDate`. Copy that item, and the tests that cover it, from
the [chrono 0.4.45 source] into the same module here.

[chrono]: https://github.com/chronotope/chrono
[chrono 0.4.45 source]: https://docs.rs/crate/chrono/0.4.45/source/
[chronotope/chrono#1768]: https://github.com/chronotope/chrono/issues/1768
[rustsec/advisory-db#3214]: https://github.com/rustsec/advisory-db/pull/3214
