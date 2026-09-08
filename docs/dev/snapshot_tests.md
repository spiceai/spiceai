# Snapshot Tests

Spice makes use of `insta` for snapshot testing.

## Updating snapshots

To update snapshots, install [`cargo insta`](https://insta.rs/docs/cli/) and run `cargo insta review`.

Alternatively, rename the `*.snap.new` files that are generated to `*.snap` and commit them after reviewing the diffs.

## Adding a new snapshot test

Add a new snapshot test by using one of the [`assert_snapshot!`](https://insta.rs/docs/quickstart/#writing-tests) macros provided by `insta`.

The `insta` docs are a good place to look for more advanced usage, such as the ability to "redact" part of the input that changes across test runs: <https://insta.rs/docs/>
